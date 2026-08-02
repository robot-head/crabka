//! The crabgresql AST for the SP2 slice.

use crabka_pgtypes::{ColumnType, Datum};

/// A relation name exactly as written: an optional schema qualifier and a name.
///
/// This is deliberately *unresolved*. The parser has no catalog, so it carries
/// `s.t` as the pair `(Some("s"), "t")` whether or not `s` exists. The executor
/// decides how to report a missing schema. `PostgreSQL` reports
/// `3F000 schema "s" does not exist` from a utility statement such as
/// `DROP TABLE s.t` but `42P01 relation "s.t" does not exist` from a
/// `SELECT`-style reference, a distinction the parser cannot draw.
///
/// Both parts arrive from the lexer already case-folded (unquoted spellings
/// lowercased, quoted ones preserved), so [`Display`](std::fmt::Display)
/// renders exactly the dotted, unquoted form `PostgreSQL` names in those
/// messages: `SELECT * FROM S.T` reports `relation "s.t" does not exist`.
///
/// There is no N-part form. The engine has one database, so a three-part name
/// is only ever the `cross-database references are not implemented` refusal,
/// which is a check against two fields rather than a `match` on a list length
/// in every consumer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct RelationRef {
    /// The qualifier, when one was written. `None` is a bare name, which
    /// resolves against the search path.
    pub schema: Option<String>,
    /// The relation's own name, never containing the qualifier. A quoted
    /// `"a.b"` is one name with a dot in it, not a qualified reference.
    pub name: String,
}

impl RelationRef {
    /// An unqualified reference: `t`.
    #[must_use]
    pub fn bare(name: impl Into<String>) -> Self {
        Self {
            schema: None,
            name: name.into(),
        }
    }

    /// A schema-qualified reference: `s.t`.
    #[must_use]
    pub fn qualified(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: Some(schema.into()),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for RelationRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.schema {
            Some(schema) => write!(f, "{schema}.{}", self.name),
            None => f.write_str(&self.name),
        }
    }
}

impl From<&str> for RelationRef {
    /// The whole string is the relation's name. A dot in it is part of the
    /// name, exactly as a quoted `"a.b"` is. Use [`RelationRef::qualified`]
    /// for a qualifier.
    fn from(name: &str) -> Self {
        Self::bare(name)
    }
}

impl From<String> for RelationRef {
    /// See [`From<&str>`](RelationRef::from).
    fn from(name: String) -> Self {
        Self::bare(name)
    }
}

/// The point at which an ordinary trigger fires relative to its event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

/// One event named by a `CREATE TRIGGER` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerEvent {
    Insert,
    Update { columns: Vec<String> },
    Delete,
    Truncate,
}

/// Whether an ordinary trigger fires once per affected row or once per statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerLevel {
    Row,
    Statement,
}

/// An `OLD TABLE` or `NEW TABLE` transition relation declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerTransition {
    pub old: bool,
    pub name: String,
}

/// `CREATE [OR REPLACE] [CONSTRAINT] TRIGGER`.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTrigger {
    pub name: String,
    pub or_replace: bool,
    pub constraint: bool,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub table: RelationRef,
    pub referenced_table: Option<RelationRef>,
    pub deferrable: bool,
    pub initially_deferred: bool,
    pub transitions: Vec<TriggerTransition>,
    pub level: TriggerLevel,
    pub when: Option<Expr>,
    /// Exact source of the `WHEN` condition, without its enclosing parentheses.
    pub when_source: Option<String>,
    pub function: String,
    pub arguments: Vec<String>,
}

/// The two actions supported by `ALTER TRIGGER` in `PostgreSQL` 18.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTriggerAction {
    RenameTo(String),
    DependsOnExtension { extension: String, dependent: bool },
}

/// Events supported by `CREATE EVENT TRIGGER` in `PostgreSQL` 18.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventTriggerEvent {
    Login,
    DdlCommandStart,
    DdlCommandEnd,
    SqlDrop,
    TableRewrite,
}

/// A `WHEN variable IN (...)` event-trigger filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTriggerFilter {
    pub variable: String,
    pub values: Vec<String>,
}

/// `CREATE EVENT TRIGGER`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateEventTrigger {
    pub name: String,
    pub event: EventTriggerEvent,
    pub filters: Vec<EventTriggerFilter>,
    pub function: String,
}

/// Firing modes shared by ordinary and event triggers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerEnableMode {
    Origin,
    Replica,
    Always,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerSelector {
    Named(String),
    All,
    User,
}

/// Actions supported by `ALTER EVENT TRIGGER` in `PostgreSQL` 18.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterEventTriggerAction {
    Enable(TriggerEnableMode),
    OwnerTo(String),
    RenameTo(String),
}

#[rustfmt::skip]
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// A `PostgreSQL` command that is recognized deliberately but cannot be
    /// executed by the Gres architecture. Metadata lives on [`RefusalCommand`]
    /// so parser, session, and compatibility tooling share one contract.
    CompatibilityRefusal(RefusalCommand),
    CreateTrigger(CreateTrigger),
    AlterTrigger {
        name: String,
        table: RelationRef,
        action: AlterTriggerAction,
    },
    DropTrigger {
        name: String,
        table: RelationRef,
        if_exists: bool,
        cascade: bool,
    },
    CreateEventTrigger(CreateEventTrigger),
    AlterEventTrigger {
        name: String,
        action: AlterEventTriggerAction,
    },
    DropEventTrigger {
        name: String,
        if_exists: bool,
        cascade: bool,
    },
    CreateTable {
        name: RelationRef,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
        sharded: bool,
        sharding: Option<ShardingSpec>,
        /// `CREATE TABLE IF NOT EXISTS`: an existing relation is a notice, not
        /// a `42P07`.
        if_not_exists: bool,
        /// `CREATE TEMP`/`TEMPORARY TABLE`: the relation lives only for the
        /// creating session.
        temporary: bool,
        /// `(LIKE source [INCLUDING …])` clauses, in the order written.
        like: Vec<LikeClause>,
        /// `INHERITS (parent, …)` parent relation names.
        inherits: Vec<RelationRef>,
        /// `ON COMMIT {PRESERVE ROWS | DELETE ROWS | DROP}` for a temp table.
        on_commit: Option<OnCommitAction>,
        /// `PARTITION BY <strategy> (<key>, …)`: the relation is a partitioned
        /// parent and holds no rows of its own.
        partition_by: Option<PartitionBy>,
        /// `PARTITION OF <parent> <bound>`: the relation is a leaf (or an
        /// intermediate parent, when `partition_by` is also set).
        partition_of: Option<PartitionOf>,
        /// Explicit non-default relation placement.
        tablespace: Option<String>,
    },
    CreateIndex {
        /// An index name is never schema-qualified in `PostgreSQL`'s grammar,
        /// because an index lands in its table's schema. So this carries a
        /// qualifier only for the `CREATE SEQUENCE` spelling, which shares this
        /// variant.
        name: Option<RelationRef>,
        table: RelationRef,
        keys: Vec<IndexKey>,
        unique: bool,
        placement: IndexPlacement,
        if_not_exists: bool,
        concurrently: bool,
        /// `USING <method>`: the access method name, lowercased.
        method: Option<String>,
        /// `INCLUDE (col, …)` non-key payload columns.
        include: Vec<String>,
        /// Source text of a partial index's `WHERE` predicate.
        predicate: Option<String>,
        tablespace: Option<String>,
    },
    /// `COMMENT ON <kind> <name> IS {'text' | NULL}`.
    Comment {
        /// Lowercase object-kind keyword (`table`, `column`, `index`, …).
        object_kind: String,
        /// Catalog name of the object; `table.column` for a column comment.
        object_name: String,
        comment: Option<String>,
    },
    DropIndex {
        name: RelationRef,
        if_exists: bool,
        /// `CASCADE` was written: dependent objects are dropped too rather than
        /// the drop being refused with 2BP01. `RESTRICT` is the default and is
        /// accepted as the explicit spelling of it.
        cascade: bool,
    },
    AlterIndexTablespace {
        name: RelationRef,
        tablespace: String,
    },
    CreateView {
        name: RelationRef,
        /// Exact query text following `AS`, retained for durable catalog storage.
        definition: String,
        /// Parsed definition used by the executor to validate the view schema.
        query: QueryExpr,
        /// `CREATE OR REPLACE VIEW`: the statement redefines an existing view
        /// of the same name in place and does not report 42P07, if the new
        /// query keeps every existing output column's name, type and
        /// collation.
        or_replace: bool,
        /// `CREATE TEMP VIEW`: the view lives in the session's temporary
        /// namespace and dies with the session. A view over a temporary
        /// relation becomes one whether or not this was written.
        temporary: bool,
        /// The optional `VIEW name (a, b, c)` alias list, which renames the
        /// query's output columns positionally.
        columns: Option<Vec<String>>,
    },
    DropTable {
        /// One entry per name in `DROP TABLE a, b, c`; the drop is
        /// all-or-nothing across the list, matching `PostgreSQL`.
        names: Vec<RelationRef>,
        if_exists: bool,
        /// `CASCADE` was written: dependent objects (views) are dropped too
        /// rather than the drop being refused with 2BP01.
        cascade: bool,
    },
    DropView {
        name: RelationRef,
        if_exists: bool,
        /// `CASCADE` was written: dependent objects are dropped too rather than
        /// the drop being refused with 2BP01. `RESTRICT` is the default and is
        /// accepted as the explicit spelling of it.
        cascade: bool,
    },
    /// `CREATE SCHEMA [IF NOT EXISTS] [name] [AUTHORIZATION role] [<element>…]`.
    CreateSchema {
        /// `None` for `CREATE SCHEMA AUTHORIZATION role`, whose schema takes
        /// the role's own name.
        name: Option<String>,
        authorization: Option<String>,
        if_not_exists: bool,
        /// The `CREATE TABLE`/`CREATE VIEW`/`GRANT` statements written inside
        /// the `CREATE SCHEMA`, in order. They run after the schema exists.
        elements: Vec<Statement>,
    },
    /// `ALTER SCHEMA name {RENAME TO … | OWNER TO …}`.
    AlterSchema {
        name: String,
        action: AlterSchemaAction,
    },
    /// `DROP SCHEMA [IF EXISTS] name [, …] [CASCADE | RESTRICT]`.
    DropSchema {
        names: Vec<String>,
        if_exists: bool,
        cascade: bool,
    },
    /// `ALTER TABLE [IF EXISTS] [ONLY] name <action> [, <action> …]`. The
    /// comma form is one statement: every action applies atomically or none
    /// does, matching `PostgreSQL`.
    AlterTable {
        table: RelationRef,
        if_exists: bool,
        actions: Vec<AlterTableAction>,
    },
    Insert {
        table: RelationRef,
        columns: Option<Vec<String>>,
        source: InsertSource,
        /// The statement's `WITH` list, which may contain data-modifying CTEs.
        with: Option<WithClause>,
        /// `ON CONFLICT …` (absent for a plain INSERT).
        on_conflict: Option<OnConflict>,
        returning: Option<Returning>,
    },
    Query(QueryExpr),
    Begin {
        isolation: Option<IsolationLevel>,
        /// `READ ONLY` / `READ WRITE`; `None` when the statement said neither.
        read_only: Option<bool>,
        /// `DEFERRABLE` / `NOT DEFERRABLE`; `None` when the statement said
        /// neither. Only meaningful for a SERIALIZABLE READ ONLY transaction.
        deferrable: Option<bool>,
    },
    /// `COMMIT`/`END [WORK|TRANSACTION] [AND [NO] CHAIN]`.
    Commit {
        /// `AND CHAIN`: end this block and immediately open another with the
        /// same transaction characteristics.
        chain: bool,
    },
    /// `ROLLBACK`/`ABORT [WORK|TRANSACTION] [AND [NO] CHAIN]`.
    Rollback {
        /// `AND CHAIN`, as on [`Statement::Commit`].
        chain: bool,
    },
    Update {
        table: RelationRef,
        /// The statement's `WITH` list, which may contain data-modifying CTEs.
        with: Option<WithClause>,
        /// `UPDATE t AS x …`: the target's alias, which replaces the table name
        /// as the qualifier every expression in the statement resolves against.
        alias: Option<String>,
        assignments: Vec<Assignment>,
        /// `UPDATE … FROM a, b …`: extra relations joined to the target. Empty
        /// for the plain form.
        from: Vec<TableExpr>,
        filter: Option<Expr>,
        returning: Option<Returning>,
    },
    Delete {
        table: RelationRef,
        /// The statement's `WITH` list, which may contain data-modifying CTEs.
        with: Option<WithClause>,
        /// `DELETE FROM t AS x …`: see [`Statement::Update`]'s `alias`.
        alias: Option<String>,
        /// `DELETE … USING a, b …`: `USING` is `DELETE`'s spelling of `FROM`.
        using: Vec<TableExpr>,
        filter: Option<Expr>,
        returning: Option<Returning>,
    },
    /// `MERGE INTO target USING source ON join_condition WHEN …`.
    Merge {
        table: RelationRef,
        /// The statement's `WITH` list, which may contain data-modifying CTEs.
        with: Option<WithClause>,
        alias: Option<String>,
        source: MergeSource,
        on: Expr,
        /// The `WHEN` clauses in written order; the first whose match kind and
        /// optional `AND` condition hold decides a row's action.
        clauses: Vec<MergeWhen>,
        returning: Option<Returning>,
    },
    /// `CREATE TABLE … AS <query>` and its `SELECT … INTO <table>` spelling.
    CreateTableAs {
        name: RelationRef,
        if_not_exists: bool,
        /// An explicit output column list, which renames the query's columns.
        columns: Option<Vec<String>>,
        query: Box<QueryExpr>,
        /// `WITH DATA` (the default) populates the table; `WITH NO DATA` creates
        /// it empty.
        with_data: bool,
        tablespace: Option<String>,
    },
    /// `VACUUM` with any option/table tail, accepted as a hint: reclamation is
    /// autonomous (adaptive background vacuum with idle drain), so the command
    /// carries no payload.
    Vacuum,
    Truncate {
        /// One entry per name in `TRUNCATE a, b, c`; the statement is
        /// all-or-nothing across the list, matching `PostgreSQL`.
        names: Vec<RelationRef>,
        /// `RESTART IDENTITY` was given (`CONTINUE IDENTITY` is the default).
        restart_identity: bool,
        /// `CASCADE` was given, widening the truncated set to every table
        /// holding a foreign key onto one of `names` (and, transitively, onto
        /// those). `RESTRICT`, the default, instead refuses with `0A000` when
        /// such a table is not itself listed. `CASCADE` does not fire
        /// `ON DELETE` actions; it only enlarges the set.
        cascade: bool,
    },
    /// SP37: `SET [LOCAL] <name> = <value>` / `SET <name> TO <value>` / `SET TIME ZONE ...`.
    Set {
        local: bool,
        name: String,
        value: SetValue,
    },
    /// SP37: `SHOW <name>` / `SHOW TIME ZONE`.
    Show {
        name: String,
    },
    /// SP37: `RESET <name>`.
    Reset {
        target: ResetTarget,
    },
    /* SQL parity matrix row: CREATE ROLE / CREATE USER. */ CreateRole {
        name: String,
        can_login: bool,
        member_of: Vec<String>,
    },
    /* SQL parity matrix row: DROP ROLE / DROP USER. */ DropRole {
        name: String,
    },
    /* SQL parity matrix row: GRANT. */ GrantTablePrivileges {
        privileges: Vec<String>,
        table: RelationRef,
        grantees: Vec<String>,
    },
    /* SQL parity matrix row: GRANT. */ GrantSchemaPrivileges {
        privileges: Vec<String>,
        schemas: Vec<String>,
        grantees: Vec<String>,
    },
    /* SQL parity matrix row: REVOKE. */ RevokeTablePrivileges {
        privileges: Vec<String>,
        table: RelationRef,
        grantees: Vec<String>,
    },
    /* SQL parity matrix row: REVOKE. */ RevokeSchemaPrivileges {
        privileges: Vec<String>,
        schemas: Vec<String>,
        grantees: Vec<String>,
    },
    /* SQL parity matrix row: SET ROLE. */ SetRole {
        role: Option<String>,
        /// `true` for the `RESET ROLE` spelling, which `PostgreSQL` tags
        /// `RESET` where `SET ROLE NONE` tags `SET`.
        reset: bool,
    },
    // SP40: FDW DDL
    /// `CREATE FOREIGN DATA WRAPPER <name> OPTIONS (…)`
    CreateFdw {
        name: String,
        options: OptionList,
    },
    /// `DROP FOREIGN DATA WRAPPER [IF EXISTS] <name>`
    DropFdw {
        name: String,
        if_exists: bool,
        /// `CASCADE` was written: dependent objects are dropped too rather than
        /// the drop being refused with 2BP01. `RESTRICT` is the default and is
        /// accepted as the explicit spelling of it.
        cascade: bool,
    },
    /// `CREATE SERVER <name> FOREIGN DATA WRAPPER <wrapper> OPTIONS (…)`
    CreateServer {
        name: String,
        wrapper: String,
        options: OptionList,
    },
    /// `ALTER SERVER <name> OPTIONS (…)`
    AlterServer {
        name: String,
        options: OptionList,
    },
    /// `DROP SERVER [IF EXISTS] <name>`
    DropServer {
        name: String,
        if_exists: bool,
        /// `CASCADE` was written: dependent objects are dropped too rather than
        /// the drop being refused with 2BP01. `RESTRICT` is the default and is
        /// accepted as the explicit spelling of it.
        cascade: bool,
    },
    /// `CREATE USER MAPPING FOR <user> SERVER <server> OPTIONS (…)`
    CreateUserMapping {
        user: String,
        server: String,
        options: OptionList,
    },
    /// `ALTER USER MAPPING FOR <user> SERVER <server> OPTIONS (…)`
    AlterUserMapping {
        user: String,
        server: String,
        options: OptionList,
    },
    /// `DROP USER MAPPING [IF EXISTS] FOR <user> SERVER <server>`
    DropUserMapping {
        user: String,
        server: String,
        if_exists: bool,
        /// `CASCADE` was written: dependent objects are dropped too rather than
        /// the drop being refused with 2BP01. `RESTRICT` is the default and is
        /// accepted as the explicit spelling of it.
        cascade: bool,
    },
    /// `CREATE FOREIGN TABLE <name> (<col> <type>, …) SERVER <server> OPTIONS (…)`
    CreateForeignTable {
        name: RelationRef,
        columns: Vec<ColumnDef>,
        server: String,
        options: OptionList,
    },
    /// `DROP FOREIGN TABLE [IF EXISTS] <name>`
    DropForeignTable {
        name: RelationRef,
        if_exists: bool,
        /// `CASCADE` was written: dependent objects are dropped too rather than
        /// the drop being refused with 2BP01. `RESTRICT` is the default and is
        /// accepted as the explicit spelling of it.
        cascade: bool,
    },
    /// `IMPORT FOREIGN SCHEMA <remote_schema> [LIMIT TO | EXCEPT (<tables>)] FROM SERVER <server> [INTO <local_schema>]`
    ImportForeignSchema {
        remote_schema: String,
        selector: ImportSelector,
        server: String,
        into_schema: String,
    },
    /// `LISTEN <channel>`: subscribe the session to an asynchronous notification
    /// channel. The channel is an identifier (unquoted spellings fold to
    /// lowercase; quoted spellings keep their case).
    Listen {
        channel: String,
    },
    /// `NOTIFY <channel> [, '<payload>']`: queue a notification for delivery at
    /// commit. `payload` is `None` for the bare form (`PostgreSQL` delivers an
    /// empty payload).
    Notify {
        channel: String,
        payload: Option<String>,
    },
    /// `UNLISTEN { <channel> | * }`.
    Unlisten {
        target: UnlistenTarget,
    },
    /// S1: `SAVEPOINT <name>`. Opens a named sub-transaction level.
    Savepoint {
        name: String,
    },
    /// S1: `ROLLBACK { TO | TO SAVEPOINT } <name>`.
    RollbackToSavepoint {
        name: String,
    },
    /// S1: `RELEASE [SAVEPOINT] <name>`.
    ReleaseSavepoint {
        name: String,
    },
    /// S2: `DECLARE <name> [BINARY] [INSENSITIVE] [[NO] SCROLL] CURSOR
    /// [{WITH|WITHOUT} HOLD] FOR <query>`.
    DeclareCursor {
        name: String,
        binary: bool,
        /// `Some(true)` for `SCROLL`, `Some(false)` for `NO SCROLL`, `None` when
        /// neither was written (`PostgreSQL`'s plan-dependent default).
        scroll: Option<bool>,
        hold: bool,
        query: Box<QueryExpr>,
    },
    /// S2: `FETCH`/`MOVE` over an open cursor. `MOVE` is the row-discarding form.
    FetchCursor {
        cursor: String,
        direction: FetchDirection,
        /// `true` for `MOVE` (report the count, discard the rows).
        move_only: bool,
    },
    /// S2: `CLOSE { <name> | ALL }`.
    CloseCursor {
        target: CursorTarget,
    },
    /// S2: `PREPARE <name> [(<type>, …)] AS <statement>`.
    PrepareStatement {
        name: String,
        param_types: Vec<ColumnType>,
        /// The whole query string the `PREPARE` arrived in, verbatim.
        /// `PostgreSQL` stores the parse state's source text, not the
        /// statement's own slice of it. So a `PREPARE` sent alongside other
        /// statements reports all of them, and a trailing semicolon or trailing
        /// whitespace survives into `pg_prepared_statements.statement`.
        source: String,
        statement: Box<Statement>,
    },
    /// S2: `EXECUTE <name> [(<expr>, …)]`.
    ExecuteStatement {
        name: String,
        args: Vec<Expr>,
    },
    /// S2: `DEALLOCATE [PREPARE] { <name> | ALL }`.
    Deallocate {
        target: CursorTarget,
    },
    /// S3: `LOCK [TABLE] <name> [, …] [IN <mode> MODE] [NOWAIT]`.
    LockTable {
        tables: Vec<RelationRef>,
        mode: TableLockMode,
        nowait: bool,
    },
    /// S6: `EXPLAIN [ ( option [, …] ) | ANALYZE | VERBOSE ] <statement>`.
    Explain {
        options: ExplainOptions,
        statement: Box<Statement>,
    },
    /// F-1: `DISCARD { ALL | PLANS | SEQUENCES | TEMPORARY | TEMP }`.
    Discard {
        target: DiscardTarget,
    },
    /// P2: `CREATE [OR REPLACE] { FUNCTION | PROCEDURE } name (args) …`.
    CreateRoutine(Box<CreateRoutineStmt>),
    /// P2: `DROP { FUNCTION | PROCEDURE | ROUTINE } [IF EXISTS] sig [, …]
    /// [CASCADE | RESTRICT]`.
    DropRoutine {
        object: RoutineObject,
        if_exists: bool,
        routines: Vec<RoutineSignature>,
        cascade: bool,
    },
    /// P2: `ALTER { FUNCTION | PROCEDURE | ROUTINE } sig <action>`.
    AlterRoutine {
        object: RoutineObject,
        routine: RoutineSignature,
        action: AlterRoutineAction,
    },
    /// P2: `CALL name ( [arg, …] )`.
    Call {
        name: String,
        args: Vec<Expr>,
    },
    /// P2: `DO [LANGUAGE lang] <body> [LANGUAGE lang]`. The language defaults
    /// to `plpgsql`, exactly as `PostgreSQL` does.
    DoBlock {
        language: String,
        body: String,
    },
    /// `CREATE TYPE name AS { (field type, …) | ENUM (label, …) }`.
    CreateType {
        name: RelationRef,
        definition: CreateTypeDefinition,
    },
    /// `ALTER TYPE name <action>`.
    AlterType {
        name: RelationRef,
        action: AlterTypeAction,
    },
    /// `DROP TYPE [IF EXISTS] name [, …] [CASCADE | RESTRICT]`.
    DropType {
        names: Vec<RelationRef>,
        if_exists: bool,
        cascade: bool,
    },
    /// `CREATE DOMAIN name [AS] base [constraint …]`.
    CreateDomain {
        name: RelationRef,
        base: ColumnType,
        constraints: Vec<DomainConstraint>,
    },
    /// `ALTER DOMAIN name <action>`.
    AlterDomain {
        name: RelationRef,
        action: AlterDomainAction,
    },
    /// `DROP DOMAIN [IF EXISTS] name [, …] [CASCADE | RESTRICT]`.
    DropDomain {
        names: Vec<RelationRef>,
        if_exists: bool,
        cascade: bool,
    },
    /// P5/D6/D8: utility statements whose whole payload is their identity.
    Utility(UtilityStatement),
}

/// What a `CREATE TYPE` creates.
#[derive(Debug, Clone, PartialEq)]
pub enum CreateTypeDefinition {
    /// `AS (field type, …)`: a composite type. The list may be empty
    /// (`CREATE TYPE t AS ()` is legal and creates a zero-attribute type).
    Composite(Vec<CompositeFieldDef>),
    /// `AS ENUM ('a', 'b', …)`: the labels in declaration order, which is the
    /// order `<` uses. The list may be empty.
    Enum(Vec<String>),
    /// `AS RANGE (SUBTYPE = …, …)`.
    Range {
        subtype: ColumnType,
        collation: Option<String>,
        multirange_type_name: Option<RelationRef>,
    },
    /// A bare `CREATE TYPE name` — a shell type.
    Shell,
}

/// One field of a `CREATE TYPE … AS (…)` composite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeFieldDef {
    pub name: String,
    pub ty: ColumnType,
    /// `COLLATE "name"`, accepted and ignored. Every collation the engine has
    /// orders text by byte value.
    pub collation: Option<String>,
}

/// An `ALTER TYPE` action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTypeAction {
    /// `ADD ATTRIBUTE name type [COLLATE collation]`.
    AddAttribute(CompositeFieldDef),
    /// `ADD VALUE [IF NOT EXISTS] 'label' [{BEFORE | AFTER} 'existing']`.
    AddValue {
        label: String,
        if_not_exists: bool,
        position: Option<EnumValuePosition>,
    },
    /// `RENAME VALUE 'from' TO 'to'`.
    RenameValue { from: String, to: String },
    /// `RENAME TO new_name`.
    RenameTo(String),
    /// `OWNER TO role`: accepted and ignored; the engine has one type owner.
    OwnerTo(String),
}

/// Where `ALTER TYPE … ADD VALUE` puts the new label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnumValuePosition {
    Before(String),
    After(String),
}

/// One constraint clause of a `CREATE DOMAIN`.
#[derive(Debug, Clone, PartialEq)]
pub enum DomainConstraint {
    /// `DEFAULT <expr>`: the source text, which the executor evaluates.
    Default(String),
    NotNull,
    Null,
    /// `[CONSTRAINT name] CHECK (VALUE …)`.
    Check {
        name: Option<String>,
        /// The predicate's source text, with `VALUE` naming the tested value.
        text: String,
    },
}

/// An `ALTER DOMAIN` action.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterDomainAction {
    /// `SET DEFAULT <expr>`.
    SetDefault(String),
    /// `DROP DEFAULT`.
    DropDefault,
    /// `SET NOT NULL` / `DROP NOT NULL`.
    SetNotNull(bool),
    /// `ADD [CONSTRAINT name] CHECK (…) [NOT VALID]`.
    AddConstraint {
        name: Option<String>,
        text: String,
        not_valid: bool,
    },
    /// `DROP CONSTRAINT [IF EXISTS] name [CASCADE | RESTRICT]`.
    DropConstraint { name: String, if_exists: bool },
    /// `VALIDATE CONSTRAINT name`.
    ValidateConstraint(String),
    /// `RENAME CONSTRAINT from TO to`.
    RenameConstraint { from: String, to: String },
    /// `RENAME TO new_name`.
    RenameTo(String),
    /// `OWNER TO role`: accepted and ignored.
    OwnerTo(String),
}

/// A `PostgreSQL` utility command accepted as a documented mapping or refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UtilityStatement {
    /// `ANALYZE [ ( option … ) ] [VERBOSE] [table [(cols)] [, …]]`.
    Analyze,
    /// `CLUSTER [VERBOSE] [table [USING index]]`.
    Cluster,
    /// `REINDEX [ ( option … ) ] { INDEX | TABLE | SCHEMA | DATABASE | SYSTEM } [CONCURRENTLY] [name]`.
    Reindex,
    /// `CHECKPOINT`.
    Checkpoint,
    /// `CREATE TABLESPACE name [OWNER role] LOCATION 'path' [WITH (...)]`.
    CreateTablespace {
        name: String,
        owner: Option<String>,
        location: String,
        options: OptionList,
    },
    /// `DROP TABLESPACE [IF EXISTS] name`.
    DropTablespace { name: String, if_exists: bool },
    /// `ALTER TABLESPACE` catalog metadata changes.
    AlterTablespace {
        name: String,
        action: TablespaceAlterAction,
    },
    CreateOperatorFamily {
        name: RelationRef,
        method: String,
    },
    CreateOperatorClass {
        name: RelationRef,
        default: bool,
        input_type: ColumnType,
        method: String,
        family: Option<RelationRef>,
        key_type: Option<ColumnType>,
    },
    AlterOperatorObject {
        kind: OperatorObjectKind,
        name: RelationRef,
        method: String,
        action: OperatorObjectAlterAction,
    },
    DropOperatorObject {
        kind: OperatorObjectKind,
        name: RelationRef,
        method: String,
        if_exists: bool,
        cascade: bool,
    },
    /// `ALTER SYSTEM SET <name> = <value>` / `ALTER SYSTEM RESET { <name> | ALL }`.
    /// `name` is `None` for `RESET ALL`.
    AlterSystem { name: Option<String> },
    /// `SET CONSTRAINTS { ALL | name [, …] } { DEFERRED | IMMEDIATE }`.
    SetConstraints {
        /// The named constraints in written order, or `None` for the `ALL`
        /// spelling.
        names: Option<Vec<String>>,
        /// `DEFERRED` was written; `false` is the `IMMEDIATE` spelling, which
        /// drains the pending checks at once.
        deferred: bool,
    },
    /// `SET [SESSION | LOCAL] SESSION AUTHORIZATION { <role> | DEFAULT }`;
    /// `None` is the `DEFAULT` spelling (and `RESET SESSION AUTHORIZATION`).
    SetSessionAuthorization {
        role: Option<String>,
        /// `true` for the `RESET SESSION AUTHORIZATION` spelling, which
        /// `PostgreSQL` tags `RESET` where the `SET … DEFAULT` spelling tags
        /// `SET`.
        reset: bool,
    },
    /// SQL-managed text-search configurations and dictionaries. Parser and
    /// template objects remain explicit C-bound non-goals.
    TextSearch(TextSearchDdl),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TablespaceAlterAction {
    Set(OptionList),
    Reset(Vec<String>),
    RenameTo(String),
    OwnerTo(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorObjectKind {
    Class,
    Family,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorObjectAlterAction {
    RenameTo(String),
    OwnerTo(String),
    SetSchema(String),
    AddMembers(Vec<OperatorFamilyMember>),
    DropMembers(Vec<OperatorFamilyMemberKey>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorFamilyMember {
    Operator {
        number: u16,
        operator: String,
        left_type: ColumnType,
        right_type: ColumnType,
        order_family: Option<RelationRef>,
    },
    Function {
        number: u16,
        left_type: Option<ColumnType>,
        right_type: Option<ColumnType>,
        function: RelationRef,
        argument_types: Vec<OperatorFamilyFunctionType>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorFamilyFunctionType {
    Builtin(ColumnType),
    Internal,
}

impl OperatorFamilyFunctionType {
    #[must_use]
    pub fn oid(self) -> u32 {
        match self {
            Self::Builtin(ty) => ty.oid(),
            Self::Internal => 2281,
        }
    }

    #[must_use]
    pub const fn column(self) -> Option<ColumnType> {
        match self {
            Self::Builtin(ty) => Some(ty),
            Self::Internal => None,
        }
    }

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Builtin(ty) => ty.name(),
            Self::Internal => "internal",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorFamilyMemberKey {
    Operator {
        number: u16,
        left_type: ColumnType,
        right_type: ColumnType,
    },
    Function {
        number: u16,
        left_type: ColumnType,
        right_type: ColumnType,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextSearchObjectKind {
    Configuration,
    Dictionary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextSearchDdl {
    Create {
        kind: TextSearchObjectKind,
        name: String,
        /// `COPY` for a configuration or `TEMPLATE` for a dictionary.
        base: String,
    },
    Alter {
        kind: TextSearchObjectKind,
        name: String,
        /// Present for `RENAME TO`; other `PostgreSQL` mapping/option alterations
        /// update the existing object in place.
        rename_to: Option<String>,
    },
    Drop {
        kind: TextSearchObjectKind,
        name: String,
        if_exists: bool,
    },
}

/// `DISCARD` targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardTarget {
    All,
    Plans,
    Sequences,
    Temporary,
}

/// A `CLOSE`/`DEALLOCATE` target: one name or the whole session's set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorTarget {
    Name(String),
    All,
}

/// A `FETCH`/`MOVE` direction clause, normalized to `PostgreSQL`'s
/// (direction, count) pair. `Count::All` is the `ALL` spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchDirection {
    /// `NEXT`, `PRIOR`, `FORWARD n`, `BACKWARD n`, a bare `n`, `ALL`,
    /// `FORWARD ALL`, `BACKWARD ALL`. A negative count reverses the direction,
    /// exactly as `PostgreSQL` normalizes it.
    Relative(FetchCount),
    /// `ABSOLUTE n`, `FIRST` (`ABSOLUTE 1`), `LAST` (`ABSOLUTE -1`).
    Absolute(i64),
    /// `RELATIVE n`: one row, `n` positions from the current one.
    RelativeOne(i64),
}

/// The count in a relative `FETCH`/`MOVE` direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchCount {
    /// A signed row count; negative means backward.
    Rows(i64),
    /// `ALL`: every remaining row forward.
    AllForward,
    /// `BACKWARD ALL`: every remaining row backward.
    AllBackward,
}

/// The eight `PostgreSQL` table-level lock modes, weakest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TableLockMode {
    AccessShare,
    RowShare,
    RowExclusive,
    ShareUpdateExclusive,
    Share,
    ShareRowExclusive,
    Exclusive,
    AccessExclusive,
}

impl TableLockMode {
    /// The mode's canonical `PostgreSQL` spelling (as `pg_locks.mode` reports it).
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::AccessShare => "AccessShareLock",
            Self::RowShare => "RowShareLock",
            Self::RowExclusive => "RowExclusiveLock",
            Self::ShareUpdateExclusive => "ShareUpdateExclusiveLock",
            Self::Share => "ShareLock",
            Self::ShareRowExclusive => "ShareRowExclusiveLock",
            Self::Exclusive => "ExclusiveLock",
            Self::AccessExclusive => "AccessExclusiveLock",
        }
    }

    /// `PostgreSQL`'s `LockConflicts[]` matrix: does holding `other` block
    /// acquiring `self`? The matrix is symmetric.
    #[must_use]
    pub const fn conflicts_with(self, other: Self) -> bool {
        // One row per mode in `rank` order; bit `0b1000_0000 >> column` is set
        // when the row's mode conflicts with the column's.
        const CONFLICTS: [u8; 8] = [
            0b0000_0001, // AccessShare
            0b0000_0011, // RowShare
            0b0000_1111, // RowExclusive
            0b0001_1111, // ShareUpdateExclusive
            0b0011_0111, // Share
            0b0011_1111, // ShareRowExclusive
            0b0111_1111, // Exclusive
            0b1111_1111, // AccessExclusive
        ];
        CONFLICTS[self.rank()] & (0b1000_0000_u8 >> other.rank()) != 0
    }

    /// The mode's position in `PostgreSQL`'s weakest-to-strongest order.
    const fn rank(self) -> usize {
        match self {
            Self::AccessShare => 0,
            Self::RowShare => 1,
            Self::RowExclusive => 2,
            Self::ShareUpdateExclusive => 3,
            Self::Share => 4,
            Self::ShareRowExclusive => 5,
            Self::Exclusive => 6,
            Self::AccessExclusive => 7,
        }
    }
}

/// The parsed `EXPLAIN` option list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainOptions {
    pub analyze: bool,
    pub verbose: bool,
    /// `COSTS` defaults to on; `EXPLAIN (COSTS OFF)` turns the estimates off.
    pub costs: bool,
    pub format: ExplainFormat,
}

impl Default for ExplainOptions {
    /// The stock defaults: no ANALYZE/VERBOSE, costs on, text format.
    fn default() -> Self {
        Self {
            analyze: false,
            verbose: false,
            costs: true,
            format: ExplainFormat::Text,
        }
    }
}

/// `EXPLAIN (FORMAT …)` output formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExplainFormat {
    #[default]
    Text,
    Json,
    Yaml,
    Xml,
}

/// The target of an `UNLISTEN` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnlistenTarget {
    /// `UNLISTEN <channel>`: drop one subscription.
    Channel(String),
    /// `UNLISTEN *`: drop every subscription held by the session.
    All,
}

/// Where an `INSERT`'s rows come from.
#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    /// `VALUES (…), (…)`: a row may contain `DEFAULT` in any position.
    Values(Vec<Vec<Expr>>),
    /// `INSERT … <query>`: a `SELECT`, a set operation, a `TABLE t`, or a
    /// `VALUES` carrying its own `ORDER BY`/`LIMIT`.
    Query(Box<QueryExpr>),
    /// `DEFAULT VALUES`: exactly one row, every column defaulted.
    DefaultValues,
}

/// A `RETURNING` clause. `PostgreSQL` 18 lets the clause name the row images it
/// projects: `RETURNING WITH (OLD AS o, NEW AS n) …`. When an alias is absent
/// the default spellings `old`/`new` apply, unless the statement already has a
/// relation in scope under that name.
#[derive(Debug, Clone, PartialEq)]
pub struct Returning {
    pub old_alias: Option<String>,
    pub new_alias: Option<String>,
    pub items: Vec<SelectItem>,
}

/// One `SET` entry of an `UPDATE` or a `MERGE` update action.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    /// One name for `SET a = e`; two or more for the parenthesised
    /// `SET (a, b) = …` form.
    pub targets: Vec<String>,
    /// The subscripts of a subscripted target (`SET j['a'][0] = e`,
    /// `SET a[1:2] = ARRAY[…]`); empty for an ordinary column assignment. Only
    /// the single-target form can carry them. The assignment then *updates* the
    /// column and does not replace it. So, unlike a plain assignment, two
    /// subscripted entries may name the same column.
    pub subscripts: Vec<ArraySubscript>,
    pub value: AssignmentValue,
}

/// The right-hand side of an [`Assignment`].
#[derive(Debug, Clone, PartialEq)]
pub enum AssignmentValue {
    /// `SET a = e`: one expression for one target.
    Expr(Expr),
    /// `SET (a, b) = ROW(e1, e2)` or `SET (a, b) = (e1, e2)`: one expression
    /// per target.
    Row(Vec<Expr>),
    /// `SET (a, b) = (SELECT …)`: a single-row subquery whose column count
    /// must equal the target count. Zero rows assign NULL to every target.
    Subquery(Box<QueryExpr>),
}

/// The `USING` relation of a `MERGE`.
#[derive(Debug, Clone, PartialEq)]
pub enum MergeSource {
    Table {
        name: RelationRef,
        alias: Option<String>,
    },
    Query {
        query: Box<QueryExpr>,
        alias: String,
        columns: Option<Vec<String>>,
    },
}

/// One `WHEN …` clause of a `MERGE`.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeWhen {
    pub kind: MergeMatchKind,
    /// The optional `AND <condition>` that further restricts the clause.
    pub condition: Option<Expr>,
    pub action: MergeAction,
}

/// Which side of the join a `MERGE` `WHEN` clause fires on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeMatchKind {
    /// `WHEN MATCHED`: a source row that joined a target row.
    Matched,
    /// `WHEN NOT MATCHED [BY TARGET]`: a source row with no target match.
    NotMatchedByTarget,
    /// `WHEN NOT MATCHED BY SOURCE` (`PostgreSQL` 17): a target row that no
    /// source row joined.
    NotMatchedBySource,
}

/// What a `MERGE` `WHEN` clause does with the row it fires on.
#[derive(Debug, Clone, PartialEq)]
pub enum MergeAction {
    Update(Vec<Assignment>),
    Delete,
    DoNothing,
    /// `INSERT [(cols)] { VALUES (…) | DEFAULT VALUES }`; `values` is `None` for
    /// the `DEFAULT VALUES` spelling.
    Insert {
        columns: Option<Vec<String>>,
        values: Option<Vec<Expr>>,
    },
}

/// The parsed `ON CONFLICT` clause of an `INSERT`.
#[derive(Debug, Clone, PartialEq)]
pub struct OnConflict {
    pub target: OnConflictTarget,
    pub action: OnConflictAction,
}

/// How the conflicting unique index is chosen ("arbiter inference").
#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictTarget {
    /// No inference specification: legal only with `DO NOTHING`, where every
    /// unique index arbitrates.
    None,
    /// `( col, … ) [WHERE <index_predicate>]`. Entries are plain column names;
    /// expression/collation/opclass inference is not accepted.
    Columns {
        columns: Vec<String>,
        /// The `WHERE` inside the inference specification (partial-index
        /// predicate). Retained for parse fidelity; the executor refuses it.
        index_predicate: Option<Expr>,
    },
    /// `ON CONSTRAINT <name>`: arbitrate by constraint name.
    OnConstraint(String),
}

/// What to do with a row that conflicts.
#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictAction {
    /// `DO NOTHING`: skip the row.
    DoNothing,
    /// `DO UPDATE SET a = e, … [WHERE <filter>]`. Assignment right-hand sides and
    /// the filter may reference the target table and the pseudo-table `excluded`.
    DoUpdate {
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
}

/// Stable, typed metadata for commands that parse normally and then fail clear.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusalCommand {
    AlterDatabase,
    CreateDatabase,
    DropDatabase,
    AlterExtension,
    DropExtension,
    PrepareTransaction,
    CommitPrepared,
    RollbackPrepared,
    AlterServer,
    AlterUserMapping,
    /// P5: extended planner statistics objects. Gres has no planner statistics,
    /// so the parser recognizes the whole family and refuses it. It does not
    /// persist metadata that nothing reads.
    CreateStatistics,
    AlterStatistics,
    DropStatistics,
    NonGoal(NonGoalCommand),
}

impl RefusalCommand {
    #[must_use]
    pub const fn command_name(self) -> &'static str {
        match self {
            Self::AlterDatabase => "ALTER DATABASE",
            Self::CreateDatabase => "CREATE DATABASE",
            Self::DropDatabase => "DROP DATABASE",
            Self::AlterExtension => "ALTER EXTENSION",
            Self::DropExtension => "DROP EXTENSION",
            Self::PrepareTransaction => "PREPARE TRANSACTION",
            Self::CommitPrepared => "COMMIT PREPARED",
            Self::RollbackPrepared => "ROLLBACK PREPARED",
            Self::AlterServer => "ALTER SERVER",
            Self::AlterUserMapping => "ALTER USER MAPPING",
            Self::CreateStatistics => "CREATE STATISTICS",
            Self::AlterStatistics => "ALTER STATISTICS",
            Self::DropStatistics => "DROP STATISTICS",
            Self::NonGoal(command) => command.command_name(),
        }
    }

    #[must_use]
    pub const fn sqlstate(self) -> &'static str {
        match self {
            Self::PrepareTransaction | Self::CommitPrepared | Self::RollbackPrepared => "55000",
            _ => "0A000",
        }
    }

    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::AlterDatabase | Self::CreateDatabase | Self::DropDatabase => {
                "database lifecycle is managed by tenant provisioning"
            }
            Self::AlterExtension | Self::DropExtension => {
                "extension lifecycle is not supported; use built-in compatibility shims"
            }
            Self::PrepareTransaction | Self::CommitPrepared | Self::RollbackPrepared => {
                "SQL-level prepared transactions are not available"
            }
            Self::AlterServer => "ALTER SERVER is not supported",
            Self::AlterUserMapping => "ALTER USER MAPPING is not supported",
            Self::CreateStatistics | Self::AlterStatistics | Self::DropStatistics => {
                "extended planner statistics objects are not supported"
            }
            Self::NonGoal(command) => command.message(),
        }
    }
}

impl Statement {
    /// Return the centralized refusal contract for richer refusal AST variants.
    #[must_use]
    pub const fn compatibility_refusal(&self) -> Option<RefusalCommand> {
        match self {
            Self::CompatibilityRefusal(command) => Some(*command),
            Self::AlterServer { .. } => Some(RefusalCommand::AlterServer),
            Self::AlterUserMapping { .. } => Some(RefusalCommand::AlterUserMapping),
            _ => None,
        }
    }
}

/// Architectural non-goal commands tracked by the `PostgreSQL` compatibility matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonGoalCommand {
    AlterConversion,
    AlterLanguage,
    AlterLargeObject,
    AlterOperator,
    AlterOperatorClass,
    AlterOperatorFamily,
    AlterPublication,
    AlterRule,
    AlterSubscription,
    AlterTextSearchParser,
    AlterTextSearchTemplate,
    CreateAccessMethod,
    CreateConversion,
    CreateLanguage,
    CreateOperator,
    CreateOperatorFamily,
    CreatePublication,
    CreateRule,
    CreateSubscription,
    CreateTextSearchParser,
    CreateTextSearchTemplate,
    CreateTransform,
    DropAccessMethod,
    DropConversion,
    DropLanguage,
    DropOperator,
    DropOperatorClass,
    DropOperatorFamily,
    DropPublication,
    DropRule,
    DropSubscription,
    DropTextSearchParser,
    DropTextSearchTemplate,
    DropTransform,
    Load,
    SecurityLabel,
}

impl NonGoalCommand {
    #[must_use]
    pub const fn command_name(self) -> &'static str {
        match self {
            Self::AlterConversion => "ALTER CONVERSION",
            Self::AlterLanguage => "ALTER LANGUAGE",
            Self::AlterLargeObject => "ALTER LARGE OBJECT",
            Self::AlterOperator => "ALTER OPERATOR",
            Self::AlterOperatorClass => "ALTER OPERATOR CLASS",
            Self::AlterOperatorFamily => "ALTER OPERATOR FAMILY",
            Self::AlterPublication => "ALTER PUBLICATION",
            Self::AlterRule => "ALTER RULE",
            Self::AlterSubscription => "ALTER SUBSCRIPTION",
            Self::AlterTextSearchParser => "ALTER TEXT SEARCH PARSER",
            Self::AlterTextSearchTemplate => "ALTER TEXT SEARCH TEMPLATE",
            Self::CreateAccessMethod => "CREATE ACCESS METHOD",
            Self::CreateConversion => "CREATE CONVERSION",
            Self::CreateLanguage => "CREATE LANGUAGE",
            Self::CreateOperator => "CREATE OPERATOR",
            Self::CreateOperatorFamily => "CREATE OPERATOR FAMILY",
            Self::CreatePublication => "CREATE PUBLICATION",
            Self::CreateRule => "CREATE RULE",
            Self::CreateSubscription => "CREATE SUBSCRIPTION",
            Self::CreateTextSearchParser => "CREATE TEXT SEARCH PARSER",
            Self::CreateTextSearchTemplate => "CREATE TEXT SEARCH TEMPLATE",
            Self::CreateTransform => "CREATE TRANSFORM",
            Self::DropAccessMethod => "DROP ACCESS METHOD",
            Self::DropConversion => "DROP CONVERSION",
            Self::DropLanguage => "DROP LANGUAGE",
            Self::DropOperator => "DROP OPERATOR",
            Self::DropOperatorClass => "DROP OPERATOR CLASS",
            Self::DropOperatorFamily => "DROP OPERATOR FAMILY",
            Self::DropPublication => "DROP PUBLICATION",
            Self::DropRule => "DROP RULE",
            Self::DropSubscription => "DROP SUBSCRIPTION",
            Self::DropTextSearchParser => "DROP TEXT SEARCH PARSER",
            Self::DropTextSearchTemplate => "DROP TEXT SEARCH TEMPLATE",
            Self::DropTransform => "DROP TRANSFORM",
            Self::Load => "LOAD",
            Self::SecurityLabel => "SECURITY LABEL",
        }
    }

    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::AlterConversion | Self::CreateConversion | Self::DropConversion => {
                "conversion objects are unavailable on the UTF-8-only server"
            }
            Self::AlterLanguage | Self::CreateLanguage | Self::DropLanguage => {
                "only built-in procedural languages are available"
            }
            Self::AlterLargeObject => "large objects are unavailable; use bytea storage",
            Self::AlterOperator
            | Self::AlterOperatorClass
            | Self::AlterOperatorFamily
            | Self::CreateOperator
            | Self::CreateOperatorFamily
            | Self::DropOperator
            | Self::DropOperatorClass
            | Self::DropOperatorFamily => "C-bound operator objects are not supported",
            Self::AlterPublication
            | Self::AlterSubscription
            | Self::CreatePublication
            | Self::CreateSubscription
            | Self::DropPublication
            | Self::DropSubscription => "physical replication SQL is not supported",
            Self::AlterRule | Self::CreateRule | Self::DropRule => {
                "the legacy rewrite rule system is not supported"
            }
            Self::AlterTextSearchParser
            | Self::AlterTextSearchTemplate
            | Self::CreateTextSearchParser
            | Self::CreateTextSearchTemplate
            | Self::DropTextSearchParser
            | Self::DropTextSearchTemplate => "C-bound text search objects are not supported",
            Self::CreateAccessMethod | Self::DropAccessMethod => {
                "C-bound access methods are not supported"
            }
            Self::CreateTransform | Self::DropTransform => {
                "C-bound transform objects are not supported"
            }
            Self::Load => "C-bound code loading is not supported",
            Self::SecurityLabel => "C-bound security labels are not supported",
        }
    }
}

/// One bounded `PostgreSQL` 18 syntax representative for an architectural refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonGoalRefusalSpec {
    pub command: RefusalCommand,
    pub identity: crate::command::CommandIdentity,
    pub representative_sql: &'static str,
}

macro_rules! non_goal_specs {
    ($(($variant:ident, $sql:literal)),+ $(,)?) => {
        pub const NON_GOAL_REFUSALS: &[NonGoalRefusalSpec] = &[
            $(NonGoalRefusalSpec {
                command: RefusalCommand::NonGoal(NonGoalCommand::$variant),
                identity: crate::command::CommandIdentity::$variant,
                representative_sql: $sql,
            }),+
        ];
    };
}

non_goal_specs!(
    (AlterConversion, "ALTER CONVERSION conv RENAME TO conv2"),
    (AlterLanguage, "ALTER LANGUAGE lang RENAME TO lang2"),
    (AlterLargeObject, "ALTER LARGE OBJECT 1 OWNER TO postgres"),
    (
        AlterOperator,
        "ALTER OPERATOR +(integer, integer) OWNER TO postgres"
    ),
    (AlterPublication, "ALTER PUBLICATION pub ADD TABLE t"),
    (AlterRule, "ALTER RULE r ON t RENAME TO r2"),
    (AlterSubscription, "ALTER SUBSCRIPTION sub DISABLE"),
    (
        AlterTextSearchParser,
        "ALTER TEXT SEARCH PARSER p RENAME TO p2"
    ),
    (
        AlterTextSearchTemplate,
        "ALTER TEXT SEARCH TEMPLATE t RENAME TO t2"
    ),
    (
        CreateAccessMethod,
        "CREATE ACCESS METHOD am TYPE INDEX HANDLER handler_fn"
    ),
    (
        CreateConversion,
        "CREATE CONVERSION conv FOR 'UTF8' TO 'LATIN1' FROM func"
    ),
    (CreateLanguage, "CREATE LANGUAGE lang"),
    (
        CreateOperator,
        "CREATE OPERATOR === (FUNCTION = int4eq, LEFTARG = integer, RIGHTARG = integer)"
    ),
    (CreatePublication, "CREATE PUBLICATION pub"),
    (
        CreateRule,
        "CREATE RULE r AS ON SELECT TO t DO INSTEAD NOTHING"
    ),
    (
        CreateSubscription,
        "CREATE SUBSCRIPTION sub CONNECTION 'host=x' PUBLICATION pub"
    ),
    (
        CreateTextSearchParser,
        "CREATE TEXT SEARCH PARSER p (START = f, GETTOKEN = f, END = f, LEXTYPES = f)"
    ),
    (
        CreateTextSearchTemplate,
        "CREATE TEXT SEARCH TEMPLATE t (LEXIZE = f)"
    ),
    (
        CreateTransform,
        "CREATE TRANSFORM FOR integer LANGUAGE sql (FROM SQL WITH FUNCTION f(integer), TO SQL WITH FUNCTION f(integer))"
    ),
    (DropAccessMethod, "DROP ACCESS METHOD am"),
    (DropConversion, "DROP CONVERSION conv"),
    (DropLanguage, "DROP LANGUAGE lang"),
    (DropOperator, "DROP OPERATOR +(integer, integer)"),
    (DropPublication, "DROP PUBLICATION pub"),
    (DropRule, "DROP RULE r ON t"),
    (DropSubscription, "DROP SUBSCRIPTION sub"),
    (DropTextSearchParser, "DROP TEXT SEARCH PARSER p"),
    (DropTextSearchTemplate, "DROP TEXT SEARCH TEMPLATE t"),
    (DropTransform, "DROP TRANSFORM FOR integer LANGUAGE sql"),
    (Load, "LOAD 'lib'"),
    (SecurityLabel, "SECURITY LABEL ON TABLE t IS 'label'"),
);

/// One `ALTER TABLE` subcommand.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction {
    AddColumn {
        if_not_exists: bool,
        column: ColumnDef,
    },
    DropColumn {
        column: String,
        if_exists: bool,
        cascade: bool,
    },
    SetNotNull(String),
    DropNotNull(String),
    SetDefault {
        column: String,
        expr: Expr,
    },
    DropDefault(String),
    /// `ALTER [COLUMN] c TYPE t [USING expr]`.
    SetType {
        column: String,
        ty: ColumnType,
        using: Option<Expr>,
    },
    AddConstraint(TableConstraint),
    DropConstraint {
        name: String,
        if_exists: bool,
        cascade: bool,
    },
    RenameTable {
        new_name: String,
    },
    RenameColumn {
        column: String,
        new_name: String,
    },
    RenameConstraint {
        name: String,
        new_name: String,
    },
    ValidateConstraint(String),
    /// `SET (param = value, …)`: heap storage parameters.
    SetStorageParameters(Vec<(String, Option<String>)>),
    /// `RESET (param, …)`.
    ResetStorageParameters(Vec<String>),
    SetTablespace(String),
    OwnerTo(String),
    SetTriggerMode {
        selector: TriggerSelector,
        mode: TriggerEnableMode,
    },
    /// `ATTACH PARTITION <name> <bound>`.
    AttachPartition {
        partition: RelationRef,
        bound: PartitionBound,
    },
    /// `DETACH PARTITION <name> [CONCURRENTLY | FINALIZE]`.
    DetachPartition {
        partition: RelationRef,
        concurrently: bool,
        finalize: bool,
    },
    /// `SET SCHEMA name`, `SET {LOGGED|UNLOGGED}`,
    /// `CLUSTER ON`, `SET WITHOUT CLUSTER`, `{EN,DIS}ABLE TRIGGER`, … — the
    /// subcommands that parse but have no counterpart in Crabka's storage
    /// model. `label` is the `PostgreSQL` subcommand text for the refusal.
    Unsupported(String),
}

/// `PARTITION BY <strategy> ( <key>, … )` on a `CREATE TABLE`.
///
/// `strategy` is the word as written, lowercased, rather than an enum: an
/// unrecognized strategy is a parse-analysis error in `PostgreSQL` (22023
/// `unrecognized partitioning strategy "magic"`), not a syntax error, so the
/// word has to survive parsing for the executor to report it.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionBy {
    pub strategy: String,
    pub keys: Vec<PartitionKeyElem>,
}

/// One element of a `PARTITION BY` key list.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionKeyElem {
    /// The referenced column for a plain key; `None` for an expression key.
    pub column: Option<String>,
    /// The element as written, used for catalog storage and error messages.
    pub text: String,
    /// `COLLATE "…"`, when written.
    pub collation: Option<String>,
    /// The operator-class name, when written.
    pub opclass: Option<String>,
}

/// `PARTITION OF <parent> [ (<column options>) ] <bound>` on a `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionOf {
    pub parent: RelationRef,
    pub bound: PartitionBound,
    /// `(a NOT NULL, b WITH OPTIONS DEFAULT 0)`: extra constraints on columns
    /// the partition inherits from its parent. A partition declares no types of
    /// its own, so only the qualifier list is written.
    pub column_options: Vec<(String, Vec<ColumnConstraint>)>,
}

/// A partition's bound specification, as written.
///
/// The spelling is checked against the parent's strategy by the executor, not
/// the parser: `PostgreSQL` accepts every spelling syntactically and reports a
/// mismatch as 42P16 `invalid bound specification for a list partition`.
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionBound {
    /// `DEFAULT`: the catch-all partition.
    Default,
    /// `FOR VALUES IN (…)`.
    List(Vec<Expr>),
    /// `FOR VALUES FROM (…) TO (…)`.
    Range {
        from: Vec<RangeBoundValue>,
        to: Vec<RangeBoundValue>,
    },
    /// `FOR VALUES WITH (MODULUS m, REMAINDER r)`.
    Hash { modulus: i64, remainder: i64 },
}

/// One value in a range partition's `FROM`/`TO` list.
#[derive(Debug, Clone, PartialEq)]
pub enum RangeBoundValue {
    MinValue,
    MaxValue,
    Value(Expr),
}

/// `ALTER SCHEMA <name> …` subcommand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterSchemaAction {
    RenameTo(String),
    OwnerTo(String),
}

/// `ON COMMIT` disposition for a temporary table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnCommitAction {
    PreserveRows,
    DeleteRows,
    Drop,
}

/// A property a `(LIKE source …)` clause can copy from its source relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LikeOption {
    Defaults,
    Constraints,
    Indexes,
    Identity,
}

impl LikeOption {
    /// Every option `INCLUDING ALL` turns on.
    pub const ALL: &[Self] = &[
        Self::Defaults,
        Self::Constraints,
        Self::Indexes,
        Self::Identity,
    ];
}

/// One `(LIKE source [INCLUDING …|EXCLUDING …])` clause in a `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LikeClause {
    pub source: RelationRef,
    /// The properties an `INCLUDING` clause turned on, in no particular order.
    /// `EXCLUDING` removes an option, so the last mention of a property wins
    /// exactly as it does in `PostgreSQL`.
    pub including: Vec<LikeOption>,
}

impl LikeClause {
    /// True when `option` is copied from the source relation.
    #[must_use]
    pub fn includes(&self, option: LikeOption) -> bool {
        self.including.contains(&option)
    }

    /// Turn `option` on or off, keeping the list free of duplicates.
    pub fn set(&mut self, option: LikeOption, including: bool) {
        self.including.retain(|held| *held != option);
        if including {
            self.including.push(option);
        }
    }
}

/// One key of a `CREATE INDEX` key list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexKey {
    /// The referenced column for a plain key; `None` for an expression key.
    pub column: Option<String>,
    /// Source text of the key: the column name, or the expression as written.
    pub text: String,
    pub descending: bool,
    /// `NULLS FIRST` (`Some(true)`) / `NULLS LAST` (`Some(false)`); `None` when
    /// the clause is absent and `PostgreSQL`'s direction-derived default applies.
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyStmt {
    pub table: RelationRef,
    pub columns: Option<Vec<String>>,
    pub source: CopySource,
    pub format: CopyFormat,
}

pub const COPY_FROM_STDIN_SENTINEL: &str = "__copy_from_stdin";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopySource {
    Stdin,
    File(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Text,
    Csv,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResetTarget {
    Name(String),
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexPlacement {
    Local,
    Global,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardingSpec {
    Hash(HashShardingSpec),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashShardingSpec {
    pub columns: Vec<String>,
    pub buckets: u32,
    pub co_location_group: Option<String>,
}

/// A key-value option list for FDW DDL: `OPTIONS (key 'value', …)`.
pub type OptionList = Vec<(String, String)>;

/// The optional table-filter for `IMPORT FOREIGN SCHEMA`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportSelector {
    /// Import all tables (no filter clause).
    All,
    /// `LIMIT TO (table, …)`: import only the listed tables.
    LimitTo(Vec<String>),
    /// `EXCEPT (table, …)`: import all tables except the listed ones.
    Except(Vec<String>),
}

/// SP37: the right-hand side of a `SET` (or the value form of `SET TIME ZONE`).
/// `Default` is `SET ... = DEFAULT` / `SET TIME ZONE { DEFAULT | LOCAL }` (resets
/// the parameter to its built-in default); `Value` is a literal/identifier value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetValue {
    Default,
    /// The comma-separated items as written, each already joined from the
    /// space-adjacent tokens that make it up (`SET statement_timeout = 1 min`
    /// is one item). The items stay separate because a *list* parameter
    /// re-quotes each one on output. `SET search_path = "MySchema", public`
    /// has to report back as `"MySchema", public`, and an item that holds a
    /// comma has no representation at all once they are joined.
    Value(Vec<String>),
}

impl SetValue {
    /// The value as a scalar parameter takes it: the items joined back with
    /// `", "`, no re-quoting. A list parameter uses the items instead.
    #[must_use]
    pub fn plain(&self) -> String {
        match self {
            Self::Default => String::new(),
            Self::Value(items) => items.join(", "),
        }
    }
}

/// Transaction isolation levels supported by SP4.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// `READ UNCOMMITTED`, which `PostgreSQL` runs as `READ COMMITTED` while
    /// still reporting the requested spelling through `transaction_isolation`.
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl IsolationLevel {
    /// The spelling `SHOW transaction_isolation` reports.
    #[must_use]
    pub fn render(self) -> &'static str {
        match self {
            Self::ReadUncommitted => "read uncommitted",
            Self::ReadCommitted => "read committed",
            Self::RepeatableRead => "repeatable read",
            Self::Serializable => "serializable",
        }
    }

    /// Parse one of the four `transaction_isolation` spellings.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "read uncommitted" => Some(Self::ReadUncommitted),
            "read committed" => Some(Self::ReadCommitted),
            "repeatable read" => Some(Self::RepeatableRead),
            "serializable" => Some(Self::Serializable),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: ColumnType,
    pub serial: Option<SerialKind>,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerialKind {
    Serial,
    BigSerial,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SequenceOptions {
    pub start: Option<i64>,
    pub increment: Option<i64>,
    pub min: Option<i64>,
    pub max: Option<i64>,
    pub cache: Option<i64>,
    pub cycle: Option<bool>,
}

/// A `CHECK` predicate: the parsed expression plus the source text needed to
/// persist it (the catalog stores the text and re-parses it on every write).
#[derive(Debug, Clone, PartialEq)]
pub struct CheckPredicate {
    pub expr: Expr,
    /// Source text inside the parentheses, exactly as written.
    pub text: String,
}

/// What a foreign key does to the referencing rows when the referenced row is
/// deleted (`ON DELETE`) or its key columns are updated (`ON UPDATE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReferentialAction {
    /// `NO ACTION`, the default: refuse at the end of the statement, honouring
    /// a deferral.
    #[default]
    NoAction,
    /// `RESTRICT`: refuse immediately, without honouring a deferral.
    Restrict,
    /// `CASCADE`: delete the referencing rows, or carry the new key values into
    /// them.
    Cascade,
    /// `SET NULL`: set the referencing columns to NULL.
    SetNull,
    /// `SET DEFAULT`: set the referencing columns to their column DEFAULTs.
    SetDefault,
}

impl ReferentialAction {
    /// The action as `PostgreSQL` spells it in `pg_get_constraintdef` and in
    /// its "a column list with SET NULL is only supported for ON DELETE
    /// actions" refusal.
    #[must_use]
    pub fn as_sql(self) -> &'static str {
        match self {
            Self::NoAction => "NO ACTION",
            Self::Restrict => "RESTRICT",
            Self::Cascade => "CASCADE",
            Self::SetNull => "SET NULL",
            Self::SetDefault => "SET DEFAULT",
        }
    }
}

/// How a foreign key treats a partly-NULL composite key.
///
/// `MATCH PARTIAL` has no variant: `PostgreSQL` refuses it at parse analysis
/// with `0A000` "MATCH PARTIAL not yet implemented", and so does this parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MatchType {
    /// `MATCH SIMPLE` (the default, written or not): a row with any NULL
    /// referencing column satisfies the constraint.
    #[default]
    Simple,
    /// `MATCH FULL`: the referencing columns must be all NULL or all non-NULL.
    Full,
}

/// The `REFERENCES <table> [(col, …)] [MATCH …] [ON DELETE …] [ON UPDATE …]`
/// target of a foreign key, shared by the column-level `REFERENCES` spelling
/// and the table-level `FOREIGN KEY (…) REFERENCES` one.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ForeignKeyRef {
    pub table: RelationRef,
    /// The referenced columns as written, or empty when the list was omitted
    /// and the referenced table's primary key is meant.
    pub columns: Vec<String>,
    pub match_type: MatchType,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
    /// `ON DELETE SET { NULL | DEFAULT } (a, b)`: the referencing columns the
    /// action writes to, in written order. Empty means every referencing
    /// column, which is also the only possibility for `ON UPDATE`: a column
    /// list there is a `0A000` refusal.
    pub set_columns: Vec<String>,
}

/// The trailing attributes any constraint may carry: `[NOT] DEFERRABLE`,
/// `INITIALLY { DEFERRED | IMMEDIATE }`, and `NOT VALID`.
///
/// `PostgreSQL` also accepts `ENFORCED` / `NOT ENFORCED` and `NO INHERIT` here;
/// those are consumed and dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConstraintAttributes {
    /// `NOT VALID` was written. In `ALTER TABLE … ADD CONSTRAINT` this skips
    /// back-validation of the rows already stored; `PostgreSQL` ignores it in
    /// `CREATE TABLE`, where there are none. It belongs to the table-constraint
    /// grammar only, so it is always false on a column constraint.
    pub not_valid: bool,
    /// The constraint may be `SET CONSTRAINTS … DEFERRED` within a transaction.
    /// Writing `INITIALLY DEFERRED` alone implies it.
    pub deferrable: bool,
    /// `INITIALLY DEFERRED`: the constraint starts each transaction deferred.
    /// Never true without [`ConstraintAttributes::deferrable`], because `NOT
    /// DEFERRABLE INITIALLY DEFERRED` is a `42601` refusal.
    pub initially_deferred: bool,
}

/// `GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [( <sequence options> )]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentitySpec {
    /// True for `GENERATED ALWAYS`, false for `GENERATED BY DEFAULT`.
    pub always: bool,
    pub options: SequenceOptions,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnConstraint {
    /// Explicit `CONSTRAINT <name>` label, when one was written.
    pub name: Option<String>,
    pub kind: ColumnConstraintKind,
    /// The deferrability written after this one constraint. `not_valid` is
    /// always false here, because `PostgreSQL` accepts `NOT VALID` only on a
    /// table constraint.
    pub attributes: ConstraintAttributes,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraintKind {
    NotNull,
    /// An explicit `NULL` column constraint. `PostgreSQL` accepts it and it
    /// means "not NOT NULL".
    Null,
    Default(Expr),
    PrimaryKey,
    Unique {
        nulls_not_distinct: bool,
    },
    Check(CheckPredicate),
    References(ForeignKeyRef),
    Identity(IdentitySpec),
    /// `GENERATED ALWAYS AS (<expr>) STORED`.
    Generated(CheckPredicate),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableConstraint {
    /// Explicit `CONSTRAINT <name>` label, when one was written.
    pub name: Option<String>,
    pub kind: TableConstraintKind,
    /// The `[NOT] DEFERRABLE` / `INITIALLY …` / `NOT VALID` tail.
    pub attributes: ConstraintAttributes,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraintKind {
    PrimaryKey(Vec<String>),
    Unique {
        columns: Vec<String>,
        nulls_not_distinct: bool,
    },
    Check(CheckPredicate),
    ForeignKey {
        columns: Vec<String>,
        references: ForeignKeyRef,
    },
    Exclude {
        method: String,
        elements: Vec<ExclusionElement>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExclusionElement {
    pub column: String,
    pub operator: BinaryOp,
}

/// `PostgreSQL`'s four row-lock strengths, ordered weakest to strongest so a
/// query with several `FOR …` clauses folds onto the strongest one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RowLockStrength {
    ForKeyShare,
    ForShare,
    ForNoKeyUpdate,
    ForUpdate,
}

impl RowLockStrength {
    /// The clause as `PostgreSQL` spells it in its `0A000` refusal messages
    /// ("FOR UPDATE is not allowed with DISTINCT clause").
    #[must_use]
    pub fn as_sql(self) -> &'static str {
        match self {
            Self::ForKeyShare => "FOR KEY SHARE",
            Self::ForShare => "FOR SHARE",
            Self::ForNoKeyUpdate => "FOR NO KEY UPDATE",
            Self::ForUpdate => "FOR UPDATE",
        }
    }
}

/// What a locking read does when a row it wants is already locked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockWaitPolicy {
    /// Block until the conflicting transaction ends (the default).
    Wait,
    /// `NOWAIT`: fail immediately with `55P03`.
    NoWait,
    /// `SKIP LOCKED`: omit the row from the result.
    SkipLocked,
}

/// A `FOR UPDATE` / `FOR NO KEY UPDATE` / `FOR SHARE` / `FOR KEY SHARE` clause.
///
/// `PostgreSQL` accepts several such clauses on one query, each naming its own
/// relations; the parser folds them onto one strength (the strongest), the union
/// of the named relations, and the strictest wait policy. That is
/// indistinguishable from `PostgreSQL` for the single-base-table locking reads
/// the executor supports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockingClause {
    pub strength: RowLockStrength,
    /// `OF table [, …]`: empty means every relation in the FROM clause.
    pub of: Vec<String>,
    pub wait: LockWaitPolicy,
}

/// `SELECT`'s duplicate-elimination clause.
#[derive(Debug, Clone, PartialEq)]
pub enum DistinctClause {
    /// No `DISTINCT`: the `ALL` default.
    All,
    /// `SELECT DISTINCT`: dedup whole projected output rows.
    Distinct,
    /// `SELECT DISTINCT ON (expr, …)`: keep the first row of each key group in
    /// ORDER BY order. Never empty: `DISTINCT ON ()` is a syntax error.
    On(Vec<Expr>),
}

impl DistinctClause {
    /// Does this clause eliminate duplicates at all?
    #[must_use]
    pub fn dedups(&self) -> bool {
        !matches!(self, Self::All)
    }

    /// The `DISTINCT ON` key expressions, if this is the `ON` form.
    #[must_use]
    pub fn on_exprs(&self) -> Option<&[Expr]> {
        match self {
            Self::On(exprs) => Some(exprs),
            Self::All | Self::Distinct => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub projection: Vec<SelectItem>,
    /// SP33: the FROM clause, a list of join trees. Empty for a FROM-less SELECT;
    /// the comma form (`FROM a, b`) is a `Vec<TableExpr>` with len > 1 (implicit
    /// cross join).
    pub from: Vec<TableExpr>,
    pub filter: Option<Expr>,
    /// SP28: `SELECT DISTINCT` / `SELECT DISTINCT ON (…)`.
    pub distinct: DistinctClause,
    /// SP27: the flattened `GROUP BY` grouping expressions, deduplicated in order
    /// of first appearance. Empty when there is no `GROUP BY`. For a grouping-set
    /// clause this holds every expression mentioned anywhere in the clause, and
    /// [`SelectStmt::grouping`] describes the set structure over these indices.
    pub group_by: Vec<Expr>,
    /// `GROUP BY ROLLUP/CUBE/GROUPING SETS/()` structure over `group_by`, or the
    /// `GROUP BY DISTINCT` modifier. `None` for a plain `GROUP BY <expr-list>`.
    pub grouping: Option<GroupingClause>,
    /// SP27: `HAVING <predicate>` (evaluated per group).
    pub having: Option<Expr>,
    /// `WINDOW name AS (…)` definitions, in declaration order.
    pub windows: Vec<NamedWindow>,
    /// Every `f(…) OVER …` call written in this SELECT, in the order the parser
    /// met them. A [`window_placeholder`] that carries the call's index here
    /// holds each call's place in the expression tree. So an ordinary
    /// expression walk never has to know about the window calls.
    pub window_calls: Vec<WindowCall>,
    pub order_by: Vec<OrderItem>,
    /// `LIMIT <expr>` / `FETCH FIRST <expr> ROWS ONLY`. `None` covers both an
    /// absent limit and the explicit `LIMIT ALL`, which mean the same thing.
    pub limit: Option<Expr>,
    /// SP28: `OFFSET <expr>`. Skips the first n output rows (before LIMIT).
    pub offset: Option<Expr>,
    /// `FETCH … WITH TIES`: also emit rows whose ORDER BY key ties the last
    /// row the limit admits.
    pub with_ties: bool,
    pub locking: Option<LockingClause>,
}

/// The set-producing structure of a `GROUP BY` clause (PG18 `group_clause`).
///
/// Present only when the clause needs expansion into several grouping sets,
/// that is, whenever `ROLLUP`, `CUBE`, `GROUPING SETS`, the empty grouping set
/// `()`, or the `DISTINCT` modifier appears. A plain `GROUP BY a, b` leaves
/// [`SelectStmt::grouping`] `None`, and the ordinary grouped path executes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupingClause {
    /// `GROUP BY DISTINCT …`: deduplicate the expanded grouping sets. `ALL` is
    /// the default and is recorded as `false`.
    pub distinct: bool,
    /// The clause's items in source order; the expansion is their cross product.
    pub items: Vec<GroupItem>,
}

/// One element of a `GROUP BY` list (PG18 `group_by_item`).
///
/// Leaves are indices into [`SelectStmt::group_by`] rather than expressions, so a
/// pass that rewrites the grouping expressions (subquery resolution, output-ordinal
/// resolution) only has to touch that one list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupItem {
    /// A plain grouping expression.
    Expr(usize),
    /// The empty grouping set, `()`.
    Empty,
    /// `(a, b)` used inside `ROLLUP`/`CUBE`/`GROUPING SETS` as one composite
    /// element. The whole tuple joins or leaves a grouping set together.
    Composite(Vec<usize>),
    /// `ROLLUP (e1, e2, …)`: the n+1 prefixes of the element list, longest first.
    Rollup(Vec<GroupItem>),
    /// `CUBE (e1, e2, …)`: every subset of the element list.
    Cube(Vec<GroupItem>),
    /// `GROUPING SETS (item, …)`: the listed sets, which may nest.
    GroupingSets(Vec<GroupItem>),
}

/// A complete row-producing SQL query expression. The body may be a lone SELECT,
/// a lone VALUES list, or a set-operation tree. The tail applies to the complete
/// query expression.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryExpr {
    pub with: Option<WithClause>,
    pub body: SetExpr,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub with_ties: bool,
    pub locking: Option<LockingClause>,
}

/// SP39: a VALUES row constructor list. Every row is non-empty. Executor
/// analysis checks cross-row arity, so it gets `PostgreSQL`'s analysis SQLSTATE.
#[derive(Debug, Clone, PartialEq)]
pub struct ValuesStmt {
    pub rows: Vec<Vec<Expr>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub recursive: bool,
    pub ctes: Vec<Cte>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub body: CteBody,
    /// `MATERIALIZED` / `NOT MATERIALIZED`. `None` is `PostgreSQL`'s default, which
    /// inlines a side-effect-free CTE referenced exactly once. This is an optimizer
    /// hint only. It never changes the rows a CTE produces.
    pub materialized: Option<bool>,
    /// `SEARCH BREADTH FIRST BY … SET col` / `SEARCH DEPTH FIRST BY … SET col`.
    pub search: Option<CteSearch>,
    /// `CYCLE … SET col [TO v DEFAULT d] USING col`.
    pub cycle: Option<CteCycle>,
}

/// A recursive CTE's `SEARCH` clause: it appends one ordering column so the caller
/// can `ORDER BY` it to obtain a breadth- or depth-first traversal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CteSearch {
    /// `true` for `DEPTH FIRST`, `false` for `BREADTH FIRST`.
    pub depth_first: bool,
    /// The `BY` column list, naming columns of the CTE's own output.
    pub by: Vec<String>,
    /// The `SET` column: the name of the appended ordering column.
    pub set: String,
}

/// A recursive CTE's `CYCLE` clause: it stops the recursion when a row repeats a
/// key already on its own path, and appends a cycle-mark and a path column.
#[derive(Debug, Clone, PartialEq)]
pub struct CteCycle {
    /// The cycle key columns, naming columns of the CTE's own output.
    pub by: Vec<String>,
    /// The `SET` column: the appended cycle mark.
    pub set: String,
    /// `TO value DEFAULT default`: the marked/unmarked values. `None` is
    /// `PostgreSQL`'s default of `TRUE`/`FALSE`, which makes the column boolean.
    pub mark_values: Option<(Expr, Expr)>,
    /// The `USING` column: the appended path column.
    pub using: String,
}

/// What a `WITH` list entry contains. `PostgreSQL` allows a data-modifying
/// statement as well as a query; such a CTE runs exactly once per statement,
/// against the statement's snapshot, and is visible to the rest of the
/// statement only through its `RETURNING` output.
#[derive(Debug, Clone, PartialEq)]
pub enum CteBody {
    Query(Box<QueryExpr>),
    /// An `INSERT`, `UPDATE`, `DELETE`, or `MERGE` statement.
    Dml(Box<Statement>),
}

impl CteBody {
    /// The query body, for the read-only spelling.
    #[must_use]
    pub const fn as_query(&self) -> Option<&QueryExpr> {
        match self {
            Self::Query(q) => Some(q),
            Self::Dml(_) => None,
        }
    }
}

impl WithClause {
    /// Whether any entry is a data-modifying statement, which makes the whole
    /// statement a write even when its outer body is a plain query.
    #[must_use]
    pub fn has_data_modifying_cte(&self) -> bool {
        self.ctes
            .iter()
            .any(|cte| matches!(cte.body, CteBody::Dml(_)))
    }
}

/// SP39: query bodies that may appear as set-operation leaves or derived tables.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryBody {
    Select(Box<SelectStmt>),
    Values(ValuesStmt),
    Nested(Box<QueryExpr>),
}

/// SP38: a node in the set-operation tree. A `Query` leaf is one query block; a
/// `SetOp` combines two sub-trees. INTERSECT binds tighter than UNION/EXCEPT;
/// UNION/EXCEPT are left-associative (the parser encodes this in the tree shape).
#[derive(Debug, Clone, PartialEq)]
pub enum SetExpr {
    Query(QueryBody),
    SetOp {
        op: SetOp,
        /// `true` for `… ALL …` (keep duplicates); `false` for the default
        /// (duplicate-eliminating) form.
        all: bool,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Wildcard,
    /// SP33: `a.*`, every column of one table in scope.
    QualifiedWildcard(String),
    Expr {
        expr: Expr,
        alias: Option<String>,
    },
}

/// SP33: one entry in the FROM clause. An entry is a base table, a derived
/// table (subquery), or a join of two table-exprs. The comma form (`FROM a, b`)
/// is a `Vec<TableExpr>` with len > 1 (implicit cross join).
#[derive(Debug, Clone, PartialEq)]
pub enum TableExpr {
    Table {
        name: RelationRef,
        /// `ONLY relation` excludes inheritance descendants.
        only: bool,
        alias: Option<String>,
        /// The alias's column list (`t AS q(x, y)`), which renames the leading
        /// columns. Shorter than the relation is allowed; longer is `42P10`.
        columns: Option<Vec<String>>,
        /// `TABLESAMPLE method (percent) [REPEATABLE (seed)]`. Only a base table
        /// may carry one, matching `PostgreSQL`'s grammar.
        sample: Option<TableSample>,
    },
    Derived {
        subquery: QueryExpr,
        alias: String, // PG requires a derived table to be aliased
        columns: Option<Vec<String>>,
        /// `LATERAL (subquery)`: the subquery may reference columns of FROM
        /// items to its left, and is re-evaluated for each of their rows.
        lateral: bool,
    },
    Join {
        left: Box<TableExpr>,
        right: Box<TableExpr>,
        kind: JoinKind,
        constraint: JoinConstraint,
    },
    /// One or more set-returning functions in FROM position
    /// (`unnest(a) AS u(x)`, `ROWS FROM (f(…), g(…)) WITH ORDINALITY`). The
    /// parser accepts any function name and argument list. The executor decides
    /// which functions are actually table-producing.
    Function {
        /// The calls this item expands. Longer than one element only for the
        /// `ROWS FROM (…)` spelling.
        functions: Vec<TableFuncCall>,
        /// `true` when the item was written `ROWS FROM (…)`. A single-call
        /// `ROWS FROM` differs from a bare call only in that a bare call may
        /// carry a column-definition list directly.
        rows_from: bool,
        /// `WITH ORDINALITY`: append a `bigint` column counting output rows
        /// from 1.
        with_ordinality: bool,
        /// Explicit `LATERAL`. Function arguments are lateral in `PostgreSQL`
        /// whether or not the keyword is written, so the executor also treats an
        /// unmarked call whose arguments reference an earlier FROM item as
        /// lateral; this flag records only what the user spelled.
        lateral: bool,
        alias: Option<String>,
        column_aliases: Option<Vec<String>>,
    },
}

/// One function call inside a FROM-position function item.
#[derive(Debug, Clone, PartialEq)]
pub struct TableFuncCall {
    pub name: String,
    pub args: Vec<Expr>,
    /// `AS (col type, …)`: a column-definition list, which `PostgreSQL` allows
    /// only for functions returning `record`.
    pub column_defs: Option<Vec<TableFuncColumnDef>>,
}

/// One entry of a FROM-function column-definition list (`AS t(a int, b text)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableFuncColumnDef {
    pub name: String,
    pub ty: ColumnType,
}

/// `TABLESAMPLE <method> (<percent>) [REPEATABLE (<seed>)]`.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSample {
    /// The sampling method as written, lowercased (`system`, `bernoulli`).
    pub method: String,
    pub percent: Expr,
    pub repeatable: Option<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<String>),
    Natural,
    None, // CROSS JOIN / comma
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub asc: bool,
    /// Where NULLs sort. The parser resolves `PostgreSQL`'s defaults here:
    /// `NULLS LAST` for ASC, `NULLS FIRST` for DESC. So comparison never has to
    /// re-derive them from `asc`.
    pub nulls_first: bool,
}

/// One entry of a subscript chain: `a[i]`, `a[lo:hi]`, `a[:hi]`, `a[lo:]`.
///
/// An omitted slice bound means "this dimension's own bound", which the
/// executor fills in from the array being read or written; `PostgreSQL` has no
/// syntax for an omitted bound outside a slice.
#[derive(Debug, Clone, PartialEq)]
pub enum ArraySubscript {
    /// `a[i]`: one element of this dimension.
    Index(Expr),
    /// `a[lo:hi]`: a range of this dimension, either bound omissible.
    Slice {
        lower: Option<Expr>,
        upper: Option<Expr>,
    },
}

impl ArraySubscript {
    /// The bound expressions this entry carries, for the generic AST walks.
    #[must_use]
    pub fn bounds(&self) -> Vec<&Expr> {
        match self {
            ArraySubscript::Index(e) => vec![e],
            ArraySubscript::Slice { lower, upper } => lower.iter().chain(upper.iter()).collect(),
        }
    }

    /// [`ArraySubscript::bounds`] for a rewriting walk.
    pub fn bounds_mut(&mut self) -> Vec<&mut Expr> {
        match self {
            ArraySubscript::Index(e) => vec![e],
            ArraySubscript::Slice { lower, upper } => {
                lower.iter_mut().chain(upper.iter_mut()).collect()
            }
        }
    }

    /// Is this a slice? A chain containing one produces an array, not an element.
    #[must_use]
    pub fn is_slice(&self) -> bool {
        matches!(self, ArraySubscript::Slice { .. })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    IntLiteral(String),
    /// SP32: a decimal/exponent literal. `PostgreSQL` types these as `numeric`
    /// (SP30 typed them `float8`; SP32 introduced `numeric`, so a bare `1.5`/`1e3`
    /// is now scale-faithful `numeric`, and `float8` needs an explicit cast).
    NumericLiteral(String),
    StringLiteral(String),
    BoolLiteral(bool),
    NullLiteral,
    /// SP33: a column reference, optionally table-qualified (`a.col`). `table` is
    /// `None` for a bare `col`.
    Column {
        table: Option<String>,
        name: String,
    },
    Param(u32),
    /// `DEFAULT` in INSERT/UPDATE value position. The executor replaces this with
    /// the target column's catalog default or NULL.
    Default,
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// SP27: a function call, e.g. `count(*)`, `sum(a + 1)`, `count(DISTINCT x)`.
    /// The executor decides whether a name is an aggregate or an
    /// unknown/undefined function. The parser does not.
    Func(FuncCall),
    /// SP28: `expr IS [NOT] NULL`. Never evaluates to NULL itself.
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    /// SP28: `expr [NOT] IN (e1, e2, …)`, value-list membership (not a subquery).
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// SP28: `expr [NOT] BETWEEN low AND high` (bounds inclusive).
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    /// SP28: `expr [NOT] LIKE pat` / `[NOT] ILIKE pat` / `[NOT] SIMILAR TO pat`.
    /// `kind` selects the pattern language; `escape` is the optional `ESCAPE c`
    /// clause (`None` means each language's default escape character, `\`).
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        kind: MatchKind,
        escape: Option<Box<Expr>>,
    },
    /// SP28: a `CASE` expression. `operand` is `Some` for the simple form
    /// (`CASE x WHEN v THEN r …`) and `None` for the searched form
    /// (`CASE WHEN cond THEN r …`). `whens` is non-empty (parser-enforced).
    Case {
        operand: Option<Box<Expr>>,
        whens: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    /// SP31: an explicit cast, `CAST(expr AS ty)` or `expr::ty`. The target type
    /// The parser resolves the target type to a [`ColumnType`], and an unknown
    /// type name is a parse error. The executor does the value conversion.
    Cast {
        expr: Box<Expr>,
        ty: ColumnType,
    },
    /// `(expr).field`: one attribute of a composite value. Only reachable
    /// after a parenthesised expression, which is what distinguishes it from
    /// the table-qualified column reference `a.b`.
    FieldSelect {
        base: Box<Expr>,
        field: String,
    },
    /// `(expr).*`: every attribute of a composite value, expanded into as many
    /// output columns.
    FieldSelectAll(Box<Expr>),
    /// `expr COLLATE "name"`: a collation derivation. It never changes the
    /// value, only the collation the comparison and ordering of that value use.
    /// The engine has exactly the collations `pg_collation` reports (`default`,
    /// `C`, `POSIX`), which all order text by byte value, so every collation it
    /// accepts leaves the operand's behaviour unchanged; the parser refuses any
    /// other name. What survives into the tree is the type check `PostgreSQL`
    /// applies: a `COLLATE` on a non-collatable operand is `42804`.
    Collate {
        expr: Box<Expr>,
        collation: String,
    },
    /// SP34: a scalar subquery `(SELECT …)`, one row, one column, usable as an
    /// expression. Resolved (uncorrelated) to `Const` by the executor pre-pass.
    ScalarSubquery(Box<QueryExpr>),
    /// SP34: `EXISTS (SELECT …)`, true if and only if the subquery returns ≥1 row. `NOT
    /// EXISTS` is the prefix `NOT` wrapping this.
    Exists(Box<QueryExpr>),
    /// SP34: `expr [NOT] IN (SELECT …)`, subquery membership (single-column subquery).
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<QueryExpr>,
        negated: bool,
    },
    /// SP34: `expr op ANY|SOME|ALL (SELECT …)`. `all` is the `ALL` form; `ANY`/`SOME`
    /// are `all == false`. The subquery is single-column.
    Quantified {
        expr: Box<Expr>,
        op: BinaryOp,
        all: bool,
        subquery: Box<QueryExpr>,
    },
    /// `expr op ANY|SOME|ALL (<array expression>)`: the array form of a
    /// quantified comparison, which every driver that binds an IN-list as one
    /// parameter emits (`= ANY($1)`). The subquery form is
    /// [`Expr::Quantified`]. Lookahead after the quantifier's `(` tells the two
    /// apart.
    QuantifiedArray {
        expr: Box<Expr>,
        op: BinaryOp,
        all: bool,
        array: Box<Expr>,
    },
    /// `ARRAY[e1, e2, …]`: an array constructor. The element list may be empty
    /// (`ARRAY[]`), in which case the executor needs a cast to type it. A
    /// nested constructor (`ARRAY[[1,2],[3,4]]`, `ARRAY[ARRAY[1,2]]`) is an
    /// element that is itself an `ArrayLiteral`, and adds a dimension.
    ArrayLiteral(Vec<Expr>),
    /// `ARRAY(subquery)`: the array aggregation of a single-column subquery's
    /// rows, in the subquery's own order.
    ArraySubquery(Box<QueryExpr>),
    /// A row constructor: `ROW(a, b, …)` or the bare parenthesised `(a, b, …)`
    /// with two or more elements. `ROW(x)` and `ROW()` are rows too; a bare
    /// `(x)` is ordinary grouping, not a one-element row.
    ///
    /// Row values compare lexicographically field by field, take part in `IN`
    /// and `IS [NOT] DISTINCT FROM`, and follow `PostgreSQL`'s field-wise
    /// `IS NULL` rule (`ROW(1, NULL) IS NULL` is false).
    Row(Vec<Expr>),
    /// `base[index]`: a single-subscript reference. This is the jsonb
    /// subscripting form as well as the one-dimensional array one, and a chain
    /// of jsonb subscripts nests as `Subscript { base: Subscript { … } }`.
    Subscript {
        base: Box<Expr>,
        index: Box<Expr>,
    },
    /// `base[s1][s2]…` where the chain is longer than one plain subscript or
    /// contains a slice. `PostgreSQL` treats the whole chain as **one** array
    /// reference. `a[2][3]` picks an element of a two-dimensional array and
    /// does not subscript `a[2]`. So a nest of [`Expr::Subscript`] cannot model
    /// it.
    ArrayRef {
        base: Box<Expr>,
        subscripts: Vec<ArraySubscript>,
    },
    /// SP34: an executor-produced literal, a resolved subquery folded to a value
    /// carrying its static type. The parser NEVER emits this; `ty` matters because a
    /// zero-row scalar subquery is a typed NULL.
    Const {
        value: Datum,
        ty: ColumnType,
    },
    /// One of the SQL/JSON standard expression forms: `IS JSON`, the
    /// `JSON_OBJECT`/`JSON_ARRAY` constructors, and the
    /// `JSON_EXISTS`/`JSON_VALUE`/`JSON_QUERY` query functions. Boxed because
    /// the payload is much larger than every other variant.
    SqlJson(Box<SqlJsonExpr>),
}

/// The SQL/JSON standard expression forms `PostgreSQL` 18 spells out.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlJsonExpr {
    /// `expr IS [NOT] JSON [VALUE | SCALAR | ARRAY | OBJECT]
    /// [WITH | WITHOUT UNIQUE [KEYS]]`.
    IsJson {
        expr: Expr,
        negated: bool,
        item: JsonItemType,
        unique_keys: bool,
    },
    /// `JSON_OBJECT(k VALUE v, … [{NULL | ABSENT} ON NULL]
    /// [{WITH | WITHOUT} UNIQUE [KEYS]] [RETURNING type])`. `k: v` is the same
    /// entry written with a colon.
    Object {
        entries: Vec<(Expr, Expr)>,
        absent_on_null: bool,
        unique_keys: bool,
        returning: Option<ColumnType>,
    },
    /// `JSON_ARRAY(e, … [{NULL | ABSENT} ON NULL] [RETURNING type])`.
    Array {
        items: Vec<Expr>,
        absent_on_null: bool,
        returning: Option<ColumnType>,
    },
    /// `JSON_SCALAR(expr)`: the SQL value as the JSON scalar it maps to.
    Scalar(Expr),
    /// `JSON_SERIALIZE(expr [RETURNING type])`: a JSON value rendered as text.
    Serialize {
        expr: Expr,
        returning: Option<ColumnType>,
    },
    /// `JSON(expr [FORMAT JSON] [{WITH | WITHOUT} UNIQUE [KEYS]])`: parse text
    /// as a JSON document.
    Parse { expr: Expr, unique_keys: bool },
    /// `JSON_EXISTS` / `JSON_VALUE` / `JSON_QUERY`. Boxed: it is by far the
    /// largest of these forms.
    Query(Box<JsonQuery>),
}

impl SqlJsonExpr {
    /// Every sub-expression this node evaluates, in evaluation order. The
    /// executor's expression walks drive this. You must list a new field that
    /// carries an `Expr` here. If you do not, those walks cannot see it.
    #[must_use]
    pub fn children(&self) -> Vec<&Expr> {
        match self {
            SqlJsonExpr::IsJson { expr, .. }
            | SqlJsonExpr::Scalar(expr)
            | SqlJsonExpr::Serialize { expr, .. }
            | SqlJsonExpr::Parse { expr, .. } => vec![expr],
            SqlJsonExpr::Object { entries, .. } => entries
                .iter()
                .flat_map(|(k, v)| [k, v].into_iter())
                .collect(),
            SqlJsonExpr::Array { items, .. } => items.iter().collect(),
            SqlJsonExpr::Query(q) => {
                let mut out = vec![&q.context, &q.path];
                out.extend(q.passing.iter().map(|(_, e)| e));
                if let Some(JsonBehavior::Default(e)) = &q.on_empty {
                    out.push(e);
                }
                if let Some(JsonBehavior::Default(e)) = &q.on_error {
                    out.push(e);
                }
                out
            }
        }
    }

    /// The mutable counterpart of [`SqlJsonExpr::children`].
    #[must_use]
    pub fn children_mut(&mut self) -> Vec<&mut Expr> {
        match self {
            SqlJsonExpr::IsJson { expr, .. }
            | SqlJsonExpr::Scalar(expr)
            | SqlJsonExpr::Serialize { expr, .. }
            | SqlJsonExpr::Parse { expr, .. } => vec![expr],
            SqlJsonExpr::Object { entries, .. } => entries
                .iter_mut()
                .flat_map(|(k, v)| [k, v].into_iter())
                .collect(),
            SqlJsonExpr::Array { items, .. } => items.iter_mut().collect(),
            SqlJsonExpr::Query(q) => {
                let mut out = vec![&mut q.context, &mut q.path];
                out.extend(q.passing.iter_mut().map(|(_, e)| e));
                if let Some(JsonBehavior::Default(e)) = &mut q.on_empty {
                    out.push(e);
                }
                if let Some(JsonBehavior::Default(e)) = &mut q.on_error {
                    out.push(e);
                }
                out
            }
        }
    }

    /// Rebuild this node with every sub-expression replaced by `f` applied to
    /// it, in the same order [`SqlJsonExpr::children`] visits them.
    ///
    /// # Errors
    ///
    /// Whatever `f` returns for the first sub-expression it rejects.
    pub fn map_children<E>(&self, mut f: impl FnMut(&Expr) -> Result<Expr, E>) -> Result<Self, E> {
        let mut out = self.clone();
        for child in out.children_mut() {
            *child = f(child)?;
        }
        Ok(out)
    }

    /// The SQL type this expression reports, given its `RETURNING` clause.
    #[must_use]
    pub fn result_type(&self) -> ColumnType {
        match self {
            SqlJsonExpr::IsJson { .. } => ColumnType::Bool,
            SqlJsonExpr::Object { returning, .. } | SqlJsonExpr::Array { returning, .. } => {
                returning.unwrap_or(ColumnType::Jsonb)
            }
            SqlJsonExpr::Scalar(_) | SqlJsonExpr::Parse { .. } => ColumnType::Jsonb,
            SqlJsonExpr::Serialize { returning, .. } => returning.unwrap_or(ColumnType::Text),
            SqlJsonExpr::Query(q) => match q.op {
                JsonQueryOp::Exists => q.returning.unwrap_or(ColumnType::Bool),
                JsonQueryOp::Value => q.returning.unwrap_or(ColumnType::Text),
                JsonQueryOp::Query => q.returning.unwrap_or(ColumnType::Jsonb),
            },
        }
    }

    /// The name `PostgreSQL` labels an unaliased select item with.
    #[must_use]
    pub fn output_label(&self) -> &'static str {
        match self {
            SqlJsonExpr::IsJson { .. } => "?column?",
            SqlJsonExpr::Object { .. } => "json_object",
            SqlJsonExpr::Array { .. } => "json_array",
            SqlJsonExpr::Scalar(_) => "json_scalar",
            SqlJsonExpr::Serialize { .. } => "json_serialize",
            SqlJsonExpr::Parse { .. } => "json",
            SqlJsonExpr::Query(q) => match q.op {
                JsonQueryOp::Exists => "json_exists",
                JsonQueryOp::Value => "json_value",
                JsonQueryOp::Query => "json_query",
            },
        }
    }
}

/// The item type an `IS JSON` predicate tests for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonItemType {
    /// Bare `IS JSON`, and the explicit `IS JSON VALUE`.
    Value,
    Scalar,
    Array,
    Object,
}

/// One of the three SQL/JSON query functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonQueryOp {
    /// `JSON_EXISTS`: boolean, "does the path match anything?".
    Exists,
    /// `JSON_VALUE`: one SQL scalar, unquoted.
    Value,
    /// `JSON_QUERY`: one JSON value.
    Query,
}

/// `JSON_QUERY`'s array-wrapper option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonWrapper {
    /// `WITHOUT WRAPPER`: the default; more than one item is an error.
    Without,
    /// `WITH CONDITIONAL WRAPPER`: wrap only when the result is not a single item.
    Conditional,
    /// `WITH [UNCONDITIONAL] WRAPPER`: always wrap in an array.
    Unconditional,
}

/// What an `ON EMPTY` / `ON ERROR` clause asks for.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonBehavior {
    Error,
    Null,
    True,
    False,
    Unknown,
    EmptyArray,
    EmptyObject,
    Default(Expr),
}

/// A parsed `JSON_EXISTS` / `JSON_VALUE` / `JSON_QUERY` call.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonQuery {
    pub op: JsonQueryOp,
    /// The `jsonb` document the path runs over.
    pub context: Expr,
    /// The jsonpath, as an expression (usually a string literal).
    pub path: Expr,
    /// `PASSING v AS name, …`: the jsonpath variables.
    pub passing: Vec<(String, Expr)>,
    pub returning: Option<ColumnType>,
    pub wrapper: JsonWrapper,
    /// `OMIT QUOTES` on `JSON_QUERY`.
    pub omit_quotes: bool,
    pub on_empty: Option<JsonBehavior>,
    pub on_error: Option<JsonBehavior>,
}

/// SP27: a parsed function call. `name` is lowercased by the lexer.
#[derive(Debug, Clone, PartialEq)]
pub struct FuncCall {
    pub name: String,
    /// `true` for `f(DISTINCT …)`. `ALL` (the default) parses to `false`.
    pub distinct: bool,
    pub args: FuncArgs,
    /// `agg(args) FILTER (WHERE predicate)`: the aggregate receives only the
    /// rows for which the predicate is true. `None` when the call had no
    /// clause.
    ///
    /// Meaningful only for an aggregate (and, in `PostgreSQL`, an aggregate used
    /// as a window function). A plain scalar call cannot carry one, and any
    /// path that cannot honour it refuses and never drops it without a
    /// message.
    pub filter: Option<Box<Expr>>,
}

/// SP27: a function call's argument list. `Star` is the `f(*)` form (only
/// `count(*)` is meaningful); `Exprs` is a (possibly empty) positional list.
#[derive(Debug, Clone, PartialEq)]
pub enum FuncArgs {
    Star,
    Exprs(Vec<Expr>),
}

/// The scope qualifier that binds a `SELECT`'s window-function results during
/// the evaluation of its expressions.
///
/// `$` cannot begin an unquoted identifier, so no user column can collide with
/// a window binding and no user expression can name one.
pub const WINDOW_QUALIFIER: &str = "$w";

/// The scope binding name for the window call at `index`, carrying the output
/// label `PostgreSQL` gives an unaliased window call (the function's name).
#[must_use]
pub fn window_binding_name(index: usize, label: &str) -> String {
    format!("{WINDOW_QUALIFIER}{index} {label}")
}

/// Split a [`window_binding_name`] back into its call index and output label.
#[must_use]
pub fn window_binding_parts(name: &str) -> Option<(usize, &str)> {
    let rest = name.strip_prefix(WINDOW_QUALIFIER)?;
    let (index, label) = rest.split_once(' ')?;
    Some((index.parse().ok()?, label))
}

/// The expression that stands in for the window call at `index`.
///
/// The expression is a reference to the synthetic column that holds the
/// window call's value.
#[must_use]
pub fn window_placeholder(index: usize, label: &str) -> Expr {
    Expr::Column {
        table: Some(WINDOW_QUALIFIER.to_string()),
        name: window_binding_name(index, label),
    }
}

/// The window call index `expr` stands in for, if it is a [`window_placeholder`].
#[must_use]
pub fn window_placeholder_index(expr: &Expr) -> Option<usize> {
    let Expr::Column {
        table: Some(qualifier),
        name,
    } = expr
    else {
        return None;
    };
    if qualifier != WINDOW_QUALIFIER {
        return None;
    }
    window_binding_parts(name).map(|(index, _)| index)
}

/// One `f(…) [FILTER (WHERE …)] OVER …` call lifted out of a `SELECT`.
///
/// A [`window_placeholder`] that carries this call's index in
/// [`SelectStmt::window_calls`] holds the call's place in the expression tree.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowCall {
    /// The function name, lowercased by the lexer.
    pub name: String,
    /// `true` for `f(DISTINCT …) OVER …`, which `PostgreSQL` refuses (0A000).
    pub distinct: bool,
    pub args: FuncArgs,
    /// `FILTER (WHERE …)`: allowed only on an ordinary aggregate.
    pub filter: Option<Expr>,
    pub over: WindowRef,
}

/// What follows `OVER`: a bare window name, or an inline window definition.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowRef {
    Named(String),
    /// Boxed because an inline spec is far larger than a window name, and an
    /// `OVER` clause is overwhelmingly the named form in real queries.
    Spec(Box<WindowSpec>),
}

/// A `WINDOW name AS (…)` definition.
#[derive(Debug, Clone, PartialEq)]
pub struct NamedWindow {
    pub name: String,
    pub spec: WindowSpec,
}

/// The body of an `OVER (…)` clause or a `WINDOW` definition.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WindowSpec {
    /// The leading `existing_window_name` of `OVER (w ORDER BY …)`: the copied
    /// window supplies the partitioning (and its ordering, if it has one).
    pub base: Option<String>,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderItem>,
    /// `None` is `PostgreSQL`'s default frame: `RANGE BETWEEN UNBOUNDED
    /// PRECEDING AND CURRENT ROW`.
    pub frame: Option<WindowFrame>,
}

/// `{ ROWS | RANGE | GROUPS } BETWEEN <start> AND <end> [ EXCLUDE … ]`.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub mode: FrameMode,
    pub start: FrameBound,
    /// `CURRENT ROW` for the single-bound `{ROWS|RANGE|GROUPS} <start>` form,
    /// exactly as `PostgreSQL` expands it.
    pub end: FrameBound,
    pub exclusion: FrameExclusion,
}

/// What a frame offset counts: physical rows, ordering-value distance, or whole
/// peer groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameMode {
    Rows,
    Range,
    Groups,
}

/// One end of a frame.
#[derive(Debug, Clone, PartialEq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(Expr),
    CurrentRow,
    Following(Expr),
    UnboundedFollowing,
}

/// `EXCLUDE …`: which rows the frame drops around the current row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FrameExclusion {
    /// `EXCLUDE NO OTHERS`, the default.
    #[default]
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

/// Which pattern language an [`Expr::Like`] node's pattern is written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchKind {
    /// `LIKE`: `%` and `_` wildcards, case-sensitive.
    Like,
    /// `ILIKE`: `LIKE` with ASCII case folding.
    ILike,
    /// `SIMILAR TO`: the SQL-standard regular-expression dialect. `PostgreSQL`
    /// implements it and translates the pattern to a POSIX regexp.
    Similar,
}

/// A one-operand operator: the prefix forms (`NOT`, unary `-`, `~`, `@`, `|/`,
/// `||/`) and SQL's six postfix boolean tests. The tests belong here because
/// that is exactly what they are: one boolean operand in, one never-NULL
/// boolean out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
    /// Prefix `+`: identity on the numeric types, and defined on nothing else
    /// (`+'x'::text` is 42883 in `PostgreSQL`, not a no-op). It is an operator,
    /// not a sign: `ORDER BY +1` sorts by the constant `1`, where `ORDER BY -1`
    /// is the output position -1.
    Plus,
    /// `expr IS TRUE`
    IsTrue,
    /// `expr IS NOT TRUE`
    IsNotTrue,
    /// `expr IS FALSE`
    IsFalse,
    /// `expr IS NOT FALSE`
    IsNotFalse,
    /// `expr IS UNKNOWN`: `IS NULL` restricted to a boolean operand.
    IsUnknown,
    /// `expr IS NOT UNKNOWN`
    IsNotUnknown,
    /// Prefix `~`: bitwise NOT. Spelled like the infix regex-match operator;
    /// only the position tells them apart.
    BitNot,
    /// Prefix `@`: absolute value.
    Abs,
    /// Prefix `|/`: square root (`float8`).
    Sqrt,
    /// Prefix `||/`: cube root (`float8`).
    Cbrt,
    /// Prefix `!!`: tsquery boolean negation.
    TsNot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    /// `-`. Also the jsonb/array "delete key/element" operator. The operand
    /// types disambiguate at evaluation time, not at parse time.
    Sub,
    Mul,
    Div,
    /// SP29: `||` string concatenation. Also jsonb and array concatenation.
    Concat,
    /// jsonb `->`: object field / array element, as jsonb.
    JsonGet,
    /// jsonb `->>`: object field / array element, as text.
    JsonGetText,
    /// jsonb `#>`: value at a text path, as jsonb.
    JsonGetPath,
    /// jsonb `#>>`: value at a text path, as text.
    JsonGetPathText,
    /// jsonb/array `@>`: left contains right.
    Contains,
    /// jsonb/array `<@`: left is contained by right.
    ContainedBy,
    /// jsonb `?`: the string exists as a top-level key (or array element).
    KeyExists,
    /// jsonb `?|`: any of the given strings exist as top-level keys.
    KeyExistsAny,
    /// jsonb `?&`: all of the given strings exist as top-level keys.
    KeyExistsAll,
    /// jsonb `@?`: the jsonpath on the right finds at least one item.
    JsonPathExists,
    /// jsonb `@@`: the jsonpath predicate on the right, as a three-valued boolean.
    JsonPathMatch,
    /// array `&&`: the two arrays have at least one element in common.
    Overlaps,
    /// Range does not extend right of the other range (`&<`).
    DoesNotExtendRight,
    /// Range does not extend left of the other range (`&>`).
    DoesNotExtendLeft,
    /// Ranges are adjacent (`-|-`).
    Adjacent,
    /// `tsquery <-> tsquery` — adjacent phrase composition.
    Phrase,
    /// `~`: the left string matches the POSIX regular expression on the right.
    Match,
    /// `~*`: [`BinaryOp::Match`], case-insensitively.
    MatchCi,
    /// `!~`: the negation of [`BinaryOp::Match`].
    NotMatch,
    /// `!~*`: the negation of [`BinaryOp::MatchCi`].
    NotMatchCi,
    /// `&`: bitwise AND on two integers of the same width.
    BitAnd,
    /// `|`: bitwise OR on two integers of the same width.
    BitOr,
    /// `#`: bitwise XOR on two integers of the same width.
    BitXor,
    /// `<<`: bitwise left shift.
    Shl,
    /// `>>`: bitwise (arithmetic) right shift.
    Shr,
    /// `^`: exponentiation. `float8` unless an operand is `numeric`, and
    /// LEFT-associative in `PostgreSQL` (`2^3^2` is 64, not 512).
    Pow,
    /// `%`: modulo. Integer and `numeric` only; `float8` has no `%`.
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    /// `IS DISTINCT FROM`: null-safe inequality. Two NULLs are *not* distinct,
    /// a NULL and a non-NULL are; the result is never NULL.
    IsDistinctFrom,
    /// `IS NOT DISTINCT FROM`: null-safe equality, the negation of
    /// [`BinaryOp::IsDistinctFrom`]. Never returns NULL.
    IsNotDistinctFrom,
    And,
    Or,
}

/// P2: which spelling of the routine family a lifecycle statement used.
///
/// `PostgreSQL` reports the spelling back in the completion tag and enforces it
/// against the stored routine's kind, so `DROP FUNCTION p(int)` on a procedure
/// is `42809`, not a silent success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutineObject {
    Function,
    Procedure,
    /// The kind-agnostic `ROUTINE` spelling, which matches either kind.
    Routine,
}

impl RoutineObject {
    /// The command word `PostgreSQL` uses in the completion tag.
    #[must_use]
    pub const fn tag_word(self) -> &'static str {
        match self {
            Self::Function => "FUNCTION",
            Self::Procedure => "PROCEDURE",
            Self::Routine => "ROUTINE",
        }
    }
}

/// P2: a routine parameter's mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RoutineArgMode {
    #[default]
    In,
    Out,
    InOut,
    Variadic,
}

impl RoutineArgMode {
    /// True when the mode contributes to the routine's *input* signature, the
    /// type list that identifies a routine for overload resolution.
    #[must_use]
    pub const fn is_input(self) -> bool {
        matches!(self, Self::In | Self::InOut | Self::Variadic)
    }

    /// True when the mode contributes to the routine's *output* row.
    #[must_use]
    pub const fn is_output(self) -> bool {
        matches!(self, Self::Out | Self::InOut)
    }

    /// The `pg_proc.proargmodes` letter.
    #[must_use]
    pub const fn catalog_code(self) -> &'static str {
        match self {
            Self::In => "i",
            Self::Out => "o",
            Self::InOut => "b",
            Self::Variadic => "v",
        }
    }

    /// The prefix `pg_get_function_arguments` writes before the parameter.
    #[must_use]
    pub const fn spelled_prefix(self) -> &'static str {
        match self {
            Self::In => "",
            Self::Out => "OUT ",
            Self::InOut => "INOUT ",
            Self::Variadic => "VARIADIC ",
        }
    }
}

/// P2: one declared parameter of a routine.
#[derive(Debug, Clone, PartialEq)]
pub struct RoutineArg {
    /// The declared parameter name, absent for a positional-only declaration.
    pub name: Option<String>,
    pub mode: RoutineArgMode,
    pub ty: RoutineType,
    /// The SQL source text of the parameter's `DEFAULT`, if written. Stored as
    /// text so the catalog can spell it back exactly as `PostgreSQL` does in
    /// `pg_get_function_arguments`.
    pub default: Option<String>,
}

/// P2: a type written in a routine signature.
///
/// A routine may name a composite type with a relation name, which the parser
/// cannot resolve. The parser carries those names through as
/// [`RoutineType::Named`], and the catalog resolves them when the routine is
/// created.
#[derive(Debug, Clone, PartialEq)]
pub struct RoutineType {
    /// The resolved built-in type, absent when the name is not a built-in.
    pub resolved: Option<ColumnType>,
    /// The type name as `PostgreSQL` spells it back.
    pub name: String,
}

impl RoutineType {
    /// A built-in type resolved by the parser.
    #[must_use]
    pub fn builtin(ty: ColumnType, name: String) -> Self {
        Self {
            resolved: Some(ty),
            name,
        }
    }

    /// A name the parser could not resolve to a built-in type.
    #[must_use]
    pub const fn named(name: String) -> Self {
        Self {
            resolved: None,
            name,
        }
    }
}

/// P2: one output column of a `RETURNS TABLE(…)` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct RoutineTableColumn {
    pub name: String,
    pub ty: RoutineType,
}

/// P2: what a routine returns.
#[derive(Debug, Clone, PartialEq)]
pub enum RoutineReturn {
    /// No `RETURNS` clause: a procedure, or a function whose whole result
    /// comes from its `OUT`/`INOUT` parameters.
    Unspecified,
    /// `RETURNS [SETOF] <type>`. `void` and `record` arrive here as named types.
    Type { ty: RoutineType, setof: bool },
    /// `RETURNS TABLE (name type, …)`, which is `SETOF record` with names.
    Table(Vec<RoutineTableColumn>),
}

/// P2: a routine's volatility class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RoutineVolatility {
    Immutable,
    Stable,
    #[default]
    Volatile,
}

impl RoutineVolatility {
    /// The `pg_proc.provolatile` letter.
    #[must_use]
    pub const fn catalog_code(self) -> &'static str {
        match self {
            Self::Immutable => "i",
            Self::Stable => "s",
            Self::Volatile => "v",
        }
    }
}

/// P2: a routine's parallel-safety class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RoutineParallel {
    Safe,
    Restricted,
    #[default]
    Unsafe,
}

impl RoutineParallel {
    /// The `pg_proc.proparallel` letter.
    #[must_use]
    pub const fn catalog_code(self) -> &'static str {
        match self {
            Self::Safe => "s",
            Self::Restricted => "r",
            Self::Unsafe => "u",
        }
    }
}

/// P2: a routine's body.
#[derive(Debug, Clone, PartialEq)]
pub enum RoutineBody {
    /// `AS 'text'` / `AS $$ … $$`: the body exactly as written.
    Source(String),
    /// `PostgreSQL` 14's `BEGIN ATOMIC … END` SQL body, parsed at definition
    /// time. `text` is the source of the statement list, used to render
    /// `pg_get_functiondef`.
    Atomic {
        statements: Vec<Statement>,
        text: String,
    },
    /// `PostgreSQL` 14's `RETURN <expr>` single-expression SQL body. `text` is
    /// the expression source, used to render `pg_get_functiondef`.
    Return { expr: Expr, text: String },
}

/// P2: one `CREATE [OR REPLACE] {FUNCTION | PROCEDURE}` clause that the parser
/// accepts but that carries no execution meaning of its own.
#[derive(Debug, Clone, PartialEq)]
pub enum RoutineOption {
    Language(String),
    Body(RoutineBody),
    Volatility(RoutineVolatility),
    Strict(bool),
    SecurityDefiner(bool),
    Leakproof(bool),
    Parallel(RoutineParallel),
    Cost(f64),
    Rows(f64),
    Support(String),
    Window,
    /// `SET name { TO | = } value` / `SET name FROM CURRENT` / `RESET name`.
    Set {
        name: String,
        value: Option<String>,
    },
    /// `TRANSFORM FOR TYPE …`: recorded but not otherwise interpreted.
    Transform(Vec<String>),
}

/// P2: `CREATE [OR REPLACE] { FUNCTION | PROCEDURE } name (args) …`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateRoutineStmt {
    pub name: String,
    /// `FUNCTION` or `PROCEDURE`; `CREATE ROUTINE` is not `PostgreSQL` syntax.
    pub object: RoutineObject,
    pub or_replace: bool,
    pub args: Vec<RoutineArg>,
    pub returns: RoutineReturn,
    pub options: Vec<RoutineOption>,
}

/// A parsed PL/pgSQL block. Byte ranges are relative to the routine body.
#[derive(Debug, Clone, PartialEq)]
pub struct PlPgSqlBlock {
    /// How unqualified names shared by a PL variable and SQL column resolve.
    pub variable_conflict: PlPgSqlVariableConflict,
    pub label: Option<String>,
    pub declarations: Vec<PlPgSqlDeclaration>,
    pub statements: Vec<PlPgSqlStatement>,
    pub exceptions: Vec<PlPgSqlExceptionHandler>,
    pub end_label: Option<String>,
    pub span: std::ops::Range<usize>,
}

/// The PL/pgSQL `#variable_conflict` compiler directive.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PlPgSqlVariableConflict {
    /// Report SQLSTATE 42702 when both a PL variable and SQL column match.
    #[default]
    Error,
    /// Prefer the PL variable.
    UseVariable,
    /// Prefer the SQL column.
    UseColumn,
}

/// One entry in a PL/pgSQL `DECLARE` section.
#[derive(Debug, Clone, PartialEq)]
pub enum PlPgSqlDeclaration {
    Variable {
        name: String,
        ty: RoutineType,
        constant: bool,
        not_null: bool,
        default: Option<Expr>,
    },
    Alias {
        name: String,
        target: String,
    },
    Cursor {
        name: String,
        scroll: Option<bool>,
        arguments: Vec<(String, RoutineType)>,
        query: Box<Statement>,
    },
}

/// An assignable PL/pgSQL datum: a variable, record field, or subscripted value.
#[derive(Debug, Clone, PartialEq)]
pub struct PlPgSqlTarget {
    pub path: Vec<String>,
    pub subscripts: Vec<Expr>,
}

/// The target list accepted by `INTO` and cursor fetches.
#[derive(Debug, Clone, PartialEq)]
pub struct PlPgSqlInto {
    pub strict: bool,
    pub targets: Vec<PlPgSqlTarget>,
}

/// One executable PL/pgSQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum PlPgSqlStatement {
    Block(Box<PlPgSqlBlock>),
    Assign {
        target: PlPgSqlTarget,
        value: Expr,
    },
    Sql {
        statement: Box<Statement>,
        source: String,
        into: Option<PlPgSqlInto>,
    },
    Perform {
        query: Box<Statement>,
    },
    If {
        branches: Vec<(Expr, Vec<PlPgSqlStatement>)>,
        else_body: Vec<PlPgSqlStatement>,
    },
    Case {
        operand: Option<Expr>,
        arms: Vec<(Vec<Expr>, Vec<PlPgSqlStatement>)>,
        else_body: Option<Vec<PlPgSqlStatement>>,
    },
    Loop {
        label: Option<String>,
        kind: Box<PlPgSqlLoop>,
        body: Vec<PlPgSqlStatement>,
        end_label: Option<String>,
    },
    Exit {
        continuing: bool,
        label: Option<String>,
        when: Option<Expr>,
    },
    Return(Option<Expr>),
    ReturnNext(Option<Expr>),
    ReturnQuery(Box<Statement>),
    ReturnQueryExecute {
        query: Expr,
        using: Vec<Expr>,
    },
    Raise(PlPgSqlRaise),
    Execute {
        query: Expr,
        into: Option<PlPgSqlInto>,
        using: Vec<Expr>,
    },
    Open {
        cursor: String,
        scroll: Option<bool>,
        arguments: Vec<Expr>,
        query: Option<Box<Statement>>,
        dynamic_query: Option<Expr>,
        using: Vec<Expr>,
    },
    Fetch {
        cursor: String,
        direction: String,
        into: Option<PlPgSqlInto>,
        move_only: bool,
    },
    Close(String),
    GetDiagnostics {
        stacked: bool,
        items: Vec<(PlPgSqlTarget, String)>,
    },
    Assert {
        condition: Expr,
        message: Option<Expr>,
    },
    Transaction {
        commit: bool,
        chain: bool,
    },
    Null,
}

/// The source iterated by a PL/pgSQL loop.
#[derive(Debug, Clone, PartialEq)]
pub enum PlPgSqlLoop {
    Unconditional,
    While(Expr),
    Integer {
        variable: String,
        reverse: bool,
        lower: Expr,
        upper: Expr,
        step: Option<Expr>,
    },
    Query {
        targets: Vec<PlPgSqlTarget>,
        query: Box<Statement>,
    },
    Dynamic {
        targets: Vec<PlPgSqlTarget>,
        query: Expr,
        using: Vec<Expr>,
    },
    Foreach {
        target: PlPgSqlTarget,
        slice: Option<u32>,
        array: Expr,
    },
}

/// One `WHEN` arm of a block's exception section.
#[derive(Debug, Clone, PartialEq)]
pub struct PlPgSqlExceptionHandler {
    pub conditions: Vec<String>,
    pub statements: Vec<PlPgSqlStatement>,
}

/// A `RAISE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct PlPgSqlRaise {
    pub level: PlPgSqlRaiseLevel,
    pub condition: Option<String>,
    pub message: Option<String>,
    pub parameters: Vec<Expr>,
    pub options: Vec<(String, Expr)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlPgSqlRaiseLevel {
    Debug,
    Log,
    Info,
    Notice,
    Warning,
    Exception,
}

/// P2: a routine named for a lifecycle statement.
#[derive(Debug, Clone, PartialEq)]
pub struct RoutineSignature {
    pub name: String,
    /// The written argument list. `None` is the no-parentheses spelling, which
    /// resolves only when exactly one routine carries the name.
    pub args: Option<Vec<RoutineArg>>,
}

/// P2: the action of an `ALTER { FUNCTION | PROCEDURE | ROUTINE }`.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterRoutineAction {
    RenameTo(String),
    OwnerTo(String),
    SetSchema(String),
    /// A run of definition options, applied in order.
    Options(Vec<RoutineOption>),
    /// `DEPENDS ON EXTENSION` / `NO DEPENDS ON EXTENSION`, which Gres records
    /// as a no-op because it has no extension catalog.
    DependsOnExtension {
        name: String,
        no: bool,
    },
}
