//! The crabgresql AST for the SP2 slice.

use crabka_pgtypes::{ColumnType, Datum};

#[rustfmt::skip]
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// A `PostgreSQL` command that is recognized deliberately but cannot be
    /// executed by the Gres architecture. Metadata lives on [`RefusalCommand`]
    /// so parser, session, and compatibility tooling share one contract.
    CompatibilityRefusal(RefusalCommand),
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
        sharded: bool,
        sharding: Option<ShardingSpec>,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
        unique: bool,
        placement: IndexPlacement,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
    CreateView {
        name: String,
        /// Exact query text following `AS`, retained for durable catalog storage.
        definition: String,
        /// Parsed definition used by the executor to validate the view schema.
        query: QueryExpr,
    },
    DropTable {
        name: String,
    },
    DropView {
        name: String,
        if_exists: bool,
    },
    /// Bounded `ALTER TABLE` rename support. Column renames are parsed so they
    /// can fail with a clear unsupported-feature error until dependencies can be
    /// rewritten safely.
    AlterTableRename {
        table: String,
        rename: AlterTableRename,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<Expr>>,
        returning: Option<Vec<SelectItem>>,
    },
    Query(QueryExpr),
    Begin {
        isolation: Option<IsolationLevel>,
    },
    Commit,
    Rollback,
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
        returning: Option<Vec<SelectItem>>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
        returning: Option<Vec<SelectItem>>,
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
    },
    /* SQL parity matrix row: DROP ROLE / DROP USER. */ DropRole {
        name: String,
    },
    /* SQL parity matrix row: GRANT. */ GrantTablePrivileges {
        privileges: Vec<String>,
        table: String,
        grantees: Vec<String>,
    },
    /* SQL parity matrix row: REVOKE. */ RevokeTablePrivileges {
        privileges: Vec<String>,
        table: String,
        grantees: Vec<String>,
    },
    /* SQL parity matrix row: SET ROLE. */ SetRole {
        role: Option<String>,
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
    },
    /// `CREATE FOREIGN TABLE <name> (<col> <type>, …) SERVER <server> OPTIONS (…)`
    CreateForeignTable {
        name: String,
        columns: Vec<ColumnDef>,
        server: String,
        options: OptionList,
    },
    /// `DROP FOREIGN TABLE [IF EXISTS] <name>`
    DropForeignTable {
        name: String,
        if_exists: bool,
    },
    /// `IMPORT FOREIGN SCHEMA <remote_schema> [LIMIT TO | EXCEPT (<tables>)] FROM SERVER <server> [INTO <local_schema>]`
    ImportForeignSchema {
        remote_schema: String,
        selector: ImportSelector,
        server: String,
        into_schema: String,
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
    AlterTablespace,
    AlterTextSearchParser,
    AlterTextSearchTemplate,
    CreateAccessMethod,
    CreateConversion,
    CreateLanguage,
    CreateOperator,
    CreateOperatorClass,
    CreateOperatorFamily,
    CreatePublication,
    CreateRule,
    CreateSubscription,
    CreateTablespace,
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
    DropTablespace,
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
            Self::AlterTablespace => "ALTER TABLESPACE",
            Self::AlterTextSearchParser => "ALTER TEXT SEARCH PARSER",
            Self::AlterTextSearchTemplate => "ALTER TEXT SEARCH TEMPLATE",
            Self::CreateAccessMethod => "CREATE ACCESS METHOD",
            Self::CreateConversion => "CREATE CONVERSION",
            Self::CreateLanguage => "CREATE LANGUAGE",
            Self::CreateOperator => "CREATE OPERATOR",
            Self::CreateOperatorClass => "CREATE OPERATOR CLASS",
            Self::CreateOperatorFamily => "CREATE OPERATOR FAMILY",
            Self::CreatePublication => "CREATE PUBLICATION",
            Self::CreateRule => "CREATE RULE",
            Self::CreateSubscription => "CREATE SUBSCRIPTION",
            Self::CreateTablespace => "CREATE TABLESPACE",
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
            Self::DropTablespace => "DROP TABLESPACE",
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
            | Self::CreateOperatorClass
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
            Self::AlterTablespace | Self::CreateTablespace | Self::DropTablespace => {
                "tablespaces are not part of the chapter storage model"
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
    (
        AlterOperatorClass,
        "ALTER OPERATOR CLASS opc USING btree RENAME TO opc2"
    ),
    (
        AlterOperatorFamily,
        "ALTER OPERATOR FAMILY opf USING btree RENAME TO opf2"
    ),
    (AlterPublication, "ALTER PUBLICATION pub ADD TABLE t"),
    (AlterRule, "ALTER RULE r ON t RENAME TO r2"),
    (AlterSubscription, "ALTER SUBSCRIPTION sub DISABLE"),
    (AlterTablespace, "ALTER TABLESPACE ts RENAME TO ts2"),
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
    (
        CreateOperatorClass,
        "CREATE OPERATOR CLASS opc FOR TYPE integer USING btree AS OPERATOR 1 < (integer, integer)"
    ),
    (
        CreateOperatorFamily,
        "CREATE OPERATOR FAMILY opf USING btree"
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
    (CreateTablespace, "CREATE TABLESPACE ts LOCATION '/tmp/ts'"),
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
    (DropOperatorClass, "DROP OPERATOR CLASS opc USING btree"),
    (DropOperatorFamily, "DROP OPERATOR FAMILY opf USING btree"),
    (DropPublication, "DROP PUBLICATION pub"),
    (DropRule, "DROP RULE r ON t"),
    (DropSubscription, "DROP SUBSCRIPTION sub"),
    (DropTablespace, "DROP TABLESPACE ts"),
    (DropTextSearchParser, "DROP TEXT SEARCH PARSER p"),
    (DropTextSearchTemplate, "DROP TEXT SEARCH TEMPLATE t"),
    (DropTransform, "DROP TRANSFORM FOR integer LANGUAGE sql"),
    (Load, "LOAD 'lib'"),
    (SecurityLabel, "SECURITY LABEL ON TABLE t IS 'label'"),
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTableRename {
    Table { new_name: String },
    Column { column: String, new_name: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyStmt {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub format: CopyFormat,
}

pub const COPY_FROM_STDIN_SENTINEL: &str = "__copy_from_stdin";

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
    /// `LIMIT TO (table, …)` — import only the listed tables.
    LimitTo(Vec<String>),
    /// `EXCEPT (table, …)` — import all tables except the listed ones.
    Except(Vec<String>),
}

/// SP37: the right-hand side of a `SET` (or the value form of `SET TIME ZONE`).
/// `Default` is `SET ... = DEFAULT` / `SET TIME ZONE { DEFAULT | LOCAL }` (resets
/// the parameter to its built-in default); `Value` is a literal/identifier value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetValue {
    Default,
    Value(String),
}

/// Transaction isolation levels supported by SP4.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
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

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    NotNull,
    Default(Expr),
    PrimaryKey,
    Unique,
    Check(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
    Check(Expr),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLockStrength {
    ForUpdate,
    ForShare,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub projection: Vec<SelectItem>,
    /// SP33: the FROM clause — a list of join trees. Empty for a FROM-less SELECT;
    /// the comma form (`FROM a, b`) is a `Vec<TableExpr>` with len > 1 (implicit
    /// cross join).
    pub from: Vec<TableExpr>,
    pub filter: Option<Expr>,
    /// SP28: `SELECT DISTINCT` — dedup the projected output rows.
    pub distinct: bool,
    /// SP27: `GROUP BY <expr-list>` (empty when absent).
    pub group_by: Vec<Expr>,
    /// SP27: `HAVING <predicate>` (evaluated per group).
    pub having: Option<Expr>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<i64>,
    /// SP28: `OFFSET <n>` — skip the first `n` output rows (before LIMIT).
    pub offset: Option<i64>,
    pub locking: Option<RowLockStrength>,
}

/// A complete row-producing SQL query expression. The body may be a lone SELECT,
/// a lone VALUES list, or a set-operation tree. The tail applies to the complete
/// query expression.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryExpr {
    pub with: Option<WithClause>,
    pub body: SetExpr,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub locking: Option<RowLockStrength>,
}

/// SP39: a VALUES row constructor list. Every row is non-empty; cross-row arity
/// is checked during executor analysis so it gets `PostgreSQL`'s analysis SQLSTATE.
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
    pub query: QueryExpr,
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
    /// SP33: `a.*` — every column of one table in scope.
    QualifiedWildcard(String),
    Expr {
        expr: Expr,
        alias: Option<String>,
    },
}

/// SP33: one entry in the FROM clause — a base table, a derived table
/// (subquery), or a join of two table-exprs. The comma form (`FROM a, b`) is a
/// `Vec<TableExpr>` with len > 1 (implicit cross join).
#[derive(Debug, Clone, PartialEq)]
pub enum TableExpr {
    Table {
        name: String,
        alias: Option<String>,
    },
    Derived {
        subquery: QueryExpr,
        alias: String, // PG requires a derived table to be aliased
        columns: Option<Vec<String>>,
    },
    Join {
        left: Box<TableExpr>,
        right: Box<TableExpr>,
        kind: JoinKind,
        constraint: JoinConstraint,
    },
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    IntLiteral(String),
    /// SP32: a decimal/exponent literal. `PostgreSQL` types these as `numeric`
    /// (SP30 typed them `float8`; SP32 introduced `numeric`, so a bare `1.5`/`1e3`
    /// is now scale-faithful `numeric` — `float8` requires an explicit cast).
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
    /// Whether a name is an aggregate (vs. an unknown/undefined function) is
    /// decided by the executor, not the parser.
    Func(FuncCall),
    /// SP28: `expr IS [NOT] NULL`. Never evaluates to NULL itself.
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    /// SP28: `expr [NOT] IN (e1, e2, …)` — value-list membership (not a subquery).
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
    /// SP28: `expr [NOT] LIKE pat` / `[NOT] ILIKE pat`. `%`/`_` wildcards with a
    /// `\` escape; `case_insensitive` is the ILIKE form.
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        case_insensitive: bool,
    },
    /// SP28: a `CASE` expression. `operand` is `Some` for the simple form
    /// (`CASE x WHEN v THEN r …`) and `None` for the searched form
    /// (`CASE WHEN cond THEN r …`). `whens` is non-empty (parser-enforced).
    Case {
        operand: Option<Box<Expr>>,
        whens: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    /// SP31: an explicit cast — `CAST(expr AS ty)` or `expr::ty`. The target type
    /// is resolved to a [`ColumnType`] by the parser (an unknown type name is a
    /// parse error); the executor performs the value conversion.
    Cast {
        expr: Box<Expr>,
        ty: ColumnType,
    },
    /// SP34: a scalar subquery `(SELECT …)` — one row, one column, usable as an
    /// expression. Resolved (uncorrelated) to `Const` by the executor pre-pass.
    ScalarSubquery(Box<QueryExpr>),
    /// SP34: `EXISTS (SELECT …)` — true iff the subquery returns ≥1 row. `NOT
    /// EXISTS` is the prefix `NOT` wrapping this.
    Exists(Box<QueryExpr>),
    /// SP34: `expr [NOT] IN (SELECT …)` — subquery membership (single-column subquery).
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
    /// SP34: an executor-produced literal — a resolved subquery folded to a value
    /// carrying its static type. The parser NEVER emits this; `ty` matters because a
    /// zero-row scalar subquery is a typed NULL.
    Const {
        value: Datum,
        ty: ColumnType,
    },
}

/// SP27: a parsed function call. `name` is lowercased by the lexer.
#[derive(Debug, Clone, PartialEq)]
pub struct FuncCall {
    pub name: String,
    /// `true` for `f(DISTINCT …)`. `ALL` (the default) parses to `false`.
    pub distinct: bool,
    pub args: FuncArgs,
}

/// SP27: a function call's argument list. `Star` is the `f(*)` form (only
/// `count(*)` is meaningful); `Exprs` is a (possibly empty) positional list.
#[derive(Debug, Clone, PartialEq)]
pub enum FuncArgs {
    Star,
    Exprs(Vec<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    /// SP29: `||` string concatenation.
    Concat,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}
