//! Catalog as a stateless view over a `Kv` store.
//!
//! The catalog holds tables and their columns, and does CRUD with `PostgreSQL`
//! error codes. SP3's KV layer stores the data.

#![doc(html_root_url = "https://docs.rs/crabka-pgcatalog/0.4.0")]

pub mod largeobject;
pub mod policy;
pub mod routine;
pub mod rule;
pub mod serde;
pub mod trigger;

use std::{
    collections::{BTreeMap, HashSet},
    sync::atomic::{AtomicU32, Ordering},
};

use crabka_pgkv::{Kv, KvError, WriteOp, key};
use crabka_pgtypes::{
    ColumnType, Datum,
    usertype::{UserType, UserTypeBody},
};
use zerocopy::{
    FromBytes, IntoBytes,
    byteorder::big_endian::{U32, U64},
};

use crate::serde::{
    deserialize_fdw, deserialize_foreign_key, deserialize_index, deserialize_schema,
    deserialize_sequence, deserialize_server, deserialize_sharding, deserialize_user_mapping,
    deserialize_user_type, deserialize_user_type_with, deserialize_view, serialize_fdw,
    serialize_foreign_key, serialize_index, serialize_schema, serialize_sequence, serialize_server,
    serialize_sharding, serialize_user_mapping, serialize_user_type, serialize_view,
};

/// OID-style table identifier (never 0; 0 is reserved/invalid).
pub type TableId = u32;

/// OID-style index identifier (never 0; 0 is reserved/invalid).
pub type IndexId = u32;

/// OID-style foreign-key identifier (never 0; 0 is reserved/invalid).
///
/// One monotonic counter supplies these ids. A comparison of two ids therefore
/// compares the order in which the constraints were created. `PostgreSQL` fires
/// their referential-integrity triggers in that order, and lists them in that
/// order as dependents of an object that is dropped.
pub type ForeignKeyId = u32;

/// A relation's resolved name: the schema, and the name within that schema.
///
/// The catalog never flattens the two halves into one string, because it cannot
/// recover them from one. `PostgreSQL` lets a relation called `a.b` in `public`
/// and a relation called `b` in schema `a` exist at the same time with distinct
/// contents, and a `schema.relation` string cannot tell them apart. Every
/// catalog key that names a relation is therefore built from these two parts,
/// each length-prefixed.
///
/// This type is the only way to reach a catalog lookup. An unqualified name
/// therefore cannot silently mean "whatever `public` holds". It must pass
/// through the resolver that knows the search path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelationName {
    /// The schema, always resolved. This is never a qualifier as written.
    pub schema: String,
    /// The relation's name within [`RelationName::schema`].
    pub name: String,
}

impl RelationName {
    /// A relation named `name` in `schema`.
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            name: name.into(),
        }
    }

    /// A relation in `public`, the schema where the default search path puts
    /// every unqualified name and where the bootstrap and test fixtures live.
    pub fn public(name: impl Into<String>) -> Self {
        Self::new(PUBLIC_SCHEMA, name)
    }

    /// True when this relation is in `public`.
    #[must_use]
    pub fn is_public(&self) -> bool {
        self.schema == PUBLIC_SCHEMA
    }

    /// Another object in the same schema, such as an index or a sequence beside
    /// the table that owns it. `PostgreSQL` puts both there.
    #[must_use]
    pub fn sibling(&self, name: impl Into<String>) -> Self {
        Self::new(self.schema.clone(), name)
    }
}

impl std::fmt::Display for RelationName {
    /// Spelled the way `PostgreSQL` spells a relation in a diagnostic. A
    /// relation in `public` is bare, because the default search path makes that
    /// the unqualified name. Every other relation is `schema.name`, with the
    /// schema spelled as [`displayed_schema`] spells it — so a temporary
    /// relation is `pg_temp.t` and never `pg_temp_<backend id>.t`.
    ///
    /// The backend id is the one part of a resolved name that no run of the
    /// same statements has to agree on: it counts the sessions the process has
    /// opened, so anything that opens one more session earlier renumbers every
    /// temporary namespace after it. A diagnostic carrying it therefore changes
    /// text without changing meaning, and `postgres:18.4` never puts a session's
    /// *own* backend id in front of a relation name either.
    ///
    /// # This rendering is not an identity
    ///
    /// Two relations in two sessions' temporary namespaces spell the same. Never
    /// key a map, a set or a lookup on this string; use the [`RelationName`]
    /// itself, which keeps the schema apart.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_public() {
            f.write_str(&self.name)
        } else {
            write!(f, "{}.{}", displayed_schema(&self.schema), self.name)
        }
    }
}

/// The schema an unqualified name resolves to under the default search path.
pub const PUBLIC_SCHEMA: &str = "public";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnDefault {
    Value(Datum),
    NextVal(String),
    Expression(String),
}

/// How a `GENERATED … AS IDENTITY` column reacts to a user-supplied value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityKind {
    /// `GENERATED ALWAYS AS IDENTITY`. A non-DEFAULT value causes error 428C9
    /// unless the statement says `OVERRIDING SYSTEM VALUE`.
    Always,
    /// `GENERATED BY DEFAULT AS IDENTITY`. A supplied value wins over the
    /// sequence.
    ByDefault,
}

/// Whether a generated column's value is kept in the row or recomputed on
/// every read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeneratedKind {
    /// `STORED`: computed on write and kept in the row.
    Stored,
    /// `VIRTUAL`: never stored. The row holds a NULL placeholder at the
    /// column's position and every reader recomputes the value, which is why
    /// changing the expression changes what rows written earlier report.
    /// `PostgreSQL` 18 makes this the default when neither keyword is written.
    Virtual,
}

/// A column's `GENERATED ALWAYS AS (…)` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedColumn {
    /// Source text of the generation expression, without the enclosing parens.
    pub expr: String,
    /// Whether the value is written into the row or recomputed on every read.
    pub kind: GeneratedKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
    pub not_null: bool,
    pub default: Option<ColumnDefault>,
    /// The column's `GENERATED ALWAYS AS (<expr>)` clause, stored or virtual.
    pub generated: Option<GeneratedColumn>,
    /// Set when the column is an identity column; the sequence itself lives in
    /// `default` as a [`ColumnDefault::NextVal`].
    pub identity: Option<IdentityKind>,
    /// The column's explicitly written `COLLATE "name"`, if any. `None` means
    /// the type's own default collation — which is what a collatable column
    /// with no clause reports and the only thing a non-collatable one can have.
    ///
    /// Every collation this engine has orders text by byte value, so this only
    /// changes what `pg_attribute.attcollation` (and so `\d`) reports; it never
    /// changes how two values compare.
    pub collation: Option<String>,
}

impl Column {
    #[must_use]
    pub fn new(name: impl Into<String>, ty: ColumnType) -> Self {
        Self {
            name: name.into(),
            ty,
            not_null: false,
            default: None,
            generated: None,
            identity: None,
            collation: None,
        }
    }

    /// The generation expression's source text, whatever its kind.
    #[must_use]
    pub fn generation_expr(&self) -> Option<&str> {
        self.generated.as_ref().map(|g| g.expr.as_str())
    }

    /// True for `GENERATED ALWAYS AS (…) VIRTUAL`, whose value is never stored:
    /// the row carries a NULL placeholder and each reader recomputes it.
    #[must_use]
    pub fn is_virtual_generated(&self) -> bool {
        matches!(
            self.generated,
            Some(GeneratedColumn {
                kind: GeneratedKind::Virtual,
                ..
            })
        )
    }

    /// True for `GENERATED ALWAYS AS (…) STORED`, whose value is computed on
    /// write and written into the row.
    #[must_use]
    pub fn is_stored_generated(&self) -> bool {
        matches!(
            self.generated,
            Some(GeneratedColumn {
                kind: GeneratedKind::Stored,
                ..
            })
        )
    }

    /// `pg_attribute.attgenerated`: `"s"` for a stored generated column, `"v"`
    /// for a virtual one, and the empty string for a column that is not
    /// generated.
    #[must_use]
    pub fn attgenerated(&self) -> &'static str {
        match self.generated.as_ref().map(|g| g.kind) {
            Some(GeneratedKind::Stored) => "s",
            Some(GeneratedKind::Virtual) => "v",
            None => "",
        }
    }
}

/// A named table `CHECK` constraint.
///
/// The catalog stores the predicate as source text and parses it again when a
/// row is written. That also makes the predicate rewriteable:
/// `ALTER TABLE … RENAME COLUMN` rewrites the text through the parser's lexer.
/// A `CHECK` predicate is scoped to exactly one relation, so every bare column
/// reference in it belongs to that relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckConstraint {
    pub name: String,
    /// Source text of the predicate, without the enclosing parentheses.
    pub expr: String,
    /// `pg_constraint.convalidated`. This is false for a constraint added
    /// `NOT VALID`. The catalog enforces such a constraint for new writes, but
    /// never checked it against the rows already stored.
    /// `ALTER TABLE … VALIDATE CONSTRAINT` runs that scan and sets the field to
    /// true.
    pub validated: bool,
}

/// Metadata stored alongside a foreign table that links it to its server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignTableMeta {
    /// The foreign server name this table is attached to.
    pub server: String,
    /// Table-level OPTIONS, for example `topic = 'orders'`.
    pub options: Vec<(String, String)>,
}

/// Ordinary-table creation options stored in the catalog schema record.
///
/// Every flag here is a single bit of the schema record's table-option byte, so
/// they travel together: a reader that recovers one has recovered all of them
/// or has refused the record outright.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TableOptions {
    /// True when writes use global visibility and routing may span ranges.
    pub sharded: bool,
    /// `pg_class.relrowsecurity`: `ALTER TABLE … ENABLE ROW LEVEL SECURITY`.
    /// While it is set, a read of the relation is filtered by the policies in
    /// [`crate::policy`] — including when the relation has none, which hides
    /// every row rather than showing every row.
    pub row_security: bool,
    /// `pg_class.relforcerowsecurity`: `ALTER TABLE … FORCE ROW LEVEL
    /// SECURITY`, which stops the relation's owner from bypassing its own
    /// policies. Meaningless on its own — it only has an effect while
    /// [`Self::row_security`] is set.
    pub force_row_security: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardingStrategy {
    Hash(HashSharding),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashSharding {
    pub columns: Vec<String>,
    pub buckets: u32,
    pub co_location_group: Option<String>,
}

/// Physical ownership of a secondary index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexPlacement {
    /// Index entries live with the base row's owning range.
    Local,
    /// Index entries live in separate index ranges and must use timestamp txns.
    Global,
}

/// Physical access method used by a secondary index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMethod {
    Btree,
    Hash,
    Gist,
    Gin,
    Spgist,
}

/// Constraint backed by an automatically-created index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexConstraint {
    PrimaryKey,
    Unique,
    Exclusion(Vec<ExclusionOperator>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExclusionOperator {
    Equal,
    Overlaps,
}

/// When a constraint is checked, as `pg_constraint`'s `condeferrable` and
/// `condeferred` pair spells it.
///
/// `condeferred` without `condeferrable` is not a state `PostgreSQL` can be in,
/// so the two columns are one value here and the impossible pair cannot be
/// written down.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ConstraintDeferral {
    /// `NOT DEFERRABLE` — checked as each row is written.
    #[default]
    Immediate,
    /// `DEFERRABLE INITIALLY IMMEDIATE` — checked once at the end of the
    /// statement, and `SET CONSTRAINTS … DEFERRED` may move it to `COMMIT`.
    Deferrable,
    /// `DEFERRABLE INITIALLY DEFERRED` — checked at `COMMIT`.
    Deferred,
}

impl ConstraintDeferral {
    /// The value the `[NOT] DEFERRABLE` / `INITIALLY …` tail of a constraint
    /// spells. `INITIALLY DEFERRED` implies `DEFERRABLE`, which the grammar
    /// already guarantees.
    #[must_use]
    pub fn of(deferrable: bool, initially_deferred: bool) -> Self {
        match (deferrable, initially_deferred) {
            (_, true) => Self::Deferred,
            (true, false) => Self::Deferrable,
            (false, false) => Self::Immediate,
        }
    }

    /// May `SET CONSTRAINTS` move this constraint's check point?
    #[must_use]
    pub fn is_deferrable(self) -> bool {
        self != Self::Immediate
    }

    /// Does the constraint start each transaction deferred?
    #[must_use]
    pub fn initially_deferred(self) -> bool {
        self == Self::Deferred
    }

    /// The `(condeferrable, condeferred)` pair, in catalog column order.
    #[must_use]
    pub fn columns(self) -> (bool, bool) {
        match self {
            Self::Immediate => (false, false),
            Self::Deferrable => (true, false),
            Self::Deferred => (true, true),
        }
    }
}

/// Secondary-index catalog definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Index {
    pub id: IndexId,
    pub name: String,
    pub table: RelationName,
    pub table_id: TableId,
    pub columns: Vec<String>,
    pub unique: bool,
    pub placement: IndexPlacement,
    pub method: IndexMethod,
    pub constraint: Option<IndexConstraint>,
    /// `PostgreSQL` 18's `PRIMARY KEY`/`UNIQUE (…, c WITHOUT OVERLAPS)`: the
    /// last key column is a range or multirange held apart by `&&` rather than
    /// `=`, so the key is enforced like an exclusion constraint even though it
    /// is catalogued as a primary key or unique constraint
    /// (`pg_constraint.conperiod`).
    pub without_overlaps: bool,
    /// `pg_index.indisclustered`: this is the index a bare `CLUSTER <table>`
    /// reorders the heap by. At most one index per relation carries it —
    /// `CLUSTER … USING` and `ALTER TABLE … CLUSTER ON` clear it from the
    /// relation's other indexes as they set it here.
    pub clustered: bool,
    /// When the `PRIMARY KEY` or `UNIQUE` constraint this index enforces is
    /// checked. Always [`ConstraintDeferral::Immediate`] for an index that
    /// backs no constraint: `CREATE INDEX` has no deferrability to write.
    pub deferral: ConstraintDeferral,
}

/// The identity a table uses when logical replication needs to identify a row.
///
/// The default is absent from storage, so databases created before this record
/// existed retain PostgreSQL's `DEFAULT` behaviour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaIdentity {
    Default,
    Full,
    Nothing,
    Index(String),
}

impl Index {
    /// The index's own qualified name. An index lives in the schema of the
    /// table it indexes, exactly as in `PostgreSQL`.
    #[must_use]
    pub fn qualified_name(&self) -> RelationName {
        self.table.sibling(&self.name)
    }

    /// The per-column operators this index holds rows apart with, when it is
    /// enforced by exclusion rather than by equality.
    ///
    /// An explicit `EXCLUDE` constraint carries its own list. A `WITHOUT
    /// OVERLAPS` key implies one: `=` on every leading column and `&&` on the
    /// trailing range, which is precisely the `EXCLUDE USING gist (a WITH =, b
    /// WITH &&)` that `PostgreSQL` builds for it. Everything else — a plain
    /// unique index, a primary key — returns `None` and is enforced by the
    /// equality path.
    #[must_use]
    pub fn exclusion_operators(&self) -> Option<Vec<ExclusionOperator>> {
        if let Some(IndexConstraint::Exclusion(operators)) = &self.constraint {
            return Some(operators.clone());
        }
        if !self.without_overlaps || self.columns.is_empty() {
            return None;
        }
        let mut operators = vec![ExclusionOperator::Equal; self.columns.len() - 1];
        operators.push(ExclusionOperator::Overlaps);
        Some(operators)
    }
}

/// Secondary-index catalog definition to create for a known table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewIndex {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub placement: IndexPlacement,
    pub method: IndexMethod,
    pub constraint: Option<IndexConstraint>,
    /// See [`Index::without_overlaps`].
    pub without_overlaps: bool,
    /// See [`Index::deferral`].
    pub deferral: ConstraintDeferral,
}

const INDEX_EXPRESSION_PREFIX: &str = "\0expr:";

/// Encode an expression key in the existing ordered key list. `PostgreSQL`
/// columns cannot contain NUL, so this cannot collide with a real column name.
#[must_use]
pub fn expression_index_key(source: &str) -> String {
    format!("{INDEX_EXPRESSION_PREFIX}{source}")
}

/// Return the stored source for an expression key, or `None` for a column key.
#[must_use]
pub fn index_key_expression(key: &str) -> Option<&str> {
    key.strip_prefix(INDEX_EXPRESSION_PREFIX)
}

/// What a foreign key does to the referencing rows when the referenced row is
/// deleted (`ON DELETE`) or its key columns are updated (`ON UPDATE`) —
/// `pg_constraint.confdeltype` / `confupdtype`.
///
/// The action applies when the referenced row is deleted (`ON DELETE`) or its
/// key columns are updated (`ON UPDATE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferentialAction {
    /// Refuse at the end of the statement, deferring the check when the
    /// constraint is deferred. The default.
    NoAction,
    /// Refuse immediately, without honoring a deferral.
    Restrict,
    /// Delete the referencing rows, or carry the new key values into them.
    Cascade,
    /// Set the referencing columns to NULL.
    SetNull,
    /// Set the referencing columns to their column DEFAULTs.
    SetDefault,
}

/// How a foreign key treats a partly-NULL composite key, from
/// `pg_constraint.confmatchtype`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchType {
    /// `MATCH SIMPLE` (the default): a row with any NULL referencing column
    /// satisfies the constraint.
    Simple,
    /// `MATCH FULL`: the referencing columns must be all NULL or all non-NULL.
    Full,
}

/// A `FOREIGN KEY` constraint, as stored in the catalog.
///
/// The identity is `(table_id, name)`. Constraint names are per-relation in
/// `PostgreSQL`, so two relations may each carry a `fk_owner`.
///
/// [`ForeignKey::referenced_table_id`] and [`ForeignKey::referenced_index_id`]
/// identify the referent. They are the analogues of `pg_constraint.confrelid`
/// and `conindid`. [`ForeignKey::referenced_table`] and
/// [`ForeignKey::referenced_index`] are denormalized display copies for error
/// messages and `pg_get_constraintdef`, and a rename rewrites them. The catalog
/// stores columns as names, like [`Index::columns`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
    /// Creation-order id, the stand-in for `pg_constraint.oid`.
    ///
    /// This is not the identity. The identity stays `(table_id, name)`, because
    /// a lookup uses the name a statement writes. This id orders constraints
    /// against each other, and it survives a rename, exactly as an OID does.
    pub id: ForeignKeyId,
    /// Constraint name, unique within the child relation.
    pub name: String,
    /// Child relation name, a display copy that a rename rewrites.
    pub table: RelationName,
    /// Child relation id; the authority, and the `fk/by-table` key.
    pub table_id: TableId,
    /// Referencing columns, in written order.
    pub columns: Vec<String>,
    /// Referenced relation name, a display copy that a rename rewrites.
    pub referenced_table: RelationName,
    /// Referenced relation id; the authority, and the `fk/by-ref` key.
    pub referenced_table_id: TableId,
    /// Referenced columns, paired 1:1 with [`ForeignKey::columns`].
    pub referenced_columns: Vec<String>,
    /// The unique index that proves the referenced columns are a key, resolved
    /// once at DDL time.
    pub referenced_index_id: IndexId,
    /// Referenced index name, a display copy.
    pub referenced_index: String,
    pub match_type: MatchType,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
    /// `ON DELETE SET {NULL|DEFAULT} (a, b)`. Empty means all of
    /// [`ForeignKey::columns`].
    pub set_columns: Vec<String>,
    /// The constraint may be `SET CONSTRAINTS … DEFERRED` within a transaction.
    pub deferrable: bool,
    /// The constraint starts each transaction deferred; implies `deferrable`.
    pub initially_deferred: bool,
    /// `pg_constraint.convalidated`. This is false for a constraint added
    /// `NOT VALID`. The catalog enforces such a constraint for new writes, but
    /// never checked it against the rows already stored.
    pub validated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sequence {
    pub start: i64,
    pub increment: i64,
    pub min: i64,
    pub max: i64,
    pub cache: i64,
    pub cycle: bool,
    pub last_value: i64,
    pub is_called: bool,
}

impl Sequence {
    #[must_use]
    pub fn new(
        start: i64,
        increment: i64,
        min: Option<i64>,
        max: Option<i64>,
        cache: Option<i64>,
        cycle: bool,
    ) -> Self {
        let min = min.unwrap_or(if increment > 0 { 1 } else { i64::MIN });
        let max = max.unwrap_or(if increment > 0 { i64::MAX } else { -1 });
        Self {
            start,
            increment,
            min,
            max,
            cache: cache.unwrap_or(1),
            cycle,
            last_value: start,
            is_called: false,
        }
    }
}

/// A foreign-data wrapper registration (`CREATE FOREIGN DATA WRAPPER …`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignDataWrapper {
    pub name: String,
    /// The optional handler routine named by `HANDLER`.
    pub handler: Option<String>,
    /// The optional validator routine named by `VALIDATOR`.
    pub validator: Option<String>,
    /// OPTIONS, for example the handler and the validator.
    pub options: Vec<(String, String)>,
}

/// A foreign server registration (`CREATE SERVER … FOREIGN DATA WRAPPER …`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignServer {
    pub name: String,
    /// The FDW this server belongs to.
    pub wrapper: String,
    /// The optional server type named by `TYPE`.
    pub server_type: Option<String>,
    /// The optional server version named by `VERSION`.
    pub version: Option<String>,
    /// Server-level OPTIONS, for example `bootstrap_servers`.
    pub options: Vec<(String, String)>,
}

/// A user mapping (`CREATE USER MAPPING FOR … SERVER …`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserMapping {
    pub user: String,
    pub server: String,
    /// Mapping-level OPTIONS.
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Role {
    pub name: String,
    pub can_login: bool,
    /// The boolean attributes `pg_authid` projects. `PostgreSQL`'s `CREATE ROLE`
    /// defaults are all false except `rolinherit`.
    pub attributes: RoleAttributes,
}

/// One `CREATE`/`ALTER ROLE … WITH` boolean attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleAttribute {
    Superuser,
    Inherit,
    CreateRole,
    CreateDb,
    Replication,
    BypassRls,
}

impl RoleAttribute {
    /// Every attribute, in `pg_authid` column order.
    pub const ALL: [Self; 6] = [
        Self::Superuser,
        Self::Inherit,
        Self::CreateRole,
        Self::CreateDb,
        Self::Replication,
        Self::BypassRls,
    ];

    const fn bit(self) -> u8 {
        match self {
            Self::Superuser => 1 << 0,
            Self::Inherit => 1 << 1,
            Self::CreateRole => 1 << 2,
            Self::CreateDb => 1 << 3,
            Self::Replication => 1 << 4,
            Self::BypassRls => 1 << 5,
        }
    }
}

/// `PostgreSQL`'s boolean role attributes, held as a bitset so the durable
/// record is one byte and the set cannot drift out of sync with its encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleAttributes {
    bits: u8,
}

impl Default for RoleAttributes {
    /// `PostgreSQL`'s `CREATE ROLE` defaults: everything off but `INHERIT`.
    fn default() -> Self {
        Self {
            bits: RoleAttribute::Inherit.bit(),
        }
    }
}

impl RoleAttributes {
    /// Whether `attribute` is set.
    #[must_use]
    pub const fn has(self, attribute: RoleAttribute) -> bool {
        self.bits & attribute.bit() != 0
    }

    /// Set or clear `attribute`.
    pub const fn set(&mut self, attribute: RoleAttribute, value: bool) {
        if value {
            self.bits |= attribute.bit();
        } else {
            self.bits &= !attribute.bit();
        }
    }

    const fn to_bits(self) -> u8 {
        self.bits
    }

    const fn from_bits(bits: u8) -> Self {
        Self { bits }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePrivilege {
    pub table: RelationName,
    pub grantee: String,
    pub privilege: String,
}

/// A table privilege applied to relations a role creates in one schema, or in
/// every schema when [`DefaultTablePrivilege::schema`] is `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultTablePrivilege {
    pub owner: String,
    pub schema: Option<String>,
    pub grantee: String,
    pub privilege: String,
    pub grant: bool,
}

/// One recorded `GRANT … (column) ON relation TO grantee` — `PostgreSQL`'s
/// `pg_attribute.attacl`, one row per bit rather than one array per column.
///
/// It is a record of its own rather than a field on [`TablePrivilege`] because
/// the two answer different questions and neither implies the other:
/// `GRANT SELECT ON t` lets a grantee read every column, present and future,
/// while `GRANT SELECT (a) ON t` lets it read exactly `a`. Keeping them in
/// separate key ranges means a relation-level scan never has to step over the
/// column grants, and a column check never has to filter them out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPrivilege {
    /// The relation the column belongs to.
    pub table: RelationName,
    /// The column name, stored exactly as the statement spelled it after the
    /// parser folded case. Unlike [`Self::privilege`] it is not uppercased,
    /// because a quoted column name is case-sensitive.
    pub column: String,
    /// The role the grant names, which may be [`PUBLIC_ROLE`].
    pub grantee: String,
    /// One name from [`COLUMN_PRIVILEGES`], uppercased. `ALL` is never stored:
    /// see [`expand_column_privileges`].
    pub privilege: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub id: TableId,
    pub name: RelationName,
    /// The role that owns the relation — `pg_class.relowner`. Set from the
    /// creating session's `current_user` and rewritten by
    /// `ALTER TABLE … OWNER TO`.
    pub owner: String,
    pub columns: Vec<Column>,
    /// True when the table uses global-visibility semantics and may span ranges.
    pub sharded: bool,
    /// `pg_class.relrowsecurity` — see [`TableOptions::row_security`].
    ///
    /// It is a field on `Table`, not a side catalog keyed by relation, for one
    /// reason: every read path already holds a `Table`, so it holds this too.
    /// There is no lookup to forget and no window in which a relation has been
    /// fetched but its row-security state has not.
    pub row_security: bool,
    /// `pg_class.relforcerowsecurity` — see
    /// [`TableOptions::force_row_security`].
    pub force_row_security: bool,
    /// Optional physical sharding strategy for range routing.
    pub sharding: Option<ShardingStrategy>,
    /// Present when the table is a foreign table; `None` for ordinary tables.
    /// Mutually exclusive with [`Self::materialized`]: a relation's contents
    /// come either from a remote server or from a stored query, never both.
    pub foreign: Option<ForeignTableMeta>,
    /// Present when the relation is a materialized view; `None` for every other
    /// stored relation. Mutually exclusive with [`Self::foreign`].
    pub materialized: Option<MaterializedView>,
    /// Table `CHECK` constraints, in declaration order.
    pub checks: Vec<CheckConstraint>,
}

/// What makes a stored relation a materialized view: the query its contents
/// come from, and whether those contents have been computed yet.
///
/// It rides on [`Table`] rather than in a catalog of its own for the reason
/// [`Table::row_security`] gives — every read path already holds a `Table`, so
/// it holds this too, and there is no second lookup to forget. That also makes
/// `relkind` a pure function of the record: a `Table` carrying this is `m`,
/// one carrying [`Table::foreign`] is `f`, and so on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedView {
    /// The `AS <query>` text as written, which `pg_matviews.definition` and
    /// `pg_get_viewdef` deparse from — stored exactly as [`View::definition`]
    /// is, so the two render through one path.
    pub definition: String,
    /// `pg_class.relispopulated`. `CREATE … WITH NO DATA` and
    /// `REFRESH … WITH NO DATA` clear it; a successful `REFRESH` sets it.
    /// Scanning a relation whose flag is clear is `55000`.
    pub populated: bool,
}

/// A stored view definition and its resolved output schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct View {
    pub name: RelationName,
    pub definition: String,
    /// The role that owns the relation — `pg_class.relowner`. Set from the
    /// creating session's `current_user`, and what lets a privilege check admit
    /// the owner of a view it has granted nobody else access to.
    pub owner: String,
    pub columns: Vec<Column>,
    /// `pg_class.reloptions` — the `WITH (…)` list the view was written with.
    pub options: ViewOptions,
}

/// The `CREATE VIEW … WITH (…)` reloptions this catalog keeps.
///
/// [`Self::security_invoker`] is honoured. A view without it evaluates its body
/// with [`View::owner`]'s rights — both the privilege checks and the
/// row-security policies applied to whatever the body reads — and one with it
/// keeps the caller's. What makes the owner-rights default safe is that the
/// view's *own* ACL is still checked against the caller first: a view can reach
/// relations the caller cannot, so the caller has to have been granted the
/// view.
///
/// [`Self::security_barrier`] is recorded and inert, and it is inert for a
/// structural reason rather than a pending one: a view body is materialized
/// before the reader's own qualifier runs, so there is no reordering for a
/// barrier to forbid.
///
/// [`Self::check_option`] is enforced on writes rewritten through the view: a
/// row written through it must satisfy the view's own qualification, and
/// `CASCADED` extends that to every view underneath. Storing it as an `Option`
/// rather than a level plus a flag is what makes "the view has no check option"
/// distinguishable from "the view has a `LOCAL` one", which is the difference
/// between a parent's cascade reaching this level and stopping here.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ViewOptions {
    pub security_invoker: bool,
    pub security_barrier: bool,
    pub check_option: Option<ViewCheckOption>,
}

/// How far a view's `WITH CHECK OPTION` reaches.
///
/// Spelled here rather than borrowed from the parser because this crate is the
/// durable catalog and depends on no SQL grammar: it stores a view's body as
/// text and never parses one. The executor converts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewCheckOption {
    /// Only this view's own qualification is checked.
    Local,
    /// This view's qualification and every underlying view's, whether or not
    /// those views declare an option of their own.
    Cascaded,
}

impl Table {
    /// Zero-based ordinal of a column by name, or None.
    #[must_use]
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error("relation \"{0}\" already exists")]
    DuplicateTable(String),
    #[error("relation \"{0}\" does not exist")]
    UndefinedTable(String),
    #[error("\"{0}\" is not a view")]
    WrongObjectType(String),
    #[error("column \"{0}\" does not exist")]
    UndefinedColumn(String),
    /// `PostgreSQL` creates indexes in the relation namespace, so it reports a
    /// name collision against the *relation*, not the index.
    #[error("relation \"{0}\" already exists")]
    DuplicateIndex(String),
    #[error("index \"{0}\" does not exist")]
    UndefinedIndex(String),
    #[error("cannot drop index \"{0}\" because it is required by a table constraint")]
    DependentObjectsStillExist(String),
    #[error("tablespace \"{0}\" is not empty")]
    TablespaceNotEmpty(String),
    #[error("cannot drop operator family \"{0}\" because other objects depend on it")]
    OperatorFamilyNotEmpty(String),
    /// A relation already carries a constraint of this name. Constraint names
    /// are per-relation, so `PostgreSQL` reports the relation beside the name.
    /// It reports 42710, not the 42P07 that an index name collision gets.
    #[error("constraint \"{name}\" for relation \"{relation}\" already exists")]
    DuplicateConstraint { name: String, relation: String },
    /// A constraint lookup or `ALTER TABLE … DROP CONSTRAINT` named a
    /// constraint the relation does not have (42704).
    #[error("constraint \"{0}\" does not exist")]
    UndefinedConstraint(String),
    /// A row-security policy name is per-relation, exactly like a constraint
    /// name, so `PostgreSQL` reports the relation alongside the name.
    #[error("policy \"{name}\" for table \"{relation}\" already exists")]
    DuplicatePolicy { name: String, relation: String },
    /// `ALTER POLICY`/`DROP POLICY` named a policy the relation does not carry.
    #[error("policy \"{name}\" for table \"{relation}\" does not exist")]
    UndefinedPolicy { name: String, relation: String },
    #[error("sequence \"{0}\" already exists")]
    DuplicateSequence(String),
    #[error("sequence \"{0}\" does not exist")]
    UndefinedSequence(String),
    #[error("large object {0} does not exist")]
    UndefinedLargeObject(u32),
    #[error("large object {0} already exists")]
    DuplicateLargeObject(u32),
    #[error("invalid sequence definition: {0}")]
    InvalidSequence(String),
    /// A sharding definition that describes a table this engine has no encoding
    /// for. The code is 0A000 rather than 22023, because the spec is well
    /// formed and only the shape it asks for is not supported.
    #[error("invalid sharding definition: {0}")]
    InvalidSharding(String),
    #[error("relation \"{0}\" is not an ordinary table")]
    NotOrdinaryTable(String),
    #[error("table conversion rewrite does not remove every existing physical tuple")]
    IncompleteConversionRewrite,
    /// A stored view names the relation under rename in a position the catalog
    /// cannot rewrite. The catalog stores views as SQL text, not as a parsed
    /// rule over relation oids.
    #[error(
        "cannot rename relation \"{0}\": a stored view references it in a position this catalog cannot rewrite"
    )]
    StoredViewDependency(String),
    /// Generic "object already exists" (42710), for FDW, server, user-mapping.
    #[error("object \"{0}\" already exists")]
    DuplicateObject(String),
    /// An FDW option list names one setting more than once.
    #[error("option \"{0}\" provided more than once")]
    DuplicateOption(String),
    /// Generic "undefined object" (42704), for FDW, server, user-mapping.
    #[error("object \"{0}\" does not exist")]
    UndefinedObject(String),
    /// `CREATE SCHEMA` named a schema that already exists (42P06).
    #[error("schema \"{0}\" already exists")]
    DuplicateSchema(String),
    /// `CREATE SCHEMA` named a schema whose name carries the reserved
    /// [`RESERVED_SCHEMA_PREFIX`] (42939). `PostgreSQL` checks the prefix
    /// before it checks for a duplicate, so this outranks
    /// [`CatalogError::DuplicateSchema`] even for `pg_catalog` itself.
    #[error("unacceptable schema name \"{0}\"")]
    ReservedSchemaName(String),
    /// `DROP SCHEMA` named one of the [`SYSTEM_SCHEMAS`] (2BP01).
    #[error("cannot drop schema {0} because it is required by the database system")]
    SystemSchemaDrop(String),
    /// `ALTER SCHEMA … RENAME TO` named one of the [`SYSTEM_SCHEMAS`] (2BP01).
    ///
    /// `PostgreSQL` refuses these on ownership instead, because its system
    /// schemas are ordinary `pg_namespace` rows a superuser may rewrite. Here
    /// they are synthesised by name rather than stored — see
    /// [`BOOTSTRAP_SCHEMAS`] — so a rename would leave their contents
    /// unreachable rather than move them.
    #[error("cannot rename schema {0} because it is required by the database system")]
    SystemSchemaRename(String),
    /// `ALTER SCHEMA … RENAME TO` found an object in the schema that the
    /// rename batch cannot relocate (0A000).
    ///
    /// The batch moves the relation families and the grants on them. A type,
    /// an operator and an operator class each carry their schema in their own
    /// key *and* in oid-linked records that name them, so moving one is not
    /// the same problem and this refuses rather than strands it.
    #[error(
        "cannot rename schema {schema}: it contains {object}, which this catalog cannot move to another schema"
    )]
    UnmovableSchemaObject {
        schema: String,
        object: &'static str,
    },
    /// `DROP SCHEMA … RESTRICT` found relations still in the schema (2BP01).
    /// This differs from [`CatalogError::DependentObjectsStillExist`], whose
    /// message is about an index and whose payload is an index name.
    #[error("cannot drop schema {0} because other objects depend on it")]
    SchemaNotEmpty(String),
    /// A schema-qualified name or schema command named a schema that does not
    /// exist (3F000).
    #[error("schema \"{0}\" does not exist")]
    UndefinedSchema(String),
    #[error("catalog storage error: {0}")]
    Storage(#[from] KvError),
}

impl CatalogError {
    #[must_use]
    pub fn sqlstate(&self) -> &'static str {
        match self {
            CatalogError::DuplicateTable(_)
            | CatalogError::DuplicateIndex(_)
            | CatalogError::DuplicateSequence(_) => "42P07",
            CatalogError::DuplicateLargeObject(_) => "42710",
            CatalogError::UndefinedTable(_) | CatalogError::UndefinedSequence(_) => "42P01",
            CatalogError::UndefinedLargeObject(_) => "42704",
            CatalogError::WrongObjectType(_) => "42809",
            CatalogError::UndefinedColumn(_) => "42703",
            CatalogError::UndefinedIndex(_)
            | CatalogError::UndefinedObject(_)
            | CatalogError::UndefinedConstraint(_)
            | CatalogError::UndefinedPolicy { .. } => "42704",
            CatalogError::DependentObjectsStillExist(_)
            | CatalogError::TablespaceNotEmpty(_)
            | CatalogError::OperatorFamilyNotEmpty(_)
            | CatalogError::SystemSchemaDrop(_)
            | CatalogError::SystemSchemaRename(_)
            | CatalogError::SchemaNotEmpty(_) => "2BP01",
            CatalogError::InvalidSequence(_) => "22023",
            CatalogError::NotOrdinaryTable(_)
            | CatalogError::StoredViewDependency(_)
            | CatalogError::UnmovableSchemaObject { .. }
            | CatalogError::InvalidSharding(_) => "0A000",
            CatalogError::DuplicateObject(_)
            | CatalogError::DuplicateOption(_)
            | CatalogError::DuplicateConstraint { .. }
            | CatalogError::DuplicatePolicy { .. } => "42710",
            CatalogError::DuplicateSchema(_) => "42P06",
            CatalogError::ReservedSchemaName(_) => "42939",
            CatalogError::UndefinedSchema(_) => "3F000",
            CatalogError::Storage(KvError::Io(_)) => "58030",
            CatalogError::IncompleteConversionRewrite
            | CatalogError::Storage(
                KvError::CorruptRow(_)
                | KvError::RestoreTargetNotEmpty
                | KvError::UnsortedSnapshot
                | KvError::ConditionalPutUnsupported,
            ) => "XX000",
        }
    }
}

/// A SQL schema (a `pg_namespace` row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub name: String,
    pub owner: String,
}

/// A user-created tablespace catalog row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tablespace {
    pub oid: u32,
    pub name: String,
    pub owner: String,
    pub location: String,
    pub options: Vec<(String, String)>,
}

/// A user-defined row in `pg_am`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessMethodKind {
    Index,
    Table,
}

/// A user-created access method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessMethod {
    pub oid: u32,
    pub name: String,
    pub kind: AccessMethodKind,
    pub handler: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorFamily {
    pub oid: u32,
    pub name: RelationName,
    pub method: String,
    pub owner: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorClass {
    pub oid: u32,
    pub name: RelationName,
    pub method: String,
    pub owner: String,
    pub family_oid: u32,
    pub input_type_oid: u32,
    pub default: bool,
    pub key_type_oid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorFamilyMember {
    Operator {
        number: u16,
        operator: String,
        left_type_oid: u32,
        right_type_oid: u32,
        order_family_oid: u32,
    },
    Function {
        number: u16,
        function: String,
        left_type_oid: u32,
        right_type_oid: u32,
        argument_type_oids: Vec<u32>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperatorFamilyMemberKey {
    Operator {
        number: u16,
        left_type_oid: u32,
        right_type_oid: u32,
    },
    Function {
        number: u16,
        left_type_oid: u32,
        right_type_oid: u32,
    },
}

/// The bootstrap superuser. Every object a session does not name an owner for
/// belongs to whoever created it; this is the role a cluster starts out as, and
/// the owner the catalog's own convenience constructors use.
pub const BOOTSTRAP_ROLE: &str = "postgres";

/// The pseudo-role every role implicitly belongs to. It has no `pg_authid`
/// row, cannot be granted membership of anything, and cannot own an object —
/// so a session carrying it as its user has no authenticated role at all.
pub const PUBLIC_ROLE: &str = "public";

/// `public`'s owner.
///
/// `PostgreSQL` gives the schema to the implicit `pg_database_owner` role
/// rather than to the bootstrap superuser. Whoever owns the database therefore
/// owns the schema the database starts with.
pub const PUBLIC_SCHEMA_OWNER: &str = "pg_database_owner";

/// The fixed `pg_authid` identities every `PostgreSQL` cluster supplies.
///
/// They are catalog fixtures, not stored roles: user-created roles keep their
/// durable records, while these retain the OIDs `PostgreSQL` assigns them.
pub const PREDEFINED_ROLES: &[(&str, i32)] = &[
    ("pg_monitor", 3373),
    ("pg_read_all_settings", 3374),
    ("pg_read_all_stats", 3375),
    ("pg_stat_scan_tables", 3377),
    ("pg_signal_backend", 4200),
    ("pg_checkpoint", 4544),
    ("pg_use_reserved_connections", 4550),
    ("pg_read_server_files", 4569),
    ("pg_write_server_files", 4570),
    ("pg_execute_server_program", 4571),
    ("pg_database_owner", 6171),
    ("pg_read_all_data", 6181),
    ("pg_write_all_data", 6182),
    ("pg_create_subscription", 6304),
    ("pg_maintain", 6337),
    ("pg_signal_autovacuum_worker", 6392),
];

fn builtin_role(name: &str) -> Option<Role> {
    if name != BOOTSTRAP_ROLE && !PREDEFINED_ROLES.iter().any(|(role, _)| *role == name) {
        return None;
    }
    let mut attributes = RoleAttributes::default();
    if name == BOOTSTRAP_ROLE {
        for attribute in [
            RoleAttribute::Superuser,
            RoleAttribute::CreateRole,
            RoleAttribute::CreateDb,
            RoleAttribute::BypassRls,
        ] {
            attributes.set(attribute, true);
        }
    }
    Some(Role {
        name: name.to_string(),
        can_login: name == BOOTSTRAP_ROLE,
        attributes,
    })
}

/// The schemas a database has before anything is created in it, each with the
/// owner `PostgreSQL` bootstraps it under.
///
/// The catalog stores none of them. A fresh catalog holds no schema rows at
/// all, and [`list_schemas`] synthesises these three until a stored row
/// supersedes one or a tombstone removes it. `public` is an ordinary schema
/// that merely happens to exist already, so it can be dropped and created
/// again. The [`SYSTEM_SCHEMAS`] cannot be dropped and cannot be created,
/// because [`RESERVED_SCHEMA_PREFIX`] covers their names or the names are
/// already taken.
pub const BOOTSTRAP_SCHEMAS: &[(&str, &str)] = &[
    ("pg_catalog", BOOTSTRAP_ROLE),
    ("information_schema", BOOTSTRAP_ROLE),
    ("public", PUBLIC_SCHEMA_OWNER),
];

/// The bootstrap schemas the database system itself needs.
///
/// `DROP SCHEMA` refuses them with 2BP01 even when they are empty.
pub const SYSTEM_SCHEMAS: &[&str] = &["pg_catalog", "information_schema"];

/// The schema-name prefix `PostgreSQL` reserves for system schemas.
///
/// `CREATE SCHEMA` refuses any name that carries it, before it looks for a
/// duplicate.
pub const RESERVED_SCHEMA_PREFIX: &str = "pg_";

/// The qualifier that names *this* session's temporary namespace, whatever it
/// is called.
///
/// `CREATE TABLE pg_temp.t` creates a temporary relation. `search_path` may
/// name this qualifier to place the temporary namespace explicitly.
pub const PG_TEMP_ALIAS: &str = "pg_temp";

/// The prefix every session's temporary namespace carries.
const TEMP_SCHEMA_PREFIX: &str = "pg_temp_";

/// The temporary namespace of the session the wire layer announced
/// `backend_id` for.
///
/// Verified against `postgres:18.4`, where a session's `current_schemas(true)`
/// reports `pg_temp_<n>` first.
///
/// This is the namespace's *name*, which is what the catalog stores it under and
/// what `pg_namespace`, `pg_tables`, `information_schema` and `current_schemas`
/// all report. It is not what a diagnostic or a deparsed reference spells: see
/// [`displayed_schema`].
#[must_use]
pub fn temp_schema_name(backend_id: i32) -> String {
    format!("{TEMP_SCHEMA_PREFIX}{backend_id}")
}

/// True when `name` is some session's temporary namespace.
///
/// A relation's persistence comes from the schema that holds it, and the
/// catalog does not store it beside the relation. `PostgreSQL` keeps every
/// temporary relation in the owning session's temporary namespace, and keeps
/// nothing else there. This covers tables, views, indexes and the sequence
/// behind a `serial` column. The schema and the persistence are therefore the
/// same fact.
#[must_use]
pub fn is_temp_schema(name: &str) -> bool {
    name.strip_prefix(TEMP_SCHEMA_PREFIX)
        .is_some_and(|suffix| !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()))
}

/// The name a *display* context spells `schema` with: [`PG_TEMP_ALIAS`] for a
/// temporary namespace, and the schema's own name for everything else.
///
/// This is `PostgreSQL`'s `get_namespace_name_or_temp`, which is what every
/// deparsed reference and every object identity goes through. The plain
/// `get_namespace_name` is what the catalog's own columns report, and those keep
/// the number. Measured on `postgres:18.4` with a temporary table `probe_t`:
///
/// ```text
/// EXPLAIN (VERBOSE) SELECT * FROM probe_t   Seq Scan on pg_temp.probe_t
/// pg_get_indexdef(…)                        … ON pg_temp.probe_t USING btree (a)
/// pg_identify_object(…).identity            pg_temp.probe_t
/// pg_identify_object_as_address(…)          {pg_temp,probe_t}
/// pg_event_trigger_ddl_commands().schema    pg_temp
///
/// current_schemas(true)                     {pg_temp_19,pg_catalog,public}
/// pg_namespace.nspname                      pg_temp_19
/// pg_tables.schemaname                      pg_temp_19
/// pg_identify_object(…).schema              pg_temp_19
/// \d probe_t                                Table "pg_temp_19.probe_t"
/// ```
///
/// # One session's namespace is not another's
///
/// `PostgreSQL` compares the namespace against `myTempNamespace`, so it prints
/// the alias only for the *reading* session's own namespace and keeps the number
/// for every other one — `Seq Scan on pg_temp_20.other_sess_t`, measured with
/// two connections. This function has no session to compare against, so it
/// spells every temporary namespace `pg_temp`. Its callers are diagnostics,
/// where the divergence needs a statement that writes another live session's
/// `pg_temp_<n>` as a qualifier, and it is one of under-specification: `pg_temp`
/// reads back as the reader's *own* namespace, so no name this produces can
/// reach a namespace that was not already reachable. Spelling the alias exactly
/// as `PostgreSQL` does means giving every rendering site the session's
/// [`temp_schema_name`] to compare against.
#[must_use]
pub fn displayed_schema(schema: &str) -> &str {
    if is_temp_schema(schema) {
        PG_TEMP_ALIAS
    } else {
        schema
    }
}

/// The `pg_class.relpersistence` a relation in `schema` reports.
#[must_use]
pub fn relpersistence_of(schema: &str) -> char {
    if is_temp_schema(schema) { 't' } else { 'p' }
}

/// The op that records a session's temporary namespace.
///
/// [`create_schema_ops`] refuses a `pg_`-prefixed name, as `CREATE SCHEMA`
/// must. The engine creates a temporary namespace on the session's behalf.
/// A statement that names the namespace never creates it.
#[must_use]
pub fn create_temp_schema_op(name: &str) -> WriteOp {
    WriteOp::Put {
        key: schema_key(name),
        value: BOOTSTRAP_ROLE.as_bytes().to_vec(),
    }
}

const SCHEMA_PREFIX: &[u8] = b"\0\0\0\0catalog_schema/by-name/";
const TABLESPACE_PREFIX: &[u8] = b"\0\0\0\0catalog_tablespace/by-name/";
const RELATION_TABLESPACE_PREFIX: &[u8] = b"\0\0\0\0catalog_tablespace/by-relation/";
const RELATION_ACCESS_METHOD_PREFIX: &[u8] = b"\0\0\0\0catalog_access_method/by-relation/";
const NEXT_TABLESPACE_OID_KEY: &[u8] = b"\0\0\0\0meta/next_tablespace_oid";
const FIRST_USER_TABLESPACE_OID: u32 = 300_000;
const ACCESS_METHOD_PREFIX: &[u8] = b"\0\0\0\0catalog_access_method/by-name/";
const NEXT_ACCESS_METHOD_OID_KEY: &[u8] = b"\0\0\0\0meta/next_access_method_oid";
const FIRST_USER_ACCESS_METHOD_OID: u32 = 320_000;
const OPERATOR_FAMILY_PREFIX: &[u8] = b"\0\0\0\0catalog_operator_family/";
const OPERATOR_CLASS_PREFIX: &[u8] = b"\0\0\0\0catalog_operator_class/";
const OPERATOR_FAMILY_OPERATOR_PREFIX: &[u8] = b"\0\0\0\0catalog_operator_family_operator/";
const OPERATOR_FAMILY_FUNCTION_PREFIX: &[u8] = b"\0\0\0\0catalog_operator_family_function/";
const NEXT_OPERATOR_OBJECT_OID_KEY: &[u8] = b"\0\0\0\0meta/next_operator_object_oid";
const FIRST_USER_OPERATOR_OBJECT_OID: u32 = 310_000;

fn tablespace_key(name: &str) -> Vec<u8> {
    let mut key = TABLESPACE_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key
}

fn access_method_key(name: &str) -> Vec<u8> {
    let mut key = ACCESS_METHOD_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key
}

fn serialize_access_method(access_method: &AccessMethod) -> Vec<u8> {
    let mut bytes = access_method.oid.to_be_bytes().to_vec();
    bytes.push(match access_method.kind {
        AccessMethodKind::Index => b'i',
        AccessMethodKind::Table => b't',
    });
    bytes.extend_from_slice(access_method.handler.as_bytes());
    bytes
}

fn deserialize_access_method(name: String, bytes: &[u8]) -> Result<AccessMethod, CatalogError> {
    let oid = u32::from_be_bytes(
        bytes
            .get(..4)
            .ok_or_else(|| KvError::CorruptRow("access method oid is missing".into()))?
            .try_into()
            .expect("4"),
    );
    let kind = match *bytes
        .get(4)
        .ok_or_else(|| KvError::CorruptRow("access method kind is missing".into()))?
    {
        b'i' => AccessMethodKind::Index,
        b't' => AccessMethodKind::Table,
        kind => {
            return Err(KvError::CorruptRow(format!("unknown access method kind {kind}")).into());
        }
    };
    let handler = String::from_utf8(bytes[5..].to_vec())
        .map_err(|_| KvError::CorruptRow("access method handler is not UTF-8".into()))?;
    Ok(AccessMethod {
        oid,
        name,
        kind,
        handler,
    })
}

fn relation_tablespace_key(relation: &RelationName) -> Vec<u8> {
    let mut out = RELATION_TABLESPACE_PREFIX.to_vec();
    key::push_key_part(&mut out, &relation.schema);
    key::push_key_part(&mut out, &relation.name);
    out
}

fn relation_access_method_key(relation: &RelationName) -> Vec<u8> {
    let mut out = RELATION_ACCESS_METHOD_PREFIX.to_vec();
    key::push_key_part(&mut out, &relation.schema);
    key::push_key_part(&mut out, &relation.name);
    out
}

/// Resolve a bootstrap or user-created tablespace name to its catalog oid.
///
/// # Errors
///
/// Returns undefined-object or catalog storage errors.
pub fn tablespace_oid(kv: &dyn Kv, name: &str) -> Result<u32, CatalogError> {
    match name {
        "pg_default" => Ok(0),
        "pg_global" => Ok(1664),
        _ => Ok(get_tablespace(kv, name)?.oid),
    }
}

/// Store the SQL-visible placement of a table or index.
#[must_use]
pub fn set_relation_tablespace_op(relation: &RelationName, oid: u32) -> WriteOp {
    if oid == 0 {
        WriteOp::Delete {
            key: relation_tablespace_key(relation),
        }
    } else {
        WriteOp::Put {
            key: relation_tablespace_key(relation),
            value: U32::new(oid).as_bytes().to_vec(),
        }
    }
}

/// Read a relation's placement; zero means the database default tablespace.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn relation_tablespace_oid(kv: &dyn Kv, relation: &RelationName) -> Result<u32, CatalogError> {
    kv.get(&relation_tablespace_key(relation))?
        .map_or(Ok(0), |bytes| {
            U32::read_from_prefix(&bytes)
                .map(|(oid, _)| oid.get())
                .map_err(|_| {
                    KvError::CorruptRow("relation tablespace oid is not u32".into()).into()
                })
        })
}

fn drop_relation_tablespace_op(relation: &RelationName) -> WriteOp {
    WriteOp::Delete {
        key: relation_tablespace_key(relation),
    }
}

/// Store an explicitly selected table access method for a relation.
#[must_use]
pub fn set_relation_access_method_op(relation: &RelationName, oid: u32) -> WriteOp {
    WriteOp::Put {
        key: relation_access_method_key(relation),
        value: U32::new(oid).as_bytes().to_vec(),
    }
}

/// Remove an explicit table access method and return to the relation default.
#[must_use]
pub fn clear_relation_access_method_op(relation: &RelationName) -> WriteOp {
    drop_relation_access_method_op(relation)
}

/// Read a relation's explicit table access method, if it has one.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn relation_access_method_oid(
    kv: &dyn Kv,
    relation: &RelationName,
) -> Result<Option<u32>, CatalogError> {
    kv.get(&relation_access_method_key(relation))?
        .map(|bytes| {
            U32::read_from_prefix(&bytes)
                .map(|(oid, _)| oid.get())
                .map_err(|_| {
                    KvError::CorruptRow("relation access method oid is not u32".into()).into()
                })
        })
        .transpose()
}

fn drop_relation_access_method_op(relation: &RelationName) -> WriteOp {
    WriteOp::Delete {
        key: relation_access_method_key(relation),
    }
}

fn operator_object_key(prefix: &[u8], method: &str, name: &RelationName) -> Vec<u8> {
    let mut out = prefix.to_vec();
    key::push_key_part(&mut out, method);
    key::push_key_part(&mut out, &name.schema);
    key::push_key_part(&mut out, &name.name);
    out
}

fn operator_object_bytes(oid: u32, owner: &str, fields: &[u32]) -> Vec<u8> {
    let mut out = U32::new(oid).as_bytes().to_vec();
    out.extend_from_slice(owner.as_bytes());
    out.push(0);
    for field in fields {
        out.extend_from_slice(U32::new(*field).as_bytes());
    }
    out
}

fn read_operator_object(bytes: &[u8]) -> Result<(u32, String, &[u8]), CatalogError> {
    let (oid, rest) = U32::read_from_prefix(bytes)
        .map_err(|_| KvError::CorruptRow("operator object oid is not u32".into()))?;
    let split = rest
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| KvError::CorruptRow("operator object owner is missing".into()))?;
    let owner = String::from_utf8(rest[..split].to_vec())
        .map_err(|_| KvError::CorruptRow("operator object owner is not UTF-8".into()))?;
    Ok((oid.get(), owner, &rest[split + 1..]))
}

fn operator_object_oid(kv: &dyn Kv) -> Result<u32, CatalogError> {
    kv.get(NEXT_OPERATOR_OBJECT_OID_KEY)?
        .map_or(Ok(FIRST_USER_OPERATOR_OBJECT_OID), |bytes| {
            U32::read_from_prefix(&bytes)
                .map(|(oid, _)| oid.get())
                .map_err(|_| {
                    KvError::CorruptRow("next operator object oid is not u32".into()).into()
                })
        })
}

/// Create an operator family and advance the shared operator-object oid cursor.
///
/// # Errors
///
/// Returns duplicate-object, catalog storage, or corruption errors.
pub fn create_operator_family_ops(
    kv: &dyn Kv,
    name: &RelationName,
    method: &str,
    owner: &str,
) -> Result<(OperatorFamily, Vec<WriteOp>), CatalogError> {
    let key = operator_object_key(OPERATOR_FAMILY_PREFIX, method, name);
    if kv.get(&key)?.is_some() {
        return Err(CatalogError::DuplicateObject(name.name.clone()));
    }
    let oid = operator_object_oid(kv)?;
    let family = OperatorFamily {
        oid,
        name: name.clone(),
        method: method.to_string(),
        owner: owner.to_string(),
    };
    Ok((
        family,
        vec![
            WriteOp::Put {
                key,
                value: operator_object_bytes(oid, owner, &[]),
            },
            WriteOp::Put {
                key: NEXT_OPERATOR_OBJECT_OID_KEY.to_vec(),
                value: U32::new(oid + 1).as_bytes().to_vec(),
            },
        ],
    ))
}

/// Create an operator class and its same-named implicit family when needed.
///
/// # Errors
///
/// Returns duplicate/undefined-object, catalog storage, or corruption errors.
#[expect(
    clippy::too_many_arguments,
    reason = "the PostgreSQL operator-class catalog tuple is the creation contract"
)]
pub fn create_operator_class_ops(
    kv: &dyn Kv,
    name: &RelationName,
    method: &str,
    owner: &str,
    family: Option<&RelationName>,
    input_type_oid: u32,
    default: bool,
    key_type_oid: u32,
) -> Result<(OperatorClass, Vec<WriteOp>), CatalogError> {
    let key = operator_object_key(OPERATOR_CLASS_PREFIX, method, name);
    if kv.get(&key)?.is_some() {
        return Err(CatalogError::DuplicateObject(name.name.clone()));
    }
    let family_name = family.unwrap_or(name);
    let family_key = operator_object_key(OPERATOR_FAMILY_PREFIX, method, family_name);
    let (family_oid, mut ops, oid) = if let Some(bytes) = kv.get(&family_key)? {
        (
            read_operator_object(&bytes)?.0,
            Vec::new(),
            operator_object_oid(kv)?,
        )
    } else if family.is_some() {
        return Err(CatalogError::UndefinedObject(family_name.name.clone()));
    } else {
        let family_oid = operator_object_oid(kv)?;
        (
            family_oid,
            vec![WriteOp::Put {
                key: family_key,
                value: operator_object_bytes(family_oid, owner, &[]),
            }],
            family_oid + 1,
        )
    };
    let class = OperatorClass {
        oid,
        name: name.clone(),
        method: method.to_string(),
        owner: owner.to_string(),
        family_oid,
        input_type_oid,
        default,
        key_type_oid,
    };
    ops.push(WriteOp::Put {
        key,
        value: operator_object_bytes(
            oid,
            owner,
            &[family_oid, input_type_oid, u32::from(default), key_type_oid],
        ),
    });
    ops.push(WriteOp::Put {
        key: NEXT_OPERATOR_OBJECT_OID_KEY.to_vec(),
        value: U32::new(oid + 1).as_bytes().to_vec(),
    });
    Ok((class, ops))
}

/// List user-defined operator families.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn list_operator_families(kv: &dyn Kv) -> Result<Vec<OperatorFamily>, CatalogError> {
    let mut out = Vec::new();
    for (key, bytes) in kv.scan_prefix(OPERATOR_FAMILY_PREFIX)? {
        let parts = key::key_parts(&key[OPERATOR_FAMILY_PREFIX.len()..], 3)
            .ok_or_else(|| KvError::CorruptRow("operator family key is incomplete".into()))?;
        let (oid, owner, _) = read_operator_object(&bytes)?;
        out.push(OperatorFamily {
            oid,
            method: parts[0].to_string(),
            name: RelationName::new(parts[1], parts[2]),
            owner,
        });
    }
    out.sort_by_key(|family| family.oid);
    Ok(out)
}

/// List user-defined operator classes.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn list_operator_classes(kv: &dyn Kv) -> Result<Vec<OperatorClass>, CatalogError> {
    let mut out = Vec::new();
    for (key, bytes) in kv.scan_prefix(OPERATOR_CLASS_PREFIX)? {
        let parts = key::key_parts(&key[OPERATOR_CLASS_PREFIX.len()..], 3)
            .ok_or_else(|| KvError::CorruptRow("operator class key is incomplete".into()))?;
        let (oid, owner, fields) = read_operator_object(&bytes)?;
        let mut fields = fields;
        let mut next = || {
            let (value, rest) = U32::read_from_prefix(fields)
                .map_err(|_| KvError::CorruptRow("operator class fields are incomplete".into()))?;
            fields = rest;
            Ok::<_, KvError>(value.get())
        };
        out.push(OperatorClass {
            oid,
            method: parts[0].to_string(),
            name: RelationName::new(parts[1], parts[2]),
            owner,
            family_oid: next()?,
            input_type_oid: next()?,
            default: next()? != 0,
            key_type_oid: next()?,
        });
    }
    out.sort_by_key(|class| class.oid);
    Ok(out)
}

/// Resolve one user-defined operator family.
///
/// # Errors
///
/// Returns undefined-object, catalog storage, or corruption errors.
pub fn get_operator_family(
    kv: &dyn Kv,
    name: &RelationName,
    method: &str,
) -> Result<OperatorFamily, CatalogError> {
    let bytes = kv
        .get(&operator_object_key(OPERATOR_FAMILY_PREFIX, method, name))?
        .ok_or_else(|| CatalogError::UndefinedObject(name.name.clone()))?;
    let (oid, owner, _) = read_operator_object(&bytes)?;
    Ok(OperatorFamily {
        oid,
        name: name.clone(),
        method: method.to_string(),
        owner,
    })
}

fn operator_family_member_key(family_oid: u32, member: OperatorFamilyMemberKey) -> Vec<u8> {
    let (prefix, number, left_type_oid, right_type_oid) = match member {
        OperatorFamilyMemberKey::Operator {
            number,
            left_type_oid,
            right_type_oid,
        } => (
            OPERATOR_FAMILY_OPERATOR_PREFIX,
            number,
            left_type_oid,
            right_type_oid,
        ),
        OperatorFamilyMemberKey::Function {
            number,
            left_type_oid,
            right_type_oid,
        } => (
            OPERATOR_FAMILY_FUNCTION_PREFIX,
            number,
            left_type_oid,
            right_type_oid,
        ),
    };
    let mut key = prefix.to_vec();
    for part in [
        family_oid.to_string(),
        number.to_string(),
        left_type_oid.to_string(),
        right_type_oid.to_string(),
    ] {
        key::push_key_part(&mut key, &part);
    }
    key
}

fn operator_family_member_identity(member: &OperatorFamilyMember) -> OperatorFamilyMemberKey {
    match member {
        OperatorFamilyMember::Operator {
            number,
            left_type_oid,
            right_type_oid,
            ..
        } => OperatorFamilyMemberKey::Operator {
            number: *number,
            left_type_oid: *left_type_oid,
            right_type_oid: *right_type_oid,
        },
        OperatorFamilyMember::Function {
            number,
            left_type_oid,
            right_type_oid,
            ..
        } => OperatorFamilyMemberKey::Function {
            number: *number,
            left_type_oid: *left_type_oid,
            right_type_oid: *right_type_oid,
        },
    }
}

/// Whether an operator or support-function slot exists in a family.
///
/// # Errors
///
/// Returns catalog storage errors.
pub fn operator_family_member_exists(
    kv: &dyn Kv,
    family_oid: u32,
    member: OperatorFamilyMemberKey,
) -> Result<bool, CatalogError> {
    Ok(kv
        .get(&operator_family_member_key(family_oid, member))?
        .is_some())
}

/// List every durable user-defined operator-family member with its family oid.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn list_operator_family_members(
    kv: &dyn Kv,
) -> Result<Vec<(u32, OperatorFamilyMember)>, CatalogError> {
    let mut out = Vec::new();
    for (prefix, operator) in [
        (OPERATOR_FAMILY_OPERATOR_PREFIX, true),
        (OPERATOR_FAMILY_FUNCTION_PREFIX, false),
    ] {
        for (key, value) in kv.scan_prefix(prefix)? {
            let parts = key::key_parts(&key[prefix.len()..], 4).ok_or_else(|| {
                KvError::CorruptRow("operator family member key is incomplete".into())
            })?;
            let parse = |part: &str| -> Result<u32, CatalogError> {
                part.parse::<u32>().map_err(|_| {
                    KvError::CorruptRow("operator family member key is invalid".into()).into()
                })
            };
            let family_oid = parse(parts[0])?;
            let number = u16::try_from(parse(parts[1])?).map_err(|_| {
                KvError::CorruptRow("operator family member number is invalid".into())
            })?;
            let left_type_oid = parse(parts[2])?;
            let right_type_oid = parse(parts[3])?;
            let separator = value.iter().position(|byte| *byte == 0).ok_or_else(|| {
                KvError::CorruptRow("operator family member value is incomplete".into())
            })?;
            let name = std::str::from_utf8(&value[..separator])
                .map_err(|_| KvError::CorruptRow("operator family member name is invalid".into()))?
                .to_string();
            let mut trailing = &value[separator + 1..];
            let member = if operator {
                let (order_family_oid, rest) = U32::read_from_prefix(trailing).map_err(|_| {
                    KvError::CorruptRow("operator family member oid is invalid".into())
                })?;
                if !rest.is_empty() {
                    return Err(KvError::CorruptRow(
                        "operator family member value has trailing data".into(),
                    )
                    .into());
                }
                OperatorFamilyMember::Operator {
                    number,
                    operator: name,
                    left_type_oid,
                    right_type_oid,
                    order_family_oid: order_family_oid.get(),
                }
            } else {
                let mut argument_type_oids = Vec::new();
                while !trailing.is_empty() {
                    let (oid, rest) = U32::read_from_prefix(trailing).map_err(|_| {
                        KvError::CorruptRow("operator family member oid is invalid".into())
                    })?;
                    argument_type_oids.push(oid.get());
                    trailing = rest;
                }
                OperatorFamilyMember::Function {
                    number,
                    function: name,
                    left_type_oid,
                    right_type_oid,
                    argument_type_oids,
                }
            };
            out.push((family_oid, member));
        }
    }
    Ok(out)
}

/// Add durable operator and support-function members to one family atomically.
///
/// The family is named by oid rather than by record because a member may join a
/// family `PostgreSQL` ships. Those have no row here — the built-in fixture is
/// their whole definition — so there is nothing to re-read, and the caller has
/// already resolved the oid it passes.
///
/// # Errors
///
/// Returns duplicate-object, catalog storage, or corruption errors.
pub fn add_operator_family_members_ops(
    kv: &dyn Kv,
    family_oid: u32,
    members: &[OperatorFamilyMember],
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut identities = HashSet::new();
    let mut ops = Vec::with_capacity(members.len());
    for member in members {
        let identity = operator_family_member_identity(member);
        let key = operator_family_member_key(family_oid, identity);
        if !identities.insert(identity) || kv.get(&key)?.is_some() {
            return Err(CatalogError::DuplicateObject(format!(
                "operator family member {identity:?}"
            )));
        }
        let value = match member {
            OperatorFamilyMember::Operator {
                operator,
                order_family_oid,
                ..
            } => {
                let mut value = operator.as_bytes().to_vec();
                value.push(0);
                value.extend_from_slice(U32::new(*order_family_oid).as_bytes());
                value
            }
            OperatorFamilyMember::Function {
                function,
                argument_type_oids,
                ..
            } => {
                let mut value = function.as_bytes().to_vec();
                value.push(0);
                for oid in argument_type_oids {
                    value.extend_from_slice(U32::new(*oid).as_bytes());
                }
                value
            }
        };
        ops.push(WriteOp::Put { key, value });
    }
    Ok(ops)
}

/// Drop existing family members atomically.
///
/// The family is named by oid for the reason
/// [`add_operator_family_members_ops`] gives.
///
/// # Errors
///
/// Returns undefined-object, catalog storage, or corruption errors.
pub fn drop_operator_family_members_ops(
    kv: &dyn Kv,
    family_oid: u32,
    members: &[OperatorFamilyMemberKey],
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut identities = HashSet::new();
    let mut ops = Vec::with_capacity(members.len());
    for member in members {
        let key = operator_family_member_key(family_oid, *member);
        if !identities.insert(*member) || kv.get(&key)?.is_none() {
            return Err(CatalogError::UndefinedObject(format!(
                "operator family member {member:?}"
            )));
        }
        ops.push(WriteOp::Delete { key });
    }
    Ok(ops)
}

fn drop_operator_family_member_ops(
    kv: &dyn Kv,
    family_oid: u32,
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut ops = Vec::new();
    let family_oid = family_oid.to_string();
    for prefix in [
        OPERATOR_FAMILY_OPERATOR_PREFIX,
        OPERATOR_FAMILY_FUNCTION_PREFIX,
    ] {
        for (key, _) in kv.scan_prefix(prefix)? {
            let parts = key::key_parts(&key[prefix.len()..], 4).ok_or_else(|| {
                KvError::CorruptRow("operator family member key is incomplete".into())
            })?;
            if parts[0] == family_oid {
                ops.push(WriteOp::Delete { key });
            }
        }
    }
    Ok(ops)
}

/// Resolve one user-defined operator class.
///
/// # Errors
///
/// Returns undefined-object, catalog storage, or corruption errors.
pub fn get_operator_class(
    kv: &dyn Kv,
    name: &RelationName,
    method: &str,
) -> Result<OperatorClass, CatalogError> {
    list_operator_classes(kv)?
        .into_iter()
        .find(|class| class.method == method && class.name == *name)
        .ok_or_else(|| CatalogError::UndefinedObject(name.name.clone()))
}

/// Replace a user-defined operator family, preserving its oid.
///
/// # Errors
///
/// Returns undefined/duplicate-object, catalog storage, or corruption errors.
pub fn replace_operator_family_ops(
    kv: &dyn Kv,
    old_name: &RelationName,
    family: &OperatorFamily,
) -> Result<Vec<WriteOp>, CatalogError> {
    get_operator_family(kv, old_name, &family.method)?;
    let old_key = operator_object_key(OPERATOR_FAMILY_PREFIX, &family.method, old_name);
    let new_key = operator_object_key(OPERATOR_FAMILY_PREFIX, &family.method, &family.name);
    if old_key != new_key && kv.get(&new_key)?.is_some() {
        return Err(CatalogError::DuplicateObject(family.name.name.clone()));
    }
    let mut ops = if old_key == new_key {
        Vec::new()
    } else {
        vec![WriteOp::Delete { key: old_key }]
    };
    ops.push(WriteOp::Put {
        key: new_key,
        value: operator_object_bytes(family.oid, &family.owner, &[]),
    });
    Ok(ops)
}

/// Replace a user-defined operator class, preserving its oid and family link.
///
/// # Errors
///
/// Returns undefined/duplicate-object, catalog storage, or corruption errors.
pub fn replace_operator_class_ops(
    kv: &dyn Kv,
    old_name: &RelationName,
    class: &OperatorClass,
) -> Result<Vec<WriteOp>, CatalogError> {
    get_operator_class(kv, old_name, &class.method)?;
    let old_key = operator_object_key(OPERATOR_CLASS_PREFIX, &class.method, old_name);
    let new_key = operator_object_key(OPERATOR_CLASS_PREFIX, &class.method, &class.name);
    if old_key != new_key && kv.get(&new_key)?.is_some() {
        return Err(CatalogError::DuplicateObject(class.name.name.clone()));
    }
    let mut ops = if old_key == new_key {
        Vec::new()
    } else {
        vec![WriteOp::Delete { key: old_key }]
    };
    ops.push(WriteOp::Put {
        key: new_key,
        value: operator_object_bytes(
            class.oid,
            &class.owner,
            &[
                class.family_oid,
                class.input_type_oid,
                u32::from(class.default),
                class.key_type_oid,
            ],
        ),
    });
    Ok(ops)
}

/// Drop one user-defined operator class.
///
/// # Errors
///
/// Returns undefined-object, catalog storage, or corruption errors.
pub fn drop_operator_class_ops(
    kv: &dyn Kv,
    name: &RelationName,
    method: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    get_operator_class(kv, name, method)?;
    Ok(vec![WriteOp::Delete {
        key: operator_object_key(OPERATOR_CLASS_PREFIX, method, name),
    }])
}

/// Drop one user-defined operator family and optionally its dependent classes.
///
/// # Errors
///
/// Returns undefined/dependent-object, catalog storage, or corruption errors.
pub fn drop_operator_family_ops(
    kv: &dyn Kv,
    name: &RelationName,
    method: &str,
    cascade: bool,
) -> Result<Vec<WriteOp>, CatalogError> {
    let family = get_operator_family(kv, name, method)?;
    let classes: Vec<_> = list_operator_classes(kv)?
        .into_iter()
        .filter(|class| class.family_oid == family.oid)
        .collect();
    if !cascade && !classes.is_empty() {
        return Err(CatalogError::OperatorFamilyNotEmpty(name.name.clone()));
    }
    let mut ops = classes
        .into_iter()
        .map(|class| WriteOp::Delete {
            key: operator_object_key(OPERATOR_CLASS_PREFIX, &class.method, &class.name),
        })
        .collect::<Vec<_>>();
    ops.extend(drop_operator_family_member_ops(kv, family.oid)?);
    ops.push(WriteOp::Delete {
        key: operator_object_key(OPERATOR_FAMILY_PREFIX, method, name),
    });
    Ok(ops)
}

/// One user-defined operator, as `pg_operator` describes it.
///
/// The field names are `PostgreSQL`'s column names without the `opr` prefix,
/// because this row *is* the catalog tuple: `CREATE OPERATOR` writes it whole
/// and `pg_operator` projects it whole. A `0` oid means "no such object" in
/// every position, exactly as `InvalidOid` does upstream.
///
/// The links are stored as oids and not as names. `DROP OPERATOR` must find
/// every reference to the operator it removes, and an oid is the only thing
/// that a rename or a schema change cannot invalidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserOperator {
    /// `0` asks [`put_user_operator_ops`] to allocate one.
    pub oid: u32,
    /// The namespace the operator lives in. Always resolved; never a written
    /// qualifier.
    pub schema: String,
    /// The symbol, without a qualifier: `===`.
    pub symbol: String,
    pub owner: String,
    /// `b` for an infix operator, `l` for a prefix one. `PostgreSQL` 14 removed
    /// postfix operators, so `r` can no longer be written.
    pub kind: char,
    /// `0` for a prefix operator, which has no left operand.
    pub left_type_oid: u32,
    pub right_type_oid: u32,
    pub result_type_oid: u32,
    /// `pg_proc.oid` of the function that implements the operator.
    pub code_oid: u32,
    pub commutator_oid: u32,
    pub negator_oid: u32,
    pub restrict_oid: u32,
    pub join_oid: u32,
    pub can_merge: bool,
    pub can_hash: bool,
}

const USER_OPERATOR_PREFIX: &[u8] = b"\0\0\0\0catalog_operator/";

/// The identity of an operator: its namespace, its symbol, and both operand
/// types, because a symbol alone is overloaded.
///
/// The operand oids go in as decimal text so the key stays one
/// [`key::push_key_part`] list that [`key::key_parts`] can split back apart.
fn user_operator_key(schema: &str, symbol: &str, left: u32, right: u32) -> Vec<u8> {
    let mut out = USER_OPERATOR_PREFIX.to_vec();
    key::push_key_part(&mut out, schema);
    key::push_key_part(&mut out, symbol);
    key::push_key_part(&mut out, &left.to_string());
    key::push_key_part(&mut out, &right.to_string());
    out
}

/// The fields of a stored operator, in the order [`read_user_operator`] reads
/// them back.
fn user_operator_fields(operator: &UserOperator) -> [u32; 11] {
    [
        u32::from(operator.kind),
        operator.left_type_oid,
        operator.right_type_oid,
        operator.result_type_oid,
        operator.code_oid,
        operator.commutator_oid,
        operator.negator_oid,
        operator.restrict_oid,
        operator.join_oid,
        u32::from(operator.can_merge),
        u32::from(operator.can_hash),
    ]
}

fn read_user_operator(
    schema: &str,
    symbol: &str,
    bytes: &[u8],
) -> Result<UserOperator, CatalogError> {
    let (oid, owner, mut fields) = read_operator_object(bytes)?;
    let mut next = || {
        let (value, rest) = U32::read_from_prefix(fields)
            .map_err(|_| KvError::CorruptRow("operator fields are incomplete".into()))?;
        fields = rest;
        Ok::<_, KvError>(value.get())
    };
    let kind = char::from_u32(next()?)
        .ok_or_else(|| KvError::CorruptRow("operator kind is not a character".into()))?;
    Ok(UserOperator {
        oid,
        schema: schema.to_string(),
        symbol: symbol.to_string(),
        owner,
        kind,
        left_type_oid: next()?,
        right_type_oid: next()?,
        result_type_oid: next()?,
        code_oid: next()?,
        commutator_oid: next()?,
        negator_oid: next()?,
        restrict_oid: next()?,
        join_oid: next()?,
        can_merge: next()? != 0,
        can_hash: next()? != 0,
    })
}

/// The oid a new operator will carry, and the op that advances the cursor.
///
/// The oid is handed out *before* the row is built, and not allocated by the
/// write, because `CREATE OPERATOR === (…, COMMUTATOR = ===)` has to store the
/// operator's own oid inside its own tuple. `PostgreSQL` reaches that state by
/// inserting the row and then updating it; a write batch has no "then", so the
/// oid is read out first and the row is written once.
///
/// The cursor is the one operator classes and families already draw on.
/// Sharing it is what keeps a user operator's oid distinct from every other
/// object this catalog allocates.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn allocate_user_operator_oid(kv: &dyn Kv) -> Result<(u32, WriteOp), CatalogError> {
    let oid = operator_object_oid(kv)?;
    Ok((
        oid,
        WriteOp::Put {
            key: NEXT_OPERATOR_OBJECT_OID_KEY.to_vec(),
            value: U32::new(oid + 1).as_bytes().to_vec(),
        },
    ))
}

/// Store `operator` whole.
///
/// The same call creates an operator and rewrites one whose commutator or
/// negator link changed, because `PostgreSQL`'s `OperatorUpd` rewrites the
/// whole tuple too.
#[must_use]
pub fn put_user_operator_ops(operator: &UserOperator) -> Vec<WriteOp> {
    vec![WriteOp::Put {
        key: user_operator_key(
            &operator.schema,
            &operator.symbol,
            operator.left_type_oid,
            operator.right_type_oid,
        ),
        value: operator_object_bytes(
            operator.oid,
            &operator.owner,
            &user_operator_fields(operator),
        ),
    }]
}

/// Remove one user-defined operator. Its back-links are the caller's business.
#[must_use]
pub fn drop_user_operator_ops(operator: &UserOperator) -> Vec<WriteOp> {
    vec![WriteOp::Delete {
        key: user_operator_key(
            &operator.schema,
            &operator.symbol,
            operator.left_type_oid,
            operator.right_type_oid,
        ),
    }]
}

/// Every user-defined operator, in oid order.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn list_user_operators(kv: &dyn Kv) -> Result<Vec<UserOperator>, CatalogError> {
    let mut out = Vec::new();
    for (key, bytes) in kv.scan_prefix(USER_OPERATOR_PREFIX)? {
        let parts = key::key_parts(&key[USER_OPERATOR_PREFIX.len()..], 4)
            .ok_or_else(|| KvError::CorruptRow("operator key is incomplete".into()))?;
        out.push(read_user_operator(parts[0], parts[1], &bytes)?);
    }
    out.sort_by_key(|operator| operator.oid);
    Ok(out)
}

/// One user-defined operator by its full identity, or `None`.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn get_user_operator(
    kv: &dyn Kv,
    schema: &str,
    symbol: &str,
    left_type_oid: u32,
    right_type_oid: u32,
) -> Result<Option<UserOperator>, CatalogError> {
    let key = user_operator_key(schema, symbol, left_type_oid, right_type_oid);
    kv.get(&key)?
        .map(|bytes| read_user_operator(schema, symbol, &bytes))
        .transpose()
}

fn serialize_tablespace(tablespace: &Tablespace) -> Vec<u8> {
    let mut bytes = U32::new(tablespace.oid).as_bytes().to_vec();
    for field in [&tablespace.owner, &tablespace.location] {
        bytes.extend_from_slice(field.as_bytes());
        bytes.push(0);
    }
    for (name, value) in &tablespace.options {
        bytes.extend_from_slice(name.as_bytes());
        bytes.push(b'=');
        bytes.extend_from_slice(value.as_bytes());
        bytes.push(0);
    }
    bytes
}

fn deserialize_tablespace(name: String, bytes: &[u8]) -> Result<Tablespace, CatalogError> {
    let (oid, fields) = U32::read_from_prefix(bytes)
        .map_err(|_| KvError::CorruptRow("tablespace oid is not u32".into()))?;
    let mut fields = fields.split(|byte| *byte == 0);
    let owner = fields
        .next()
        .ok_or_else(|| KvError::CorruptRow("tablespace owner is missing".into()))?;
    let location = fields
        .next()
        .ok_or_else(|| KvError::CorruptRow("tablespace location is missing".into()))?;
    let utf8 = |field: &[u8]| {
        String::from_utf8(field.to_vec()).map_err(|_| {
            CatalogError::Storage(KvError::CorruptRow("non-UTF-8 tablespace row".into()))
        })
    };
    let mut options = Vec::new();
    for field in fields.filter(|field| !field.is_empty()) {
        let Some(separator) = field.iter().position(|byte| *byte == b'=') else {
            return Err(KvError::CorruptRow("tablespace option has no value".into()).into());
        };
        options.push((utf8(&field[..separator])?, utf8(&field[separator + 1..])?));
    }
    Ok(Tablespace {
        oid: oid.get(),
        name,
        owner: utf8(owner)?,
        location: utf8(location)?,
        options,
    })
}

/// Build the atomic catalog batch for a user-created tablespace.
///
/// # Errors
///
/// Returns duplicate-object or catalog storage errors.
pub fn create_tablespace_ops(
    kv: &dyn Kv,
    name: &str,
    owner: &str,
    location: &str,
    options: Vec<(String, String)>,
) -> Result<Vec<WriteOp>, CatalogError> {
    if matches!(name, "pg_default" | "pg_global") || kv.get(&tablespace_key(name))?.is_some() {
        return Err(CatalogError::DuplicateObject(name.to_string()));
    }
    let oid = match kv.get(NEXT_TABLESPACE_OID_KEY)? {
        Some(bytes) => U32::read_from_prefix(bytes.as_slice())
            .map_err(|_| KvError::CorruptRow("next tablespace oid is not u32".into()))?
            .0
            .get(),
        None => FIRST_USER_TABLESPACE_OID,
    };
    let tablespace = Tablespace {
        oid,
        name: name.to_string(),
        owner: owner.to_string(),
        location: location.to_string(),
        options,
    };
    Ok(vec![
        WriteOp::Put {
            key: tablespace_key(name),
            value: serialize_tablespace(&tablespace),
        },
        WriteOp::Put {
            key: NEXT_TABLESPACE_OID_KEY.to_vec(),
            value: U32::new(oid + 1).as_bytes().to_vec(),
        },
    ])
}

/// List user-created tablespaces in oid order.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn list_tablespaces(kv: &dyn Kv) -> Result<Vec<Tablespace>, CatalogError> {
    let mut tablespaces = Vec::new();
    for (key, value) in kv.scan_prefix(TABLESPACE_PREFIX)? {
        let name = String::from_utf8(key[TABLESPACE_PREFIX.len()..].to_vec())
            .map_err(|_| KvError::CorruptRow("non-UTF-8 tablespace name".into()))?;
        tablespaces.push(deserialize_tablespace(name, &value)?);
    }
    tablespaces.sort_by_key(|tablespace| tablespace.oid);
    Ok(tablespaces)
}

/// Build the atomic catalog batch for a user-created access method.
///
/// # Errors
///
/// Returns duplicate-object or catalog storage errors.
pub fn create_access_method_ops(
    kv: &dyn Kv,
    name: &str,
    kind: AccessMethodKind,
    handler: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    if kv.get(&access_method_key(name))?.is_some() {
        return Err(CatalogError::DuplicateObject(name.to_string()));
    }
    let oid = match kv.get(NEXT_ACCESS_METHOD_OID_KEY)? {
        Some(bytes) => U32::read_from_prefix(bytes.as_slice())
            .map_err(|_| KvError::CorruptRow("next access method oid is not u32".into()))?
            .0
            .get(),
        None => FIRST_USER_ACCESS_METHOD_OID,
    };
    let access_method = AccessMethod {
        oid,
        name: name.to_string(),
        kind,
        handler: handler.to_string(),
    };
    Ok(vec![
        WriteOp::Put {
            key: access_method_key(name),
            value: serialize_access_method(&access_method),
        },
        WriteOp::Put {
            key: NEXT_ACCESS_METHOD_OID_KEY.to_vec(),
            value: U32::new(oid + 1).as_bytes().to_vec(),
        },
    ])
}

/// List user-created access methods in oid order.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn list_access_methods(kv: &dyn Kv) -> Result<Vec<AccessMethod>, CatalogError> {
    let mut methods = Vec::new();
    for (key, value) in kv.scan_prefix(ACCESS_METHOD_PREFIX)? {
        let name = String::from_utf8(key[ACCESS_METHOD_PREFIX.len()..].to_vec())
            .map_err(|_| KvError::CorruptRow("non-UTF-8 access method name".into()))?;
        methods.push(deserialize_access_method(name, &value)?);
    }
    methods.sort_by_key(|method| method.oid);
    Ok(methods)
}

/// Read one user-created access method by name.
///
/// # Errors
///
/// Returns undefined-object or catalog storage errors.
pub fn get_access_method(kv: &dyn Kv, name: &str) -> Result<AccessMethod, CatalogError> {
    let bytes = kv
        .get(&access_method_key(name))?
        .ok_or_else(|| CatalogError::UndefinedObject(name.to_string()))?;
    deserialize_access_method(name.to_string(), &bytes)
}

/// Read a user-created tablespace by name.
///
/// # Errors
///
/// Returns undefined-object or catalog storage errors.
pub fn get_tablespace(kv: &dyn Kv, name: &str) -> Result<Tablespace, CatalogError> {
    let bytes = kv
        .get(&tablespace_key(name))?
        .ok_or_else(|| CatalogError::UndefinedObject(name.to_string()))?;
    deserialize_tablespace(name.to_string(), &bytes)
}

/// Replace a user-created tablespace, optionally changing its name.
///
/// # Errors
///
/// Returns undefined/duplicate-object or catalog storage errors.
pub fn replace_tablespace_ops(
    kv: &dyn Kv,
    old_name: &str,
    tablespace: &Tablespace,
) -> Result<Vec<WriteOp>, CatalogError> {
    get_tablespace(kv, old_name)?;
    if tablespace.name != old_name
        && (matches!(tablespace.name.as_str(), "pg_default" | "pg_global")
            || kv.get(&tablespace_key(&tablespace.name))?.is_some())
    {
        return Err(CatalogError::DuplicateObject(tablespace.name.clone()));
    }
    let mut ops = Vec::new();
    if tablespace.name != old_name {
        ops.push(WriteOp::Delete {
            key: tablespace_key(old_name),
        });
    }
    ops.push(WriteOp::Put {
        key: tablespace_key(&tablespace.name),
        value: serialize_tablespace(tablespace),
    });
    Ok(ops)
}

/// Build the catalog batch that drops a user-created tablespace.
///
/// # Errors
///
/// Returns undefined-object or catalog storage errors. Bootstrap tablespaces
/// cannot be dropped.
pub fn drop_tablespace_ops(kv: &dyn Kv, name: &str) -> Result<Vec<WriteOp>, CatalogError> {
    if matches!(name, "pg_default" | "pg_global") {
        return Err(CatalogError::UndefinedObject(name.to_string()));
    }
    let tablespace = get_tablespace(kv, name)?;
    for (_, bytes) in kv.scan_prefix(RELATION_TABLESPACE_PREFIX)? {
        let oid = U32::read_from_prefix(&bytes)
            .map(|(oid, _)| oid.get())
            .map_err(|_| KvError::CorruptRow("relation tablespace oid is not u32".into()))?;
        if oid == tablespace.oid {
            return Err(CatalogError::TablespaceNotEmpty(name.to_string()));
        }
    }
    Ok(vec![WriteOp::Delete {
        key: tablespace_key(name),
    }])
}

/// Tombstones for dropped [`BOOTSTRAP_SCHEMAS`].
///
/// The catalog synthesises a bootstrap schema rather than storing it, so it
/// must record the absence instead. Only `public` can be dropped, so only
/// `public` ever reaches this prefix.
const DROPPED_SCHEMA_PREFIX: &[u8] = b"\0\0\0\0catalog_schema/dropped/";

/// Key for a relation's stored schema record.
fn catalog_key(relation: &RelationName) -> Vec<u8> {
    key::catalog_key(&relation.schema, &relation.name)
}

/// The durable create order shared by relations and user types.
const CREATION_ORDER_PREFIX: &[u8] = b"\0\0\0\0catalog_creation_order/";

fn creation_order_key(name: &RelationName) -> Vec<u8> {
    let mut key = CREATION_ORDER_PREFIX.to_vec();
    key::push_key_part(&mut key, &name.schema);
    key::push_key_part(&mut key, &name.name);
    key
}

fn next_creation_order_key() -> Vec<u8> {
    b"\0\0\0\0meta/next_creation_order".to_vec()
}

fn creation_order_ops(kv: &dyn Kv, name: &RelationName) -> Result<Vec<WriteOp>, CatalogError> {
    if kv.get(&creation_order_key(name))?.is_some() {
        return Ok(Vec::new());
    }
    let next = match kv.get(&next_creation_order_key())? {
        Some(bytes) => U64::read_from_prefix(bytes.as_slice())
            .map_err(|_| KvError::CorruptRow("next_creation_order is not u64".into()))?
            .0
            .get(),
        None => 1,
    };
    Ok(vec![
        WriteOp::Put {
            key: creation_order_key(name),
            value: U64::new(next).as_bytes().to_vec(),
        },
        WriteOp::Put {
            key: next_creation_order_key(),
            value: U64::new(next + 1).as_bytes().to_vec(),
        },
    ])
}

/// The durable creation order of `name`, if this catalog predates the index.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn creation_order(kv: &dyn Kv, name: &RelationName) -> Result<Option<u64>, CatalogError> {
    Ok(kv
        .get(&creation_order_key(name))?
        .map(|bytes| {
            U64::read_from_prefix(bytes.as_slice())
                .map_err(|_| KvError::CorruptRow("creation_order is not u64".into()))
                .map(|(order, _)| order.get())
        })
        .transpose()?)
}

fn drop_creation_order_op(name: &RelationName) -> WriteOp {
    WriteOp::Delete {
        key: creation_order_key(name),
    }
}

fn move_creation_order_ops(
    kv: &dyn Kv,
    name: &RelationName,
    new_name: &RelationName,
) -> Result<Vec<WriteOp>, CatalogError> {
    let Some(value) = kv.get(&creation_order_key(name))? else {
        return Ok(Vec::new());
    };
    Ok(vec![
        drop_creation_order_op(name),
        WriteOp::Put {
            key: creation_order_key(new_name),
            value,
        },
    ])
}

/// Key for a relation's optional sharding strategy.
fn sharding_key(relation: &RelationName) -> Vec<u8> {
    key::catalog_sharding_key(&relation.schema, &relation.name)
}

const REPLICA_IDENTITY_PREFIX: &[u8] = b"\0\0\0\0catalog_replica_identity/";
const TYPED_TABLE_PREFIX: &[u8] = b"\0\0\0\0catalog_typed_table/";

fn replica_identity_key(table_id: TableId) -> Vec<u8> {
    let mut key = REPLICA_IDENTITY_PREFIX.to_vec();
    key.extend_from_slice(&table_id.to_be_bytes());
    key
}

fn typed_table_key(relation: &RelationName) -> Vec<u8> {
    let mut out = TYPED_TABLE_PREFIX.to_vec();
    key::push_key_part(&mut out, &relation.schema);
    key::push_key_part(&mut out, &relation.name);
    out
}

/// Record that `relation` was created `OF` the composite type with `oid`.
#[must_use]
pub fn set_typed_table_type_op(relation: &RelationName, oid: u32) -> WriteOp {
    WriteOp::Put {
        key: typed_table_key(relation),
        value: U32::new(oid).as_bytes().to_vec(),
    }
}

/// Clear a relation's `OF composite_type` association.
#[must_use]
pub fn clear_typed_table_type_op(relation: &RelationName) -> WriteOp {
    WriteOp::Delete {
        key: typed_table_key(relation),
    }
}

/// The composite type a typed table was declared `OF`, if any.
///
/// # Errors
///
/// Returns storage or malformed-catalog errors.
pub fn typed_table_type(kv: &dyn Kv, relation: &RelationName) -> Result<Option<u32>, CatalogError> {
    let Some(bytes) = kv.get(&typed_table_key(relation))? else {
        return Ok(None);
    };
    let (oid, rest) = U32::read_from_prefix(&bytes)
        .map_err(|_| KvError::CorruptRow("typed table oid is not u32".into()))?;
    if !rest.is_empty() {
        return Err(KvError::CorruptRow("typed table oid has trailing bytes".into()).into());
    }
    Ok(Some(oid.get()))
}

/// Return a table's replica identity, with a missing record meaning `DEFAULT`.
///
/// # Errors
///
/// Returns storage or corruption errors from the catalog KV seam.
pub fn replica_identity(kv: &dyn Kv, table_id: TableId) -> Result<ReplicaIdentity, CatalogError> {
    let Some(bytes) = kv.get(&replica_identity_key(table_id))? else {
        return Ok(ReplicaIdentity::Default);
    };
    match bytes.as_slice() {
        b"f" => Ok(ReplicaIdentity::Full),
        b"n" => Ok(ReplicaIdentity::Nothing),
        [b'i', name @ ..] => String::from_utf8(name.to_vec())
            .map(ReplicaIdentity::Index)
            .map_err(|_| {
                CatalogError::Storage(KvError::CorruptRow(
                    "invalid replica identity index name".into(),
                ))
            }),
        _ => Err(CatalogError::Storage(KvError::CorruptRow(
            "invalid replica identity record".into(),
        ))),
    }
}

/// Build the catalog write that records a table's replica identity.
#[must_use]
pub fn set_replica_identity_ops(table_id: TableId, identity: &ReplicaIdentity) -> Vec<WriteOp> {
    let key = replica_identity_key(table_id);
    match identity {
        ReplicaIdentity::Default => vec![WriteOp::Delete { key }],
        ReplicaIdentity::Full => vec![WriteOp::Put {
            key,
            value: b"f".to_vec(),
        }],
        ReplicaIdentity::Nothing => vec![WriteOp::Put {
            key,
            value: b"n".to_vec(),
        }],
        ReplicaIdentity::Index(name) => {
            let mut value = Vec::with_capacity(name.len() + 1);
            value.push(b'i');
            value.extend_from_slice(name.as_bytes());
            vec![WriteOp::Put { key, value }]
        }
    }
}

/// The id-keyed index entry naming `relation`.
fn catalog_by_id_op(table_id: TableId, relation: &RelationName) -> WriteOp {
    let mut value = Vec::new();
    key::push_key_part(&mut value, &relation.schema);
    key::push_key_part(&mut value, &relation.name);
    WriteOp::Put {
        key: key::catalog_by_id_key(table_id),
        value,
    }
}

/// The relation `table_id` names, or `None` when no table carries that id.
///
/// A range RPC ships a table id rather than a relation name. A name is
/// session-dependent once `search_path` and `pg_temp` exist, and the receiving
/// node has no notion of the originating session. This function serves that
/// RPC. It reads an id-keyed index entry that the create, drop and rename
/// batches maintain, so one lookup costs a `get` instead of a scan of every
/// table in the catalog.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn relation_name_of(
    kv: &dyn Kv,
    table_id: TableId,
) -> Result<Option<RelationName>, CatalogError> {
    let Some(value) = kv.get(&key::catalog_by_id_key(table_id))? else {
        return Ok(None);
    };
    // `key_parts` yields exactly the count it was asked for, or nothing.
    let parts = key::key_parts(&value, 2).ok_or_else(|| {
        KvError::CorruptRow("table id index entry is not two length-prefixed parts".into())
    })?;
    Ok(Some(RelationName::new(parts[0], parts[1])))
}

/// The table `table_id` names.
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors from the catalog KV seam.
pub fn table_by_id(kv: &dyn Kv, table_id: TableId) -> Result<Table, CatalogError> {
    let name = relation_name_of(kv, table_id)?
        .ok_or_else(|| CatalogError::UndefinedTable(format!("table id {table_id}")))?;
    get_table(kv, &name)
}

const VIEW_PREFIX: &[u8] = b"\0\0\0\0catalog_view/";
const SEQUENCE_PREFIX: &[u8] = b"\0\0\0\0catalog_sequence/";

fn view_key(relation: &RelationName) -> Vec<u8> {
    let mut key = VIEW_PREFIX.to_vec();
    key::push_key_part(&mut key, &relation.schema);
    key::push_key_part(&mut key, &relation.name);
    key
}

fn catalog_sequence_key(relation: &RelationName) -> Vec<u8> {
    let mut key = SEQUENCE_PREFIX.to_vec();
    key::push_key_part(&mut key, &relation.schema);
    key::push_key_part(&mut key, &relation.name);
    key
}

fn schema_key(name: &str) -> Vec<u8> {
    let mut key = SCHEMA_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key
}

fn dropped_schema_key(name: &str) -> Vec<u8> {
    let mut key = DROPPED_SCHEMA_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key
}

/// The owner that `name` is bootstrapped under, when it is a bootstrap schema.
fn bootstrap_schema_owner(name: &str) -> Option<&'static str> {
    BOOTSTRAP_SCHEMAS
        .iter()
        .find(|(schema, _)| *schema == name)
        .map(|(_, owner)| *owner)
}

/// Every schema, bootstrap ones included, sorted by name.
///
/// A stored row wins over the bootstrap row of the same name.
/// `ALTER SCHEMA … OWNER TO` on a bootstrap schema therefore replaces that row
/// rather than adding a second one.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_schemas(kv: &dyn Kv) -> Result<Vec<Schema>, CatalogError> {
    let mut schemas = kv
        .scan_prefix(SCHEMA_PREFIX)?
        .into_iter()
        .map(|(key, value)| {
            let name = String::from_utf8(key[SCHEMA_PREFIX.len()..].to_vec()).map_err(|_| {
                CatalogError::Storage(KvError::CorruptRow("non-UTF-8 schema name".into()))
            })?;
            let owner = String::from_utf8(value).map_err(|_| {
                CatalogError::Storage(KvError::CorruptRow("non-UTF-8 schema owner".into()))
            })?;
            Ok(Schema { name, owner })
        })
        .collect::<Result<Vec<_>, CatalogError>>()?;
    for (name, owner) in BOOTSTRAP_SCHEMAS {
        let stored = schemas.iter().any(|schema| schema.name == *name);
        if stored || kv.get(&dropped_schema_key(name))?.is_some() {
            continue;
        }
        schemas.push(Schema {
            name: (*name).to_string(),
            owner: (*owner).to_string(),
        });
    }
    schemas.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(schemas)
}

/// True when `name` denotes an existing schema.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn schema_exists(kv: &dyn Kv, name: &str) -> Result<bool, CatalogError> {
    if kv.get(&schema_key(name))?.is_some() {
        return Ok(true);
    }
    Ok(bootstrap_schema_owner(name).is_some() && kv.get(&dropped_schema_key(name))?.is_none())
}

/// Build the write batch for `CREATE SCHEMA`.
///
/// This function applies the reserved-prefix rule before it looks up the name.
/// `CREATE SCHEMA pg_catalog` therefore reports an unacceptable name rather
/// than a duplicate.
///
/// # Errors
///
/// Returns reserved-name, duplicate-schema, or storage/corruption errors from
/// the catalog KV seam.
pub fn create_schema_ops(
    kv: &dyn Kv,
    name: &str,
    owner: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    if name.starts_with(RESERVED_SCHEMA_PREFIX) {
        return Err(CatalogError::ReservedSchemaName(name.to_string()));
    }
    if schema_exists(kv, name)? {
        return Err(CatalogError::DuplicateSchema(name.to_string()));
    }
    let mut ops = vec![WriteOp::Put {
        key: schema_key(name),
        value: owner.as_bytes().to_vec(),
    }];
    if bootstrap_schema_owner(name).is_some() {
        // Re-creating a dropped bootstrap schema retires its tombstone.
        ops.push(WriteOp::Delete {
            key: dropped_schema_key(name),
        });
    }
    Ok(ops)
}

/// Build the write batch for `ALTER SCHEMA … OWNER TO`.
///
/// # Errors
///
/// Returns undefined-schema or storage/corruption errors from the catalog KV
/// seam.
pub fn set_schema_owner_ops(
    kv: &dyn Kv,
    name: &str,
    owner: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    if !schema_exists(kv, name)? {
        return Err(CatalogError::UndefinedSchema(name.to_string()));
    }
    Ok(vec![WriteOp::Put {
        key: schema_key(name),
        value: owner.as_bytes().to_vec(),
    }])
}

/// The comment kinds whose key begins with a relation name, and so whose key
/// moves when the relation's schema is renamed.
///
/// A comment on anything else — a database, a role — keys on a bare name that
/// is not a schema, and a one-part key is indistinguishable from a relation's
/// by shape alone. Listing the relation kinds is what tells the two apart.
const RELATION_COMMENT_KINDS: &[&str] = &[
    "table",
    "view",
    "materialized view",
    "foreign table",
    "index",
    "sequence",
    "column",
];

/// Check `ALTER SCHEMA … RENAME TO` and build the half of its batch that the
/// schema itself owns: the `pg_namespace` row, the grants on the schema, and
/// the comments on its relations.
///
/// The relations are the other half, and they are not here. A relation's
/// catalog key carries its schema, so each one has to be moved by
/// [`move_relation_to_schema_ops`] — and moved *one at a time over a view of
/// the batch so far*, because a foreign key or an inheritance link between two
/// relations in the schema is read from one end while the other end is being
/// rewritten. This crate has no such view; the executor has one and drives that
/// loop. [`schema_contents`] answers what the loop must cover.
///
/// The checks and their order come from `RenameSchema` in
/// `src/backend/commands/schemacmds.c`: the schema must exist, the new name
/// must be free, and only then is the new name held to
/// [`RESERVED_SCHEMA_PREFIX`]. Ownership sits between the second and the third
/// upstream; this catalog has no schema-ownership test for any statement, so
/// there is none to run here either.
///
/// # Errors
///
/// Returns undefined-schema, duplicate-schema, reserved-name, system-schema,
/// unmovable-object, or storage/corruption errors from the catalog KV seam.
pub fn rename_schema_ops(
    kv: &dyn Kv,
    name: &str,
    new_name: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    if !schema_exists(kv, name)? {
        return Err(CatalogError::UndefinedSchema(name.to_string()));
    }
    if schema_exists(kv, new_name)? {
        return Err(CatalogError::DuplicateSchema(new_name.to_string()));
    }
    if new_name.starts_with(RESERVED_SCHEMA_PREFIX) {
        return Err(CatalogError::ReservedSchemaName(new_name.to_string()));
    }
    if SYSTEM_SCHEMAS.contains(&name) {
        return Err(CatalogError::SystemSchemaRename(name.to_string()));
    }
    reject_unmovable_schema_objects(kv, name)?;

    let mut ops = rename_schema_record_ops(kv, name, new_name)?;
    ops.extend(move_schema_part_keys(
        kv,
        SCHEMA_PRIVILEGE_PREFIX,
        name,
        new_name,
    )?);
    ops.extend(move_schema_comment_keys(kv, name, new_name)?);
    Ok(ops)
}

/// Move one relation of a renamed schema onto its new name.
///
/// A table goes through [`rename_table_ops`], which already relocates a
/// relation's whole subtree — the schema record, the id index, the tablespace
/// and sharding rows, every index under both of its keys, the table- and
/// column-level grants, and the foreign-key display names. A view and a
/// sequence each carry their schema in their own key, and a view carries it
/// once more in its record.
///
/// See [`rename_schema_ops`] for the other half of the batch and for why this
/// is a call per relation rather than a loop inside it.
///
/// # Errors
///
/// Returns duplicate-relation or storage/corruption errors from the catalog KV
/// seam.
pub fn move_relation_to_schema_ops(
    kv: &dyn Kv,
    name: &RelationName,
    new_name: &RelationName,
) -> Result<Vec<WriteOp>, CatalogError> {
    if kv.get(&catalog_key(name))?.is_some() {
        return rename_table_ops(kv, name, new_name);
    }
    if let Some(bytes) = kv.get(&view_key(name))? {
        let mut view = deserialize_view(&bytes)?;
        view.name = new_name.clone();
        let mut ops = vec![
            WriteOp::Delete {
                key: view_key(name),
            },
            put_view_op(&view),
        ];
        ops.extend(move_creation_order_ops(kv, name, new_name)?);
        return Ok(ops);
    }
    if let Some(bytes) = kv.get(&catalog_sequence_key(name))? {
        let mut ops = vec![
            WriteOp::Delete {
                key: catalog_sequence_key(name),
            },
            WriteOp::Put {
                key: catalog_sequence_key(new_name),
                value: bytes,
            },
        ];
        ops.extend(move_creation_order_ops(kv, name, new_name)?);
        return Ok(ops);
    }
    Ok(Vec::new())
}

/// Refuse the rename when the schema holds something the batch cannot move.
///
/// Each of these is reachable from an oid held elsewhere — a column's type, an
/// operator's commutator, an index's operator class — so relocating the key
/// alone would leave the record naming a schema that no longer exists. The
/// refusal keeps the failure loud instead of leaving a stranded row behind.
fn reject_unmovable_schema_objects(kv: &dyn Kv, schema: &str) -> Result<(), CatalogError> {
    let unmovable = |object: &'static str| CatalogError::UnmovableSchemaObject {
        schema: schema.to_string(),
        object,
    };
    if list_user_types(kv)?.iter().any(|ty| {
        ty.schema == schema
            || ty
                .multirange_identity()
                .is_some_and(|(owner, _)| owner == schema)
    }) {
        return Err(unmovable("a user-defined type"));
    }
    if list_user_operators(kv)?
        .iter()
        .any(|operator| operator.schema == schema)
    {
        return Err(unmovable("a user-defined operator"));
    }
    if list_operator_families(kv)?
        .iter()
        .any(|family| family.name.schema == schema)
    {
        return Err(unmovable("an operator family"));
    }
    if list_operator_classes(kv)?
        .iter()
        .any(|class| class.name.schema == schema)
    {
        return Err(unmovable("an operator class"));
    }
    Ok(())
}

/// Move the `pg_namespace` row itself, keeping the owner.
///
/// A bootstrap schema has no stored row to delete — [`list_schemas`]
/// synthesises it — so leaving it means writing the same tombstone
/// [`drop_schema_ops`] writes. Renaming *to* a dropped bootstrap name retires
/// that name's tombstone, exactly as re-creating it would.
fn rename_schema_record_ops(
    kv: &dyn Kv,
    name: &str,
    new_name: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    let owner = match kv.get(&schema_key(name))? {
        Some(stored) => String::from_utf8(stored)
            .map_err(|_| KvError::CorruptRow("non-UTF-8 schema owner".into()))?,
        // `schema_exists` has already passed, so an absent row means the name
        // is a live bootstrap schema.
        None => bootstrap_schema_owner(name)
            .ok_or_else(|| KvError::CorruptRow("schema row vanished mid-rename".into()))?
            .to_string(),
    };
    let mut ops = vec![
        WriteOp::Delete {
            key: schema_key(name),
        },
        WriteOp::Put {
            key: schema_key(new_name),
            value: owner.into_bytes(),
        },
    ];
    if bootstrap_schema_owner(name).is_some() {
        ops.push(WriteOp::Put {
            key: dropped_schema_key(name),
            value: Vec::new(),
        });
    }
    if bootstrap_schema_owner(new_name).is_some() {
        ops.push(WriteOp::Delete {
            key: dropped_schema_key(new_name),
        });
    }
    Ok(ops)
}

/// Move every key in `prefix` whose leading key part is `name` onto `new_name`,
/// keeping the value.
///
/// This serves the families that hold the schema in the key and nowhere else.
fn move_schema_part_keys(
    kv: &dyn Kv,
    prefix: &[u8],
    name: &str,
    new_name: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut scanned = prefix.to_vec();
    key::push_key_part(&mut scanned, name);
    let mut moved = prefix.to_vec();
    key::push_key_part(&mut moved, new_name);
    let mut ops = Vec::new();
    for (key, value) in kv.scan_prefix(&scanned)? {
        let mut renamed = moved.clone();
        renamed.extend_from_slice(&key[scanned.len()..]);
        ops.push(WriteOp::Delete { key });
        ops.push(WriteOp::Put {
            key: renamed,
            value,
        });
    }
    Ok(ops)
}

/// Move the comments on the schema's relations and their columns.
///
/// A comment key is the object kind, a `/`, and then the object's name parts,
/// so the kind has to be split off before the schema part can be recognised.
fn move_schema_comment_keys(
    kv: &dyn Kv,
    name: &str,
    new_name: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut ops = Vec::new();
    for kind in RELATION_COMMENT_KINDS {
        let mut prefix = COMMENT_PREFIX.to_vec();
        prefix.extend_from_slice(kind.as_bytes());
        prefix.push(b'/');
        ops.extend(move_schema_part_keys(kv, &prefix, name, new_name)?);
    }
    Ok(ops)
}

/// The names of every relation, view and sequence stored in `schema`.
///
/// Each family is a prefix scan over the schema's own subtree. A flat namespace
/// could answer this only by a list of the whole catalog and a filter over it.
/// That is also why `CREATE TABLE "a/b"` used to escape
/// `DROP SCHEMA … CASCADE`. The scan recovered names, and it rejected any key
/// suffix that held a `/`.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn schema_contents(kv: &dyn Kv, schema: &str) -> Result<Vec<RelationName>, CatalogError> {
    let mut names = Vec::new();
    for family in [
        key::catalog_prefix(),
        VIEW_PREFIX.to_vec(),
        SEQUENCE_PREFIX.to_vec(),
    ] {
        let mut in_schema = family.clone();
        key::push_key_part(&mut in_schema, schema);
        for (stored, _) in kv.scan_prefix(&in_schema)? {
            if let Some(name) = relation_name_from_key(&family, &stored) {
                names.push(name);
            }
        }
    }
    names.sort();
    names.dedup();
    Ok(names)
}

/// Recover the `(schema, name)` a relation-family key was built from.
///
/// A key that does not decode as exactly two length-prefixed parts belongs to a
/// neighbouring family, and this function skips it. The rejection is
/// structural. The flat layout instead had to guess: it looked for a separator
/// character that the name was assumed not to contain.
fn relation_name_from_key(family_prefix: &[u8], stored: &[u8]) -> Option<RelationName> {
    let suffix = stored.strip_prefix(family_prefix)?;
    let [schema, name] = key::key_parts(suffix, 2)?[..] else {
        return None;
    };
    Some(RelationName::new(schema, name))
}

/// Build the write batch for dropping an empty schema.
///
/// The caller drops the schema's contents first when the statement wrote
/// `CASCADE`. This function refuses a non-empty schema with 2BP01, exactly as
/// `RESTRICT` does in `PostgreSQL`. It refuses a
/// [system schema](SYSTEM_SCHEMAS) outright.
///
/// A drop of a bootstrap schema leaves a tombstone behind, because there is no
/// stored row to delete. The schema exists only because [`list_schemas`]
/// synthesises it.
///
/// # Errors
///
/// Returns undefined-schema, system-schema, dependent-object, or
/// storage/corruption errors.
pub fn drop_schema_ops(
    kv: &dyn Kv,
    name: &str,
    cascade: bool,
) -> Result<Vec<WriteOp>, CatalogError> {
    if !schema_exists(kv, name)? {
        return Err(CatalogError::UndefinedSchema(name.to_string()));
    }
    if SYSTEM_SCHEMAS.contains(&name) {
        return Err(CatalogError::SystemSchemaDrop(name.to_string()));
    }
    if !cascade
        && (!schema_contents(kv, name)?.is_empty()
            || list_user_types(kv)?.iter().any(|ty| {
                ty.schema == name
                    || ty
                        .multirange_identity()
                        .is_some_and(|(schema, _)| schema == name)
            }))
    {
        return Err(CatalogError::SchemaNotEmpty(name.to_string()));
    }
    let mut ops = vec![WriteOp::Delete {
        key: schema_key(name),
    }];
    let mut privilege_prefix = SCHEMA_PRIVILEGE_PREFIX.to_vec();
    key::push_key_part(&mut privilege_prefix, name);
    ops.extend(
        kv.scan_prefix(&privilege_prefix)?
            .into_iter()
            .map(|(key, _)| WriteOp::Delete { key }),
    );
    for (key, privilege) in scan_default_table_privileges(kv)? {
        if privilege.schema.as_deref() == Some(name) {
            ops.push(WriteOp::Delete { key });
        }
    }
    if bootstrap_schema_owner(name).is_some() {
        ops.push(WriteOp::Put {
            key: dropped_schema_key(name),
            value: Vec::new(),
        });
    }
    Ok(ops)
}

/// Build the atomic catalog batch for renaming an ordinary or foreign table.
///
/// Immutable IDs key the rows and the local secondary-index entries, so their
/// physical keys do not move. Index *metadata* and both the table-level and
/// column-level privileges carry the table name and are rewritten in the same
/// batch. Index names are preserved.
/// Foreign keys are id-keyed on both sides, so only the denormalized display
/// names in their payloads are rewritten — on the table's own constraints and
/// on every constraint that references it. Row-security policies need no
/// rewriting at all: they are keyed and stored by table id only, so a rename
/// cannot strand one under the old name and leave the relation unprotected.
/// Stored views retain SQL text rather
/// than dependency identities; until that representation can be rewritten
/// safely, any stored view blocks a rename.
///
/// The two names may differ in their schema as well as their relation part —
/// that is how [`rename_schema_ops`] moves a relation. Two things that a
/// same-schema rename leaves alone then have to move: an index's by-name key,
/// which carries the schema, and a `SERIAL` column's default, which names its
/// sequence as text and so would otherwise keep naming the schema the relation
/// just left.
///
/// # Errors
///
/// Returns missing/wrong-type/duplicate-relation, stored-view-dependency, or
/// storage/corruption errors from the catalog KV seam.
pub fn rename_table_ops(
    kv: &dyn Kv,
    name: &RelationName,
    new_name: &RelationName,
) -> Result<Vec<WriteOp>, CatalogError> {
    let schema = match kv.get(&catalog_key(name))? {
        Some(schema) => schema,
        None if kv.get(&view_key(name))?.is_some() => {
            return Err(CatalogError::WrongObjectType(name.to_string()));
        }
        None => return Err(CatalogError::UndefinedTable(name.to_string())),
    };
    if relation_exists(kv, new_name)? {
        return Err(CatalogError::DuplicateTable(new_name.to_string()));
    }

    let (table_id, ..) = deserialize_schema(&schema)?;
    let schema = move_default_sequences(kv, schema, &name.schema, &new_name.schema)?;
    let mut ops = vec![
        WriteOp::Delete {
            key: catalog_key(name),
        },
        WriteOp::Put {
            key: catalog_key(new_name),
            value: schema,
        },
        catalog_by_id_op(table_id, new_name),
    ];
    if let Some(placement) = kv.get(&relation_tablespace_key(name))? {
        ops.push(drop_relation_tablespace_op(name));
        ops.push(WriteOp::Put {
            key: relation_tablespace_key(new_name),
            value: placement,
        });
    }
    if let Some(access_method) = kv.get(&relation_access_method_key(name))? {
        ops.push(drop_relation_access_method_op(name));
        ops.push(WriteOp::Put {
            key: relation_access_method_key(new_name),
            value: access_method,
        });
    }
    if let Some(sharding) = kv.get(&sharding_key(name))? {
        ops.push(WriteOp::Delete {
            key: sharding_key(name),
        });
        ops.push(WriteOp::Put {
            key: sharding_key(new_name),
            value: sharding,
        });
    }
    if let Some(typed_of) = kv.get(&typed_table_key(name))? {
        ops.push(WriteOp::Delete {
            key: typed_table_key(name),
        });
        ops.push(WriteOp::Put {
            key: typed_table_key(new_name),
            value: typed_of,
        });
    }
    for (table_index_key, index_bytes) in kv.scan_prefix(&catalog_table_index_prefix(table_id))? {
        let mut index = deserialize_index(&index_bytes)?;
        let vacated = index.qualified_name();
        index.table = new_name.clone();
        let renamed_index = serialize_index(&index);
        // An index lives in its table's schema, so the by-name key it answers
        // to moves with the table even though the index keeps its own name.
        if vacated != index.qualified_name() {
            ops.push(WriteOp::Delete {
                key: catalog_index_key(&vacated),
            });
        }
        ops.push(WriteOp::Put {
            key: catalog_index_key(&index.qualified_name()),
            value: renamed_index.clone(),
        });
        ops.push(WriteOp::Put {
            key: table_index_key,
            value: renamed_index,
        });
    }
    for (privilege_key, bytes) in kv.scan_prefix(&table_privilege_relation_prefix(name))? {
        let privilege = deserialize_table_privilege(&bytes)?;
        ops.push(WriteOp::Delete { key: privilege_key });
        ops.push(WriteOp::Put {
            key: table_privilege_key(new_name, &privilege.grantee, &privilege.privilege),
            value: serialize_table_privilege(new_name, &privilege.grantee, &privilege.privilege),
        });
    }
    for (privilege_key, bytes) in kv.scan_prefix(&column_privilege_relation_prefix(name))? {
        let privilege = deserialize_column_privilege(&bytes)?;
        let ColumnPrivilege {
            column,
            grantee,
            privilege,
            ..
        } = &privilege;
        ops.push(WriteOp::Delete { key: privilege_key });
        ops.push(WriteOp::Put {
            key: column_privilege_key(new_name, column, grantee, privilege),
            value: serialize_column_privilege(new_name, column, grantee, privilege),
        });
    }
    ops.extend(rename_table_foreign_key_ops(kv, table_id, new_name)?);
    ops.extend(move_creation_order_ops(kv, name, new_name)?);
    Ok(ops)
}

/// Repoint a relation's `SERIAL` and identity defaults at `to` when they name a
/// sequence that lives in `from`.
///
/// A default names its sequence as the text [`RelationName`] displays, not as an
/// oid, so a relation that changes schema takes the text with it. Only a
/// sequence that `from` really holds is repointed: a default may name a
/// sequence in a third schema, which does not move, and a relation name may
/// itself contain a dot, which only a lookup can tell from a qualifier.
///
/// Returns `schema` unchanged when the relation stays where it is, which is
/// every `ALTER TABLE … RENAME TO`.
fn move_default_sequences(
    kv: &dyn Kv,
    schema: Vec<u8>,
    from: &str,
    to: &str,
) -> Result<Vec<u8>, CatalogError> {
    if from == to {
        return Ok(schema);
    }
    let (table_id, mut columns, options, owner, meta, checks, materialized) =
        deserialize_schema(&schema)?;
    let mut moved = false;
    for column in &mut columns {
        let Some(ColumnDefault::NextVal(sequence)) = &column.default else {
            continue;
        };
        let Some(base) = displayed_relation_in(sequence, from) else {
            continue;
        };
        if kv
            .get(&catalog_sequence_key(&RelationName::new(from, base)))?
            .is_none()
        {
            continue;
        }
        column.default = Some(ColumnDefault::NextVal(
            RelationName::new(to, base).to_string(),
        ));
        moved = true;
    }
    if !moved {
        return Ok(schema);
    }
    Ok(serialize_schema(
        table_id,
        &columns,
        options,
        &owner,
        meta.as_ref(),
        materialized.as_ref(),
        &checks,
    ))
}

/// The relation part of `displayed`, when [`RelationName`] would display a
/// relation of `schema` that way.
///
/// A relation in `public` displays bare, so there is no qualifier to strip.
fn displayed_relation_in<'a>(displayed: &'a str, schema: &str) -> Option<&'a str> {
    if schema == PUBLIC_SCHEMA {
        return (!displayed.contains('.')).then_some(displayed);
    }
    displayed.strip_prefix(&format!("{}.", displayed_schema(schema)))
}

/// Build the write batch for creating a table (schema + sequence init +
/// `next_table_id` bump) WITHOUT writing — caller persists the ops. Returns the
/// allocated `TableId` alongside the batch. Validation (duplicate-table check,
/// `next_table_id` read) is identical to `create_table`.
///
/// The table is created under [`BOOTSTRAP_ROLE`]. Executor DDL names the
/// session's `current_user` instead, through
/// [`create_table_with_options_ops`].
///
/// # Errors
///
/// Returns duplicate-table or storage/corruption errors from the catalog KV seam.
pub fn create_table_ops(
    kv: &dyn Kv,
    name: &RelationName,
    columns: Vec<Column>,
) -> Result<(TableId, Vec<WriteOp>), CatalogError> {
    create_table_with_options_ops(
        kv,
        name,
        columns,
        TableOptions::default(),
        Vec::new(),
        TableCreation::bootstrap(),
    )
}

/// Where the id for a new table comes from.
///
/// The shared counter's read-bump-commit is atomic only while the caller holds
/// the lock that covers it. A session that already claimed a block of ids under
/// that lock therefore hands one out itself, and does not touch the counter
/// again. That is what keeps `CREATE TEMP TABLE` off the cluster-wide critical
/// path. The cost is that ids stop being dense in creation order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableIdSource {
    /// Read the shared counter and bump it in the same batch. The caller must
    /// hold whatever lock makes that pair atomic.
    Counter,
    /// Use an id the caller already reserved. This does not read the counter
    /// and does not write it.
    Reserved(TableId),
}

impl TableIdSource {
    /// The id to create under, and the counter bump the batch owes.
    fn allocate(self, kv: &dyn Kv) -> Result<(TableId, Option<WriteOp>), CatalogError> {
        match self {
            Self::Counter => {
                let next = read_next_table_id(kv)?;
                Ok((next, Some(set_next_table_id_op(next + 1))))
            }
            Self::Reserved(id) => Ok((id, None)),
        }
    }
}

/// The creation-time facts a new relation needs beyond its schema: who it
/// belongs to, where its id comes from, and — for `CREATE MATERIALIZED VIEW` —
/// the query its contents come from. Bundled so the create batteries keep a
/// workable parameter count as more of them accumulate.
///
/// The materialized-view metadata is borrowed rather than owned so this stays a
/// `Copy` handle onto facts the caller already holds, which is what lets the
/// create batteries keep taking it by value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableCreation<'a> {
    /// The role the new relation is owned by — the creating session's
    /// `current_user`.
    pub owner: &'a str,
    /// Where the new relation's [`TableId`] comes from.
    pub id: TableIdSource,
    /// Present when the relation being created is a materialized view, so
    /// `CREATE MATERIALIZED VIEW` writes one schema record rather than creating
    /// a table and then rewriting it. `None` for every other relation.
    pub materialized: Option<&'a MaterializedView>,
}

impl TableCreation<'_> {
    /// Creation under [`BOOTSTRAP_ROLE`] from the shared counter — what the
    /// catalog's own convenience constructors use when no session supplies a
    /// user.
    #[must_use]
    pub const fn bootstrap() -> Self {
        Self {
            owner: BOOTSTRAP_ROLE,
            id: TableIdSource::Counter,
            materialized: None,
        }
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "preserves the donor catalog API shape consumed by executor DDL paths"
)]
/// Build the write batch for creating a table with explicit metadata options.
///
/// # Errors
///
/// Returns duplicate-table or storage/corruption errors from the catalog KV seam.
pub fn create_table_with_options_ops(
    kv: &dyn Kv,
    name: &RelationName,
    columns: Vec<Column>,
    options: TableOptions,
    checks: Vec<CheckConstraint>,
    creation: TableCreation<'_>,
) -> Result<(TableId, Vec<WriteOp>), CatalogError> {
    if relation_exists(kv, name)? {
        return Err(CatalogError::DuplicateTable(name.to_string()));
    }
    let (next, bump) = creation.id.allocate(kv)?;
    let mut batch = vec![
        WriteOp::Put {
            key: catalog_key(name),
            value: serialize_schema(
                next,
                &columns,
                options,
                creation.owner,
                None,
                creation.materialized,
                &checks,
            ),
        },
        WriteOp::Put {
            key: key::seq_key(next),
            value: U64::new(1).as_bytes().to_vec(),
        },
        catalog_by_id_op(next, name),
    ];
    batch.extend(bump);
    batch.extend(creation_order_ops(kv, name)?);
    Ok((next, batch))
}

/// Build the write batch that creates a view without persisting it.
///
/// `owner` is the creating session's `current_user`; the catalog's own
/// convenience callers pass [`BOOTSTRAP_ROLE`].
///
/// # Errors
///
/// Returns duplicate-relation or storage/corruption errors from the catalog KV seam.
pub fn create_view_ops(
    kv: &dyn Kv,
    name: &RelationName,
    definition: String,
    columns: Vec<Column>,
    options: ViewOptions,
    owner: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    if relation_exists(kv, name)? {
        return Err(CatalogError::DuplicateTable(name.to_string()));
    }
    let view = View {
        name: name.clone(),
        definition,
        owner: owner.to_string(),
        columns,
        options,
    };
    let mut ops = vec![WriteOp::Put {
        key: view_key(name),
        value: serialize_view(&view),
    }];
    ops.extend(creation_order_ops(kv, name)?);
    Ok(ops)
}

/// Create a view and its output schema in one atomic batch.
///
/// `owner` is the creating session's `current_user`.
///
/// # Errors
///
/// Returns duplicate-relation or storage/corruption errors from the catalog KV seam.
pub fn create_view(
    kv: &dyn Kv,
    name: &RelationName,
    definition: String,
    columns: Vec<Column>,
    options: ViewOptions,
    owner: &str,
) -> Result<(), CatalogError> {
    kv.write_batch(&create_view_ops(
        kv, name, definition, columns, options, owner,
    )?)?;
    Ok(())
}

/// Look up a view by relation name.
///
/// # Errors
///
/// Returns undefined-relation or storage/corruption errors from the catalog KV seam.
pub fn get_view(kv: &dyn Kv, name: &RelationName) -> Result<View, CatalogError> {
    let bytes = kv
        .get(&view_key(name))?
        .ok_or_else(|| CatalogError::UndefinedTable(name.to_string()))?;
    deserialize_view(&bytes).map_err(CatalogError::from)
}

/// Return every stored view in catalog-name order.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_views(kv: &dyn Kv) -> Result<Vec<View>, CatalogError> {
    let mut views = kv
        .scan_prefix(VIEW_PREFIX)?
        .into_iter()
        .map(|(_, bytes)| deserialize_view(&bytes).map_err(CatalogError::from))
        .collect::<Result<Vec<View>, CatalogError>>()?;
    views.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(views)
}

/// Overwrite a stored view record in place, keeping the name and replacing the
/// definition and the columns.
#[must_use]
pub fn put_view_op(view: &View) -> WriteOp {
    WriteOp::Put {
        key: view_key(&view.name),
        value: serialize_view(view),
    }
}

/// Overwrite a secondary-index catalog record in place, under both the
/// by-name and by-table keys the catalog maintains.
#[must_use]
pub fn put_index_ops(index: &Index) -> Vec<WriteOp> {
    let bytes = serialize_index(index);
    vec![
        WriteOp::Put {
            key: catalog_index_key(&index.qualified_name()),
            value: bytes.clone(),
        },
        WriteOp::Put {
            key: catalog_table_index_key(index.table_id, &index.name),
            value: bytes,
        },
    ]
}

/// Build the write batch that replaces an ordinary table's column list,
/// `CHECK` constraints and owner, preserving its id, storage options, and
/// foreign metadata. Every `ALTER TABLE` subcommand that only edits the schema
/// record funnels through here so the encoding lives in exactly one place.
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors from the catalog KV seam.
pub fn replace_table_schema_ops(
    kv: &dyn Kv,
    name: &RelationName,
    table: &Table,
) -> Result<Vec<WriteOp>, CatalogError> {
    let bytes = kv
        .get(&catalog_key(name))?
        .ok_or_else(|| CatalogError::UndefinedTable(name.to_string()))?;
    let (id, _, options, _, foreign, _, _) = deserialize_schema(&bytes)?;
    Ok(vec![WriteOp::Put {
        key: catalog_key(name),
        value: serialize_schema(
            id,
            &table.columns,
            TableOptions {
                // The row-security flags come from the working relation rather
                // than from storage: `ALTER TABLE … ENABLE ROW LEVEL SECURITY`
                // folds into the same `Table` every other subcommand edits, and
                // one write of the schema record has to carry all of them.
                // Reading them back from storage here would quietly undo the
                // subcommand.
                row_security: table.row_security,
                force_row_security: table.force_row_security,
                ..options
            },
            &table.owner,
            foreign.as_ref(),
            // Like the row-security flags, the materialized-view metadata comes
            // from the working relation: `REFRESH MATERIALIZED VIEW` folds into
            // the same `Table` an `ALTER` subcommand edits, so re-reading it
            // from storage here would undo the refresh.
            table.materialized.as_ref(),
            &table.checks,
        ),
    }])
}

/// Build the write batch that sets a relation's row-security flags, preserving
/// every other part of its schema record.
///
/// The two flags are set together because they are read together: `FORCE ROW
/// LEVEL SECURITY` alone means nothing, and a caller that could move one
/// without the other could leave a relation forced-but-not-enabled, which reads
/// as "unprotected".
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors from the catalog KV seam.
pub fn set_row_security_ops(
    kv: &dyn Kv,
    name: &RelationName,
    row_security: bool,
    force_row_security: bool,
) -> Result<Vec<WriteOp>, CatalogError> {
    let bytes = kv
        .get(&catalog_key(name))?
        .ok_or_else(|| CatalogError::UndefinedTable(name.to_string()))?;
    let (id, columns, options, owner, foreign, checks, materialized) = deserialize_schema(&bytes)?;
    Ok(vec![WriteOp::Put {
        key: catalog_key(name),
        value: serialize_schema(
            id,
            &columns,
            TableOptions {
                row_security,
                force_row_security,
                ..options
            },
            &owner,
            foreign.as_ref(),
            materialized.as_ref(),
            &checks,
        ),
    }])
}

/// Build the write op that flips a materialized view's population flag and
/// writes back the definition `table` carries, leaving every other field of the
/// schema record alone.
///
/// It is a pure function of the relation rather than a read-modify-write
/// against storage because a `Table` already determines the whole record — id,
/// columns, option flags, owner, relation-kind payload and `CHECK` list — so
/// `REFRESH MATERIALIZED VIEW` can put the flag flip in the same batch as the
/// heap rewrite it belongs with, without a catalog read in between.
///
/// A relation that is not a materialized view has no flag to set, so the op
/// rewrites its record unchanged; callers reject `REFRESH` on an ordinary
/// relation with `42809` long before they get here.
#[must_use]
pub fn set_materialized_populated_op(table: &Table, populated: bool) -> WriteOp {
    let materialized = table.materialized.as_ref().map(|matview| MaterializedView {
        definition: matview.definition.clone(),
        populated,
    });
    WriteOp::Put {
        key: catalog_key(&table.name),
        value: serialize_schema(
            table.id,
            &table.columns,
            TableOptions {
                sharded: table.sharded,
                row_security: table.row_security,
                force_row_security: table.force_row_security,
            },
            &table.owner,
            table.foreign.as_ref(),
            materialized.as_ref(),
            &table.checks,
        ),
    }
}

/// Whether `name` names a materialized view.
///
/// A name that is not a stored relation at all — a plain view, a sequence, or
/// nothing — answers `false` rather than erroring, because every caller is
/// asking which of several relation kinds it is holding and has its own
/// "relation does not exist" path already.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn is_materialized_view(kv: &dyn Kv, name: &RelationName) -> Result<bool, CatalogError> {
    let Some(bytes) = kv.get(&catalog_key(name))? else {
        return Ok(false);
    };
    let (.., materialized) = deserialize_schema(&bytes)?;
    Ok(materialized.is_some())
}

/// Build the write batch that drops a view without persisting it.
///
/// # Errors
///
/// Returns undefined-relation or storage/corruption errors from the catalog KV seam.
pub fn drop_view_ops(kv: &dyn Kv, name: &RelationName) -> Result<Vec<WriteOp>, CatalogError> {
    if kv.get(&view_key(name))?.is_some() {
        let mut ops = vec![WriteOp::Delete {
            key: view_key(name),
        }];
        ops.push(drop_creation_order_op(name));
        ops.extend(drop_table_privilege_ops(kv, name)?);
        ops.extend(drop_relation_column_privilege_ops(kv, name)?);
        return Ok(ops);
    }
    if kv.get(&catalog_key(name))?.is_some() {
        return Err(CatalogError::WrongObjectType(name.to_string()));
    }
    Err(CatalogError::UndefinedTable(name.to_string()))
}

/// Drop a view in one atomic batch.
///
/// # Errors
///
/// Returns undefined-relation or storage/corruption errors from the catalog KV seam.
pub fn drop_view(kv: &dyn Kv, name: &RelationName) -> Result<(), CatalogError> {
    kv.write_batch(&drop_view_ops(kv, name)?)?;
    Ok(())
}

/// Build the write batch for creating a table and optional sharding metadata.
///
/// # Errors
///
/// Returns duplicate-table or storage/corruption errors from the catalog KV seam.
pub fn create_table_with_sharding_ops(
    kv: &dyn Kv,
    name: &RelationName,
    columns: Vec<Column>,
    options: TableOptions,
    sharding: Option<&ShardingStrategy>,
    checks: Vec<CheckConstraint>,
    creation: TableCreation<'_>,
) -> Result<(TableId, Vec<WriteOp>), CatalogError> {
    if let Some(ShardingStrategy::Hash(hash)) = sharding {
        validate_hash_sharding_column_defs(&columns, hash)?;
    }
    let (table_id, mut ops) =
        create_table_with_options_ops(kv, name, columns, options, checks, creation)?;
    if let Some(strategy) = sharding {
        ops.push(WriteOp::Put {
            key: sharding_key(name),
            value: serialize_sharding(Some(strategy)),
        });
    }
    Ok((table_id, ops))
}

/// Create a table in one atomic batch.
///
/// The batch allocates a `TableId`, persists the schema, and inits the
/// sequence. The caller serializes concurrent DDL.
///
/// # Errors
///
/// Returns duplicate-table or storage/corruption errors from the catalog KV seam.
pub fn create_table(
    kv: &dyn Kv,
    name: &RelationName,
    columns: Vec<Column>,
) -> Result<TableId, CatalogError> {
    let (next, batch) = create_table_ops(kv, name, columns)?;
    kv.write_batch(&batch)?;
    Ok(next)
}

/// Create a table with explicit metadata options.
///
/// # Errors
///
/// Returns duplicate-table or storage/corruption errors from the catalog KV seam.
pub fn create_table_with_options(
    kv: &dyn Kv,
    name: &RelationName,
    columns: Vec<Column>,
    options: TableOptions,
) -> Result<TableId, CatalogError> {
    let (next, batch) = create_table_with_options_ops(
        kv,
        name,
        columns,
        options,
        Vec::new(),
        TableCreation::bootstrap(),
    )?;
    kv.write_batch(&batch)?;
    Ok(next)
}

/// Look up a table by name.
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors from the catalog KV seam.
pub fn get_table(kv: &dyn Kv, name: &RelationName) -> Result<Table, CatalogError> {
    let bytes = kv
        .get(&catalog_key(name))?
        .ok_or_else(|| CatalogError::UndefinedTable(name.to_string()))?;
    table_from_schema_bytes(kv, name, &bytes)
}

/// Return every ordinary/foreign table in catalog-name order.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_tables(kv: &dyn Kv) -> Result<Vec<Table>, CatalogError> {
    let prefix = key::catalog_prefix();
    let mut tables = kv
        .scan_prefix(&prefix)?
        .into_iter()
        .filter_map(|(table_key, bytes)| {
            relation_name_from_key(&prefix, &table_key)
                .map(|name| table_from_schema_bytes(kv, &name, &bytes))
        })
        .collect::<Result<Vec<_>, _>>()?;
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(tables)
}

/// True when `name` is taken by a table, a view or a sequence.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn relation_exists(kv: &dyn Kv, name: &RelationName) -> Result<bool, CatalogError> {
    Ok(kv.get(&catalog_key(name))?.is_some()
        || kv.get(&view_key(name))?.is_some()
        || kv.get(&catalog_sequence_key(name))?.is_some()
        // An index shares the relation namespace with tables, views and
        // sequences, exactly as it does in `pg_class`: `CREATE TABLE i` after
        // `CREATE INDEX i` is `42P07 relation "i" already exists`. Leaving
        // indexes out here is not merely a missing duplicate check — it also
        // makes an index invisible to unqualified name resolution, so a
        // `DROP INDEX` naming one in a schema other than the fallback walks
        // the whole search path, matches nothing, and reports `42704`.
        || kv.get(&catalog_index_key(name))?.is_some())
}

fn table_from_schema_bytes(
    kv: &dyn Kv,
    name: &RelationName,
    bytes: &[u8],
) -> Result<Table, CatalogError> {
    let (id, columns, options, owner, foreign, checks, materialized) = deserialize_schema(bytes)?;
    let sharding = kv
        .get(&sharding_key(name))?
        .map(|bytes| deserialize_sharding(&bytes))
        .transpose()?
        .flatten();
    Ok(Table {
        id,
        name: name.clone(),
        owner,
        columns,
        sharded: options.sharded,
        row_security: options.row_security,
        force_row_security: options.force_row_security,
        sharding,
        foreign,
        materialized,
        checks,
    })
}

/// Return a table's optional hash-sharding strategy metadata.
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors from the catalog KV seam.
pub fn get_table_sharding(
    kv: &dyn Kv,
    name: &RelationName,
) -> Result<Option<ShardingStrategy>, CatalogError> {
    let _table = get_table(kv, name)?;
    let Some(bytes) = kv.get(&sharding_key(name))? else {
        return Ok(None);
    };

    Ok(deserialize_sharding(&bytes)?)
}

/// Build a write batch that replaces a table's optional sharding metadata.
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors from the catalog KV seam.
pub fn set_table_sharding_ops(
    kv: &dyn Kv,
    name: &RelationName,
    sharding: Option<&ShardingStrategy>,
) -> Result<Vec<WriteOp>, CatalogError> {
    let table = get_table(kv, name)?;
    if let Some(ShardingStrategy::Hash(hash)) = sharding {
        validate_hash_sharding_columns(&table, hash)?;
    }
    let key = sharding_key(name);
    let op = match sharding {
        None => WriteOp::Delete { key },
        Some(strategy) => WriteOp::Put {
            key,
            value: serialize_sharding(Some(strategy)),
        },
    };
    Ok(vec![op])
}

/// Complete a table conversion batch.
///
/// The batch atomically publishes sharded visibility and replaces the optional
/// physical sharding metadata.
///
/// `rewrite_ops` must contain the complete physical data transition for the
/// table. Callers must commit the returned batch as one unit. A batch that
/// publishes this metadata without the rewrite makes existing xid-MVCC rows
/// unreadable to timestamp scans.
///
/// # Errors
///
/// Returns undefined-table, unsupported foreign-table conversion,
/// undefined-column, or storage/corruption errors from the catalog KV seam.
pub fn complete_table_conversion_ops(
    kv: &dyn Kv,
    name: &RelationName,
    sharding: Option<&ShardingStrategy>,
    mut rewrite_ops: Vec<WriteOp>,
) -> Result<Vec<WriteOp>, CatalogError> {
    let bytes = kv
        .get(&catalog_key(name))?
        .ok_or_else(|| CatalogError::UndefinedTable(name.to_string()))?;
    let (id, columns, options, owner, foreign, checks, materialized) = deserialize_schema(&bytes)?;
    if foreign.is_some() {
        return Err(CatalogError::NotOrdinaryTable(name.to_string()));
    }
    let table = Table {
        id,
        name: name.clone(),
        owner,
        columns,
        sharded: options.sharded,
        row_security: options.row_security,
        force_row_security: options.force_row_security,
        sharding: get_table_sharding(kv, name)?,
        foreign: None,
        materialized,
        checks,
    };
    if let Some(ShardingStrategy::Hash(hash)) = sharding {
        validate_hash_sharding_columns(&table, hash)?;
    }
    validate_conversion_rewrite(kv, table.id, &rewrite_ops)?;

    rewrite_ops.push(WriteOp::Put {
        key: catalog_key(name),
        value: serialize_schema(
            id,
            &table.columns,
            // Only the sharding flag changes: rewriting the whole option set
            // here would silently clear the relation's row-security flags,
            // which is a total policy bypass rather than a lost preference.
            TableOptions {
                sharded: true,
                ..options
            },
            &table.owner,
            None,
            table.materialized.as_ref(),
            &table.checks,
        ),
    });
    rewrite_ops.extend(set_table_sharding_ops(kv, name, sharding)?);
    Ok(rewrite_ops)
}

fn validate_conversion_rewrite(
    kv: &dyn Kv,
    table_id: TableId,
    rewrite_ops: &[WriteOp],
) -> Result<(), CatalogError> {
    let table_prefix = key::table_prefix(table_id);
    let mut final_tuples = kv
        .scan_prefix(&key::table_prefix(table_id))?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let table_was_empty = final_tuples.is_empty();
    if table_was_empty
        && !rewrite_ops
            .iter()
            .any(|op| matches!(op, WriteOp::Delete { key } if *key == table_prefix))
    {
        // An empty table still needs the executor's explicit physical-rewrite
        // proof. The prefix cannot be a tuple key because it lacks a rowid.
        return Err(CatalogError::IncompleteConversionRewrite);
    }

    for op in rewrite_ops {
        match op {
            WriteOp::Delete { key } if key.starts_with(&table_prefix) => {
                final_tuples.remove(key);
            }
            WriteOp::Put { key, value } | WriteOp::ConditionalPut { key, value, .. }
                if key.starts_with(&table_prefix) =>
            {
                final_tuples.insert(key.clone(), value.clone());
            }
            WriteOp::Put { .. } | WriteOp::ConditionalPut { .. } | WriteOp::Delete { .. } => {}
        }
    }

    if final_tuples
        .values()
        .all(|value| crabka_pgmvcc::version::decode_ts_tuple(value).is_ok())
    {
        return Ok(());
    }
    Err(CatalogError::IncompleteConversionRewrite)
}

fn catalog_index_key(name: &RelationName) -> Vec<u8> {
    let mut out = catalog_index_prefix();
    key::push_key_part(&mut out, &name.schema);
    key::push_key_part(&mut out, &name.name);
    out
}

fn catalog_index_prefix() -> Vec<u8> {
    b"\0\0\0\0catalog_index/by-name/".to_vec()
}

fn catalog_table_index_key(table_id: TableId, index_name: &str) -> Vec<u8> {
    let mut out = b"\0\0\0\0catalog_index/by-table/".to_vec();
    out.extend_from_slice(&table_id.to_be_bytes());
    out.extend_from_slice(b"/");
    out.extend_from_slice(index_name.as_bytes());
    out
}

fn catalog_table_index_prefix(table_id: TableId) -> Vec<u8> {
    let mut out = b"\0\0\0\0catalog_index/by-table/".to_vec();
    out.extend_from_slice(&table_id.to_be_bytes());
    out.extend_from_slice(b"/");
    out
}

fn meta_next_index_id_key() -> Vec<u8> {
    b"\0\0\0\0meta/next_index_id".to_vec()
}

const COMMENT_PREFIX: &[u8] = b"\0\0\0\0catalog_comment/";

/// What a `COMMENT ON` statement attached its comment to.
///
/// A column comment names a *pair*, the relation and the column, and a relation
/// name is itself a pair. A dotted string that flattens either pair loses the
/// boundary: `COMMENT ON COLUMN s.t.c` and a relation literally called `s.t.c`
/// would land on the same key. The catalog stores each part length-prefixed
/// instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommentObject<'a> {
    /// A relation: a table, view, index or sequence.
    Relation(&'a RelationName),
    /// One column of a relation.
    Column(&'a RelationName, &'a str),
    /// Anything that is not a relation: a schema, a database, a role.
    Named(&'a str),
}

impl<'a> CommentObject<'a> {
    fn key_parts(self) -> Vec<&'a str> {
        match self {
            Self::Relation(relation) => vec![&relation.schema, &relation.name],
            Self::Column(relation, column) => vec![&relation.schema, &relation.name, column],
            Self::Named(name) => vec![name],
        }
    }
}

fn comment_key(object_kind: &str, object: CommentObject<'_>) -> Vec<u8> {
    let mut out = COMMENT_PREFIX.to_vec();
    out.extend_from_slice(object_kind.as_bytes());
    out.push(b'/');
    for part in object.key_parts() {
        key::push_key_part(&mut out, part);
    }
    out
}

/// Build the write op that sets an object comment, or clears it for `None`.
///
/// `object_kind` is the lowercase `COMMENT ON <kind>` keyword.
#[must_use]
pub fn set_comment_op(
    object_kind: &str,
    object: CommentObject<'_>,
    comment: Option<&str>,
) -> WriteOp {
    match comment {
        Some(text) => WriteOp::Put {
            key: comment_key(object_kind, object),
            value: text.as_bytes().to_vec(),
        },
        None => WriteOp::Delete {
            key: comment_key(object_kind, object),
        },
    }
}

/// Read an object comment previously set by `COMMENT ON`.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn get_comment(
    kv: &dyn Kv,
    object_kind: &str,
    object: CommentObject<'_>,
) -> Result<Option<String>, CatalogError> {
    let Some(bytes) = kv.get(&comment_key(object_kind, object))? else {
        return Ok(None);
    };
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| CatalogError::Storage(KvError::CorruptRow("non-UTF-8 comment".into())))
}

/// Build the write ops that delete every comment attached to a relation and its
/// columns.
///
/// A `DROP` or a rename therefore never leaves an orphaned comment behind.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn drop_relation_comment_ops(
    kv: &dyn Kv,
    name: &RelationName,
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut ops = Vec::new();
    for kind in ["table", "view", "index", "sequence", "column"] {
        // The relation's two length-prefixed parts are a complete prefix of
        // both its own comment key and each of its column comment keys, and of
        // nothing else — so unlike the flat layout this needs no guard against
        // `t` matching `t2`.
        let prefix = comment_key(kind, CommentObject::Relation(name));
        for (key, _) in kv.scan_prefix(&prefix)? {
            ops.push(WriteOp::Delete { key });
        }
    }
    Ok(ops)
}

fn read_next_index_id(kv: &dyn Kv) -> Result<IndexId, CatalogError> {
    match kv.get(&meta_next_index_id_key())? {
        Some(bytes) => {
            let (id, _) = U32::read_from_prefix(bytes.as_slice())
                .map_err(|_| KvError::CorruptRow("next_index_id is not u32".into()))?;
            Ok(id.get())
        }
        None => Ok(1),
    }
}

/// Hands out the [`IndexId`]s that stamp one write batch's new indexes, across
/// however many relations that batch touches.
///
/// [`create_indexes_on_table_ops`] covers one relation's worth of indexes on
/// its own. A statement that indexes several relations at once cannot use it
/// twice: none of the first relation's records are in the KV yet, so the second
/// call would read the same stored counter and stamp the same ids again.
/// `ALTER TABLE … ATTACH PARTITION` of a sub-partitioned relation is exactly
/// that statement — every partition below the one being attached needs the
/// parent's indexes copied onto it.
///
/// The cursor stays in memory across the batch instead, and [`Self::commit_op`]
/// writes the counter past the last id it stamped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexIds {
    /// `None` until a caller asks for the first id. A statement that creates no
    /// index must not read the counter at all, and must write nothing back.
    next: Option<IndexId>,
}

impl IndexIds {
    /// The next creation-order id. The first call reads the shared counter.
    ///
    /// # Errors
    ///
    /// Returns storage/corruption errors from the catalog KV seam.
    pub fn allocate(&mut self, kv: &dyn Kv) -> Result<IndexId, CatalogError> {
        let id = match self.next {
            Some(id) => id,
            None => read_next_index_id(kv)?,
        };
        self.next = Some(id + 1);
        Ok(id)
    }

    /// The counter write that moves the stored value past every id stamped, or
    /// `None` when nothing was stamped.
    #[must_use]
    pub fn commit_op(self) -> Option<WriteOp> {
        self.next.map(|next| WriteOp::Put {
            key: meta_next_index_id_key(),
            value: U32::new(next).as_bytes().to_vec(),
        })
    }
}

/// Build the write batch for creating a secondary-index catalog record.
///
/// # Errors
///
/// Returns duplicate-index, undefined-table/column, or storage/corruption errors
/// from the catalog KV seam.
pub fn create_index_ops(
    kv: &dyn Kv,
    name: &str,
    table: &RelationName,
    columns: Vec<String>,
    unique: bool,
    placement: IndexPlacement,
) -> Result<(IndexId, Vec<WriteOp>), CatalogError> {
    create_index_with_method_ops(
        kv,
        name,
        table,
        columns,
        unique,
        placement,
        IndexMethod::Btree,
    )
}

/// Build the write batch for an explicitly selected index access method.
///
/// # Errors
///
/// Returns duplicate-index, undefined-table/column, or storage/corruption errors.
pub fn create_index_with_method_ops(
    kv: &dyn Kv,
    name: &str,
    table: &RelationName,
    columns: Vec<String>,
    unique: bool,
    placement: IndexPlacement,
    method: IndexMethod,
) -> Result<(IndexId, Vec<WriteOp>), CatalogError> {
    if kv.get(&catalog_index_key(&table.sibling(name)))?.is_some() {
        return Err(CatalogError::DuplicateIndex(name.to_string()));
    }
    let table_meta = get_table(kv, table)?;
    validate_index_columns(&table_meta, &columns)?;
    let id = read_next_index_id(kv)?;
    let index = Index {
        id,
        name: name.to_string(),
        table: table.clone(),
        table_id: table_meta.id,
        columns,
        unique,
        placement,
        method,
        constraint: None,
        without_overlaps: false,
        clustered: false,
        deferral: ConstraintDeferral::Immediate,
    };
    let value = serialize_index(&index);
    let ops = vec![
        WriteOp::Put {
            key: catalog_index_key(&index.qualified_name()),
            value: value.clone(),
        },
        WriteOp::Put {
            key: catalog_table_index_key(table_meta.id, name),
            value,
        },
        WriteOp::Put {
            key: meta_next_index_id_key(),
            value: U32::new(id + 1).as_bytes().to_vec(),
        },
    ];
    Ok((id, ops))
}

/// Build the write batch for creating a secondary-index catalog record on a
/// table that may not be visible in the catalog KV yet.
///
/// # Errors
///
/// Returns duplicate-index, undefined-column, or storage/corruption errors from
/// the catalog KV seam.
pub fn create_index_on_table_ops(
    kv: &dyn Kv,
    table: &Table,
    name: &str,
    columns: Vec<String>,
    unique: bool,
    placement: IndexPlacement,
) -> Result<(IndexId, Vec<WriteOp>), CatalogError> {
    if kv
        .get(&catalog_index_key(&table.name.sibling(name)))?
        .is_some()
    {
        return Err(CatalogError::DuplicateIndex(name.to_string()));
    }
    validate_index_columns(table, &columns)?;
    let id = read_next_index_id(kv)?;
    let index = Index {
        id,
        name: name.to_string(),
        table: table.name.clone(),
        table_id: table.id,
        columns,
        unique,
        placement,
        method: IndexMethod::Btree,
        constraint: None,
        without_overlaps: false,
        clustered: false,
        deferral: ConstraintDeferral::Immediate,
    };
    let value = serialize_index(&index);
    let ops = vec![
        WriteOp::Put {
            key: catalog_index_key(&index.qualified_name()),
            value: value.clone(),
        },
        WriteOp::Put {
            key: catalog_table_index_key(table.id, name),
            value,
        },
        WriteOp::Put {
            key: meta_next_index_id_key(),
            value: U32::new(id + 1).as_bytes().to_vec(),
        },
    ];
    Ok((id, ops))
}

/// Build the write batch for creating one constraint-backed index on an
/// existing table.
///
/// This function returns the full allocated [`Index`], so the caller can
/// backfill index entries into the same durable batch.
///
/// Unlike [`create_index_on_table_ops`], it honors every [`NewIndex`] field,
/// `constraint` included. `ALTER TABLE … ADD PRIMARY KEY` therefore records the
/// constraint marker that blocks `DROP INDEX` and a second primary key.
///
/// # Errors
///
/// Returns duplicate-index, undefined-column, or storage/corruption errors from
/// the catalog KV seam.
pub fn create_constraint_index_ops(
    kv: &dyn Kv,
    table: &Table,
    new_index: &NewIndex,
) -> Result<(Index, Vec<WriteOp>), CatalogError> {
    if kv
        .get(&catalog_index_key(&table.name.sibling(&new_index.name)))?
        .is_some()
    {
        return Err(CatalogError::DuplicateIndex(new_index.name.clone()));
    }
    validate_index_columns(table, &new_index.columns)?;
    let id = read_next_index_id(kv)?;
    let index = Index {
        id,
        name: new_index.name.clone(),
        table: table.name.clone(),
        table_id: table.id,
        columns: new_index.columns.clone(),
        unique: new_index.unique,
        placement: new_index.placement,
        method: new_index.method,
        constraint: new_index.constraint.clone(),
        without_overlaps: new_index.without_overlaps,
        clustered: false,
        deferral: new_index.deferral,
    };
    let value = serialize_index(&index);
    let ops = vec![
        WriteOp::Put {
            key: catalog_index_key(&index.qualified_name()),
            value: value.clone(),
        },
        WriteOp::Put {
            key: catalog_table_index_key(table.id, &index.name),
            value,
        },
        WriteOp::Put {
            key: meta_next_index_id_key(),
            value: U32::new(id + 1).as_bytes().to_vec(),
        },
    ];
    Ok((index, ops))
}

/// Build the write batch that marks the named columns NOT NULL on a table's
/// schema record.
///
/// `ALTER TABLE … ADD PRIMARY KEY` sets its key columns NOT NULL, which matches
/// `PostgreSQL`. The batch keeps the sharding metadata and the foreign-table
/// linkage, and it keeps an already-NOT-NULL column unchanged.
///
/// # Errors
///
/// Returns undefined-table, undefined-column, or storage/corruption errors
/// from the catalog KV seam.
pub fn set_columns_not_null_ops(
    kv: &dyn Kv,
    table_name: &RelationName,
    not_null_columns: &[String],
) -> Result<Vec<WriteOp>, CatalogError> {
    let bytes = kv
        .get(&catalog_key(table_name))?
        .ok_or_else(|| CatalogError::UndefinedTable(table_name.to_string()))?;
    let (id, mut columns, options, owner, foreign, checks, materialized) =
        deserialize_schema(&bytes)?;
    for name in not_null_columns {
        let column = columns
            .iter_mut()
            .find(|column| column.name == *name)
            .ok_or_else(|| CatalogError::UndefinedColumn(name.clone()))?;
        column.not_null = true;
    }
    Ok(vec![WriteOp::Put {
        key: catalog_key(table_name),
        value: serialize_schema(
            id,
            &columns,
            options,
            &owner,
            foreign.as_ref(),
            materialized.as_ref(),
            &checks,
        ),
    }])
}

/// Build the write batch for creating secondary-index catalog records on a
/// table that may not be visible in the catalog KV yet.
///
/// # Errors
///
/// Returns duplicate-index, undefined-column, or storage/corruption errors from
/// the catalog KV seam.
pub fn create_indexes_on_table_ops(
    kv: &dyn Kv,
    table: &Table,
    indexes: &[NewIndex],
) -> Result<Vec<WriteOp>, CatalogError> {
    if indexes.is_empty() {
        return Ok(Vec::new());
    }

    let mut seen_names = HashSet::with_capacity(indexes.len());
    for index in indexes {
        if !seen_names.insert(index.name.as_str())
            || kv
                .get(&catalog_index_key(&table.name.sibling(&index.name)))?
                .is_some()
        {
            return Err(CatalogError::DuplicateIndex(index.name.clone()));
        }
        validate_index_columns(table, &index.columns)?;
    }

    let first_id = read_next_index_id(kv)?;
    let mut ops = Vec::with_capacity(indexes.len() * 2 + 1);
    for (offset, new_index) in indexes.iter().enumerate() {
        let id = first_id
            + u32::try_from(offset).map_err(|_| {
                CatalogError::Storage(KvError::CorruptRow("too many indexes in one batch".into()))
            })?;
        let index = Index {
            id,
            name: new_index.name.clone(),
            table: table.name.clone(),
            table_id: table.id,
            columns: new_index.columns.clone(),
            unique: new_index.unique,
            placement: new_index.placement,
            method: new_index.method,
            constraint: new_index.constraint.clone(),
            without_overlaps: new_index.without_overlaps,
            clustered: false,
            deferral: new_index.deferral,
        };
        let value = serialize_index(&index);
        ops.push(WriteOp::Put {
            key: catalog_index_key(&index.qualified_name()),
            value: value.clone(),
        });
        ops.push(WriteOp::Put {
            key: catalog_table_index_key(table.id, &index.name),
            value,
        });
    }
    let index_count = u32::try_from(indexes.len()).map_err(|_| {
        CatalogError::Storage(KvError::CorruptRow("too many indexes in one batch".into()))
    })?;
    ops.push(WriteOp::Put {
        key: meta_next_index_id_key(),
        value: U32::new(first_id + index_count).as_bytes().to_vec(),
    });
    Ok(ops)
}

/// Persist a secondary-index catalog record.
///
/// # Errors
///
/// Returns duplicate-index, undefined-table/column, or storage/corruption errors.
pub fn create_index(
    kv: &dyn Kv,
    name: &str,
    table: &RelationName,
    columns: Vec<String>,
    unique: bool,
    placement: IndexPlacement,
) -> Result<IndexId, CatalogError> {
    let (id, ops) = create_index_ops(kv, name, table, columns, unique, placement)?;
    kv.write_batch(&ops)?;
    Ok(id)
}

/// Look up a secondary index by name.
///
/// # Errors
///
/// Returns undefined-index or storage/corruption errors.
pub fn get_index(kv: &dyn Kv, name: &RelationName) -> Result<Index, CatalogError> {
    let bytes = kv
        .get(&catalog_index_key(name))?
        .ok_or_else(|| CatalogError::UndefinedIndex(name.name.clone()))?;
    Ok(deserialize_index(&bytes)?)
}

/// Build the metadata write batch for dropping an index without persisting it.
///
/// The returned definition lets the executor remove the matching local-index
/// entries in the same durable write batch.
///
/// # Errors
///
/// Returns undefined-index, wrong-object-type, dependent-object, or storage
/// errors from the catalog KV seam.
pub fn drop_index_ops(
    kv: &dyn Kv,
    name: &RelationName,
) -> Result<(Index, Vec<WriteOp>), CatalogError> {
    let index = match get_index(kv, name) {
        Ok(index) => index,
        Err(CatalogError::UndefinedIndex(_)) if relation_exists(kv, name)? => {
            return Err(CatalogError::WrongObjectType(name.name.clone()));
        }
        Err(error) => return Err(error),
    };
    if index.constraint.is_some() {
        return Err(CatalogError::DependentObjectsStillExist(name.name.clone()));
    }
    Ok((index.clone(), drop_index_record_ops(&index)))
}

/// Build the write batch that removes a secondary-index catalog record,
/// a constraint-backed one included.
///
/// `DROP INDEX` must refuse a constraint-backed index (2BP01). By design,
/// `ALTER TABLE … DROP CONSTRAINT` and `DROP COLUMN` do drop one, so they reach
/// the record removal directly.
///
/// # Errors
///
/// Returns undefined-index or storage/corruption errors from the catalog KV seam.
pub fn drop_constraint_index_ops(
    kv: &dyn Kv,
    name: &RelationName,
) -> Result<(Index, Vec<WriteOp>), CatalogError> {
    let index = get_index(kv, name)?;
    Ok((index.clone(), drop_index_record_ops(&index)))
}

fn drop_index_record_ops(index: &Index) -> Vec<WriteOp> {
    vec![
        WriteOp::Delete {
            key: catalog_index_key(&index.qualified_name()),
        },
        WriteOp::Delete {
            key: catalog_table_index_key(index.table_id, &index.name),
        },
        drop_relation_tablespace_op(&index.qualified_name()),
    ]
}

/// Return every secondary-index catalog record, sorted by index name.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_indexes(kv: &dyn Kv) -> Result<Vec<Index>, CatalogError> {
    let mut indexes = kv
        .scan_prefix(&catalog_index_prefix())?
        .into_iter()
        .map(|(_, bytes)| deserialize_index(&bytes).map_err(CatalogError::from))
        .collect::<Result<Vec<_>, _>>()?;
    indexes.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(indexes)
}

// ── Foreign keys ──────────────────────────────────────────────────────────────

/// Authoritative foreign-key records, keyed `<child table id BE u32>/<name>`.
///
/// `(child_table_id, name)` *is* a constraint's identity. Constraint names are
/// per-relation in `PostgreSQL`, so the child id must be part of the key. The
/// key is id-based rather than name-based, so a later wave that re-keys the
/// name-keyed catalog families by schema does not have to touch this one. A
/// relation can move or get a new name, and its foreign keys stay put.
const FOREIGN_KEY_BY_TABLE_PREFIX: &[u8] = b"\0\0\0\0catalog_fk/by-table/";

/// Reverse index over the referenced side, keyed
/// `<parent table id BE u32>/<child table id BE u32>/<name>` with an empty
/// payload.
///
/// The parent side needs "who references me?" on every DELETE or UPDATE of a
/// referenced table. A scan of every foreign key in the catalog would make that
/// check O(constraints in the database).
const FOREIGN_KEY_BY_REF_PREFIX: &[u8] = b"\0\0\0\0catalog_fk/by-ref/";

fn meta_next_foreign_key_id_key() -> Vec<u8> {
    b"\0\0\0\0meta/next_foreign_key_id".to_vec()
}

fn read_next_foreign_key_id(kv: &dyn Kv) -> Result<ForeignKeyId, CatalogError> {
    match kv.get(&meta_next_foreign_key_id_key())? {
        Some(bytes) => {
            let (id, _) = U32::read_from_prefix(bytes.as_slice())
                .map_err(|_| KvError::CorruptRow("next_foreign_key_id is not u32".into()))?;
            Ok(id.get())
        }
        None => Ok(1),
    }
}

/// Hands out the [`ForeignKeyId`]s that stamp one write batch's new
/// constraints.
///
/// A single statement can create several constraints. `CREATE TABLE` can carry
/// two `FOREIGN KEY` clauses, and `ALTER TABLE` can carry two `ADD CONSTRAINT`
/// subcommands. None of them are in the KV until the batch commits, so every
/// one of them would read the same stored counter.
///
/// The cursor stays in memory across the batch instead. Each
/// [`create_foreign_key_ops`] carries the counter write that moves the stored
/// value past the id it stamped. The last write applied leaves the counter
/// correct.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ForeignKeyIds {
    /// `None` until a caller asks for the first id. A statement that creates no
    /// constraint must not read the counter at all.
    next: Option<ForeignKeyId>,
}

impl ForeignKeyIds {
    /// The next creation-order id. The first call reads the shared counter.
    ///
    /// # Errors
    ///
    /// Returns storage/corruption errors from the catalog KV seam.
    pub fn allocate(&mut self, kv: &dyn Kv) -> Result<ForeignKeyId, CatalogError> {
        let id = match self.next {
            Some(id) => id,
            None => read_next_foreign_key_id(kv)?,
        };
        self.next = Some(id + 1);
        Ok(id)
    }
}

fn catalog_foreign_key_key(table_id: TableId, name: &str) -> Vec<u8> {
    let mut out = catalog_table_foreign_key_prefix(table_id);
    out.extend_from_slice(name.as_bytes());
    out
}

fn catalog_table_foreign_key_prefix(table_id: TableId) -> Vec<u8> {
    let mut out = FOREIGN_KEY_BY_TABLE_PREFIX.to_vec();
    out.extend_from_slice(&table_id.to_be_bytes());
    out.push(b'/');
    out
}

fn catalog_foreign_key_ref_key(
    referenced_table_id: TableId,
    table_id: TableId,
    name: &str,
) -> Vec<u8> {
    let mut out = catalog_referencing_foreign_key_prefix(referenced_table_id);
    out.extend_from_slice(&table_id.to_be_bytes());
    out.push(b'/');
    out.extend_from_slice(name.as_bytes());
    out
}

fn catalog_referencing_foreign_key_prefix(referenced_table_id: TableId) -> Vec<u8> {
    let mut out = FOREIGN_KEY_BY_REF_PREFIX.to_vec();
    out.extend_from_slice(&referenced_table_id.to_be_bytes());
    out.push(b'/');
    out
}

/// Recover `(referenced table id, child table id, constraint name)` from a
/// `fk/by-ref` key.
///
/// Both ids are fixed-width and the name closes the key. A constraint name that
/// contains the separator therefore stays unambiguous.
fn foreign_key_ref_key_parts(key: &[u8]) -> Result<(TableId, TableId, String), CatalogError> {
    let corrupt = || CatalogError::Storage(KvError::CorruptRow("malformed fk/by-ref key".into()));
    let suffix = key
        .strip_prefix(FOREIGN_KEY_BY_REF_PREFIX)
        .ok_or_else(corrupt)?;
    let (referenced_table_id, rest) = split_key_table_id(suffix).ok_or_else(corrupt)?;
    let (table_id, name) = split_key_table_id(rest).ok_or_else(corrupt)?;
    let name = String::from_utf8(name.to_vec()).map_err(|_| {
        CatalogError::Storage(KvError::CorruptRow("non-UTF-8 constraint name".into()))
    })?;
    Ok((referenced_table_id, table_id, name))
}

/// Split a leading `<table id BE u32>/` off a catalog key tail.
fn split_key_table_id(bytes: &[u8]) -> Option<(TableId, &[u8])> {
    let (id, rest) = bytes.split_at_checked(4)?;
    let id = TableId::from_be_bytes(id.try_into().expect("4"));
    Some((id, rest.strip_prefix(b"/")?))
}

/// Overwrite a foreign-key catalog record in place, under both the by-table and
/// by-ref keys the catalog maintains.
///
/// Both keys derive from the ids, so this is an in-place rewrite of one
/// constraint. Use it for a display-name rewrite. Do not use it for a change of
/// `table_id` or `referenced_table_id`. Such a change moves the record, and
/// needs [`drop_foreign_key_ops`] plus [`create_foreign_key_ops`] to remove the
/// old keys.
#[must_use]
pub fn put_foreign_key_ops(fk: &ForeignKey) -> Vec<WriteOp> {
    vec![
        WriteOp::Put {
            key: catalog_foreign_key_key(fk.table_id, &fk.name),
            value: serialize_foreign_key(fk),
        },
        WriteOp::Put {
            key: catalog_foreign_key_ref_key(fk.referenced_table_id, fk.table_id, &fk.name),
            value: Vec::new(),
        },
    ]
}

/// Build the write batch that records a new foreign-key constraint.
///
/// The caller has already resolved the referent, the backing unique index and
/// the [`ForeignKeyId`] from a [`ForeignKeyIds`] cursor. This function only
/// refuses a name the child relation already uses, and adds the counter write
/// that keeps the next constraint's id above this one's.
///
/// # Errors
///
/// Returns duplicate-constraint or storage/corruption errors from the catalog
/// KV seam.
pub fn create_foreign_key_ops(kv: &dyn Kv, fk: &ForeignKey) -> Result<Vec<WriteOp>, CatalogError> {
    if kv
        .get(&catalog_foreign_key_key(fk.table_id, &fk.name))?
        .is_some()
    {
        return Err(CatalogError::DuplicateConstraint {
            name: fk.name.clone(),
            relation: fk.table.to_string(),
        });
    }
    let mut ops = put_foreign_key_ops(fk);
    ops.push(WriteOp::Put {
        key: meta_next_foreign_key_id_key(),
        value: U32::new(fk.id + 1).as_bytes().to_vec(),
    });
    Ok(ops)
}

/// Build the write batch that removes a foreign-key constraint, returning the
/// definition so the caller can drop whatever it backs in the same batch.
///
/// # Errors
///
/// Returns undefined-constraint or storage/corruption errors from the catalog
/// KV seam.
pub fn drop_foreign_key_ops(
    kv: &dyn Kv,
    table_id: TableId,
    name: &str,
) -> Result<(ForeignKey, Vec<WriteOp>), CatalogError> {
    let fk = get_foreign_key(kv, table_id, name)?;
    let ops = drop_foreign_key_record_ops(&fk);
    Ok((fk, ops))
}

fn drop_foreign_key_record_ops(fk: &ForeignKey) -> Vec<WriteOp> {
    vec![
        WriteOp::Delete {
            key: catalog_foreign_key_key(fk.table_id, &fk.name),
        },
        WriteOp::Delete {
            key: catalog_foreign_key_ref_key(fk.referenced_table_id, fk.table_id, &fk.name),
        },
    ]
}

/// Look up one foreign key by its `(child table, name)` identity.
///
/// # Errors
///
/// Returns undefined-constraint or storage/corruption errors from the catalog
/// KV seam.
pub fn get_foreign_key(
    kv: &dyn Kv,
    table_id: TableId,
    name: &str,
) -> Result<ForeignKey, CatalogError> {
    let bytes = kv
        .get(&catalog_foreign_key_key(table_id, name))?
        .ok_or_else(|| CatalogError::UndefinedConstraint(name.to_string()))?;
    Ok(deserialize_foreign_key(&bytes)?)
}

/// Every foreign key declared *on* a table, the child side, in creation order.
///
/// The order decides what a row that violates two of them reports.
/// `PostgreSQL` fires the constraints' triggers in OID order, so the constraint
/// declared first raises the 23503, whatever the two are called.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_table_foreign_keys(
    kv: &dyn Kv,
    table_id: TableId,
) -> Result<Vec<ForeignKey>, CatalogError> {
    let mut foreign_keys = kv
        .scan_prefix(&catalog_table_foreign_key_prefix(table_id))?
        .into_iter()
        .map(|(_, bytes)| deserialize_foreign_key(&bytes).map_err(CatalogError::from))
        .collect::<Result<Vec<_>, _>>()?;
    foreign_keys.sort_by_key(|fk| fk.id);
    Ok(foreign_keys)
}

/// Every foreign key that *references* a table, the parent side, in creation
/// order.
///
/// This is the read behind referential maintenance. A DELETE or key UPDATE on a
/// referenced table asks it for the constraints that must be enforced. The
/// order is load-bearing whenever two of them act on the same referencing
/// column. `PostgreSQL` fires their RI triggers in OID order. An `ON DELETE SET
/// NULL` declared before an `ON DELETE CASCADE` therefore clears the key the
/// cascade would have matched, and the row survives. The other order gives the
/// opposite outcome. [`ForeignKeyId`] is that order, and it is a total order
/// across relations, so the several children a parent may have need no
/// tie-break.
///
/// The same order drives the 2BP01 `DETAIL` that lists an object's dependent
/// constraints, which `PostgreSQL` also emits in OID order.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam, including a
/// reverse-index entry whose authoritative record is missing.
pub fn list_referencing_foreign_keys(
    kv: &dyn Kv,
    table_id: TableId,
) -> Result<Vec<ForeignKey>, CatalogError> {
    let mut foreign_keys = Vec::new();
    for (key, _) in kv.scan_prefix(&catalog_referencing_foreign_key_prefix(table_id))? {
        let (_, child_id, name) = foreign_key_ref_key_parts(&key)?;
        foreign_keys.push(read_indexed_foreign_key(kv, child_id, &name)?);
    }
    foreign_keys.sort_by_key(|fk| fk.id);
    Ok(foreign_keys)
}

/// Read the record a reverse-index entry points at.
///
/// A miss here is catalog corruption, because the reverse entry exists. It is
/// not the ordinary "no such constraint" that a caller-supplied name can
/// produce.
fn read_indexed_foreign_key(
    kv: &dyn Kv,
    table_id: TableId,
    name: &str,
) -> Result<ForeignKey, CatalogError> {
    get_foreign_key(kv, table_id, name).map_err(|error| match error {
        CatalogError::UndefinedConstraint(name) => CatalogError::Storage(KvError::CorruptRow(
            format!("foreign key \"{name}\" is indexed by referent but has no record"),
        )),
        error => error,
    })
}

/// Every foreign key in the catalog, in child-table-id then constraint-name
/// order. This is the enumeration behind `pg_constraint` introspection.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_foreign_keys(kv: &dyn Kv) -> Result<Vec<ForeignKey>, CatalogError> {
    // The by-table key is the id then the name, so the scan already arrives in
    // that order.
    kv.scan_prefix(FOREIGN_KEY_BY_TABLE_PREFIX)?
        .into_iter()
        .map(|(_, bytes)| deserialize_foreign_key(&bytes).map_err(CatalogError::from))
        .collect()
}

/// The write ops that remove every foreign key a table owns as the *child*,
/// from both key families. The ops also remove any reverse entry that names the
/// table as a child and whose record has already gone missing.
///
/// These ops leave constraints that *reference* the table alone. To refuse such
/// a drop, or to cascade it, is a policy decision above the catalog.
fn drop_table_foreign_key_ops(
    kv: &dyn Kv,
    table_id: TableId,
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut ops = Vec::new();
    let mut reverse_keys = HashSet::new();
    for (fk_key, bytes) in kv.scan_prefix(&catalog_table_foreign_key_prefix(table_id))? {
        let fk = deserialize_foreign_key(&bytes)?;
        let reverse_key =
            catalog_foreign_key_ref_key(fk.referenced_table_id, fk.table_id, &fk.name);
        reverse_keys.insert(reverse_key.clone());
        ops.push(WriteOp::Delete { key: fk_key });
        ops.push(WriteOp::Delete { key: reverse_key });
    }
    for (key, _) in kv.scan_prefix(FOREIGN_KEY_BY_REF_PREFIX)? {
        let (_, child_id, _) = foreign_key_ref_key_parts(&key)?;
        if child_id == table_id && !reverse_keys.contains(&key) {
            ops.push(WriteOp::Delete { key });
        }
    }
    Ok(ops)
}

/// The write ops that rewrite the denormalized relation names carried by every
/// foreign key that touches `table_id`, for a relation renamed to `new_name`.
///
/// Both key families are id-keyed, so nothing moves and the ops rewrite only
/// payloads. A self-referencing constraint appears on both sides. The ops
/// rewrite it once and update both names.
fn rename_table_foreign_key_ops(
    kv: &dyn Kv,
    table_id: TableId,
    new_name: &RelationName,
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut renamed: BTreeMap<Vec<u8>, ForeignKey> = BTreeMap::new();
    for (fk_key, bytes) in kv.scan_prefix(&catalog_table_foreign_key_prefix(table_id))? {
        let mut fk = deserialize_foreign_key(&bytes)?;
        fk.table = new_name.clone();
        renamed.insert(fk_key, fk);
    }
    for (key, _) in kv.scan_prefix(&catalog_referencing_foreign_key_prefix(table_id))? {
        let (_, child_id, name) = foreign_key_ref_key_parts(&key)?;
        let fk_key = catalog_foreign_key_key(child_id, &name);
        let fk = match renamed.remove(&fk_key) {
            Some(fk) => fk,
            None => read_indexed_foreign_key(kv, child_id, &name)?,
        };
        renamed.insert(
            fk_key,
            ForeignKey {
                referenced_table: new_name.clone(),
                ..fk
            },
        );
    }
    Ok(renamed
        .into_iter()
        .map(|(key, fk)| WriteOp::Put {
            key,
            value: serialize_foreign_key(&fk),
        })
        .collect())
}

/// Build the write batch for creating a sequence catalog record.
///
/// # Errors
///
/// Returns duplicate-sequence, invalid-sequence, or storage/corruption errors.
pub fn create_sequence_ops(
    kv: &dyn Kv,
    name: &RelationName,
    sequence: Sequence,
) -> Result<Vec<WriteOp>, CatalogError> {
    validate_sequence(sequence)?;
    if kv.get(&catalog_sequence_key(name))?.is_some() {
        return Err(CatalogError::DuplicateSequence(name.to_string()));
    }
    let mut ops = vec![WriteOp::Put {
        key: catalog_sequence_key(name),
        value: serialize_sequence(sequence),
    }];
    ops.extend(creation_order_ops(kv, name)?);
    Ok(ops)
}

/// Every sequence in the catalog, name and record, sorted by name.
///
/// The catalog introspection relations enumerate sequences through this
/// function: `pg_class` rows of kind `S`, `pg_sequence`, and
/// `information_schema.sequences`. Nothing else needs the whole set.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_sequences(kv: &dyn Kv) -> Result<Vec<(RelationName, Sequence)>, CatalogError> {
    let mut sequences = kv
        .scan_prefix(SEQUENCE_PREFIX)?
        .into_iter()
        .filter_map(|(key, bytes)| {
            let name = relation_name_from_key(SEQUENCE_PREFIX, &key)?;
            Some(deserialize_sequence(&bytes).map(|sequence| (name, sequence)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    sequences.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(sequences)
}

/// The string `version()` reports.
///
/// Clients parse this string. JDBC, npgsql and `SQLAlchemy` all read the
/// major/minor version out of it with a `PostgreSQL <major>.<minor>` prefix
/// match. The prefix is therefore PostgreSQL-shaped, and it names the same 18.4
/// that the wire-level `server_version_num` reports. The parenthesised build
/// tag says which engine answered, exactly as a packaged `PostgreSQL` names its
/// distribution.
#[must_use]
pub fn server_version_string() -> String {
    format!(
        "PostgreSQL 18.4 (Crabka Gres {}) on {}, compiled by rustc, 64-bit",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::ARCH,
    )
}

/// Look up a sequence by name.
///
/// # Errors
///
/// Returns undefined-sequence or storage/corruption errors.
pub fn get_sequence(kv: &dyn Kv, name: &RelationName) -> Result<Sequence, CatalogError> {
    let bytes = kv
        .get(&catalog_sequence_key(name))?
        .ok_or_else(|| CatalogError::UndefinedSequence(name.to_string()))?;
    Ok(deserialize_sequence(&bytes)?)
}

/// The sequence a catalog key names, or `None` when the key belongs to another
/// family.
///
/// A caller that holds a finished write batch uses this to tell which sequences
/// the batch touches, without a second derivation from the statement. The batch
/// is the authority on what reached the catalog. That includes the implicit
/// sequence behind a `SERIAL` column, and one that a `DROP TABLE` cascaded to.
#[must_use]
pub fn sequence_name_from_key(key: &[u8]) -> Option<RelationName> {
    relation_name_from_key(SEQUENCE_PREFIX, key)
}

/// Replace a sequence record.
#[must_use]
pub fn put_sequence_op(name: &RelationName, sequence: Sequence) -> WriteOp {
    WriteOp::Put {
        key: catalog_sequence_key(name),
        value: serialize_sequence(sequence),
    }
}

/// Build the write batch for dropping a sequence.
///
/// # Errors
///
/// Returns undefined-sequence or storage/corruption errors.
pub fn drop_sequence_ops(kv: &dyn Kv, name: &RelationName) -> Result<Vec<WriteOp>, CatalogError> {
    let _ = get_sequence(kv, name)?;
    Ok(vec![
        WriteOp::Delete {
            key: catalog_sequence_key(name),
        },
        drop_creation_order_op(name),
    ])
}

fn validate_sequence(sequence: Sequence) -> Result<(), CatalogError> {
    if sequence.increment == 0 {
        return Err(CatalogError::InvalidSequence(
            "INCREMENT must not be zero".into(),
        ));
    }
    if sequence.cache <= 0 {
        return Err(CatalogError::InvalidSequence(
            "CACHE must be greater than zero".into(),
        ));
    }
    if sequence.min > sequence.max {
        return Err(CatalogError::InvalidSequence(
            "MINVALUE must be less than or equal to MAXVALUE".into(),
        ));
    }
    if sequence.start < sequence.min || sequence.start > sequence.max {
        return Err(CatalogError::InvalidSequence(
            "START value is outside sequence bounds".into(),
        ));
    }
    Ok(())
}

/// Return indexes declared on a table, sorted by index name.
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors.
pub fn list_table_indexes(kv: &dyn Kv, table: &RelationName) -> Result<Vec<Index>, CatalogError> {
    let table = get_table(kv, table)?;
    let mut indexes = kv
        .scan_prefix(&catalog_table_index_prefix(table.id))?
        .into_iter()
        .map(|(_, bytes)| deserialize_index(&bytes).map_err(CatalogError::from))
        .collect::<Result<Vec<_>, _>>()?;
    indexes.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(indexes)
}

fn validate_index_columns(table: &Table, columns: &[String]) -> Result<(), CatalogError> {
    if columns.is_empty() {
        return Err(CatalogError::UndefinedColumn(String::new()));
    }
    for column in columns {
        if index_key_expression(column).is_some_and(|source| !source.is_empty()) {
            continue;
        }
        if table.column_index(column).is_none() {
            return Err(CatalogError::UndefinedColumn(column.clone()));
        }
    }
    Ok(())
}

/// A hash sharding names exactly one column.
///
/// That is the arity the executor's row encoder hashes to place a row, and the
/// only arity it agrees with the gateway on. The gateway derives a statement's
/// route from *every* hash column's bytes. A two-column key would therefore
/// store rows under the hash of the first column alone, in a range that routing
/// never visits.
///
/// The SQL grammar already caps `SHARDED BY HASH (…)` at one column. This check
/// gates the callers that build a [`HashSharding`] against this API directly,
/// so nothing ever creates a table that could never be written to.
fn validate_hash_sharding_arity(hash: &HashSharding) -> Result<(), CatalogError> {
    if hash.columns.len() == 1 {
        return Ok(());
    }
    Err(CatalogError::InvalidSharding(
        "hash sharding requires exactly one column".into(),
    ))
}

fn validate_hash_sharding_columns(table: &Table, hash: &HashSharding) -> Result<(), CatalogError> {
    validate_hash_sharding_arity(hash)?;
    for column in &hash.columns {
        if table.column_index(column).is_none() {
            return Err(CatalogError::UndefinedColumn(column.clone()));
        }
    }
    Ok(())
}

fn validate_hash_sharding_column_defs(
    columns: &[Column],
    hash: &HashSharding,
) -> Result<(), CatalogError> {
    validate_hash_sharding_arity(hash)?;
    for hash_column in &hash.columns {
        if !columns.iter().any(|column| column.name == *hash_column) {
            return Err(CatalogError::UndefinedColumn(hash_column.clone()));
        }
    }
    Ok(())
}

/// Build the write batch for dropping a table WITHOUT writing it.
///
/// The batch removes the catalog entry, the sequence and every row. The caller
/// persists the ops. The errors are identical to `drop_table`, including 42P01
/// on a missing table. The executor uses this to route DDL writes through the
/// durable-write seam.
///
/// The table's own foreign keys go with it, from both the by-table and the
/// by-ref key family. The batch leaves foreign keys that *reference* the table
/// in place. To refuse such a drop, or to cascade it, is a policy decision
/// above the catalog, and those constraints belong to relations that still
/// exist.
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors from the catalog KV seam.
pub fn drop_table_ops(kv: &dyn Kv, name: &RelationName) -> Result<Vec<WriteOp>, CatalogError> {
    let table = get_table(kv, name)?;
    let mut ops = vec![
        WriteOp::Delete {
            key: catalog_key(name),
        },
        drop_creation_order_op(name),
        WriteOp::Delete {
            key: key::seq_key(table.id),
        },
        WriteOp::Delete {
            key: key::catalog_by_id_key(table.id),
        },
        WriteOp::Delete {
            key: replica_identity_key(table.id),
        },
        WriteOp::Delete {
            key: typed_table_key(name),
        },
        drop_relation_tablespace_op(name),
        drop_relation_access_method_op(name),
    ];
    for (row_key, _) in kv.scan_prefix(&key::table_prefix(table.id))? {
        ops.push(WriteOp::Delete { key: row_key });
    }
    for (redirect_key, _) in kv.scan_prefix(&key::update_target_prefix(table.id))? {
        ops.push(WriteOp::Delete { key: redirect_key });
    }
    for (index_table_key, index_bytes) in kv.scan_prefix(&catalog_table_index_prefix(table.id))? {
        let index = deserialize_index(&index_bytes)?;
        ops.push(WriteOp::Delete {
            key: catalog_index_key(&index.qualified_name()),
        });
        ops.push(WriteOp::Delete {
            key: index_table_key,
        });
        ops.push(drop_relation_tablespace_op(&index.qualified_name()));
        ops.push(drop_relation_access_method_op(&index.qualified_name()));
    }
    ops.extend(drop_table_foreign_key_ops(kv, table.id)?);
    // Row-security policies are keyed by table id, and a table id can be handed
    // out again, so leaving them behind would attach one relation's policies to
    // an unrelated later one.
    ops.extend(policy::drop_policies_for_table_ops(kv, table.id)?);
    ops.extend(rule::drop_rules_for_table_ops(kv, table.id)?);
    ops.extend(drop_table_privilege_ops(kv, name)?);
    ops.extend(drop_owner_table_privilege_revoke_ops(kv, name)?);
    ops.extend(drop_relation_column_privilege_ops(kv, name)?);
    Ok(ops)
}

/// Delete every grant recorded against a relation name.
///
/// Grants are keyed by name, and a dropped name can be created again, so a
/// stranded grant would authorize a relation its grantee was never given
/// anything on.
fn drop_table_privilege_ops(
    kv: &dyn Kv,
    name: &RelationName,
) -> Result<Vec<WriteOp>, CatalogError> {
    Ok(kv
        .scan_prefix(&table_privilege_relation_prefix(name))?
        .into_iter()
        .map(|(key, _)| WriteOp::Delete { key })
        .collect())
}

fn drop_owner_table_privilege_revoke_ops(
    kv: &dyn Kv,
    name: &RelationName,
) -> Result<Vec<WriteOp>, CatalogError> {
    Ok(kv
        .scan_prefix(&owner_table_privilege_revoke_relation_prefix(name))?
        .into_iter()
        .map(|(key, _)| WriteOp::Delete { key })
        .collect())
}

/// Delete every *column* grant recorded against a relation name, for the same
/// reason as [`drop_table_privilege_ops`]: the keys carry the name, and the
/// name can come back attached to a different relation.
///
/// This is the whole-relation sweep. To drop one column's grants — what
/// `ALTER TABLE … DROP COLUMN` needs — use [`drop_column_privileges_ops`].
fn drop_relation_column_privilege_ops(
    kv: &dyn Kv,
    name: &RelationName,
) -> Result<Vec<WriteOp>, CatalogError> {
    Ok(kv
        .scan_prefix(&column_privilege_relation_prefix(name))?
        .into_iter()
        .map(|(key, _)| WriteOp::Delete { key })
        .collect())
}

/// Drop a table: delete the catalog entry, the sequence, and all its rows — one
/// atomic batch.
///
/// # Errors
///
/// Returns undefined-table or storage/corruption errors from the catalog KV seam.
pub fn drop_table(kv: &dyn Kv, name: &RelationName) -> Result<(), CatalogError> {
    let ops = drop_table_ops(kv, name)?;
    kv.write_batch(&ops)?;
    Ok(())
}

// ── Roles and table privileges ────────────────────────────────────────────────

const ROLE_PREFIX: &[u8] = b"catalog/role/";
const ROLE_MEMBERSHIP_PREFIX: &[u8] = b"catalog/role_membership/";
const TABLE_PRIVILEGE_PREFIX: &[u8] = b"catalog/table_privilege/";
const DEFAULT_TABLE_PRIVILEGE_PREFIX: &[u8] = b"catalog/default_table_privilege/";
const OWNER_TABLE_PRIVILEGE_REVOKE_PREFIX: &[u8] = b"catalog/table_owner_privilege_revoke/";
const COLUMN_PRIVILEGE_PREFIX: &[u8] = b"catalog/column_privilege/";
const SCHEMA_PRIVILEGE_PREFIX: &[u8] = b"catalog/schema_privilege/";

/// Create a role or login-capable user metadata row.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
pub fn create_role(kv: &dyn Kv, name: &str, can_login: bool) -> Result<(), CatalogError> {
    let ops = create_role_ops(kv, name, can_login, RoleAttributes::default())?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for creating a role without writing.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
pub fn create_role_ops(
    kv: &dyn Kv,
    name: &str,
    can_login: bool,
    attributes: RoleAttributes,
) -> Result<Vec<WriteOp>, CatalogError> {
    if role_exists(kv, name)? {
        return Err(CatalogError::DuplicateObject(name.to_string()));
    }
    Ok(vec![WriteOp::Put {
        key: role_key(name),
        value: serialize_role(name, can_login, attributes),
    }])
}

/// Rewrite an existing role's login flag and boolean attributes.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn alter_role_ops(
    kv: &dyn Kv,
    name: &str,
    can_login: bool,
    attributes: RoleAttributes,
) -> Result<Vec<WriteOp>, CatalogError> {
    if !role_exists(kv, name)? {
        return Err(CatalogError::UndefinedObject(name.to_string()));
    }
    Ok(vec![WriteOp::Put {
        key: role_key(name),
        value: serialize_role(name, can_login, attributes),
    }])
}

/// Build the atomic role row and `IN ROLE` membership records.
///
/// # Errors
///
/// Returns duplicate/undefined-object or catalog storage errors.
pub fn create_role_with_memberships_ops(
    kv: &dyn Kv,
    name: &str,
    can_login: bool,
    attributes: RoleAttributes,
    member_of: &[String],
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut ops = create_role_ops(kv, name, can_login, attributes)?;
    for role in member_of {
        if !role_exists(kv, role)? {
            return Err(CatalogError::UndefinedObject(role.clone()));
        }
        ops.push(WriteOp::Put {
            key: role_membership_key(name, role),
            value: Vec::new(),
        });
    }
    Ok(ops)
}

/// Build the membership records for `GRANT <role> [, …] TO <member> [, …]`.
///
/// This is the second spelling of what `CREATE ROLE … IN ROLE` writes: the same
/// key, the same empty payload, so [`role_has_privs_of`] and [`role_can_set`]
/// see a membership granted either way without knowing which statement made it.
/// Re-granting an existing membership is a no-op rather than an error, matching
/// `PostgreSQL`.
///
/// # Errors
///
/// Returns undefined-object when a named role does not exist, or storage errors
/// from the catalog KV seam.
pub fn grant_role_memberships_ops(
    kv: &dyn Kv,
    roles: &[String],
    members: &[String],
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut ops = Vec::with_capacity(roles.len() * members.len());
    for role in roles {
        if !role_is_nameable(kv, role)? {
            return Err(CatalogError::UndefinedObject(role.clone()));
        }
        for member in members {
            if !role_is_nameable(kv, member)? {
                return Err(CatalogError::UndefinedObject(member.clone()));
            }
            ops.push(WriteOp::Put {
                key: role_membership_key(member, role),
                value: Vec::new(),
            });
        }
    }
    Ok(ops)
}

/// Build the deletes for `REVOKE <role> [, …] FROM <member> [, …]`.
///
/// Revoking a membership that was never granted is a no-op, as it is in
/// `PostgreSQL`; only an unknown *role* is an error.
///
/// # Errors
///
/// Returns undefined-object when a named role does not exist, or storage errors
/// from the catalog KV seam.
pub fn revoke_role_memberships_ops(
    kv: &dyn Kv,
    roles: &[String],
    members: &[String],
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut ops = Vec::with_capacity(roles.len() * members.len());
    for role in roles {
        if !role_is_nameable(kv, role)? {
            return Err(CatalogError::UndefinedObject(role.clone()));
        }
        for member in members {
            if !role_is_nameable(kv, member)? {
                return Err(CatalogError::UndefinedObject(member.clone()));
            }
            ops.push(WriteOp::Delete {
                key: role_membership_key(member, role),
            });
        }
    }
    Ok(ops)
}

/// Whether `member` may assume `role`, including inherited memberships.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn role_can_set(kv: &dyn Kv, member: &str, role: &str) -> Result<bool, CatalogError> {
    if member == role || member == BOOTSTRAP_ROLE {
        return Ok(true);
    }
    let memberships = kv.scan_prefix(ROLE_MEMBERSHIP_PREFIX)?;
    let mut pending = vec![member.to_string()];
    let mut seen = HashSet::new();
    while let Some(current) = pending.pop() {
        if !seen.insert(current.clone()) {
            continue;
        }
        for (key, _) in &memberships {
            let Some(parts) = key::key_parts(&key[ROLE_MEMBERSHIP_PREFIX.len()..], 2) else {
                return Err(KvError::CorruptRow("role membership key is incomplete".into()).into());
            };
            if parts[0] == current {
                if parts[1] == role {
                    return Ok(true);
                }
                pending.push(parts[1].to_string());
            }
        }
    }
    Ok(false)
}

/// Whether `member` holds the privileges of `role` — `PostgreSQL`'s
/// `has_privs_of_role`, the predicate a row-level-security policy's `TO` list
/// is matched with.
///
/// Deliberately *not* [`role_can_set`], and the two must not be merged. `SET
/// ROLE` asks which identities a session may assume: it counts every
/// membership and lets the bootstrap superuser assume anything, which is
/// correct for that question. Privilege inheritance asks which rights apply
/// *without* a `SET ROLE`, so it follows a membership only through a role that
/// inherits and gives the bootstrap superuser no shortcut. Answering a policy's
/// `TO` list with the looser predicate would match a permissive policy the
/// session cannot actually exercise, and a permissive policy that matches
/// grants rows.
///
/// One half of `PostgreSQL`'s rule has nothing to read here: a membership
/// record is a bare key with no payload, so a grant made `WITH INHERIT FALSE`
/// is indistinguishable from a plain one and every grant is followed. The
/// `rolinherit` attribute of each role on the path *is* stored, and is
/// honoured — a `NOINHERIT` role contributes only the identity it is.
///
/// `PUBLIC` is not a membership: it is matched where a policy's role list is
/// read, not here.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn role_has_privs_of(kv: &dyn Kv, member: &str, role: &str) -> Result<bool, CatalogError> {
    if member == role {
        return Ok(true);
    }
    let memberships = kv.scan_prefix(ROLE_MEMBERSHIP_PREFIX)?;
    let mut pending = vec![member.to_string()];
    let mut seen = HashSet::new();
    while let Some(current) = pending.pop() {
        if !seen.insert(current.clone()) {
            continue;
        }
        if !role_inherits(kv, &current)? {
            continue;
        }
        for (key, _) in &memberships {
            let Some(parts) = key::key_parts(&key[ROLE_MEMBERSHIP_PREFIX.len()..], 2) else {
                return Err(KvError::CorruptRow("role membership key is incomplete".into()).into());
            };
            if parts[0] == current {
                if parts[1] == role {
                    return Ok(true);
                }
                pending.push(parts[1].to_string());
            }
        }
    }
    Ok(false)
}

/// Whether `name`'s stored `rolinherit` is set. A role that does not exist
/// inherits nothing, which is the same answer as `NOINHERIT` for every caller
/// of [`role_has_privs_of`].
fn role_inherits(kv: &dyn Kv, name: &str) -> Result<bool, CatalogError> {
    match get_role(kv, name) {
        Ok(role) => Ok(role.attributes.has(RoleAttribute::Inherit)),
        Err(CatalogError::UndefinedObject(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Look up a role by name.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn get_role(kv: &dyn Kv, name: &str) -> Result<Role, CatalogError> {
    if name == "public" {
        return Ok(Role {
            name: "public".into(),
            can_login: true,
            attributes: RoleAttributes::default(),
        });
    }
    if let Some(role) = builtin_role(name) {
        return Ok(role);
    }
    let bytes = kv
        .get(&role_key(name))?
        .ok_or_else(|| CatalogError::UndefinedObject(name.to_string()))?;
    deserialize_role(&bytes)
}

/// Whether a name may stand in a role position: it has a stored record, or it
/// is one of the two roles every cluster has without one.
///
/// `PUBLIC` and the bootstrap superuser hold no `pg_authid` row. Validating
/// either against stored records would reject the two names every cluster
/// always answers for — `GRANT … TO PUBLIC`, and a grant to the role an
/// unauthenticated session acts as.
///
/// Whether a *pseudo*-role is admissible in a particular position is not
/// settled here: `PUBLIC` is a grantee of privileges and never a member of
/// anything, and only the caller knows which of the two it is asking about.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn role_is_nameable(kv: &dyn Kv, name: &str) -> Result<bool, CatalogError> {
    Ok(name == PUBLIC_ROLE || name == BOOTSTRAP_ROLE || role_exists(kv, name)?)
}

/// Return whether a role exists.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn role_exists(kv: &dyn Kv, name: &str) -> Result<bool, CatalogError> {
    if name == "public" || builtin_role(name).is_some() {
        return Ok(true);
    }
    Ok(kv.get(&role_key(name))?.is_some())
}

/// List the `pg_authid` rows a fresh cluster contains plus stored roles.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_roles(kv: &dyn Kv) -> Result<Vec<Role>, CatalogError> {
    let mut roles = std::iter::once(BOOTSTRAP_ROLE)
        .chain(PREDEFINED_ROLES.iter().map(|(name, _)| *name))
        .filter_map(builtin_role)
        .collect::<Vec<_>>();
    for (_, bytes) in kv.scan_prefix(ROLE_PREFIX)? {
        let role = deserialize_role(&bytes)?;
        if role.name != "public" && builtin_role(&role.name).is_none() {
            roles.push(role);
        }
    }
    roles.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(roles)
}

/// Drop a role metadata row.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_role(kv: &dyn Kv, name: &str) -> Result<(), CatalogError> {
    let ops = drop_role_ops(kv, name)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for dropping a role without writing.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_role_ops(kv: &dyn Kv, name: &str) -> Result<Vec<WriteOp>, CatalogError> {
    if name == "public" {
        return Err(CatalogError::UndefinedObject(name.to_string()));
    }
    let _ = get_role(kv, name)?;
    let mut ops = vec![WriteOp::Delete {
        key: role_key(name),
    }];
    for (key, privilege) in scan_table_privileges(kv)? {
        if privilege.grantee == name {
            ops.push(WriteOp::Delete { key });
        }
    }
    for (key, privilege) in scan_column_privileges(kv)? {
        if privilege.grantee == name {
            ops.push(WriteOp::Delete { key });
        }
    }
    for (key, privilege) in scan_default_table_privileges(kv)? {
        if privilege.owner == name || privilege.grantee == name {
            ops.push(WriteOp::Delete { key });
        }
    }
    for (key, _) in kv.scan_prefix(SCHEMA_PRIVILEGE_PREFIX)? {
        let Some(parts) = key::key_parts(&key[SCHEMA_PRIVILEGE_PREFIX.len()..], 3) else {
            return Err(KvError::CorruptRow("schema privilege key is incomplete".into()).into());
        };
        if parts[1] == name {
            ops.push(WriteOp::Delete { key });
        }
    }
    for (key, _) in kv.scan_prefix(ROLE_MEMBERSHIP_PREFIX)? {
        let Some(parts) = key::key_parts(&key[ROLE_MEMBERSHIP_PREFIX.len()..], 2) else {
            return Err(KvError::CorruptRow("role membership key is incomplete".into()).into());
        };
        if parts.contains(&name) {
            ops.push(WriteOp::Delete { key });
        }
    }
    Ok(ops)
}

/// The table privileges `PostgreSQL` 18 recognises, which is what `ALL
/// PRIVILEGES` on a relation names.
///
/// Public because it is also the set a privilege *question* may be asked about:
/// `has_table_privilege` must tell a name that could have been granted apart
/// from one that could not, and deriving that list a second time is how the two
/// would drift.
pub const TABLE_PRIVILEGES: &[&str] = &[
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "TRUNCATE",
    "REFERENCES",
    "TRIGGER",
    "MAINTAIN",
];

/// Build write ops for recording table privilege grants.
///
/// # Errors
///
/// Returns undefined-table, undefined-object, or storage/corruption errors.
pub fn grant_table_privileges_ops(
    kv: &dyn Kv,
    table: &RelationName,
    grantees: &[String],
    privileges: &[String],
) -> Result<Vec<WriteOp>, CatalogError> {
    table_privilege_ops(kv, table, grantees, privileges, true)
}

/// Build write ops for removing recorded table privilege grants.
///
/// # Errors
///
/// Returns undefined-table, undefined-object, or storage/corruption errors.
pub fn revoke_table_privileges_ops(
    kv: &dyn Kv,
    table: &RelationName,
    grantees: &[String],
    privileges: &[String],
) -> Result<Vec<WriteOp>, CatalogError> {
    table_privilege_ops(kv, table, grantees, privileges, false)
}

/// Build write ops for `ALTER DEFAULT PRIVILEGES … ON TABLES`.
///
/// An empty `schemas` list writes the cluster-wide default. Otherwise each
/// named schema receives its own default, which combines with the global one
/// when a relation is created there.
///
/// # Errors
///
/// Returns undefined-object or undefined-schema errors, or catalog storage
/// failures.
pub fn alter_default_table_privileges_ops(
    kv: &dyn Kv,
    owner: &str,
    schemas: &[String],
    grantees: &[String],
    privileges: &[String],
    grant: bool,
) -> Result<Vec<WriteOp>, CatalogError> {
    if !role_is_nameable(kv, owner)? {
        return Err(CatalogError::UndefinedObject(owner.to_string()));
    }
    let scopes: Vec<Option<&str>> = if schemas.is_empty() {
        vec![None]
    } else {
        schemas.iter().map(|schema| Some(schema.as_str())).collect()
    };
    for schema in schemas {
        if !schema_exists(kv, schema)? {
            return Err(CatalogError::UndefinedSchema(schema.clone()));
        }
    }
    let mut ops = Vec::new();
    for grantee in grantees {
        if !role_is_nameable(kv, grantee)? {
            return Err(CatalogError::UndefinedObject(grantee.clone()));
        }
        for privilege in expand_table_privileges(privileges) {
            for schema in &scopes {
                let key = default_table_privilege_key(owner, *schema, grantee, &privilege);
                ops.push(if grant {
                    WriteOp::Put {
                        value: serialize_default_table_privilege(
                            owner, *schema, grantee, &privilege, true,
                        ),
                        key,
                    }
                } else {
                    WriteOp::Put {
                        value: serialize_default_table_privilege(
                            owner, *schema, grantee, &privilege, false,
                        ),
                        key,
                    }
                });
            }
        }
    }
    Ok(ops)
}

/// The table privilege defaults that apply when `owner` creates a relation in
/// `schema`. Schema-local defaults add to the global defaults.
///
/// # Errors
///
/// Returns storage or corruption errors from the catalog KV seam.
pub fn default_table_privileges_of(
    kv: &dyn Kv,
    owner: &str,
    schema: &str,
) -> Result<Vec<DefaultTablePrivilege>, CatalogError> {
    let mut prefix = DEFAULT_TABLE_PRIVILEGE_PREFIX.to_vec();
    key::push_key_part(&mut prefix, owner);
    Ok(kv
        .scan_prefix(&prefix)?
        .into_iter()
        .map(|(_, bytes)| deserialize_default_table_privilege(&bytes))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|privilege| {
            privilege.owner == owner
                && (privilege.schema.is_none() || privilege.schema.as_deref() == Some(schema))
        })
        .collect())
}

fn table_privilege_ops(
    kv: &dyn Kv,
    table: &RelationName,
    grantees: &[String],
    privileges: &[String],
    grant: bool,
) -> Result<Vec<WriteOp>, CatalogError> {
    // `GRANT … ON` names a relation, not specifically a table: a view is a
    // grantable object in its own right and the regression suite grants on one.
    // Whether the relation is there at all is the caller's question, not this
    // one's — the engine synthesises relations that hold no record under any
    // key here, and `PostgreSQL` grants on those too.
    let mut ops = Vec::new();
    for grantee in grantees {
        if !role_is_nameable(kv, grantee)? {
            return Err(CatalogError::UndefinedObject(grantee.clone()));
        }
        for privilege in expand_table_privileges(privileges) {
            let key = table_privilege_key(table, grantee, &privilege);
            ops.push(if grant {
                WriteOp::Put {
                    value: serialize_table_privilege(table, grantee, &privilege),
                    key,
                }
            } else {
                WriteOp::Delete { key }
            });
        }
    }
    Ok(ops)
}

/// Resolve a statement's privilege list to the exact set of names a grant is
/// stored under.
///
/// `ALL` is expanded at *both* grant and revoke time rather than stored as a
/// name of its own, so the two spellings compose the way `PostgreSQL`'s
/// per-privilege ACL bits do: `GRANT ALL` then `REVOKE SELECT` leaves the other
/// seven behind, and `GRANT SELECT` then `REVOKE ALL` removes it. A stored
/// `ALL` row would answer neither question.
fn expand_table_privileges(privileges: &[String]) -> Vec<String> {
    privileges
        .iter()
        .flat_map(|privilege| {
            let privilege = privilege.trim().to_ascii_uppercase();
            if privilege == "ALL" || privilege == "ALL PRIVILEGES" {
                TABLE_PRIVILEGES.iter().map(|p| (*p).to_string()).collect()
            } else {
                vec![privilege]
            }
        })
        .collect()
}

/// List recorded table privileges.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_table_privileges(kv: &dyn Kv) -> Result<Vec<TablePrivilege>, CatalogError> {
    scan_table_privileges(kv).map(|entries| {
        entries
            .into_iter()
            .map(|(_, privilege)| privilege)
            .collect()
    })
}

/// Every recorded grant on one relation.
///
/// This scans the relation's own key range rather than filtering
/// [`list_table_privileges`], because an enforcement check runs on every
/// statement, once per relation the statement touches. Filtering the full list
/// would make each of those checks cost the whole cluster's grants — a table
/// nobody has granted anything on would still pay for every other relation's
/// ACL. The key layout puts schema and name first precisely so this is a range.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn table_privileges_of(
    kv: &dyn Kv,
    relation: &RelationName,
) -> Result<Vec<TablePrivilege>, CatalogError> {
    kv.scan_prefix(&table_privilege_relation_prefix(relation))?
        .into_iter()
        .map(|(_, bytes)| deserialize_table_privilege(&bytes))
        .collect()
}

/// Whether `grantee` itself holds `privilege` on `relation`.
///
/// A point lookup on the one key that would record it. This answers only the
/// literal question — it does not consider `PUBLIC`, role membership, or
/// ownership, all of which the caller composes on top.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn has_stored_table_privilege(
    kv: &dyn Kv,
    relation: &RelationName,
    grantee: &str,
    privilege: &str,
) -> Result<bool, CatalogError> {
    let privilege = privilege.to_ascii_uppercase();
    Ok(kv
        .get(&table_privilege_key(relation, grantee, &privilege))?
        .is_some())
}

/// The privileges `PostgreSQL` 18 lets a grant name a *column* of, which is
/// what `ALL PRIVILEGES (column)` names.
///
/// A strict subset of [`TABLE_PRIVILEGES`]: `src/include/utils/acl.h` defines
/// `ACL_ALL_RIGHTS_COLUMN` as `INSERT|SELECT|UPDATE|REFERENCES`, and the grant
/// path rejects every other bit with "invalid privilege type … for column".
/// `DELETE`, `TRUNCATE`, `TRIGGER` and `MAINTAIN` act on a whole relation, so
/// naming one column of it is meaningless rather than merely unimplemented.
///
/// Public for the reason [`TABLE_PRIVILEGES`] is: the set a grant can record is
/// also the set a privilege *question* may be asked about, and deriving it a
/// second time is how the two would drift.
pub const COLUMN_PRIVILEGES: &[&str] = &["SELECT", "INSERT", "UPDATE", "REFERENCES"];

/// Build write ops for recording column privilege grants.
///
/// Every named grantee takes every named privilege on every named column, which
/// is the cross product `GRANT SELECT, UPDATE (a, b) ON t TO r, s` sets bits
/// for.
///
/// Whether the columns are columns of the relation is the caller's question,
/// not this one's — the same call that parsed the statement holds the relation
/// and can say `42703` about a name that is not there, and this seam builds
/// grants for relations it holds no record of at all.
///
/// # Errors
///
/// Returns undefined-object for a grantee no role holds, or storage/corruption
/// errors from the catalog KV seam.
pub fn grant_column_privileges_ops(
    kv: &dyn Kv,
    table: &RelationName,
    columns: &[String],
    grantees: &[String],
    privileges: &[String],
) -> Result<Vec<WriteOp>, CatalogError> {
    column_privilege_ops(kv, table, columns, grantees, privileges, true)
}

/// Build write ops for removing recorded column privilege grants.
///
/// # Errors
///
/// Returns undefined-object for a grantee no role holds, or storage/corruption
/// errors from the catalog KV seam.
pub fn revoke_column_privileges_ops(
    kv: &dyn Kv,
    table: &RelationName,
    columns: &[String],
    grantees: &[String],
    privileges: &[String],
) -> Result<Vec<WriteOp>, CatalogError> {
    column_privilege_ops(kv, table, columns, grantees, privileges, false)
}

fn column_privilege_ops(
    kv: &dyn Kv,
    table: &RelationName,
    columns: &[String],
    grantees: &[String],
    privileges: &[String],
    grant: bool,
) -> Result<Vec<WriteOp>, CatalogError> {
    let privileges = expand_column_privileges(privileges);
    let mut ops = Vec::new();
    for grantee in grantees {
        if !role_is_nameable(kv, grantee)? {
            return Err(CatalogError::UndefinedObject(grantee.clone()));
        }
        for column in columns {
            for privilege in &privileges {
                let key = column_privilege_key(table, column, grantee, privilege);
                ops.push(if grant {
                    WriteOp::Put {
                        value: serialize_column_privilege(table, column, grantee, privilege),
                        key,
                    }
                } else {
                    WriteOp::Delete { key }
                });
            }
        }
    }
    Ok(ops)
}

/// Resolve a statement's privilege list to the exact set of names a column
/// grant is stored under.
///
/// The counterpart of the relation-level expansion, and `ALL` is expanded at
/// *both* grant and revoke time for the same reason: a stored `ALL` row would
/// answer neither `GRANT ALL (a)` then `REVOKE SELECT (a)` nor
/// `GRANT SELECT (a)` then `REVOKE ALL (a)`.
///
/// It expands to [`COLUMN_PRIVILEGES`], not to the relation-level set, so
/// `GRANT ALL (a)` records the four bits `PostgreSQL` records and not four
/// more that no column grant can carry.
#[must_use]
pub fn expand_column_privileges(privileges: &[String]) -> Vec<String> {
    privileges
        .iter()
        .flat_map(|privilege| {
            let privilege = privilege.trim().to_ascii_uppercase();
            if privilege == "ALL" || privilege == "ALL PRIVILEGES" {
                COLUMN_PRIVILEGES.iter().map(|p| (*p).to_string()).collect()
            } else {
                vec![privilege]
            }
        })
        .collect()
}

/// List every recorded column privilege in the cluster.
///
/// This is the whole-namespace scan `information_schema.column_privileges`
/// wants. An enforcement check wants [`column_privileges_of`] or
/// [`has_stored_column_privilege`] instead.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_column_privileges(kv: &dyn Kv) -> Result<Vec<ColumnPrivilege>, CatalogError> {
    scan_column_privileges(kv).map(|entries| {
        entries
            .into_iter()
            .map(|(_, privilege)| privilege)
            .collect()
    })
}

/// Every recorded column grant on one relation.
///
/// A range scan rather than a filter over [`list_column_privileges`], for the
/// reason [`table_privileges_of`] is: a per-statement check must not cost the
/// whole cluster's grants. The key layout puts schema and name first so this
/// is one range, and the column next so a single column's grants are a
/// sub-range of it.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn column_privileges_of(
    kv: &dyn Kv,
    table: &RelationName,
) -> Result<Vec<ColumnPrivilege>, CatalogError> {
    kv.scan_prefix(&column_privilege_relation_prefix(table))?
        .into_iter()
        .map(|(_, bytes)| deserialize_column_privilege(&bytes))
        .collect()
}

/// Whether `grantee` itself holds `privilege` on `table`.`column`.
///
/// A point lookup on the one key that would record it. Like
/// [`has_stored_table_privilege`] it answers the literal question only: it
/// considers neither `PUBLIC`, role membership nor ownership, and it does not
/// consider the relation-level grant that would cover the column as well. The
/// caller composes all of those on top.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn has_stored_column_privilege(
    kv: &dyn Kv,
    table: &RelationName,
    column: &str,
    grantee: &str,
    privilege: &str,
) -> Result<bool, CatalogError> {
    let privilege = privilege.to_ascii_uppercase();
    Ok(kv
        .get(&column_privilege_key(table, column, grantee, &privilege))?
        .is_some())
}

/// Build write ops deleting every grant recorded against one column.
///
/// `ALTER TABLE … DROP COLUMN` needs this: the keys carry the column name, and
/// `ADD COLUMN` can hand that name back, so a stranded grant would authorize a
/// column its grantee was never given anything on. Dropping the whole relation
/// needs no call here — [`drop_table_ops`] and [`drop_view_ops`] already sweep
/// the relation's entire column-grant range.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn drop_column_privileges_ops(
    kv: &dyn Kv,
    table: &RelationName,
    column: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    Ok(kv
        .scan_prefix(&column_privilege_column_prefix(table, column))?
        .into_iter()
        .map(|(key, _)| WriteOp::Delete { key })
        .collect())
}

/// Build write ops for schema privilege grants.
///
/// # Errors
///
/// Returns an error for unknown schemas or roles, or catalog storage failures.
pub fn grant_schema_privileges_ops(
    kv: &dyn Kv,
    schemas: &[String],
    grantees: &[String],
    privileges: &[String],
) -> Result<Vec<WriteOp>, CatalogError> {
    schema_privilege_ops(kv, schemas, grantees, privileges, true)
}

/// Build write ops for schema privilege revocations.
///
/// # Errors
///
/// Returns an error for unknown schemas or roles, or catalog storage failures.
pub fn revoke_schema_privileges_ops(
    kv: &dyn Kv,
    schemas: &[String],
    grantees: &[String],
    privileges: &[String],
) -> Result<Vec<WriteOp>, CatalogError> {
    schema_privilege_ops(kv, schemas, grantees, privileges, false)
}

fn schema_privilege_ops(
    kv: &dyn Kv,
    schemas: &[String],
    grantees: &[String],
    privileges: &[String],
    grant: bool,
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut ops = Vec::new();
    for schema in schemas {
        if !schema_exists(kv, schema)? {
            return Err(CatalogError::UndefinedSchema(schema.clone()));
        }
        for grantee in grantees {
            if !role_is_nameable(kv, grantee)? {
                return Err(CatalogError::UndefinedObject(grantee.clone()));
            }
            for privilege in expand_schema_privileges(privileges) {
                let key = schema_privilege_key(schema, grantee, privilege);
                ops.push(if grant {
                    WriteOp::Put {
                        key,
                        value: Vec::new(),
                    }
                } else {
                    WriteOp::Delete { key }
                });
            }
        }
    }
    Ok(ops)
}

fn expand_schema_privileges(privileges: &[String]) -> Vec<&str> {
    privileges
        .iter()
        .flat_map(|privilege| {
            if privilege.eq_ignore_ascii_case("all") {
                vec!["CREATE", "USAGE"]
            } else {
                vec![privilege.as_str()]
            }
        })
        .collect()
}

/// Whether a role has a schema privilege through ownership, PUBLIC, or role membership.
///
/// # Errors
///
/// Returns an error for an unknown schema or catalog storage failures.
pub fn has_schema_privilege(
    kv: &dyn Kv,
    schema: &str,
    role: &str,
    privilege: &str,
) -> Result<bool, CatalogError> {
    let owner = list_schemas(kv)?
        .into_iter()
        .find(|item| item.name == schema)
        .ok_or_else(|| CatalogError::UndefinedSchema(schema.to_string()))?
        .owner;
    if role == BOOTSTRAP_ROLE || role_can_set(kv, role, &owner)? {
        return Ok(true);
    }
    let privilege = privilege.to_ascii_uppercase();
    // `initdb` grants `USAGE` on each of `BOOTSTRAP_SCHEMAS` to `PUBLIC`, so
    // `postgres:18.4` reports `nspacl` as `{postgres=UC/postgres,=U/postgres}`
    // for `pg_catalog` and `{pg_database_owner=UC/…,=U/…}` for `public` on a
    // fresh cluster. crabka synthesises those schemas instead of storing them,
    // so no stored grant carries that, and it has to be answered here. Only
    // `USAGE`: `PostgreSQL` 15 removed `PUBLIC`'s `CREATE` on `public`, and
    // `CREATE` was never granted on the two system schemas.
    if privilege == "USAGE" && BOOTSTRAP_SCHEMAS.iter().any(|(name, _)| *name == schema) {
        return Ok(true);
    }
    if kv
        .get(&schema_privilege_key(schema, "public", &privilege))?
        .is_some()
    {
        return Ok(true);
    }
    for candidate in list_roles(kv)? {
        if role_can_set(kv, role, &candidate.name)?
            && kv
                .get(&schema_privilege_key(schema, &candidate.name, &privilege))?
                .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn scan_table_privileges(kv: &dyn Kv) -> Result<Vec<(Vec<u8>, TablePrivilege)>, CatalogError> {
    kv.scan_prefix(TABLE_PRIVILEGE_PREFIX)?
        .into_iter()
        .map(|(key, bytes)| Ok((key, deserialize_table_privilege(&bytes)?)))
        .collect()
}

fn scan_default_table_privileges(
    kv: &dyn Kv,
) -> Result<Vec<(Vec<u8>, DefaultTablePrivilege)>, CatalogError> {
    kv.scan_prefix(DEFAULT_TABLE_PRIVILEGE_PREFIX)?
        .into_iter()
        .map(|(key, bytes)| Ok((key, deserialize_default_table_privilege(&bytes)?)))
        .collect()
}

fn scan_column_privileges(kv: &dyn Kv) -> Result<Vec<(Vec<u8>, ColumnPrivilege)>, CatalogError> {
    kv.scan_prefix(COLUMN_PRIVILEGE_PREFIX)?
        .into_iter()
        .map(|(key, bytes)| Ok((key, deserialize_column_privilege(&bytes)?)))
        .collect()
}

fn role_key(name: &str) -> Vec<u8> {
    let mut key = ROLE_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key
}

fn role_membership_key(member: &str, role: &str) -> Vec<u8> {
    let mut key = ROLE_MEMBERSHIP_PREFIX.to_vec();
    key::push_key_part(&mut key, member);
    key::push_key_part(&mut key, role);
    key
}

/// The key range holding exactly one relation's grants.
///
/// Every full key is built by extending this, so the range a scan walks and the
/// key a write lands on cannot drift apart. The parts are length-prefixed, so
/// `t`'s range does not swallow `t2`'s.
fn table_privilege_relation_prefix(table: &RelationName) -> Vec<u8> {
    let mut key = TABLE_PRIVILEGE_PREFIX.to_vec();
    for part in [&table.schema, &table.name] {
        key::push_key_part(&mut key, part);
    }
    key
}

fn table_privilege_key(table: &RelationName, grantee: &str, privilege: &str) -> Vec<u8> {
    let mut key = table_privilege_relation_prefix(table);
    for part in [grantee, privilege] {
        key::push_key_part(&mut key, part);
    }
    key
}

fn default_table_privilege_key(
    owner: &str,
    schema: Option<&str>,
    grantee: &str,
    privilege: &str,
) -> Vec<u8> {
    let mut key = DEFAULT_TABLE_PRIVILEGE_PREFIX.to_vec();
    for part in [owner, schema.unwrap_or(""), grantee, privilege] {
        key::push_key_part(&mut key, part);
    }
    key
}

fn owner_table_privilege_revoke_relation_prefix(table: &RelationName) -> Vec<u8> {
    let mut key = OWNER_TABLE_PRIVILEGE_REVOKE_PREFIX.to_vec();
    for part in [&table.schema, &table.name] {
        key::push_key_part(&mut key, part);
    }
    key
}

fn owner_table_privilege_revoke_key(table: &RelationName, privilege: &str) -> Vec<u8> {
    let mut key = owner_table_privilege_revoke_relation_prefix(table);
    key::push_key_part(&mut key, privilege);
    key
}

/// Build the marker that makes an explicit default ACL revocation from a table
/// owner effective for a table the owner creates.
#[must_use]
pub fn revoke_owner_table_privilege_op(table: &RelationName, privilege: &str) -> WriteOp {
    WriteOp::Put {
        key: owner_table_privilege_revoke_key(table, privilege),
        value: Vec::new(),
    }
}

/// Build the deletion that temporarily restores an owner's implicit privilege.
#[must_use]
pub fn restore_owner_table_privilege_op(table: &RelationName, privilege: &str) -> WriteOp {
    WriteOp::Delete {
        key: owner_table_privilege_revoke_key(table, privilege),
    }
}

/// Whether an explicit default ACL revoked this privilege from the table owner.
///
/// # Errors
///
/// Returns storage errors from the catalog KV seam.
pub fn owner_table_privilege_is_revoked(
    kv: &dyn Kv,
    table: &RelationName,
    privilege: &str,
) -> Result<bool, CatalogError> {
    Ok(kv
        .get(&owner_table_privilege_revoke_key(table, privilege))?
        .is_some())
}

/// The key range holding exactly one relation's column grants.
///
/// A namespace of its own rather than a deeper part of the table-privilege key,
/// so a relation-level scan cannot pick up column grants and vice versa. As
/// there, every full key extends this one and the parts are length-prefixed, so
/// `t`'s range does not swallow `t2`'s.
fn column_privilege_relation_prefix(table: &RelationName) -> Vec<u8> {
    let mut key = COLUMN_PRIVILEGE_PREFIX.to_vec();
    for part in [&table.schema, &table.name] {
        key::push_key_part(&mut key, part);
    }
    key
}

/// The key range holding exactly one column's grants — the sub-range a dropped
/// column's cleanup deletes.
fn column_privilege_column_prefix(table: &RelationName, column: &str) -> Vec<u8> {
    let mut key = column_privilege_relation_prefix(table);
    key::push_key_part(&mut key, column);
    key
}

fn column_privilege_key(
    table: &RelationName,
    column: &str,
    grantee: &str,
    privilege: &str,
) -> Vec<u8> {
    let mut key = column_privilege_column_prefix(table, column);
    for part in [grantee, privilege] {
        key::push_key_part(&mut key, part);
    }
    key
}

fn schema_privilege_key(schema: &str, grantee: &str, privilege: &str) -> Vec<u8> {
    let mut key = SCHEMA_PRIVILEGE_PREFIX.to_vec();
    for part in [schema, grantee, privilege] {
        key::push_key_part(&mut key, part);
    }
    key
}

fn serialize_role(name: &str, can_login: bool, attributes: RoleAttributes) -> Vec<u8> {
    let mut bytes = vec![2, u8::from(can_login), attributes.to_bits()];
    bytes.extend_from_slice(name.as_bytes());
    bytes
}

fn deserialize_role(bytes: &[u8]) -> Result<Role, CatalogError> {
    if bytes.len() < 3 || bytes[0] != 2 {
        return Err(KvError::CorruptRow("role record has invalid version".into()).into());
    }
    let name = std::str::from_utf8(&bytes[3..])
        .map_err(|_| KvError::CorruptRow("role name is not utf8".into()))?
        .to_string();
    Ok(Role {
        name,
        can_login: bytes[1] != 0,
        attributes: RoleAttributes::from_bits(bytes[2]),
    })
}

fn serialize_table_privilege(table: &RelationName, grantee: &str, privilege: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    for part in [&table.schema, &table.name, grantee, privilege] {
        key::push_key_part(&mut bytes, part);
    }
    bytes
}

fn deserialize_table_privilege(bytes: &[u8]) -> Result<TablePrivilege, CatalogError> {
    let parts = key::key_parts(bytes, 4)
        .ok_or_else(|| KvError::CorruptRow("table privilege has invalid shape".into()))?;
    let [schema, table, grantee, privilege] = parts[..] else {
        return Err(KvError::CorruptRow("table privilege has invalid shape".into()).into());
    };
    Ok(TablePrivilege {
        table: RelationName::new(schema, table),
        grantee: grantee.to_string(),
        privilege: privilege.to_string(),
    })
}

fn serialize_default_table_privilege(
    owner: &str,
    schema: Option<&str>,
    grantee: &str,
    privilege: &str,
    grant: bool,
) -> Vec<u8> {
    let mut bytes = vec![u8::from(grant)];
    for part in [owner, schema.unwrap_or(""), grantee, privilege] {
        key::push_key_part(&mut bytes, part);
    }
    bytes
}

fn deserialize_default_table_privilege(
    bytes: &[u8],
) -> Result<DefaultTablePrivilege, CatalogError> {
    let Some((&grant, parts)) = bytes.split_first() else {
        return Err(KvError::CorruptRow("default table privilege has invalid shape".into()).into());
    };
    let parts = key::key_parts(parts, 4)
        .ok_or_else(|| KvError::CorruptRow("default table privilege has invalid shape".into()))?;
    let [owner, schema, grantee, privilege] = parts[..] else {
        return Err(KvError::CorruptRow("default table privilege has invalid shape".into()).into());
    };
    Ok(DefaultTablePrivilege {
        owner: owner.to_string(),
        schema: (!schema.is_empty()).then(|| schema.to_string()),
        grantee: grantee.to_string(),
        privilege: privilege.to_string(),
        grant: grant != 0,
    })
}

fn serialize_column_privilege(
    table: &RelationName,
    column: &str,
    grantee: &str,
    privilege: &str,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    for part in [&table.schema, &table.name, column, grantee, privilege] {
        key::push_key_part(&mut bytes, part);
    }
    bytes
}

fn deserialize_column_privilege(bytes: &[u8]) -> Result<ColumnPrivilege, CatalogError> {
    let parts = key::key_parts(bytes, 5)
        .ok_or_else(|| KvError::CorruptRow("column privilege has invalid shape".into()))?;
    let [schema, table, column, grantee, privilege] = parts[..] else {
        return Err(KvError::CorruptRow("column privilege has invalid shape".into()).into());
    };
    Ok(ColumnPrivilege {
        table: RelationName::new(schema, table),
        column: column.to_string(),
        grantee: grantee.to_string(),
        privilege: privilege.to_string(),
    })
}

// ── User-defined types ────────────────────────────────────────────────────────

/// The write batch that records a new user-defined type and allocates its oid.
///
/// Rejects a duplicate name with `PostgreSQL`'s 42710. The oid counter is a
/// catalog key so oids survive a restart and agree across nodes; the returned
/// [`UserType`] carries the allocated oid so the caller can publish it to the
/// process type registry after the durable catalog commit is accepted.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
pub fn create_user_type_ops(
    kv: &dyn Kv,
    name: &RelationName,
    body: UserTypeBody,
) -> Result<(UserType, Vec<WriteOp>), CatalogError> {
    if get_user_type(kv, name)?.is_some() {
        return Err(CatalogError::DuplicateObject(name.to_string()));
    }
    let oid = read_next_type_oid(kv)?;
    let ty = UserType {
        oid,
        array_oid: crabka_pgtypes::usertype::user_array_oid(oid),
        schema: name.schema.clone(),
        name: name.name.clone(),
        body,
    };
    let mut ops = vec![
        WriteOp::Put {
            key: key::user_type_key(&name.schema, &name.name),
            value: serialize_user_type(&ty),
        },
        WriteOp::Put {
            key: key::meta_next_type_oid_key(),
            value: U32::new(oid + USER_TYPE_OID_STRIDE).as_bytes().to_vec(),
        },
    ];
    ops.extend(creation_order_ops(kv, name)?);
    Ok((ty, ops))
}

/// The write batch that replaces an existing type's definition in place,
/// preserving its oid (`ALTER TYPE` / `ALTER DOMAIN`).
///
/// # Errors
///
/// Returns catalog storage or corruption errors while checking for a matching
/// legacy record.
pub fn put_user_type_ops(kv: &dyn Kv, ty: &UserType) -> Result<Vec<WriteOp>, CatalogError> {
    let mut ops = matching_legacy_user_type_delete(kv, ty)?
        .into_iter()
        .collect::<Vec<_>>();
    ops.push(WriteOp::Put {
        key: key::user_type_key(&ty.schema, &ty.name),
        value: serialize_user_type(ty),
    });
    Ok(ops)
}

/// The write batch that renames a type, keeping its oid.
///
/// # Errors
///
/// Returns catalog storage or corruption errors while migrating legacy keys.
pub fn rename_user_type_ops(
    kv: &dyn Kv,
    old: &UserType,
    renamed: &UserType,
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut ops = drop_user_type_ops(kv, old)?;
    ops.extend(put_user_type_ops(kv, renamed)?);
    ops.extend(move_creation_order_ops(
        kv,
        &RelationName::new(&old.schema, &old.name),
        &RelationName::new(&renamed.schema, &renamed.name),
    )?);
    Ok(ops)
}

/// The write batch that forgets a type.
///
/// # Errors
///
/// Returns catalog storage or corruption errors while checking for a matching
/// legacy record.
pub fn drop_user_type_ops(kv: &dyn Kv, ty: &UserType) -> Result<Vec<WriteOp>, CatalogError> {
    let mut ops = vec![WriteOp::Delete {
        key: key::user_type_key(&ty.schema, &ty.name),
    }];
    ops.push(drop_creation_order_op(&RelationName::new(
        &ty.schema, &ty.name,
    )));
    let oid = ty.oid.to_string();
    ops.push(set_comment_op("type", CommentObject::Named(&oid), None));
    ops.push(set_comment_op("domain", CommentObject::Named(&oid), None));
    ops.extend(matching_legacy_user_type_delete(kv, ty)?);
    Ok(ops)
}

fn matching_legacy_user_type_delete(
    kv: &dyn Kv,
    target: &UserType,
) -> Result<Option<WriteOp>, CatalogError> {
    let key = key::legacy_user_type_key(&target.qualified_name());
    let Some(bytes) = kv.get(&key)? else {
        return Ok(None);
    };
    let stored = deserialize_user_type(&bytes)?;
    Ok(
        (stored.oid == target.oid && stored.schema == target.schema && stored.name == target.name)
            .then_some(WriteOp::Delete { key }),
    )
}

/// Look up a user-defined type by name.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn get_user_type(kv: &dyn Kv, name: &RelationName) -> Result<Option<UserType>, CatalogError> {
    if let Some(bytes) = kv.get(&key::user_type_key(&name.schema, &name.name))? {
        return Ok(Some(deserialize_user_type(&bytes)?));
    }
    let Some(bytes) = kv.get(&key::legacy_user_type_key(&name.to_string()))? else {
        return Ok(None);
    };
    let ty = deserialize_user_type(&bytes)?;
    if ty.schema == name.schema && ty.name == name.name {
        Ok(Some(ty))
    } else {
        Ok(None)
    }
}

/// Every user-defined type in the catalog, ordered by oid.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_user_types(kv: &dyn Kv) -> Result<Vec<UserType>, CatalogError> {
    user_type_records(kv)?
        .into_values()
        .map(|(_, bytes)| deserialize_user_type(&bytes).map_err(CatalogError::from))
        .collect()
}

fn user_type_records(kv: &dyn Kv) -> Result<BTreeMap<u32, (bool, Vec<u8>)>, CatalogError> {
    let mut records = BTreeMap::<u32, (bool, Vec<u8>)>::new();
    for (key, bytes) in kv.scan_prefix(&key::user_type_prefix())? {
        let structured = key::user_type_key_parts(&key).is_some();
        let oid = u32::from_be_bytes(
            bytes
                .get(..4)
                .ok_or_else(|| KvError::CorruptRow("truncated user type oid".into()))?
                .try_into()
                .expect("four bytes fit u32"),
        );
        match records.entry(oid) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert((structured, bytes));
            }
            std::collections::btree_map::Entry::Occupied(mut entry)
                if structured && !entry.get().0 =>
            {
                entry.insert((true, bytes));
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
    Ok(records)
}

/// Hydrate the process type registry from durable records in dependency order.
///
/// Unlike [`list_user_types`], this is explicitly the stateful startup path.
///
/// # Errors
///
/// Returns storage/corruption errors, including an unresolved referenced oid.
pub fn hydrate_user_types(kv: &dyn Kv) -> Result<Vec<UserType>, CatalogError> {
    hydrate_user_types_with(kv, &|_| None)
}

/// Hydrate durable user types, resolving catalog-owned types supplied by the
/// caller as dependencies.
///
/// Relation rowtypes are owned by `pgexec`, not this crate, but PostgreSQL
/// permits a domain or composite field to name one.  The caller supplies only
/// its own catalog types, so unrelated process-global registrations cannot
/// make an invalid durable record appear valid.
///
/// # Errors
///
/// Returns storage/corruption errors, including an unresolved referenced oid.
pub fn hydrate_user_types_with(
    kv: &dyn Kv,
    resolve_catalog_type: &dyn Fn(u32) -> Option<ColumnType>,
) -> Result<Vec<UserType>, CatalogError> {
    let mut pending: Vec<Vec<u8>> = user_type_records(kv)?
        .into_values()
        .map(|(_, bytes)| bytes)
        .collect();
    let mut decoded = BTreeMap::<u32, UserType>::new();
    while !pending.is_empty() {
        let pending_len = pending.len();
        let mut deferred = Vec::new();
        let mut first_unresolved = None;
        for bytes in pending {
            match deserialize_user_type_with(&bytes, &|oid| {
                hydrated_column_type(&decoded, oid).or_else(|| resolve_catalog_type(oid))
            }) {
                Ok(ty) => {
                    decoded.insert(ty.oid, ty);
                }
                Err(serde::UserTypeDecodeError::UnresolvedUserType(oid)) => {
                    first_unresolved.get_or_insert(oid);
                    deferred.push(bytes);
                }
                Err(serde::UserTypeDecodeError::Corrupt(error)) => return Err(error.into()),
            }
        }
        if deferred.is_empty() {
            break;
        }
        if deferred.len() == pending_len {
            let Some(oid) = first_unresolved else {
                return Err(KvError::CorruptRow("unresolved user type dependency".into()).into());
            };
            return Err(KvError::CorruptRow(format!(
                "column type oid {oid} is not present in this catalog"
            ))
            .into());
        }
        pending = deferred;
    }

    let types: Vec<UserType> = decoded.into_values().collect();
    for ty in &types {
        crabka_pgtypes::usertype::replace(ty);
    }
    Ok(types)
}

fn hydrated_column_type(types: &BTreeMap<u32, UserType>, oid: u32) -> Option<ColumnType> {
    types.get(&oid).and_then(UserType::column_type).or_else(|| {
        types
            .get(&oid.checked_sub(3)?)?
            .multirange_type()
            .filter(|ty| ty.oid() == oid)
    })
}

/// The first oid handed out to a user-defined type, and the stride between two
/// of them.
///
/// The stride leaves room for each type's derived relation and array oids. Both
/// values must match `crabka_pgtypes::usertype`.
const FIRST_USER_TYPE_OID: u32 = 300_000;
const USER_TYPE_OID_STRIDE: u32 = 4;

/// New catalogs can coexist in this process (notably independent `SqlEngine`
/// instances in one server). Keep their newly allocated type OIDs disjoint;
/// every chosen value is still written to the catalog's durable counter.
static PROCESS_NEXT_USER_TYPE_OID: AtomicU32 = AtomicU32::new(FIRST_USER_TYPE_OID);

fn read_next_type_oid(kv: &dyn Kv) -> Result<u32, CatalogError> {
    let stored = match kv.get(&key::meta_next_type_oid_key())? {
        Some(b) => {
            let (v, _) = U32::read_from_prefix(b.as_slice())
                .map_err(|_| KvError::CorruptRow("next_type_oid is not u32".into()))?;
            v.get()
        }
        None => FIRST_USER_TYPE_OID,
    };
    PROCESS_NEXT_USER_TYPE_OID
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |next| {
            next.max(stored).checked_add(USER_TYPE_OID_STRIDE)
        })
        .map(|previous| previous.max(stored))
        .map_err(|_| KvError::CorruptRow("user type oid space exhausted".into()).into())
}

// ── Foreign-data wrapper ──────────────────────────────────────────────────────

/// Register a foreign-data wrapper.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
pub fn create_fdw(
    kv: &dyn Kv,
    name: &str,
    options: Vec<(String, String)>,
) -> Result<(), CatalogError> {
    let ops = create_fdw_ops(kv, name, options)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for registering a foreign-data wrapper without writing.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
#[expect(
    clippy::needless_pass_by_value,
    reason = "preserves donor CRUD API ownership for metadata options"
)]
pub fn create_fdw_ops(
    kv: &dyn Kv,
    name: &str,
    options: Vec<(String, String)>,
) -> Result<Vec<WriteOp>, CatalogError> {
    create_fdw_with_routines_ops(kv, name, None, None, options)
}

/// Build the write batch for registering an FDW and its optional routines.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
pub fn create_fdw_with_routines_ops(
    kv: &dyn Kv,
    name: &str,
    handler: Option<&str>,
    validator: Option<&str>,
    options: Vec<(String, String)>,
) -> Result<Vec<WriteOp>, CatalogError> {
    ensure_unique_options(&options)?;
    if kv.get(&key::fdw_key(name))?.is_some() {
        return Err(CatalogError::DuplicateObject(name.to_string()));
    }
    Ok(vec![WriteOp::Put {
        key: key::fdw_key(name),
        value: serialize_fdw(name, handler, validator, &options),
    }])
}

/// Look up a foreign-data wrapper by name.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn get_fdw(kv: &dyn Kv, name: &str) -> Result<ForeignDataWrapper, CatalogError> {
    let bytes = kv
        .get(&key::fdw_key(name))?
        .ok_or_else(|| CatalogError::UndefinedObject(name.to_string()))?;
    Ok(deserialize_fdw(&bytes)?)
}

/// List foreign-data wrappers by name.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn list_fdws(kv: &dyn Kv) -> Result<Vec<ForeignDataWrapper>, CatalogError> {
    let mut wrappers = kv
        .scan_prefix(&key::fdw_prefix())?
        .into_iter()
        .map(|(_, value)| deserialize_fdw(&value).map_err(CatalogError::from))
        .collect::<Result<Vec<_>, _>>()?;
    wrappers.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(wrappers)
}

/// Drop a foreign-data wrapper.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_fdw(kv: &dyn Kv, name: &str) -> Result<(), CatalogError> {
    let ops = drop_fdw_ops(kv, name)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for dropping a foreign-data wrapper without writing.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_fdw_ops(kv: &dyn Kv, name: &str) -> Result<Vec<WriteOp>, CatalogError> {
    let _ = get_fdw(kv, name)?;
    Ok(vec![WriteOp::Delete {
        key: key::fdw_key(name),
    }])
}

// ── Foreign server ────────────────────────────────────────────────────────────

/// Register a foreign server.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
pub fn create_server(
    kv: &dyn Kv,
    name: &str,
    wrapper: &str,
    options: Vec<(String, String)>,
) -> Result<(), CatalogError> {
    let ops = create_server_ops(kv, name, wrapper, options)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for registering a foreign server without writing.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
#[expect(
    clippy::needless_pass_by_value,
    reason = "preserves donor CRUD API ownership for metadata options"
)]
pub fn create_server_ops(
    kv: &dyn Kv,
    name: &str,
    wrapper: &str,
    options: Vec<(String, String)>,
) -> Result<Vec<WriteOp>, CatalogError> {
    create_server_with_identity_ops(kv, name, wrapper, None, None, options)
}

/// Build the write batch for registering a foreign server and its identity fields.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
pub fn create_server_with_identity_ops(
    kv: &dyn Kv,
    name: &str,
    wrapper: &str,
    server_type: Option<&str>,
    version: Option<&str>,
    options: Vec<(String, String)>,
) -> Result<Vec<WriteOp>, CatalogError> {
    ensure_unique_options(&options)?;
    if kv.get(&key::server_key(name))?.is_some() {
        return Err(CatalogError::DuplicateObject(name.to_string()));
    }
    Ok(vec![WriteOp::Put {
        key: key::server_key(name),
        value: serialize_server(name, wrapper, server_type, version, &options),
    }])
}

/// Look up a foreign server by name.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn get_server(kv: &dyn Kv, name: &str) -> Result<ForeignServer, CatalogError> {
    let bytes = kv
        .get(&key::server_key(name))?
        .ok_or_else(|| CatalogError::UndefinedObject(name.to_string()))?;
    Ok(deserialize_server(&bytes)?)
}

/// List foreign servers by name.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn list_servers(kv: &dyn Kv) -> Result<Vec<ForeignServer>, CatalogError> {
    let mut servers = kv
        .scan_prefix(&key::server_prefix())?
        .into_iter()
        .map(|(_, value)| deserialize_server(&value).map_err(CatalogError::from))
        .collect::<Result<Vec<_>, _>>()?;
    servers.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(servers)
}

/// Drop a foreign server.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_server(kv: &dyn Kv, name: &str) -> Result<(), CatalogError> {
    let ops = drop_server_ops(kv, name)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for dropping a foreign server without writing.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_server_ops(kv: &dyn Kv, name: &str) -> Result<Vec<WriteOp>, CatalogError> {
    let _ = get_server(kv, name)?;
    Ok(vec![WriteOp::Delete {
        key: key::server_key(name),
    }])
}

// ── User mapping ──────────────────────────────────────────────────────────────

/// Register a user mapping.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
pub fn create_user_mapping(
    kv: &dyn Kv,
    user: &str,
    server: &str,
    options: Vec<(String, String)>,
) -> Result<(), CatalogError> {
    let ops = create_user_mapping_ops(kv, user, server, options)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for registering a user mapping without writing.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV seam.
#[expect(
    clippy::needless_pass_by_value,
    reason = "preserves donor CRUD API ownership for metadata options"
)]
pub fn create_user_mapping_ops(
    kv: &dyn Kv,
    user: &str,
    server: &str,
    options: Vec<(String, String)>,
) -> Result<Vec<WriteOp>, CatalogError> {
    ensure_unique_options(&options)?;
    if kv.get(&key::user_mapping_key(user, server))?.is_some() {
        return Err(CatalogError::DuplicateObject(format!("{user}@{server}")));
    }
    Ok(vec![WriteOp::Put {
        key: key::user_mapping_key(user, server),
        value: serialize_user_mapping(user, server, &options),
    }])
}

/// Look up a user mapping.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn get_user_mapping(
    kv: &dyn Kv,
    user: &str,
    server: &str,
) -> Result<UserMapping, CatalogError> {
    let bytes = kv
        .get(&key::user_mapping_key(user, server))?
        .ok_or_else(|| CatalogError::UndefinedObject(format!("{user}@{server}")))?;
    Ok(deserialize_user_mapping(&bytes)?)
}

/// List user mappings by server then user.
///
/// # Errors
///
/// Returns catalog storage or corruption errors.
pub fn list_user_mappings(kv: &dyn Kv) -> Result<Vec<UserMapping>, CatalogError> {
    let mut mappings = kv
        .scan_prefix(&key::user_mapping_prefix())?
        .into_iter()
        .map(|(_, value)| deserialize_user_mapping(&value).map_err(CatalogError::from))
        .collect::<Result<Vec<_>, _>>()?;
    mappings.sort_by(|left, right| (&left.server, &left.user).cmp(&(&right.server, &right.user)));
    Ok(mappings)
}

/// Drop a user mapping.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_user_mapping(kv: &dyn Kv, user: &str, server: &str) -> Result<(), CatalogError> {
    let ops = drop_user_mapping_ops(kv, user, server)?;
    kv.write_batch(&ops)?;
    Ok(())
}

/// Build the write batch for dropping a user mapping without writing.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV seam.
pub fn drop_user_mapping_ops(
    kv: &dyn Kv,
    user: &str,
    server: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    let _ = get_user_mapping(kv, user, server)?;
    Ok(vec![WriteOp::Delete {
        key: key::user_mapping_key(user, server),
    }])
}

// ── Foreign table ─────────────────────────────────────────────────────────────

/// The envelope columns that go in front of every foreign (Kafka) table.
fn envelope_columns() -> Vec<Column> {
    vec![
        Column::new("_partition", ColumnType::Int4),
        Column::new("_offset", ColumnType::Int8),
        Column::new("_timestamp", ColumnType::Timestamptz),
        Column::new("_key", ColumnType::Bytea),
        Column::new("_headers", ColumnType::Text),
    ]
}

/// Create a foreign table linked to an existing server.
///
/// The server must already exist. If it does not, this function returns
/// `UndefinedObject`. The envelope columns come first, and the user-supplied
/// value columns follow.
///
/// # Errors
///
/// Returns undefined-object, duplicate-table, or storage/corruption errors from
/// the catalog KV seam.
pub fn create_foreign_table(
    kv: &dyn Kv,
    name: &RelationName,
    value_columns: Vec<Column>,
    server: &str,
    options: Vec<(String, String)>,
) -> Result<TableId, CatalogError> {
    let (next, batch) = create_foreign_table_ops(
        kv,
        name,
        value_columns,
        server,
        options,
        Vec::new(),
        TableCreation::bootstrap(),
    )?;
    kv.write_batch(&batch)?;
    Ok(next)
}

/// Build the write batch for creating a foreign table without writing.
///
/// # Errors
///
/// Returns undefined-object, duplicate-table, or storage/corruption errors from
/// the catalog KV seam.
pub fn create_foreign_table_ops(
    kv: &dyn Kv,
    name: &RelationName,
    value_columns: Vec<Column>,
    server: &str,
    options: Vec<(String, String)>,
    checks: Vec<CheckConstraint>,
    creation: TableCreation<'_>,
) -> Result<(TableId, Vec<WriteOp>), CatalogError> {
    let _ = get_server(kv, server)?;
    ensure_unique_options(&options)?;

    if relation_exists(kv, name)? {
        return Err(CatalogError::DuplicateTable(name.to_string()));
    }

    let (next, bump) = creation.id.allocate(kv)?;
    let mut columns = envelope_columns();
    columns.extend(value_columns);

    let meta = ForeignTableMeta {
        server: server.to_string(),
        options,
    };

    let mut batch = vec![
        WriteOp::Put {
            key: catalog_key(name),
            value: serialize_schema(
                next,
                &columns,
                TableOptions::default(),
                creation.owner,
                Some(&meta),
                // A foreign table's contents come from its server, so the
                // materialized-view payload is not its to carry even when the
                // caller left one in the creation record.
                None,
                &checks,
            ),
        },
        WriteOp::Put {
            key: key::seq_key(next),
            value: U64::new(1).as_bytes().to_vec(),
        },
        catalog_by_id_op(next, name),
    ];
    batch.extend(bump);
    Ok((next, batch))
}

fn ensure_unique_options(options: &[(String, String)]) -> Result<(), CatalogError> {
    let mut names = HashSet::with_capacity(options.len());
    for (name, _) in options {
        if !names.insert(name) {
            return Err(CatalogError::DuplicateOption(name.clone()));
        }
    }
    Ok(())
}

/// Read the next `TableId`. This is 1 when the meta key is absent.
///
/// This function is public because the session claims a block of ids from this
/// counter under a lock of its own. Every `CREATE TABLE` therefore does not
/// read and bump the counter under the cluster-wide catalog lock.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn read_next_table_id(kv: &dyn Kv) -> Result<TableId, CatalogError> {
    match kv.get(&key::meta_next_table_id_key())? {
        Some(b) => {
            let (v, _) = U32::read_from_prefix(b.as_slice())
                .map_err(|_| KvError::CorruptRow("next_table_id is not u32".into()))?;
            Ok(v.get())
        }
        None => Ok(1),
    }
}

/// The op that sets the shared next-`TableId` counter to `next`.
#[must_use]
pub fn set_next_table_id_op(next: TableId) -> WriteOp {
    WriteOp::Put {
        key: key::meta_next_table_id_key(),
        value: U32::new(next).as_bytes().to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use crabka_pgkv::{FjallKv, MemKv, WriteOp};
    use crabka_pgtypes::{
        ColumnType,
        usertype::{DomainBody, RangeBody, UserTypeRef, intern},
    };

    use super::*;

    /// A relation in `public`, which is where every fixture here lives.
    fn rel(name: &str) -> RelationName {
        RelationName::public(name)
    }

    fn cols() -> Vec<Column> {
        vec![
            Column::new("id", ColumnType::Int4),
            Column::new("name", ColumnType::Text),
        ]
    }

    fn schema(name: &str, owner: &str) -> Schema {
        Schema {
            name: name.to_string(),
            owner: owner.to_string(),
        }
    }

    /// Every schema a relation can sit in, and the way a diagnostic spells it.
    ///
    /// `public` is bare because the default search path reaches it unqualified.
    /// A temporary namespace is `pg_temp`, which is the alias
    /// `get_namespace_name_or_temp` prints and the only spelling
    /// `postgres:18.4` ever puts in front of a temporary relation. Everything
    /// else is its own name, including the schemas whose names only *look*
    /// temporary — the suffix has to be digits and there has to be at least one.
    #[test]
    fn a_relation_is_spelled_the_way_a_diagnostic_spells_it() {
        use assert2::assert;

        let cases = [
            ("public", "t"),
            ("s1", "s1.t"),
            ("pg_catalog", "pg_catalog.t"),
            ("information_schema", "information_schema.t"),
            // The alias itself, which a written `pg_temp` qualifier keeps.
            ("pg_temp", "pg_temp.t"),
            ("pg_temp_1", "pg_temp.t"),
            ("pg_temp_33000", "pg_temp.t"),
            // Named by a user, not by the engine: no digits, or not only
            // digits, or nothing after the underscore.
            ("pg_temp_", "pg_temp_.t"),
            ("pg_tempx", "pg_tempx.t"),
            ("pg_temp_1a", "pg_temp_1a.t"),
            ("pg_temp_ 1", "pg_temp_ 1.t"),
        ];
        for (schema, spelled) in cases {
            assert!(
                RelationName::new(schema, "t").to_string() == spelled,
                "{schema}"
            );
        }
    }

    /// The property the spelling exists for: two sessions running the same
    /// statements produce the same diagnostic.
    ///
    /// The backend id is whatever the wire layer handed out, so a rendering that
    /// carried it would differ between two runs of one script. No id is pinned
    /// here; the test asserts only that no two of them can be told apart.
    #[test]
    fn a_temporary_relation_is_spelled_the_same_whatever_the_backend_id() {
        use assert2::assert;

        let backend_ids = [1, 7, 33_000, i32::MAX];
        let spellings: Vec<String> = backend_ids
            .into_iter()
            .map(|backend_id| RelationName::new(temp_schema_name(backend_id), "t").to_string())
            .collect();
        assert!(spellings == vec!["pg_temp.t".to_string(); backend_ids.len()]);
    }

    /// The spelling is the only thing that changes. A temporary namespace is
    /// still recognised as one, still reports `relpersistence = 't'`, and still
    /// keys the catalog under the name that has the backend id in it.
    #[test]
    fn spelling_a_namespace_pg_temp_does_not_rename_it() {
        use assert2::assert;

        let stored = temp_schema_name(33_000);
        assert!(is_temp_schema(&stored));
        assert!(relpersistence_of(&stored) == 't');
        assert!(displayed_schema(&stored) == PG_TEMP_ALIAS);
        // The alias is not itself a temporary namespace, so nothing that keys
        // off the stored name can be satisfied by the spelling.
        assert!(!is_temp_schema(PG_TEMP_ALIAS));
        assert!(relpersistence_of(PG_TEMP_ALIAS) == 'p');
        // Two relations that a diagnostic spells alike are still two relations:
        // the catalog keys them apart, so a lookup cannot follow the spelling.
        let kv = MemKv::default();
        let name = RelationName::new(&stored, "t");
        let aliased = RelationName::new(PG_TEMP_ALIAS, "t");
        assert!(name.schema == stored);
        assert!(name != aliased);
        assert!(name.to_string() == aliased.to_string());
        let (_, ops) = create_table_ops(&kv, &name, cols()).expect("create");
        kv.write_batch(&ops).expect("write");
        assert!(get_table(&kv, &name).is_ok());
        assert!(matches!(
            get_table(&kv, &aliased),
            Err(CatalogError::UndefinedTable(_))
        ));
    }

    #[test]
    fn structured_dotted_type_does_not_delete_colliding_legacy_key() {
        let kv = MemKv::default();
        let legacy = UserType {
            oid: 300_000,
            array_oid: crabka_pgtypes::usertype::user_array_oid(300_000),
            schema: "a".into(),
            name: "b".into(),
            body: UserTypeBody::Composite(Vec::new()),
        };
        kv.put(
            key::legacy_user_type_key("a.b"),
            serialize_user_type(&legacy),
        )
        .expect("legacy record");
        let dotted = UserType {
            oid: legacy.oid,
            array_oid: legacy.array_oid,
            schema: PUBLIC_SCHEMA.into(),
            name: "a.b".into(),
            body: UserTypeBody::Composite(Vec::new()),
        };

        let ops = put_user_type_ops(&kv, &dotted).expect("replacement ops");
        assert!(ops.iter().all(|op| !matches!(op, WriteOp::Delete { .. })));
    }

    #[test]
    fn fresh_catalogs_allocate_distinct_user_type_oids() {
        let first = MemKv::default();
        let second = MemKv::default();
        let (first_type, first_ops) = create_user_type_ops(
            &first,
            &RelationName::public("first_catalog_type"),
            UserTypeBody::Composite(Vec::new()),
        )
        .expect("first type");
        first.write_batch(&first_ops).expect("first write");
        let (second_type, second_ops) = create_user_type_ops(
            &second,
            &RelationName::public("second_catalog_type"),
            UserTypeBody::Composite(Vec::new()),
        )
        .expect("second type");
        second.write_batch(&second_ops).expect("second write");

        assert_ne!(first_type.oid, second_type.oid);
        assert_eq!(
            first_type.array_oid,
            crabka_pgtypes::usertype::user_array_oid(first_type.oid)
        );
        assert_eq!(
            second_type.array_oid,
            crabka_pgtypes::usertype::user_array_oid(second_type.oid)
        );
    }

    #[test]
    fn hydration_uses_only_types_from_its_catalog() {
        let kv = MemKv::default();
        let foreign = UserType {
            oid: 1_100_000,
            array_oid: crabka_pgtypes::usertype::user_array_oid(1_100_000),
            schema: PUBLIC_SCHEMA.into(),
            name: "other_tenant_type".into(),
            body: UserTypeBody::Composite(Vec::new()),
        };
        crabka_pgtypes::usertype::replace(&foreign);
        let dependent = UserType {
            oid: 1_100_004,
            array_oid: crabka_pgtypes::usertype::user_array_oid(1_100_004),
            schema: PUBLIC_SCHEMA.into(),
            name: "local_domain_missing_base".into(),
            body: UserTypeBody::Domain(DomainBody {
                base: foreign
                    .column_type()
                    .expect("a composite always has a column type"),
                not_null: false,
                not_null_name: None,
                default: None,
                checks: Vec::new(),
            }),
        };
        store_user_type(&kv, &dependent);

        let error = hydrate_user_types(&kv).expect_err("foreign registry entry is ignored");
        assert!(error.to_string().contains("1100000"));
        assert_eq!(
            crabka_pgtypes::usertype::lookup_oid(foreign.oid).as_deref(),
            Some(&foreign)
        );
        assert!(crabka_pgtypes::usertype::lookup_oid(dependent.oid).is_none());
    }

    #[test]
    fn hydration_decodes_dependencies_before_lower_oid_dependents() {
        let kv = MemKv::default();
        let base = UserType {
            oid: 1_200_004,
            array_oid: crabka_pgtypes::usertype::user_array_oid(1_200_004),
            schema: PUBLIC_SCHEMA.into(),
            name: "later_oid_base".into(),
            body: UserTypeBody::Range(RangeBody {
                subtype: ColumnType::Int4,
                collation: None,
                multirange_schema: Some(PUBLIC_SCHEMA.into()),
                multirange_name: Some("later_oid_multirange".into()),
            }),
        };
        let dependent = UserType {
            oid: 1_200_000,
            array_oid: crabka_pgtypes::usertype::user_array_oid(1_200_000),
            schema: PUBLIC_SCHEMA.into(),
            name: "earlier_oid_domain".into(),
            body: UserTypeBody::Domain(DomainBody {
                base: base.multirange_type().expect("range has a multirange"),
                not_null: false,
                not_null_name: None,
                default: None,
                checks: Vec::new(),
            }),
        };
        store_user_type(&kv, &dependent);
        store_user_type(&kv, &base);

        let hydrated = hydrate_user_types(&kv).expect("dependency retry succeeds");
        assert_eq!(hydrated.len(), 2);
        assert_eq!(hydrated[0].oid, dependent.oid);
        assert_eq!(
            hydrated[0].domain().expect("domain").base.name(),
            "later_oid_multirange"
        );
    }

    #[test]
    fn hydration_resolves_a_supplied_relation_rowtype() {
        let kv = MemKv::default();
        let rowtype = UserTypeRef {
            oid: 1_250_000,
            array_oid: 1_260_000,
            name: intern("public.hydration_source"),
        };
        let dependent = UserType {
            oid: 1_250_004,
            array_oid: crabka_pgtypes::usertype::user_array_oid(1_250_004),
            schema: PUBLIC_SCHEMA.into(),
            name: "domain_over_relation_rowtype".into(),
            body: UserTypeBody::Domain(DomainBody {
                base: ColumnType::Record(Some(rowtype)),
                not_null: false,
                not_null_name: None,
                default: None,
                checks: Vec::new(),
            }),
        };
        store_user_type(&kv, &dependent);

        let hydrated = hydrate_user_types_with(&kv, &|oid| {
            (oid == rowtype.oid).then_some(ColumnType::Record(Some(rowtype)))
        })
        .expect("caller-owned rowtype resolves the durable dependency");

        assert_eq!(hydrated, vec![dependent]);
    }

    #[test]
    fn corrupt_hydration_does_not_publish_valid_prefix() {
        let kv = MemKv::default();
        let original = UserType {
            oid: 1_300_000,
            array_oid: crabka_pgtypes::usertype::user_array_oid(1_300_000),
            schema: PUBLIC_SCHEMA.into(),
            name: "registry_before_failed_hydration".into(),
            body: UserTypeBody::Composite(Vec::new()),
        };
        crabka_pgtypes::usertype::replace(&original);
        let catalog_type = UserType {
            oid: original.oid,
            array_oid: original.array_oid,
            schema: PUBLIC_SCHEMA.into(),
            name: "catalog_type_not_published".into(),
            body: UserTypeBody::Composite(Vec::new()),
        };
        store_user_type(&kv, &catalog_type);
        kv.put(
            key::user_type_key(PUBLIC_SCHEMA, "corrupt_type"),
            1_300_004u32.to_be_bytes().to_vec(),
        )
        .expect("store corrupt record with readable oid");

        hydrate_user_types(&kv).expect_err("corrupt record rejects the whole hydration");
        assert_eq!(
            crabka_pgtypes::usertype::lookup_oid(original.oid).as_deref(),
            Some(&original)
        );
        assert!(crabka_pgtypes::usertype::lookup_oid(1_300_004).is_none());
    }

    fn store_user_type(kv: &MemKv, ty: &UserType) {
        kv.put(
            key::user_type_key(&ty.schema, &ty.name),
            serialize_user_type(ty),
        )
        .expect("store user type");
    }

    fn apply(kv: &MemKv, ops: &[WriteOp]) {
        kv.write_batch(ops).expect("write");
    }

    #[test]
    fn typed_table_type_can_be_cleared() {
        use assert2::assert;

        let kv = MemKv::new();
        let relation = RelationName::new("public", "items");
        apply(&kv, &[set_typed_table_type_op(&relation, 30_001)]);
        assert!(typed_table_type(&kv, &relation).expect("typed row") == Some(30_001));
        apply(&kv, &[clear_typed_table_type_op(&relation)]);
        assert!(
            typed_table_type(&kv, &relation)
                .expect("cleared row")
                .is_none()
        );
    }

    /// A catalog nobody has written to still reports three schemas, each with
    /// the owner `PostgreSQL` bootstraps it under. The catalog stores nothing
    /// for them, so [`list_schemas`] synthesises all three.
    #[test]
    fn a_fresh_catalog_reports_the_bootstrap_schemas() {
        use assert2::assert;
        let kv = MemKv::default();
        assert!(
            list_schemas(&kv).expect("list")
                == vec![
                    schema("information_schema", "postgres"),
                    schema("pg_catalog", "postgres"),
                    schema("public", "pg_database_owner"),
                ]
        );
        for name in ["public", "pg_catalog", "information_schema"] {
            assert!(schema_exists(&kv, name).expect("exists"), "{name}");
        }
        assert!(!schema_exists(&kv, "nosuch").expect("exists"));
    }

    /// `public` is a real schema rather than a projection. Nothing can create
    /// over it, it can be dropped, and it stays dropped. A second creation
    /// gives an ordinary schema owned by its creator.
    #[test]
    fn public_is_a_droppable_schema_that_already_exists() {
        use assert2::assert;
        let kv = MemKv::default();
        assert!(matches!(
            create_schema_ops(&kv, "public", "alice"),
            Err(CatalogError::DuplicateSchema(name)) if name == "public"
        ));

        apply(
            &kv,
            &drop_schema_ops(&kv, "public", false).expect("drop ops"),
        );
        assert!(!schema_exists(&kv, "public").expect("exists"));
        assert!(
            list_schemas(&kv).expect("list")
                == vec![
                    schema("information_schema", "postgres"),
                    schema("pg_catalog", "postgres"),
                ]
        );
        assert!(matches!(
            drop_schema_ops(&kv, "public", false),
            Err(CatalogError::UndefinedSchema(name)) if name == "public"
        ));

        apply(
            &kv,
            &create_schema_ops(&kv, "public", "alice").expect("create ops"),
        );
        assert!(
            list_schemas(&kv).expect("list")
                == vec![
                    schema("information_schema", "postgres"),
                    schema("pg_catalog", "postgres"),
                    schema("public", "alice"),
                ]
        );
    }

    /// The system schemas refuse a drop even when they are empty. A new owner
    /// replaces the synthesised row rather than adding a second one.
    #[test]
    fn system_schemas_refuse_a_drop_and_are_not_duplicated_by_a_stored_row() {
        use assert2::assert;
        let kv = MemKv::default();
        for name in SYSTEM_SCHEMAS {
            assert!(
                matches!(
                    drop_schema_ops(&kv, name, true),
                    Err(CatalogError::SystemSchemaDrop(dropped)) if dropped == *name
                ),
                "{name}"
            );
        }

        apply(
            &kv,
            &set_schema_owner_ops(&kv, "pg_catalog", "alice").expect("owner ops"),
        );
        assert!(
            list_schemas(&kv).expect("list")
                == vec![
                    schema("information_schema", "postgres"),
                    schema("pg_catalog", "alice"),
                    schema("public", "pg_database_owner"),
                ]
        );
        assert!(matches!(
            drop_schema_ops(&kv, "pg_catalog", true),
            Err(CatalogError::SystemSchemaDrop(name)) if name == "pg_catalog"
        ));
    }

    /// The catalog applies the reserved prefix before it looks up the name.
    /// `pg_catalog`, which does exist, therefore reports an unacceptable name
    /// rather than a duplicate. Every SQLSTATE follows from the variant.
    #[test]
    fn the_reserved_prefix_outranks_the_duplicate_check() {
        use assert2::assert;
        let kv = MemKv::default();
        for name in ["pg_catalog", "pg_anything", "pg_"] {
            assert!(
                matches!(
                    create_schema_ops(&kv, name, "alice"),
                    Err(CatalogError::ReservedSchemaName(refused)) if refused == name
                ),
                "{name}"
            );
        }
        apply(
            &kv,
            &create_schema_ops(&kv, "pgfoo", "alice").expect("create ops"),
        );
        assert!(schema_exists(&kv, "pgfoo").expect("exists"));

        assert!(CatalogError::ReservedSchemaName("pg_x".into()).sqlstate() == "42939");
        assert!(CatalogError::SystemSchemaDrop("pg_catalog".into()).sqlstate() == "2BP01");
    }

    /// `CHECK` constraints, identity kinds, and generated-column expressions
    /// survive the catalog round trip beside the columns they belong to.
    #[test]
    fn table_checks_and_column_metadata_round_trip() {
        use assert2::assert;
        let kv = MemKv::default();
        let columns = vec![
            Column {
                name: "id".into(),
                ty: ColumnType::Int4,
                not_null: true,
                default: Some(ColumnDefault::NextVal("t_id_seq".into())),
                generated: None,
                identity: Some(IdentityKind::Always),
                collation: None,
            },
            Column {
                name: "doubled".into(),
                ty: ColumnType::Int4,
                not_null: false,
                default: None,
                generated: Some(GeneratedColumn {
                    expr: "id * 2".into(),
                    kind: GeneratedKind::Stored,
                }),
                identity: None,
                collation: None,
            },
        ];
        let checks = vec![
            CheckConstraint {
                name: "t_id_check".into(),
                expr: "id > 0".into(),
                validated: true,
            },
            CheckConstraint {
                name: "t_check".into(),
                expr: "id < doubled".into(),
                validated: false,
            },
        ];
        let (_, ops) = create_table_with_options_ops(
            &kv,
            &rel("t"),
            columns.clone(),
            TableOptions::default(),
            checks.clone(),
            TableCreation::bootstrap(),
        )
        .expect("create ops");
        kv.write_batch(&ops).expect("write");

        let table = get_table(&kv, &rel("t")).expect("table");
        assert!(table.columns == columns);
        assert!(table.checks == checks);
    }

    /// The generated-column accessors report the kind a column was built with:
    /// only a `VIRTUAL` column is virtual, only a `STORED` one is stored, and
    /// `attgenerated` spells each the way `pg_attribute` does.
    #[test]
    fn generated_column_accessors_follow_the_kind() {
        use assert2::assert;

        for (generated, expr, stored, virt, attgenerated) in [
            (None, None, false, false, ""),
            (
                Some(GeneratedKind::Stored),
                Some("id * 2"),
                true,
                false,
                "s",
            ),
            (
                Some(GeneratedKind::Virtual),
                Some("id * 2"),
                false,
                true,
                "v",
            ),
        ] {
            let column = Column {
                generated: generated.map(|kind| GeneratedColumn {
                    expr: "id * 2".into(),
                    kind,
                }),
                ..Column::new("doubled", ColumnType::Int4)
            };

            assert!(column.generation_expr() == expr);
            assert!(column.is_stored_generated() == stored);
            assert!(column.is_virtual_generated() == virt);
            assert!(column.attgenerated() == attgenerated);
        }
    }

    /// `replace_table_schema_ops` swaps the column list and CHECK list while
    /// preserving the table id and storage options — the contract every
    /// `ALTER TABLE` subcommand relies on.
    #[test]
    fn replacing_a_table_schema_preserves_its_identity() {
        use assert2::assert;
        let kv = MemKv::default();
        create_table_with_options(
            &kv,
            &rel("t"),
            cols(),
            TableOptions {
                sharded: true,
                ..TableOptions::default()
            },
        )
        .expect("create");
        let before = get_table(&kv, &rel("t")).expect("table");

        let mut columns = before.columns.clone();
        columns.push(Column::new("extra", ColumnType::Text));
        let checks = vec![CheckConstraint {
            name: "t_check".into(),
            expr: "id > 0".into(),
            validated: true,
        }];
        let ops = replace_table_schema_ops(
            &kv,
            &rel("t"),
            &Table {
                columns: columns.clone(),
                checks: checks.clone(),
                ..before.clone()
            },
        )
        .expect("replace ops");
        kv.write_batch(&ops).expect("write");

        let after = get_table(&kv, &rel("t")).expect("table");
        assert!(
            after
                == Table {
                    columns,
                    checks,
                    ..before
                }
        );
    }

    /// The catalog keys comments by object kind and name, and `None` clears
    /// one. A relation drop takes its column comments with it, and does not
    /// touch a same-prefixed sibling.
    #[test]
    fn comments_round_trip_and_drop_with_their_relation() {
        use assert2::assert;
        let kv = MemKv::default();
        kv.write_batch(&[
            set_comment_op(
                "table",
                CommentObject::Relation(&rel("t")),
                Some("table comment"),
            ),
            set_comment_op(
                "column",
                CommentObject::Column(&rel("t"), "id"),
                Some("column comment"),
            ),
            set_comment_op(
                "table",
                CommentObject::Relation(&rel("t2")),
                Some("sibling comment"),
            ),
        ])
        .expect("write");

        assert!(
            get_comment(&kv, "table", CommentObject::Relation(&rel("t"))).expect("get")
                == Some("table comment".into())
        );
        assert!(
            get_comment(&kv, "column", CommentObject::Column(&rel("t"), "id")).expect("get")
                == Some("column comment".into())
        );

        kv.write_batch(&[set_comment_op(
            "table",
            CommentObject::Relation(&rel("t")),
            None,
        )])
        .expect("clear");
        assert!(
            get_comment(&kv, "table", CommentObject::Relation(&rel("t")))
                .expect("get")
                .is_none()
        );

        let ops = drop_relation_comment_ops(&kv, &rel("t")).expect("drop ops");
        kv.write_batch(&ops).expect("write");
        assert!(
            get_comment(&kv, "column", CommentObject::Column(&rel("t"), "id"))
                .expect("get")
                .is_none()
        );
        assert!(
            get_comment(&kv, "table", CommentObject::Relation(&rel("t2"))).expect("get")
                == Some("sibling comment".into())
        );
    }

    /// `DROP INDEX` refuses a constraint-backed index (2BP01) while
    /// `ALTER TABLE … DROP CONSTRAINT` removes the same record.
    #[test]
    fn constraint_backed_indexes_drop_only_through_the_constraint_path() {
        use assert2::assert;
        let kv = MemKv::default();
        create_table(&kv, &rel("t"), cols()).expect("create");
        let table = get_table(&kv, &rel("t")).expect("table");
        let (_, ops) = create_constraint_index_ops(
            &kv,
            &table,
            &NewIndex {
                name: "t_pkey".into(),
                columns: vec!["id".into()],
                unique: true,
                placement: IndexPlacement::Local,
                method: IndexMethod::Btree,
                constraint: Some(IndexConstraint::PrimaryKey),
                without_overlaps: false,
                deferral: ConstraintDeferral::Immediate,
            },
        )
        .expect("index ops");
        kv.write_batch(&ops).expect("write");

        assert!(
            drop_index_ops(&kv, &rel("t_pkey")).unwrap_err()
                == CatalogError::DependentObjectsStillExist("t_pkey".into())
        );
        let (_, drop_ops) =
            drop_constraint_index_ops(&kv, &rel("t_pkey")).expect("constraint drop");
        kv.write_batch(&drop_ops).expect("write");
        assert!(get_index(&kv, &rel("t_pkey")).is_err());
    }

    #[test]
    fn roles_and_table_privileges_round_trip() {
        let kv = MemKv::default();
        create_table(&kv, &rel("docs"), vec![Column::new("id", ColumnType::Int4)]).expect("table");
        create_role(&kv, "reader", false).expect("role");

        let ops = grant_table_privileges_ops(
            &kv,
            &rel("docs"),
            &["reader".to_string()],
            &["SELECT".to_string()],
        )
        .expect("grant ops");
        kv.write_batch(&ops).expect("grant write");

        assert_eq!(
            list_table_privileges(&kv).expect("privileges"),
            vec![TablePrivilege {
                table: rel("docs"),
                grantee: "reader".into(),
                privilege: "SELECT".into(),
            }]
        );

        let ops = revoke_table_privileges_ops(
            &kv,
            &rel("docs"),
            &["reader".to_string()],
            &["SELECT".to_string()],
        )
        .expect("revoke ops");
        kv.write_batch(&ops).expect("revoke write");
        assert!(list_table_privileges(&kv).expect("privileges").is_empty());
    }

    #[test]
    fn default_table_privileges_combine_global_and_schema_scopes() {
        let kv = MemKv::default();
        create_role(&kv, "owner", false).expect("owner");
        create_role(&kv, "reader", false).expect("reader");
        kv.write_batch(&create_schema_ops(&kv, "private", "owner").expect("schema ops"))
            .expect("schema");
        let global = alter_default_table_privileges_ops(
            &kv,
            "owner",
            &[],
            &["reader".into()],
            &["SELECT".into()],
            true,
        )
        .expect("global ops");
        kv.write_batch(&global).expect("global defaults");
        let local = alter_default_table_privileges_ops(
            &kv,
            "owner",
            &["private".into()],
            &["reader".into()],
            &["INSERT".into()],
            true,
        )
        .expect("schema ops");
        kv.write_batch(&local).expect("schema defaults");
        let mut privileges = default_table_privileges_of(&kv, "owner", "private")
            .expect("defaults")
            .into_iter()
            .map(|privilege| privilege.privilege)
            .collect::<Vec<_>>();
        privileges.sort();
        assert_eq!(privileges, ["INSERT", "SELECT"]);
        assert_eq!(
            default_table_privileges_of(&kv, "owner", "public")
                .expect("defaults")
                .into_iter()
                .map(|privilege| privilege.privilege)
                .collect::<Vec<_>>(),
            ["SELECT"]
        );
        let table = rel("owner_revoke");
        assert!(!owner_table_privilege_is_revoked(&kv, &table, "INSERT").expect("not revoked"));
        kv.write_batch(&[revoke_owner_table_privilege_op(&table, "INSERT")])
            .expect("revoke marker");
        assert!(owner_table_privilege_is_revoked(&kv, &table, "INSERT").expect("revoked"));
        kv.write_batch(&[restore_owner_table_privilege_op(&table, "INSERT")])
            .expect("restore marker");
        assert!(!owner_table_privilege_is_revoked(&kv, &table, "INSERT").expect("restored"));
    }

    #[test]
    fn predefined_roles_are_pg_authid_rows_but_public_is_not() {
        let kv = MemKv::default();
        create_role(&kv, "reader", true).expect("reader");
        // A stored `PUBLIC` row is corrupt catalog state, but must not turn the
        // pseudo-role into a `pg_authid` row if one is encountered on read.
        kv.write_batch(&[WriteOp::Put {
            key: role_key(PUBLIC_ROLE),
            value: serialize_role(PUBLIC_ROLE, true, RoleAttributes::default()),
        }])
        .expect("corrupt public row");
        let names = list_roles(&kv)
            .expect("roles")
            .into_iter()
            .map(|role| role.name)
            .collect::<Vec<_>>();

        assert!(
            names
                == vec![
                    "pg_checkpoint",
                    "pg_create_subscription",
                    "pg_database_owner",
                    "pg_execute_server_program",
                    "pg_maintain",
                    "pg_monitor",
                    "pg_read_all_data",
                    "pg_read_all_settings",
                    "pg_read_all_stats",
                    "pg_read_server_files",
                    "pg_signal_autovacuum_worker",
                    "pg_signal_backend",
                    "pg_stat_scan_tables",
                    "pg_use_reserved_connections",
                    "pg_write_all_data",
                    "pg_write_server_files",
                    "postgres",
                    "reader",
                ]
        );
        assert!(get_role(&kv, "pg_monitor").expect("pg_monitor").can_login == false);
        let bootstrap = get_role(&kv, BOOTSTRAP_ROLE).expect("bootstrap");
        assert!(bootstrap.can_login);
        for attribute in [
            RoleAttribute::Superuser,
            RoleAttribute::CreateRole,
            RoleAttribute::CreateDb,
            RoleAttribute::BypassRls,
        ] {
            assert!(bootstrap.attributes.has(attribute), "{attribute:?}");
        }
        assert!(role_exists(&kv, "pg_monitor").expect("exists"));
        assert!(!names.iter().any(|name| name == PUBLIC_ROLE));
    }

    fn grant(kv: &dyn Kv, relation: &RelationName, grantee: &str, privileges: &[&str]) {
        let ops = grant_table_privileges_ops(
            kv,
            relation,
            &[grantee.to_string()],
            &privileges
                .iter()
                .map(|p| (*p).to_string())
                .collect::<Vec<_>>(),
        )
        .expect("grant ops");
        kv.write_batch(&ops).expect("grant write");
    }

    fn revoke(kv: &dyn Kv, relation: &RelationName, grantee: &str, privileges: &[&str]) {
        let ops = revoke_table_privileges_ops(
            kv,
            relation,
            &[grantee.to_string()],
            &privileges
                .iter()
                .map(|p| (*p).to_string())
                .collect::<Vec<_>>(),
        )
        .expect("revoke ops");
        kv.write_batch(&ops).expect("revoke write");
    }

    /// The privilege names recorded against `relation`, sorted so a comparison
    /// does not depend on scan order.
    fn privilege_names(kv: &dyn Kv, relation: &RelationName) -> Vec<String> {
        let mut names: Vec<String> = table_privileges_of(kv, relation)
            .expect("privileges")
            .into_iter()
            .map(|privilege| privilege.privilege)
            .collect();
        names.sort();
        names
    }

    fn sorted_all_table_privileges() -> Vec<String> {
        let mut all: Vec<String> = TABLE_PRIVILEGES.iter().map(|p| (*p).to_string()).collect();
        all.sort();
        all
    }

    fn view_fixture(kv: &dyn Kv, name: &str) {
        create_view(
            kv,
            &rel(name),
            "SELECT 1 AS total".into(),
            vec![Column::new("total", ColumnType::Int4)],
            ViewOptions::default(),
            BOOTSTRAP_ROLE,
        )
        .expect("create view");
    }

    #[test]
    fn grant_all_on_a_view_to_public_records_every_table_privilege() {
        use assert2::assert;

        let kv = MemKv::new();
        view_fixture(&kv, "atest12v");
        grant(&kv, &rel("atest12v"), PUBLIC_ROLE, &["ALL"]);

        assert!(privilege_names(&kv, &rel("atest12v")) == sorted_all_table_privileges());
        assert!(
            table_privileges_of(&kv, &rel("atest12v"))
                .expect("privileges")
                .iter()
                .all(|privilege| privilege.table == rel("atest12v")
                    && privilege.grantee == PUBLIC_ROLE)
        );
    }

    #[test]
    fn all_is_expanded_at_grant_and_revoke_so_the_two_compose() {
        use assert2::assert;

        // (granted, revoked, what should remain)
        let cases: [(&[&str], &[&str], &[&str]); 5] = [
            (
                &["ALL"],
                &["SELECT"],
                &[
                    "DELETE",
                    "INSERT",
                    "MAINTAIN",
                    "REFERENCES",
                    "TRIGGER",
                    "TRUNCATE",
                    "UPDATE",
                ],
            ),
            (&["SELECT"], &["ALL"], &[]),
            (&["ALL PRIVILEGES"], &["all"], &[]),
            (&["select", "insert"], &["SELECT"], &["INSERT"]),
            (
                &["ALL"],
                &["update", "delete"],
                &[
                    "INSERT",
                    "MAINTAIN",
                    "REFERENCES",
                    "SELECT",
                    "TRIGGER",
                    "TRUNCATE",
                ],
            ),
        ];

        for (index, (granted, revoked, remaining)) in cases.into_iter().enumerate() {
            let kv = MemKv::new();
            let relation = rel(&format!("t{index}"));
            create_table(&kv, &relation, cols()).expect("table");
            grant(&kv, &relation, PUBLIC_ROLE, granted);
            revoke(&kv, &relation, PUBLIC_ROLE, revoked);

            let expected: Vec<String> = remaining.iter().map(|p| (*p).to_string()).collect();
            assert!(privilege_names(&kv, &relation) == expected);
        }
    }

    #[test]
    fn table_privileges_of_returns_only_the_named_relation() {
        use assert2::assert;

        let kv = MemKv::new();
        // `t` is a byte-prefix of `t2`: a scan over unlength-prefixed key parts
        // would hand `t`'s lookup `t2`'s grants as well.
        create_table(&kv, &rel("t"), cols()).expect("t");
        create_table(&kv, &rel("t2"), cols()).expect("t2");
        grant(&kv, &rel("t"), PUBLIC_ROLE, &["SELECT"]);
        grant(&kv, &rel("t2"), PUBLIC_ROLE, &["INSERT", "UPDATE"]);

        assert!(privilege_names(&kv, &rel("t")) == vec!["SELECT".to_string()]);
        assert!(
            privilege_names(&kv, &rel("t2")) == vec!["INSERT".to_string(), "UPDATE".to_string()]
        );
        assert!(list_table_privileges(&kv).expect("privileges").len() == 3);
    }

    #[test]
    fn public_and_bootstrap_are_grantable_without_a_stored_role() {
        use assert2::assert;

        let kv = MemKv::new();
        create_table(&kv, &rel("docs"), cols()).expect("table");

        for grantee in [PUBLIC_ROLE, BOOTSTRAP_ROLE] {
            assert!(
                grant_table_privileges_ops(
                    &kv,
                    &rel("docs"),
                    &[grantee.to_string()],
                    &["SELECT".to_string()],
                )
                .is_ok()
            );
        }
        assert!(
            grant_table_privileges_ops(
                &kv,
                &rel("docs"),
                &["nobody".to_string()],
                &["SELECT".to_string()],
            )
            .expect_err("an unheld name is not a grantee")
                == CatalogError::UndefinedObject("nobody".into())
        );
    }

    /// Whether the relation is there is the caller's question: this builds the
    /// grant for whatever name it is handed, because the engine synthesises
    /// relations that hold no record under any key here and `PostgreSQL` grants
    /// on those too. What it still refuses is a grantee no role holds.
    #[test]
    fn granting_on_a_name_with_no_record_still_builds_the_grant() {
        use assert2::assert;

        let kv = MemKv::new();
        for ops in [
            grant_table_privileges_ops(
                &kv,
                &rel("missing"),
                &[PUBLIC_ROLE.to_string()],
                &["SELECT".to_string()],
            ),
            revoke_table_privileges_ops(
                &kv,
                &rel("missing"),
                &[PUBLIC_ROLE.to_string()],
                &["SELECT".to_string()],
            ),
        ] {
            assert!(
                ops.expect("the relation lookup is not this function's")
                    .len()
                    == 1
            );
        }
        assert!(
            grant_table_privileges_ops(
                &kv,
                &rel("missing"),
                &["nobody".to_string()],
                &["SELECT".to_string()],
            )
            .expect_err("an unheld name is not a grantee")
                == CatalogError::UndefinedObject("nobody".into())
        );
    }

    #[test]
    fn has_stored_table_privilege_is_a_case_insensitive_point_lookup() {
        use assert2::assert;

        let kv = MemKv::new();
        create_table(&kv, &rel("docs"), cols()).expect("table");
        create_role(&kv, "reader", false).expect("role");
        grant(&kv, &rel("docs"), "reader", &["select"]);

        for probe in ["SELECT", "select", "SeLeCt"] {
            assert!(
                has_stored_table_privilege(&kv, &rel("docs"), "reader", probe).expect("lookup")
            );
        }
        assert!(
            !has_stored_table_privilege(&kv, &rel("docs"), "reader", "INSERT").expect("lookup")
        );
        assert!(
            !has_stored_table_privilege(&kv, &rel("docs"), PUBLIC_ROLE, "SELECT").expect("lookup")
        );
    }

    #[test]
    fn dropping_a_relation_takes_its_grants_with_it() {
        use assert2::assert;

        let kv = MemKv::new();
        create_table(&kv, &rel("docs"), cols()).expect("table");
        view_fixture(&kv, "docs_v");
        grant(&kv, &rel("docs"), PUBLIC_ROLE, &["ALL"]);
        grant(&kv, &rel("docs_v"), PUBLIC_ROLE, &["SELECT"]);

        drop_table(&kv, &rel("docs")).expect("drop table");
        assert!(privilege_names(&kv, &rel("docs")).is_empty());
        assert!(privilege_names(&kv, &rel("docs_v")) == vec!["SELECT".to_string()]);

        drop_view(&kv, &rel("docs_v")).expect("drop view");
        assert!(list_table_privileges(&kv).expect("privileges").is_empty());

        // A recreated name must start with no grants, which is the leak the
        // deletion exists to prevent.
        create_table(&kv, &rel("docs"), cols()).expect("recreate");
        assert!(
            !has_stored_table_privilege(&kv, &rel("docs"), PUBLIC_ROLE, "SELECT").expect("get")
        );
    }

    #[test]
    fn renaming_a_table_moves_its_grants_to_the_new_name() {
        use assert2::assert;

        let kv = MemKv::new();
        create_table(&kv, &rel("docs"), cols()).expect("table");
        grant(&kv, &rel("docs"), PUBLIC_ROLE, &["ALL"]);
        let ops = rename_table_ops(&kv, &rel("docs"), &rel("papers")).expect("rename ops");
        kv.write_batch(&ops).expect("rename write");

        assert!(privilege_names(&kv, &rel("docs")).is_empty());
        assert!(privilege_names(&kv, &rel("papers")) == sorted_all_table_privileges());
    }

    fn grant_columns(
        kv: &dyn Kv,
        relation: &RelationName,
        columns: &[&str],
        grantee: &str,
        privileges: &[&str],
    ) {
        let ops = grant_column_privileges_ops(
            kv,
            relation,
            &strings(columns),
            &[grantee.to_string()],
            &strings(privileges),
        )
        .expect("grant ops");
        kv.write_batch(&ops).expect("grant write");
    }

    fn revoke_columns(
        kv: &dyn Kv,
        relation: &RelationName,
        columns: &[&str],
        grantee: &str,
        privileges: &[&str],
    ) {
        let ops = revoke_column_privileges_ops(
            kv,
            relation,
            &strings(columns),
            &[grantee.to_string()],
            &strings(privileges),
        )
        .expect("revoke ops");
        kv.write_batch(&ops).expect("revoke write");
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    fn column_grant(
        relation: &RelationName,
        column: &str,
        grantee: &str,
        privilege: &str,
    ) -> ColumnPrivilege {
        ColumnPrivilege {
            table: relation.clone(),
            column: column.to_string(),
            grantee: grantee.to_string(),
            privilege: privilege.to_string(),
        }
    }

    /// Recorded column grants in a fixed order, so a whole-record comparison
    /// does not depend on how the key encoding happens to sort.
    fn sorted_column_grants(mut privileges: Vec<ColumnPrivilege>) -> Vec<ColumnPrivilege> {
        privileges.sort_by(|left, right| {
            (
                &left.table.schema,
                &left.table.name,
                &left.column,
                &left.grantee,
                &left.privilege,
            )
                .cmp(&(
                    &right.table.schema,
                    &right.table.name,
                    &right.column,
                    &right.grantee,
                    &right.privilege,
                ))
        });
        privileges
    }

    /// The privilege names recorded against one column, sorted.
    fn column_privilege_names(kv: &dyn Kv, relation: &RelationName, column: &str) -> Vec<String> {
        let mut names: Vec<String> = column_privileges_of(kv, relation)
            .expect("privileges")
            .into_iter()
            .filter(|privilege| privilege.column == column)
            .map(|privilege| privilege.privilege)
            .collect();
        names.sort();
        names
    }

    fn sorted_all_column_privileges() -> Vec<String> {
        let mut all: Vec<String> = COLUMN_PRIVILEGES.iter().map(|p| (*p).to_string()).collect();
        all.sort();
        all
    }

    #[test]
    fn has_stored_column_privilege_is_a_point_lookup_on_all_four_parts() {
        use assert2::assert;

        let kv = MemKv::new();
        create_table(&kv, &rel("docs"), cols()).expect("table");
        create_role(&kv, "reader", false).expect("reader");
        create_role(&kv, "writer", false).expect("writer");
        grant_columns(&kv, &rel("docs"), &["name"], "reader", &["select"]);

        for probe in ["SELECT", "select", "SeLeCt"] {
            assert!(
                has_stored_column_privilege(&kv, &rel("docs"), "name", "reader", probe)
                    .expect("lookup")
            );
        }
        // One wrong part at a time: column, grantee, privilege.
        for (column, grantee, privilege) in [
            ("id", "reader", "SELECT"),
            ("name", "writer", "SELECT"),
            ("name", "reader", "UPDATE"),
        ] {
            assert!(
                !has_stored_column_privilege(&kv, &rel("docs"), column, grantee, privilege)
                    .expect("lookup"),
                "{column}/{grantee}/{privilege}"
            );
        }
    }

    #[test]
    fn column_all_expands_to_the_four_column_privileges_at_grant_and_revoke() {
        use assert2::assert;

        // (granted, revoked, what should remain)
        let cases: [(&[&str], &[&str], &[&str]); 5] = [
            (&["ALL"], &[], &["INSERT", "REFERENCES", "SELECT", "UPDATE"]),
            (&["ALL"], &["SELECT"], &["INSERT", "REFERENCES", "UPDATE"]),
            (&["SELECT"], &["ALL"], &[]),
            (&["ALL PRIVILEGES"], &["all"], &[]),
            (&["select", "insert"], &["SELECT"], &["INSERT"]),
        ];

        for (index, (granted, revoked, remaining)) in cases.into_iter().enumerate() {
            let kv = MemKv::new();
            let relation = rel(&format!("t{index}"));
            create_table(&kv, &relation, cols()).expect("table");
            grant_columns(&kv, &relation, &["name"], PUBLIC_ROLE, granted);
            revoke_columns(&kv, &relation, &["name"], PUBLIC_ROLE, revoked);

            assert!(column_privilege_names(&kv, &relation, "name") == strings(remaining));
        }

        // `ALL` on a column is the column mask, not the relation mask: the four
        // relation-only names must never be recorded against a column.
        let kv = MemKv::new();
        create_table(&kv, &rel("docs"), cols()).expect("table");
        grant_columns(&kv, &rel("docs"), &["name"], PUBLIC_ROLE, &["ALL"]);
        assert!(
            column_privilege_names(&kv, &rel("docs"), "name") == sorted_all_column_privileges()
        );
        for relation_only in ["DELETE", "TRUNCATE", "TRIGGER", "MAINTAIN"] {
            assert!(
                !has_stored_column_privilege(&kv, &rel("docs"), "name", PUBLIC_ROLE, relation_only)
                    .expect("lookup"),
                "{relation_only}"
            );
        }
    }

    #[test]
    fn revoking_one_column_privilege_leaves_its_siblings() {
        use assert2::assert;

        let kv = MemKv::new();
        create_table(&kv, &rel("docs"), cols()).expect("table");
        create_role(&kv, "reader", false).expect("reader");
        grant_columns(
            &kv,
            &rel("docs"),
            &["id", "name"],
            "reader",
            &["SELECT", "UPDATE"],
        );
        grant_columns(&kv, &rel("docs"), &["name"], PUBLIC_ROLE, &["SELECT"]);
        revoke_columns(&kv, &rel("docs"), &["name"], "reader", &["SELECT"]);

        assert!(
            sorted_column_grants(column_privileges_of(&kv, &rel("docs")).expect("privileges"))
                == vec![
                    column_grant(&rel("docs"), "id", "reader", "SELECT"),
                    column_grant(&rel("docs"), "id", "reader", "UPDATE"),
                    column_grant(&rel("docs"), "name", PUBLIC_ROLE, "SELECT"),
                    column_grant(&rel("docs"), "name", "reader", "UPDATE"),
                ]
        );
    }

    #[test]
    fn column_privileges_of_returns_only_the_named_relation() {
        use assert2::assert;

        let kv = MemKv::new();
        // `t` is a byte-prefix of `t2`, as in the relation-level case.
        create_table(&kv, &rel("t"), cols()).expect("t");
        create_table(&kv, &rel("t2"), cols()).expect("t2");
        grant_columns(&kv, &rel("t"), &["name"], PUBLIC_ROLE, &["SELECT"]);
        grant_columns(&kv, &rel("t2"), &["id"], PUBLIC_ROLE, &["INSERT"]);

        assert!(
            column_privileges_of(&kv, &rel("t")).expect("privileges")
                == vec![column_grant(&rel("t"), "name", PUBLIC_ROLE, "SELECT")]
        );
        assert!(
            column_privileges_of(&kv, &rel("t2")).expect("privileges")
                == vec![column_grant(&rel("t2"), "id", PUBLIC_ROLE, "INSERT")]
        );
        assert!(
            sorted_column_grants(list_column_privileges(&kv).expect("privileges"))
                == vec![
                    column_grant(&rel("t"), "name", PUBLIC_ROLE, "SELECT"),
                    column_grant(&rel("t2"), "id", PUBLIC_ROLE, "INSERT"),
                ]
        );
    }

    /// The two grant kinds share a relation but not a key range, so neither
    /// scan can see the other's rows.
    #[test]
    fn a_relation_grant_and_a_column_grant_do_not_collide() {
        use assert2::assert;

        let kv = MemKv::new();
        create_table(&kv, &rel("docs"), cols()).expect("table");
        create_role(&kv, "reader", false).expect("reader");
        grant(&kv, &rel("docs"), "reader", &["SELECT"]);
        grant_columns(&kv, &rel("docs"), &["name"], "reader", &["SELECT"]);

        assert!(
            list_table_privileges(&kv).expect("privileges")
                == vec![TablePrivilege {
                    table: rel("docs"),
                    grantee: "reader".into(),
                    privilege: "SELECT".into(),
                }]
        );
        assert!(
            list_column_privileges(&kv).expect("privileges")
                == vec![column_grant(&rel("docs"), "name", "reader", "SELECT")]
        );

        // Revoking one leaves the other exactly as it was.
        revoke_columns(&kv, &rel("docs"), &["name"], "reader", &["SELECT"]);
        assert!(list_column_privileges(&kv).expect("privileges").is_empty());
        assert!(has_stored_table_privilege(&kv, &rel("docs"), "reader", "SELECT").expect("lookup"));
    }

    #[test]
    fn column_grants_refuse_a_grantee_no_role_holds() {
        use assert2::assert;

        let kv = MemKv::new();
        create_table(&kv, &rel("docs"), cols()).expect("table");

        for grantee in [PUBLIC_ROLE, BOOTSTRAP_ROLE] {
            assert!(
                grant_column_privileges_ops(
                    &kv,
                    &rel("docs"),
                    &strings(&["name"]),
                    &[grantee.to_string()],
                    &strings(&["SELECT"]),
                )
                .is_ok()
            );
        }
        for ops in [
            grant_column_privileges_ops(
                &kv,
                &rel("docs"),
                &strings(&["name"]),
                &strings(&["nobody"]),
                &strings(&["SELECT"]),
            ),
            revoke_column_privileges_ops(
                &kv,
                &rel("docs"),
                &strings(&["name"]),
                &strings(&["nobody"]),
                &strings(&["SELECT"]),
            ),
        ] {
            assert!(
                ops.expect_err("an unheld name is not a grantee")
                    == CatalogError::UndefinedObject("nobody".into())
            );
        }
    }

    #[test]
    fn dropping_a_relation_takes_its_column_grants_with_it() {
        use assert2::assert;

        let kv = MemKv::new();
        create_table(&kv, &rel("docs"), cols()).expect("table");
        view_fixture(&kv, "docs_v");
        grant_columns(&kv, &rel("docs"), &["id", "name"], PUBLIC_ROLE, &["ALL"]);
        grant_columns(&kv, &rel("docs_v"), &["total"], PUBLIC_ROLE, &["SELECT"]);

        drop_table(&kv, &rel("docs")).expect("drop table");
        assert!(column_privileges_of(&kv, &rel("docs")).expect("privileges") == vec![]);
        assert!(
            column_privileges_of(&kv, &rel("docs_v")).expect("privileges")
                == vec![column_grant(&rel("docs_v"), "total", PUBLIC_ROLE, "SELECT")]
        );

        drop_view(&kv, &rel("docs_v")).expect("drop view");
        assert!(list_column_privileges(&kv).expect("privileges").is_empty());

        // A recreated name must start with no grants, which is the leak the
        // deletion exists to prevent.
        create_table(&kv, &rel("docs"), cols()).expect("recreate");
        assert!(
            !has_stored_column_privilege(&kv, &rel("docs"), "name", PUBLIC_ROLE, "SELECT")
                .expect("lookup")
        );
    }

    #[test]
    fn renaming_a_table_moves_its_column_grants_to_the_new_name() {
        use assert2::assert;

        let kv = MemKv::new();
        create_table(&kv, &rel("docs"), cols()).expect("table");
        grant_columns(&kv, &rel("docs"), &["name"], PUBLIC_ROLE, &["ALL"]);
        let ops = rename_table_ops(&kv, &rel("docs"), &rel("papers")).expect("rename ops");
        kv.write_batch(&ops).expect("rename write");

        assert!(column_privileges_of(&kv, &rel("docs")).expect("privileges") == vec![]);
        assert!(
            sorted_column_grants(column_privileges_of(&kv, &rel("papers")).expect("privileges"))
                == sorted_all_column_privileges()
                    .iter()
                    .map(|privilege| column_grant(&rel("papers"), "name", PUBLIC_ROLE, privilege))
                    .collect::<Vec<_>>()
        );
    }

    #[test]
    fn dropping_a_column_takes_only_that_columns_grants() {
        use assert2::assert;

        let kv = MemKv::new();
        create_table(&kv, &rel("docs"), cols()).expect("table");
        create_role(&kv, "reader", false).expect("reader");
        grant_columns(&kv, &rel("docs"), &["id", "name"], "reader", &["ALL"]);
        grant_columns(&kv, &rel("docs"), &["name"], PUBLIC_ROLE, &["SELECT"]);

        let ops = drop_column_privileges_ops(&kv, &rel("docs"), "name").expect("drop ops");
        kv.write_batch(&ops).expect("drop write");

        assert!(column_privilege_names(&kv, &rel("docs"), "name") == Vec::<String>::new());
        assert!(column_privilege_names(&kv, &rel("docs"), "id") == sorted_all_column_privileges());
    }

    #[test]
    fn dropping_a_role_takes_its_column_grants_with_it() {
        use assert2::assert;

        let kv = MemKv::new();
        create_table(&kv, &rel("docs"), cols()).expect("table");
        create_role(&kv, "reader", false).expect("reader");
        grant_columns(&kv, &rel("docs"), &["name"], "reader", &["SELECT"]);
        grant_columns(&kv, &rel("docs"), &["name"], PUBLIC_ROLE, &["SELECT"]);

        drop_role(&kv, "reader").expect("drop role");

        assert!(
            list_column_privileges(&kv).expect("privileges")
                == vec![column_grant(&rel("docs"), "name", PUBLIC_ROLE, "SELECT")]
        );
    }

    fn check_crud(kv: &dyn Kv) {
        let id = create_table(kv, &rel("t"), cols()).expect("create");
        let t = get_table(kv, &rel("t")).expect("lookup");
        assert_eq!(t.id, id);
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.column_index("id"), Some(0));
        assert_eq!(t.column_index("name"), Some(1));
        assert_eq!(t.column_index("nope"), None);
        assert!(t.foreign.is_none());
        assert!(!t.sharded);
        assert_eq!(
            create_table(kv, &rel("t"), cols())
                .expect_err("dup")
                .sqlstate(),
            "42P07"
        );
        let id2 = create_table(kv, &rel("u"), cols()).expect("create u");
        assert_ne!(id, id2);
        drop_table(kv, &rel("t")).expect("drop");
        assert_eq!(
            get_table(kv, &rel("t")).expect_err("gone").sqlstate(),
            "42P01"
        );
        assert_eq!(
            drop_table(kv, &rel("nope"))
                .expect_err("missing")
                .sqlstate(),
            "42P01"
        );
    }

    #[test]
    fn conversion_batch_rejects_metadata_only_rewrite() {
        let kv = MemKv::new();
        create_table(&kv, &rel("conversion"), cols()).expect("create table");

        assert_eq!(
            complete_table_conversion_ops(&kv, &rel("conversion"), None, Vec::new())
                .expect_err("empty rewrite must not publish conversion"),
            CatalogError::IncompleteConversionRewrite
        );
        assert!(
            !get_table(&kv, &rel("conversion"))
                .expect("table remains plain")
                .sharded
        );
    }

    #[test]
    fn views_persist_schema_share_relation_namespace_and_drop() {
        let kv = MemKv::new();
        let columns = vec![Column::new("total", ColumnType::Int4)];
        let options = ViewOptions {
            security_invoker: true,
            security_barrier: false,
            check_option: None,
        };
        create_view(
            &kv,
            &rel("sales_view"),
            "SELECT 1 AS total".into(),
            columns.clone(),
            options,
            "analyst",
        )
        .expect("create view");
        assert_eq!(
            get_view(&kv, &rel("sales_view")).expect("stored view"),
            View {
                name: rel("sales_view"),
                definition: "SELECT 1 AS total".into(),
                owner: "analyst".into(),
                columns,
                options,
            }
        );
        assert_eq!(
            create_table(&kv, &rel("sales_view"), cols())
                .expect_err("view name owns relation namespace")
                .sqlstate(),
            "42P07"
        );
        assert_eq!(
            create_view(
                &kv,
                &rel("sales_view"),
                "SELECT 1".into(),
                vec![],
                ViewOptions::default(),
                BOOTSTRAP_ROLE,
            )
            .expect_err("duplicate view")
            .sqlstate(),
            "42P07"
        );
        drop_view(&kv, &rel("sales_view")).expect("drop view");
        assert_eq!(
            get_view(&kv, &rel("sales_view"))
                .expect_err("dropped view")
                .sqlstate(),
            "42P01"
        );

        create_table(&kv, &rel("sales_table"), cols()).expect("create table");
        assert_eq!(
            drop_view(&kv, &rel("sales_table"))
                .expect_err("table cannot be dropped as a view")
                .sqlstate(),
            "42809"
        );
    }

    #[test]
    fn conversion_batch_rejects_xid_tuple_reinserted_after_delete() {
        let kv = MemKv::new();
        let table_id = create_table(&kv, &rel("conversion"), cols()).expect("create table");
        let tuple_key = crabka_pgmvcc::version::version_key_xid(table_id, 1, 7);
        let xid_tuple = crabka_pgmvcc::version::encode_tuple(
            7,
            0,
            &[Datum::Int4(1), Datum::Text("old".into())],
        );
        kv.put(tuple_key.clone(), xid_tuple.clone())
            .expect("write old tuple");

        assert_eq!(
            complete_table_conversion_ops(
                &kv,
                &rel("conversion"),
                None,
                vec![
                    WriteOp::Delete {
                        key: tuple_key.clone(),
                    },
                    WriteOp::Put {
                        key: tuple_key,
                        value: xid_tuple,
                    },
                ],
            )
            .expect_err("final state contains an xid tuple"),
            CatalogError::IncompleteConversionRewrite
        );
    }

    fn check_fdw_crud(kv: &dyn Kv) {
        create_server(
            kv,
            "s",
            "kafka_fdw",
            vec![("bootstrap".into(), "h:9092".into())],
        )
        .expect("create server");
        let s = get_server(kv, "s").expect("get");
        assert_eq!(s.wrapper, "kafka_fdw");
        assert_eq!(
            create_server(kv, "s", "kafka_fdw", vec![])
                .expect_err("dup")
                .sqlstate(),
            "42710"
        );
        assert_eq!(
            get_server(kv, "nope").expect_err("missing").sqlstate(),
            "42704"
        );

        let cols = vec![Column::new("id", ColumnType::Int4)];
        create_foreign_table(
            kv,
            &rel("orders"),
            cols,
            "s",
            vec![("topic".into(), "orders".into())],
        )
        .expect("ft");
        let t = get_table(kv, &rel("orders")).expect("get ft");
        assert!(t.foreign.is_some());
        assert_eq!(t.columns[0].name, "_partition");
        assert_eq!(t.columns[0].ty, ColumnType::Int4);
        assert_eq!(t.columns[3].name, "_key");
        assert_eq!(t.columns.last().expect("value col").name, "id");
    }

    #[test]
    fn fdw_crud_memkv() {
        check_fdw_crud(&MemKv::new());
    }

    #[test]
    fn create_fdw_persists() {
        let kv = MemKv::new();
        create_fdw(&kv, "w", vec![]).expect("create");
        let fdw = get_fdw(&kv, "w").expect("must be persisted");
        assert_eq!(fdw.name, "w");
    }

    #[test]
    fn foreign_options_must_be_unique() {
        let kv = MemKv::new();
        let options = || {
            vec![
                ("testing".into(), "1".into()),
                ("testing".into(), "2".into()),
            ]
        };
        let error = create_fdw(&kv, "w", options()).expect_err("fdw options must be unique");
        assert_eq!(
            error.to_string(),
            "option \"testing\" provided more than once"
        );
        assert_eq!(error.sqlstate(), "42710");
        assert!(create_server(&kv, "s", "w", options()).is_err());
        assert!(create_user_mapping(&kv, "alice", "s", options()).is_err());

        create_server(&kv, "valid", "w", vec![]).expect("create server");
        assert!(create_foreign_table(&kv, &rel("t"), vec![], "valid", options()).is_err());
    }

    #[test]
    fn drop_fdw_removes() {
        let kv = MemKv::new();
        create_fdw(&kv, "w", vec![]).expect("create");
        drop_fdw(&kv, "w").expect("drop");
        assert_eq!(get_fdw(&kv, "w").expect_err("gone").sqlstate(), "42704");
    }

    #[test]
    fn drop_server_removes() {
        let kv = MemKv::new();
        create_server(&kv, "s", "fdw", vec![]).expect("create");
        drop_server(&kv, "s").expect("drop");
        assert_eq!(get_server(&kv, "s").expect_err("gone").sqlstate(), "42704");
    }

    #[test]
    fn create_user_mapping_persists() {
        let kv = MemKv::new();
        create_user_mapping(&kv, "alice", "s", vec![("k".into(), "v".into())]).expect("create");
        let m = get_user_mapping(&kv, "alice", "s").expect("must be persisted");
        assert_eq!(m.user, "alice");
        assert_eq!(m.server, "s");
    }

    #[test]
    fn drop_user_mapping_removes() {
        let kv = MemKv::new();
        create_user_mapping(&kv, "bob", "s2", vec![]).expect("create");
        drop_user_mapping(&kv, "bob", "s2").expect("drop");
        assert_eq!(
            get_user_mapping(&kv, "bob", "s2")
                .expect_err("gone")
                .sqlstate(),
            "42704"
        );
    }

    #[test]
    fn crud_on_memkv() {
        check_crud(&MemKv::new());
    }

    #[test]
    fn sharded_table_metadata_roundtrips() {
        let kv = MemKv::new();
        let id = create_table_with_options(
            &kv,
            &rel("sharded_t"),
            cols(),
            TableOptions {
                sharded: true,
                ..TableOptions::default()
            },
        )
        .expect("create sharded table");
        let table = get_table(&kv, &rel("sharded_t")).expect("lookup sharded table");
        assert_eq!(table.id, id);
        assert!(table.sharded);
        assert!(table.foreign.is_none());
    }

    #[test]
    fn table_hash_sharding_metadata_roundtrips() {
        let kv = MemKv::new();
        create_table_with_options(
            &kv,
            &rel("hash_t"),
            cols(),
            TableOptions {
                sharded: true,
                ..TableOptions::default()
            },
        )
        .expect("create hash table");
        let sharding = ShardingStrategy::Hash(HashSharding {
            columns: vec!["id".into()],
            buckets: 16,
            co_location_group: Some("group_a".into()),
        });

        kv.write_batch(
            &set_table_sharding_ops(&kv, &rel("hash_t"), Some(&sharding)).expect("sharding ops"),
        )
        .expect("write sharding");

        assert_eq!(
            get_table_sharding(&kv, &rel("hash_t")).expect("read sharding"),
            Some(sharding)
        );
    }

    fn hash_sharding(columns: &[&str]) -> ShardingStrategy {
        ShardingStrategy::Hash(HashSharding {
            columns: columns.iter().map(|column| (*column).to_string()).collect(),
            buckets: 16,
            co_location_group: None,
        })
    }

    /// A hash sharding names exactly one column, whichever seam attaches it.
    /// A wider key has no row encoding, so the catalog refuses the table it
    /// would describe at creation, and does not create an unwritable table. The
    /// column must still exist. The arity gate does not swallow that rejection.
    #[test]
    fn creating_a_table_refuses_a_hash_sharding_that_is_not_one_column() {
        use assert2::assert;

        let arity =
            CatalogError::InvalidSharding("hash sharding requires exactly one column".into());
        for (columns, expected) in [
            (&[][..], Some(arity.clone())),
            (&["id"][..], None),
            (&["id", "name"][..], Some(arity.clone())),
            (&["id", "missing"][..], Some(arity)),
            (
                &["missing"][..],
                Some(CatalogError::UndefinedColumn("missing".into())),
            ),
        ] {
            let kv = MemKv::default();
            let created = create_table_with_sharding_ops(
                &kv,
                &rel("t"),
                cols(),
                TableOptions {
                    sharded: true,
                    ..TableOptions::default()
                },
                Some(&hash_sharding(columns)),
                Vec::new(),
                TableCreation::bootstrap(),
            );
            assert!(created.err() == expected, "{columns:?}");

            // The same arity is refused when the sharding is attached to an
            // existing table instead of declared with it.
            create_table_with_options(
                &kv,
                &rel("existing"),
                cols(),
                TableOptions {
                    sharded: true,
                    ..TableOptions::default()
                },
            )
            .expect("create table");
            let attached =
                set_table_sharding_ops(&kv, &rel("existing"), Some(&hash_sharding(columns)));
            assert!(attached.err() == expected, "{columns:?}");
        }
    }

    /// The accepted single-column shape still creates the table and persists
    /// its sharding in the same batch.
    #[test]
    fn creating_a_table_with_single_column_sharding_persists_it() {
        use assert2::assert;

        let kv = MemKv::default();
        let sharding = ShardingStrategy::Hash(HashSharding {
            columns: vec!["id".into()],
            buckets: 16,
            co_location_group: Some("group_a".into()),
        });
        let (table_id, ops) = create_table_with_sharding_ops(
            &kv,
            &rel("hash_t"),
            cols(),
            TableOptions {
                sharded: true,
                ..TableOptions::default()
            },
            Some(&sharding),
            Vec::new(),
            TableCreation::bootstrap(),
        )
        .expect("create with sharding");
        kv.write_batch(&ops).expect("write batch");

        let table = get_table(&kv, &rel("hash_t")).expect("lookup table");
        assert!(table.id == table_id);
        assert!(table.sharded);
        assert!(get_table_sharding(&kv, &rel("hash_t")).expect("read sharding") == Some(sharding));
    }

    #[test]
    fn create_index_metadata_roundtrips_and_lists_by_table() {
        let kv = MemKv::new();
        let table_id = create_table(&kv, &rel("users"), cols()).expect("create table");

        let index_id = create_index(
            &kv,
            "users_name_idx",
            &rel("users"),
            vec!["name".into()],
            true,
            IndexPlacement::Global,
        )
        .expect("create index");

        let expected = Index {
            id: index_id,
            name: "users_name_idx".into(),
            table: rel("users"),
            table_id,
            columns: vec!["name".into()],
            unique: true,
            placement: IndexPlacement::Global,
            method: IndexMethod::Btree,
            constraint: None,
            without_overlaps: false,
            clustered: false,
            deferral: ConstraintDeferral::Immediate,
        };
        assert_eq!(
            get_index(&kv, &rel("users_name_idx")).expect("index"),
            expected
        );
        assert_eq!(
            list_table_indexes(&kv, &rel("users")).expect("list"),
            vec![expected]
        );
    }

    #[test]
    fn create_index_rejects_missing_columns_and_duplicate_names() {
        let kv = MemKv::new();
        create_table(&kv, &rel("users"), cols()).expect("create table");
        create_index(
            &kv,
            "users_name_idx",
            &rel("users"),
            vec!["name".into()],
            false,
            IndexPlacement::Local,
        )
        .expect("create index");

        assert_eq!(
            create_index(
                &kv,
                "users_name_idx",
                &rel("users"),
                vec!["id".into()],
                false,
                IndexPlacement::Local,
            )
            .expect_err("duplicate")
            .sqlstate(),
            "42P07"
        );
        assert_eq!(
            create_index(
                &kv,
                "users_bad_idx",
                &rel("users"),
                vec!["missing".into()],
                false,
                IndexPlacement::Local,
            )
            .expect_err("missing column")
            .sqlstate(),
            "42703"
        );
    }

    #[test]
    fn crud_on_fjallkv() {
        let dir = tempfile::tempdir().expect("tempdir");
        check_crud(&FjallKv::open(dir.path()).expect("open"));
    }

    /// F-2: `pg_class` rows of kind `S`, `pg_sequence` and
    /// `information_schema.sequences` all enumerate sequences through
    /// [`list_sequences`]. It must report every record, name and all, sorted.
    /// It must not confuse a name that prefixes another.
    #[test]
    fn list_sequences_reports_every_sequence_by_name_in_order() {
        use assert2::assert;
        let kv = MemKv::default();
        for name in ["s_b", "s_a", "s_ab"] {
            let ops =
                create_sequence_ops(&kv, &rel(name), Sequence::new(7, 2, None, None, None, true))
                    .expect("create sequence ops");
            kv.write_batch(&ops).expect("write");
        }
        let listed = list_sequences(&kv).expect("list");
        let names = listed
            .iter()
            .map(|(name, _)| name.name.as_str())
            .collect::<Vec<_>>();
        assert!(names == ["s_a", "s_ab", "s_b"]);
        assert!(listed[0].1 == Sequence::new(7, 2, None, None, None, true));
    }

    #[test]
    fn list_sequences_is_empty_before_any_sequence_exists() {
        use assert2::assert;
        let kv = MemKv::default();
        assert!(list_sequences(&kv).expect("list").is_empty());
    }

    fn foreign_key(
        id: ForeignKeyId,
        name: &str,
        table: (&str, TableId),
        referenced_table: (&str, TableId),
    ) -> ForeignKey {
        ForeignKey {
            id,
            name: name.into(),
            table: rel(table.0),
            table_id: table.1,
            columns: vec!["parent_id".into()],
            referenced_table: rel(referenced_table.0),
            referenced_table_id: referenced_table.1,
            referenced_columns: vec!["id".into()],
            referenced_index_id: 1,
            referenced_index: format!("{}_pkey", referenced_table.0),
            match_type: MatchType::Simple,
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            set_columns: Vec::new(),
            deferrable: false,
            initially_deferred: false,
            validated: true,
        }
    }

    // Every by-table record is stored under its own `(table_id, name)` identity
    // and has exactly one reverse entry; every reverse entry has a record.
    fn assert_foreign_key_families_agree(kv: &dyn Kv) {
        use std::collections::BTreeSet;

        use assert2::assert;

        let mut expected = BTreeSet::new();
        for (key, bytes) in kv
            .scan_prefix(FOREIGN_KEY_BY_TABLE_PREFIX)
            .expect("by-table")
        {
            let fk = deserialize_foreign_key(&bytes).expect("record");
            assert!(key == catalog_foreign_key_key(fk.table_id, &fk.name));
            expected.insert(catalog_foreign_key_ref_key(
                fk.referenced_table_id,
                fk.table_id,
                &fk.name,
            ));
        }
        let stored = kv
            .scan_prefix(FOREIGN_KEY_BY_REF_PREFIX)
            .expect("by-ref")
            .into_iter()
            .map(|(key, value)| {
                assert!(value.is_empty(), "reverse entries carry no payload");
                key
            })
            .collect::<BTreeSet<_>>();
        assert!(stored == expected);
    }

    fn parent_and_child(kv: &dyn Kv) -> (TableId, TableId) {
        let parent = create_table(kv, &rel("parent"), cols()).expect("parent");
        let child = create_table(
            kv,
            &rel("child"),
            vec![
                Column::new("id", ColumnType::Int4),
                Column::new("parent_id", ColumnType::Int4),
            ],
        )
        .expect("child");
        (parent, child)
    }

    /// A created foreign key is readable by its `(child, name)` identity, from
    /// the child side, from the parent side, and from the whole-catalog
    /// enumeration. The two key families agree.
    #[test]
    fn creating_a_foreign_key_indexes_it_from_both_sides() {
        use assert2::assert;
        let kv = MemKv::default();
        let (parent_id, child_id) = parent_and_child(&kv);
        let fk = foreign_key(
            1,
            "child_parent_id_fkey",
            ("child", child_id),
            ("parent", parent_id),
        );

        kv.write_batch(&create_foreign_key_ops(&kv, &fk).expect("create ops"))
            .expect("write");

        assert!(get_foreign_key(&kv, child_id, &fk.name).expect("get") == fk);
        assert!(list_table_foreign_keys(&kv, child_id).expect("child side") == vec![fk.clone()]);
        assert!(
            list_referencing_foreign_keys(&kv, parent_id).expect("parent side") == vec![fk.clone()]
        );
        assert!(list_foreign_keys(&kv).expect("all") == vec![fk]);
        assert!(
            list_table_foreign_keys(&kv, parent_id)
                .expect("parent owns none")
                .is_empty()
        );
        assert!(
            list_referencing_foreign_keys(&kv, child_id)
                .expect("nothing references the child")
                .is_empty()
        );
        assert_foreign_key_families_agree(&kv);
    }

    /// Constraint names are per-relation. The same name on a second child is
    /// correct, and a second one on the same child is 42710. Both then reach
    /// the parent, in creation order. That order is total even when the two
    /// share a name.
    #[test]
    fn constraint_names_are_unique_per_relation_not_per_catalog() {
        use assert2::assert;
        let kv = MemKv::default();
        let (parent_id, child_id) = parent_and_child(&kv);
        let other_id = create_table(&kv, &rel("other"), cols()).expect("other");
        let first = foreign_key(1, "fk_owner", ("child", child_id), ("parent", parent_id));
        let second = foreign_key(2, "fk_owner", ("other", other_id), ("parent", parent_id));

        kv.write_batch(&create_foreign_key_ops(&kv, &first).expect("first ops"))
            .expect("write");
        kv.write_batch(&create_foreign_key_ops(&kv, &second).expect("second ops"))
            .expect("write");

        let duplicate = create_foreign_key_ops(&kv, &first).expect_err("duplicate identity");
        assert!(
            duplicate
                == CatalogError::DuplicateConstraint {
                    name: "fk_owner".into(),
                    relation: "child".into(),
                }
        );
        assert!(duplicate.sqlstate() == "42710");
        assert!(
            duplicate.to_string()
                == "constraint \"fk_owner\" for relation \"child\" already exists"
        );
        let referencing = list_referencing_foreign_keys(&kv, parent_id).expect("parent side");
        assert!(referencing == vec![first, second]);
        assert_foreign_key_families_agree(&kv);
    }

    /// The catalog indexes a self-referencing constraint once on each side. The
    /// parent-side lookup reports it exactly once, even though the child and
    /// the parent are the same relation.
    #[test]
    fn a_self_referencing_foreign_key_is_reported_once() {
        use assert2::assert;
        let kv = MemKv::default();
        let tree_id = create_table(
            &kv,
            &rel("tree"),
            vec![
                Column::new("id", ColumnType::Int4),
                Column::new("parent_id", ColumnType::Int4),
            ],
        )
        .expect("tree");
        let fk = foreign_key(
            1,
            "tree_parent_id_fkey",
            ("tree", tree_id),
            ("tree", tree_id),
        );

        kv.write_batch(&create_foreign_key_ops(&kv, &fk).expect("create ops"))
            .expect("write");

        assert!(
            list_referencing_foreign_keys(&kv, tree_id).expect("parent side") == vec![fk.clone()]
        );
        assert!(list_table_foreign_keys(&kv, tree_id).expect("child side") == vec![fk]);
        assert_foreign_key_families_agree(&kv);
    }

    /// A drop of one constraint clears both key families, and leaves a sibling
    /// constraint on the same relation untouched.
    #[test]
    fn dropping_a_foreign_key_clears_both_key_families() {
        use assert2::assert;
        let kv = MemKv::default();
        let (parent_id, child_id) = parent_and_child(&kv);
        let dropped = foreign_key(1, "fk_dropped", ("child", child_id), ("parent", parent_id));
        let kept = foreign_key(2, "fk_kept", ("child", child_id), ("parent", parent_id));
        for fk in [&dropped, &kept] {
            kv.write_batch(&create_foreign_key_ops(&kv, fk).expect("create ops"))
                .expect("write");
        }

        let (returned, ops) = drop_foreign_key_ops(&kv, child_id, "fk_dropped").expect("drop ops");
        kv.write_batch(&ops).expect("write");

        assert!(returned == dropped);
        assert!(list_table_foreign_keys(&kv, child_id).expect("child side") == vec![kept.clone()]);
        assert!(list_referencing_foreign_keys(&kv, parent_id).expect("parent side") == vec![kept]);
        assert_foreign_key_families_agree(&kv);

        let missing = drop_foreign_key_ops(&kv, child_id, "fk_dropped").expect_err("gone");
        assert!(missing == CatalogError::UndefinedConstraint("fk_dropped".into()));
        assert!(missing.sqlstate() == "42704");
        assert!(get_foreign_key(&kv, child_id, "fk_dropped").is_err());
    }

    /// `DROP TABLE` takes the relation's own constraints out of both families,
    /// including a reverse entry that an earlier partial write orphaned. A
    /// constraint owned by another relation survives.
    #[test]
    fn dropping_a_table_removes_its_foreign_keys_from_both_families() {
        use assert2::assert;
        let kv = MemKv::default();
        let (parent_id, child_id) = parent_and_child(&kv);
        let other_id = create_table(&kv, &rel("other"), cols()).expect("other");
        let dropped = foreign_key(1, "child_fkey", ("child", child_id), ("parent", parent_id));
        let survivor = foreign_key(2, "other_fkey", ("other", other_id), ("parent", parent_id));
        for fk in [&dropped, &survivor] {
            kv.write_batch(&create_foreign_key_ops(&kv, fk).expect("create ops"))
                .expect("write");
        }
        kv.put(
            catalog_foreign_key_ref_key(other_id, child_id, "orphan_fkey"),
            Vec::new(),
        )
        .expect("orphaned reverse entry");

        kv.write_batch(&drop_table_ops(&kv, &rel("child")).expect("drop ops"))
            .expect("write");

        assert!(
            list_table_foreign_keys(&kv, child_id)
                .expect("child side")
                .is_empty()
        );
        assert!(
            list_referencing_foreign_keys(&kv, parent_id).expect("parent side") == vec![survivor]
        );
        assert!(
            list_referencing_foreign_keys(&kv, other_id)
                .expect("orphan swept, so nothing to resolve")
                .is_empty()
        );
        assert_foreign_key_families_agree(&kv);
    }

    /// A rename rewrites only the denormalized display names. The id-keyed
    /// records stay where they are, on the child's own constraints and on every
    /// constraint that references the renamed relation.
    #[test]
    fn renaming_a_relation_rewrites_foreign_key_display_names() {
        use assert2::assert;
        let kv = MemKv::default();
        let (parent_id, child_id) = parent_and_child(&kv);
        let fk = foreign_key(
            1,
            "child_parent_id_fkey",
            ("child", child_id),
            ("parent", parent_id),
        );
        kv.write_batch(&create_foreign_key_ops(&kv, &fk).expect("create ops"))
            .expect("write");

        kv.write_batch(&rename_table_ops(&kv, &rel("child"), &rel("kid")).expect("child rename"))
            .expect("write");
        kv.write_batch(
            &rename_table_ops(&kv, &rel("parent"), &rel("ancestor")).expect("parent rename"),
        )
        .expect("write");

        assert!(
            get_foreign_key(&kv, child_id, &fk.name).expect("get")
                == ForeignKey {
                    table: rel("kid"),
                    referenced_table: rel("ancestor"),
                    ..fk
                }
        );
        assert_foreign_key_families_agree(&kv);
    }

    /// A rename of a self-referencing relation must rewrite both display names.
    /// The child scan and the parent scan both reach the constraint, and the
    /// second pass must not undo the first.
    #[test]
    fn renaming_a_self_referencing_relation_rewrites_both_display_names() {
        use assert2::assert;
        let kv = MemKv::default();
        let tree_id = create_table(
            &kv,
            &rel("tree"),
            vec![
                Column::new("id", ColumnType::Int4),
                Column::new("parent_id", ColumnType::Int4),
            ],
        )
        .expect("tree");
        let fk = foreign_key(
            1,
            "tree_parent_id_fkey",
            ("tree", tree_id),
            ("tree", tree_id),
        );
        kv.write_batch(&create_foreign_key_ops(&kv, &fk).expect("create ops"))
            .expect("write");

        kv.write_batch(&rename_table_ops(&kv, &rel("tree"), &rel("forest")).expect("rename"))
            .expect("write");

        assert!(
            get_foreign_key(&kv, tree_id, &fk.name).expect("get")
                == ForeignKey {
                    table: rel("forest"),
                    referenced_table: rel("forest"),
                    ..fk
                }
        );
        assert_foreign_key_families_agree(&kv);
    }

    /// The whole-catalog enumeration reports every constraint in child-table-id
    /// then name order, whichever order they were created in.
    #[test]
    fn list_foreign_keys_enumerates_the_catalog_in_key_order() {
        use assert2::assert;
        let kv = MemKv::default();
        let (parent_id, child_id) = parent_and_child(&kv);
        let child_b = foreign_key(1, "fk_b", ("child", child_id), ("parent", parent_id));
        let child_a = foreign_key(2, "fk_a", ("child", child_id), ("parent", parent_id));
        let parent_self = foreign_key(3, "fk_self", ("parent", parent_id), ("parent", parent_id));
        for fk in [&child_b, &child_a, &parent_self] {
            kv.write_batch(&create_foreign_key_ops(&kv, fk).expect("create ops"))
                .expect("write");
        }

        assert!(list_foreign_keys(&kv).expect("all") == vec![parent_self, child_a, child_b]);
    }

    /// Both per-relation listings report constraints in creation order, not by
    /// name. `PostgreSQL` fires referential triggers in OID order, so a
    /// constraint declared first acts first even when its name sorts late.
    #[test]
    fn per_relation_listings_are_ordered_by_creation_not_by_name() {
        use assert2::assert;
        let kv = MemKv::default();
        let (parent_id, child_id) = parent_and_child(&kv);
        // Names descend as ids ascend, so name order and creation order are
        // exact opposites and no listing can satisfy both.
        let zz = foreign_key(1, "zz", ("child", child_id), ("parent", parent_id));
        let mm = foreign_key(2, "mm", ("child", child_id), ("parent", parent_id));
        let aa = foreign_key(3, "aa", ("child", child_id), ("parent", parent_id));
        for fk in [&zz, &mm, &aa] {
            kv.write_batch(&create_foreign_key_ops(&kv, fk).expect("create ops"))
                .expect("write");
        }

        let expected = vec![zz, mm, aa];
        assert!(list_table_foreign_keys(&kv, child_id).expect("child side") == expected);
        assert!(list_referencing_foreign_keys(&kv, parent_id).expect("parent side") == expected);
    }

    /// The id cursor hands out ascending ids and does not read the counter
    /// again, so several constraints created in one batch are ordered against
    /// each other. The counter that each `create` op carries then leaves the
    /// stored value past the last id used, so the next batch does not repeat
    /// them.
    #[test]
    fn the_foreign_key_id_cursor_spans_a_batch_and_leaves_the_counter_past_it() {
        use assert2::assert;
        let kv = MemKv::default();
        let (parent_id, child_id) = parent_and_child(&kv);
        let mut ids = ForeignKeyIds::default();
        let mut batch = Vec::new();
        for name in ["zz", "mm"] {
            let id = ids.allocate(&kv).expect("allocate");
            let fk = foreign_key(id, name, ("child", child_id), ("parent", parent_id));
            batch.extend(create_foreign_key_ops(&kv, &fk).expect("create ops"));
        }
        kv.write_batch(&batch).expect("write");

        let listed = list_table_foreign_keys(&kv, child_id).expect("child side");
        assert!(
            listed.iter().map(|fk| fk.id).collect::<Vec<_>>() == vec![1, 2],
            "one cursor, ascending ids"
        );
        // A fresh cursor reads the stored counter, which the batch moved past
        // both ids.
        assert!(
            ForeignKeyIds::default().allocate(&kv).expect("allocate") == 3,
            "the counter survives the batch"
        );
    }

    /// A reverse entry whose authoritative record has gone missing is catalog
    /// corruption, not an empty result. The parent-side read must not silently
    /// skip a constraint it is meant to enforce.
    #[test]
    fn a_reverse_entry_without_a_record_is_reported_as_corruption() {
        use assert2::assert;
        let kv = MemKv::default();
        let (parent_id, child_id) = parent_and_child(&kv);
        kv.put(
            catalog_foreign_key_ref_key(parent_id, child_id, "ghost_fkey"),
            Vec::new(),
        )
        .expect("orphaned reverse entry");

        let error = list_referencing_foreign_keys(&kv, parent_id).expect_err("orphan");
        assert!(error.sqlstate() == "XX000");
        assert!(error.to_string().contains("ghost_fkey"));
    }

    /// The string `version()` reports. Clients parse the `PostgreSQL` version
    /// out of the prefix, so the prefix is fixed even as the build tag moves.
    #[test]
    fn the_server_version_string_is_postgresql_shaped() {
        use assert2::assert;
        let version = server_version_string();
        assert!(version.starts_with("PostgreSQL 18.4 (Crabka Gres "));
        assert!(version.ends_with(", 64-bit"));
        assert!(version.contains(") on "));
    }
}

/// One `CREATE CAST` conversion, as `pg_cast` records it.
///
/// `castsource`/`casttarget` are the identity: `PostgreSQL`'s only unique index
/// on `pg_cast` is the type pair, so a second `CREATE CAST` over the same pair
/// is a duplicate-object error rather than a second row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserCast {
    /// `pg_cast.oid`. Allocated in creation order, which is the order
    /// `PostgreSQL` reports a cascade in and the only reason it is stored.
    pub oid: u32,
    /// `pg_cast.castsource`.
    pub source: u32,
    /// `pg_cast.casttarget`.
    pub target: u32,
    /// `pg_cast.castmethod`: `b` binary-coercible, `f` via a function, `i` via
    /// the source's output and the target's input function.
    pub method: char,
    /// `pg_cast.castcontext`: `e` explicit, `a` assignment, `i` implicit.
    pub context: char,
    /// `pg_cast.castfunc` as a decimal routine oid, empty for a `WITHOUT
    /// FUNCTION` or `WITH INOUT` cast.
    pub function: String,
}

/// Serialize a cast: `oid | method | context | function`.
#[must_use]
pub fn serialize_user_cast(cast: &UserCast) -> Vec<u8> {
    let mut out = cast.oid.to_be_bytes().to_vec();
    out.push(u8::try_from(cast.method).unwrap_or(b'b'));
    out.push(u8::try_from(cast.context).unwrap_or(b'e'));
    out.extend_from_slice(cast.function.as_bytes());
    out
}

/// The first oid a user-declared cast takes. Above every built-in `pg_cast`
/// row, which `crabka_pgexec::builtin_casts` numbers from 10000.
pub const FIRST_USER_CAST_OID: u32 = 200_000;

/// The write batch that records a cast, with its oid drawn from the durable
/// counter.
///
/// # Errors
///
/// Returns storage or corruption errors from the catalog KV seam.
///
/// # Panics
///
/// If a four-byte slice does not fit a `u32`, which the length check above
/// already established.
pub fn create_user_cast_ops(kv: &dyn Kv, cast: &UserCast) -> Result<Vec<WriteOp>, CatalogError> {
    let oid = match kv.get(&key::meta_next_cast_oid_key())? {
        Some(bytes) => u32::from_be_bytes(
            bytes
                .get(..4)
                .ok_or_else(|| KvError::CorruptRow("truncated next cast oid".into()))?
                .try_into()
                .expect("four bytes fit u32"),
        ),
        None => FIRST_USER_CAST_OID,
    };
    let cast = UserCast {
        oid,
        ..cast.clone()
    };
    Ok(vec![
        WriteOp::Put {
            key: key::meta_next_cast_oid_key(),
            value: oid.saturating_add(1).to_be_bytes().to_vec(),
        },
        WriteOp::Put {
            key: key::cast_key(cast.source, cast.target),
            value: serialize_user_cast(&cast),
        },
    ])
}

/// The write batch that forgets a cast.
#[must_use]
pub fn drop_user_cast_ops(source: u32, target: u32) -> Vec<WriteOp> {
    vec![WriteOp::Delete {
        key: key::cast_key(source, target),
    }]
}

/// Read one cast by its type pair.
///
/// # Errors
///
/// Returns storage or corruption errors from the catalog KV seam.
pub fn get_user_cast(kv: &dyn Kv, source: u32, target: u32) -> Result<Option<UserCast>, KvError> {
    let Some(bytes) = kv.get(&key::cast_key(source, target))? else {
        return Ok(None);
    };
    decode_user_cast(source, target, &bytes).map(Some)
}

/// Every user-defined cast, in creation (oid) order — the order `PostgreSQL`
/// reports a cascade in.
///
/// # Errors
///
/// Returns storage or corruption errors from the catalog KV seam.
///
/// # Panics
///
/// If a four-byte slice does not fit a `u32`, which the key-width check above
/// already established.
pub fn list_user_casts(kv: &dyn Kv) -> Result<Vec<UserCast>, KvError> {
    let prefix = key::cast_prefix();
    let mut casts = Vec::new();
    for (stored_key, bytes) in kv.scan_prefix(&prefix)? {
        let suffix = stored_key
            .strip_prefix(prefix.as_slice())
            .ok_or_else(|| KvError::CorruptRow("cast key lost its prefix".into()))?;
        let pair: [u8; 8] = suffix
            .try_into()
            .map_err(|_| KvError::CorruptRow("cast key is not a type pair".into()))?;
        let source = u32::from_be_bytes(pair[..4].try_into().expect("four bytes fit u32"));
        let target = u32::from_be_bytes(pair[4..].try_into().expect("four bytes fit u32"));
        casts.push(decode_user_cast(source, target, &bytes)?);
    }
    casts.sort_by_key(|cast| cast.oid);
    Ok(casts)
}

fn decode_user_cast(source: u32, target: u32, bytes: &[u8]) -> Result<UserCast, KvError> {
    let corrupt = || KvError::CorruptRow("truncated cast record".into());
    let (oid, rest) = bytes.split_at_checked(4).ok_or_else(corrupt)?;
    let oid = u32::from_be_bytes(oid.try_into().expect("four bytes fit u32"));
    let (&method, rest) = rest.split_first().ok_or_else(corrupt)?;
    let (&context, rest) = rest.split_first().ok_or_else(corrupt)?;
    Ok(UserCast {
        oid,
        source,
        target,
        method: char::from(method),
        context: char::from(context),
        function: String::from_utf8(rest.to_vec())
            .map_err(|_| KvError::CorruptRow("cast function name is not UTF-8".into()))?,
    })
}
