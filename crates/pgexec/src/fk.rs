//! Foreign keys on the local MVCC write path: DDL resolution, the per-statement
//! check queue and its end-of-statement drain, referential actions, and the
//! transaction-scoped deferred queue.
//!
//! # Timing
//!
//! `PostgreSQL` implements referential integrity as `AFTER ROW` triggers, so
//! even a `NOT DEFERRABLE` constraint is checked once the statement's rows
//! exist. `INSERT INTO t (id, boss) VALUES (1, 1)` against a self-referencing
//! foreign key succeeds there with no `DEFERRABLE` clause anywhere. The write
//! path therefore never probes inline: the hooks
//! ([`FkCheckQueue::after_insert`] and friends) only append, and
//! [`drain_statement_checks`] runs once the whole statement — `WITH` list plus
//! body — is done.
//!
//! # What the drain reads
//!
//! [`FkExecContext::kv`] is a read-only overlay: the statement's pending write
//! batch layered over the store. The drain therefore reads the statement's own
//! rows, which is what lets the self-referencing insert above find the parent
//! it just wrote, and what lets a parent-side re-probe see a key another part
//! of the same command re-supplied. The commit-time drain needs no overlay of
//! its own: the statements whose checks it runs are finished, and their rows
//! are in the KV under this transaction's xid.
//!
//! The overlay grows as the drain runs: an action's ops are folded into it
//! before [`FkCascade::modify_row`] returns, as well as handed back to the
//! caller. Everything a probe sees is therefore the transaction's current
//! state — a row an action deleted reads as gone, a row it re-keyed reads under
//! its new key — so no probe has a set of exceptions to carry, and a second
//! constraint's action reaching the same row operates on the image the first one
//! left. It is also what terminates a cascade cycle, exactly as it terminates
//! `PostgreSQL`'s: the row the cycle comes back to no longer matches the key
//! being chased.
//!
//! # Deferral
//!
//! Only *checks* defer. `PostgreSQL` creates a constraint's check triggers with
//! its declared deferrability and its referential-action triggers non-deferrable,
//! so a `DEFERRABLE INITIALLY DEFERRED ON DELETE CASCADE` still deletes its
//! children inside the `DELETE` statement and only the "does this row have a
//! parent" checks wait for `COMMIT`. [`PendingCheck::is_check`] is that rule and
//! [`DeferredConstraints::defer`] is the only thing that applies it, which is
//! what leaves the commit-time drain with no referential action of its own to
//! run.
//!
//! # Concurrency
//!
//! Both sides of a foreign key name the same lock identity: the referenced
//! index's entry prefix for the key value, the byte string the uniqueness check
//! already locks. The child side takes it [`FkLockMode::Shared`], the parent
//! side [`FkLockMode::Exclusive`], so many children of one parent key never
//! contend, and a *non-key* update of the parent never touches the key lock at
//! all. No new lock mode exists; key locks and row locks share one wait-for
//! graph, so a cycle spanning both is still reported as `40P01`.
//!
//! The engine's pre-existing `FOR KEY SHARE` row-lock over-blocking is a
//! different thing entirely and this protocol does not inherit it.
//!
//! # Column order
//!
//! `PostgreSQL` stores both column lists in the order the `FOREIGN KEY` clause
//! writes them, paired positionally, and matches the referenced *index* by
//! column set. `crabka_pgkv::key::secondary_index_entry_prefix` length-prefixes
//! the whole encoded tuple, so key bytes are order-sensitive and a partial value
//! list is not a byte prefix of a full key: a composite foreign key whose column
//! order differs from the referenced key's would probe the wrong bytes while
//! every single-column test passed. [`key_permutation`] is the one function that
//! computes that reordering, and every probe goes through it.
//!
//! # Engine seams
//!
//! The drain reaches the engine through two traits rather than the executor's
//! internal write context: [`FkKeyLocks`] for the key lock and [`FkCascade`] for
//! the row modifications a referential action performs. That keeps the MVCC row
//! mutation (and the statement's write bookkeeping, which is what terminates
//! cascade cycles) in the write path that owns it, and makes this module's logic
//! testable without an engine. It is also the seam the sharded-table wave needs:
//! the probe target is already a parameter.

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    sync::Arc,
};

use crabka_pgcatalog::{
    Column, ForeignKey, ForeignKeyId, Index, IndexConstraint, MatchType, ReferentialAction,
    RelationName, Table, TableId,
};
use crabka_pgkv::{Kv, WriteOp};
use crabka_pgmvcc::visibility::Snapshot;
use crabka_pgparser::ast::{ConstraintAttributes, ForeignKeyRef};
use crabka_pgtypes::{ColumnType, Datum};

use crate::{
    clock::EvalCtx,
    error::{
        DependentForeignKey, ExecError, ForeignKeyTypeMismatch, ForeignKeyViolation,
        ForeignKeyViolationSide,
    },
};

// ---------------------------------------------------------------------------
// DDL resolution
// ---------------------------------------------------------------------------

/// A relation a `FOREIGN KEY` clause names, described without going through the
/// catalog.
///
/// `CREATE TABLE` resolves its own constraints before the relation exists, so
/// neither side of a self-reference can be looked up; the same shape describes a
/// relation that *is* in the catalog, so [`resolve_foreign_key`] has one code
/// path rather than two.
pub struct FkRelation<'a> {
    pub id: TableId,
    pub name: &'a RelationName,
    pub columns: &'a [Column],
    /// The relation's indexes, with the ids they will be created under. Only
    /// the parent side reads this.
    pub indexes: &'a [Index],
    pub sharded: bool,
}

impl<'a> FkRelation<'a> {
    /// Borrow a catalog relation and its indexes as a resolution input.
    #[must_use]
    pub fn of(table: &'a Table, indexes: &'a [Index]) -> Self {
        Self {
            id: table.id,
            name: &table.name,
            columns: &table.columns,
            indexes,
            sharded: table.sharded,
        }
    }

    fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|column| column.name == name)
    }
}

/// Where the parent relation came from, so the two cases share one view.
enum ParentSource<'a> {
    /// The ordinary case: read from the catalog by name. Boxed because the
    /// in-flight variant is one pointer wide and a `Table` is an order of
    /// magnitude larger.
    Catalog(Box<Table>, Vec<Index>),
    /// `CREATE TABLE t (… REFERENCES t …)`: the relation being created.
    InFlight(&'a FkRelation<'a>),
}

impl ParentSource<'_> {
    fn view(&self) -> FkRelation<'_> {
        match self {
            ParentSource::Catalog(table, indexes) => FkRelation::of(table, indexes),
            ParentSource::InFlight(relation) => FkRelation {
                id: relation.id,
                name: relation.name,
                columns: relation.columns,
                indexes: relation.indexes,
                sharded: relation.sharded,
            },
        }
    }
}

/// One `FOREIGN KEY (…) REFERENCES …` clause, as `CREATE TABLE` and
/// `ALTER TABLE … ADD CONSTRAINT` both present it.
pub struct ForeignKeyRequest<'a> {
    /// The creation-order id to stamp the constraint with, from the statement's
    /// [`crabka_pgcatalog::ForeignKeyIds`] cursor. It decides which of two
    /// constraints acts first, so it has to ascend with the order the clauses
    /// are written — one cursor per statement, not one read per clause.
    pub id: ForeignKeyId,
    /// An explicit `CONSTRAINT <name>` label, or `None` to derive
    /// `<table>_<col>…_fkey`.
    pub name: Option<&'a str>,
    /// The referencing columns, in the order the clause writes them.
    pub columns: &'a [String],
    /// The parsed `REFERENCES` target.
    pub reference: &'a ForeignKeyRef,
    pub attributes: ConstraintAttributes,
    /// `pg_constraint.convalidated`. `ALTER TABLE … ADD CONSTRAINT … NOT VALID`
    /// clears it; `CREATE TABLE` never does, because `PostgreSQL` ignores
    /// `NOT VALID` where there are no stored rows to validate.
    pub validated: bool,
    /// The relation being created, when the clause references *it* — the
    /// `CREATE TABLE t (… REFERENCES t …)` case, where the parent is not in the
    /// catalog yet. Deliberately explicit: a caller that has no in-flight
    /// relation passes `None` and the parent is always the catalog's.
    pub self_reference: Option<&'a FkRelation<'a>>,
}

/// The name `PostgreSQL` derives for an unnamed foreign key: the referencing
/// relation, every referencing column in clause order, then `fkey`.
///
/// The relation contributes its bare name, never its schema: `ChooseConstraintName`
/// builds the label from `RelationGetRelationName`, so a `s.child` referencing
/// `s.parent` carries a `child_pid_fkey`, and a constraint name is per-relation
/// anyway.
#[must_use]
pub fn default_foreign_key_name(table: &RelationName, columns: &[String]) -> String {
    let mut name = table.name.clone();
    for column in columns {
        name.push('_');
        name.push_str(column);
    }
    name.push_str("_fkey");
    name
}

/// Turn one parsed `FOREIGN KEY` clause into the catalog record, applying every
/// DDL-time validation `PostgreSQL` applies and reporting its SQLSTATEs.
///
/// The referenced-column list may be empty, meaning the parent's primary key.
/// The referenced *index* is matched by column set — not by order — and the
/// record keeps both column lists in clause order, paired positionally, exactly
/// as `pg_constraint.conkey`/`confkey` do.
///
/// # Errors
///
/// - `42703` a referencing or referenced column does not exist;
/// - `42P01` the referenced relation does not exist;
/// - `42809` the referenced relation is not a table;
/// - `42830` no unique constraint matches the referenced columns, the two lists
///   disagree in length, or the referenced list repeats a column;
/// - `42804` a column pair's types are not comparable;
/// - `42P10` an `ON DELETE SET …` column list names a non-key column;
/// - `0A000` either relation is sharded, which this wave does not enforce
///   across ranges.
pub fn resolve_foreign_key(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    child: &FkRelation<'_>,
    request: &ForeignKeyRequest<'_>,
) -> Result<ForeignKey, ExecError> {
    let name = request.name.map_or_else(
        || default_foreign_key_name(child.name, request.columns),
        ToString::to_string,
    );
    if child.sharded {
        return Err(sharded_refusal(&name));
    }
    for column in request.columns {
        if child.column(column).is_none() {
            return Err(ExecError::UndefinedForeignKeyColumn(column.clone()));
        }
    }

    // The parent may be the relation being created, which no catalog read can
    // find; anything else is looked up by name.
    let referenced_name = crate::relname::resolve_relation(
        catalog_kv,
        resolution,
        &request.reference.table,
        crate::relname::SchemaDisposition::Utility,
    )?;
    let source = match request
        .self_reference
        .filter(|relation| *relation.name == referenced_name)
    {
        Some(in_flight) => ParentSource::InFlight(in_flight),
        None => {
            let (table, indexes) = load_referenced_relation(catalog_kv, &referenced_name)?;
            ParentSource::Catalog(Box::new(table), indexes)
        }
    };
    let parent = source.view();
    if parent.sharded {
        return Err(sharded_refusal(&name));
    }
    persistence_boundary(child.name, parent.name)?;

    let referenced_columns = if request.reference.columns.is_empty() {
        primary_key_columns(&parent)?
    } else {
        request.reference.columns.clone()
    };
    let mut seen = BTreeSet::new();
    for column in &referenced_columns {
        if !seen.insert(column.as_str()) {
            return Err(ExecError::DuplicateForeignKeyReferencedColumn);
        }
    }
    if referenced_columns.len() != request.columns.len() {
        return Err(ExecError::ForeignKeyColumnCountMismatch);
    }
    for column in &referenced_columns {
        if parent.column(column).is_none() {
            return Err(ExecError::UndefinedForeignKeyColumn(column.clone()));
        }
    }

    let referenced_index = select_referenced_index(&parent, &referenced_columns)?;
    for (referencing, referenced) in request.columns.iter().zip(referenced_columns.iter()) {
        let child_type = child
            .column(referencing)
            .ok_or_else(|| ExecError::UndefinedForeignKeyColumn(referencing.clone()))?
            .ty;
        let parent_type = parent
            .column(referenced)
            .ok_or_else(|| ExecError::UndefinedForeignKeyColumn(referenced.clone()))?
            .ty;
        if !types_are_comparable(child_type, parent_type) {
            return Err(ExecError::ForeignKeyTypeMismatch(Box::new(
                ForeignKeyTypeMismatch {
                    constraint: name,
                    referencing_column: referencing.clone(),
                    referenced_column: referenced.clone(),
                    referencing_type: child_type.name().to_string(),
                    referenced_type: parent_type.name().to_string(),
                },
            )));
        }
    }
    for column in &request.reference.set_columns {
        if !request.columns.iter().any(|key| key == column) {
            return Err(ExecError::ForeignKeySetColumnNotInKey(column.clone()));
        }
    }

    Ok(ForeignKey {
        id: request.id,
        name,
        table: child.name.clone(),
        table_id: child.id,
        columns: request.columns.to_vec(),
        referenced_table: parent.name.clone(),
        referenced_table_id: parent.id,
        referenced_columns,
        referenced_index_id: referenced_index.id,
        referenced_index: referenced_index.name.clone(),
        match_type: match_type_of(request.reference.match_type),
        on_delete: action_of(request.reference.on_delete),
        on_update: action_of(request.reference.on_update),
        set_columns: request.reference.set_columns.clone(),
        deferrable: request.attributes.deferrable || request.attributes.initially_deferred,
        initially_deferred: request.attributes.initially_deferred,
        validated: request.validated,
    })
}

/// A constraint may not cross the temporary/permanent line, in either
/// direction.
///
/// The second direction is the one a reader expects not to exist, and it is
/// real: a *temporary* table may not reference a permanent one either. Both are
/// `42P16`, with the wording verified against `postgres:18.4`:
///
/// ```text
/// CREATE TABLE perm (a int REFERENCES <temp>(id));
///   42P16  constraints on permanent tables may reference only permanent tables
/// CREATE TEMP TABLE tmp (a int REFERENCES <permanent>(id));
///   42P16  constraints on temporary tables may reference only temporary tables
/// ```
fn persistence_boundary(
    child: &crabka_pgcatalog::RelationName,
    parent: &crabka_pgcatalog::RelationName,
) -> Result<(), ExecError> {
    let child_temp = crabka_pgcatalog::is_temp_schema(&child.schema);
    if child_temp == crabka_pgcatalog::is_temp_schema(&parent.schema) {
        return Ok(());
    }
    let message = if child_temp {
        "constraints on temporary tables may reference only temporary tables"
    } else {
        "constraints on permanent tables may reference only permanent tables"
    };
    Err(ExecError::InvalidTableDefinition(message.into()))
}

fn sharded_refusal(constraint: &str) -> ExecError {
    ExecError::Unsupported(format!(
        "foreign key constraint \"{constraint}\" on a sharded table is not supported"
    ))
}

/// Read the referenced relation, distinguishing "no such relation" (42P01) from
/// "that relation is not a table" (42809), which `PostgreSQL` words differently
/// from its general wrong-object-type message.
///
/// The 42809 names the relation `RelationGetRelationName` would — the bare name,
/// because the relation was opened. The 42P01 the catalog raises instead names it
/// as written, which is the whole point of that message.
fn load_referenced_relation(
    catalog_kv: &dyn Kv,
    name: &RelationName,
) -> Result<(Table, Vec<Index>), ExecError> {
    match crabka_pgcatalog::get_table(catalog_kv, name) {
        Ok(table) => {
            if table.foreign.is_some() {
                return Err(ExecError::ReferencedRelationNotATable(name.name.clone()));
            }
            let indexes = crabka_pgcatalog::list_table_indexes(catalog_kv, name)?;
            Ok((table, indexes))
        }
        Err(error) => {
            if crabka_pgcatalog::get_view(catalog_kv, name).is_ok()
                || crabka_pgcatalog::get_sequence(catalog_kv, name).is_ok()
            {
                return Err(ExecError::ReferencedRelationNotATable(name.name.clone()));
            }
            Err(error.into())
        }
    }
}

/// The parent's primary-key columns, in index order — what an omitted
/// referenced-column list means.
fn primary_key_columns(parent: &FkRelation<'_>) -> Result<Vec<String>, ExecError> {
    parent
        .indexes
        .iter()
        .find(|index| index.constraint == Some(IndexConstraint::PrimaryKey))
        .map(|index| index.columns.clone())
        .ok_or_else(|| no_unique_constraint(parent))
}

/// 42830, naming the parent the way `PostgreSQL` does: with
/// `RelationGetRelationName`, so the message carries the bare relation name.
fn no_unique_constraint(parent: &FkRelation<'_>) -> ExecError {
    ExecError::NoUniqueConstraintForReferencedTable(parent.name.name.clone())
}

/// The unique index that proves the referenced columns are a key.
///
/// Matching is by column *set*, as `PostgreSQL`'s is. When several indexes
/// match, the primary key wins, then the lowest-named unique constraint, then
/// the lowest-named bare unique index — `information_schema` reports a
/// constraint name for the first two and NULL for the third, so the choice is
/// observable.
fn select_referenced_index<'a>(
    parent: &'a FkRelation<'a>,
    referenced_columns: &[String],
) -> Result<&'a Index, ExecError> {
    let wanted: BTreeSet<&str> = referenced_columns.iter().map(String::as_str).collect();
    let matches = || {
        parent.indexes.iter().filter(|index| {
            index.unique
                && index.columns.len() == referenced_columns.len()
                && index
                    .columns
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>()
                    == wanted
        })
    };
    let rank = |index: &Index| match index.constraint {
        Some(IndexConstraint::PrimaryKey) => 0,
        Some(IndexConstraint::Unique) => 1,
        None => 2,
    };
    matches()
        .min_by(|left, right| {
            rank(left)
                .cmp(&rank(right))
                .then_with(|| left.name.cmp(&right.name))
        })
        .ok_or_else(|| no_unique_constraint(parent))
}

/// The comparison families a foreign key may pair.
///
/// `PostgreSQL` requires an equality operator in the referenced index's operator
/// family, which is what makes `integer` and `numeric` incompatible even though
/// both are numbers. Grouping by family reproduces that at the granularity the
/// engine's types have, and the members of one family share a `Datum`
/// representation or convert losslessly into one another at probe time (see
/// [`align_probe_value`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypeFamily {
    Integer,
    Float,
    Numeric,
    Text,
    Other(ColumnType),
}

fn type_family(ty: ColumnType) -> TypeFamily {
    match ty {
        ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 => TypeFamily::Integer,
        ColumnType::Float4 | ColumnType::Float8 => TypeFamily::Float,
        ColumnType::Numeric(_) => TypeFamily::Numeric,
        ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Char(_) => TypeFamily::Text,
        other => TypeFamily::Other(other),
    }
}

fn types_are_comparable(child: ColumnType, parent: ColumnType) -> bool {
    type_family(child) == type_family(parent)
}

fn match_type_of(parsed: crabka_pgparser::ast::MatchType) -> MatchType {
    match parsed {
        crabka_pgparser::ast::MatchType::Simple => MatchType::Simple,
        crabka_pgparser::ast::MatchType::Full => MatchType::Full,
    }
}

fn action_of(parsed: crabka_pgparser::ast::ReferentialAction) -> ReferentialAction {
    use crabka_pgparser::ast::ReferentialAction as Parsed;
    match parsed {
        Parsed::NoAction => ReferentialAction::NoAction,
        Parsed::Restrict => ReferentialAction::Restrict,
        Parsed::Cascade => ReferentialAction::Cascade,
        Parsed::SetNull => ReferentialAction::SetNull,
        Parsed::SetDefault => ReferentialAction::SetDefault,
    }
}

// ---------------------------------------------------------------------------
// Key math
// ---------------------------------------------------------------------------

/// Where each referenced-index column sits in the `FOREIGN KEY` clause.
///
/// `permutation[j]` is the clause position of `index_columns[j]`, so
/// `permuted[j] = clause_values[permutation[j]]` rebuilds the value tuple in the
/// index's own order — the only order whose bytes match a stored index entry.
/// Returns `None` when the two lists are not permutations of one another, which
/// is a catalog inconsistency rather than a user error.
///
/// This is the single place the reordering is computed. Every probe, every key
/// lock and every child search goes through it, because a composite foreign key
/// written `FOREIGN KEY (b, a) REFERENCES p (y, x)` over a `(x, y)` index probes
/// the wrong bytes without it while every single-column case still passes.
#[must_use]
pub fn key_permutation(clause_columns: &[String], index_columns: &[String]) -> Option<Vec<usize>> {
    if clause_columns.len() != index_columns.len() {
        return None;
    }
    let mut permutation = Vec::with_capacity(index_columns.len());
    for index_column in index_columns {
        permutation.push(clause_columns.iter().position(|c| c == index_column)?);
    }
    Some(permutation)
}

/// Reorder clause-ordered key values into the referenced index's order.
#[must_use]
pub fn permute_key(values: &[Datum], permutation: &[usize]) -> Vec<Datum> {
    permutation
        .iter()
        .filter_map(|&position| values.get(position).cloned())
        .collect()
}

/// What `MATCH` semantics say about one key before any probe happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOutcome {
    /// The key satisfies the constraint without a probe: `MATCH SIMPLE` with any
    /// NULL, `MATCH FULL` with every column NULL.
    Satisfied,
    /// Probe the referenced key.
    Probe,
    /// `MATCH FULL` mixed null and non-null columns — a 23503 whose DETAIL names
    /// no key.
    MixedNulls,
}

/// Apply `MATCH` semantics to one child-side key.
#[must_use]
pub fn match_outcome(match_type: MatchType, key: &[Datum]) -> MatchOutcome {
    let nulls = key.iter().filter(|value| value.is_null()).count();
    if nulls == 0 {
        return MatchOutcome::Probe;
    }
    match match_type {
        MatchType::Simple => MatchOutcome::Satisfied,
        MatchType::Full if nulls == key.len() => MatchOutcome::Satisfied,
        MatchType::Full => MatchOutcome::MixedNulls,
    }
}

/// Pull `columns`' values out of `row` by the ordinals resolved once per
/// statement.
fn key_of(row: &[Datum], ordinals: &[usize], names: &[String]) -> Result<Vec<Datum>, ExecError> {
    ordinals
        .iter()
        .enumerate()
        .map(|(position, &ordinal)| {
            row.get(ordinal).cloned().ok_or_else(|| {
                ExecError::UndefinedForeignKeyColumn(
                    names.get(position).cloned().unwrap_or_default(),
                )
            })
        })
        .collect()
}

/// Resolve `columns` to their ordinals in `table`.
fn column_ordinals(table: &Table, columns: &[String]) -> Result<Vec<usize>, ExecError> {
    columns
        .iter()
        .map(|column| {
            table
                .column_index(column)
                .ok_or_else(|| ExecError::UndefinedForeignKeyColumn(column.clone()))
        })
        .collect()
}

/// Convert a key value into the representation the referenced column stores.
///
/// Index-entry bytes are tagged per `Datum` variant, so a `bigint` child of an
/// `integer` parent must probe with an `Int4` or it reads a key that cannot
/// exist. `None` means the value has no counterpart in the target type — an
/// out-of-range integer, say — which is exactly "the key is not present".
fn align_probe_value(value: &Datum, target: ColumnType) -> Option<Datum> {
    let integer = |n: i64| match target {
        ColumnType::Int2 => i16::try_from(n).ok().map(Datum::Int2),
        ColumnType::Int4 => i32::try_from(n).ok().map(Datum::Int4),
        ColumnType::Int8 => Some(Datum::Int8(n)),
        _ => None,
    };
    match value {
        Datum::Int2(n) => integer(i64::from(*n)),
        Datum::Int4(n) => integer(i64::from(*n)),
        Datum::Int8(n) => integer(*n),
        Datum::Float4(n) => match target {
            ColumnType::Float8 => Some(Datum::Float8(f64::from(*n))),
            _ => Some(value.clone()),
        },
        Datum::Float8(n) => match target {
            // Only a float4-representable value can equal a stored `real`, and
            // the comparison is over the bits because the key it has to match is
            // a byte string, not a numeric comparison.
            ColumnType::Float4 => {
                let narrowed = *n as f32;
                (f64::from(narrowed).to_bits() == n.to_bits()).then_some(Datum::Float4(narrowed))
            }
            _ => Some(value.clone()),
        },
        other => Some(other.clone()),
    }
}

/// Align a whole key, giving up as soon as one column cannot be represented.
fn align_probe_key(values: &[Datum], types: &[ColumnType]) -> Option<Vec<Datum>> {
    values
        .iter()
        .zip(types.iter())
        .map(|(value, &ty)| align_probe_value(value, ty))
        .collect()
}

/// A non-NULL datum's `PostgreSQL` text output, for the `Key (…)=(…)` fragment.
fn datum_text(value: &Datum, ctx: &EvalCtx) -> String {
    if value.is_null() {
        return "null".to_string();
    }
    String::from_utf8_lossy(&crabka_pgtypes::encoding::encode_text(
        value,
        &ctx.time_zone,
    ))
    .into_owned()
}

fn rendered_key(columns: &[String], values: &[Datum], ctx: &EvalCtx) -> String {
    let rendered: Vec<String> = values.iter().map(|value| datum_text(value, ctx)).collect();
    ForeignKeyViolationSide::render_key(columns, &rendered)
}

/// A 23503/23001, naming every relation in it the way `ri_ReportViolation`
/// does: with `RelationGetRelationName`, so both the message and its `DETAIL`
/// carry bare relation names even for a foreign key spanning two schemas.
fn violation(fk: &ForeignKey, table: &RelationName, side: ForeignKeyViolationSide) -> ExecError {
    ExecError::ForeignKeyViolation(Box::new(ForeignKeyViolation {
        table: table.name.clone(),
        constraint: fk.name.clone(),
        side,
    }))
}

// ---------------------------------------------------------------------------
// Per-statement context
// ---------------------------------------------------------------------------

/// One foreign key as one side of it sees the statement's target relation.
#[derive(Debug, Clone)]
pub struct FkSide {
    pub fk: Arc<ForeignKey>,
    /// Ordinals, in the target relation, of the columns this side keys on: the
    /// referencing columns on the child side, the referenced ones on the parent
    /// side. Always in `FOREIGN KEY` clause order.
    pub columns: Vec<usize>,
}

impl FkSide {
    fn key(&self, row: &[Datum], names: &[String]) -> Result<Vec<Datum>, ExecError> {
        key_of(row, &self.columns, names)
    }
}

/// The foreign keys one statement's target relation participates in, resolved
/// once beside the writable local indexes.
///
/// [`StatementFkContext::is_empty`] is the fast path the design turns on: a
/// relation in no foreign key pays one boolean test per write hook and nothing
/// else.
#[derive(Debug, Clone, Default)]
pub struct StatementFkContext {
    /// Foreign keys whose *child* relation is the target: an insert or update
    /// here must find a parent.
    pub child_side: Vec<FkSide>,
    /// Foreign keys whose *parent* relation is the target: a delete, or an
    /// update of the referenced columns, must account for the children.
    pub parent_side: Vec<FkSide>,
}

impl StatementFkContext {
    /// Resolve both sides for `table`.
    ///
    /// # Errors
    ///
    /// Returns a catalog error when the foreign-key records cannot be read, and
    /// 42703 when one names a column the relation no longer has.
    pub fn resolve(catalog_kv: &dyn Kv, table: &Table) -> Result<Self, ExecError> {
        Self::resolve_for_truncate(catalog_kv, table, &BTreeSet::new())
    }

    /// [`StatementFkContext::resolve`], minus the parent-side foreign keys whose
    /// child relation is inside `truncate_set`.
    ///
    /// `TRUNCATE` refuses when a relation outside the set references one inside
    /// it and `TRUNCATE … CASCADE` widens the *set* rather than firing the
    /// actions, so by construction every remaining parent-side key of a
    /// truncated relation is suppressed here — expressed as a set-membership
    /// test rather than a "referential integrity off" mode.
    ///
    /// # Errors
    ///
    /// As [`StatementFkContext::resolve`].
    pub fn resolve_for_truncate(
        catalog_kv: &dyn Kv,
        table: &Table,
        truncate_set: &BTreeSet<TableId>,
    ) -> Result<Self, ExecError> {
        let mut child_side = Vec::new();
        for fk in crabka_pgcatalog::list_table_foreign_keys(catalog_kv, table.id)? {
            let columns = column_ordinals(table, &fk.columns)?;
            child_side.push(FkSide {
                fk: Arc::new(fk),
                columns,
            });
        }
        let mut parent_side = Vec::new();
        for fk in crabka_pgcatalog::list_referencing_foreign_keys(catalog_kv, table.id)? {
            if truncate_set.contains(&fk.table_id) {
                continue;
            }
            let columns = column_ordinals(table, &fk.referenced_columns)?;
            parent_side.push(FkSide {
                fk: Arc::new(fk),
                columns,
            });
        }
        Ok(Self {
            child_side,
            parent_side,
        })
    }

    /// True when the relation participates in no foreign key at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.child_side.is_empty() && self.parent_side.is_empty()
    }
}

// ---------------------------------------------------------------------------
// The queue
// ---------------------------------------------------------------------------

/// One queued referential check.
#[derive(Debug, Clone, PartialEq)]
pub enum PendingCheck {
    /// A referencing row must find its parent.
    Child {
        fk: Arc<ForeignKey>,
        /// The referencing row. `UPDATE` preserves the rowid here, so this is a
        /// stable row identity for the life of the transaction.
        rowid: u64,
        /// The key as the statement wrote it, in clause order — what the
        /// statement drain checks, saving it the row read that would produce the
        /// same values. A check that is *deferred* drops it, so the commit-time
        /// drain re-derives the key from the row's then-current version and "the
        /// row moved" is handled by construction.
        key: Option<Vec<Datum>>,
    },
    /// A referenced key was deleted or moved and the children must be accounted
    /// for. The key is recorded by value, because by drain time the parent row
    /// is gone or re-keyed.
    Parent {
        fk: Arc<ForeignKey>,
        /// The parent row the key came from.
        rowid: u64,
        /// The referenced values as they were, in clause order.
        key: Vec<Datum>,
        /// The referenced values as they now are, for `ON UPDATE`; `None` for a
        /// delete.
        new_key: Option<Vec<Datum>>,
    },
}

impl PendingCheck {
    fn fk(&self) -> &Arc<ForeignKey> {
        match self {
            PendingCheck::Child { fk, .. } | PendingCheck::Parent { fk, .. } => fk,
        }
    }

    /// The referential action a parent-side entry runs under; `None` on the
    /// child side.
    fn action(&self) -> Option<ReferentialAction> {
        match self {
            PendingCheck::Child { .. } => None,
            PendingCheck::Parent { fk, new_key, .. } => Some(if new_key.is_some() {
                fk.on_update
            } else {
                fk.on_delete
            }),
        }
    }

    /// Does this entry stand for a *check*, rather than for a referential
    /// action?
    ///
    /// `PostgreSQL` splits one constraint's triggers in two and gives them
    /// different deferrability. The check triggers — `RI_FKey_check_ins` and
    /// `_upd` on the child side, `RI_FKey_noaction_del` and `_upd` on the parent
    /// — are created with the constraint's own `deferrable` / `initdeferred`.
    /// Every *action* trigger — `RI_FKey_restrict_*`, `cascade_*`, `setnull_*`,
    /// `setdefault_*` — is created `NOT DEFERRABLE` whatever the clause says.
    ///
    /// So an action always runs inside the statement that provoked it: a
    /// `DEFERRABLE INITIALLY DEFERRED ON DELETE CASCADE` has already deleted its
    /// children by the next statement of the block, and only the constraint's
    /// checks wait for `COMMIT`.
    fn is_check(&self) -> bool {
        matches!(self.action(), None | Some(ReferentialAction::NoAction))
    }

    /// Drop the staged key so a deferred entry re-derives it at commit.
    fn deferred(mut self) -> Self {
        if let PendingCheck::Child { key, .. } = &mut self {
            *key = None;
        }
        self
    }
}

/// The checks one statement queued, drained once the statement is complete.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FkCheckQueue {
    entries: VecDeque<PendingCheck>,
}

impl FkCheckQueue {
    /// True when nothing is queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// How many checks are queued.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// The queued checks, in the order they were appended.
    #[must_use]
    pub fn checks(&self) -> &VecDeque<PendingCheck> {
        &self.entries
    }

    fn push(&mut self, check: PendingCheck) {
        self.entries.push_back(check);
    }

    /// Queue the checks an inserted row owes.
    ///
    /// A new row can only break the constraints it references; nothing that
    /// references *it* can be disturbed by its arrival.
    ///
    /// # Errors
    ///
    /// 42703 when a foreign key names a column the row does not carry.
    pub fn after_insert(
        &mut self,
        ctx: &StatementFkContext,
        rowid: u64,
        row: &[Datum],
    ) -> Result<(), ExecError> {
        for side in &ctx.child_side {
            let key = side.key(row, &side.fk.columns)?;
            self.push(PendingCheck::Child {
                fk: Arc::clone(&side.fk),
                rowid,
                key: Some(key),
            });
        }
        Ok(())
    }

    /// Queue the checks an updated row owes on both sides.
    ///
    /// A side whose key values are unchanged queues nothing: `PostgreSQL`'s
    /// referential triggers compare the old and new keys first, and that is what
    /// keeps a non-key update of a hot parent row off the key lock entirely.
    ///
    /// # Errors
    ///
    /// 42703 when a foreign key names a column the row does not carry.
    pub fn after_update(
        &mut self,
        ctx: &StatementFkContext,
        rowid: u64,
        old_row: &[Datum],
        new_row: &[Datum],
    ) -> Result<(), ExecError> {
        for side in &ctx.child_side {
            let old = side.key(old_row, &side.fk.columns)?;
            let new = side.key(new_row, &side.fk.columns)?;
            if old == new {
                continue;
            }
            self.push(PendingCheck::Child {
                fk: Arc::clone(&side.fk),
                rowid,
                key: Some(new),
            });
        }
        for side in &ctx.parent_side {
            let old = side.key(old_row, &side.fk.referenced_columns)?;
            let new = side.key(new_row, &side.fk.referenced_columns)?;
            if old == new {
                continue;
            }
            self.push(PendingCheck::Parent {
                fk: Arc::clone(&side.fk),
                rowid,
                key: old,
                new_key: Some(new),
            });
        }
        Ok(())
    }

    /// Queue the checks a deleted row owes: one per foreign key that references
    /// it.
    ///
    /// # Errors
    ///
    /// 42703 when a foreign key names a column the row does not carry.
    pub fn after_delete(
        &mut self,
        ctx: &StatementFkContext,
        rowid: u64,
        old_row: &[Datum],
    ) -> Result<(), ExecError> {
        for side in &ctx.parent_side {
            let key = side.key(old_row, &side.fk.referenced_columns)?;
            self.push(PendingCheck::Parent {
                fk: Arc::clone(&side.fk),
                rowid,
                key,
                new_key: None,
            });
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Deferral
// ---------------------------------------------------------------------------

/// The per-transaction `SET CONSTRAINTS` overrides.
///
/// Kept apart from the pending entries because a savepoint has to capture and
/// restore exactly this — `SET CONSTRAINTS` is a utility statement and *is*
/// rollback-able, while the pending queue can never need unwinding (rolling back
/// to a savepoint across a row-modifying sub-transaction is already refused, and
/// every statement that queues a check modifies rows).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeferralModes {
    /// `SET CONSTRAINTS ALL { DEFERRED | IMMEDIATE }`, when one has run.
    all: Option<bool>,
    /// Per-constraint overrides, keyed as the catalog keys a constraint:
    /// `(child relation, name)`.
    named: HashMap<(TableId, String), bool>,
}

impl DeferralModes {
    #[must_use]
    pub fn is_trigger_deferred(
        &self,
        table: TableId,
        name: &str,
        deferrable: bool,
        initially_deferred: bool,
    ) -> bool {
        deferrable
            && self
                .named
                .get(&(table, name.to_string()))
                .copied()
                .or(self.all)
                .unwrap_or(initially_deferred)
    }

    /// Apply `SET CONSTRAINTS ALL`. `PostgreSQL` resets every per-constraint
    /// setting with it.
    pub fn set_all(&mut self, deferred: bool) {
        self.all = Some(deferred);
        self.named.clear();
    }

    /// Apply `SET CONSTRAINTS <name>` to one already-resolved constraint.
    pub fn set_one(&mut self, table: TableId, name: &str, deferred: bool) {
        self.named.insert((table, name.to_string()), deferred);
    }

    /// Is this constraint deferred right now?
    ///
    /// A constraint that is not `DEFERRABLE` is never deferred, whatever
    /// `SET CONSTRAINTS` says.
    #[must_use]
    pub fn is_deferred(&self, fk: &ForeignKey) -> bool {
        if !fk.deferrable {
            return false;
        }
        self.named
            .get(&(fk.table_id, fk.name.clone()))
            .copied()
            .or(self.all)
            .unwrap_or(fk.initially_deferred)
    }
}

/// The transaction's deferred checks and deferral modes.
///
/// Modelled on the session's pending `NOTIFY` queue: it accumulates during a
/// transaction, is drained as part of `COMMIT`, and is discarded on rollback
/// through the same teardown that clears the rest of the transaction's state.
#[derive(Debug, Clone, Default)]
pub struct DeferredConstraints {
    modes: DeferralModes,
    /// The entries waiting for `COMMIT`. Every one of them is a *check*:
    /// [`DeferredConstraints::defer`] is the only way in and it is the one place
    /// the rule lives, so a referential action can never be found here.
    pending: Vec<PendingCheck>,
}

impl DeferredConstraints {
    /// True when nothing is deferred.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// The deferral modes, for a savepoint frame to capture.
    #[must_use]
    pub fn modes(&self) -> &DeferralModes {
        &self.modes
    }

    /// The deferral modes, for `SET CONSTRAINTS` to change.
    pub fn modes_mut(&mut self) -> &mut DeferralModes {
        &mut self.modes
    }

    /// Restore the modes a savepoint captured. The pending entries are left
    /// alone: they belong to row-modifying statements, which cannot be rolled
    /// back to a savepoint here.
    pub fn restore_modes(&mut self, modes: DeferralModes) {
        self.modes = modes;
    }

    /// Take an entry that should wait for `COMMIT`, or hand it straight back to
    /// be run now.
    ///
    /// Only a check ever waits — see [`PendingCheck::is_check`] — so a
    /// `DEFERRABLE INITIALLY DEFERRED` constraint carrying `CASCADE`,
    /// `SET NULL`, `SET DEFAULT` or `RESTRICT` still performs it inside the
    /// statement, and `RESTRICT` versus `NO ACTION` is the visible edge of that
    /// same rule rather than a case of its own.
    ///
    /// The rule and the queue are one operation deliberately: it is what makes
    /// "everything pending here is a check" hold by construction, which is in
    /// turn what lets the commit-time drain assume no write of its own is
    /// missing from what it reads.
    ///
    /// A check a referential action produced defers like any other: its row is
    /// only staged now, but every statement's ops reach the KV under this
    /// transaction's xid before the next statement runs, so by `COMMIT` the
    /// re-derivation reads the row the cascade wrote.
    pub fn defer(&mut self, check: PendingCheck) -> Option<PendingCheck> {
        if check.is_check() && self.modes.is_deferred(check.fk()) {
            self.pending.push(check.deferred());
            return None;
        }
        Some(check)
    }

    /// Take every deferred check — the `COMMIT` drain.
    pub fn take_all(&mut self) -> Vec<PendingCheck> {
        std::mem::take(&mut self.pending)
    }

    /// Take the checks that are no longer deferred — what
    /// `SET CONSTRAINTS … IMMEDIATE` drains mid-transaction.
    pub fn take_immediate(&mut self) -> Vec<PendingCheck> {
        let (ready, still_deferred) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|check| !self.modes.is_deferred(check.fk()));
        self.pending = still_deferred;
        ready
    }

    /// Discard everything, for transaction teardown.
    pub fn clear(&mut self) {
        self.pending.clear();
        self.modes = DeferralModes::default();
    }
}

// ---------------------------------------------------------------------------
// Engine seams
// ---------------------------------------------------------------------------

/// Which way a key lock is taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FkLockMode {
    /// The child side: many referencing rows share one parent key and must not
    /// convoy.
    Shared,
    /// The parent side: the key is being removed or moved.
    Exclusive,
}

/// The key lock both sides of a foreign key take.
///
/// The key bytes are `secondary_index_entry_prefix(parent, referenced index,
/// key)` — the same identity the uniqueness check locks, so the implementation
/// is a `LockKey::UniqueKey` acquire in the engine's row-lock manager and no new
/// lock mode exists.
pub trait FkKeyLocks {
    /// Acquire `key`, blocking until granted.
    fn lock_key(
        &self,
        key: Vec<u8>,
        mode: FkLockMode,
    ) -> impl std::future::Future<Output = Result<(), ExecError>> + Send;
}

/// What a referential action does to one referencing row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FkRowChange {
    /// `ON DELETE CASCADE`.
    Delete,
    /// `ON UPDATE CASCADE` and `SET NULL`: assign these `(column ordinal,
    /// value)` pairs. The engine coerces each value to its column's type, as an
    /// ordinary assignment does.
    Assign(Vec<(usize, Datum)>),
    /// `SET DEFAULT`: assign each named column its `DEFAULT`, which only the
    /// engine can evaluate (a `nextval` default has a side effect).
    AssignDefaults(Vec<usize>),
}

/// One row a referential action wants changed.
pub struct FkCascadeRequest<'a> {
    pub table: &'a Table,
    pub rowid: u64,
    pub change: FkRowChange,
    /// The constraint whose action this is, named as the catalog names it
    /// within `table`. The engine's write bookkeeping is keyed by it, so one
    /// constraint's action writes a given row once while another's is free to
    /// write it too.
    pub constraint: &'a str,
}

/// What the engine did with an [`FkCascadeRequest`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FkCascadeOutcome {
    /// The row was changed. `new_row` is the row as written, or `None` for a
    /// delete; the drain needs it to queue the follow-on checks the change
    /// itself owes.
    Applied { new_row: Option<Vec<Datum>> },
    /// Nothing happened, because there is no row left for the action to write:
    /// it is gone — deleted by a concurrent committed transaction, by this
    /// command's own DML, or by an earlier action of this same drain — or *this
    /// constraint's* action has already written it, which bounds one drain's
    /// work at one write per row per constraint.
    ///
    /// It is not a divergence and not a cycle terminator. Cycles terminate on
    /// the data, as `PostgreSQL`'s do: the drain folds each action's ops into
    /// the view it reads, so the row a cycle comes back to no longer matches the
    /// key being chased and no search offers it here at all. Neither a row an
    /// earlier part of the same *command* modified nor one *another*
    /// constraint's action just rewrote is skipped — an action is a command of
    /// its own, applied to that row's current image exactly as `PostgreSQL`
    /// applies it, which is what nulls both referencing columns when one
    /// `DELETE` removes a key two foreign keys point at.
    Skipped,
}

/// The write path a referential action re-enters.
///
/// The implementation shares the *outer statement's* write bookkeeping, claims
/// each row it changes there under [`FkCascadeRequest::constraint`], and folds
/// the ops it produces into what the drain reads before returning them. That
/// fold is what lets every later probe see the action's effect, so the drain's
/// searches and re-probes need no record of what it has written.
pub trait FkCascade {
    /// Fire statement-level hooks around one referential-action query.
    ///
    /// # Errors
    ///
    /// Returns an execution error when a `BEFORE` statement trigger fails.
    fn begin_action(
        &mut self,
        _table: &Table,
        _delete: bool,
        _updated: &[usize],
    ) -> Result<(), ExecError> {
        Ok(())
    }

    /// Fire the matching statement-level hooks after a referential action.
    ///
    /// # Errors
    ///
    /// Returns an execution error when an `AFTER` statement trigger fails.
    fn end_action(
        &mut self,
        _table: &Table,
        _delete: bool,
        _updated: &[usize],
    ) -> Result<(), ExecError> {
        Ok(())
    }

    /// Apply one referential action to one row, returning the ops to add to the
    /// statement's batch — after folding them into the view the drain reads.
    ///
    /// Reports [`FkCascadeOutcome::Skipped`] when there is no row left to write.
    fn modify_row(
        &mut self,
        request: FkCascadeRequest<'_>,
    ) -> impl std::future::Future<Output = Result<(FkCascadeOutcome, Vec<WriteOp>), ExecError>> + Send;
}

/// Everything the drain reads through.
pub struct FkExecContext<'a> {
    pub catalog_kv: &'a dyn Kv,
    /// The row store the drain probes and scans. For the end-of-statement drain
    /// it is an overlay of the statement's pending write batch, whose premise is
    /// that the statement's rows already exist; the commit-time drain reads the
    /// store as it stands. Either way the drain's own referential actions fold
    /// their ops in as they run, so every probe reads the transaction's current
    /// state.
    pub kv: &'a dyn Kv,
    pub global: &'a dyn Kv,
    pub global_snapshot: &'a Snapshot,
    pub snapshot: &'a Snapshot,
    pub xid: u64,
    pub eval_ctx: &'a EvalCtx,
}

/// A snapshot that sees every committed transaction, for the probe under the key
/// lock: the lock serializes check-then-write per key, and the probe then reads
/// the then-current committed state exactly as the uniqueness check does.
fn all_committed() -> Snapshot {
    Snapshot {
        xmin: 0,
        xmax: u64::MAX,
        xip: Vec::new(),
    }
}

impl FkExecContext<'_> {
    /// The visible version of one row, or `None` when nothing is visible.
    ///
    /// `global_snapshot` decides when a cross-range `Prepared` marker resolves,
    /// and travels with `snapshot`: a probe under the key lock reads the
    /// then-current committed state on both axes, a re-read of our own row reads
    /// the statement's.
    fn visible_row(
        &self,
        table: TableId,
        rowid: u64,
        snapshot: &Snapshot,
        global_snapshot: &Snapshot,
    ) -> Result<Option<Vec<Datum>>, ExecError> {
        let status = crate::exec::global_status(self.kv, self.global, global_snapshot);
        let mut best: Option<(u64, Vec<Datum>)> = None;
        for (_, value) in self
            .kv
            .scan_prefix(&crabka_pgkv::key::row_key(table, rowid))?
        {
            let (xmin, xmax, row) = crabka_pgmvcc::version::decode_tuple(&value)?;
            if crabka_pgmvcc::visibility::satisfies_mvcc(
                xmin,
                xmax,
                snapshot,
                Some(self.xid),
                &status,
            )? && best.as_ref().is_none_or(|(current, _)| xmin > *current)
            {
                best = Some((xmin, row));
            }
        }
        Ok(best.map(|(_, row)| row))
    }

    /// Rows currently holding `values` in `index`, as `(rowid, row)`.
    ///
    /// Mirrors the executor's own equality probe: the index entry only names a
    /// rowid, so each candidate is resolved through MVCC and its visible row's
    /// values re-checked, which is what discounts dead entries left by old
    /// versions and aborted writers.
    fn rows_with_key(
        &self,
        table: &Table,
        index: &Index,
        values: &[Datum],
    ) -> Result<Vec<(u64, Vec<Datum>)>, ExecError> {
        let prefix = crabka_pgkv::key::secondary_index_entry_prefix(table.id, index.id, values);
        let ordinals = column_ordinals(table, &index.columns)?;
        let snapshot = all_committed();
        let mut rowids = BTreeSet::new();
        for (key, _) in self.kv.scan_prefix(&prefix)? {
            rowids.insert(crabka_pgkv::key::secondary_index_rowid_of(
                table.id, index.id, &key,
            )?);
        }
        let mut rows = Vec::new();
        for rowid in rowids {
            let Some(row) = self.visible_row(table.id, rowid, &snapshot, &snapshot)? else {
                continue;
            };
            if key_of(&row, &ordinals, &index.columns)? == values {
                rows.push((rowid, row));
            }
        }
        Ok(rows)
    }
}

// ---------------------------------------------------------------------------
// Catalog lookups the drain shares
// ---------------------------------------------------------------------------

/// The relations, indexes and foreign-key contexts one drain touches, read once
/// each. A cascade walks a chain of relations, and every step needs the same
/// handful of records.
struct DrainCatalog<'a> {
    kv: &'a dyn Kv,
    tables: HashMap<RelationName, Arc<Table>>,
    indexes: HashMap<RelationName, Arc<Index>>,
    table_indexes: HashMap<TableId, Arc<Vec<Index>>>,
    contexts: HashMap<TableId, Arc<StatementFkContext>>,
}

impl<'a> DrainCatalog<'a> {
    fn new(kv: &'a dyn Kv) -> Self {
        Self {
            kv,
            tables: HashMap::new(),
            indexes: HashMap::new(),
            table_indexes: HashMap::new(),
            contexts: HashMap::new(),
        }
    }

    fn table(&mut self, name: &RelationName) -> Result<Arc<Table>, ExecError> {
        if let Some(table) = self.tables.get(name) {
            return Ok(Arc::clone(table));
        }
        let table = Arc::new(crabka_pgcatalog::get_table(self.kv, name)?);
        self.tables.insert(name.clone(), Arc::clone(&table));
        Ok(table)
    }

    fn index(&mut self, name: &RelationName) -> Result<Arc<Index>, ExecError> {
        if let Some(index) = self.indexes.get(name) {
            return Ok(Arc::clone(index));
        }
        let index = Arc::new(crabka_pgcatalog::get_index(self.kv, name)?);
        self.indexes.insert(name.clone(), Arc::clone(&index));
        Ok(index)
    }

    fn indexes_of(&mut self, table: &Table) -> Result<Arc<Vec<Index>>, ExecError> {
        if let Some(indexes) = self.table_indexes.get(&table.id) {
            return Ok(Arc::clone(indexes));
        }
        let indexes = Arc::new(crabka_pgcatalog::list_table_indexes(self.kv, &table.name)?);
        self.table_indexes.insert(table.id, Arc::clone(&indexes));
        Ok(indexes)
    }

    fn context(&mut self, table: &Table) -> Result<Arc<StatementFkContext>, ExecError> {
        if let Some(context) = self.contexts.get(&table.id) {
            return Ok(Arc::clone(context));
        }
        let context = Arc::new(StatementFkContext::resolve(self.kv, table)?);
        self.contexts.insert(table.id, Arc::clone(&context));
        Ok(context)
    }
}

/// A foreign key with its two relations and the referenced index resolved.
struct FkParts {
    child: Arc<Table>,
    parent: Arc<Table>,
    referenced_index: Arc<Index>,
    /// Clause order to referenced-index order.
    permutation: Vec<usize>,
    /// The referenced columns' types, in clause order.
    referenced_types: Vec<ColumnType>,
    /// The referencing columns' ordinals in the child relation, clause order.
    child_columns: Vec<usize>,
}

impl FkParts {
    fn resolve(catalog: &mut DrainCatalog<'_>, fk: &ForeignKey) -> Result<Self, ExecError> {
        let child = catalog.table(&fk.table)?;
        let parent = catalog.table(&fk.referenced_table)?;
        // An index lives in the schema of the table it indexes, so the
        // constraint's referenced index is the parent's sibling.
        let referenced_index = catalog.index(&fk.referenced_table.sibling(&fk.referenced_index))?;
        let permutation = key_permutation(&fk.referenced_columns, &referenced_index.columns)
            .ok_or_else(|| ExecError::UndefinedForeignKeyColumn(fk.referenced_index.clone()))?;
        let referenced_types = fk
            .referenced_columns
            .iter()
            .map(|column| {
                parent
                    .column_index(column)
                    .and_then(|ordinal| parent.columns.get(ordinal))
                    .map(|column| column.ty)
                    .ok_or_else(|| ExecError::UndefinedForeignKeyColumn(column.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let child_columns = column_ordinals(&child, &fk.columns)?;
        Ok(Self {
            child,
            parent,
            referenced_index,
            permutation,
            referenced_types,
            child_columns,
        })
    }

    /// The key bytes both sides lock.
    ///
    /// Takes the values already in referenced-index order and already in the
    /// referenced columns' representation — the identity has to be the one the
    /// parent's own uniqueness check locks, so a `bigint` child of an `integer`
    /// parent must not name a different byte string than the parent does.
    fn lock_bytes(&self, index_ordered: &[Datum]) -> Vec<u8> {
        crabka_pgkv::key::secondary_index_entry_prefix(
            self.parent.id,
            self.referenced_index.id,
            index_ordered,
        )
    }

    /// The probe values, in referenced-index order, or `None` when the key
    /// cannot exist in the parent's types at all.
    fn probe_values(&self, key: &[Datum]) -> Option<Vec<Datum>> {
        let aligned = align_probe_key(key, &self.referenced_types)?;
        Some(permute_key(&aligned, &self.permutation))
    }
}

// ---------------------------------------------------------------------------
// The drain
// ---------------------------------------------------------------------------

/// Drain the statement's queue: run every immediate check, move every deferred
/// one to the transaction's queue, and loop until the referential actions stop
/// producing work.
///
/// Runs once for the whole statement — `WITH` list plus body — because
/// `PostgreSQL` treats that as one command and fires its trigger queue once for
/// it.
///
/// # Errors
///
/// The referential-integrity violations (23503 and 23001), plus whatever the
/// catalog, the KV, the lock manager or a cascaded write reports.
pub async fn drain_statement_checks<L, C>(
    ctx: &FkExecContext<'_>,
    locks: &L,
    cascade: &mut C,
    queue: &mut FkCheckQueue,
    deferred: Option<&mut DeferredConstraints>,
) -> Result<Vec<WriteOp>, ExecError>
where
    L: FkKeyLocks + Sync,
    C: FkCascade + Send,
{
    let entries = std::mem::take(&mut queue.entries);
    run_drain(ctx, locks, cascade, entries, deferred).await
}

/// Drain checks that were deferred: the `COMMIT` path, and
/// `SET CONSTRAINTS … IMMEDIATE` mid-transaction.
///
/// A failure here is an ordinary failed commit. The transaction's rows are
/// already in the KV but stay invisible because its commit-log entry is never
/// written, so the existing abort path needs no addition.
///
/// Every entry `checks` carries came out of [`DeferredConstraints`], so every
/// one of them is a check rather than a referential action, and this drain
/// therefore performs no cascade of its own. It reads the store as the
/// transaction has left it, which is what makes a key re-supplied by a later
/// statement satisfy a `NO ACTION` check queued by an earlier one.
///
/// # Errors
///
/// As [`drain_statement_checks`].
pub async fn drain_deferred_checks<L, C>(
    ctx: &FkExecContext<'_>,
    locks: &L,
    cascade: &mut C,
    checks: Vec<PendingCheck>,
) -> Result<Vec<WriteOp>, ExecError>
where
    L: FkKeyLocks + Sync,
    C: FkCascade + Send,
{
    run_drain(ctx, locks, cascade, checks.into(), None).await
}

async fn run_drain<L, C>(
    ctx: &FkExecContext<'_>,
    locks: &L,
    cascade: &mut C,
    mut entries: VecDeque<PendingCheck>,
    mut deferred: Option<&mut DeferredConstraints>,
) -> Result<Vec<WriteOp>, ExecError>
where
    L: FkKeyLocks + Sync,
    C: FkCascade + Send,
{
    let mut catalog = DrainCatalog::new(ctx.catalog_kv);
    let mut ops = Vec::new();
    // Referential actions append, so this is a fixpoint loop, not a walk over a
    // fixed list. It terminates on the data — each action's ops are folded into
    // what is read here, so a row already dealt with no longer matches the key
    // being chased — and the engine's write bookkeeping bounds it regardless, by
    // allowing one constraint's action one write per row.
    while let Some(check) = entries.pop_front() {
        let check = match deferred.as_deref_mut() {
            Some(store) => {
                let Some(check) = store.defer(check) else {
                    continue;
                };
                check
            }
            None => check,
        };
        match check {
            PendingCheck::Child { fk, rowid, key } => {
                run_child_check(ctx, locks, &mut catalog, &fk, rowid, key).await?;
            }
            PendingCheck::Parent {
                fk, key, new_key, ..
            } => {
                let follow_on = run_parent_check(
                    ctx,
                    locks,
                    cascade,
                    &mut catalog,
                    &ParentCheck {
                        fk: &fk,
                        key: &key,
                        new_key: new_key.as_deref(),
                    },
                    &mut ops,
                )
                .await?;
                entries.extend(follow_on);
            }
        }
    }
    Ok(ops)
}

/// Check that one referencing row's key exists in the parent.
///
/// The statement drain checks the key the write staged rather than re-reading
/// the row, because the two agree: through the overlay the row still reads as
/// that write left it — no other part of the command may modify it a second
/// time, and a referential action that has since deleted or re-keyed it is not
/// in what the drain reads anyway — so the read would only return the values
/// already in hand. A deferred check has no staged key and must re-derive one,
/// whole statements having run since it was queued.
async fn run_child_check<L>(
    ctx: &FkExecContext<'_>,
    locks: &L,
    catalog: &mut DrainCatalog<'_>,
    fk: &ForeignKey,
    rowid: u64,
    staged_key: Option<Vec<Datum>>,
) -> Result<(), ExecError>
where
    L: FkKeyLocks + Sync,
{
    let parts = FkParts::resolve(catalog, fk)?;
    let key = match staged_key {
        Some(key) => key,
        None => {
            // A deferred check re-derives the key from the row's then-current
            // version; a row that is gone by now owes nothing.
            let Some(row) =
                ctx.visible_row(parts.child.id, rowid, ctx.snapshot, ctx.global_snapshot)?
            else {
                return Ok(());
            };
            key_of(&row, &parts.child_columns, &fk.columns)?
        }
    };
    match match_outcome(fk.match_type, &key) {
        MatchOutcome::Satisfied => return Ok(()),
        MatchOutcome::MixedNulls => {
            return Err(violation(
                fk,
                &fk.table,
                ForeignKeyViolationSide::MatchFullMixedNulls,
            ));
        }
        MatchOutcome::Probe => {}
    }
    let missing = || {
        violation(
            fk,
            &fk.table,
            ForeignKeyViolationSide::KeyNotPresent {
                key: rendered_key(&fk.columns, &key, ctx.eval_ctx),
                referenced_table: fk.referenced_table.name.clone(),
            },
        )
    };
    // A key with no counterpart in the parent's types cannot exist there, and
    // no concurrent writer can create it, so there is nothing to serialize
    // against and the lock is skipped.
    let Some(probe) = parts.probe_values(&key) else {
        return Err(missing());
    };
    locks
        .lock_key(parts.lock_bytes(&probe), FkLockMode::Shared)
        .await?;
    if ctx
        .rows_with_key(&parts.parent, &parts.referenced_index, &probe)?
        .is_empty()
    {
        return Err(missing());
    }
    Ok(())
}

/// One parent-side check's inputs.
struct ParentCheck<'a> {
    fk: &'a ForeignKey,
    key: &'a [Datum],
    new_key: Option<&'a [Datum]>,
}

/// Account for the children of a referenced key that was deleted or moved,
/// returning the checks the referential action itself owes.
async fn run_parent_check<L, C>(
    ctx: &FkExecContext<'_>,
    locks: &L,
    cascade: &mut C,
    catalog: &mut DrainCatalog<'_>,
    check: &ParentCheck<'_>,
    ops: &mut Vec<WriteOp>,
) -> Result<Vec<PendingCheck>, ExecError>
where
    L: FkKeyLocks + Sync,
    C: FkCascade + Send,
{
    let fk = check.fk;
    // A key with a NULL in it references nothing and is referenced by nothing:
    // a referencing row must have every key column non-null to match.
    if check.key.iter().any(Datum::is_null) {
        return Ok(Vec::new());
    }
    let parts = FkParts::resolve(catalog, fk)?;
    let action = if check.new_key.is_some() {
        fk.on_update
    } else {
        fk.on_delete
    };
    // The key came off the parent's own columns, so alignment is a no-op here
    // and only the permutation matters.
    let probe = parts
        .probe_values(check.key)
        .unwrap_or_else(|| permute_key(check.key, &parts.permutation));
    locks
        .lock_key(parts.lock_bytes(&probe), FkLockMode::Exclusive)
        .await?;

    // `ri_Check_Pk_Match`: under NO ACTION a live parent row supplying the same
    // key again makes the check moot. RESTRICT skips this step, which is what
    // makes the two differ once a constraint is deferred.
    if action != ReferentialAction::Restrict {
        // Every holder here is a genuine re-supply, whoever supplied it — the
        // row the statement that queued this check re-inserted under the key it
        // deleted, or, once the check has waited for `COMMIT`, the row a whole
        // later statement supplied. A row *this drain's* own action deleted or
        // re-keyed is not among them, because the action folded its ops into
        // this view before returning.
        if !ctx
            .rows_with_key(&parts.parent, &parts.referenced_index, &probe)?
            .is_empty()
        {
            return Ok(Vec::new());
        }
    }

    let referencing = find_referencing_rows(ctx, catalog, &parts, fk, check.key)?;
    if referencing.is_empty() {
        return Ok(Vec::new());
    }
    // The parent side names the REFERENCED columns: `check.key` holds the
    // parent's own key values, in `referenced_columns` clause order. (The
    // child-side `KeyNotPresent` renderings name `fk.columns`, because there the
    // values came off the referencing row.)
    let rendered = rendered_key(&fk.referenced_columns, check.key, ctx.eval_ctx);
    match action {
        ReferentialAction::NoAction => Err(violation(
            fk,
            &fk.referenced_table,
            ForeignKeyViolationSide::StillReferenced {
                key: rendered,
                referencing_table: fk.table.name.clone(),
            },
        )),
        ReferentialAction::Restrict => Err(violation(
            fk,
            &fk.referenced_table,
            ForeignKeyViolationSide::Restricted {
                key: rendered,
                referencing_table: fk.table.name.clone(),
            },
        )),
        ReferentialAction::Cascade | ReferentialAction::SetNull | ReferentialAction::SetDefault => {
            apply_referential_action(
                cascade,
                catalog,
                &Action {
                    parts: &parts,
                    fk,
                    kind: action,
                    new_key: check.new_key,
                    referencing,
                },
                ops,
            )
            .await
        }
    }
}

/// One referential action, ready to run over the rows it applies to.
struct Action<'a> {
    parts: &'a FkParts,
    fk: &'a ForeignKey,
    kind: ReferentialAction,
    new_key: Option<&'a [Datum]>,
    referencing: Vec<(u64, Vec<Datum>)>,
}

/// The rows of the child relation that reference `key`.
///
/// Prefers an index whose column list is a permutation of the foreign key's,
/// falling back to a scan. No index is created for a foreign key —
/// `PostgreSQL` does not create one either, and a synthetic one would show up in
/// `\d` and `pg_indexes`. A leading-prefix index does not help, because the
/// secondary-index key encoding length-prefixes the whole tuple.
fn find_referencing_rows(
    ctx: &FkExecContext<'_>,
    catalog: &mut DrainCatalog<'_>,
    parts: &FkParts,
    fk: &ForeignKey,
    key: &[Datum],
) -> Result<Vec<(u64, Vec<Datum>)>, ExecError> {
    let child_types: Vec<ColumnType> = parts
        .child_columns
        .iter()
        .filter_map(|&ordinal| parts.child.columns.get(ordinal).map(|column| column.ty))
        .collect();
    let Some(wanted) = align_probe_key(key, &child_types) else {
        return Ok(Vec::new());
    };
    let indexes = catalog.indexes_of(&parts.child)?;
    let usable = indexes.iter().find(|index| {
        key_permutation(&fk.columns, &index.columns).is_some()
            && index.placement == crabka_pgcatalog::IndexPlacement::Local
    });
    let candidates = match usable {
        Some(index) => {
            let permutation = key_permutation(&fk.columns, &index.columns).unwrap_or_default();
            ctx.rows_with_key(&parts.child, index, &permute_key(&wanted, &permutation))?
        }
        None => crate::exec::scan_live(
            ctx.kv,
            ctx.global,
            ctx.global_snapshot,
            &all_committed(),
            Some(ctx.xid),
            &parts.child,
        )?
        .into_iter()
        .map(|(rowid, _, row)| (rowid, row))
        .collect(),
    };
    let mut rows = Vec::new();
    for (rowid, row) in candidates {
        // Whether the row still references the key is the whole test, and it is
        // asked of the row's current image: an earlier part of the *command*
        // and an earlier *action* of this same drain both read as they left it,
        // so `PostgreSQL` and this agree on counting a row whose key still
        // matches and passing over one whose key no longer does.
        if key_of(&row, &parts.child_columns, &fk.columns)? == wanted {
            rows.push((rowid, row));
        }
    }
    Ok(rows)
}

/// Run `CASCADE` / `SET NULL` / `SET DEFAULT` over the referencing rows,
/// returning the checks the changes themselves owe.
async fn apply_referential_action<C>(
    cascade: &mut C,
    catalog: &mut DrainCatalog<'_>,
    request: &Action<'_>,
    ops: &mut Vec<WriteOp>,
) -> Result<Vec<PendingCheck>, ExecError>
where
    C: FkCascade + Send,
{
    let Action {
        parts,
        fk,
        kind,
        new_key,
        referencing,
    } = request;
    // `ON DELETE SET NULL (a, b)` writes only the listed columns; an empty list
    // means every referencing column.
    let written: Vec<usize> = if fk.set_columns.is_empty() {
        parts.child_columns.clone()
    } else {
        column_ordinals(&parts.child, &fk.set_columns)?
    };
    let child_context = catalog.context(&parts.child)?;
    let mut follow_on = Vec::new();
    let deletes = matches!((*kind, *new_key), (ReferentialAction::Cascade, None));
    let updated = if deletes { &[][..] } else { &written };
    cascade.begin_action(&parts.child, deletes, updated)?;
    for (rowid, old_row) in referencing.iter().cloned() {
        let change = match (*kind, *new_key) {
            (ReferentialAction::Cascade, None) => FkRowChange::Delete,
            (ReferentialAction::Cascade, Some(new_key)) => FkRowChange::Assign(
                parts
                    .child_columns
                    .iter()
                    .copied()
                    .zip(new_key.iter().cloned())
                    .collect(),
            ),
            (ReferentialAction::SetNull, _) => FkRowChange::Assign(
                written
                    .iter()
                    .map(|&ordinal| (ordinal, Datum::Null))
                    .collect(),
            ),
            (ReferentialAction::SetDefault, _) => FkRowChange::AssignDefaults(written.clone()),
            (ReferentialAction::NoAction | ReferentialAction::Restrict, _) => continue,
        };
        let (outcome, row_ops) = cascade
            .modify_row(FkCascadeRequest {
                table: &parts.child,
                rowid,
                change,
                constraint: &fk.name,
            })
            .await?;
        ops.extend(row_ops);
        let FkCascadeOutcome::Applied { new_row } = outcome else {
            continue;
        };
        match new_row {
            // A cascaded delete may itself orphan grandchildren.
            None => {
                for side in &child_context.parent_side {
                    follow_on.push(PendingCheck::Parent {
                        fk: Arc::clone(&side.fk),
                        rowid,
                        key: side.key(&old_row, &side.fk.referenced_columns)?,
                        new_key: None,
                    });
                }
            }
            // A cascaded update owes the checks any update owes — including this
            // constraint, which `SET DEFAULT` can genuinely fail.
            Some(new_row) => {
                for side in &child_context.child_side {
                    let old = side.key(&old_row, &side.fk.columns)?;
                    let new = side.key(&new_row, &side.fk.columns)?;
                    if old == new {
                        continue;
                    }
                    follow_on.push(PendingCheck::Child {
                        fk: Arc::clone(&side.fk),
                        rowid,
                        key: Some(new),
                    });
                }
                for side in &child_context.parent_side {
                    let old = side.key(&old_row, &side.fk.referenced_columns)?;
                    let new = side.key(&new_row, &side.fk.referenced_columns)?;
                    if old == new {
                        continue;
                    }
                    follow_on.push(PendingCheck::Parent {
                        fk: Arc::clone(&side.fk),
                        rowid,
                        key: old,
                        new_key: Some(new),
                    });
                }
            }
        }
    }
    cascade.end_action(&parts.child, deletes, updated)?;
    Ok(follow_on)
}

// ---------------------------------------------------------------------------
// Back-validation
// ---------------------------------------------------------------------------

/// Check stored rows against a foreign key: the scan
/// `ALTER TABLE … ADD CONSTRAINT` runs, that `NOT VALID` skips and
/// `VALIDATE CONSTRAINT` runs later.
///
/// `PostgreSQL` reuses the row-write message verbatim here, naming the first
/// offending key, so this raises the same 23503 as an insert would.
///
/// `rows` is supplied by the caller rather than scanned here, because a
/// multi-subcommand `ALTER TABLE` must validate the rows as its earlier
/// subcommands rewrote them, not as storage still holds them.
///
/// No key lock is taken: the DDL that runs this already serializes against
/// concurrent writers through the catalog lock and the table write gate, so
/// per-key serialization would buy nothing and would hold one lock per stored
/// row for the rest of the transaction.
///
/// # Errors
///
/// 23503 for the first row whose key is absent from the parent, plus catalog and
/// KV errors.
pub fn validate_foreign_key_rows(
    ctx: &FkExecContext<'_>,
    fk: &ForeignKey,
    rows: &[Vec<Datum>],
) -> Result<(), ExecError> {
    let mut catalog = DrainCatalog::new(ctx.catalog_kv);
    let parts = FkParts::resolve(&mut catalog, fk)?;
    for row in rows {
        let key = key_of(row, &parts.child_columns, &fk.columns)?;
        match match_outcome(fk.match_type, &key) {
            MatchOutcome::Satisfied => continue,
            MatchOutcome::MixedNulls => {
                return Err(violation(
                    fk,
                    &fk.table,
                    ForeignKeyViolationSide::MatchFullMixedNulls,
                ));
            }
            MatchOutcome::Probe => {}
        }
        let present = match parts.probe_values(&key) {
            Some(probe) => !ctx
                .rows_with_key(&parts.parent, &parts.referenced_index, &probe)?
                .is_empty(),
            None => false,
        };
        if !present {
            return Err(violation(
                fk,
                &fk.table,
                ForeignKeyViolationSide::KeyNotPresent {
                    key: rendered_key(&fk.columns, &key, ctx.eval_ctx),
                    referenced_table: fk.referenced_table.name.clone(),
                },
            ));
        }
    }
    Ok(())
}

/// [`validate_foreign_key_rows`] over the relation's stored rows.
///
/// # Errors
///
/// As [`validate_foreign_key_rows`].
pub fn validate_foreign_key(ctx: &FkExecContext<'_>, fk: &ForeignKey) -> Result<(), ExecError> {
    let child = crabka_pgcatalog::get_table(ctx.catalog_kv, &fk.table)?;
    let rows: Vec<Vec<Datum>> = crate::exec::scan_live(
        ctx.kv,
        ctx.global,
        ctx.global_snapshot,
        ctx.snapshot,
        Some(ctx.xid),
        &child,
    )?
    .into_iter()
    .map(|(_, _, row)| row)
    .collect();
    validate_foreign_key_rows(ctx, fk, &rows)
}

// ---------------------------------------------------------------------------
// TRUNCATE and DROP
// ---------------------------------------------------------------------------

/// The relations one `TRUNCATE` will empty.
#[derive(Debug, Clone, Default)]
pub struct TruncateSet {
    /// Every relation in the set, the named ones first, then the ones `CASCADE`
    /// pulled in.
    pub tables: Vec<Table>,
    /// The relations `CASCADE` added, in the order they were added — one
    /// `truncate cascades to table "…"` NOTICE each.
    pub cascaded: Vec<RelationName>,
}

impl TruncateSet {
    /// The ids in the set, for [`StatementFkContext::resolve_for_truncate`].
    #[must_use]
    pub fn ids(&self) -> BTreeSet<TableId> {
        self.tables.iter().map(|table| table.id).collect()
    }
}

/// Expand a `TRUNCATE` list to the relations it must empty, or refuse.
///
/// `PostgreSQL` does not fire `ON DELETE CASCADE` for `TRUNCATE`: it refuses
/// when a relation outside the set references one inside it, and
/// `TRUNCATE … CASCADE` widens the *set* instead. Expansion is transitive.
///
/// # Errors
///
/// `0A000` naming both relations when a referencing relation is outside the set
/// and `CASCADE` was not given.
pub fn expand_truncate_set(
    catalog_kv: &dyn Kv,
    named: &[Table],
    cascade: bool,
) -> Result<TruncateSet, ExecError> {
    let mut set = TruncateSet {
        tables: named.to_vec(),
        cascaded: Vec::new(),
    };
    let mut ids: BTreeSet<TableId> = set.ids();
    let mut frontier: Vec<Table> = named.to_vec();
    while let Some(table) = frontier.pop() {
        for fk in crabka_pgcatalog::list_referencing_foreign_keys(catalog_kv, table.id)? {
            if ids.contains(&fk.table_id) {
                continue;
            }
            if !cascade {
                // `heap_truncate_check_FKs` names both relations with
                // `RelationGetRelationName`, so neither carries its schema.
                return Err(ExecError::TruncateReferencedByForeignKey {
                    referencing_table: fk.table.name.clone(),
                    referenced_table: table.name.name.clone(),
                });
            }
            let child = crabka_pgcatalog::get_table(catalog_kv, &fk.table)?;
            ids.insert(child.id);
            set.cascaded.push(child.name.clone());
            frontier.push(child.clone());
            set.tables.push(child);
        }
    }
    Ok(set)
}

/// The foreign keys that block dropping `table`, one `DETAIL` line each.
///
/// A constraint the relation owns itself never blocks its own drop — it goes
/// away with the relation — so only keys defined elsewhere count.
///
/// # Errors
///
/// Catalog and KV errors.
pub fn dependents_blocking_table_drop(
    catalog_kv: &dyn Kv,
    table: &Table,
) -> Result<Vec<DependentForeignKey>, ExecError> {
    Ok(
        crabka_pgcatalog::list_referencing_foreign_keys(catalog_kv, table.id)?
            .into_iter()
            .filter(|fk| fk.table_id != table.id)
            .map(|fk| DependentForeignKey {
                constraint: fk.name,
                table: fk.table,
            })
            .collect(),
    )
}

/// The foreign keys that block dropping `index` — the ones that chose it as the
/// index proving their referenced columns unique.
///
/// # Errors
///
/// Catalog and KV errors.
pub fn dependents_blocking_index_drop(
    catalog_kv: &dyn Kv,
    index: &Index,
) -> Result<Vec<DependentForeignKey>, ExecError> {
    Ok(
        crabka_pgcatalog::list_referencing_foreign_keys(catalog_kv, index.table_id)?
            .into_iter()
            .filter(|fk| fk.referenced_index_id == index.id)
            .map(|fk| DependentForeignKey {
                constraint: fk.name,
                table: fk.table,
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::{IndexMethod, IndexPlacement, NewIndex};
    use crabka_pgkv::MemKv;
    use crabka_pgparser::ast::{MatchType as AstMatchType, ReferentialAction as AstAction};
    use crabka_pgwire::error::PgError;

    use super::*;

    fn columns(names: &[(&str, ColumnType)]) -> Vec<Column> {
        names
            .iter()
            .map(|(name, ty)| Column::new(*name, *ty))
            .collect()
    }

    fn new_index(name: &str, cols: &[&str], constraint: Option<IndexConstraint>) -> NewIndex {
        NewIndex {
            name: name.to_string(),
            columns: cols.iter().map(|c| (*c).to_string()).collect(),
            unique: true,
            method: IndexMethod::Btree,
            placement: IndexPlacement::Local,
            constraint,
        }
    }

    /// The two relations every resolution test names, kept alive for the
    /// duration of the run because [`FkRelation`] borrows its name.
    static PARENT: std::sync::LazyLock<RelationName> =
        std::sync::LazyLock::new(|| RelationName::public("pperm"));
    static CHILD: std::sync::LazyLock<RelationName> =
        std::sync::LazyLock::new(|| RelationName::public("cperm"));

    fn scope() -> &'static crate::relname::ResolutionScope {
        crate::relname::ResolutionScope::default_scope()
    }

    /// A catalog with one parent relation keyed `(x, y)` and no children.
    fn catalog_with_parent(indexes: &[NewIndex]) -> (MemKv, Table) {
        let kv = MemKv::default();
        crabka_pgcatalog::create_table(
            &kv,
            &PARENT,
            columns(&[
                ("x", ColumnType::Int4),
                ("y", ColumnType::Int4),
                ("z", ColumnType::Text),
            ]),
        )
        .expect("create parent");
        let parent = crabka_pgcatalog::get_table(&kv, &PARENT).expect("parent");
        let ops = crabka_pgcatalog::create_indexes_on_table_ops(&kv, &parent, indexes)
            .expect("index ops");
        kv.write_batch(&ops).expect("write indexes");
        (kv, parent)
    }

    fn reference(table: &str, cols: &[&str]) -> ForeignKeyRef {
        ForeignKeyRef {
            table: crabka_pgparser::ast::RelationRef::bare(table),
            columns: cols.iter().map(|c| (*c).to_string()).collect(),
            match_type: AstMatchType::Simple,
            on_delete: AstAction::NoAction,
            on_update: AstAction::NoAction,
            set_columns: Vec::new(),
        }
    }

    fn child_relation<'a>(columns: &'a [Column], indexes: &'a [Index]) -> FkRelation<'a> {
        FkRelation {
            id: 7,
            name: &CHILD,
            columns,
            indexes,
            sharded: false,
        }
    }

    fn request<'a>(columns: &'a [String], reference: &'a ForeignKeyRef) -> ForeignKeyRequest<'a> {
        ForeignKeyRequest {
            id: 1,
            name: None,
            columns,
            reference,
            attributes: ConstraintAttributes::default(),
            validated: true,
            self_reference: None,
        }
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| (*name).to_string()).collect()
    }

    fn sqlstate(error: ExecError) -> String {
        let rendered: PgError = error.into_pg();
        rendered.code
    }

    // -- the permutation ----------------------------------------------------

    #[test]
    fn a_composite_key_written_out_of_index_order_permutes_into_it() {
        // FOREIGN KEY (b, a) REFERENCES pperm (y, x) over a (x, y) index.
        let permutation = key_permutation(&names(&["y", "x"]), &names(&["x", "y"]))
            .expect("the two lists are permutations");
        assert!(permutation == vec![1, 0]);
        // Clause-ordered values (y = 1, x = 2) become index-ordered (x, y).
        let permuted = permute_key(&[Datum::Int4(1), Datum::Int4(2)], &permutation);
        assert!(permuted == vec![Datum::Int4(2), Datum::Int4(1)]);
    }

    #[test]
    fn an_in_order_key_permutes_to_itself() {
        let permutation = key_permutation(&names(&["x", "y"]), &names(&["x", "y"])).expect("same");
        assert!(permutation == vec![0, 1]);
        let values = vec![Datum::Int4(3), Datum::Int4(4)];
        assert!(permute_key(&values, &permutation) == values);
    }

    #[test]
    fn a_permuted_key_probes_the_bytes_the_index_stores() {
        // The regression the encoding makes possible: the index entry is built
        // from the parent row in INDEX order, so a clause-ordered probe reads a
        // different byte string entirely.
        let index_order =
            crabka_pgkv::key::secondary_index_entry_prefix(1, 2, &[Datum::Int4(2), Datum::Int4(1)]);
        let clause_order =
            crabka_pgkv::key::secondary_index_entry_prefix(1, 2, &[Datum::Int4(1), Datum::Int4(2)]);
        assert!(index_order != clause_order);
        let permutation = key_permutation(&names(&["y", "x"]), &names(&["x", "y"])).expect("perm");
        let probed = crabka_pgkv::key::secondary_index_entry_prefix(
            1,
            2,
            &permute_key(&[Datum::Int4(1), Datum::Int4(2)], &permutation),
        );
        assert!(probed == index_order);
    }

    #[test]
    fn lists_that_are_not_permutations_have_no_mapping() {
        assert!(key_permutation(&names(&["a"]), &names(&["a", "b"])).is_none());
        assert!(key_permutation(&names(&["a", "b"]), &names(&["a", "c"])).is_none());
    }

    // -- MATCH --------------------------------------------------------------

    #[test]
    fn match_semantics_follow_the_oracle() {
        let cases = [
            (MatchType::Simple, vec![Datum::Int4(1)], MatchOutcome::Probe),
            (
                MatchType::Simple,
                vec![Datum::Null],
                MatchOutcome::Satisfied,
            ),
            (
                MatchType::Simple,
                vec![Datum::Int4(1), Datum::Null],
                MatchOutcome::Satisfied,
            ),
            (
                MatchType::Full,
                vec![Datum::Int4(1), Datum::Int4(2)],
                MatchOutcome::Probe,
            ),
            (
                MatchType::Full,
                vec![Datum::Null, Datum::Null],
                MatchOutcome::Satisfied,
            ),
            (
                MatchType::Full,
                vec![Datum::Int4(1), Datum::Null],
                MatchOutcome::MixedNulls,
            ),
        ];
        for (match_type, key, expected) in cases {
            assert!(
                match_outcome(match_type, &key) == expected,
                "{match_type:?} {key:?}"
            );
        }
    }

    // -- index selection ----------------------------------------------------

    #[test]
    fn the_primary_key_wins_over_every_other_matching_index() {
        let (kv, parent) = catalog_with_parent(&[
            new_index("pperm_pkey", &["x", "y"], Some(IndexConstraint::PrimaryKey)),
            new_index("aaa_uq", &["y", "x"], Some(IndexConstraint::Unique)),
            new_index("bare_uq", &["x", "y"], None),
        ]);
        let indexes = crabka_pgcatalog::list_table_indexes(&kv, &PARENT).expect("indexes");
        let relation = FkRelation::of(&parent, &indexes);
        let chosen = select_referenced_index(&relation, &names(&["x", "y"])).expect("chosen");
        assert!(chosen.name == "pperm_pkey");
    }

    #[test]
    fn a_unique_constraint_wins_over_a_bare_unique_index() {
        let (kv, parent) = catalog_with_parent(&[
            new_index("aaa_bare", &["x"], None),
            new_index("zzz_uq", &["x"], Some(IndexConstraint::Unique)),
        ]);
        let indexes = crabka_pgcatalog::list_table_indexes(&kv, &PARENT).expect("indexes");
        let relation = FkRelation::of(&parent, &indexes);
        let chosen = select_referenced_index(&relation, &names(&["x"])).expect("chosen");
        assert!(chosen.name == "zzz_uq");
    }

    #[test]
    fn the_lowest_named_matching_index_breaks_a_tie() {
        let (kv, parent) = catalog_with_parent(&[
            new_index("b_uq", &["x"], Some(IndexConstraint::Unique)),
            new_index("a_uq", &["x"], Some(IndexConstraint::Unique)),
        ]);
        let indexes = crabka_pgcatalog::list_table_indexes(&kv, &PARENT).expect("indexes");
        let relation = FkRelation::of(&parent, &indexes);
        let chosen = select_referenced_index(&relation, &names(&["x"])).expect("chosen");
        assert!(chosen.name == "a_uq");
    }

    #[test]
    fn an_index_is_matched_by_column_set_not_order() {
        let (kv, parent) = catalog_with_parent(&[new_index(
            "pperm_pkey",
            &["x", "y"],
            Some(IndexConstraint::PrimaryKey),
        )]);
        let indexes = crabka_pgcatalog::list_table_indexes(&kv, &PARENT).expect("indexes");
        let relation = FkRelation::of(&parent, &indexes);
        let chosen = select_referenced_index(&relation, &names(&["y", "x"])).expect("chosen");
        assert!(chosen.name == "pperm_pkey");
    }

    // -- DDL resolution -----------------------------------------------------

    #[test]
    fn a_composite_key_records_both_lists_as_written() {
        let (kv, parent) = catalog_with_parent(&[new_index(
            "pperm_pkey",
            &["x", "y"],
            Some(IndexConstraint::PrimaryKey),
        )]);
        let child_columns = columns(&[("a", ColumnType::Int4), ("b", ColumnType::Int4)]);
        let child = child_relation(&child_columns, &[]);
        let reference = reference("pperm", &["y", "x"]);
        let clause = names(&["b", "a"]);
        let resolved = resolve_foreign_key(&kv, scope(), &child, &request(&clause, &reference))
            .expect("resolve");
        let index =
            crabka_pgcatalog::get_index(&kv, &RelationName::public("pperm_pkey")).expect("index");
        assert!(
            resolved
                == ForeignKey {
                    id: 1,
                    name: "cperm_b_a_fkey".into(),
                    table: RelationName::public("cperm"),
                    table_id: 7,
                    columns: names(&["b", "a"]),
                    referenced_table: RelationName::public("pperm"),
                    referenced_table_id: parent.id,
                    referenced_columns: names(&["y", "x"]),
                    referenced_index_id: index.id,
                    referenced_index: "pperm_pkey".into(),
                    match_type: MatchType::Simple,
                    on_delete: ReferentialAction::NoAction,
                    on_update: ReferentialAction::NoAction,
                    set_columns: Vec::new(),
                    deferrable: false,
                    initially_deferred: false,
                    validated: true,
                }
        );
    }

    #[test]
    fn an_omitted_referenced_list_resolves_to_the_primary_key_in_index_order() {
        let (kv, _) = catalog_with_parent(&[new_index(
            "pperm_pkey",
            &["x", "y"],
            Some(IndexConstraint::PrimaryKey),
        )]);
        let child_columns = columns(&[("a", ColumnType::Int4), ("b", ColumnType::Int4)]);
        let child = child_relation(&child_columns, &[]);
        let reference = reference("pperm", &[]);
        let clause = names(&["a", "b"]);
        let resolved = resolve_foreign_key(&kv, scope(), &child, &request(&clause, &reference))
            .expect("resolve");
        assert!(resolved.referenced_columns == names(&["x", "y"]));
        assert!(resolved.referenced_index == "pperm_pkey");
    }

    #[test]
    fn a_self_reference_resolves_against_the_relation_being_created() {
        let kv = MemKv::default();
        let self_name = RelationName::public("selfref");
        let in_flight_columns = columns(&[("id", ColumnType::Int4), ("boss", ColumnType::Int4)]);
        let in_flight_indexes = vec![Index {
            id: 42,
            name: "selfref_pkey".into(),
            table: self_name.clone(),
            table_id: 9,
            columns: names(&["id"]),
            unique: true,
            method: IndexMethod::Btree,
            placement: IndexPlacement::Local,
            constraint: Some(IndexConstraint::PrimaryKey),
        }];
        let relation = FkRelation {
            id: 9,
            name: &self_name,
            columns: &in_flight_columns,
            indexes: &in_flight_indexes,
            sharded: false,
        };
        let reference = reference("selfref", &["id"]);
        let clause = names(&["boss"]);
        let mut request = request(&clause, &reference);
        request.self_reference = Some(&relation);
        let resolved = resolve_foreign_key(&kv, scope(), &relation, &request).expect("resolve");
        assert!(resolved.referenced_table_id == 9);
        assert!(resolved.referenced_index_id == 42);
        assert!(resolved.name == "selfref_boss_fkey");
    }

    #[test]
    fn without_the_in_flight_relation_a_self_reference_is_an_undefined_relation() {
        let kv = MemKv::default();
        let self_name = RelationName::public("selfref");
        let in_flight_columns = columns(&[("id", ColumnType::Int4), ("boss", ColumnType::Int4)]);
        let relation = FkRelation {
            id: 9,
            name: &self_name,
            columns: &in_flight_columns,
            indexes: &[],
            sharded: false,
        };
        let reference = reference("selfref", &["id"]);
        let clause = names(&["boss"]);
        let error = resolve_foreign_key(&kv, scope(), &relation, &request(&clause, &reference))
            .expect_err("no catalog relation");
        assert!(sqlstate(error) == "42P01");
    }

    #[test]
    fn ddl_validation_reports_the_oracle_sqlstates() {
        let (kv, _) = catalog_with_parent(&[
            new_index("pperm_pkey", &["x", "y"], Some(IndexConstraint::PrimaryKey)),
            new_index("pperm_z_key", &["z"], Some(IndexConstraint::Unique)),
        ]);
        crabka_pgcatalog::create_view(
            &kv,
            &RelationName::public("aview"),
            "SELECT 1".into(),
            columns(&[("one", ColumnType::Int4)]),
        )
        .expect("create view");
        let child_columns = columns(&[
            ("a", ColumnType::Int4),
            ("b", ColumnType::Int4),
            ("t", ColumnType::Text),
        ]);
        let child = child_relation(&child_columns, &[]);

        let cases: Vec<(&str, Vec<String>, ForeignKeyRef, &str)> = vec![
            (
                "unknown referencing column",
                names(&["nope"]),
                reference("pperm", &["x"]),
                "42703",
            ),
            (
                "unknown referenced column",
                names(&["a"]),
                reference("pperm", &["nope"]),
                "42703",
            ),
            (
                "missing relation",
                names(&["a"]),
                reference("nosuch", &["x"]),
                "42P01",
            ),
            (
                "relation is a view",
                names(&["a"]),
                reference("aview", &["one"]),
                "42809",
            ),
            (
                "column count mismatch",
                names(&["a"]),
                reference("pperm", &["x", "y"]),
                "42830",
            ),
            (
                "duplicate referenced column",
                names(&["a", "b"]),
                reference("pperm", &["x", "x"]),
                "42830",
            ),
            (
                "no unique constraint over the referenced columns",
                names(&["a"]),
                reference("pperm", &["x"]),
                "42830",
            ),
            (
                "incomparable types",
                names(&["a"]),
                reference("pperm", &["z"]),
                "42804",
            ),
        ];
        for (label, clause, reference, expected) in cases {
            let error = resolve_foreign_key(&kv, scope(), &child, &request(&clause, &reference))
                .expect_err(label);
            assert!(sqlstate(error) == expected, "{label}");
        }
    }

    #[test]
    fn incomparable_column_types_are_42804_naming_both_sides() {
        let (kv, _) = catalog_with_parent(&[new_index(
            "pperm_z_key",
            &["z"],
            Some(IndexConstraint::Unique),
        )]);
        let child_columns = columns(&[("a", ColumnType::Int4)]);
        let child = child_relation(&child_columns, &[]);
        let reference = reference("pperm", &["z"]);
        let clause = names(&["a"]);
        let error = resolve_foreign_key(&kv, scope(), &child, &request(&clause, &reference))
            .expect_err("mismatch");
        assert!(
            error
                == ExecError::ForeignKeyTypeMismatch(Box::new(ForeignKeyTypeMismatch {
                    constraint: "cperm_a_fkey".into(),
                    referencing_column: "a".into(),
                    referenced_column: "z".into(),
                    referencing_type: "integer".into(),
                    referenced_type: "text".into(),
                }))
        );
    }

    #[test]
    fn an_integer_child_of_a_wider_integer_parent_is_comparable() {
        assert!(types_are_comparable(ColumnType::Int8, ColumnType::Int4));
        assert!(types_are_comparable(
            ColumnType::Varchar(Some(10)),
            ColumnType::Text
        ));
        assert!(!types_are_comparable(
            ColumnType::Int4,
            ColumnType::Numeric(None)
        ));
        assert!(!types_are_comparable(ColumnType::Text, ColumnType::Int4));
    }

    #[test]
    fn a_set_column_outside_the_key_is_42p10() {
        let (kv, _) = catalog_with_parent(&[new_index(
            "pperm_pkey",
            &["x"],
            Some(IndexConstraint::PrimaryKey),
        )]);
        let child_columns = columns(&[("a", ColumnType::Int4), ("b", ColumnType::Int4)]);
        let child = child_relation(&child_columns, &[]);
        let mut reference = reference("pperm", &["x"]);
        reference.on_delete = AstAction::SetNull;
        reference.set_columns = names(&["b"]);
        let clause = names(&["a"]);
        let error = resolve_foreign_key(&kv, scope(), &child, &request(&clause, &reference))
            .expect_err("42P10");
        assert!(error == ExecError::ForeignKeySetColumnNotInKey("b".into()));
    }

    #[test]
    fn the_default_name_joins_every_referencing_column() {
        assert!(
            default_foreign_key_name(&RelationName::public("c12"), &names(&["a", "b"]))
                == "c12_a_b_fkey"
        );
        // A relation outside `public` still contributes only its bare name, the
        // way `ChooseConstraintName` builds the label.
        assert!(
            default_foreign_key_name(&RelationName::new("sch", "cperm"), &names(&["b", "a"]))
                == "cperm_b_a_fkey"
        );
    }

    // -- probe alignment ----------------------------------------------------

    #[test]
    fn probe_values_take_the_referenced_columns_representation() {
        assert!(align_probe_value(&Datum::Int8(1), ColumnType::Int4) == Some(Datum::Int4(1)));
        assert!(align_probe_value(&Datum::Int2(1), ColumnType::Int8) == Some(Datum::Int8(1)));
        // Out of the parent column's range: the key cannot exist there.
        assert!(align_probe_value(&Datum::Int8(1 << 40), ColumnType::Int4).is_none());
        assert!(
            align_probe_value(&Datum::Text("a".into()), ColumnType::Text)
                == Some(Datum::Text("a".into()))
        );
    }

    // -- deferral -----------------------------------------------------------

    fn deferrable_fk(name: &str, initially_deferred: bool) -> ForeignKey {
        ForeignKey {
            id: 1,
            name: name.to_string(),
            table: RelationName::public("c"),
            table_id: 1,
            columns: names(&["a"]),
            referenced_table: RelationName::public("p"),
            referenced_table_id: 2,
            referenced_columns: names(&["id"]),
            referenced_index_id: 3,
            referenced_index: "p_pkey".into(),
            match_type: MatchType::Simple,
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            set_columns: Vec::new(),
            deferrable: true,
            initially_deferred,
            validated: true,
        }
    }

    #[test]
    fn deferral_mode_resolution_follows_the_clause_then_set_constraints() {
        let immediate = deferrable_fk("immediate", false);
        let deferred = deferrable_fk("deferred", true);
        let mut not_deferrable = deferrable_fk("rigid", true);
        not_deferrable.deferrable = false;

        let mut modes = DeferralModes::default();
        assert!(!modes.is_deferred(&immediate));
        assert!(modes.is_deferred(&deferred));
        assert!(!modes.is_deferred(&not_deferrable));

        modes.set_all(true);
        assert!(modes.is_deferred(&immediate));
        assert!(!modes.is_deferred(&not_deferrable));

        modes.set_one(immediate.table_id, &immediate.name, false);
        assert!(!modes.is_deferred(&immediate));
        assert!(modes.is_deferred(&deferred));

        // SET CONSTRAINTS ALL resets the per-constraint settings.
        modes.set_all(false);
        assert!(!modes.is_deferred(&immediate));
        assert!(!modes.is_deferred(&deferred));
    }

    /// The whole of the deferral rule, over a constraint that is
    /// `DEFERRABLE INITIALLY DEFERRED` in every case: a parent-side entry waits
    /// for `COMMIT` only under `NO ACTION`, because that is the only parent-side
    /// trigger `PostgreSQL` creates deferrable, and the child-side check waits
    /// whatever the action is.
    #[test]
    fn only_a_check_ever_defers_never_a_referential_action() {
        struct Case {
            action: ReferentialAction,
            parent_defers: bool,
            why: &'static str,
        }
        let cases = [
            Case {
                action: ReferentialAction::NoAction,
                parent_defers: true,
                why: "RI_FKey_noaction_del inherits the constraint's deferrability",
            },
            Case {
                action: ReferentialAction::Restrict,
                parent_defers: false,
                why: "RI_FKey_restrict_del is created NOT DEFERRABLE",
            },
            Case {
                action: ReferentialAction::Cascade,
                parent_defers: false,
                why: "so is RI_FKey_cascade_del: the cascade runs inside the statement",
            },
            Case {
                action: ReferentialAction::SetNull,
                parent_defers: false,
                why: "and RI_FKey_setnull_del",
            },
            Case {
                action: ReferentialAction::SetDefault,
                parent_defers: false,
                why: "and RI_FKey_setdefault_del",
            },
        ];
        for case in cases {
            let mut fk = deferrable_fk("fk", true);
            fk.on_delete = case.action;
            fk.on_update = case.action;
            let fk = Arc::new(fk);
            let mut store = DeferredConstraints::default();

            // A delete of the parent, then a move of its key: `ON DELETE` and
            // `ON UPDATE` are read from the same clause here, so both answer the
            // same way.
            for new_key in [None, Some(vec![Datum::Int4(2)])] {
                let parent = PendingCheck::Parent {
                    fk: Arc::clone(&fk),
                    rowid: 1,
                    key: vec![Datum::Int4(1)],
                    new_key,
                };
                let handed_back = store.defer(parent.clone());
                let expected = if case.parent_defers {
                    None
                } else {
                    Some(parent)
                };
                assert!(handed_back == expected, "{}", case.why);
            }

            // The child side is a check under every action, so it always waits.
            let child = PendingCheck::Child {
                fk,
                rowid: 1,
                key: Some(vec![Datum::Int4(1)]),
            };
            assert!(store.defer(child).is_none(), "{}", case.why);
        }
    }

    #[test]
    fn a_deferred_child_check_drops_its_staged_key() {
        let mut store = DeferredConstraints::default();
        let deferred = store.defer(PendingCheck::Child {
            fk: Arc::new(deferrable_fk("fk", true)),
            rowid: 5,
            key: Some(vec![Datum::Int4(1)]),
        });
        assert!(deferred.is_none());
        assert!(
            store.take_all()
                == vec![PendingCheck::Child {
                    fk: Arc::new(deferrable_fk("fk", true)),
                    rowid: 5,
                    key: None,
                }]
        );
    }

    #[test]
    fn set_constraints_immediate_takes_only_the_checks_that_became_immediate() {
        let mut store = DeferredConstraints::default();
        let held = deferrable_fk("held", true);
        let released = deferrable_fk("released", true);
        for (fk, rowid) in [(&held, 1), (&released, 2)] {
            let deferred = store.defer(PendingCheck::Child {
                fk: Arc::new(fk.clone()),
                rowid,
                key: None,
            });
            assert!(deferred.is_none());
        }
        store
            .modes_mut()
            .set_one(released.table_id, &released.name, false);
        let ready = store.take_immediate();
        assert!(
            ready
                == vec![PendingCheck::Child {
                    fk: Arc::new(released),
                    rowid: 2,
                    key: None,
                }]
        );
        assert!(!store.is_empty());
        assert!(
            store.take_all()
                == vec![PendingCheck::Child {
                    fk: Arc::new(held),
                    rowid: 1,
                    key: None,
                }]
        );
    }

    // -- the queue ----------------------------------------------------------

    fn statement_context(child: &[ForeignKey], parent: &[ForeignKey]) -> StatementFkContext {
        StatementFkContext {
            child_side: child
                .iter()
                .map(|fk| FkSide {
                    fk: Arc::new(fk.clone()),
                    columns: vec![0],
                })
                .collect(),
            parent_side: parent
                .iter()
                .map(|fk| FkSide {
                    fk: Arc::new(fk.clone()),
                    columns: vec![0],
                })
                .collect(),
        }
    }

    #[test]
    fn an_insert_queues_one_child_check_per_foreign_key_and_nothing_else() {
        let fk = deferrable_fk("fk", false);
        let ctx = statement_context(std::slice::from_ref(&fk), std::slice::from_ref(&fk));
        let mut queue = FkCheckQueue::default();
        queue
            .after_insert(&ctx, 11, &[Datum::Int4(4), Datum::Text("x".into())])
            .expect("queue");
        assert!(
            queue.checks().iter().cloned().collect::<Vec<_>>()
                == vec![PendingCheck::Child {
                    fk: Arc::new(fk),
                    rowid: 11,
                    key: Some(vec![Datum::Int4(4)]),
                }]
        );
    }

    #[test]
    fn an_update_that_leaves_the_key_alone_queues_nothing() {
        let fk = deferrable_fk("fk", false);
        let ctx = statement_context(std::slice::from_ref(&fk), std::slice::from_ref(&fk));
        let mut queue = FkCheckQueue::default();
        queue
            .after_update(
                &ctx,
                11,
                &[Datum::Int4(4), Datum::Text("old".into())],
                &[Datum::Int4(4), Datum::Text("new".into())],
            )
            .expect("queue");
        assert!(queue.is_empty());
    }

    #[test]
    fn an_update_that_moves_the_key_queues_both_sides() {
        let fk = deferrable_fk("fk", false);
        let ctx = statement_context(std::slice::from_ref(&fk), std::slice::from_ref(&fk));
        let mut queue = FkCheckQueue::default();
        queue
            .after_update(&ctx, 11, &[Datum::Int4(4)], &[Datum::Int4(5)])
            .expect("queue");
        assert!(
            queue.checks().iter().cloned().collect::<Vec<_>>()
                == vec![
                    PendingCheck::Child {
                        fk: Arc::new(fk.clone()),
                        rowid: 11,
                        key: Some(vec![Datum::Int4(5)]),
                    },
                    PendingCheck::Parent {
                        fk: Arc::new(fk),
                        rowid: 11,
                        key: vec![Datum::Int4(4)],
                        new_key: Some(vec![Datum::Int4(5)]),
                    },
                ]
        );
    }

    #[test]
    fn a_delete_queues_one_parent_check_per_referencing_key() {
        let fk = deferrable_fk("fk", false);
        let ctx = statement_context(&[], std::slice::from_ref(&fk));
        let mut queue = FkCheckQueue::default();
        queue
            .after_delete(&ctx, 11, &[Datum::Int4(4)])
            .expect("queue");
        assert!(
            queue.checks().iter().cloned().collect::<Vec<_>>()
                == vec![PendingCheck::Parent {
                    fk: Arc::new(fk),
                    rowid: 11,
                    key: vec![Datum::Int4(4)],
                    new_key: None,
                }]
        );
    }

    #[test]
    fn a_relation_in_no_foreign_key_resolves_to_the_empty_context() {
        let kv = MemKv::default();
        let lonely = RelationName::public("lonely");
        crabka_pgcatalog::create_table(&kv, &lonely, columns(&[("a", ColumnType::Int4)]))
            .expect("create");
        let table = crabka_pgcatalog::get_table(&kv, &lonely).expect("table");
        let ctx = StatementFkContext::resolve(&kv, &table).expect("resolve");
        assert!(ctx.is_empty());
    }

    // -- the fixpoint drain -------------------------------------------------

    /// A cascade seam that records what it was asked to do and refuses one
    /// constraint a second change to the same row, exactly as the statement's
    /// write bookkeeping does for an action's claim.
    #[derive(Default)]
    struct FakeCascade {
        claimed: BTreeSet<(TableId, u64, String)>,
        applied: Vec<(TableId, u64, FkRowChange)>,
        /// The row each modification should report as written; `None` deletes.
        result: Option<Vec<Datum>>,
    }

    impl FkCascade for FakeCascade {
        async fn modify_row(
            &mut self,
            request: FkCascadeRequest<'_>,
        ) -> Result<(FkCascadeOutcome, Vec<WriteOp>), ExecError> {
            if !self.claimed.insert((
                request.table.id,
                request.rowid,
                request.constraint.to_string(),
            )) {
                return Ok((FkCascadeOutcome::Skipped, Vec::new()));
            }
            self.applied
                .push((request.table.id, request.rowid, request.change));
            Ok((
                FkCascadeOutcome::Applied {
                    new_row: self.result.clone(),
                },
                Vec::new(),
            ))
        }
    }

    /// The seam's claim contract, read off its own results: a constraint gets
    /// one bite at a row and a second reports `Skipped` changing nothing, while
    /// a *different* constraint reaching the same row is applied — which is what
    /// lets one `DELETE` run both foreign keys' actions over one child row.
    #[test]
    fn one_constraint_writes_a_row_once_and_another_constraint_still_writes_it() {
        let table = Table {
            id: 4,
            name: RelationName::public("c"),
            columns: columns(&[("a", ColumnType::Int4)]),
            sharded: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        };
        let mut cascade = FakeCascade::default();
        let mut modify = |constraint: &'static str| {
            futures_block_on(cascade.modify_row(FkCascadeRequest {
                table: &table,
                rowid: 1,
                change: FkRowChange::Delete,
                constraint,
            }))
            .expect("modify")
            .0
        };
        assert!(modify("k1") == FkCascadeOutcome::Applied { new_row: None });
        assert!(modify("k1") == FkCascadeOutcome::Skipped);
        assert!(modify("k2") == FkCascadeOutcome::Applied { new_row: None });
        assert!(cascade.applied == vec![(4, 1, FkRowChange::Delete), (4, 1, FkRowChange::Delete)]);
    }

    /// Run one future to completion on a current-thread runtime.
    fn futures_block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(future)
    }

    /// A lock seam that records the key bytes and the mode each side asked for.
    #[derive(Default)]
    struct FakeLocks {
        taken: std::sync::Mutex<Vec<(Vec<u8>, FkLockMode)>>,
    }

    impl FkKeyLocks for FakeLocks {
        async fn lock_key(&self, key: Vec<u8>, mode: FkLockMode) -> Result<(), ExecError> {
            self.taken.lock().expect("locks").push((key, mode));
            Ok(())
        }
    }

    #[test]
    fn a_deferred_statement_drain_moves_entries_instead_of_running_them() {
        let kv = MemKv::default();
        let eval_ctx = EvalCtx::test_default();
        let global_snapshot = all_committed();
        let snapshot = all_committed();
        let ctx = FkExecContext {
            catalog_kv: &kv,
            kv: &kv,
            global: &kv,
            global_snapshot: &global_snapshot,
            snapshot: &snapshot,
            xid: 1,
            eval_ctx: &eval_ctx,
        };
        let locks = FakeLocks::default();
        let mut cascade = FakeCascade::default();
        let fk = deferrable_fk("fk", true);
        let mut queue = FkCheckQueue::default();
        queue.push(PendingCheck::Child {
            fk: Arc::new(fk.clone()),
            rowid: 3,
            key: Some(vec![Datum::Int4(1)]),
        });
        let mut store = DeferredConstraints::default();
        let ops = futures_block_on(drain_statement_checks(
            &ctx,
            &locks,
            &mut cascade,
            &mut queue,
            Some(&mut store),
        ))
        .expect("drain");

        // Nothing ran: no catalog read, no lock, no ops — the check simply moved.
        assert!(ops.is_empty());
        assert!(locks.taken.lock().expect("locks").is_empty());
        assert!(
            store.take_all()
                == vec![PendingCheck::Child {
                    fk: Arc::new(fk),
                    rowid: 3,
                    key: None,
                }]
        );
    }
}
