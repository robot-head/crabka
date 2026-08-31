use super::*;

pub(super) type TableFunctionRows = (Vec<(String, ColumnType)>, Vec<Vec<Datum>>);

/// Read a table's durable next-rowid (1 if unset). Single source of truth for
/// the sequence read.
pub(crate) fn read_seq_kv(kv: &dyn Kv, table: TableId) -> Result<u64, ExecError> {
    match kv.get(&crabka_pgkv::key::seq_key(table))? {
        Some(b) => {
            let (v, _) = U64::read_from_prefix(b.as_slice())
                .map_err(|_| crabka_pgkv::KvError::CorruptRow("sequence is not u64".into()))?;
            Ok(v.get())
        }
        None => Ok(1),
    }
}

/// A `QueryResult::Command` with the given PostgreSQL completion tag.
pub(super) fn command(tag: &str) -> QueryResult {
    QueryResult::Command { tag: tag.into() }
}

pub(super) fn trigger_modified_row_error(operation: CommandOperation) -> ExecError {
    ExecError::Remote(crabka_pgwire::error::PgError::error(
        "27000",
        format!(
            "tuple to be {} was already modified by an operation triggered by the current command",
            operation.as_str(),
        ),
    )
    .with_hint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows."))
}

pub(super) fn merge_trigger_error(error: ExecError) -> ExecError {
    match error {
        ExecError::Remote(error) if error.code == "27000" => {
            trigger_modified_row_error(CommandOperation::UpdatedOrDeleted)
        }
        error => error,
    }
}

/// The name every expression in a DML statement resolves the target's columns
/// under: its alias when it has one, else the table name. `PostgreSQL` hides
/// the real name once an alias is given.
pub(super) fn table_qualifier<'a>(table: &'a Table, alias: &'a Option<String>) -> &'a str {
    alias.as_deref().unwrap_or(&table.name.name)
}

/// How far below the relation it names a statement is asking to reach.
///
/// `PostgreSQL` spells this with one keyword — `ONLY` — and the engine used to
/// carry it as the one boolean the parser produces. That is why `ONLY` was
/// ignored on a partitioned parent: the keyword answers two independent
/// questions at once, and the engine's own tree walks need the two answers to
/// differ.
///
/// * An inheritance parent stores rows of its own, and its descendants store
///   more. `ONLY` asks for the parent's.
/// * A partitioned parent stores no rows at all. Its leaves store them, and
///   `ONLY` therefore asks for nothing.
///
/// A walk that has already enumerated the inheritance descendants must not
/// enumerate them again — it would read every relation once per path to it
/// through a multiple-inheritance DAG — but it still has to reach a partitioned
/// relation's leaves. Written as one boolean those two are the same value, and
/// they are not the same request: `TRUNCATE parted` desugars to an unfiltered
/// `DELETE` that says "do not walk inheritance", and reading that as the user's
/// `ONLY` empties nothing at all.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Reach {
    /// Everything at or below the named relation: its own rows, its inheritance
    /// descendants', and the leaves' of any partitioned relation among them.
    /// What a statement that omitted `ONLY` asks for.
    Tree,
    /// The rows the named relation stores itself, and nothing below it. What
    /// `ONLY` asks for. A partitioned parent stores none, so `ONLY` over one
    /// reads and writes nothing — which is what `PostgreSQL` does.
    OwnRows,
    /// The named relation's own rows, plus its leaves' if it is partitioned, but
    /// no inheritance walk: the caller has already done that walk. Only the
    /// engine's own tree walks and desugarings ask for this; no SQL spells it.
    Storage,
}

impl Reach {
    /// What the statement itself asks for, before any engine rewrite.
    pub(super) fn of(stmt: &Statement) -> Self {
        match stmt {
            Statement::Update { only: true, .. } | Statement::Delete { only: true, .. } => {
                Reach::OwnRows
            }
            _ => Reach::Tree,
        }
    }

    /// Whether a partitioned relation reached this way expands into its leaves.
    pub(super) fn spans_partitions(self) -> bool {
        match self {
            Reach::Tree | Reach::Storage => true,
            Reach::OwnRows => false,
        }
    }
}

/// For each of `target`'s columns, the ordinal of the same-named column in
/// `source`, the permutation that rewrites a `source`-shaped row into a
/// `target`-shaped one. A partition and its parent always declare the same
/// column names, but `ATTACH PARTITION` maps them by name, not by position.
pub(crate) fn column_mapping(target: &Table, source: &Table) -> Result<Vec<usize>, ExecError> {
    target
        .columns
        .iter()
        .map(|column| {
            source
                .column_index(&column.name)
                .ok_or_else(|| ExecError::ChildMissingColumn(column.name.clone()))
        })
        .collect()
}

/// `row` read through `ordinals`: `ordinals[i]` is the index in `row` of the
/// result's `i`th column, as [`column_mapping`] computes it. An index the
/// source row does not have reads as NULL.
pub(crate) fn permuted_row(row: &[Datum], ordinals: &[usize]) -> Vec<Datum> {
    ordinals
        .iter()
        .map(|ordinal| row.get(*ordinal).cloned().unwrap_or(Datum::Null))
        .collect()
}

/// `row` permuted by `ordinals`, or handed straight back when there are none —
/// which is how a relation reshapes rows it read from itself, whose mapping is
/// the identity and whose permutation would be a copy.
pub(super) fn reshape_row(row: Vec<Datum>, ordinals: Option<&[usize]>) -> Vec<Datum> {
    match ordinals {
        Some(ordinals) => permuted_row(&row, ordinals),
        None => row,
    }
}

/// The leaf partition a row of `parent`'s shape belongs to, together with the
/// row permuted into that leaf's own column order.
///
/// A row no partition accepts is `PostgreSQL`'s 23514, raised here rather than
/// reported upwards, because only this frame knows *which* relation declined
/// it. In a multi-level tree that is the sub-partitioned child the row reached,
/// not the parent the statement named, and the key quoted alongside it is that
/// child's own key — which need not share a single column with the parent's.
/// Naming the root instead would report a key the row never failed against.
pub(super) fn route_row_to_leaf(
    write_ctx: &WriteContext<'_>,
    parent: &Table,
    row: &[Datum],
) -> Result<(Table, Vec<Datum>), ExecError> {
    let kv = write_ctx.catalog_kv;
    let Some(scheme) = crate::partition::scheme_of(kv, &parent.name)? else {
        return Ok((parent.clone(), row.to_vec()));
    };
    let partitions = crate::partition::partitions_of(kv, &parent.name)?;
    let Some(chosen) = crate::partition::route(&scheme, &parent.columns, &partitions, row)? else {
        return Err(ExecError::NoPartitionForRow {
            relation: parent.name.to_string(),
            key: may_describe_key(write_ctx, parent)
                .then(|| {
                    crate::partition::key_description(
                        &scheme,
                        &parent.columns,
                        row,
                        write_ctx.eval_ctx,
                    )
                })
                .transpose()?,
        });
    };
    let child = crabka_pgcatalog::get_table(kv, &chosen.name)?;
    let child_row = permuted_row(row, &column_mapping(&child, parent)?);
    route_row_to_leaf(write_ctx, &child, &child_row)
}

pub(crate) const PG_CATALOG_NAMESPACE_OID: i32 = 11;
pub(crate) const PG_TOAST_NAMESPACE_OID: i32 = 99;
pub(crate) const INFORMATION_SCHEMA_NAMESPACE_OID: i32 = 13_370;
pub(crate) const PUBLIC_NAMESPACE_OID: i32 = 2200;
/// The database name a session falls back to when nothing told it one.
///
/// A session opened over the wire takes its name from the startup packet, and
/// that is the name every catalog projection answers with. This constant is
/// only the floor for a session that never saw a startup packet: a planning
/// context, a unit test, or an embedded caller. It is `PostgreSQL`'s own
/// `initdb` default so those contexts read the way a fresh cluster does.
///
/// Nothing may compare a written name against this constant. The question
/// "does this name mean the database I am in?" is a question about the
/// session, answered by [`crate::clock::EvalCtx::database`]; asking it of a
/// constant made `REINDEX DATABASE <the open database>` fail and
/// `postgres.public.t` resolve locally from every database at once.
pub(crate) const DEFAULT_DATABASE: &str = "postgres";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BuiltinTypeRow {
    pub(super) oid: i32,
    pub(super) name: &'static str,
    pub(super) len: i16,
    pub(super) category: &'static str,
    /// `pg_type.typelem`: the element type of an array type, 0 for a scalar.
    pub(super) elem: i32,
    /// `pg_type.typarray`: the array type over a scalar, 0 for an array type
    /// and for the scalars crabka has no array type for (`varchar`, `char(n)`,
    /// `regclass`). [`crabka_pgtypes::ElemType::from_column_type`] refuses
    /// those, so a pointer at an absent row would be worse than a report of
    /// none.
    pub(super) array: i32,
}

/// `pg_type.oid` of `_timetz`. `ElemType` has no `timetz` variant, so crabka
/// cannot build a `timetz[]` value, but `timetz.typarray` still has to resolve
/// — the same position `_inet` and `_money` are in. `crabka_pgtypes::oids` has
/// no constant for an array type it cannot construct.
pub(super) const TIMETZ_ARRAY_OID: i32 = 1270;

/// `pg_type.oid` of `_int2vector`, which is in that same position.
pub(super) const INT2VECTOR_ARRAY_OID: i32 = 1006;

/// `pg_type.oid` of `_pg_snapshot`, in that same position again.
pub(super) const PG_SNAPSHOT_ARRAY_OID: i32 = 5039;

/// `pg_type.oid` of `_txid_snapshot`.
pub(super) const TXID_SNAPSHOT_ARRAY_OID: i32 = 2949;
