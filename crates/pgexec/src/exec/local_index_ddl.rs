//! Local-index validation and backfill for DDL.

use super::*;

pub(crate) fn table_requires_unique_local_serialization(
    catalog_kv: &dyn Kv,
    table_name: &crabka_pgcatalog::RelationName,
) -> Result<UniqueLocalSerialization, ExecError> {
    let table = match crabka_pgcatalog::get_table(catalog_kv, table_name) {
        Ok(table) => table,
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_))
            if crabka_pgcatalog::get_view(catalog_kv, table_name).is_ok() =>
        {
            return Ok(UniqueLocalSerialization::None);
        }
        Err(error) => return Err(error.into()),
    };
    if table.sharded {
        return Ok(UniqueLocalSerialization::None);
    }
    let indexes = crabka_pgcatalog::list_table_indexes(catalog_kv, table_name)?;
    for index in indexes {
        if index.unique && index.placement != crabka_pgcatalog::IndexPlacement::Local {
            return Err(ExecError::Unsupported(
                "unique global indexes are not supported until global enforcement exists".into(),
            ));
        }
    }
    Ok(UniqueLocalSerialization::Shared(table.id))
}

pub(crate) fn reject_unwritable_local_index(table: &Table) -> Result<(), ExecError> {
    if table.sharded {
        return Err(ExecError::Unsupported(
            "local index maintenance for sharded timestamp writes is blocked on G-6".into(),
        ));
    }
    Ok(())
}

pub(crate) fn local_index_backfill_ops(
    kv: &dyn Kv,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    own_xid: Option<u64>,
    build: &IndexBuild<'_>,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let all_committed = all_committed_snapshot();
    // `own_xid` makes the open transaction's own uncommitted rows visible to the
    // back-validation; the all-committed snapshot alone does not, because the
    // scan still asks the commit log and this transaction is in progress there.
    let rows = scan_live(kv, kv, &all_committed, &all_committed, own_xid, table)?;
    local_index_backfill_ops_for_rows(kv, &rows, table, index, build)
}

/// What a back-validating index build needs to describe the duplicate key it
/// found, and nothing else: whose eyes the key is described to, and the output
/// styles it is spelled in.
///
/// The two travel together because neither is enough on its own, and every DDL
/// path that can raise the error already has to carry both — the role from the
/// statement's [`ForeignCtx`], because `EvalCtx::for_ddl` leaves `current_user`
/// at the conventional `"public"` and so cannot supply it.
pub(crate) struct IndexBuild<'a> {
    describer: crate::rls::Describer,
    ctx: &'a crate::clock::EvalCtx,
}

impl<'a> IndexBuild<'a> {
    pub(crate) fn new(fctx: &ForeignCtx<'_>, ctx: &'a crate::clock::EvalCtx) -> Self {
        Self {
            describer: fctx.describer(),
            ctx,
        }
    }

    /// The evaluation context the build's own scans run under.
    pub(crate) const fn ctx(&self) -> &'a crate::clock::EvalCtx {
        self.ctx
    }

    /// The 23505 a build raises for a key two live rows already share.
    fn duplicate(
        &self,
        kv: &dyn Kv,
        table: &Table,
        index: &crabka_pgcatalog::Index,
        values: &[Datum],
    ) -> ExecError {
        ExecError::UniqueIndexBuildViolation(Box::new(crate::error::UniqueViolation {
            index: index.name.clone(),
            table: table.name.clone(),
            key: self
                .describer
                .index_key(kv, table, &index.columns, values, self.ctx),
        }))
    }
}

/// Backfill index entries for already-scanned live rows.
///
/// A UNIQUE index back-validates the existing data: a duplicate non-NULL key
/// fails the index *build* with 23505 before any op is committed. Rows with a
/// NULL key column are not indexed, which matches SQL NULL-distinct
/// semantics.
pub(crate) fn local_index_backfill_ops_for_rows(
    kv: &dyn Kv,
    rows: &[(u64, u64, Vec<Datum>)],
    table: &Table,
    index: &crabka_pgcatalog::Index,
    build: &IndexBuild<'_>,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let mut seen = HashSet::new();
    let mut ops = Vec::with_capacity(rows.len());
    for (rowid, _xmin, row) in rows {
        for values in index_entries(table, index, row)? {
            if values.iter().any(Datum::is_null) {
                continue;
            }
            if index.unique && !seen.insert(values.clone()) {
                return Err(build.duplicate(kv, table, index, &values));
            }
            ops.push(crabka_pgkv::WriteOp::Put {
                key: crabka_pgkv::key::secondary_index_entry_key(
                    table.id, index.id, &values, *rowid,
                ),
                value: Vec::new(),
            });
        }
    }
    Ok(ops)
}
