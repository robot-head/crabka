//! Local-index write coordination.

use super::*;

pub(crate) fn writable_local_indexes(
    catalog_kv: &dyn Kv,
    table: &Table,
) -> Result<Vec<crabka_pgcatalog::Index>, ExecError> {
    let indexes = crabka_pgcatalog::list_table_indexes(catalog_kv, &table.name)?;
    if indexes.is_empty() {
        return Ok(Vec::new());
    }
    let mut local_indexes = Vec::new();
    for index in indexes {
        if index.placement != crabka_pgcatalog::IndexPlacement::Local {
            if index.unique {
                return Err(ExecError::Unsupported(
                    "unique global indexes are not supported until global enforcement exists"
                        .into(),
                ));
            }
            continue;
        }
        reject_unwritable_local_index(table)?;
        local_indexes.push(index);
    }
    Ok(local_indexes)
}

/// One relation `CLUSTER` will reorder, paired with the index whose order it
/// takes. A partitioned parent expands into one unit per leaf.

/// The relation whose unique-index gate a DML statement must hold SHARED for
/// its duration (until COMMIT/ROLLBACK in an explicit transaction). Unique-index
/// DDL takes that relation's gate EXCLUSIVELY while it backfills. Same-key DML
/// conflicts serialize through per-key locks in the `RowLockManager` instead.
pub(crate) enum UniqueLocalSerialization {
    None,
    Shared(crabka_pgcatalog::TableId),
}

pub(crate) fn write_requires_unique_local_serialization(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    stmt: &Statement,
) -> Result<UniqueLocalSerialization, ExecError> {
    let table_name = match stmt {
        Statement::Insert { table, .. }
        | Statement::Update { table, .. }
        | Statement::Delete { table, .. }
        | Statement::Merge { table, .. } => table,
        // `CLUSTER` moves rows to new rowids and re-emits every local index
        // entry for them, which is exactly what a concurrent unique-index
        // backfill must not run alongside.
        Statement::Cluster(Some(target)) => &target.table,
        _ => return Ok(UniqueLocalSerialization::None),
    };
    let table_name = resolve_relation(
        catalog_kv,
        resolution,
        table_name,
        SchemaDisposition::Reference,
    )?;
    if table_name.schema == crate::search_path::PG_CATALOG && table_name.name == "pg_class" {
        return Ok(UniqueLocalSerialization::None);
    }
    table_requires_unique_local_serialization(catalog_kv, &table_name)
}

pub(crate) fn copy_requires_unique_local_serialization(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    target: CopyIntoTarget<'_>,
) -> Result<UniqueLocalSerialization, ExecError> {
    table_requires_unique_local_serialization(
        catalog_kv,
        &resolve_relation(
            catalog_kv,
            resolution,
            target.name,
            SchemaDisposition::Utility,
        )?,
    )
}
