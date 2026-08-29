use super::*;

pub(crate) fn virtual_catalog_relation(
    catalog_kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
    alias: Option<&str>,
    ctx: &crate::clock::EvalCtx,
    refs: Option<&crate::scope::StatementRefs>,
) -> Result<Option<Relation>, ExecError> {
    let Some(table) = virtual_table(&virtual_lookup_key(name)) else {
        return Ok(None);
    };
    let described = virtual_catalog_table(table);
    let (scope, system) = virtual_catalog_scope(catalog_kv, &described, name, alias, refs)?;
    let rows = catalog_rows::virtual_catalog_rows(catalog_kv, table, ctx)?;
    let mut rows = rows
        .into_iter()
        .zip(1u64..)
        .map(|(mut row, ordinal)| {
            if system.tableoid {
                row.push(Datum::Int4(virtual_relation_oid(table)));
            }
            if system.ctid {
                row.push(crate::scope::row_ctid(ordinal));
            }
            row
        })
        .collect::<Vec<_>>();
    catalog_rows::resolve_regclass_at(
        catalog_kv,
        ctx.resolution(),
        &catalog_rows::regclass_column_indexes(&described, 0),
        &mut rows,
    )?;
    Ok(Some(Relation { scope, rows }))
}

pub(crate) fn virtual_catalog_relation_schema(
    catalog_kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
    alias: Option<&str>,
    refs: Option<&crate::scope::StatementRefs>,
) -> Result<Option<Relation>, ExecError> {
    let Some(table) = virtual_table(&virtual_lookup_key(name)) else {
        return Ok(None);
    };
    let described = virtual_catalog_table(table);
    let (scope, _) = virtual_catalog_scope(catalog_kv, &described, name, alias, refs)?;
    Ok(Some(Relation {
        scope,
        rows: Vec::new(),
    }))
}

pub(crate) fn relation_scope(
    catalog_kv: &dyn Kv,
    table: &Table,
    qualifier: &str,
) -> Result<Scope, ExecError> {
    let typed_row_type = crabka_pgcatalog::typed_table_type(catalog_kv, &table.name)?
        .map(|oid| {
            crabka_pgcatalog::list_user_types(catalog_kv)?
                .into_iter()
                .find(|ty| ty.oid == oid)
                .and_then(|ty| match ty.column_type() {
                    Some(ColumnType::Record(Some(row_type))) => Some(row_type),
                    _ => None,
                })
                .ok_or_else(|| {
                    ExecError::ObjectNotInPrerequisiteState(
                        "typed table refers to a missing composite type".into(),
                    )
                })
        })
        .transpose()?;
    let mut scope = Scope::single_with_row_type(
        table,
        qualifier,
        typed_row_type.or(crate::catalog_rel::relation_rowtype(
            catalog_kv,
            &table.name,
        )?),
    );
    let indexes = match crabka_pgcatalog::list_table_indexes(catalog_kv, &table.name) {
        Ok(indexes) => indexes,
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => return Ok(scope),
        Err(error) => return Err(error.into()),
    };
    if let Some(primary_key) = indexes.into_iter().find(|index| {
        matches!(
            index.constraint,
            Some(crabka_pgcatalog::IndexConstraint::PrimaryKey)
        )
    }) {
        scope.set_primary_key(qualifier, primary_key.columns);
    }
    Ok(scope)
}

fn virtual_catalog_scope(
    catalog_kv: &dyn Kv,
    described: &Table,
    name: &crabka_pgcatalog::RelationName,
    alias: Option<&str>,
    refs: Option<&crate::scope::StatementRefs>,
) -> Result<(Scope, crate::scope::SystemColumns), ExecError> {
    let qualifier = alias.unwrap_or(&name.name);
    let mut scope = relation_scope(catalog_kv, described, qualifier)?;
    let system = crate::scope::SystemColumns {
        tableoid: crate::scope::wants_tableoid(refs),
        cmax: false,
        xmax: false,
        cmin: false,
        xmin: false,
        ctid: crate::scope::SystemColumns::of(refs, described).ctid,
    };
    system.extend_scope(&mut scope, qualifier);
    Ok((scope, system))
}
