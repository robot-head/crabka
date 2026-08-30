use super::*;

pub(super) fn resolve_relation_tablespace_oid(kv: &dyn Kv, name: &str) -> Result<u32, ExecError> {
    if name == "pg_global" {
        return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
            "0A000",
            "only shared relations can be placed in pg_global tablespace",
        )));
    }
    crabka_pgcatalog::tablespace_oid(kv, name).map_err(|error| match error {
        crabka_pgcatalog::CatalogError::UndefinedObject(_) => {
            ExecError::Remote(crabka_pgwire::error::PgError::error(
                "42704",
                format!("tablespace \"{name}\" does not exist"),
            ))
        }
        other => other.into(),
    })
}

pub(super) fn constraint_deferral(
    attributes: crabka_pgparser::ast::ConstraintAttributes,
) -> crabka_pgcatalog::ConstraintDeferral {
    crabka_pgcatalog::ConstraintDeferral::of(attributes.deferrable, attributes.initially_deferred)
}

pub(super) fn create_table_constraint_index(
    table_name: &crabka_pgcatalog::RelationName,
    columns: &[String],
    primary_key: bool,
    without_overlaps: bool,
    deferral: crabka_pgcatalog::ConstraintDeferral,
) -> crabka_pgcatalog::NewIndex {
    let suffix = if primary_key { "pkey" } else { "key" };
    let table = &table_name.name;
    let name = if primary_key {
        format!("{table}_pkey")
    } else {
        format!("{table}_{}_{suffix}", columns.join("_"))
    };
    crabka_pgcatalog::NewIndex {
        name,
        columns: columns.to_vec(),
        include: Vec::new(),
        predicate: None,
        unique: true,
        placement: crabka_pgcatalog::IndexPlacement::Local,
        method: if without_overlaps {
            crabka_pgcatalog::IndexMethod::Gist
        } else {
            crabka_pgcatalog::IndexMethod::Btree
        },
        constraint: Some(if primary_key {
            crabka_pgcatalog::IndexConstraint::PrimaryKey
        } else {
            crabka_pgcatalog::IndexConstraint::Unique
        }),
        without_overlaps,
        deferral,
    }
}

pub(super) fn validate_without_overlaps_key(
    columns: &[String],
    table_columns: &[Column],
) -> Result<(), ExecError> {
    let Some((temporal, leading)) = columns.split_last() else {
        return Err(ExecError::WithoutOverlapsNeedsTwoColumns);
    };
    if leading.is_empty() {
        return Err(ExecError::WithoutOverlapsNeedsTwoColumns);
    }
    let column = table_columns
        .iter()
        .find(|column| column.name == *temporal)
        .ok_or_else(|| ExecError::UndefinedIndexColumn(temporal.clone()))?;
    if !matches!(
        column.ty.storage_type(),
        crabka_pgtypes::ColumnType::Range(_) | crabka_pgtypes::ColumnType::Multirange(_)
    ) {
        return Err(ExecError::WithoutOverlapsNotRange(temporal.clone()));
    }
    Ok(())
}

pub(super) fn create_table_primary_key_columns<'a>(
    columns: &'a [crabka_pgparser::ast::ColumnDef],
    constraints: &'a [crabka_pgparser::ast::TableConstraint],
) -> HashSet<&'a str> {
    let mut primary_key_columns = HashSet::new();
    for column in columns {
        if column.constraints.iter().any(|constraint| {
            matches!(
                constraint.kind,
                crabka_pgparser::ast::ColumnConstraintKind::PrimaryKey
            )
        }) {
            primary_key_columns.insert(column.name.as_str());
        }
    }
    for constraint in constraints {
        if let crabka_pgparser::ast::TableConstraintKind::PrimaryKey { columns, .. } =
            &constraint.kind
        {
            primary_key_columns.extend(columns.iter().map(String::as_str));
        }
    }
    primary_key_columns
}
