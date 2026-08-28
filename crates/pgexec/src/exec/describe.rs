use super::*;

// `describe` only resolves the SELECT's row description from the catalog (no
// rows are scanned), so the data store `_kv` is unused here. It is kept in the
// signature for uniformity with the other three executor entry points (all take
// `catalog_kv, kv, …`) so the session's call sites stay consistent.
pub(crate) fn describe(
    catalog_kv: &dyn Kv,
    _kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    sql: &str,
) -> Result<Vec<crabka_pgwire::engine::FieldDescription>, ExecError> {
    let statements = crabka_pgparser::parse(sql)?;
    let Some(statement) = statements.first() else {
        return Ok(Vec::new());
    };
    describe_statement(catalog_kv, resolution, statement)
}

pub(crate) fn describe_statement(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    statement: &Statement,
) -> Result<Vec<crabka_pgwire::engine::FieldDescription>, ExecError> {
    match statement {
        Statement::Query(q) => crate::query::describe_query_expr(catalog_kv, resolution, q),
        Statement::Insert {
            table, returning, ..
        }
        | Statement::Update {
            table, returning, ..
        }
        | Statement::Delete {
            table, returning, ..
        } => describe_returning(
            catalog_kv,
            &resolve_relation(catalog_kv, resolution, table, SchemaDisposition::Reference)?,
            returning.as_ref(),
            false,
        ),
        Statement::Merge {
            table, returning, ..
        } => describe_returning(
            catalog_kv,
            &resolve_relation(catalog_kv, resolution, table, SchemaDisposition::Reference)?,
            returning.as_ref(),
            true,
        ),
        _ => Ok(Vec::new()),
    }
}

pub(crate) fn describe_returning(
    catalog_kv: &dyn Kv,
    table: &crabka_pgcatalog::RelationName,
    returning: Option<&crabka_pgparser::ast::Returning>,
    merge: bool,
) -> Result<Vec<FieldDescription>, ExecError> {
    // The target is resolved even with no RETURNING clause, because analysing a
    // DML statement must reject a missing table (42P01) whether or not the
    // statement would have returned rows.
    let table = crabka_pgcatalog::get_table(catalog_kv, table)?;
    let Some(returning) = returning else {
        return Ok(Vec::new());
    };

    // Describe resolves against the target alone: OLD/NEW image columns mirror
    // its types, and a joined FROM/USING adds columns the caller must qualify.
    // The range-table entry a DML statement adds is aliased to the relation's
    // BARE name, never its schema-qualified spelling: `INSERT INTO s.t …
    // RETURNING t.c` binds in PostgreSQL, and `s.t.c` does not even parse there.
    //
    // The system columns come off the `RETURNING` list alone, which is the only
    // clause this path is given. That is enough: describing a statement means
    // naming the columns it will project, and only what the list spells is
    // projected.
    let mut refs = crate::scope::StatementRefs::default();
    refs.add_returning_items(&returning.items);
    let mut scope = Scope::single(&table, &table.name.name);
    crate::scope::SystemColumns::of(Some(&refs), &table)
        .stamp(table.id)?
        .extend_scope(&mut scope, &table.name.name);
    let spec = ReturningSpec::new(
        &table,
        &table.name.name,
        Some(returning),
        Some(&scope),
        merge,
    )?;
    let (mut fields, _exprs, _tys) = resolve_projection(&spec.items, &spec.scope)?;
    show_image_bindings_by_their_column_names(&mut fields);
    Ok(fields)
}

// ── D1/D4/D7: CREATE TABLE breadth, CHECK constraints, ALTER TABLE ───────────

/// A resolved `CREATE TABLE` definition: catalog columns, `CHECK` constraints,
/// the sequences its SERIAL/identity columns need, its constraint-backed
/// indexes, and the `FOREIGN KEY` clauses it collected (named, but not yet
/// resolved; see [`PendingForeignKey`]).
pub(super) type TableDefinition = (
    Vec<Column>,
    Vec<crabka_pgcatalog::CheckConstraint>,
    Vec<(crabka_pgcatalog::RelationName, Sequence)>,
    Vec<crabka_pgcatalog::NewIndex>,
    Vec<PendingForeignKey>,
);
