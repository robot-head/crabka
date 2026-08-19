//! DDL and catalog code carved out of `exec`.

use super::{
    AddForeignKey, AlterTableState, Column, ColumnDefault, Datum, ExecError, Expr, HashMap,
    HashSet, IndexBuild, Kv, SchemaDisposition, Scope, Sequence, Table, TableDefinition,
    all_committed_snapshot, coerce, column_from_ast, constraint_deferral,
    create_table_constraint_index, create_table_primary_key_columns, eval_assignment_value,
    expr_children, global_status, local_index_backfill_ops, no_inherit_not_null_unsupported,
    physical_rowid, resolve_relation, stored_default_value, validate_check_predicate,
    validate_without_overlaps_key,
};

/// Build the definition of a `CREATE TABLE … PARTITION OF parent`.
///
/// A partition declares no columns: it takes the parent's list and the parent's
/// `CHECK` constraints, and the written element list may only add qualifiers to
/// what it inherits.
pub(crate) fn partition_definition(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
    spec: &crabka_pgparser::ast::PartitionOf,
    constraints: &[crabka_pgparser::ast::TableConstraint],
    like: &[crabka_pgparser::ast::LikeClause],
    ctx: &crate::clock::EvalCtx,
) -> Result<TableDefinition, ExecError> {
    let resolution = ctx.resolution();
    let parent_name = &resolve_relation(kv, resolution, &spec.parent, SchemaDisposition::Utility)?;
    let parent = crabka_pgcatalog::get_table(kv, parent_name)?;
    if crate::partition::scheme_of(kv, parent_name)?.is_none() {
        return Err(ExecError::NotPartitioned(parent_name.to_string()));
    }
    let (_, extra_checks, sequences, mut indexes, foreign_keys) =
        create_table_definition(kv, name, &[], constraints, like, &parent.columns, ctx)?;
    // The partition takes a copy of every index the parent carries. See
    // [`partition_index_clones`]: the parent's own copy enforces nothing,
    // because the parent stores no rows.
    let clones = partition_index_clones(kv, parent_name, name, &indexes)?;
    indexes.extend(clones);
    let mut columns = parent.columns.clone();
    for option in &spec.column_options {
        let column = &option.column;
        let target = columns
            .iter_mut()
            .find(|candidate| candidate.name == *column)
            .ok_or_else(|| ExecError::UndefinedTableColumn {
                column: column.clone(),
                table: name.to_string(),
            })?;
        // A partition's `(a COLLATE "POSIX")` is parsed and then dropped: the
        // column keeps the collation the parent declared, which is what
        // PostgreSQL does — it accepts the clause without complaint and without
        // effect. The type rule still applies to what was written.
        if option.collation.is_some() {
            crate::eval::require_collatable(target.ty)?;
        }
        for qualifier in &option.constraints {
            match &qualifier.kind {
                crabka_pgparser::ast::ColumnConstraintKind::NotNull => target.not_null = true,
                crabka_pgparser::ast::ColumnConstraintKind::Null => target.not_null = false,
                crabka_pgparser::ast::ColumnConstraintKind::Default(expr) => {
                    let value = eval_assignment_value(expr, target.ty, &Scope::empty(), &[], ctx)?;
                    let coerced = coerce(value.clone(), target.ty, ctx)?;
                    target.default =
                        Some(ColumnDefault::Value(stored_default_value(value, coerced)));
                }
                other => {
                    return Err(ExecError::Unsupported(format!(
                        "{other:?} on a partition's inherited column is not supported"
                    )));
                }
            }
        }
    }
    let mut checks = parent.checks.clone();
    checks.extend(extra_checks);
    Ok((columns, checks, sequences, indexes, foreign_keys))
}

/// The name a partition's copy of `source` is generated under, before the
/// numeric suffix that resolves a collision.
///
/// `PostgreSQL` clones the parent's index with no name at all and lets
/// `DefineIndex` choose one, so the copy is named after the *partition*, not
/// after the index it was copied from.
pub(crate) fn cloned_index_base_name(
    child: &crabka_pgcatalog::RelationName,
    source: &crabka_pgcatalog::Index,
) -> String {
    let parts = source
        .columns
        .iter()
        .map(|key| {
            if crabka_pgcatalog::index_key_expression(key).is_some() {
                "expr"
            } else {
                key.as_str()
            }
        })
        .collect::<Vec<_>>()
        .join("_");
    match &source.constraint {
        Some(crabka_pgcatalog::IndexConstraint::PrimaryKey) => format!("{}_pkey", child.name),
        Some(crabka_pgcatalog::IndexConstraint::Exclusion(_)) => {
            format!("{}_{parts}_excl", child.name)
        }
        Some(crabka_pgcatalog::IndexConstraint::Unique) => format!("{}_{parts}_key", child.name),
        None => format!("{}_{parts}_idx", child.name),
    }
}

/// `PostgreSQL`'s `ChooseRelationName`: the first of `<base>`, `<base>1`,
/// `<base>2`, … that nothing in the relation's schema already answers to.
///
/// `taken` holds the names this statement has already handed out but not yet
/// written, which the catalog cannot see.
pub(crate) fn available_index_name(
    kv: &dyn Kv,
    table: &crabka_pgcatalog::RelationName,
    base: &str,
    taken: &HashSet<String>,
) -> String {
    let occupied = |candidate: &str| {
        let sibling = table.sibling(candidate);
        taken.contains(candidate)
            || crabka_pgcatalog::get_index(kv, &sibling).is_ok()
            || crabka_pgcatalog::get_table(kv, &sibling).is_ok()
    };
    let mut candidate = base.to_string();
    let mut suffix = 0u32;
    while occupied(&candidate) {
        suffix += 1;
        candidate = format!("{base}{suffix}");
    }
    candidate
}

/// Is `candidate`, already on the partition, the copy of `source` that the
/// partition owes its parent?
///
/// `PostgreSQL` compares the key list, the access method and the uniqueness,
/// and additionally insists that a parent index backing a constraint be matched
/// by a child index backing one too — otherwise the partition would carry the
/// key without carrying the constraint that names it.
pub(crate) fn matches_parent_index(
    candidate: &crabka_pgcatalog::Index,
    source: &crabka_pgcatalog::Index,
) -> bool {
    candidate.columns == source.columns
        && candidate.unique == source.unique
        && candidate.method == source.method
        && candidate.without_overlaps == source.without_overlaps
        && source.constraint.is_none() == candidate.constraint.is_none()
}

/// Copy `source` onto `child` under a freshly generated name.
pub(crate) fn cloned_partition_index(
    name: String,
    source: &crabka_pgcatalog::Index,
) -> crabka_pgcatalog::NewIndex {
    crabka_pgcatalog::NewIndex {
        name,
        columns: source.columns.clone(),
        unique: source.unique,
        placement: source.placement,
        method: source.method,
        constraint: source.constraint.clone(),
        without_overlaps: source.without_overlaps,
        // A cloned constraint keeps the parent's check point. Dropping the
        // deferral would turn `UNIQUE DEFERRABLE` on the parent into an
        // immediate key on every partition, which is where it is enforced.
        deferral: source.deferral,
    }
}

/// The copies of a partitioned parent's indexes that a brand-new partition owes
/// it.
///
/// A partitioned relation stores no rows, so an index on the parent enforces
/// nothing: an inserted row is routed to a leaf, and the write path consults
/// that leaf's own index list. `PostgreSQL` therefore puts a copy of every
/// parent index on every partition, and it is the copy that enforces the key,
/// names itself in the 23505 and shows up in `pg_constraint` under the
/// partition's `conrelid`.
///
/// A copy is enforceable on its own because a unique key on a partitioned table
/// must contain every partition-key column — the refusal in
/// [`partition_scheme_from_ast`] — so no two partitions can ever hold the same
/// key.
///
/// `declared` are the indexes the `CREATE TABLE` wrote for itself, which have
/// already claimed their names.
pub(crate) fn partition_index_clones(
    kv: &dyn Kv,
    parent: &crabka_pgcatalog::RelationName,
    child: &crabka_pgcatalog::RelationName,
    declared: &[crabka_pgcatalog::NewIndex],
) -> Result<Vec<crabka_pgcatalog::NewIndex>, ExecError> {
    let mut taken: HashSet<String> = declared.iter().map(|index| index.name.clone()).collect();
    let mut clones = Vec::new();
    for source in crabka_pgcatalog::list_table_indexes(kv, parent)? {
        // A global index spans the whole relation already, so it has no
        // per-partition copy to make.
        if source.placement != crabka_pgcatalog::IndexPlacement::Local {
            continue;
        }
        let name = available_index_name(kv, child, &cloned_index_base_name(child, &source), &taken);
        taken.insert(name.clone());
        clones.push(cloned_partition_index(name, &source));
    }
    Ok(clones)
}

/// The index records `ALTER TABLE parent ATTACH PARTITION child` owes, with
/// their back-validating builds.
///
/// `PostgreSQL`'s `AttachPartitionEnsureIndexes` looks for an index on the
/// candidate that already matches each of the parent's, and creates one only
/// where there is no match. A relation being attached may already hold rows, so
/// each created index is backfilled — and a `UNIQUE` copy that two of those
/// rows would share raises the 23505 there, which is what stops the attach.
///
/// The walk covers the attached relation and everything below it, because a
/// sub-partitioned candidate's own leaves are where the parent's key will
/// actually be enforced.
pub(crate) fn attached_partition_index_ops(
    kv: &dyn Kv,
    parent: &Table,
    child: &crabka_pgcatalog::RelationName,
    own_xid: Option<u64>,
    build: &IndexBuild<'_>,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let sources = crabka_pgcatalog::list_table_indexes(kv, &parent.name)?;
    let mut relations = vec![child.clone()];
    relations.extend(crate::partition::descendants(kv, child)?);
    clone_indexes_onto_partitions(kv, &sources, &relations, own_xid, build)
}

/// Put a copy of each of `sources` on every relation in `relations` that has no
/// equivalent index already, building each copy over the rows that relation
/// holds.
///
/// The shared body of the three statements that owe a partition its parent's
/// indexes: `CREATE TABLE … PARTITION OF` (which reaches it through
/// [`partition_index_clones`], because the partition does not exist yet),
/// `ATTACH PARTITION`, and `ALTER TABLE … ADD CONSTRAINT` on a parent that
/// already has partitions.
pub(crate) fn clone_indexes_onto_partitions(
    kv: &dyn Kv,
    sources: &[crabka_pgcatalog::Index],
    relations: &[crabka_pgcatalog::RelationName],
    own_xid: Option<u64>,
    build: &IndexBuild<'_>,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    // A global index spans the whole relation already, so it has no
    // per-partition copy to make.
    let sources: Vec<&crabka_pgcatalog::Index> = sources
        .iter()
        .filter(|source| source.placement == crabka_pgcatalog::IndexPlacement::Local)
        .collect();
    if sources.is_empty() || relations.is_empty() {
        return Ok(Vec::new());
    }
    let mut ids = crabka_pgcatalog::IndexIds::default();
    let mut ops = Vec::new();
    for relation in relations {
        let table = crabka_pgcatalog::get_table(kv, relation)?;
        let mut present = crabka_pgcatalog::list_table_indexes(kv, relation)?;
        let mut taken: HashSet<String> = present.iter().map(|index| index.name.clone()).collect();
        for &source in &sources {
            if present
                .iter()
                .any(|candidate| matches_parent_index(candidate, source))
            {
                continue;
            }
            let base = cloned_index_base_name(relation, source);
            let name = available_index_name(kv, relation, &base, &taken);
            taken.insert(name.clone());
            let clone = cloned_partition_index(name, source);
            let index = crabka_pgcatalog::Index {
                id: ids.allocate(kv)?,
                name: clone.name,
                table: relation.clone(),
                table_id: table.id,
                columns: clone.columns,
                unique: clone.unique,
                placement: clone.placement,
                method: clone.method,
                constraint: clone.constraint,
                without_overlaps: clone.without_overlaps,
                clustered: false,
                deferral: clone.deferral,
            };
            ops.extend(crabka_pgcatalog::put_index_ops(&index));
            ops.extend(local_index_backfill_ops(
                kv, &table, &index, own_xid, build,
            )?);
            present.push(index);
        }
    }
    ops.extend(ids.commit_op());
    Ok(ops)
}

/// Resolve a written `PARTITION BY` clause into the stored partition key.
pub(crate) fn partition_scheme_from_ast(
    spec: &crabka_pgparser::ast::PartitionBy,
    columns: &[Column],
    indexes: &[crabka_pgcatalog::NewIndex],
) -> Result<crate::partition::Scheme, ExecError> {
    use crate::partition::Strategy;
    let strategy = match spec.strategy.as_str() {
        "range" => Strategy::Range,
        "list" => Strategy::List,
        "hash" => Strategy::Hash,
        other => {
            return Err(ExecError::UnrecognizedPartitionStrategy(other.to_string()));
        }
    };
    let keys = crate::partition::key_columns(strategy, &spec.keys, columns)?;
    for index in indexes {
        reject_incomplete_partitioned_key(&keys, &index.columns, index.constraint.as_ref())?;
    }
    Ok(crate::partition::Scheme { strategy, keys })
}

/// Refuse a key on a partitioned relation that leaves a partition-key column
/// out.
///
/// Nothing can enforce such a key. Two partitions never see each other's rows,
/// so the copy each partition carries proves only that the key is unique
/// *within* that partition — and a key that omits a partitioning column can put
/// the same value in two partitions. `PostgreSQL` refuses for the same reason,
/// and the refusal is what makes the per-partition copies sound.
pub(crate) fn reject_incomplete_partitioned_key(
    partition_keys: &[String],
    columns: &[String],
    constraint: Option<&crabka_pgcatalog::IndexConstraint>,
) -> Result<(), ExecError> {
    let Some(missing) = partition_keys.iter().find(|key| !columns.contains(key)) else {
        return Ok(());
    };
    let kind = match constraint {
        Some(crabka_pgcatalog::IndexConstraint::PrimaryKey) => "PRIMARY KEY",
        Some(crabka_pgcatalog::IndexConstraint::Exclusion(_)) => "EXCLUDE",
        _ => "UNIQUE",
    };
    Err(ExecError::Unsupported(format!(
        "unique constraint on partitioned table must include all partitioning columns: the \
         {kind} constraint lacks column \"{missing}\" which is part of the partition key"
    )))
}

/// Validate a written partition bound against its parent and resolve it into
/// the stored form, returning `(parent, bound)`.
pub(crate) fn partition_attachment(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
    spec: &crabka_pgparser::ast::PartitionOf,
    columns: &[Column],
    ctx: &crate::clock::EvalCtx,
) -> Result<(crabka_pgcatalog::RelationName, crate::partition::Bound), ExecError> {
    let resolution = ctx.resolution();
    let parent_name = resolve_relation(kv, resolution, &spec.parent, SchemaDisposition::Utility)?;
    let scheme = crate::partition::scheme_of(kv, &parent_name)?
        .ok_or_else(|| ExecError::NotPartitioned(parent_name.to_string()))?;
    let bound = resolve_partition_bound(&spec.bound, &scheme, columns, ctx)?;
    let siblings = crate::partition::partitions_of(kv, &parent_name)?;
    crate::partition::check_bound_shape(scheme.strategy, &bound)?;
    crate::partition::check_hash_bound(&bound)?;
    crate::partition::check_range_not_empty(name, &bound)?;
    crate::partition::check_no_overlap(scheme.strategy, name, &bound, &siblings)?;
    Ok((parent_name, bound))
}

/// Evaluate a written bound's constant expressions and coerce each to the type
/// of the partition-key column it bounds.
pub(crate) fn resolve_partition_bound(
    bound: &crabka_pgparser::ast::PartitionBound,
    scheme: &crate::partition::Scheme,
    columns: &[Column],
    ctx: &crate::clock::EvalCtx,
) -> Result<crate::partition::Bound, ExecError> {
    use crabka_pgparser::ast::{PartitionBound as Written, RangeBoundValue};

    use crate::partition::{Bound, RangeDatum};

    let key_type = |index: usize| -> Result<crabka_pgtypes::ColumnType, ExecError> {
        let key = scheme.keys.get(index).ok_or_else(|| {
            ExecError::InvalidTableDefinition(format!(
                "{} must specify exactly one value per partitioning column",
                if matches!(bound, Written::List(_)) {
                    "IN"
                } else {
                    "FROM"
                }
            ))
        })?;
        crate::partition::key_column_type(columns, key.as_str())
    };
    // A bound value is an assignment to the partition-key column it bounds, so
    // an unadorned `'…'` is resolved by that column's type — `FOR VALUES IN
    // ('[1,2)')` on an `int4range` key is a range bound, not a `text` one.
    let value = |expr: &Expr, index: usize| -> Result<Datum, ExecError> {
        check_partition_bound_expr(expr)?;
        let ty = key_type(index)?;
        let evaluated = eval_assignment_value(expr, ty, &Scope::empty(), &[], ctx)?;
        coerce(evaluated, ty, ctx)
    };

    match bound {
        Written::Default => Ok(Bound::Default),
        Written::List(values) => values
            .iter()
            .map(|expr| value(expr, 0))
            .collect::<Result<Vec<_>, _>>()
            .map(Bound::List),
        Written::Range { from, to } => {
            let side = |written: &[RangeBoundValue]| -> Result<Vec<RangeDatum>, ExecError> {
                if written.len() != scheme.keys.len() {
                    return Err(ExecError::InvalidTableDefinition(
                        "FROM must specify exactly one value per partitioning column".into(),
                    ));
                }
                written
                    .iter()
                    .enumerate()
                    .map(|(index, written)| match written {
                        RangeBoundValue::MinValue => Ok(RangeDatum::MinValue),
                        RangeBoundValue::MaxValue => Ok(RangeDatum::MaxValue),
                        RangeBoundValue::Value(expr) => value(expr, index).map(RangeDatum::Value),
                    })
                    .collect()
            };
            Ok(Bound::Range {
                from: side(from)?,
                to: side(to)?,
            })
        }
        Written::Hash { modulus, remainder } => Ok(Bound::Hash {
            modulus: *modulus,
            remainder: *remainder,
        }),
    }
}

/// `PostgreSQL` allows only an immutable constant expression in a partition
/// bound, and reports each disallowed construct with its own SQLSTATE.
pub(crate) fn check_partition_bound_expr(expr: &Expr) -> Result<(), ExecError> {
    match expr {
        Expr::Column { .. } => {
            return Err(ExecError::Unsupported(
                "cannot use column reference in partition bound expression".into(),
            ));
        }
        Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery { .. } => {
            return Err(ExecError::Unsupported(
                "cannot use subquery in partition bound".into(),
            ));
        }
        Expr::Func(call) if crate::agg::is_aggregate_call(call) => {
            return Err(ExecError::Grouping(
                "aggregate functions are not allowed in partition bound".into(),
            ));
        }
        _ => {}
    }
    for child in expr_children(expr) {
        check_partition_bound_expr(child)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgparser::ast::{FuncArgs, FuncCall};

    use super::*;

    fn call(name: &str) -> Expr {
        Expr::Func(FuncCall {
            sql_syntax: false,
            name: name.into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![Expr::IntLiteral("1".into())]),
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        })
    }

    #[test]
    fn partition_bounds_reject_aggregates_but_keep_scalar_constants() {
        assert!(check_partition_bound_expr(&call("sum")).is_err());
        assert!(check_partition_bound_expr(&call("abs")).is_ok());
        let subquery = crabka_pgparser::parser::parse_expression("(SELECT 1)")
            .expect("scalar subquery expression");
        assert!(check_partition_bound_expr(&subquery).is_err());
    }
}

/// Apply every table-level `[CONSTRAINT n] NOT NULL <column>` a `CREATE TABLE`
/// wrote to the finished column list.
///
/// `PostgreSQL` 17 gave the not-null a table-constraint spelling so that it
/// could carry a name and a `NO INHERIT` of its own. Crabka records neither: the
/// constraint is the column's flag, named after the column when `pg_constraint`
/// is read and copied to every child. So the name is dropped and `NO INHERIT` is
/// refused.
pub(crate) fn apply_table_not_null_constraints(
    columns: &mut [Column],
    constraints: &[crabka_pgparser::ast::TableConstraint],
    table: &crabka_pgcatalog::RelationName,
) -> Result<(), ExecError> {
    for constraint in constraints {
        let crabka_pgparser::ast::TableConstraintKind::NotNull { column, no_inherit } =
            &constraint.kind
        else {
            continue;
        };
        if *no_inherit {
            return Err(no_inherit_not_null_unsupported());
        }
        let target = columns
            .iter_mut()
            .find(|candidate| candidate.name == *column)
            .ok_or_else(|| ExecError::UndefinedTableColumn {
                column: column.clone(),
                table: table.to_string(),
            })?;
        target.not_null = true;
    }
    Ok(())
}

/// Build the catalog column list and `CHECK` list for a `CREATE TABLE`,
/// expanding any `LIKE` clauses first (they contribute columns ahead of the
/// explicitly written ones, in clause order, exactly like `PostgreSQL`).
pub(crate) fn create_table_definition(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
    columns: &[crabka_pgparser::ast::ColumnDef],
    constraints: &[crabka_pgparser::ast::TableConstraint],
    like: &[crabka_pgparser::ast::LikeClause],
    // Columns the relation gets from an `INHERITS` parent. They are not part of
    // the definition this returns — the caller merges them — but a `CHECK`
    // written here may name one, so validation has to see them.
    inherited: &[Column],
    ctx: &crate::clock::EvalCtx,
) -> Result<TableDefinition, ExecError> {
    let resolution = ctx.resolution();
    let mut cols: Vec<Column> = Vec::new();
    let mut checks: Vec<crabka_pgcatalog::CheckConstraint> = Vec::new();
    let mut sequences: Vec<(crabka_pgcatalog::RelationName, Sequence)> = Vec::new();
    let mut indexes: Vec<crabka_pgcatalog::NewIndex> = Vec::new();
    let mut foreign_keys: Vec<PendingForeignKey> = Vec::new();

    for clause in like {
        let source_name =
            &resolve_relation(kv, resolution, &clause.source, SchemaDisposition::Utility)?;
        let source = crabka_pgcatalog::get_table(kv, source_name)?;
        for column in &source.columns {
            let mut copied = column.clone();
            // NOT NULL always rides along; DEFAULT and IDENTITY only when asked.
            if !clause.includes(crabka_pgparser::ast::LikeOption::Defaults)
                && copied.identity.is_none()
            {
                copied.default = None;
            }
            if !clause.includes(crabka_pgparser::ast::LikeOption::Identity)
                && copied.identity.is_some()
            {
                copied.identity = None;
                copied.default = None;
            }
            cols.push(copied);
        }
        if clause.includes(crabka_pgparser::ast::LikeOption::Constraints) {
            for check in &source.checks {
                let taken: Vec<&str> = checks.iter().map(|c| c.name.as_str()).collect();
                let name = unique_constraint_name(&taken, &check.name);
                checks.push(crabka_pgcatalog::CheckConstraint {
                    name,
                    expr: check.expr.clone(),
                    validated: check.validated,
                });
            }
        }
        if clause.includes(crabka_pgparser::ast::LikeOption::Indexes) {
            for index in crabka_pgcatalog::list_table_indexes(kv, source_name)? {
                let Some(constraint) = index.constraint else {
                    continue;
                };
                indexes.push(crabka_pgcatalog::NewIndex {
                    name: constraint_index_name(
                        name,
                        &index.columns,
                        constraint == crabka_pgcatalog::IndexConstraint::PrimaryKey,
                    ),
                    columns: index.columns.clone(),
                    unique: true,
                    placement: crabka_pgcatalog::IndexPlacement::Local,
                    method: index.method,
                    constraint: Some(constraint),
                    without_overlaps: index.without_overlaps,
                    deferral: index.deferral,
                });
            }
        }
    }

    let primary_key_columns = create_table_primary_key_columns(columns, constraints);
    for column in columns {
        cols.push(column_from_ast(
            name,
            column,
            ctx,
            &mut sequences,
            &primary_key_columns,
        )?);
    }
    // A `CHECK`'s generated name is `<table>_<column>_check` when the predicate
    // references exactly one of the relation's columns, so the name depends on
    // the columns the relation *has* — inherited ones included.
    let column_names: Vec<String> = inherited
        .iter()
        .chain(&cols)
        .map(|c| c.name.clone())
        .collect();
    for column in columns {
        for constraint in &column.constraints {
            match &constraint.kind {
                crabka_pgparser::ast::ColumnConstraintKind::Check(predicate) => {
                    let taken = non_check_constraint_names(&indexes, &foreign_keys);
                    push_table_check(
                        &mut checks,
                        name,
                        constraint.name.as_deref(),
                        &predicate.text,
                        &column_names,
                        &taken,
                    )?;
                }
                crabka_pgparser::ast::ColumnConstraintKind::PrimaryKey => {
                    indexes.push(named_constraint_index(
                        constraint.name.as_deref(),
                        name,
                        std::slice::from_ref(&column.name),
                        true,
                        false,
                        constraint_deferral(constraint.attributes),
                    ));
                }
                crabka_pgparser::ast::ColumnConstraintKind::Unique { .. } => {
                    indexes.push(named_constraint_index(
                        constraint.name.as_deref(),
                        name,
                        std::slice::from_ref(&column.name),
                        false,
                        false,
                        constraint_deferral(constraint.attributes),
                    ));
                }
                // A column-level REFERENCES is a one-column FOREIGN KEY, named
                // and resolved exactly as the table-level spelling is.
                crabka_pgparser::ast::ColumnConstraintKind::References(reference) => {
                    push_pending_foreign_key(
                        &mut foreign_keys,
                        &checks,
                        &indexes,
                        name,
                        &AddForeignKey {
                            name: constraint.name.as_deref(),
                            columns: std::slice::from_ref(&column.name),
                            reference,
                            attributes: constraint.attributes,
                        },
                    )?;
                }
                _ => {}
            }
        }
    }
    for constraint in constraints {
        match &constraint.kind {
            // A table-level `NOT NULL c` may name a column this definition does
            // not declare — an inherited one, or a partition parent's. It is
            // applied by `apply_table_not_null_constraints` once the caller has
            // merged every source of columns together.
            crabka_pgparser::ast::TableConstraintKind::NotNull { .. } => {}
            crabka_pgparser::ast::TableConstraintKind::Check(predicate) => {
                let taken = non_check_constraint_names(&indexes, &foreign_keys);
                push_table_check(
                    &mut checks,
                    name,
                    constraint.name.as_deref(),
                    &predicate.text,
                    &column_names,
                    &taken,
                )?;
            }
            crabka_pgparser::ast::TableConstraintKind::PrimaryKey {
                columns: key,
                without_overlaps,
            } => {
                if *without_overlaps {
                    validate_without_overlaps_key(key, &cols)?;
                }
                indexes.push(named_constraint_index(
                    constraint.name.as_deref(),
                    name,
                    key,
                    true,
                    *without_overlaps,
                    constraint_deferral(constraint.attributes),
                ));
            }
            crabka_pgparser::ast::TableConstraintKind::Unique {
                columns: key,
                without_overlaps,
                ..
            } => {
                if *without_overlaps {
                    validate_without_overlaps_key(key, &cols)?;
                }
                indexes.push(named_constraint_index(
                    constraint.name.as_deref(),
                    name,
                    key,
                    false,
                    *without_overlaps,
                    constraint_deferral(constraint.attributes),
                ));
            }
            crabka_pgparser::ast::TableConstraintKind::ForeignKey {
                columns: key,
                period,
                references,
            } => {
                reject_temporal_foreign_key(*period, references.period)?;
                push_pending_foreign_key(
                    &mut foreign_keys,
                    &checks,
                    &indexes,
                    name,
                    &AddForeignKey {
                        name: constraint.name.as_deref(),
                        columns: key,
                        reference: references,
                        attributes: constraint.attributes,
                    },
                )?;
            }
            crabka_pgparser::ast::TableConstraintKind::Exclude { method, elements } => {
                indexes.push(exclusion_constraint_index(
                    constraint.name.as_deref(),
                    name,
                    &cols,
                    method,
                    elements,
                )?);
            }
        }
    }
    // Resolve every CHECK against the finished column list up front, so an
    // unknown column is a 42703 at DDL time rather than at the first INSERT.
    // Inherited columns count: `CREATE TABLE c (CHECK (a > 0)) INHERITS (p)`
    // constrains `p`'s column, which this relation declares nowhere.
    let mut visible = inherited.to_vec();
    for column in &cols {
        if !visible.iter().any(|item| item.name == column.name) {
            visible.push(column.clone());
        }
    }
    let table_for_validation = Table {
        id: 0,
        owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
        name: name.clone(),
        columns: visible,
        sharded: false,
        row_security: false,
        force_row_security: false,
        sharding: None,
        foreign: None,
        materialized: None,
        checks: checks.clone(),
    };
    for check in &checks {
        validate_check_predicate(&table_for_validation, &check.expr)?;
    }
    validate_generation_expressions(&table_for_validation)?;
    for index in &indexes {
        reject_index_over_virtual_generated(
            &table_for_validation,
            &index.columns,
            index.constraint.as_ref(),
        )?;
    }
    // PostgreSQL keeps one constraint namespace per relation, so a name shared
    // by constraints of *different* kinds is 42710. Two index-backed
    // constraints collide on the index name first, which the catalog reports as
    // 42P07 instead.
    for check in &checks {
        if indexes.iter().any(|index| index.name == check.name)
            || foreign_keys.iter().any(|fk| fk.name == check.name)
        {
            return Err(ExecError::DuplicateObject(format!(
                "constraint \"{}\" for relation \"{name}\" already exists",
                check.name
            )));
        }
    }
    Ok((cols, checks, sequences, indexes, foreign_keys))
}

/// One `FOREIGN KEY` clause a `CREATE TABLE` collected, with its name already
/// drawn from the relation's constraint namespace.
///
/// Resolution waits until the relation's own id and its indexes' ids exist,
/// because `CREATE TABLE t (… REFERENCES t …)` has to resolve against them and
/// no catalog read can find them yet.
pub(crate) struct PendingForeignKey {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) reference: crabka_pgparser::ast::ForeignKeyRef,
    pub(crate) attributes: crabka_pgparser::ast::ConstraintAttributes,
}

/// The constraint names a `CREATE TABLE` has assigned to things that are not
/// `CHECK`s. There is one namespace per relation, so a `CHECK` must step
/// around them.
pub(crate) fn non_check_constraint_names<'a>(
    indexes: &'a [crabka_pgcatalog::NewIndex],
    foreign_keys: &'a [PendingForeignKey],
) -> Vec<&'a str> {
    indexes
        .iter()
        .map(|index| index.name.as_str())
        .chain(foreign_keys.iter().map(|fk| fk.name.as_str()))
        .collect()
}

/// Collect one `FOREIGN KEY` clause, assigning the name `PostgreSQL` would:
/// the explicit `CONSTRAINT <name>` when written (42710 if the relation already
/// uses it for a constraint of any kind), else `<table>_<col>…_fkey` with the
/// lowest free numeric suffix.
pub(crate) fn push_pending_foreign_key(
    foreign_keys: &mut Vec<PendingForeignKey>,
    checks: &[crabka_pgcatalog::CheckConstraint],
    indexes: &[crabka_pgcatalog::NewIndex],
    table_name: &crabka_pgcatalog::RelationName,
    request: &AddForeignKey<'_>,
) -> Result<(), ExecError> {
    let taken: Vec<&str> = checks
        .iter()
        .map(|check| check.name.as_str())
        .chain(indexes.iter().map(|index| index.name.as_str()))
        .chain(foreign_keys.iter().map(|fk| fk.name.as_str()))
        .collect();
    let name = match request.name {
        Some(name) => {
            if taken.contains(&name) {
                return Err(ExecError::DuplicateObject(format!(
                    "constraint \"{name}\" for relation \"{table_name}\" already exists"
                )));
            }
            name.to_string()
        }
        None => unique_constraint_name(
            &taken,
            &crate::fk::default_foreign_key_name(table_name, request.columns),
        ),
    };
    foreign_keys.push(PendingForeignKey {
        name,
        columns: request.columns.to_vec(),
        reference: request.reference.clone(),
        attributes: request.attributes,
    });
    Ok(())
}

/// Append one `CREATE TABLE` `CHECK`, applying `PostgreSQL`'s naming rules: an
/// explicit `CONSTRAINT <name>` that collides with a name already assigned in
/// this same statement is 42710, while a generated name takes the lowest free
/// numeric suffix.
pub(crate) fn push_table_check(
    checks: &mut Vec<crabka_pgcatalog::CheckConstraint>,
    table_name: &crabka_pgcatalog::RelationName,
    explicit: Option<&str>,
    predicate: &str,
    column_names: &[String],
    other_names: &[&str],
) -> Result<(), ExecError> {
    let name = match explicit {
        Some(name) => {
            if checks.iter().any(|check| check.name == name) {
                return Err(ExecError::DuplicateObject(format!(
                    "check constraint \"{name}\" already exists"
                )));
            }
            if other_names.contains(&name) {
                return Err(ExecError::DuplicateObject(format!(
                    "constraint \"{name}\" for relation \"{table_name}\" already exists"
                )));
            }
            name.to_string()
        }
        None => {
            let mut taken: Vec<&str> = checks.iter().map(|check| check.name.as_str()).collect();
            taken.extend_from_slice(other_names);
            unique_constraint_name(
                &taken,
                &default_check_name(table_name, predicate, column_names),
            )
        }
    };
    checks.push(crabka_pgcatalog::CheckConstraint {
        name,
        expr: predicate.to_string(),
        validated: true,
    });
    Ok(())
}

/// Reject an index — plain, `UNIQUE`, `PRIMARY KEY` or `EXCLUDE` — over a
/// `VIRTUAL` generated column.
///
/// `PostgreSQL` 18 has no way to keep such an index in step: the value it would
/// key on is computed at read time from an expression the catalog can change
/// under it, and no row write announces that change. So it refuses the index,
/// wording the refusal after the constraint the index backs.
pub(crate) fn reject_index_over_virtual_generated(
    table: &Table,
    keys: &[String],
    constraint: Option<&crabka_pgcatalog::IndexConstraint>,
) -> Result<(), ExecError> {
    let over_virtual = keys.iter().any(|key| {
        table
            .column_index(key)
            .is_some_and(|index| table.columns[index].is_virtual_generated())
    });
    if !over_virtual {
        return Ok(());
    }
    Err(ExecError::Unsupported(
        match constraint {
            Some(crabka_pgcatalog::IndexConstraint::PrimaryKey) => {
                "primary keys on virtual generated columns are not supported"
            }
            Some(crabka_pgcatalog::IndexConstraint::Unique) => {
                "unique constraints on virtual generated columns are not supported"
            }
            Some(crabka_pgcatalog::IndexConstraint::Exclusion(_)) => {
                "exclusion constraints on virtual generated columns are not supported"
            }
            None => "indexes on virtual generated columns are not supported",
        }
        .into(),
    ))
}

/// Reject a `FOREIGN KEY` whose referencing side reads a generated column that
/// `PostgreSQL` 18 refuses, in upstream's order.
///
/// A referential action that WRITES the referencing columns cannot be applied
/// to a column the relation computes for itself, so `ON UPDATE CASCADE|SET
/// NULL|SET DEFAULT` and `ON DELETE SET NULL|SET DEFAULT` are 42601 over a
/// generated column of either kind. `ON DELETE CASCADE` removes the whole row
/// and writes nothing, so it stays legal.
///
/// A key over a `VIRTUAL` column is then 0A000 outright. The value never
/// reaches storage, so the key read out of a stored row is the NULL placeholder
/// every row of the column holds, and a NULL key satisfies every foreign key:
/// the constraint would read as protection and provide none. It is also
/// unbackable on the parent side, because
/// [`reject_index_over_virtual_generated`] keeps a virtual column out of every
/// index.
pub(crate) fn reject_foreign_key_over_generated(
    columns: &[Column],
    keys: &[String],
    reference: &crabka_pgparser::ast::ForeignKeyRef,
) -> Result<(), ExecError> {
    use crabka_pgparser::ast::ReferentialAction;

    for column in keys
        .iter()
        .filter_map(|key| columns.iter().find(|column| column.name == *key))
        .filter(|column| column.generated.is_some())
    {
        if matches!(
            reference.on_update,
            ReferentialAction::Cascade | ReferentialAction::SetNull | ReferentialAction::SetDefault
        ) {
            return Err(generated_column_action_refusal("ON UPDATE"));
        }
        if matches!(
            reference.on_delete,
            ReferentialAction::SetNull | ReferentialAction::SetDefault
        ) {
            return Err(generated_column_action_refusal("ON DELETE"));
        }
        if column.is_virtual_generated() {
            return Err(ExecError::Unsupported(
                "foreign key constraints on virtual generated columns are not supported".into(),
            ));
        }
    }
    Ok(())
}

/// The 42601 [`reject_foreign_key_over_generated`] raises, worded as
/// `PostgreSQL` words it: one message with the clause substituted in.
pub(crate) fn generated_column_action_refusal(clause: &str) -> ExecError {
    ExecError::Syntax(format!(
        "invalid {clause} action for foreign key constraint containing generated column"
    ))
}

/// Reject a `GENERATED ALWAYS AS (…)` expression `PostgreSQL` refuses at DDL
/// time, for either kind of generated column.
///
/// A generation expression may read only plain stored columns of the same row:
/// another generated column is 42P17 (`PostgreSQL` has no ordering guarantee
/// that would make it well-defined), a system column other than `tableoid` is
/// 42P17, and a subquery or aggregate is 0A000 / 42803.
pub(crate) fn validate_generation_expressions(table: &Table) -> Result<(), ExecError> {
    use crabka_pgparser::ast::Expr;

    let scope = Scope::single(table, &table.name.name);
    for column in &table.columns {
        let Some(source) = column.generation_expr() else {
            continue;
        };
        let expr = crabka_pgparser::parser::parse_expression(source)?;
        let mut rejection: Option<ExecError> = None;
        crate::grouping::visit_expr(&expr, &mut |node| {
            if rejection.is_some() {
                return;
            }
            rejection = match node {
                Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery { .. }
                | Expr::Quantified { .. } => Some(ExecError::Unsupported(
                    "cannot use subquery in column generation expression".into(),
                )),
                // Every system column but `tableoid` is off limits: the
                // expression is evaluated where the row's own MVCC header is
                // not yet decided (STORED) or no longer authoritative
                // (VIRTUAL), so reading one would be reading a value that has
                // not settled.
                Expr::Column { name, .. }
                    if crate::partition::is_generation_forbidden_system_column(name) =>
                {
                    Some(ExecError::InvalidObjectDefinition(format!(
                        "cannot use system column \"{name}\" in column generation expression"
                    )))
                }
                Expr::Column {
                    table: qualifier,
                    name,
                } => match scope.resolve(qualifier.as_deref(), name) {
                    Err(_) => Some(ExecError::UndefinedColumn(name.clone())),
                    Ok(index) if table.columns[index].generated.is_some() => {
                        Some(ExecError::InvalidObjectDefinition(format!(
                            "cannot use generated column \"{name}\" in column generation expression"
                        )))
                    }
                    Ok(_) => None,
                },
                // The expression has to be IMMUTABLE for either kind. A STORED
                // column would otherwise hold a value its own expression no
                // longer produces; a VIRTUAL one would report a different value
                // on every read of the same unchanged row.
                Expr::Func(call) if !is_immutable_function(&call.name) => {
                    Some(ExecError::InvalidObjectDefinition(
                        "generation expression is not immutable".into(),
                    ))
                }
                _ => None,
            };
        });
        if let Some(error) = rejection {
            return Err(error);
        }
        if crate::agg::contains_aggregate(&expr) {
            return Err(ExecError::Grouping(
                "aggregate functions are not allowed in column generation expressions".into(),
            ));
        }
    }
    Ok(())
}

/// Whether a built-in function name is `IMMUTABLE` in `PostgreSQL`'s sense.
///
/// Only the non-immutable built-ins crabka implements are listed: everything
/// else it can evaluate is a pure function of its arguments, so an unknown name
/// is immutable by default and the call is rejected elsewhere if it does not
/// exist at all.
pub(crate) fn is_immutable_function(name: &str) -> bool {
    !matches!(
        name,
        // The clock family — VOLATILE (`clock_timestamp`) or STABLE (the rest).
        "now"
            | "current_timestamp"
            | "transaction_timestamp"
            | "statement_timestamp"
            | "clock_timestamp"
            | "current_date"
            | "current_time"
            | "localtime"
            | "localtimestamp"
            | "timeofday"
            // Random sources.
            | "random"
            | "random_normal"
            | "gen_random_uuid"
            | "uuid_generate_v4"
            // Sequence access reads, and may advance, session state.
            | "nextval"
            | "currval"
            | "lastval"
            | "setval"
            // Session, database, and installation identity.
            | "current_user"
            | "session_user"
            | "user"
            | "current_role"
            | "current_catalog"
            | "current_database"
            | "current_schema"
            | "current_schemas"
            | "current_setting"
            | "set_config"
            | "version"
            | "pg_backend_pid"
            | "pg_notification_queue_usage"
            | "pg_postmaster_start_time"
            | "txid_current"
            | "pg_current_xact_id"
            // The rest of the transaction-id surface. Every one of these reads
            // live transaction state: the two snapshot constructors are
            // STABLE, and `pg_xact_status` is VOLATILE because a transaction
            // it reported in progress can commit under it.
            | "txid_current_if_assigned"
            | "pg_current_xact_id_if_assigned"
            | "txid_current_snapshot"
            | "pg_current_snapshot"
            | "txid_status"
            | "pg_xact_status"
            | "inet_client_addr"
            | "inet_server_addr"
    )
}

/// Compute a newly added `GENERATED ALWAYS AS (…) STORED` column's value for
/// every stored row version.
///
/// `PostgreSQL` rewrites the table when `ALTER TABLE … ADD COLUMN` carries a
/// generation expression, so the rows that already exist hold the computed
/// value rather than NULL. As in the `SET DATA TYPE` rewrite, a version no
/// snapshot can reach again must not be able to fail the statement.
pub(crate) fn backfill_generated_column(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    index: usize,
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    // A virtual column stores nothing, so there is nothing to fill in. Leaving
    // the rows alone is what makes a later `SET EXPRESSION` observable on rows
    // written before it.
    if state.table.columns[index].is_virtual_generated() {
        return Ok(());
    }
    let Some(source) = state.table.columns[index]
        .generation_expr()
        .map(str::to_owned)
    else {
        return Ok(());
    };
    let ty = state.table.columns[index].ty;
    let expr = crabka_pgparser::parser::parse_expression(&source)?;
    let table_name = state.table.name.clone();
    let scope = Scope::single(&state.table, &table_name.name);
    let computed = state
        .rows_mut(kv)?
        .iter()
        .map(|(_, xmin, xmax, row)| {
            let value =
                eval_assignment_value(&expr, ty, &scope, row, ctx).and_then(|v| coerce(v, ty, ctx));
            match value {
                Ok(value) => Ok(value),
                Err(error) => {
                    if version_is_settled_dead(kv, *xmin, *xmax)? {
                        Ok(Datum::Null)
                    } else {
                        Err(error)
                    }
                }
            }
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    for ((_, _, _, row), value) in state.rows_mut(kv)?.iter_mut().zip(computed) {
        if index < row.len() {
            row[index] = value;
        }
    }
    Ok(())
}

/// A foreign key on a partitioned relation, or on one of its partitions, is
/// refused for this wave.
///
/// [`crate::fk::resolve_foreign_key`] refuses a *sharded* relation itself, but
/// [`Table`] carries no partition flag, because the partition scheme lives in
/// its own metadata. So only the DDL caller can raise this.
pub(crate) fn reject_partitioned_foreign_key(constraint: &str) -> ExecError {
    ExecError::Unsupported(format!(
        "foreign key constraint \"{constraint}\" on a partitioned table is not supported"
    ))
}

/// Check the two `PERIOD` markers of a `FOREIGN KEY (…, PERIOD c) REFERENCES t
/// (…, PERIOD c)`, and refuse the temporal foreign key itself.
///
/// `PostgreSQL` insists the two sides agree before it looks at anything else,
/// and both of those 42830s are reproducible without the feature. The agreeing
/// case is what is not implemented: a temporal foreign key holds if the child's
/// range is *covered* by the union of the parent rows sharing its scalar key,
/// which is a containment test over an aggregate rather than the single key
/// probe every other foreign key resolves to.
pub(crate) fn reject_temporal_foreign_key(
    referencing: bool,
    referenced: bool,
) -> Result<(), ExecError> {
    match (referencing, referenced) {
        (false, false) => Ok(()),
        (true, false) => Err(ExecError::ForeignKeyPeriodMismatch {
            on_referencing: true,
        }),
        (false, true) => Err(ExecError::ForeignKeyPeriodMismatch {
            on_referencing: false,
        }),
        (true, true) => Err(ExecError::Unsupported(
            "foreign keys using PERIOD are not supported".into(),
        )),
    }
}

/// The [`crabka_pgcatalog::Index`] records an index batch allocated, read back
/// out of the batch itself.
///
/// A `CREATE TABLE` whose `FOREIGN KEY` references the relation being created
/// has to name the unique index proving its referenced columns are a key, and
/// that index exists only as a staged write until the statement commits. This
/// is where a caller can observe its allocated id. Values that are not index
/// records
/// (the next-id counter) simply fail to decode, and only the names this batch
/// asked for are kept.
pub(crate) fn staged_indexes_of(
    ops: &[crabka_pgkv::WriteOp],
    pending: &[crabka_pgcatalog::NewIndex],
) -> Vec<crabka_pgcatalog::Index> {
    let mut staged: Vec<crabka_pgcatalog::Index> = Vec::with_capacity(pending.len());
    for op in ops {
        let crabka_pgkv::WriteOp::Put { value, .. } = op else {
            continue;
        };
        let Ok(index) = crabka_pgcatalog::serde::deserialize_index(value) else {
            continue;
        };
        if pending.iter().any(|new| new.name == index.name)
            && !staged.iter().any(|seen| seen.name == index.name)
        {
            staged.push(index);
        }
    }
    staged
}

pub(crate) fn named_constraint_index(
    explicit: Option<&str>,
    table_name: &crabka_pgcatalog::RelationName,
    columns: &[String],
    primary_key: bool,
    without_overlaps: bool,
    deferral: crabka_pgcatalog::ConstraintDeferral,
) -> crabka_pgcatalog::NewIndex {
    let mut index =
        create_table_constraint_index(table_name, columns, primary_key, without_overlaps, deferral);
    if let Some(name) = explicit {
        index.name = name.to_string();
    }
    index
}

pub(crate) fn exclusion_constraint_index(
    explicit: Option<&str>,
    table_name: &crabka_pgcatalog::RelationName,
    table_columns: &[Column],
    method: &str,
    elements: &[crabka_pgparser::ast::ExclusionElement],
) -> Result<crabka_pgcatalog::NewIndex, ExecError> {
    if !method.eq_ignore_ascii_case("gist") {
        return Err(ExecError::Unsupported(format!(
            "exclusion constraints using access method \"{method}\" are not supported"
        )));
    }
    let mut columns = Vec::with_capacity(elements.len());
    let mut operators = Vec::with_capacity(elements.len());
    for element in elements {
        if !table_columns
            .iter()
            .any(|column| column.name == element.column)
        {
            return Err(ExecError::UndefinedColumn(element.column.clone()));
        }
        columns.push(element.column.clone());
        operators.push(match element.operator {
            crabka_pgparser::ast::BinaryOp::Eq => crabka_pgcatalog::ExclusionOperator::Equal,
            crabka_pgparser::ast::BinaryOp::Overlaps => {
                crabka_pgcatalog::ExclusionOperator::Overlaps
            }
            _ => unreachable!("parser accepts only exclusion operators the executor supports"),
        });
    }
    let name = explicit.map_or_else(
        || format!("{}_{}_excl", table_name.name, columns.join("_")),
        str::to_string,
    );
    Ok(crabka_pgcatalog::NewIndex {
        name,
        columns,
        unique: false,
        placement: crabka_pgcatalog::IndexPlacement::Local,
        method: crabka_pgcatalog::IndexMethod::Gist,
        constraint: Some(crabka_pgcatalog::IndexConstraint::Exclusion(operators)),
        without_overlaps: false,
        deferral: crabka_pgcatalog::ConstraintDeferral::Immediate,
    })
}

/// `PostgreSQL`'s default index name for a `PRIMARY KEY`/`UNIQUE` constraint.
///
/// It is built from the relation's own name, not its qualified spelling: the
/// index for `s1.t`'s primary key is `t_pkey`, sitting in `s1` beside the
/// table.
pub(crate) fn constraint_index_name(
    table_name: &crabka_pgcatalog::RelationName,
    columns: &[String],
    primary_key: bool,
) -> String {
    if primary_key {
        format!("{}_pkey", table_name.name)
    } else {
        format!("{}_{}_key", table_name.name, columns.join("_"))
    }
}

/// `PostgreSQL`'s default `CHECK` constraint name: `<table>_<column>_check` when
/// the predicate references exactly one of the table's columns, `<table>_check`
/// otherwise. The referenced set is taken from the predicate's identifier
/// tokens, so function names and literals never contribute.
pub(crate) fn default_check_name(
    table_name: &crabka_pgcatalog::RelationName,
    predicate: &str,
    columns: &[String],
) -> String {
    let mut referenced: Vec<&String> = Vec::new();
    if let Ok(tokens) = crabka_pgparser::lexer::lex(predicate) {
        for (token, _) in &tokens {
            let crabka_pgparser::token::Token::Ident(word) = token else {
                continue;
            };
            if let Some(column) = columns.iter().find(|name| *name == word)
                && !referenced.contains(&column)
            {
                referenced.push(column);
            }
        }
    }
    match referenced.as_slice() {
        [only] => format!("{}_{only}_check", table_name.name),
        _ => format!("{}_check", table_name.name),
    }
}

/// `PostgreSQL` disambiguates a colliding default constraint name by appending
/// the lowest free positive integer.
///
/// One relation has ONE constraint namespace, shared by `CHECK`s, index-backed
/// constraints and foreign keys alike, so `taken` is every constraint name of
/// every kind the relation already carries.
pub(crate) fn unique_constraint_name(taken: &[&str], base: &str) -> String {
    if !taken.contains(&base) {
        return base.to_string();
    }
    let mut suffix = 1u32;
    loop {
        let candidate = format!("{base}{suffix}");
        if !taken.iter().any(|name| *name == candidate) {
            return candidate;
        }
        suffix += 1;
    }
}

/// A table's `CHECK` predicates, re-parsed from their stored source text and
/// resolved against the table's columns.
pub(crate) struct CompiledCheck {
    pub(crate) name: String,
    pub(crate) expr: crabka_pgparser::ast::Expr,
}

/// Re-parse every stored `CHECK` predicate and verify it resolves against the
/// current column list. An unknown column surfaces as 42703 here.
pub(crate) fn compile_check_constraints(table: &Table) -> Result<Vec<CompiledCheck>, ExecError> {
    let scope = Scope::single(table, &table.name.name);
    table
        .checks
        .iter()
        .map(|check| {
            let expr = crabka_pgparser::parser::parse_expression(&check.expr)?;
            crate::eval::infer_type(&expr, &scope)?;
            Ok(CompiledCheck {
                name: check.name.clone(),
                expr,
            })
        })
        .collect()
}

/// Evaluate a table's `CHECK` predicates against one candidate row. A NULL
/// result passes, exactly like `PostgreSQL`'s three-valued rule.
pub(crate) fn enforce_check_constraints(
    table: &Table,
    checks: &[CompiledCheck],
    row: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    let scope = Scope::single(table, &table.name.name);
    for check in checks {
        let value = crate::eval::eval(&check.expr, &scope, row, ctx)?;
        if matches!(value, Datum::Bool(false)) {
            return Err(ExecError::CheckViolation {
                table: table.name.name.clone(),
                constraint: check.name.clone(),
            });
        }
    }
    Ok(())
}

/// Settle every `GENERATED ALWAYS AS (…)` column of one candidate row, in place.
///
/// A `STORED` column is computed here, because its value is what gets written.
/// A `VIRTUAL` column is *blanked* here, whatever the row arrived carrying — a
/// `BEFORE` trigger that assigned to it does not get to have that value — and
/// is computed later only where something actually reads it.
pub(crate) fn apply_generated_columns(
    table: &Table,
    row: &mut [Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    if table
        .columns
        .iter()
        .all(|column| column.generated.is_none())
    {
        return Ok(());
    }
    let scope = Scope::single(table, &table.name.name);
    let snapshot = row.to_vec();
    for (index, column) in table.columns.iter().enumerate() {
        if column.is_virtual_generated() {
            row[index] = Datum::Null;
            continue;
        }
        let Some(source) = column.generation_expr() else {
            continue;
        };
        let expr = crabka_pgparser::parser::parse_expression(source)?;
        let value = eval_assignment_value(&expr, column.ty, &scope, &snapshot, ctx)?;
        row[index] = coerce(value, column.ty, ctx)?;
    }
    Ok(())
}

/// Whether a write has to evaluate `table`'s virtual generated columns in order
/// to check the row it is about to store.
///
/// Only a `NOT NULL` on the column itself or a `CHECK` on the relation can
/// depend on one. When neither exists, the expression is never evaluated on the
/// write path at all — which is why `PostgreSQL` reports an expression that
/// overflows on the next *read* of the row rather than on the insert.
pub(crate) fn virtual_generated_needed_for_constraints(table: &Table) -> bool {
    has_virtual_generated(table)
        && (!table.checks.is_empty()
            || table
                .columns
                .iter()
                .any(|column| column.is_virtual_generated() && column.not_null))
}

/// Whether `table` has a column whose value is never written down.
pub(crate) fn has_virtual_generated(table: &Table) -> bool {
    table.columns.iter().any(Column::is_virtual_generated)
}

/// `row` as it is written to storage: every `VIRTUAL` generated column blanked
/// back to NULL.
///
/// A virtual generated column occupies no storage. The physical row keeps a
/// NULL placeholder at the column's position — the row stays full width, so
/// every positional consumer of a decoded row is unaffected — and readers
/// recompute the value from the expression the catalog holds *at read time*.
/// That is the whole observable difference from `STORED`: changing the
/// expression changes what rows written before the change report.
pub(crate) fn stored_row<'a>(table: &Table, row: &'a [Datum]) -> std::borrow::Cow<'a, [Datum]> {
    if !has_virtual_generated(table) {
        return std::borrow::Cow::Borrowed(row);
    }
    let mut stored = row.to_vec();
    for (index, _) in table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.is_virtual_generated())
    {
        if let Some(slot) = stored.get_mut(index) {
            *slot = Datum::Null;
        }
    }
    std::borrow::Cow::Owned(stored)
}

/// Encode one MVCC row version of `table` for storage.
///
/// Every write path in the executor goes through here rather than calling
/// [`crabka_pgmvcc::version::encode_tuple`] directly, so that "a `VIRTUAL`
/// generated column is not stored" holds by construction instead of by each
/// write path remembering to blank it. See [`stored_row`].
pub(crate) fn encode_table_tuple(table: &Table, xmin: u64, xmax: u64, row: &[Datum]) -> Vec<u8> {
    crabka_pgmvcc::version::encode_tuple(xmin, xmax, &stored_row(table, row))
}

/// Fill in the `VIRTUAL` generated columns of one row read back from storage,
/// where each sits as a NULL placeholder, that `reads` says the statement can
/// observe.
///
/// This is the read-side counterpart of [`encode_table_tuple`]: the value a
/// reader sees is produced here, from the catalog's *current* expression, and
/// never read off the disk.
///
/// A column `reads` excludes keeps its placeholder. That is not an optimization
/// — it is the behaviour `PostgreSQL` has, and the reason `DELETE … WHERE a =
/// 2000000000` removes a row whose `b int GENERATED ALWAYS AS (a * 2)`
/// overflows instead of raising 22003 for a column the statement never names.
/// See [`crate::scope::GeneratedReads`] for who may narrow it and who must not.
pub(crate) fn expand_virtual_generated_row(
    table: &Table,
    row: &mut [Datum],
    ctx: &crate::clock::EvalCtx,
    reads: crate::scope::GeneratedReads<'_>,
) -> Result<(), ExecError> {
    if !has_virtual_generated(table) {
        return Ok(());
    }
    let scope = Scope::single(table, &table.name.name);
    let snapshot = row.to_vec();
    for (index, column) in table.columns.iter().enumerate() {
        if !column.is_virtual_generated() {
            continue;
        }
        if !reads.reads(&column.name) {
            continue;
        }
        let Some(source) = column.generation_expr() else {
            continue;
        };
        // A row narrower than the catalog belongs to an in-flight DDL working
        // set, which fills the new column itself.
        if index >= row.len() {
            continue;
        }
        let expr = crabka_pgparser::parser::parse_expression(source)?;
        let value = eval_assignment_value(&expr, column.ty, &scope, &snapshot, ctx)?;
        row[index] = coerce(value, column.ty, ctx)?;
    }
    Ok(())
}

/// [`expand_virtual_generated_row`] over a batch of scanned rows.
pub(crate) fn expand_virtual_generated(
    table: &Table,
    rows: &mut [Vec<Datum>],
    ctx: &crate::clock::EvalCtx,
    reads: crate::scope::GeneratedReads<'_>,
) -> Result<(), ExecError> {
    if !has_virtual_generated(table) {
        return Ok(());
    }
    for row in rows {
        expand_virtual_generated_row(table, row, ctx, reads)?;
    }
    Ok(())
}

/// One decoded row version: its physical key, `xmin`, `xmax`, and column values.
pub(crate) type RowVersion = (Vec<u8>, u64, u64, Vec<Datum>);

/// One table's stored row versions, decoded so a schema change can rewrite them
/// positionally inside the DDL batch. DDL holds the global catalog lock, so no
/// concurrent writer can add a version between the read and the rewrite.
pub(crate) fn scan_all_row_versions(
    kv: &dyn Kv,
    table: &Table,
) -> Result<Vec<RowVersion>, ExecError> {
    kv.scan_prefix(&crabka_pgkv::key::table_prefix(table.id))?
        .into_iter()
        .map(|(key, bytes)| {
            let (xmin, xmax, row) = crabka_pgmvcc::version::decode_tuple(&bytes)?;
            Ok((key, xmin, xmax, row))
        })
        .collect()
}

/// The live rows among already-decoded row versions, as `(rowid, xmin, row)` —
/// the shape [`scan_live`] returns, but derived from an in-flight `ALTER
/// TABLE`'s working set instead of from storage.
///
/// `own_xid` is the open transaction's xid when the DDL runs inside one, and it
/// carries the same weight here as it does for a unique-index backfill: every
/// caller is back-validating a constraint against the rows the relation will
/// hold once the statement commits, and inside `BEGIN; INSERT …; ALTER TABLE …
/// ADD CONSTRAINT …` those rows include the ones this very transaction wrote
/// and has not committed. Without them the validation passes over a relation it
/// has only half seen, and the transaction commits rows its own constraint
/// forbids.
pub(crate) fn live_row_versions(
    kv: &dyn Kv,
    table: &Table,
    versions: &[RowVersion],
    own_xid: Option<u64>,
) -> Result<Vec<(u64, u64, Vec<Datum>)>, ExecError> {
    let snapshot = all_committed_snapshot();
    let status = global_status(kv, kv, &snapshot);
    let mut live: HashMap<u64, (u64, Vec<Datum>)> = HashMap::new();
    for (key, xmin, xmax, row) in versions {
        if !crabka_pgmvcc::visibility::satisfies_mvcc(*xmin, *xmax, &snapshot, own_xid, &status)? {
            continue;
        }
        let rowid = physical_rowid(table, crabka_pgmvcc::version::row_prefix_of(key)?)?;
        // The MVCC at-most-one-live invariant means the greatest-xmin live
        // version wins, exactly as `scan_live_interval` selects it.
        let slot = live.entry(rowid).or_insert_with(|| (*xmin, row.clone()));
        if slot.0 < *xmin {
            *slot = (*xmin, row.clone());
        }
    }
    let mut out: Vec<(u64, u64, Vec<Datum>)> = live
        .into_iter()
        .map(|(rowid, (xmin, row))| (rowid, xmin, row))
        .collect();
    out.sort_by_key(|(rowid, ..)| *rowid);
    Ok(out)
}

/// Whether a stored row version is settled dead: its inserting transaction
/// aborted, or its deleting one committed. No snapshot can ever see such a
/// version again, so a column rewrite may put anything in it. `PostgreSQL`'s
/// own table rewrite discards them outright.
///
/// Deliberately stricter than "invisible under an all-committed snapshot": a
/// version deleted by a still-running transaction is *not* settled, because
/// that transaction may yet abort and resurrect it.
pub(crate) fn version_is_settled_dead(
    kv: &dyn Kv,
    xmin: u64,
    xmax: u64,
) -> Result<bool, ExecError> {
    use crabka_pgmvcc::clog::XidStatus;

    let snapshot = all_committed_snapshot();
    let status = global_status(kv, kv, &snapshot);
    if status(xmin)? == XidStatus::Aborted {
        return Ok(true);
    }
    if xmax == 0 {
        return Ok(false);
    }
    Ok(status(xmax)? == XidStatus::Committed)
}
