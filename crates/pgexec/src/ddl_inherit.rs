//! DDL and catalog code carved out of `exec`.

use super::{
    Column, ExecError, HashSet, Kv, SchemaDisposition, Statement, TableDefinition,
    create_table_definition, direct_children, inherit_wrong_kind, relation_kind, resolve_relation,
    stored_relation_kind,
};

pub(crate) fn inheritance_merge_notices(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    stmt: &Statement,
) -> Result<Vec<(String, bool)>, ExecError> {
    let Statement::CreateTable {
        inherits, columns, ..
    } = stmt
    else {
        return Ok(Vec::new());
    };
    let mut seen = std::collections::HashSet::new();
    let mut notices = Vec::new();
    for parent in inherits {
        let name = resolve_relation(kv, resolution, parent, SchemaDisposition::Reference)?;
        // A parent this cannot read is not reported here. The notices are
        // cosmetic and this runs ahead of the definition, so raising the lookup
        // failure would hand back `relation does not exist` for a name the
        // definition refuses by kind — which is how `INHERITS (<view>)` came to
        // claim the view was absent.
        let Ok(table) = crabka_pgcatalog::get_table(kv, &name) else {
            continue;
        };
        for column in table.columns {
            if !seen.insert(column.name.clone())
                && !notices.iter().any(|(name, _)| name == &column.name)
            {
                notices.push((column.name, true));
            }
        }
    }
    for column in columns {
        if seen.contains(&column.name) && !notices.iter().any(|(name, _)| name == &column.name) {
            notices.push((column.name.clone(), false));
        }
    }
    Ok(notices)
}

/// The `merging definition of column "c" for child "t"` notices an
/// `ALTER TABLE … ADD COLUMN` owes, as `(column, child)` pairs in the order
/// `PostgreSQL` raises them.
///
/// `ATExecAddColumn` recurses one level at a time, deliberately — the comment
/// in `tablecmds.c` says `find_all_inheritors` cannot be used here. So the walk
/// follows *edges*, not relations, and a child reachable by two paths is
/// arrived at twice. The second arrival finds the column already there, merges
/// into it, raises this notice and stops: everything below that child was
/// reached by the first arrival already.
///
/// A child that spelled the column out by hand is the same case one step
/// earlier — its *first* arrival already finds the column — so it takes the
/// notice too, and its own subtree is left alone.
///
/// This runs before the statement, off the committed catalog, which is what
/// lets it see the tree as `PostgreSQL`'s first arrival sees it. It reports
/// nothing it cannot read: an unreadable child is not a child the notice can
/// name, and the statement itself will report whatever is really wrong.
pub(crate) fn add_column_merge_notices(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    stmt: &Statement,
) -> Result<Vec<(String, String)>, ExecError> {
    use crabka_pgparser::ast::AlterTableAction as Action;

    let Statement::AlterTable {
        table,
        only: false,
        actions,
        ..
    } = stmt
    else {
        return Ok(Vec::new());
    };
    let Ok(root) = resolve_relation(kv, resolution, table, SchemaDisposition::Reference) else {
        return Ok(Vec::new());
    };
    let Ok(altered) = crabka_pgcatalog::get_table(kv, &root) else {
        return Ok(Vec::new());
    };
    let mut notices = Vec::new();
    for action in actions {
        let Action::AddColumn {
            if_not_exists,
            column,
        } = action
        else {
            continue;
        };
        // `ADD COLUMN IF NOT EXISTS` for a column the relation already has is
        // dropped whole, descendants included, so it merges into nothing.
        if *if_not_exists && altered.column_index(&column.name).is_some() {
            continue;
        }
        collect_merge_notices(kv, &root, &column.name, &mut notices)?;
    }
    Ok(notices)
}

/// Walk the tree below `root` edge by edge, recording the arrivals that find
/// the column already present.
///
/// Depth-first, because that is the order `ATExecAddColumn`'s recursion raises
/// the notices in: a child's whole subtree is walked before the next sibling.
/// The explicit stack keeps a deep inheritance chain off the call stack, and
/// `given` — the relations this walk has handed the column to — is what makes a
/// second arrival recognisable.
pub(crate) fn collect_merge_notices(
    kv: &dyn Kv,
    root: &crabka_pgcatalog::RelationName,
    column: &str,
    notices: &mut Vec<(String, String)>,
) -> Result<(), ExecError> {
    let mut given: HashSet<crabka_pgcatalog::RelationName> = HashSet::new();
    let mut pending = direct_children(kv, root)?;
    pending.reverse();
    while let Some(child) = pending.pop() {
        let present = given.contains(&child)
            || crabka_pgcatalog::get_table(kv, &child)
                .is_ok_and(|table| table.column_index(column).is_some());
        if present {
            notices.push((column.to_string(), child.name.clone()));
            continue;
        }
        given.insert(child.clone());
        let mut grandchildren = direct_children(kv, &child)?;
        grandchildren.reverse();
        pending.extend(grandchildren);
    }
    Ok(())
}

/// Build an inherited table by prepending each distinct parent column and
/// carrying inherited checks into the child's own catalog schema.
pub(crate) fn inherited_table_definition(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
    parents: &[crabka_pgcatalog::RelationName],
    columns: &[crabka_pgparser::ast::ColumnDef],
    constraints: &[crabka_pgparser::ast::TableConstraint],
    like: &[crabka_pgparser::ast::LikeClause],
    ctx: &crate::clock::EvalCtx,
) -> Result<TableDefinition, ExecError> {
    // The parents are read before the local definition, because a `CHECK`
    // written on the child may constrain a column the child inherits rather
    // than one it declares — `CREATE TABLE c (CHECK (a > 0)) INHERITS (p)` is
    // the whole point of the clause, and validating it against the local
    // columns alone reports the inherited column as undefined.
    let mut merged = Vec::<Column>::new();
    let mut inherited_checks = Vec::new();
    for parent_name in parents {
        let fetched = crabka_pgcatalog::get_table(kv, parent_name);
        // A materialized view arrives through the `Ok` side, so the fetched
        // record is asked for its kind rather than the lookup being trusted to
        // have answered only for tables.
        let kind = match &fetched {
            Ok(table) => Some(stored_relation_kind(table)),
            Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => {
                relation_kind(kv, parent_name)
            }
            Err(_) => None,
        };
        if let Some(kind) = kind
            && kind != "table"
            && kind != "foreign table"
        {
            return Err(inherit_wrong_kind(parent_name, kind));
        }
        let parent = fetched?;
        for column in parent.columns {
            if let Some(existing) = merged.iter_mut().find(|item| item.name == column.name) {
                if existing.ty != column.ty {
                    return Err(ExecError::InvalidTableDefinition(format!(
                        "inherited column \"{}\" has a type conflict",
                        column.name
                    )));
                }
                // Two parents that spell the same column with different
                // collations are as irreconcilable as two that spell it with
                // different types: the child would have to pick one, and
                // PostgreSQL refuses rather than choosing.
                if existing.collation != column.collation {
                    return Err(ExecError::InvalidTableDefinition(format!(
                        "inherited column \"{}\" has a collation conflict",
                        column.name
                    )));
                }
                existing.not_null |= column.not_null;
                if existing.default.is_none() {
                    existing.default = column.default;
                }
            } else {
                merged.push(column);
            }
        }
        inherited_checks.extend(parent.checks);
    }
    let (local_columns, mut checks, sequences, indexes, foreign_keys) =
        create_table_definition(kv, name, columns, constraints, like, &merged, ctx)?;
    for column in local_columns {
        if let Some(existing) = merged.iter_mut().find(|item| item.name == column.name) {
            if existing.ty != column.ty {
                return Err(ExecError::InvalidTableDefinition(format!(
                    "inherited column \"{}\" has a type conflict",
                    column.name
                )));
            }
            if existing.collation != column.collation {
                return Err(ExecError::InvalidTableDefinition(format!(
                    "inherited column \"{}\" has a collation conflict",
                    column.name
                )));
            }
            existing.not_null |= column.not_null;
            if existing.default.is_none() {
                existing.default = column.default;
            }
        } else {
            merged.push(column);
        }
    }
    inherited_checks.append(&mut checks);
    Ok((merged, inherited_checks, sequences, indexes, foreign_keys))
}
