//! INSERT source materialization and unknown-literal handling.

use super::*;

pub(crate) fn insert_source_rows(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    table: &Table,
    columns: &Option<Vec<String>>,
    indirections: &Option<Vec<Vec<TargetIndirection>>>,
    source: &crabka_pgparser::ast::InsertSource,
) -> Result<(Vec<usize>, Vec<Vec<Expr>>), ExecError> {
    use crabka_pgparser::ast::InsertSource;
    match source {
        InsertSource::Values(rows) => {
            // Rows of differing width are PostgreSQL's own 42601, raised before
            // the arity of the target list is even considered.
            let width = rows.first().map_or(0, Vec::len);
            if rows.iter().any(|row| row.len() != width) {
                return Err(ExecError::ValuesColumnCount);
            }
            Ok((
                resolve_insert_targets(table, columns, indirections, width)?,
                rows.clone(),
            ))
        }
        // Every column takes its default; an explicit column list is a syntax
        // error in PostgreSQL, so none can be present here.
        InsertSource::DefaultValues => Ok((Vec::new(), vec![Vec::new()])),
        InsertSource::Query(query) => {
            // Resolve the names first so an unknown column is 42703 before the
            // feeding query runs, as it is in PostgreSQL's parse analysis.
            resolve_insert_target_slots(table, columns, indirections)?;
            let read = write_ctx.read_ctx(ctes);
            let Relation { scope, rows } = crate::query::query_to_relation(&read, query)?;
            let target_idx = resolve_insert_targets(table, columns, indirections, scope.width())?;
            // A column the feeding query left `unknown` goes back to being the
            // literal it was written as, so `build_insert_row` types it against
            // the target column exactly as it types a VALUES row. The flags are
            // positional over the row, so they are used only when they span it:
            // a relation of another width is one this walk cannot index, and
            // typing the wrong column would be worse than typing none.
            let unknown = unknown_literal_columns(query);
            let unknown: &[bool] = if unknown.len() == scope.width() {
                &unknown
            } else {
                &[]
            };
            let rows = rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .zip(&scope.columns)
                        .enumerate()
                        .map(|(index, (value, column))| match value {
                            // Only a value the literal itself produced may go
                            // back to being one, which is why the datum is
                            // matched and not merely re-rendered.
                            Datum::Text(text) if unknown.get(index).copied().unwrap_or(false) => {
                                Expr::StringLiteral(text)
                            }
                            value => Expr::Const {
                                value,
                                ty: column.ty,
                            },
                        })
                        .collect()
                })
                .collect();
            Ok((target_idx, rows))
        }
    }
}

/// The output columns a query hands to an `INSERT` as PostgreSQL's `unknown`,
/// one flag per column of the query's own target list.
///
/// An `INSERT … SELECT` is the one place `PostgreSQL` declines to resolve a
/// query's unknown outputs to `text`. It analyses the feeding query with that
/// resolution switched off, and then takes every target entry that is still an
/// unknown constant *as the constant*, rather than as a reference to the
/// query's column. The literal therefore arrives at the target list untyped and
/// takes the target column's type, which is what makes `INSERT INTO point_tbl
/// SELECT '(0,0)'` a point where `SELECT '(0,0)'` on its own is text. Without
/// it, the two `INSERT` spellings of one row disagree: the `VALUES` form
/// resolves the literal and the `SELECT` form is 42804.
///
/// What is left out is left out because `PostgreSQL` has already had to choose
/// the column's type there, so nothing unknown survives to be re-typed:
///
/// * a set operation, whose column type is the common type of its branches and
///   is `text` when every branch is unknown — `… SELECT '(0,0)' UNION ALL
///   SELECT '(1,1)'` really is 42804;
/// * a sub-select, a derived table or a `VALUES` relation, each of which has
///   resolved its own unknown outputs to `text` at the boundary since
///   `PostgreSQL` 10 — so `… SELECT * FROM (VALUES ('(0,0)')) v` is 42804 too;
/// * anything that is not the bare literal: a cast, a function call, a column
///   reference or a `CASE` all carry a type of their own.
///
/// `ORDER BY`, `LIMIT`, `OFFSET` and a `WITH` prefix wrap a select list without
/// retyping it, so they are transparent and the list underneath still counts.
///
/// A `NULL` literal is unknown in `PostgreSQL` as well, and is deliberately not
/// reported here: [`coerce`] already stores a `Datum::Null` into a column of
/// any type, and sending it through [`resolve_unknown_literal`] instead would
/// hand the target's input function an empty string to parse.
///
/// The vector is empty when no column qualifies, which includes every select
/// list holding a `*`: a wildcard stands for as many columns as the relation
/// beneath it has, and no walk of the list alone can count them. A wildcard is
/// never unknown, so declining the whole list there costs nothing.
fn unknown_literal_columns(query: &crabka_pgparser::ast::QueryExpr) -> Vec<bool> {
    use crabka_pgparser::ast::{QueryBody, SelectItem, SetExpr};
    let SetExpr::Query(body) = &query.body else {
        return Vec::new();
    };
    match body {
        QueryBody::Nested(nested) => unknown_literal_columns(nested),
        QueryBody::Values(_) => Vec::new(),
        QueryBody::Select(select) => select
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::Expr { expr, .. } => Some(matches!(expr, Expr::StringLiteral(_))),
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => None,
            })
            .collect::<Option<Vec<bool>>>()
            .filter(|columns| columns.contains(&true))
            .unwrap_or_default(),
    }
}
