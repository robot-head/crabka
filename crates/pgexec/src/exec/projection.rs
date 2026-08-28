use super::*;

/// Expand the projection list into output FieldDescriptions, the expressions
/// that produce each column, and each column's `ColumnType` (the third element
/// lets `select_to_relation` build a derived table's output scope without
/// re-inferring types).
type ResolvedProjection = (Vec<FieldDescription>, Vec<Expr>, Vec<ColumnType>);

/// `SELECT *` needs a relation to expand over, and `PostgreSQL` rejects it at
/// parse analysis when the query names none.
///
/// That is *not* the same as a `FROM` naming a relation with no columns left:
/// `ALTER TABLE … DROP COLUMN` down to zero columns leaves a legal relation, and
/// `SELECT *` over it yields rows of no columns rather than an error.
pub(crate) fn reject_from_less_wildcard(items: &[SelectItem]) -> Result<(), ExecError> {
    if items
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard))
    {
        return Err(ExecError::Syntax(
            "SELECT * with no tables specified is not valid".into(),
        ));
    }
    Ok(())
}

pub(crate) fn resolve_projection(
    items: &[SelectItem],
    scope: &Scope,
) -> Result<ResolvedProjection, ExecError> {
    // SP33: expand each item in turn so `*` spans every FROM table and `a.*`
    // expands one qualifier. Each `*`-expanded column carries its qualifier so a
    // multi-table `*` re-resolves unambiguously via `scope.resolve`.
    let mut fields = Vec::new();
    let mut exprs = Vec::new();
    let mut tys = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard => {
                // An empty scope here is a relation with no columns left, which
                // `*` legitimately expands to nothing. A query with no FROM at
                // all is rejected before it gets this far.
                //
                // The synthetic window-result, grouping-set and correlated
                // select-list bindings are not part of the relation, so `*`
                // never expands to them. Neither is a USING/NATURAL join's
                // retained input column: `SELECT * FROM ja JOIN jb USING (x)`
                // yields the merged `x` once, not `x`, `ja.x` and `jb.x`.
                for (index, c) in scope.columns.iter().enumerate().filter(|(_, c)| {
                    !is_window_binding(c)
                        && !crate::grouping::is_hidden_binding(c)
                        && !is_correlated_binding(c)
                        && !c.is_join_input()
                }) {
                    fields.push(field(&c.name, c.ty));
                    exprs.push(wildcard_reference(scope, index, c));
                    tys.push(c.ty);
                }
            }
            SelectItem::QualifiedWildcard(q) => {
                // Every column carrying the qualifier EXCEPT a system column.
                // Not `!is_join_input()`, which is what a bare `*` uses: `ja.*`
                // over a USING/NATURAL join is meant to yield that side's own
                // retained columns, and only `tableoid` has to drop out. Same
                // rule, same reason, as [`Scope::whole_row`] — `SELECT a.*` and
                // `SELECT a` are both "the columns of `a`", and `PostgreSQL`
                // counts a system column in neither.
                let cols: Vec<_> = scope
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| {
                        c.qualifier.as_deref() == Some(q)
                            && c.exposure != crate::scope::Exposure::SystemColumn
                    })
                    .collect();
                if cols.is_empty() {
                    return Err(ExecError::MissingFromEntry(q.clone()));
                }
                for (index, c) in cols {
                    fields.push(field(&c.name, c.ty));
                    exprs.push(wildcard_reference(scope, index, c));
                    tys.push(c.ty);
                }
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| derived_name(expr));
                // A set-returning function in the select list contributes its
                // single output column's type; everything else infers normally.
                let ty = crate::srf::projection_type(expr, scope)?;
                fields.push(field(&name, ty));
                exprs.push(expr.clone());
                tys.push(ty);
            }
        }
    }
    Ok((fields, exprs, tys))
}

/// How a `*` expansion refers to the scope column at `index`.
///
/// By name where that name resolves back to this very column, and positionally
/// where it does not. A relation whose column names repeat, such as
/// `ROWS FROM (f(), f())` or a multi-argument `unnest`, would otherwise expand
/// `*` into
/// references `PostgreSQL` itself would call ambiguous, even though `SELECT *`
/// is valid there and only a bare reference to the repeated name is `42702`.
fn wildcard_reference(scope: &Scope, index: usize, column: &ColumnBinding) -> Expr {
    if scope.resolve(column.qualifier.as_deref(), &column.name) == Ok(index) {
        return Expr::Column {
            table: column.qualifier.clone(),
            name: column.name.clone(),
        };
    }
    Expr::Column {
        table: Some(crate::scope::POSITION_QUALIFIER.to_string()),
        name: index.to_string(),
    }
}

/// The output scope of a projected relation: the field names with their types
/// and no base-table qualifier.
pub(crate) fn projected_scope(fields: &[FieldDescription], tys: &[ColumnType]) -> Scope {
    Scope {
        columns: fields
            .iter()
            .zip(tys)
            .map(|(f, ty)| ColumnBinding {
                exposure: Exposure::Output,
                qualifier: None, // a projected result has no base-table qualifier
                name: f.name.clone(),
                ty: *ty,
            })
            .collect(),
        ..Default::default()
    }
}

/// Is this binding one of the synthetic columns a window call's result occupies?
fn is_window_binding(c: &ColumnBinding) -> bool {
    c.qualifier.as_deref() == Some(crabka_pgparser::ast::WINDOW_QUALIFIER)
}

pub(crate) fn derived_name(expr: &Expr) -> String {
    named_expr(expr).0
}

/// `FigureColnameInternal`'s name AND its strength. The strength is what makes
/// a cast override a name that itself came from a cast but not one that came
/// from a column or a function: `'1'::text::int4` is `int4`, while
/// `b::text::int4` is still `b`.
fn named_expr(expr: &Expr) -> (String, u8) {
    let (name, strength) = named_expr_inner(expr);
    (name, strength)
}

fn named_expr_inner(expr: &Expr) -> (String, u8) {
    match expr {
        // A window placeholder carries the label PostgreSQL gives an unaliased
        // window call: the function's own name.
        Expr::Column { name, .. } => (
            crabka_pgparser::ast::window_binding_parts(name)
                .map_or_else(|| name.clone(), |(_, label)| label.to_string()),
            2,
        ),
        // PostgreSQL names an aggregate output column after the function.
        Expr::Func(fc) => (fc.name.clone(), 2),
        // A SQL/JSON expression is labelled after its construct (`json_object`,
        // `json_value`, …); `IS JSON` is a predicate and stays `?column?`.
        Expr::SqlJson(json) => (json.output_label().to_string(), 2),
        // PostgreSQL's `FigureColname` looks THROUGH a cast and a COLLATE (and
        // through the parentheses the parser has already discarded), so
        // `b::numeric`, `count(*)::bigint`, `b COLLATE "C"` and `(b)` are labelled
        // `b`, `count`, `b`, `b`. When the inner expression supplies no name of
        // its own, a CAST falls back to the catalog TYPE name — `1::bigint` is
        // `int8`, not `?column?` — while a COLLATE has no such fallback.
        Expr::Cast { expr, ty } => match named_expr(expr) {
            // Strength 1 is a name a cast (or another weak form) supplied, and
            // this cast replaces it; strength 2 came from a column or a
            // function and survives.
            (_, strength) if strength <= 1 => (catalog_type_name(*ty), 1),
            named => named,
        },
        Expr::Collate { expr, .. } => named_expr(expr),
        // A field selector names the selected field, while array subscripts
        // preserve the name of their base expression.
        Expr::FieldSelect { field, .. } => (field.clone(), 2),
        Expr::Subscript { base, .. } | Expr::ArrayRef { base, .. } => named_expr(base),
        // PostgreSQL uses these construct names rather than the generic
        // `?column?` fallback.
        Expr::ArrayLiteral(_) | Expr::ArraySubquery(_) => ("array".to_string(), 2),
        Expr::Row(_) => ("row".to_string(), 2),
        Expr::Exists(_) => ("exists".to_string(), 2),
        Expr::ScalarSubquery(query) => {
            scalar_subquery_name(query).unwrap_or_else(|| ("?column?".to_string(), 0))
        }
        // A CASE borrows a strong ELSE label, otherwise its own weak `case`
        // label lets a surrounding cast replace it.
        Expr::Case { else_result, .. } => {
            let named = else_result
                .as_deref()
                .map_or_else(|| ("case".to_string(), 0), named_expr);
            if named.1 <= 1 {
                ("case".to_string(), 1)
            } else {
                named
            }
        }
        _ => ("?column?".to_string(), 0),
    }
}

/// The first output name of a scalar subquery.  A scalar subquery has one
/// column, and set operations retain their left input's output name.
fn scalar_subquery_name(query: &crabka_pgparser::ast::QueryExpr) -> Option<(String, u8)> {
    scalar_subquery_set_expr_name(&query.body)
}

fn scalar_subquery_set_expr_name(set_expr: &crabka_pgparser::ast::SetExpr) -> Option<(String, u8)> {
    use crabka_pgparser::ast::{QueryBody, SetExpr};

    match set_expr {
        SetExpr::SetOp { left, .. } => scalar_subquery_set_expr_name(left),
        SetExpr::Query(QueryBody::Nested(query)) => scalar_subquery_name(query),
        SetExpr::Query(QueryBody::Values(_)) => Some(("column1".to_string(), 2)),
        SetExpr::Query(QueryBody::Select(select)) => match select.projection.first()? {
            SelectItem::Expr { expr, alias } => Some(
                alias
                    .clone()
                    .map_or_else(|| named_expr(expr), |name| (name, 2)),
            ),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => None,
        },
    }
}

/// `FigureColname` sees the canonical type name produced by PostgreSQL's type
/// parser, which is `pg_type.typname` rather than an SQL alias (`boolean`
/// becomes `bool`, `bigint` becomes `int8`, and so on).
fn catalog_type_name(ty: ColumnType) -> String {
    if matches!(ty, ColumnType::Array(_)) {
        return ty.name().trim_end_matches("[]").to_string();
    }
    builtin_type_rows()
        .iter()
        .find(|row| u32::try_from(row.oid) == Ok(ty.oid()))
        .map_or_else(|| ty.name().to_string(), |row| row.name.to_string())
}
