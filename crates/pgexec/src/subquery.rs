//! SP34: uncorrelated subquery resolution (scalar / IN / EXISTS / ANY-ALL).
//!
//! An uncorrelated subquery's result is identical for every outer row, so it is
//! evaluated ONCE here — before the outer row loop — and the node is rewritten into
//! already-supported nodes (`Expr::Const`; an `InList` of consts; an `OR`/`AND`
//! fold of comparisons). The pure `eval`/`agg` evaluators then run unchanged over
//! the rewritten tree. Subqueries run through the SP33 join read path
//! (`exec::select_to_relation`) under the SAME snapshot handles as the outer query,
//! so the read is consistent and read-your-writes holds. Correlation (a reference
//! to an outer column) is out of scope: a subquery's scope is built solely from its
//! own FROM, so an outer-column reference fails name resolution (42703).

use crabka_pgparser::ast::{
    BinaryOp, Expr, FuncArgs, FuncCall, OrderItem, QueryExpr, SelectItem, SelectStmt, ValuesStmt,
};
use crabka_pgtypes::{ColumnType, Datum, ElemType};

use crate::error::ExecError;

/// The read-side handles a subquery needs to execute (mirrors `execute_read`'s
/// parameters). Threaded through the resolution recursion so each nested subquery
/// reads under the outer query's snapshot.
#[derive(Clone, Copy)]
pub(crate) struct SubCtx<'a> {
    pub catalog_kv: &'a dyn crabka_pgkv::Kv,
    pub kv: &'a dyn crabka_pgkv::Kv,
    pub global: &'a dyn crabka_pgkv::Kv,
    pub gsnap: &'a crabka_pgmvcc::visibility::Snapshot,
    pub snapshot: &'a crabka_pgmvcc::visibility::Snapshot,
    pub own: Option<u64>,
    pub ctes: &'a crate::cte::CteContext,
    /// The session eval context (zone + clock) — forwarded to the subquery's read
    /// path so a temporal expression inside a subquery evaluates in the session zone.
    pub eval_ctx: &'a crate::clock::EvalCtx,
    /// SP40: the foreign-table read context (scanner + current user) — forwarded so a
    /// subquery referencing a foreign table reads through the registered scanner.
    pub fctx: crate::exec::ForeignCtx<'a>,
    /// G-8: ordinary table scanner seam forwarded through nested subqueries.
    pub range_scanner: &'a dyn crate::scanner::RangeScanner,
    /// Memory retained by one blocking query operator.
    pub blocking_query_memory: crabka_units::ByteSize,
}

impl<'a> SubCtx<'a> {
    pub(crate) fn with_ctes<'b>(&'b self, ctes: &'b crate::cte::CteContext) -> SubCtx<'b>
    where
        'a: 'b,
    {
        SubCtx {
            catalog_kv: self.catalog_kv,
            kv: self.kv,
            global: self.global,
            gsnap: self.gsnap,
            snapshot: self.snapshot,
            own: self.own,
            ctes,
            eval_ctx: self.eval_ctx,
            fctx: self.fctx,
            range_scanner: self.range_scanner,
            blocking_query_memory: self.blocking_query_memory,
        }
    }
}

/// Rewrite every uncorrelated subquery in `s`'s expr-bearing clauses to a resolved
/// constant form. The FROM clause (base tables, joins, derived tables) is left to
/// the SP33 join read path; only expression positions are rewritten here.
pub(crate) fn resolve_in_select(ctx: &SubCtx, s: &SelectStmt) -> Result<SelectStmt, ExecError> {
    let mut out = s.clone();
    for item in &mut out.projection {
        if let SelectItem::Expr { expr, alias } = item {
            // P2: an unaliased routine call keeps the function's name as its
            // output label even though inlining replaces the call node.
            let label = crate::routine::call_label(expr);
            *expr = resolve_expr(ctx, expr)?;
            if alias.is_none() && !matches!(expr, Expr::Func(_)) {
                *alias = label;
            }
        }
    }
    if let Some(f) = &mut out.filter {
        *f = resolve_expr(ctx, f)?;
    }
    if let Some(h) = &mut out.having {
        *h = resolve_expr(ctx, h)?;
    }
    for g in &mut out.group_by {
        *g = resolve_expr(ctx, g)?;
    }
    for o in &mut out.order_by {
        o.expr = resolve_expr(ctx, &o.expr)?;
    }
    if let crabka_pgparser::ast::DistinctClause::On(on) = &mut out.distinct {
        for expr in on {
            *expr = resolve_expr(ctx, expr)?;
        }
    }
    // LIMIT/OFFSET take arbitrary expressions, including a scalar subquery, and
    // are evaluated once against no input row.
    for expr in out.limit.iter_mut().chain(out.offset.iter_mut()) {
        *expr = resolve_expr(ctx, expr)?;
    }
    // A window call's arguments, FILTER and window specification live beside the
    // expression tree rather than in it, so they need resolving too.
    for call in &mut out.window_calls {
        if let crabka_pgparser::ast::FuncArgs::Exprs(args) = &mut call.args {
            for arg in args {
                *arg = resolve_expr(ctx, arg)?;
            }
        }
        if let Some(filter) = &mut call.filter {
            *filter = resolve_expr(ctx, filter)?;
        }
        if let crabka_pgparser::ast::WindowRef::Spec(spec) = &mut call.over {
            for expr in &mut spec.partition_by {
                *expr = resolve_expr(ctx, expr)?;
            }
            for item in &mut spec.order_by {
                item.expr = resolve_expr(ctx, &item.expr)?;
            }
        }
    }
    for window in &mut out.windows {
        for expr in &mut window.spec.partition_by {
            *expr = resolve_expr(ctx, expr)?;
        }
        for item in &mut window.spec.order_by {
            item.expr = resolve_expr(ctx, &item.expr)?;
        }
    }
    Ok(out)
}

/// Rewrite subqueries in a query expression's `LIMIT`/`OFFSET` expressions,
/// which the non-SELECT bodies apply outside `resolve_in_select`.
pub(crate) fn resolve_row_counts(
    ctx: &SubCtx,
    q: &QueryExpr,
) -> Result<(Option<Expr>, Option<Expr>), ExecError> {
    let resolve = |expr: &Option<Expr>| -> Result<Option<Expr>, ExecError> {
        expr.as_ref().map(|e| resolve_expr(ctx, e)).transpose()
    };
    Ok((resolve(&q.limit)?, resolve(&q.offset)?))
}

/// Rewrite subqueries in query-expression ORDER BY tail items. Non-SELECT query
/// bodies apply these tails outside `resolve_in_select`, but they need the same
/// snapshot-consistent subquery folding before scalar `eval`.
pub(crate) fn resolve_order_items(
    ctx: &SubCtx,
    order_by: &[OrderItem],
) -> Result<Vec<OrderItem>, ExecError> {
    order_by
        .iter()
        .map(|item| {
            Ok(OrderItem {
                expr: resolve_expr(ctx, &item.expr)?,
                asc: item.asc,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}

pub(crate) fn resolve_in_values(ctx: &SubCtx, v: &ValuesStmt) -> Result<ValuesStmt, ExecError> {
    Ok(ValuesStmt {
        rows: v
            .rows
            .iter()
            .map(|row| row.iter().map(|expr| resolve_expr(ctx, expr)).collect())
            .collect::<Result<_, _>>()?,
    })
}

/// [`resolve_expr`] over one subscript-chain entry's bound expressions.
fn resolve_subscript(
    ctx: &SubCtx,
    subscript: &crabka_pgparser::ast::ArraySubscript,
) -> Result<crabka_pgparser::ast::ArraySubscript, ExecError> {
    use crabka_pgparser::ast::ArraySubscript;

    Ok(match subscript {
        ArraySubscript::Index(index) => ArraySubscript::Index(resolve_expr(ctx, index)?),
        ArraySubscript::Slice { lower, upper } => {
            let bound = |e: &Option<Expr>| e.as_ref().map(|e| resolve_expr(ctx, e)).transpose();
            ArraySubscript::Slice {
                lower: bound(lower)?,
                upper: bound(upper)?,
            }
        }
    })
}

/// Recursively rewrite subquery nodes in `e`, bottom-up.
pub(crate) fn resolve_expr(ctx: &SubCtx, e: &Expr) -> Result<Expr, ExecError> {
    Ok(match e {
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Default
        | Expr::Const { .. } => e.clone(),
        Expr::FieldSelect { base, field } => Expr::FieldSelect {
            base: Box::new(resolve_expr(ctx, base)?),
            field: field.clone(),
        },
        Expr::FieldSelectAll(base) => Expr::FieldSelectAll(Box::new(resolve_expr(ctx, base)?)),
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: Box::new(resolve_expr(ctx, expr)?),
        },
        Expr::Collate { expr, collation } => Expr::Collate {
            expr: Box::new(resolve_expr(ctx, expr)?),
            collation: collation.clone(),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(resolve_expr(ctx, left)?),
            right: Box::new(resolve_expr(ctx, right)?),
        },
        Expr::Func(fc) => {
            let call = FuncCall {
                name: fc.name.clone(),
                distinct: fc.distinct,
                args: match &fc.args {
                    FuncArgs::Star => FuncArgs::Star,
                    FuncArgs::Exprs(args) => FuncArgs::Exprs(
                        args.iter()
                            .map(|a| resolve_expr(ctx, a))
                            .collect::<Result<_, _>>()?,
                    ),
                },
                // The FILTER predicate resolves like an argument; dropping it here
                // would silently turn a filtered aggregate into an unfiltered one.
                filter: match &fc.filter {
                    Some(predicate) => Some(Box::new(resolve_expr(ctx, predicate)?)),
                    None => None,
                },
            };
            // P2: a call of a user-defined SQL function is inlined here, the one
            // point in the rewrite where the routine catalog is reachable.
            match crate::routine::inline_scalar(ctx.catalog_kv, &call)? {
                // The inlined body may itself call a routine, and may have
                // become a scalar subquery, so it goes back through this pass.
                Some(inlined) => {
                    let _guard = crate::routine::enter_inline()?;
                    resolve_expr(ctx, &inlined)?
                }
                None => Expr::Func(call),
            }
        }
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(resolve_expr(ctx, expr)?),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(resolve_expr(ctx, expr)?),
            list: list
                .iter()
                .map(|x| resolve_expr(ctx, x))
                .collect::<Result<_, _>>()?,
            negated: *negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(resolve_expr(ctx, expr)?),
            low: Box::new(resolve_expr(ctx, low)?),
            high: Box::new(resolve_expr(ctx, high)?),
            negated: *negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            kind,
            escape,
        } => Expr::Like {
            expr: Box::new(resolve_expr(ctx, expr)?),
            pattern: Box::new(resolve_expr(ctx, pattern)?),
            negated: *negated,
            kind: *kind,
            escape: match escape {
                Some(e) => Some(Box::new(resolve_expr(ctx, e)?)),
                None => None,
            },
        },
        Expr::Case {
            operand,
            whens,
            else_result,
        } => Expr::Case {
            operand: match operand {
                Some(o) => Some(Box::new(resolve_expr(ctx, o)?)),
                None => None,
            },
            whens: whens
                .iter()
                .map(|(c, r)| Ok((resolve_expr(ctx, c)?, resolve_expr(ctx, r)?)))
                .collect::<Result<_, ExecError>>()?,
            else_result: match else_result {
                Some(o) => Some(Box::new(resolve_expr(ctx, o)?)),
                None => None,
            },
        },
        Expr::Cast { expr, ty } => Expr::Cast {
            expr: Box::new(resolve_expr(ctx, expr)?),
            ty: *ty,
        },
        Expr::SqlJson(json) => Expr::SqlJson(Box::new(
            json.map_children(|child| resolve_expr(ctx, child))?,
        )),
        // The array expression forms carry ordinary child expressions, any of
        // which may contain a subquery (`ARRAY[(SELECT …)]`, `arr[(SELECT …)]`,
        // `x = ANY((SELECT …))`), so they recurse like every other node — the
        // node itself is not a subquery and is rebuilt unchanged.
        Expr::ArrayLiteral(items) => Expr::ArrayLiteral(
            items
                .iter()
                .map(|item| resolve_expr(ctx, item))
                .collect::<Result<_, _>>()?,
        ),
        Expr::Row(items) => Expr::Row(
            items
                .iter()
                .map(|item| resolve_expr(ctx, item))
                .collect::<Result<_, _>>()?,
        ),
        Expr::Subscript { base, index } => Expr::Subscript {
            base: Box::new(resolve_expr(ctx, base)?),
            index: Box::new(resolve_expr(ctx, index)?),
        },
        Expr::ArrayRef { base, subscripts } => Expr::ArrayRef {
            base: Box::new(resolve_expr(ctx, base)?),
            subscripts: subscripts
                .iter()
                .map(|s| resolve_subscript(ctx, s))
                .collect::<Result<_, _>>()?,
        },
        Expr::QuantifiedArray {
            expr,
            op,
            all,
            array,
        } => Expr::QuantifiedArray {
            expr: Box::new(resolve_expr(ctx, expr)?),
            op: *op,
            all: *all,
            array: Box::new(resolve_expr(ctx, array)?),
        },
        // ---- the subquery nodes: run once, fold to constants ----
        Expr::ScalarSubquery(s) => {
            let (value, ty) = run_scalar(ctx, s)?;
            Expr::Const { value, ty }
        }
        // `ARRAY(subquery)` folds to the array of the subquery's one column, in
        // the order the subquery produced its rows.
        Expr::ArraySubquery(s) => {
            let (ty, values) = run_single_column(ctx, s)?;
            let elem = ElemType::from_column_type(ty).ok_or_else(|| {
                ExecError::Unsupported(format!("arrays of {} are not supported", ty.name()))
            })?;
            Expr::Const {
                value: crate::array_fn::array_from_rows(elem, values),
                ty: ColumnType::Array(elem),
            }
        }
        Expr::Exists(s) => {
            let rows = run_rows(ctx, s)?;
            Expr::Const {
                value: Datum::Bool(!rows.is_empty()),
                ty: ColumnType::Bool,
            }
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let (ty, values) = run_single_column(ctx, subquery)?;
            Expr::InList {
                expr: Box::new(resolve_expr(ctx, expr)?),
                list: values
                    .into_iter()
                    .map(|value| Expr::Const { value, ty })
                    .collect(),
                negated: *negated,
            }
        }
        Expr::Quantified {
            expr,
            op,
            all,
            subquery,
        } => {
            let (ty, values) = run_single_column(ctx, subquery)?;
            let lhs = resolve_expr(ctx, expr)?;
            lower_quantified(&lhs, *op, *all, ty, values)
        }
    })
}

/// Reject a `FOR UPDATE/SHARE` inside a subquery (meaningless for a folded read).
fn no_locking(q: &QueryExpr) -> Result<(), ExecError> {
    if q.locking.is_some() {
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE is not allowed inside a subquery".into(),
        ));
    }
    Ok(())
}

/// Run a subquery through the join read path to its materialized rows.
fn run_relation(ctx: &SubCtx, q: &QueryExpr) -> Result<crate::join::Relation, ExecError> {
    no_locking(q)?;
    crate::query::query_to_relation_with_ctes(ctx, q)
}

/// Run a subquery to its raw rows (any shape) — used by `EXISTS`.
fn run_rows(ctx: &SubCtx, q: &QueryExpr) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(run_relation(ctx, q)?.rows)
}

/// Run a scalar subquery: exactly one column, at most one row → `(value, type)`.
fn run_scalar(ctx: &SubCtx, q: &QueryExpr) -> Result<(Datum, ColumnType), ExecError> {
    let rel = run_relation(ctx, q)?;
    if rel.scope.width() != 1 {
        return Err(ExecError::SubqueryColumns);
    }
    let ty = rel.scope.ty_at(0);
    if rel.rows.len() > 1 {
        return Err(ExecError::CardinalityViolation);
    }
    let value = rel
        .rows
        .into_iter()
        .next()
        .map(|mut r| r.remove(0))
        .unwrap_or(Datum::Null);
    Ok((value, ty))
}

/// Run a single-column subquery → its column type + every value (in row order).
fn run_single_column(ctx: &SubCtx, q: &QueryExpr) -> Result<(ColumnType, Vec<Datum>), ExecError> {
    let rel = run_relation(ctx, q)?;
    if rel.scope.width() != 1 {
        return Err(ExecError::SubqueryColumns);
    }
    let ty = rel.scope.ty_at(0);
    let col = rel.rows.into_iter().map(|mut r| r.remove(0)).collect();
    Ok((ty, col))
}

/// Lower `lhs op ANY|SOME|ALL (values)` to an `OR`/`AND` fold of comparisons, with
/// PostgreSQL's empty-set semantics (ANY → false, ALL → true). NULL three-valued
/// logic falls out of the existing `ops::or`/`ops::and`/`ops::compare`.
fn lower_quantified(
    lhs: &Expr,
    op: BinaryOp,
    all: bool,
    ty: ColumnType,
    values: Vec<Datum>,
) -> Expr {
    if values.is_empty() {
        return Expr::Const {
            value: Datum::Bool(all),
            ty: ColumnType::Bool,
        };
    }
    let join = if all { BinaryOp::And } else { BinaryOp::Or };
    let mut acc: Option<Expr> = None;
    for v in values {
        let cmp = Expr::Binary {
            op,
            left: Box::new(lhs.clone()),
            right: Box::new(Expr::Const { value: v, ty }),
        };
        acc = Some(match acc {
            None => cmp,
            Some(prev) => Expr::Binary {
                op: join,
                left: Box::new(prev),
                right: Box::new(cmp),
            },
        });
    }
    acc.expect("non-empty values")
}

// ---- describe (extended-protocol, no execution): catalog-only type pass ----

/// Execution-time type pass for contexts that already materialized CTEs. This is
/// still schema-only, but scalar subqueries can resolve FROM entries against the
/// supplied CTE context instead of catalog tables only.
pub(crate) fn resolve_types_in_projection_with_ctes(
    catalog_kv: &dyn crabka_pgkv::Kv,
    resolution: &crate::relname::ResolutionScope,
    items: &[SelectItem],
    ctes: &crate::cte::CteContext,
) -> Result<Vec<SelectItem>, ExecError> {
    items
        .iter()
        .map(|it| match it {
            SelectItem::Expr { expr, alias } => {
                let label = crate::routine::call_label(expr);
                let resolved = resolve_types_in_expr(catalog_kv, resolution, expr, ctes)?;
                let alias = match (alias, matches!(resolved, Expr::Func(_))) {
                    (Some(alias), _) => Some(alias.clone()),
                    (None, false) => label,
                    (None, true) => None,
                };
                Ok(SelectItem::Expr {
                    expr: resolved,
                    alias,
                })
            }
            other => Ok(other.clone()),
        })
        .collect()
}

pub(crate) fn resolve_types_in_values_with_ctes(
    catalog_kv: &dyn crabka_pgkv::Kv,
    resolution: &crate::relname::ResolutionScope,
    v: &ValuesStmt,
    ctes: &crate::cte::CteContext,
) -> Result<ValuesStmt, ExecError> {
    Ok(ValuesStmt {
        rows: v
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|expr| resolve_types_in_expr(catalog_kv, resolution, expr, ctes))
                    .collect()
            })
            .collect::<Result<_, _>>()?,
    })
}

/// Recursively replace scalar subqueries with `Const { Null, <type> }` (type-only).
fn resolve_types_in_expr(
    catalog_kv: &dyn crabka_pgkv::Kv,
    resolution: &crate::relname::ResolutionScope,
    e: &Expr,
    ctes: &crate::cte::CteContext,
) -> Result<Expr, ExecError> {
    Ok(match e {
        Expr::ScalarSubquery(s) => Expr::Const {
            value: Datum::Null,
            ty: scalar_subquery_type(catalog_kv, resolution, s, ctes)?,
        },
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: Box::new(resolve_types_in_expr(catalog_kv, resolution, expr, ctes)?),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(resolve_types_in_expr(catalog_kv, resolution, left, ctes)?),
            right: Box::new(resolve_types_in_expr(catalog_kv, resolution, right, ctes)?),
        },
        Expr::Cast { expr, ty } => Expr::Cast {
            expr: Box::new(resolve_types_in_expr(catalog_kv, resolution, expr, ctes)?),
            ty: *ty,
        },
        // P2: the describe path inlines a user-defined SQL function's body the
        // same way execution does, so a `Describe` reports the type the rows
        // will carry.
        Expr::Func(fc) => {
            let call = FuncCall {
                name: fc.name.clone(),
                distinct: fc.distinct,
                args: match &fc.args {
                    FuncArgs::Star => FuncArgs::Star,
                    FuncArgs::Exprs(args) => FuncArgs::Exprs(
                        args.iter()
                            .map(|a| resolve_types_in_expr(catalog_kv, resolution, a, ctes))
                            .collect::<Result<_, _>>()?,
                    ),
                },
                filter: match &fc.filter {
                    Some(predicate) => Some(Box::new(resolve_types_in_expr(
                        catalog_kv, resolution, predicate, ctes,
                    )?)),
                    None => None,
                },
            };
            if let Some(ty) = crate::routine::plpgsql_declared_call_type(catalog_kv, &call)? {
                Expr::Const {
                    value: Datum::Null,
                    ty,
                }
            } else {
                match crate::routine::inline_scalar(catalog_kv, &call)? {
                    Some(inlined) => {
                        let _guard = crate::routine::enter_inline()?;
                        resolve_types_in_expr(catalog_kv, resolution, &inlined, ctes)?
                    }
                    None => Expr::Func(call),
                }
            }
        }
        // Everything else (incl. EXISTS / IN / quantified, which infer as bool) is
        // typed directly by `infer_type` without substitution.
        other => other.clone(),
    })
}

/// The static type of a scalar subquery's single projection column (catalog only).
fn scalar_subquery_type(
    catalog_kv: &dyn crabka_pgkv::Kv,
    resolution: &crate::relname::ResolutionScope,
    q: &QueryExpr,
    ctes: &crate::cte::CteContext,
) -> Result<ColumnType, ExecError> {
    let fields = crate::query::describe_query_expr_with_ctes(catalog_kv, resolution, q, ctes)?;
    if fields.len() != 1 {
        return Err(ExecError::SubqueryColumns);
    }
    crate::exec::column_type_from_oid(fields[0].type_oid)
}

#[cfg(test)]
mod tests {
    use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

    use crate::SqlEngine;

    async fn run(engine: &SqlEngine, sql: &str) -> QueryResult {
        engine
            .connect()
            .simple_query(sql)
            .await
            .expect("query ok")
            .pop()
            .expect("one result")
    }

    fn cell0(r: &QueryResult) -> Option<String> {
        match r {
            QueryResult::Rows { rows, .. } => rows
                .first()
                .and_then(|row| row[0].as_ref())
                .map(|c: &Cell| String::from_utf8(c.text.to_vec()).expect("utf8")),
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    fn rowcount(r: &QueryResult) -> usize {
        match r {
            QueryResult::Rows { rows, .. } => rows.len(),
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    async fn seed() -> SqlEngine {
        let e = SqlEngine::new();
        run(&e, "CREATE TABLE t (id int4, v int4)").await;
        run(&e, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").await;
        e
    }

    #[tokio::test]
    async fn scalar_subquery_in_projection_and_where() {
        let e = seed().await;
        assert_eq!(
            cell0(&run(&e, "SELECT (SELECT max(v) FROM t)").await),
            Some("30".into())
        );
        let r = run(
            &e,
            "SELECT id FROM t WHERE v > (SELECT avg(v) FROM t) ORDER BY id",
        )
        .await;
        assert_eq!(rowcount(&r), 1); // only id=3 (v=30 > avg 20)
        assert_eq!(cell0(&r), Some("3".into()));
    }

    #[tokio::test]
    async fn scalar_subquery_zero_rows_is_null() {
        let e = seed().await;
        assert_eq!(
            cell0(&run(&e, "SELECT (SELECT v FROM t WHERE id = 999)").await),
            None,
        );
    }

    #[tokio::test]
    async fn scalar_subquery_more_than_one_row_is_21000() {
        let e = seed().await;
        let err = e
            .connect()
            .simple_query("SELECT (SELECT v FROM t)")
            .await
            .expect_err("cardinality");
        assert_eq!(err.code, "21000");
    }

    #[tokio::test]
    async fn scalar_subquery_more_than_one_column_is_42601() {
        let e = seed().await;
        let err = e
            .connect()
            .simple_query("SELECT (SELECT id, v FROM t WHERE id = 1)")
            .await
            .expect_err("columns");
        assert_eq!(err.code, "42601");
    }

    #[tokio::test]
    async fn exists_and_not_exists() {
        let e = seed().await;
        assert_eq!(
            cell0(&run(&e, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 1)").await),
            Some("t".into())
        );
        assert_eq!(
            cell0(&run(&e, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 999)").await),
            Some("f".into())
        );
        let r = run(
            &e,
            "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM t WHERE id = 999) ORDER BY id",
        )
        .await;
        assert_eq!(rowcount(&r), 3);
    }

    #[tokio::test]
    async fn in_subquery_and_not_in() {
        let e = seed().await;
        run(&e, "CREATE TABLE u (k int4)").await;
        run(&e, "INSERT INTO u VALUES (1), (3)").await;
        let r = run(
            &e,
            "SELECT id FROM t WHERE id IN (SELECT k FROM u) ORDER BY id",
        )
        .await;
        assert_eq!(rowcount(&r), 2);
        assert_eq!(cell0(&r), Some("1".into()));
        let r = run(
            &e,
            "SELECT id FROM t WHERE id NOT IN (SELECT k FROM u) ORDER BY id",
        )
        .await;
        assert_eq!(rowcount(&r), 1);
        assert_eq!(cell0(&r), Some("2".into()));
    }

    #[tokio::test]
    async fn not_in_with_a_null_element_is_unknown_for_all() {
        let e = seed().await;
        run(&e, "CREATE TABLE u (k int4)").await;
        run(&e, "INSERT INTO u VALUES (1), (null)").await;
        let r = run(&e, "SELECT id FROM t WHERE id NOT IN (SELECT k FROM u)").await;
        assert_eq!(rowcount(&r), 0);
    }

    #[tokio::test]
    async fn quantified_any_all_and_empty_set() {
        let e = seed().await;
        run(&e, "CREATE TABLE u (k int4)").await;
        run(&e, "INSERT INTO u VALUES (15), (25)").await;
        let r = run(
            &e,
            "SELECT id FROM t WHERE v > ALL (SELECT k FROM u) ORDER BY id",
        )
        .await;
        assert_eq!(rowcount(&r), 1);
        assert_eq!(cell0(&r), Some("3".into()));
        let r = run(
            &e,
            "SELECT id FROM t WHERE v > ANY (SELECT k FROM u) ORDER BY id",
        )
        .await;
        assert_eq!(rowcount(&r), 2);
        run(&e, "CREATE TABLE empt (k int4)").await;
        let r_any = run(&e, "SELECT id FROM t WHERE v > ANY (SELECT k FROM empt)").await;
        assert_eq!(rowcount(&r_any), 0);
        let r_all = run(&e, "SELECT id FROM t WHERE v > ALL (SELECT k FROM empt)").await;
        assert_eq!(rowcount(&r_all), 3);
    }

    #[tokio::test]
    async fn in_subquery_more_than_one_column_is_42601() {
        let e = seed().await;
        let err = e
            .connect()
            .simple_query("SELECT id FROM t WHERE id IN (SELECT id, v FROM t)")
            .await
            .expect_err("columns");
        assert_eq!(err.code, "42601");
    }

    #[tokio::test]
    async fn describe_types_a_scalar_subquery_projection_column() {
        let e = seed().await; // t (id int4, v int4)
        // A scalar subquery in the projection types as its single column's type (int4),
        // without executing — the catalog-only describe type pass.
        let fields = crate::describe_fields(&*e.kv, "SELECT (SELECT max(v) FROM t) FROM t")
            .expect("describe");
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].type_oid, crabka_pgtypes::oids::INT4); // max(int4) → int4
    }
}
