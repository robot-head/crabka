//! SP34: uncorrelated subquery resolution (scalar / IN / EXISTS / ANY-ALL).
//!
//! An uncorrelated subquery's result is identical for every outer row, so this
//! module evaluates it ONCE, before the outer row loop, and rewrites the node
//! into already-supported nodes: an `Expr::Const`, an `InList` of consts, or an
//! `OR`/`AND` fold of comparisons. The pure `eval`/`agg` evaluators then run
//! unchanged over the rewritten tree. Subqueries run through the SP33 join read
//! path (`exec::select_to_relation`) under the SAME snapshot handles as the
//! outer query, so the read is consistent and read-your-writes holds.
//! Correlation, which is a reference to an outer column, is out of scope. A
//! subquery's scope comes solely from its own FROM, so an outer-column reference
//! fails name resolution (42703).

use crabka_pgparser::ast::{
    BinaryOp, Expr, FuncArgs, FuncCall, OrderItem, QueryExpr, SelectItem, SelectStmt, ValuesStmt,
};
use crabka_pgtypes::{ColumnType, Datum, ElemType};

use crate::error::ExecError;

/// The read-side handles a subquery needs to execute. They mirror
/// `execute_read`'s parameters. The resolution recursion threads them through, so
/// each nested subquery reads under the outer query's snapshot.
#[derive(Clone)]
pub(crate) struct SubCtx<'a> {
    pub catalog_kv: &'a dyn crabka_pgkv::Kv,
    pub kv: &'a dyn crabka_pgkv::Kv,
    pub global: &'a dyn crabka_pgkv::Kv,
    pub gsnap: &'a crabka_pgmvcc::visibility::Snapshot,
    pub snapshot: &'a crabka_pgmvcc::visibility::Snapshot,
    pub own: Option<u64>,
    pub ctes: &'a crate::cte::CteContext,
    /// The session eval context (zone and clock), forwarded to the subquery's
    /// read path so a temporal expression inside a subquery evaluates in the
    /// session zone.
    pub eval_ctx: &'a crate::clock::EvalCtx,
    /// SP40: the foreign-table read context (scanner and current user),
    /// forwarded so a subquery that references a foreign table reads through the
    /// registered scanner.
    pub fctx: crate::exec::ForeignCtx<'a>,
    /// G-8: ordinary table scanner seam forwarded through nested subqueries.
    pub range_scanner: &'a dyn crate::scanner::RangeScanner,
    /// Memory retained by one blocking query operator.
    pub blocking_query_memory: crabka_units::ByteSize,
    /// Shared blocking-memory charge for this statement and all nested reads.
    pub statement_memory: crate::scanner::StatementMemory,
    /// The role whose row-security policies this read is subject to.
    ///
    /// Ordinarily the session's own role. It lives here rather than being
    /// re-derived from `fctx.current_user` at each use because `SubCtx` is the
    /// one value already threaded through every read path: substituting a
    /// view's owner for the body of that view is then one assignment, not
    /// twenty extra arguments.
    pub security_role: &'a str,
    /// The relations whose policy quals are being evaluated on this read, so a
    /// policy that reads its own relation reports 42P17 instead of recursing
    /// until the stack runs out.
    pub policy_stack: &'a crate::rls::PolicyStack,
    /// The whole-row references of the statement whose FROM clause is being
    /// built, which decide the hidden liveness markers an outer join carries.
    ///
    /// `None` is "not known here", and marks every qualifier — what every read
    /// path did before markers became conditional. Each statement overwrites it
    /// with its own as it starts, so a view body or a subquery is measured by
    /// its own text and never by the text of whatever reached it.
    pub refs: Option<&'a crate::scope::StatementRefs>,
    /// Where an `EXPLAIN ANALYZE` read retains the root state its planned path
    /// executed. Ordinary reads leave this absent.
    pub explain_plan_state: Option<
        std::sync::Arc<std::sync::Mutex<Option<crate::plan::query::PlanState>>>,
    >,
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
            statement_memory: self.statement_memory.clone(),
            security_role: self.security_role,
            policy_stack: self.policy_stack,
            refs: self.refs,
            explain_plan_state: self.explain_plan_state.clone(),
        }
    }

    /// The same read context, measuring outer-join liveness markers against
    /// `refs` — the references of the statement about to build its FROM clause.
    ///
    /// Every statement calls this as it starts, so the narrowing never outlives
    /// the text it was derived from: a view body, a derived table and a
    /// correlated subquery each replace it with their own.
    pub(crate) fn with_refs<'b>(&'b self, refs: &'b crate::scope::StatementRefs) -> SubCtx<'b>
    where
        'a: 'b,
    {
        SubCtx {
            refs: Some(refs),
            ..self.clone()
        }
    }

    /// [`Self::with_refs`] where the caller holds the refs as an `Option`, and
    /// `None` means "this statement establishes none" rather than "keep
    /// whatever was here". A DML target with no storage identity — a view —
    /// passes `None`, and would be given system columns it cannot value if the
    /// enclosing context's refs leaked in.
    pub(crate) fn with_refs_opt<'b>(
        &'b self,
        refs: Option<&'b crate::scope::StatementRefs>,
    ) -> SubCtx<'b>
    where
        'a: 'b,
    {
        SubCtx {
            refs,
            ..self.clone()
        }
    }

    /// The same read context, with privilege and row-security decisions made
    /// as `role` instead.
    ///
    /// The one caller is the view-expansion site: a view without
    /// `security_invoker` runs its body as the role that owns it. Nesting
    /// shadows rather than stacks, because each expansion derives its context
    /// from the one it was reached through — a view owned by A over a view
    /// owned by B evaluates B's body as B, and B's own ACL is checked as A.
    ///
    /// `eval_ctx` is deliberately untouched: `CURRENT_USER` reads from there,
    /// and `PostgreSQL` leaves it naming the invoking role inside a view body.
    /// Only the identity decisions are made under moves.
    pub(crate) fn with_security_role<'b>(&'b self, role: &'b str) -> SubCtx<'b>
    where
        'a: 'b,
    {
        SubCtx {
            security_role: role,
            ..self.clone()
        }
    }

    /// The same read context, resolving unqualified relation names in `scope`
    /// instead of the session's.
    ///
    /// The one caller is the view-expansion site, for the same reason
    /// [`Self::with_security_role`] is called there: a stored body is text, and
    /// what it names has to be decided by the view rather than by whoever is
    /// reading it. Nesting shadows rather than stacks — each level's body is
    /// resolved with *that* level's schema first, so a view in one schema over
    /// a view in another resolves each body where it was written.
    pub(crate) fn with_resolution<'b>(
        &'b self,
        scope: &'b crate::relname::ResolutionScope,
    ) -> SubCtx<'b>
    where
        'a: 'b,
    {
        SubCtx {
            fctx: crate::exec::ForeignCtx {
                resolution: scope,
                ..self.fctx
            },
            ..self.clone()
        }
    }

    /// What this statement decides about the joins its FROM clause builds.
    pub(crate) fn join_policy(&self) -> crate::join::JoinPolicy<'_> {
        crate::join::JoinPolicy {
            memory: self.blocking_query_memory,
            statement_memory: self.statement_memory.clone(),
            refs: self.refs,
        }
    }

    /// Retain the one planned root that produced this read's result for the
    /// enclosing `EXPLAIN ANALYZE` renderer.
    pub(crate) fn record_plan_state(&self, state: crate::plan::query::PlanState) {
        if let Some(slot) = &self.explain_plan_state {
            *slot.lock().expect("EXPLAIN plan state") = Some(state);
        }
    }

    /// The row-security decision context this read makes its decisions in.
    pub(crate) fn rls(&self) -> crate::rls::RlsCtx<'_> {
        crate::rls::RlsCtx::new(self.catalog_kv, self.security_role, self.fctx.row_security)
    }

    /// The privilege decision context this read makes its decisions in.
    ///
    /// The same role row security is judged under, deliberately: a view that
    /// one day runs its body with owner rights must move both together, or a
    /// role would read a relation under its own grants and someone else's
    /// policies.
    pub(crate) fn privileges(&self) -> crate::privilege::PrivilegeCtx<'_> {
        crate::privilege::PrivilegeCtx::new(self.catalog_kv, self.security_role)
    }
}

/// Rewrite every uncorrelated subquery in `s`'s expr-bearing clauses to a
/// resolved constant form. The FROM clause, which holds base tables, joins and
/// derived tables, belongs to the SP33 join read path. This rewrites only
/// expression positions.
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
            resolve_window_spec(ctx, spec)?;
        }
    }
    for window in &mut out.windows {
        resolve_window_spec(ctx, &mut window.spec)?;
    }
    Ok(out)
}

/// Rewrite subqueries everywhere one window specification can hold them.
///
/// The frame offsets are here with the partition and ordering keys because a
/// frame bound is an ordinary expression evaluated once for the whole window —
/// `ROWS (SELECT …) PRECEDING` is legal `PostgreSQL`. Leaving them out left a
/// raw subquery node for the scalar evaluator to refuse.
fn resolve_window_spec(
    ctx: &SubCtx,
    spec: &mut crabka_pgparser::ast::WindowSpec,
) -> Result<(), ExecError> {
    use crabka_pgparser::ast::FrameBound;

    for expr in &mut spec.partition_by {
        *expr = resolve_expr(ctx, expr)?;
    }
    for item in &mut spec.order_by {
        item.expr = resolve_expr(ctx, &item.expr)?;
    }
    if let Some(frame) = &mut spec.frame {
        for bound in [&mut frame.start, &mut frame.end] {
            match bound {
                FrameBound::Preceding(offset) | FrameBound::Following(offset) => {
                    *offset = resolve_expr(ctx, offset)?;
                }
                FrameBound::UnboundedPreceding
                | FrameBound::CurrentRow
                | FrameBound::UnboundedFollowing => {}
            }
        }
    }
    Ok(())
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
/// snapshot-consistent subquery fold before scalar `eval`.
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
    should_skip: &mut dyn FnMut(&Expr) -> bool,
) -> Result<crabka_pgparser::ast::ArraySubscript, ExecError> {
    use crabka_pgparser::ast::ArraySubscript;

    Ok(match subscript {
        ArraySubscript::Index(index) => {
            ArraySubscript::Index(resolve_expr_skipping(ctx, index, should_skip)?)
        }
        ArraySubscript::Slice { lower, upper } => ArraySubscript::Slice {
            lower: lower
                .as_ref()
                .map(|e| resolve_expr_skipping(ctx, e, should_skip))
                .transpose()?,
            upper: upper
                .as_ref()
                .map(|e| resolve_expr_skipping(ctx, e, should_skip))
                .transpose()?,
        },
    })
}

/// Recursively rewrite subquery nodes in `e`, bottom-up.
pub(crate) fn resolve_expr(ctx: &SubCtx, e: &Expr) -> Result<Expr, ExecError> {
    resolve_expr_skipping(ctx, e, &mut |_| false)
}

/// Recursively rewrite subquery nodes except those selected by `should_skip`.
///
/// The callback receives each original expression before any of its children
/// are rewritten. Returning `true` clones that whole node untouched.
pub(crate) fn resolve_expr_skipping(
    ctx: &SubCtx,
    e: &Expr,
    should_skip: &mut dyn FnMut(&Expr) -> bool,
) -> Result<Expr, ExecError> {
    if should_skip(e) {
        return Ok(e.clone());
    }

    Ok(match e {
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BitStringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Default
        | Expr::Const { .. } => e.clone(),
        Expr::FieldSelect { base, field } => Expr::FieldSelect {
            base: Box::new(resolve_expr_skipping(ctx, base, should_skip)?),
            field: field.clone(),
        },
        Expr::FieldSelectAll(base) => {
            Expr::FieldSelectAll(Box::new(resolve_expr_skipping(ctx, base, should_skip)?))
        }
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: Box::new(resolve_expr_skipping(ctx, expr, should_skip)?),
        },
        Expr::Collate { expr, collation } => Expr::Collate {
            expr: Box::new(resolve_expr_skipping(ctx, expr, should_skip)?),
            collation: collation.clone(),
        },
        Expr::Binary { op, left, right } => {
            // A parenthesized subquery normally has to project one column.  In
            // a row comparison, though, PostgreSQL lets it stand for the row
            // of its projections; lower only that context to an Expr::Row so
            // the ordinary scalar-subquery rule stays intact everywhere else.
            let (left, right) = match (&**left, &**right) {
                (Expr::Row(items), Expr::ScalarSubquery(query)) => (
                    resolve_expr_skipping(ctx, left, should_skip)?,
                    run_row_scalar(ctx, query, items.len())?,
                ),
                (Expr::ScalarSubquery(query), Expr::Row(items)) => (
                    run_row_scalar(ctx, query, items.len())?,
                    resolve_expr_skipping(ctx, right, should_skip)?,
                ),
                _ => (
                    resolve_expr_skipping(ctx, left, should_skip)?,
                    resolve_expr_skipping(ctx, right, should_skip)?,
                ),
            };
            Expr::Binary {
                op: *op,
                left: Box::new(left),
                right: Box::new(right),
            }
        }
        Expr::Func(fc) => {
            let call = FuncCall {
                sql_syntax: fc.sql_syntax,
                name: fc.name.clone(),
                distinct: fc.distinct,
                args: match &fc.args {
                    FuncArgs::Star => FuncArgs::Star,
                    FuncArgs::Exprs(args) => FuncArgs::Exprs(
                        args.iter()
                            .map(|a| resolve_expr_skipping(ctx, a, should_skip))
                            .collect::<Result<_, _>>()?,
                    ),
                    FuncArgs::Named { positional, named } => FuncArgs::Named {
                        positional: positional
                            .iter()
                            .map(|arg| resolve_expr_skipping(ctx, arg, should_skip))
                            .collect::<Result<_, _>>()?,
                        named: named
                            .iter()
                            .map(|(label, arg)| {
                                Ok((label.clone(), resolve_expr_skipping(ctx, arg, should_skip)?))
                            })
                            .collect::<Result<_, ExecError>>()?,
                    },
                    FuncArgs::Variadic { positional, array } => {
                        let mut args = positional
                            .iter()
                            .map(|arg| resolve_expr_skipping(ctx, arg, should_skip))
                            .collect::<Result<Vec<_>, _>>()?;
                        args.push(resolve_expr_skipping(ctx, array, should_skip)?);
                        FuncArgs::Exprs(args)
                    }
                },
                // An aggregate's sort keys resolve like its arguments — they
                // may name the very columns this pass rewrites.
                order_by: fc
                    .order_by
                    .iter()
                    .map(|item| {
                        Ok(crabka_pgparser::ast::OrderItem {
                            expr: resolve_expr_skipping(ctx, &item.expr, should_skip)?,
                            asc: item.asc,
                            nulls_first: item.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>, ExecError>>()?,
                // The FILTER predicate resolves like an argument; dropping it here
                // would silently turn a filtered aggregate into an unfiltered one.
                filter: match &fc.filter {
                    Some(predicate) => Some(Box::new(resolve_expr_skipping(
                        ctx,
                        predicate,
                        should_skip,
                    )?)),
                    None => None,
                },
            };
            let call = crate::routine::normalize_named_call(ctx.catalog_kv, &call)?.unwrap_or(call);
            // P2: a call of a user-defined SQL function is inlined here, the one
            // point in the rewrite where the routine catalog is reachable.
            match crate::routine::inline_scalar(ctx.catalog_kv, &call)? {
                // The inlined body may itself call a routine, and may have
                // become a scalar subquery, so it goes back through this pass.
                Some(inlined) => {
                    let _guard = crate::routine::enter_inline()?;
                    resolve_expr_skipping(ctx, &inlined, should_skip)?
                }
                None => Expr::Func(call),
            }
        }
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(resolve_expr_skipping(ctx, expr, should_skip)?),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(resolve_expr_skipping(ctx, expr, should_skip)?),
            list: list
                .iter()
                .map(|x| resolve_expr_skipping(ctx, x, should_skip))
                .collect::<Result<_, _>>()?,
            negated: *negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(resolve_expr_skipping(ctx, expr, should_skip)?),
            low: Box::new(resolve_expr_skipping(ctx, low, should_skip)?),
            high: Box::new(resolve_expr_skipping(ctx, high, should_skip)?),
            negated: *negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            kind,
            escape,
        } => Expr::Like {
            expr: Box::new(resolve_expr_skipping(ctx, expr, should_skip)?),
            pattern: Box::new(resolve_expr_skipping(ctx, pattern, should_skip)?),
            negated: *negated,
            kind: *kind,
            escape: match escape {
                Some(e) => Some(Box::new(resolve_expr_skipping(ctx, e, should_skip)?)),
                None => None,
            },
        },
        Expr::Case {
            operand,
            whens,
            else_result,
        } => Expr::Case {
            operand: match operand {
                Some(o) => Some(Box::new(resolve_expr_skipping(ctx, o, should_skip)?)),
                None => None,
            },
            whens: whens
                .iter()
                .map(|(c, r)| {
                    Ok((
                        resolve_expr_skipping(ctx, c, should_skip)?,
                        resolve_expr_skipping(ctx, r, should_skip)?,
                    ))
                })
                .collect::<Result<_, ExecError>>()?,
            else_result: match else_result {
                Some(o) => Some(Box::new(resolve_expr_skipping(ctx, o, should_skip)?)),
                None => None,
            },
        },
        Expr::Cast { expr, ty } => Expr::Cast {
            expr: Box::new(resolve_expr_skipping(ctx, expr, should_skip)?),
            ty: *ty,
        },
        Expr::SqlJson(json) => {
            Expr::SqlJson(Box::new(json.map_children(|child| {
                resolve_expr_skipping(ctx, child, should_skip)
            })?))
        }
        // The array expression forms carry ordinary child expressions, any of
        // which may contain a subquery (`ARRAY[(SELECT …)]`, `arr[(SELECT …)]`,
        // `x = ANY((SELECT …))`), so they recurse like every other node — the
        // node itself is not a subquery and is rebuilt unchanged.
        Expr::ArrayLiteral(items) => Expr::ArrayLiteral(
            items
                .iter()
                .map(|item| resolve_expr_skipping(ctx, item, should_skip))
                .collect::<Result<_, _>>()?,
        ),
        Expr::Row(items) => Expr::Row(
            items
                .iter()
                .map(|item| resolve_expr_skipping(ctx, item, should_skip))
                .collect::<Result<_, _>>()?,
        ),
        Expr::Subscript { base, index } => Expr::Subscript {
            base: Box::new(resolve_expr_skipping(ctx, base, should_skip)?),
            index: Box::new(resolve_expr_skipping(ctx, index, should_skip)?),
        },
        Expr::ArrayRef { base, subscripts } => Expr::ArrayRef {
            base: Box::new(resolve_expr_skipping(ctx, base, should_skip)?),
            subscripts: subscripts
                .iter()
                .map(|s| resolve_subscript(ctx, s, should_skip))
                .collect::<Result<_, _>>()?,
        },
        Expr::QuantifiedArray {
            expr,
            op,
            all,
            array,
        } => Expr::QuantifiedArray {
            expr: Box::new(resolve_expr_skipping(ctx, expr, should_skip)?),
            op: *op,
            all: *all,
            array: Box::new(resolve_expr_skipping(ctx, array, should_skip)?),
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
            let expr = resolve_expr_skipping(ctx, expr, should_skip)?;
            let list = if let Expr::Row(items) = &expr {
                run_row_list(ctx, subquery, items.len())?
            } else {
                let (ty, values) = run_single_column(ctx, subquery)?;
                values
                    .into_iter()
                    .map(|value| Expr::Const { value, ty })
                    .collect()
            };
            Expr::InList {
                expr: Box::new(expr),
                list,
                negated: *negated,
            }
        }
        Expr::Quantified {
            expr,
            op,
            all,
            subquery,
        } => {
            let lhs = resolve_expr_skipping(ctx, expr, should_skip)?;
            let values = if let Expr::Row(items) = &lhs {
                run_row_list(ctx, subquery, items.len())?
            } else {
                let (ty, values) = run_single_column(ctx, subquery)?;
                values
                    .into_iter()
                    .map(|value| Expr::Const { value, ty })
                    .collect()
            };
            lower_quantified(&lhs, *op, *all, values)
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

/// Run a subquery to its raw rows, in any shape. `EXISTS` uses this.
fn run_rows(ctx: &SubCtx, q: &QueryExpr) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(run_relation(ctx, q)?.rows)
}

/// Run a scalar subquery: exactly one column and at most one row, which gives
/// `(value, type)`.
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

/// Lower one row returned by a subquery into a row constructor.  The caller
/// has already established that this is a row-valued context, so its width is
/// part of the expression's contract rather than the scalar-subquery error.
fn run_row_scalar(ctx: &SubCtx, q: &QueryExpr, width: usize) -> Result<Expr, ExecError> {
    let rel = run_relation(ctx, q)?;
    if rel.scope.width() != width {
        return Err(ExecError::SubqueryColumns);
    }
    if rel.rows.len() > 1 {
        return Err(ExecError::CardinalityViolation);
    }
    let row = rel
        .rows
        .into_iter()
        .next()
        .unwrap_or_else(|| vec![Datum::Null; width]);
    Ok(row_expr(row, &rel.scope))
}

/// Lower every row of a subquery for a row `IN` / `ANY` / `ALL` expression.
fn run_row_list(ctx: &SubCtx, q: &QueryExpr, width: usize) -> Result<Vec<Expr>, ExecError> {
    let rel = run_relation(ctx, q)?;
    if rel.scope.width() != width {
        return Err(ExecError::SubqueryColumns);
    }
    Ok(rel
        .rows
        .into_iter()
        .map(|row| row_expr(row, &rel.scope))
        .collect())
}

fn row_expr(row: Vec<Datum>, scope: &crate::scope::Scope) -> Expr {
    Expr::Row(
        row.into_iter()
            .enumerate()
            .map(|(index, value)| Expr::Const {
                value,
                ty: scope.ty_at(index),
            })
            .collect(),
    )
}

/// Run a single-column subquery, and return its column type plus every value,
/// in row order.
fn run_single_column(ctx: &SubCtx, q: &QueryExpr) -> Result<(ColumnType, Vec<Datum>), ExecError> {
    let rel = run_relation(ctx, q)?;
    if rel.scope.width() != 1 {
        return Err(ExecError::SubqueryColumns);
    }
    let ty = rel.scope.ty_at(0);
    let col = rel.rows.into_iter().map(|mut r| r.remove(0)).collect();
    Ok((ty, col))
}

/// Lower `lhs op ANY|SOME|ALL (values)` to an `OR`/`AND` fold of comparisons,
/// with PostgreSQL's empty-set semantics: ANY gives false, and ALL gives true.
/// NULL three-valued logic falls out of the existing
/// `ops::or`/`ops::and`/`ops::compare`.
fn lower_quantified(lhs: &Expr, op: BinaryOp, all: bool, values: Vec<Expr>) -> Expr {
    if values.is_empty() {
        return Expr::Const {
            value: Datum::Bool(all),
            ty: ColumnType::Bool,
        };
    }
    let join = if all { BinaryOp::And } else { BinaryOp::Or };
    let mut acc: Option<Expr> = None;
    for right in values {
        let cmp = Expr::Binary {
            op,
            left: Box::new(lhs.clone()),
            right: Box::new(right),
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
///
/// The walk is [`crate::grouping::rewrite`], the crate's one exhaustive [`Expr`]
/// fold, rather than a match written out again here. Only the two nodes this pass
/// actually decides — a scalar subquery, and a routine call whose body it inlines —
/// are handled in the fold; every other node reaches its children through the
/// shared walk. That is what fixes the class of bug this replaced: a hand-written
/// match that ended in a catch-all left a scalar subquery nested under any
/// unlisted node (`CASE`, `BETWEEN`, `COALESCE`'s arguments, an array literal, …)
/// unresolved, and `eval::infer_type` refuses a `ScalarSubquery` it is handed.
///
/// EXISTS / IN / quantified subqueries stay as they are: they infer as `bool`
/// without substitution, and the shared walk already leaves their inner queries
/// (separate scopes) alone while still descending into their outer operands.
fn resolve_types_in_expr(
    catalog_kv: &dyn crabka_pgkv::Kv,
    resolution: &crate::relname::ResolutionScope,
    e: &Expr,
    ctes: &crate::cte::CteContext,
) -> Result<Expr, ExecError> {
    crate::grouping::rewrite(
        e,
        &mut |node| match node {
            Expr::ScalarSubquery(s) => Ok(Some(Expr::Const {
                value: Datum::Null,
                ty: scalar_subquery_type(catalog_kv, resolution, s, ctes)?,
            })),
            // `ARRAY(subquery)` types as an array OF the subquery's one column,
            // matching what execution folds it to in `resolve_expr_skipping`.
            Expr::ArraySubquery(s) => {
                let ty = scalar_subquery_type(catalog_kv, resolution, s, ctes)?;
                let elem = ElemType::from_column_type(ty).ok_or_else(|| {
                    ExecError::Unsupported(format!("arrays of {} are not supported", ty.name()))
                })?;
                Ok(Some(Expr::Const {
                    value: Datum::Null,
                    ty: ColumnType::Array(elem),
                }))
            }
            Expr::Func(call) => Ok(Some(resolve_types_in_call(
                catalog_kv, resolution, call, ctes,
            )?)),
            _ => Ok(None),
        },
        // An aggregate's arguments are ordinary expressions for typing purposes, so
        // a subquery inside one resolves like any other.
        true,
    )
}

/// The type-only resolution of one function call: arguments and FILTER first, then
/// the routine catalog.
///
/// P2: the describe path inlines a user-defined SQL function's body the same way
/// execution does, so a `Describe` reports the type the rows will carry.
fn resolve_types_in_call(
    catalog_kv: &dyn crabka_pgkv::Kv,
    resolution: &crate::relname::ResolutionScope,
    fc: &FuncCall,
    ctes: &crate::cte::CteContext,
) -> Result<Expr, ExecError> {
    let call = FuncCall {
        sql_syntax: fc.sql_syntax,
        name: fc.name.clone(),
        distinct: fc.distinct,
        args: match &fc.args {
            FuncArgs::Star => FuncArgs::Star,
            FuncArgs::Exprs(args) => FuncArgs::Exprs(
                args.iter()
                    .map(|a| resolve_types_in_expr(catalog_kv, resolution, a, ctes))
                    .collect::<Result<_, _>>()?,
            ),
            FuncArgs::Named { positional, named } => FuncArgs::Named {
                positional: positional
                    .iter()
                    .map(|arg| resolve_types_in_expr(catalog_kv, resolution, arg, ctes))
                    .collect::<Result<_, _>>()?,
                named: named
                    .iter()
                    .map(|(label, arg)| {
                        Ok((
                            label.clone(),
                            resolve_types_in_expr(catalog_kv, resolution, arg, ctes)?,
                        ))
                    })
                    .collect::<Result<_, ExecError>>()?,
            },
            FuncArgs::Variadic { positional, array } => {
                let mut args = positional
                    .iter()
                    .map(|arg| resolve_types_in_expr(catalog_kv, resolution, arg, ctes))
                    .collect::<Result<Vec<_>, _>>()?;
                args.push(resolve_types_in_expr(catalog_kv, resolution, array, ctes)?);
                FuncArgs::Exprs(args)
            }
        },
        order_by: fc
            .order_by
            .iter()
            .map(|item| {
                Ok(crabka_pgparser::ast::OrderItem {
                    expr: resolve_types_in_expr(catalog_kv, resolution, &item.expr, ctes)?,
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                })
            })
            .collect::<Result<Vec<_>, ExecError>>()?,
        filter: match &fc.filter {
            Some(predicate) => Some(Box::new(resolve_types_in_expr(
                catalog_kv, resolution, predicate, ctes,
            )?)),
            None => None,
        },
    };
    let call = crate::routine::normalize_named_call(catalog_kv, &call)?.unwrap_or(call);
    if let Some(ty) = crate::routine::plpgsql_declared_call_type(catalog_kv, &call)? {
        return Ok(Expr::Const {
            value: Datum::Null,
            ty,
        });
    }
    Ok(match crate::routine::inline_scalar(catalog_kv, &call)? {
        Some(inlined) => {
            let _guard = crate::routine::enter_inline()?;
            resolve_types_in_expr(catalog_kv, resolution, &inlined, ctes)?
        }
        None => Expr::Func(call),
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
    use crabka_pgkv::MemKv;
    use crabka_pgmvcc::visibility::Snapshot;
    use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};
    use crabka_units::convert::ByteSizeExt as _;

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

    #[test]
    fn derived_contexts_replace_their_requested_scope() {
        let kv = MemKv::new();
        let snapshot = Snapshot {
            xmin: 0,
            xmax: u64::MAX,
            xip: Vec::new(),
        };
        let ctes = crate::cte::CteContext::empty();
        let eval_ctx = crate::clock::EvalCtx::test_default();
        let scanner = crate::scanner::LocalRangeScanner;
        let policy_stack = crate::rls::PolicyStack::default();
        let refs = crate::scope::StatementRefs::default();
        let ctx = super::SubCtx {
            catalog_kv: &kv,
            kv: &kv,
            global: &kv,
            gsnap: &snapshot,
            snapshot: &snapshot,
            own: None,
            ctes: &ctes,
            eval_ctx: &eval_ctx,
            fctx: crate::exec::ForeignCtx::none(),
            range_scanner: &scanner,
            blocking_query_memory: crabka_units::ByteSize::from_bytes(1),
            statement_memory: crate::scanner::StatementMemory::new(
                crabka_units::ByteSize::from_bytes(1),
            ),
            security_role: "owner",
            policy_stack: &policy_stack,
            refs: Some(&refs),
            explain_plan_state: None,
        };

        assert!(ctx.with_refs_opt(None).refs.is_none());
        assert_eq!(ctx.with_security_role("reader").security_role, "reader");

        let resolution = crate::relname::ResolutionScope {
            search_path: crate::search_path::SearchPath::parse("private"),
            user: "reader".into(),
            backend_id: 42,
            database: "other".into(),
        };
        assert_eq!(ctx.with_resolution(&resolution).fctx.resolution, &resolution);
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
