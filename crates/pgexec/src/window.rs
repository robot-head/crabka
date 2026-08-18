//! Window functions: `OVER`, frames, named windows, and the window-function set.
//!
//! The parser lifts every `f(…) OVER …` call out of the expression tree onto
//! [`SelectStmt::window_calls`] and leaves a
//! [`crabka_pgparser::ast::window_placeholder`] column reference behind. This
//! module is the plan node that fills those columns in. It takes the rows the
//! `WHERE` already produced, and for a grouped query the rows the
//! `GROUP BY`/`HAVING` produced. It appends one synthetic column per window
//! call, and it hands the widened rows back to the ordinary projection path.
//!
//! Windowing therefore sits exactly where `PostgreSQL` puts it: above
//! `WHERE`/`GROUP BY`/`HAVING`, and below `DISTINCT`, `ORDER BY` and `LIMIT`.
//! No other expression code knows it exists.
//!
//! [`crate::agg`] folds aggregates used as window functions over the frame's
//! rows. Every aggregate the engine implements is therefore usable over a
//! window, with no per-aggregate work here.

use std::{cmp::Ordering, collections::HashMap};

use crabka_pgparser::ast::{
    BinaryOp, DistinctClause, Expr, FrameBound, FrameExclusion, FrameMode, FuncArgs, FuncCall,
    NamedWindow, OrderItem, SelectItem, SelectStmt, WindowCall, WindowFrame, WindowRef, WindowSpec,
};
use crabka_pgtypes::{ColumnType, Datum};
use crabka_pgwire::engine::FieldDescription;

use crate::{
    clock::EvalCtx,
    error::ExecError,
    scope::{ColumnBinding, Exposure, Scope},
};

/// Qualifier of the synthetic bindings that carry a grouped query's pre-window
/// values (its `GROUP BY` keys and aggregate results). Like the window
/// qualifier, `$` keeps it unreachable from any user expression.
const GROUPED_QUALIFIER: &str = "$g";

/// The window functions that are not ordinary aggregates. Anything else named in
/// an `OVER` call is resolved as an aggregate and folded over the frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile,
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue,
    /// An ordinary aggregate evaluated over the frame.
    Aggregate,
}

impl WindowFunc {
    fn classify(name: &str) -> Self {
        match name {
            "row_number" => Self::RowNumber,
            "rank" => Self::Rank,
            "dense_rank" => Self::DenseRank,
            "percent_rank" => Self::PercentRank,
            "cume_dist" => Self::CumeDist,
            "ntile" => Self::Ntile,
            "lag" => Self::Lag,
            "lead" => Self::Lead,
            "first_value" => Self::FirstValue,
            "last_value" => Self::LastValue,
            "nth_value" => Self::NthValue,
            _ => Self::Aggregate,
        }
    }
}

/// Is `name` a window-only function?
///
/// `PostgreSQL` refuses such a function without an `OVER` clause and raises
/// 42809. It does not report an undefined function.
#[must_use]
pub(crate) fn is_window_only_function(name: &str) -> bool {
    !matches!(WindowFunc::classify(name), WindowFunc::Aggregate)
}

/// `PostgreSQL`'s error for a window-only function written without `OVER`.
pub(crate) fn requires_over_clause(name: &str) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42809",
        message: format!("window function {name} requires an OVER clause"),
    }
}

fn windowing_error(message: impl Into<String>) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42P20",
        message: message.into(),
    }
}

fn unsupported(message: impl Into<String>) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "0A000",
        message: message.into(),
    }
}

/// Does this SELECT call any window function?
#[must_use]
pub(crate) fn has_window_calls(s: &SelectStmt) -> bool {
    !s.window_calls.is_empty()
}

/// Does `expr`, or a subexpression of it, stand in for a window call?
fn contains_placeholder(expr: &Expr) -> bool {
    if crabka_pgparser::ast::window_placeholder_index(expr).is_some() {
        return true;
    }
    match expr {
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } | Expr::Cast { expr, .. } => {
            contains_placeholder(expr)
        }
        Expr::Binary { left, right, .. }
        | Expr::Subscript {
            base: left,
            index: right,
        } => contains_placeholder(left) || contains_placeholder(right),
        Expr::InList { expr, list, .. } => {
            contains_placeholder(expr) || list.iter().any(contains_placeholder)
        }
        Expr::Between {
            expr, low, high, ..
        } => contains_placeholder(expr) || contains_placeholder(low) || contains_placeholder(high),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            contains_placeholder(expr)
                || contains_placeholder(pattern)
                || escape.as_deref().is_some_and(contains_placeholder)
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            operand.as_deref().is_some_and(contains_placeholder)
                || whens
                    .iter()
                    .any(|(c, r)| contains_placeholder(c) || contains_placeholder(r))
                || else_result.as_deref().is_some_and(contains_placeholder)
        }
        Expr::ArrayLiteral(items) | Expr::Row(items) => items.iter().any(contains_placeholder),
        Expr::QuantifiedArray { expr, array, .. } => {
            contains_placeholder(expr) || contains_placeholder(array)
        }
        Expr::Func(call) => match &call.args {
            FuncArgs::Star => false,
            FuncArgs::Exprs(args) => args.iter().any(contains_placeholder),
        },
        _ => false,
    }
}

fn reject_clause(expr: Option<&Expr>, clause: &str) -> Result<(), ExecError> {
    match expr {
        Some(expr) if contains_placeholder(expr) => Err(windowing_error(format!(
            "window functions are not allowed in {clause}"
        ))),
        _ => Ok(()),
    }
}

/// `PostgreSQL` rejects a window call in every clause evaluated at or below the
/// window plan node, with 42P20. This function checks before the `WHERE` runs,
/// so the placeholder column never reaches expression evaluation.
pub(crate) fn reject_misplaced_calls(s: &SelectStmt) -> Result<(), ExecError> {
    if !has_window_calls(s) {
        return Ok(());
    }
    for table in &s.from {
        reject_in_join_tree(table)?;
    }
    reject_clause(s.filter.as_ref(), "WHERE")?;
    for group in &s.group_by {
        reject_clause(Some(group), "GROUP BY")?;
    }
    reject_clause(s.having.as_ref(), "HAVING")?;
    reject_clause(s.limit.as_ref(), "LIMIT")?;
    reject_clause(s.offset.as_ref(), "OFFSET")?;
    Ok(())
}

/// `JOIN … ON` is evaluated below the window node, exactly like `WHERE`.
fn reject_in_join_tree(table: &crabka_pgparser::ast::TableExpr) -> Result<(), ExecError> {
    let crabka_pgparser::ast::TableExpr::Join {
        left,
        right,
        constraint,
        ..
    } = table
    else {
        return Ok(());
    };
    if let crabka_pgparser::ast::JoinConstraint::On(on) = constraint {
        reject_clause(Some(on), "JOIN conditions")?;
    }
    reject_in_join_tree(left)?;
    reject_in_join_tree(right)
}

/// A window call with its `OVER` clause resolved and its shape validated.
struct PlannedCall {
    func: WindowFunc,
    call: FuncCall,
    filter: Option<Expr>,
    spec: WindowSpec,
    result_ty: ColumnType,
    /// The type an `unknown` literal `RANGE` offset adopts. This is the type
    /// the ordering column's `in_range` support function declares. `None` when
    /// no bound is written as a bare literal.
    range_offset_ty: Option<ColumnType>,
}

impl PlannedCall {
    fn args(&self) -> &[Expr] {
        match &self.call.args {
            FuncArgs::Star => &[],
            FuncArgs::Exprs(args) => args,
        }
    }
}

/// Resolve the `WINDOW` clause: each definition may copy an EARLIER one.
fn resolve_window_clause(windows: &[NamedWindow]) -> Result<Vec<(String, WindowSpec)>, ExecError> {
    let mut resolved: Vec<(String, WindowSpec)> = Vec::with_capacity(windows.len());
    for window in windows {
        let spec = merge_base(&window.spec, &resolved)?;
        resolved.push((window.name.clone(), spec));
    }
    Ok(resolved)
}

fn lookup_window<'a>(
    name: &str,
    resolved: &'a [(String, WindowSpec)],
) -> Result<&'a WindowSpec, ExecError> {
    resolved
        .iter()
        .find(|(defined, _)| defined == name)
        .map(|(_, spec)| spec)
        .ok_or_else(|| ExecError::FunctionError {
            sqlstate: "42704",
            message: format!("window \"{name}\" does not exist"),
        })
}

/// Fold `OVER (w …)` onto the window `w` names, with `PostgreSQL`'s three
/// override rules.
fn merge_base(
    spec: &WindowSpec,
    resolved: &[(String, WindowSpec)],
) -> Result<WindowSpec, ExecError> {
    let Some(base_name) = &spec.base else {
        return Ok(spec.clone());
    };
    let base = lookup_window(base_name, resolved)?;
    if !spec.partition_by.is_empty() {
        return Err(windowing_error(format!(
            "cannot override PARTITION BY clause of window \"{base_name}\""
        )));
    }
    if !base.order_by.is_empty() && !spec.order_by.is_empty() {
        return Err(windowing_error(format!(
            "cannot override ORDER BY clause of window \"{base_name}\""
        )));
    }
    if base.frame.is_some() {
        return Err(windowing_error(format!(
            "cannot copy window \"{base_name}\" because it has a frame clause"
        )));
    }
    Ok(WindowSpec {
        base: None,
        partition_by: base.partition_by.clone(),
        order_by: if spec.order_by.is_empty() {
            base.order_by.clone()
        } else {
            spec.order_by.clone()
        },
        frame: spec.frame.clone(),
    })
}

/// Validate one call's shape and resolve its result type against `scope`, which
/// is the scope its arguments are written against.
fn plan_call(
    call: &WindowCall,
    windows: &[(String, WindowSpec)],
    scope: &Scope,
) -> Result<PlannedCall, ExecError> {
    let func = WindowFunc::classify(&call.name);
    let spec = match &call.over {
        WindowRef::Named(name) => lookup_window(name, windows)?.clone(),
        WindowRef::Spec(spec) => merge_base(spec, windows)?,
    };
    if call.distinct {
        return Err(unsupported(
            "DISTINCT is not implemented for window functions",
        ));
    }
    if call.filter.is_some() && func != WindowFunc::Aggregate {
        return Err(unsupported(
            "FILTER is not implemented for non-aggregate window functions",
        ));
    }
    let plain = FuncCall {
        sql_syntax: false,
        name: call.name.clone(),
        distinct: false,
        args: call.args.clone(),
        // A window call cannot carry a per-call sort: the parser refuses
        // `agg(x ORDER BY y) OVER (…)` exactly as PostgreSQL does.
        order_by: Vec::new(),
        filter: None,
    };
    let args = match &plain.args {
        FuncArgs::Star => &[][..],
        FuncArgs::Exprs(args) => args,
    };
    let result_ty = match func {
        WindowFunc::Aggregate if !crate::agg::is_aggregate_name(&plain.name) => {
            return Err(over_on_plain_function(&plain, scope));
        }
        WindowFunc::Aggregate => crate::agg::func_result_type(&plain, scope)?,
        WindowFunc::RowNumber | WindowFunc::Rank | WindowFunc::DenseRank => {
            require_arity(&call.name, args, 0, 0, scope)?;
            ColumnType::Int8
        }
        WindowFunc::PercentRank | WindowFunc::CumeDist => {
            require_arity(&call.name, args, 0, 0, scope)?;
            ColumnType::Float8
        }
        WindowFunc::Ntile => {
            require_arity(&call.name, args, 1, 1, scope)?;
            require_integer_arg(&call.name, args, 0, scope)?;
            ColumnType::Int4
        }
        WindowFunc::Lag | WindowFunc::Lead => {
            require_arity(&call.name, args, 1, 3, scope)?;
            require_integer_arg(&call.name, args, 1, scope)?;
            // `lag`/`lead` are declared over `anycompatible`, so the value and
            // the default resolve to ONE type and both are delivered as it.
            compatible_value_type(&call.name, args, scope)?
        }
        WindowFunc::FirstValue | WindowFunc::LastValue => {
            require_arity(&call.name, args, 1, 1, scope)?;
            crate::eval::infer_type(&args[0], scope)?
        }
        WindowFunc::NthValue => {
            require_arity(&call.name, args, 2, 2, scope)?;
            require_integer_arg(&call.name, args, 1, scope)?;
            crate::eval::infer_type(&args[0], scope)?
        }
    };
    for expr in &spec.partition_by {
        crate::eval::require_equality_operator(crate::eval::infer_type(expr, scope)?)?;
    }
    for item in &spec.order_by {
        crate::eval::require_ordering_operator(crate::eval::infer_type(&item.expr, scope)?)?;
    }
    let range_offset_ty = validate_frame(&spec, scope)?;
    Ok(PlannedCall {
        func,
        call: plain,
        filter: call.filter.clone(),
        spec,
        result_ty,
        range_offset_ty,
    })
}

/// A non-window, non-aggregate name written with `OVER`.
///
/// `PostgreSQL` reports 42809 when the call resolves to a real function and
/// 42883 when nothing of that name and argument list exists at all, so the
/// scalar resolver decides which one. `upper(t) OVER ()` is 42809.
/// `nosuchfn() OVER ()` and `abs(t) OVER ()` are 42883.
fn over_on_plain_function(call: &FuncCall, scope: &Scope) -> ExecError {
    match crate::eval::infer_type(&Expr::Func(call.clone()), scope) {
        Ok(_) => ExecError::FunctionError {
            sqlstate: "42809",
            message: format!(
                "OVER specified, but {} is not a window function nor an aggregate function",
                call.name
            ),
        },
        Err(error) => error,
    }
}

/// `PostgreSQL`'s 42883 for a window call no signature accepts, with the
/// argument types spelled the way its "function … does not exist" message
/// spells them.
fn undefined_window_function(name: &str, args: &[Expr], scope: &Scope) -> ExecError {
    let mut spelled = Vec::with_capacity(args.len());
    for arg in args {
        spelled.push(match crate::eval::infer_type(arg, scope) {
            Ok(ty) => ty.name().to_string(),
            Err(_) => "unknown".to_string(),
        });
    }
    ExecError::UndefinedFunction(format!(
        "function {name}({}) does not exist",
        spelled.join(", ")
    ))
}

fn require_arity(
    name: &str,
    args: &[Expr],
    min: usize,
    max: usize,
    scope: &Scope,
) -> Result<(), ExecError> {
    if args.len() >= min && args.len() <= max {
        return Ok(());
    }
    Err(undefined_window_function(name, args, scope))
}

/// The counting argument of `ntile`/`lag`/`lead`/`nth_value` is declared
/// `integer`. `PostgreSQL` widens `smallint` into it implicitly and lets an
/// `unknown` literal adopt it, but there is no `bigint` or `numeric` overload.
/// `lag(v, 1::bigint)` is 42883 there, not a narrowing cast.
fn require_integer_arg(
    name: &str,
    args: &[Expr],
    index: usize,
    scope: &Scope,
) -> Result<(), ExecError> {
    let Some(arg) = args.get(index) else {
        return Ok(());
    };
    let given = crate::eval::static_arg_types(std::slice::from_ref(arg), scope)?;
    match given[0].known() {
        None | Some(ColumnType::Int2 | ColumnType::Int4) => Ok(()),
        Some(_) => Err(undefined_window_function(name, args, scope)),
    }
}

/// `lag`/`lead`'s `anycompatible` result: the value argument's type unified with
/// the default's. An `unknown` literal on either side adopts the other. A pair
/// with no common type is 42883, exactly as an unresolvable `anycompatible` is
/// in `PostgreSQL`.
fn compatible_value_type(
    name: &str,
    args: &[Expr],
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    let given = crate::eval::static_arg_types(args, scope)?;
    let value = given[0].known();
    let default = given.get(2).and_then(|arg| arg.known());
    match (value, default) {
        (Some(value), Some(default)) => crate::eval::unify_types(value, default)
            .map_err(|_| undefined_window_function(name, args, scope)),
        // Both `unknown` leaves the polymorphic parameter unresolved in
        // PostgreSQL; this codebase types a bare literal as `text`, which is
        // what such a call would print anyway.
        (value, default) => Ok(value.or(default).unwrap_or(ColumnType::Text)),
    }
}

/// The scope bindings a SELECT's window results occupy, appended to `scope`.
fn extend_scope(scope: &Scope, calls: &[PlannedCall], names: &[String]) -> Scope {
    let mut extended = scope.clone();
    for (index, (call, label)) in calls.iter().zip(names).enumerate() {
        extended.columns.push(ColumnBinding {
            exposure: Exposure::Output,
            qualifier: Some(crabka_pgparser::ast::WINDOW_QUALIFIER.to_string()),
            name: crabka_pgparser::ast::window_binding_name(index, label),
            ty: call.result_ty,
        });
    }
    extended
}

/// The scope a SELECT's expressions resolve against once its window results are
/// bound. `RowDescription` is derived from this shape, for `Describe` as much
/// as for execution.
pub(crate) fn describe_scope(s: &SelectStmt, scope: &Scope) -> Result<Scope, ExecError> {
    let windows = resolve_window_clause(&s.windows)?;
    let mut calls = Vec::with_capacity(s.window_calls.len());
    for call in &s.window_calls {
        calls.push(plan_call(call, &windows, scope)?);
    }
    let names: Vec<String> = s.window_calls.iter().map(|c| c.name.clone()).collect();
    Ok(extend_scope(scope, &calls, &names))
}

/// A window query's projected output: the field descriptions, their types, and
/// the rows.
pub(crate) type WindowOutput = (Vec<FieldDescription>, Vec<ColumnType>, Vec<Vec<Datum>>);

/// Run a SELECT whose projection, `DISTINCT ON` keys or `ORDER BY` calls window
/// functions, over the rows `WHERE` already kept.
///
/// Returns the output field descriptions, their types, and the projected rows.
/// `DISTINCT`, `ORDER BY`, `OFFSET` and `LIMIT` are all applied, exactly as the
/// non-window path applies them.
/// Run a window query under the enclosing statement's shared blocking-memory
/// budget.
pub(crate) fn execute_with_memory(
    s: &SelectStmt,
    scope: &Scope,
    rows: Vec<Vec<Datum>>,
    ctx: &EvalCtx,
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<WindowOutput, ExecError> {
    let windows = resolve_window_clause(&s.windows)?;
    let mut calls = Vec::with_capacity(s.window_calls.len());
    for call in &s.window_calls {
        calls.push(plan_call(call, &windows, scope)?);
    }
    let names: Vec<String> = s.window_calls.iter().map(|c| c.name.clone()).collect();

    // Field names and types come from the ORIGINAL select list, resolved against
    // the source scope widened with this SELECT's window results.
    let projection_scope = extend_scope(scope, &calls, &names);
    let (fields, out_exprs, tys) =
        crate::exec::resolve_projection(&s.projection, &projection_scope)?;

    // A grouped query's window functions run over the GROUPED rows, so the whole
    // select list — and the window specs themselves — are re-expressed against
    // the aggregate output first.
    let lowered = lower_over_grouping(
        s,
        scope,
        &fields,
        &out_exprs,
        rows,
        ctx,
        statement_memory,
    )?;
    let calls = match &lowered.calls {
        Some(lowered_calls) => {
            let windows = resolve_window_clause(&s.windows)?;
            let mut replanned = Vec::with_capacity(lowered_calls.len());
            for call in lowered_calls {
                replanned.push(plan_call(call, &windows, &lowered.scope)?);
            }
            replanned
        }
        None => calls,
    };
    let base_scope = extend_scope(&lowered.scope, &calls, &names);

    let mut base_rows = lowered.rows;
    let values = evaluate_calls(
        &calls,
        &lowered.scope,
        &base_rows,
        ctx,
        statement_memory,
    )?;
    for (index, row) in base_rows.iter_mut().enumerate() {
        for column in &values {
            row.push(column[index].clone());
        }
    }

    // `DISTINCT ON (f() OVER …) … ORDER BY f() OVER …` writes the SAME call
    // twice, and each spelling is lifted onto its own entry — PostgreSQL matches
    // the two by comparing the parsed calls, so the sort and dedup keys are
    // canonicalized onto the first equal call before they are compared.
    let canonical = canonical_call_indices(s);
    let mut projected = s.clone();
    projected.order_by = lowered
        .order_by
        .into_iter()
        .map(|item| {
            Ok(OrderItem {
                expr: canonicalize_calls(&item.expr, &canonical, &names)?,
                ..item
            })
        })
        .collect::<Result<_, ExecError>>()?;
    projected.distinct = match lowered.distinct {
        DistinctClause::On(keys) => DistinctClause::On(
            keys.iter()
                .map(|key| canonicalize_calls(key, &canonical, &names))
                .collect::<Result<_, ExecError>>()?,
        ),
        other => other,
    };
    let rows = crate::exec::project_rows_ordered_with_memory(
        &projected,
        &base_scope,
        &fields,
        &lowered.out_exprs,
        base_rows,
        ctx,
        statement_memory,
    )?;
    Ok((fields, tys, rows))
}

/// For each window call, the index of the FIRST call equal to it. Two spellings
/// of one call are `equal()` in `PostgreSQL`'s parse tree, which is what makes
/// `DISTINCT ON (rank() OVER w) … ORDER BY rank() OVER w` a matching pair there.
fn canonical_call_indices(s: &SelectStmt) -> Vec<usize> {
    s.window_calls
        .iter()
        .enumerate()
        .map(|(index, call)| {
            s.window_calls
                .iter()
                .position(|other| other == call)
                .unwrap_or(index)
        })
        .collect()
}

/// Rewrite every window placeholder in `expr` onto its canonical call.
///
/// This function canonicalizes only the sort and dedup keys. The select list
/// keeps every call as written, because `PostgreSQL` evaluates two textually
/// identical calls separately. `first_value(random()) OVER w` written twice
/// gives two values.
fn canonicalize_calls(
    expr: &Expr,
    canonical: &[usize],
    names: &[String],
) -> Result<Expr, ExecError> {
    crate::grouping::rewrite(
        expr,
        &mut |node| {
            let Some(index) = crabka_pgparser::ast::window_placeholder_index(node) else {
                return Ok(None);
            };
            let Some((&canon, label)) = canonical.get(index).zip(names.get(index)) else {
                return Ok(None);
            };
            Ok((canon != index).then(|| crabka_pgparser::ast::window_placeholder(canon, label)))
        },
        true,
    )
}

/// The window plan node's input: the rows it runs over, the scope they resolve
/// against, and the select-list / sort / `DISTINCT ON` expressions rewritten to
/// match when the grouping step lowered them onto synthetic columns.
struct WindowInput {
    scope: Scope,
    rows: Vec<Vec<Datum>>,
    out_exprs: Vec<Expr>,
    order_by: Vec<OrderItem>,
    distinct: DistinctClause,
    /// The window calls rewritten against `scope`, when grouping lowered them.
    calls: Option<Vec<WindowCall>>,
}

/// Is this SELECT grouped? Does anything below the window node aggregate?
fn is_grouped(s: &SelectStmt, out_exprs: &[Expr], calls: &[WindowCall]) -> bool {
    !s.group_by.is_empty()
        // `GROUP BY GROUPING SETS (())` has no grouping expression at all, yet it
        // still folds the input to one row before the window node sees it.
        || s.grouping.is_some()
        || s.having.is_some()
        || out_exprs.iter().any(crate::agg::contains_aggregate)
        || s.order_by
            .iter()
            .any(|item| crate::agg::contains_aggregate(&item.expr))
        || calls.iter().any(|call| {
            let args = match &call.args {
                FuncArgs::Star => &[][..],
                FuncArgs::Exprs(args) => args,
            };
            args.iter().any(crate::agg::contains_aggregate)
                || call
                    .filter
                    .as_ref()
                    .is_some_and(crate::agg::contains_aggregate)
                || spec_exprs(&call.over).any(crate::agg::contains_aggregate)
        })
}

fn spec_exprs(over: &WindowRef) -> impl Iterator<Item = &Expr> {
    let spec = match over {
        WindowRef::Named(_) => None,
        WindowRef::Spec(spec) => Some(spec),
    };
    spec.into_iter().flat_map(|spec| {
        spec.partition_by
            .iter()
            .chain(spec.order_by.iter().map(|item| &item.expr))
    })
}

/// For a grouped query, fold `GROUP BY`/`HAVING` first and re-express every
/// window-free subexpression as a reference into the aggregate output. A
/// non-grouped query passes its rows through unchanged.
fn lower_over_grouping(
    s: &SelectStmt,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    rows: Vec<Vec<Datum>>,
    ctx: &EvalCtx,
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<WindowInput, ExecError> {
    if !is_grouped(s, out_exprs, &s.window_calls) {
        return Ok(WindowInput {
            scope: scope.clone(),
            rows,
            out_exprs: out_exprs.to_vec(),
            order_by: s.order_by.clone(),
            distinct: s.distinct.clone(),
            calls: None,
        });
    }
    let mut leaves: Vec<Expr> = Vec::new();
    let lowered_out: Vec<Expr> = out_exprs
        .iter()
        .map(|expr| split(expr, &mut leaves))
        .collect::<Result<_, ExecError>>()?;
    let mut order_by = Vec::with_capacity(s.order_by.len());
    for item in &s.order_by {
        // SQL92 output references (`ORDER BY 1`, `ORDER BY alias`) name the
        // select list, not a source expression, so they survive the rewrite as
        // written; everything else is an expression over the grouped rows.
        let expr = if matches!(item.expr, Expr::IntLiteral(_)) || is_output_label(s, &item.expr) {
            item.expr.clone()
        } else {
            split(&item.expr, &mut leaves)?
        };
        order_by.push(OrderItem {
            expr,
            asc: item.asc,
            nulls_first: item.nulls_first,
        });
    }
    let distinct = match &s.distinct {
        DistinctClause::On(keys) => DistinctClause::On(
            keys.iter()
                .map(|key| split(key, &mut leaves))
                .collect::<Result<_, ExecError>>()?,
        ),
        other => other.clone(),
    };

    // The window specs' own expressions are grouped-valid too (`rank() OVER
    // (ORDER BY sum(x))`), so they join the same leaf projection.
    let mut window_calls = Vec::with_capacity(s.window_calls.len());
    for call in &s.window_calls {
        window_calls.push(split_call(call, &mut leaves)?);
    }

    let inner = grouped_leaf_select(s, scope, fields, out_exprs, &leaves)?;
    // Through `crate::grouping`, not `crate::agg`: a grouping-set clause survives
    // into the leaf select, and it is that pass which expands it. Skipping it
    // would silently drop the clause and fold the input to one group per key.
    let leaf_rows = crate::grouping::aggregate_rows_with_memory(
        &inner,
        scope,
        rows,
        ctx,
        statement_memory,
    )?;
    let mut leaf_scope = Scope::empty();
    for (index, leaf) in leaves.iter().enumerate() {
        leaf_scope.columns.push(ColumnBinding {
            exposure: Exposure::Output,
            qualifier: Some(GROUPED_QUALIFIER.to_string()),
            name: grouped_binding_name(index),
            ty: crate::eval::infer_type(leaf, scope)?,
        });
    }
    Ok(WindowInput {
        scope: leaf_scope,
        rows: leaf_rows,
        out_exprs: lowered_out,
        order_by,
        distinct,
        calls: Some(window_calls),
    })
}

fn grouped_binding_name(index: usize) -> String {
    format!("{GROUPED_QUALIFIER}{index}")
}

fn grouped_leaf_expr(index: usize) -> Expr {
    Expr::Column {
        table: Some(GROUPED_QUALIFIER.to_string()),
        name: grouped_binding_name(index),
    }
}

/// The inner aggregate query whose output the window node reads: this SELECT's
/// `FROM`/`WHERE`/`GROUP BY`/`HAVING` with the leaf expressions as its select
/// list and every result-level modifier stripped.
///
/// The `GROUP BY` list is resolved against the *original* select list first,
/// because a SQL92 output reference (`GROUP BY 1`, `GROUP BY <alias>`) names a
/// column of that list, not of the leaf projection that replaces it. That list
/// is the one already resolved against the window-widened scope. A second
/// resolution against the source scope alone cannot see the window results, so
/// a window query's `GROUP BY 1` would report the select list's window column
/// as an unknown one.
fn grouped_leaf_select(
    s: &SelectStmt,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    leaves: &[Expr],
) -> Result<SelectStmt, ExecError> {
    let mut inner = s.clone();
    inner.group_by =
        crate::grouping::substitute_group_references(&s.group_by, scope, fields, out_exprs)?;
    // A resolved output reference may itself be a window call, which is a window
    // function in GROUP BY however it was spelled.
    for group in &inner.group_by {
        reject_clause(Some(group), "GROUP BY")?;
    }
    inner.projection = leaves
        .iter()
        .map(|expr| SelectItem::Expr {
            expr: expr.clone(),
            alias: None,
        })
        .collect();
    inner.distinct = DistinctClause::All;
    inner.windows = Vec::new();
    inner.window_calls = Vec::new();
    inner.order_by = Vec::new();
    inner.limit = None;
    inner.offset = None;
    inner.with_ties = false;
    inner.locking = None;
    Ok(inner)
}

fn is_output_label(s: &SelectStmt, expr: &Expr) -> bool {
    let Expr::Column { table: None, name } = expr else {
        return false;
    };
    s.projection.iter().any(|item| match item {
        SelectItem::Expr {
            alias: Some(alias), ..
        } => alias == name,
        SelectItem::Expr { expr, alias: None } => {
            matches!(expr, Expr::Column { name: n, .. } if n == name)
        }
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    })
}

/// Replace every maximal window-free subexpression of `expr` with a reference to
/// the grouped leaf projection, and register it there on first sight.
fn split(expr: &Expr, leaves: &mut Vec<Expr>) -> Result<Expr, ExecError> {
    if crabka_pgparser::ast::window_placeholder_index(expr).is_some() {
        return Ok(expr.clone());
    }
    if !contains_placeholder(expr) {
        let index = match leaves.iter().position(|leaf| leaf == expr) {
            Some(index) => index,
            None => {
                leaves.push(expr.clone());
                leaves.len() - 1
            }
        };
        return Ok(grouped_leaf_expr(index));
    }
    let rebuilt = match expr {
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: Box::new(split(expr, leaves)?),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(split(left, leaves)?),
            right: Box::new(split(right, leaves)?),
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(split(expr, leaves)?),
            negated: *negated,
        },
        Expr::Cast { expr, ty } => Expr::Cast {
            expr: Box::new(split(expr, leaves)?),
            ty: *ty,
        },
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            let mut cases = Vec::with_capacity(whens.len());
            for (condition, result) in whens {
                cases.push((split(condition, leaves)?, split(result, leaves)?));
            }
            Expr::Case {
                operand: match operand {
                    Some(operand) => Some(Box::new(split(operand, leaves)?)),
                    None => None,
                },
                whens: cases,
                else_result: match else_result {
                    Some(result) => Some(Box::new(split(result, leaves)?)),
                    None => None,
                },
            }
        }
        Expr::Func(call) => {
            let FuncArgs::Exprs(args) = &call.args else {
                return Err(unsupported(
                    "a window function cannot appear inside a star-argument call",
                ));
            };
            let mut lowered = Vec::with_capacity(args.len());
            for arg in args {
                lowered.push(split(arg, leaves)?);
            }
            Expr::Func(FuncCall {
                sql_syntax: call.sql_syntax,
                name: call.name.clone(),
                distinct: call.distinct,
                args: FuncArgs::Exprs(lowered),
                // An aggregate's own sort travels with it when a sibling window
                // call is lifted out of the same expression.
                order_by: call.order_by.clone(),
                filter: call.filter.clone(),
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(split(expr, leaves)?),
            list: split_all(list, leaves)?,
            negated: *negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(split(expr, leaves)?),
            low: Box::new(split(low, leaves)?),
            high: Box::new(split(high, leaves)?),
            negated: *negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            kind,
            escape,
        } => Expr::Like {
            expr: Box::new(split(expr, leaves)?),
            pattern: Box::new(split(pattern, leaves)?),
            negated: *negated,
            kind: *kind,
            escape: match escape {
                Some(escape) => Some(Box::new(split(escape, leaves)?)),
                None => None,
            },
        },
        Expr::ArrayLiteral(items) => Expr::ArrayLiteral(split_all(items, leaves)?),
        Expr::Row(items) => Expr::Row(split_all(items, leaves)?),
        Expr::Subscript { base, index } => Expr::Subscript {
            base: Box::new(split(base, leaves)?),
            index: Box::new(split(index, leaves)?),
        },
        Expr::QuantifiedArray {
            expr,
            op,
            all,
            array,
        } => Expr::QuantifiedArray {
            expr: Box::new(split(expr, leaves)?),
            op: *op,
            all: *all,
            array: Box::new(split(array, leaves)?),
        },
        other => {
            return Err(unsupported(format!(
                "a window function is not supported inside this expression: {other:?}"
            )));
        }
    };
    Ok(rebuilt)
}

fn split_all(exprs: &[Expr], leaves: &mut Vec<Expr>) -> Result<Vec<Expr>, ExecError> {
    exprs.iter().map(|expr| split(expr, leaves)).collect()
}

fn split_call(call: &WindowCall, leaves: &mut Vec<Expr>) -> Result<WindowCall, ExecError> {
    let args = match &call.args {
        FuncArgs::Star => FuncArgs::Star,
        FuncArgs::Exprs(args) => FuncArgs::Exprs(
            args.iter()
                .map(|arg| split(arg, leaves))
                .collect::<Result<_, ExecError>>()?,
        ),
    };
    let over = match &call.over {
        WindowRef::Named(name) => WindowRef::Named(name.clone()),
        WindowRef::Spec(spec) => WindowRef::Spec(Box::new(split_spec(spec, leaves)?)),
    };
    Ok(WindowCall {
        name: call.name.clone(),
        distinct: call.distinct,
        args,
        filter: match &call.filter {
            Some(filter) => Some(split(filter, leaves)?),
            None => None,
        },
        over,
    })
}

fn split_spec(spec: &WindowSpec, leaves: &mut Vec<Expr>) -> Result<WindowSpec, ExecError> {
    let mut order_by = Vec::with_capacity(spec.order_by.len());
    for item in &spec.order_by {
        order_by.push(OrderItem {
            expr: split(&item.expr, leaves)?,
            asc: item.asc,
            nulls_first: item.nulls_first,
        });
    }
    Ok(WindowSpec {
        base: spec.base.clone(),
        partition_by: spec
            .partition_by
            .iter()
            .map(|expr| split(expr, leaves))
            .collect::<Result<_, ExecError>>()?,
        order_by,
        frame: spec.frame.clone(),
    })
}

/// Compute every call's value for every base row: `result[call][row]`.
fn evaluate_calls(
    calls: &[PlannedCall],
    scope: &Scope,
    rows: &[Vec<Datum>],
    ctx: &EvalCtx,
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut out = Vec::with_capacity(calls.len());
    for call in calls {
        out.push(evaluate_call(call, scope, rows, ctx, statement_memory)?);
    }
    Ok(out)
}

fn evaluate_call(
    call: &PlannedCall,
    scope: &Scope,
    rows: &[Vec<Datum>],
    ctx: &EvalCtx,
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<Vec<Datum>, ExecError> {
    let order_by = &call.spec.order_by;
    let sort_keys = eval_key_rows(order_by.iter().map(|item| &item.expr), scope, rows, ctx)?;
    let partition_keys = eval_key_rows(call.spec.partition_by.iter(), scope, rows, ctx)?;
    let frame = resolved_frame(call, ctx)?;

    let mut values = vec![Datum::Null; rows.len()];
    for partition in partitions(&partition_keys, rows.len()) {
        let mut ordered = partition;
        ordered.sort_by(|a, b| order_cmp(&sort_keys[*a], &sort_keys[*b], order_by));
        let peers = peer_groups(&ordered, &sort_keys, order_by);
        let partition = Partition {
            ordered: &ordered,
            peers: &peers,
            sort_keys: &sort_keys,
        };
        if let Some(prefix) =
            evaluate_default_prefix_aggregate(call, &frame, &partition, scope, rows, ctx)?
        {
            for (row, value) in prefix {
                values[row] = value;
            }
            continue;
        }
        // `ntile` is the one window function whose argument PostgreSQL reads
        // once per PARTITION rather than once per row, so its bucket run is
        // carried across the positions below.
        let mut buckets = NtileState::default();
        for position in 0..ordered.len() {
            let value = evaluate_position(
                &FramedCall {
                    call,
                    frame: &frame,
                    statement_memory,
                },
                &partition,
                position,
                &mut buckets,
                scope,
                rows,
                ctx,
            )?;
            values[ordered[position]] = value;
        }
    }
    Ok(values)
}

fn evaluate_default_prefix_aggregate(
    call: &PlannedCall,
    frame: &ResolvedFrame,
    partition: &Partition<'_>,
    scope: &Scope,
    rows: &[Vec<Datum>],
    ctx: &EvalCtx,
) -> Result<Option<Vec<(usize, Datum)>>, ExecError> {
    if call.func != WindowFunc::Aggregate
        || !matches!(frame, ResolvedFrame::Default)
        || !matches!(call.call.name.as_str(), "count" | "sum")
    {
        return Ok(None);
    }
    let is_count = call.call.name == "count";
    let argument = match &call.call.args {
        FuncArgs::Star if is_count => None,
        FuncArgs::Exprs(args) => args.first(),
        FuncArgs::Star => return Ok(None),
    };
    let mut count = 0i64;
    let mut sum: Option<Datum> = None;
    let mut values = Vec::with_capacity(partition.ordered.len());
    let mut consumed = 0usize;
    for &(first, last) in &partition.peers.bounds {
        for position in consumed..=last {
            let row = &rows[partition.ordered[position]];
            if let Some(filter) = &call.filter
                && crate::eval::eval(filter, scope, row, ctx)? != Datum::Bool(true)
            {
                continue;
            }
            let value = argument
                .map(|argument| crate::eval::eval(argument, scope, row, ctx))
                .transpose()?;
            if is_count {
                if value.as_ref().is_none_or(|value| !value.is_null()) {
                    count = count
                        .checked_add(1)
                        .ok_or(crabka_pgtypes::TypeError::Overflow)?;
                }
            } else if let Some(value) = value
                && !value.is_null()
            {
                let value = crabka_pgtypes::cast::cast(&value, call.result_ty, &ctx.time_zone)?;
                sum = Some(match sum {
                    Some(current) => crabka_pgtypes::ops::add(&current, &value)?,
                    None => value,
                });
            }
        }
        consumed = last + 1;
        let value = if is_count {
            Datum::Int8(count)
        } else {
            sum.clone().unwrap_or(Datum::Null)
        };
        for position in first..=last {
            values.push((partition.ordered[position], value.clone()));
        }
    }
    Ok(Some(values))
}

/// One partition's rows in window order, with their peer-group structure.
struct Partition<'a> {
    /// Base-row indices, in the window's `ORDER BY` order.
    ordered: &'a [usize],
    peers: &'a PeerGroups,
    sort_keys: &'a [Vec<Datum>],
}

/// Where one row sits in its ordered partition: its position and the peer run
/// it belongs to. Every frame bound is a function of these.
#[derive(Clone, Copy)]
struct RowPlace {
    position: usize,
    group: usize,
    peer_first: usize,
    peer_last: usize,
}

impl RowPlace {
    fn of(partition: &Partition<'_>, position: usize) -> Self {
        let group = partition.peers.group_of[position];
        let (peer_first, peer_last) = partition.peers.bounds[group];
        Self {
            position,
            group,
            peer_first,
            peer_last,
        }
    }
}

/// Which end of the frame a bound resolves, and which way the window sorts.
#[derive(Clone, Copy)]
struct BoundSide {
    is_start: bool,
    ascending: bool,
}

/// Peer-group structure of one ordered partition, by position within it.
struct PeerGroups {
    /// The peer group each position belongs to.
    group_of: Vec<usize>,
    /// Each peer group's inclusive `[first, last]` position range.
    bounds: Vec<(usize, usize)>,
}

fn peer_groups(ordered: &[usize], sort_keys: &[Vec<Datum>], order_by: &[OrderItem]) -> PeerGroups {
    let mut group_of = Vec::with_capacity(ordered.len());
    let mut bounds: Vec<(usize, usize)> = Vec::new();
    for (position, row) in ordered.iter().enumerate() {
        let same_as_previous = position > 0
            && order_cmp(
                &sort_keys[ordered[position - 1]],
                &sort_keys[*row],
                order_by,
            ) == Ordering::Equal;
        if same_as_previous {
            let last = bounds.len() - 1;
            bounds[last].1 = position;
        } else {
            bounds.push((position, position));
        }
        group_of.push(bounds.len() - 1);
    }
    PeerGroups { group_of, bounds }
}

fn eval_key_rows<'a>(
    exprs: impl Iterator<Item = &'a Expr>,
    scope: &Scope,
    rows: &[Vec<Datum>],
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    // PARTITION BY / ORDER BY keys are the same expressions for every row, so
    // their column references are resolved once here.
    let exprs: Vec<crate::bind::BoundExpr> = exprs
        .map(|expr| crate::bind::BoundExpr::new(expr, scope))
        .collect::<Result<_, _>>()?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut key = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            key.push(crate::eval::eval(expr.expr(), scope, row, ctx)?);
        }
        out.push(key);
    }
    Ok(out)
}

fn order_cmp(a: &[Datum], b: &[Datum], order_by: &[OrderItem]) -> Ordering {
    crate::exec::order_cmp(a, b, order_by)
}

/// Group row indices by partition key, and keep first-appearance order.
fn partitions(keys: &[Vec<Datum>], len: usize) -> Vec<Vec<usize>> {
    if keys.first().is_none_or(Vec::is_empty) {
        return vec![(0..len).collect()];
    }
    let mut index: HashMap<&Vec<Datum>, usize> = HashMap::new();
    let mut out: Vec<Vec<usize>> = Vec::new();
    for (row, key) in keys.iter().enumerate() {
        match index.get(key) {
            Some(&existing) => out[existing].push(row),
            None => {
                index.insert(key, out.len());
                out.push(vec![row]);
            }
        }
    }
    out
}

/// A frame whose offsets have been evaluated. The offsets may not reference the
/// row.
enum ResolvedFrame {
    /// `PostgreSQL`'s default: `RANGE UNBOUNDED PRECEDING AND CURRENT ROW`.
    Default,
    Explicit {
        mode: FrameMode,
        start: ResolvedBound,
        end: Box<ResolvedBound>,
        exclusion: FrameExclusion,
    },
}

enum ResolvedBound {
    UnboundedPreceding,
    Preceding(Datum),
    CurrentRow,
    Following(Datum),
    UnboundedFollowing,
}

impl ResolvedFrame {
    fn exclusion(&self) -> FrameExclusion {
        match self {
            Self::Default => FrameExclusion::NoOthers,
            Self::Explicit { exclusion, .. } => *exclusion,
        }
    }
}

/// Every frame check `PostgreSQL` makes during parse analysis, so a malformed
/// frame is refused before any row is read and reported by `Describe` too.
///
/// Returns the type an `unknown` literal offset adopts, for the caller to store
/// on the planned call.
fn validate_frame(spec: &WindowSpec, scope: &Scope) -> Result<Option<ColumnType>, ExecError> {
    let Some(frame) = &spec.frame else {
        return Ok(None);
    };
    validate_frame_shape(frame)?;
    // GROUPS counts PEER GROUPS, which only an ORDER BY defines, so PostgreSQL
    // requires one whatever the bounds are — unlike RANGE, which needs one only
    // when a bound carries an offset.
    if frame.mode == FrameMode::Groups && spec.order_by.is_empty() {
        return Err(windowing_error("GROUPS mode requires an ORDER BY clause"));
    }
    if !frame_has_offset(frame) {
        return Ok(None);
    }
    match frame.mode {
        FrameMode::Range => validate_range_offsets(frame, &spec.order_by, scope),
        // ROWS and GROUPS count rows, so their offset is declared `bigint` and
        // PostgreSQL rejects anything outside the numeric tower during parse
        // analysis — before it notices the offset also references the row.
        FrameMode::Rows | FrameMode::Groups => {
            validate_row_count_offsets(frame, scope)?;
            Ok(None)
        }
    }
}

/// `PostgreSQL`'s 42804 for a `ROWS`/`GROUPS` offset of a type that is not a
/// count. An offset whose type cannot be resolved at all is left to the runtime
/// check, which is where `PostgreSQL` reports it too.
fn validate_row_count_offsets(frame: &WindowFrame, scope: &Scope) -> Result<(), ExecError> {
    for bound in [&frame.start, &frame.end] {
        let (FrameBound::Preceding(offset) | FrameBound::Following(offset)) = bound else {
            continue;
        };
        let given = crate::eval::static_arg_types(std::slice::from_ref(offset), scope);
        let Ok(given) = given else { continue };
        let Some(ty) = given[0].known() else { continue };
        if !matches!(
            ty,
            ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 | ColumnType::Float4
        ) && ty != ColumnType::Float8
            && !ty.is_numeric()
        {
            return Err(ExecError::TypeMismatch(format!(
                "argument of {} must be type bigint, not type {}",
                mode_word(frame.mode),
                ty.name()
            )));
        }
    }
    Ok(())
}

fn resolved_frame(call: &PlannedCall, ctx: &EvalCtx) -> Result<ResolvedFrame, ExecError> {
    let Some(frame) = &call.spec.frame else {
        return Ok(ResolvedFrame::Default);
    };
    let unknown_ty = call.range_offset_ty;
    Ok(ResolvedFrame::Explicit {
        mode: frame.mode,
        start: resolve_bound(&frame.start, frame.mode, "starting", unknown_ty, ctx)?,
        end: Box::new(resolve_bound(
            &frame.end, frame.mode, "ending", unknown_ty, ctx,
        )?),
        exclusion: frame.exclusion,
    })
}

fn frame_has_offset(frame: &WindowFrame) -> bool {
    matches!(
        frame.start,
        FrameBound::Preceding(_) | FrameBound::Following(_)
    ) || matches!(
        frame.end,
        FrameBound::Preceding(_) | FrameBound::Following(_)
    )
}

fn validate_frame_shape(frame: &WindowFrame) -> Result<(), ExecError> {
    if matches!(frame.start, FrameBound::UnboundedFollowing) {
        return Err(windowing_error("frame start cannot be UNBOUNDED FOLLOWING"));
    }
    if matches!(frame.end, FrameBound::UnboundedPreceding) {
        return Err(windowing_error("frame end cannot be UNBOUNDED PRECEDING"));
    }
    if matches!(frame.start, FrameBound::Following(_))
        && matches!(frame.end, FrameBound::Preceding(_) | FrameBound::CurrentRow)
    {
        return Err(windowing_error(
            "frame starting from following row cannot have preceding rows",
        ));
    }
    if matches!(frame.start, FrameBound::CurrentRow)
        && matches!(frame.end, FrameBound::Preceding(_))
    {
        return Err(windowing_error(
            "frame starting from current row cannot have preceding rows",
        ));
    }
    Ok(())
}

/// `RANGE` with an offset needs exactly one ordering column, and `PostgreSQL`
/// only has `in_range` support for a fixed set of (column, offset) type pairs.
fn validate_range_offsets(
    frame: &WindowFrame,
    order_by: &[OrderItem],
    scope: &Scope,
) -> Result<Option<ColumnType>, ExecError> {
    let [item] = order_by else {
        return Err(windowing_error(
            "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column",
        ));
    };
    let column = crate::eval::infer_type(&item.expr, scope)?;
    // An `unknown` literal offset (`RANGE '1' PRECEDING`, `RANGE NULL PRECEDING`)
    // adopts the type the ordering column's `in_range` support function declares,
    // rather than staying `text`.
    let adopted = range_offset_unknown_type(column);
    let mut used = None;
    for bound in [&frame.start, &frame.end] {
        let (FrameBound::Preceding(offset) | FrameBound::Following(offset)) = bound else {
            continue;
        };
        let given = crate::eval::static_arg_types(std::slice::from_ref(offset), scope)?;
        let offset_ty = match given[0].known() {
            Some(ty) => ty,
            None => match adopted {
                Some(ty) => {
                    used = adopted;
                    ty
                }
                // No support function to adopt from: report the column, as an
                // explicitly typed offset over the same column would.
                None => ColumnType::Text,
            },
        };
        if !range_offset_supported(column, offset_ty) {
            return Err(unsupported(range_offset_message(column, offset_ty)));
        }
    }
    Ok(used)
}

/// The offset type an `unknown` literal adopts for each ordering-column type.
/// This is the type `PostgreSQL`'s `in_range` support function for that column
/// declares, and it is what its "invalid input syntax for type …" message names
/// when the literal does not parse.
fn range_offset_unknown_type(column: ColumnType) -> Option<ColumnType> {
    match range_offset_base_type(column) {
        ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 | ColumnType::Numeric(_) => {
            Some(column)
        }
        // `in_range(float4, float8)` and `in_range(float8, float8)` both take a
        // double, so a bare literal is a double either way.
        ColumnType::Float4 | ColumnType::Float8 => Some(ColumnType::Float8),
        ColumnType::Date
        | ColumnType::Time
        | ColumnType::Timetz
        | ColumnType::Timestamp
        | ColumnType::Timestamptz
        | ColumnType::Interval => Some(ColumnType::Interval),
        _ => None,
    }
}

fn range_offset_message(column: ColumnType, offset: ColumnType) -> String {
    if range_offset_column_supported(column) {
        format!(
            "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {} and offset type {}",
            column.name(),
            offset.name()
        )
    } else {
        format!(
            "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {}",
            column.name()
        )
    }
}

fn range_offset_column_supported(column: ColumnType) -> bool {
    matches!(
        range_offset_base_type(column),
        ColumnType::Int2
            | ColumnType::Int4
            | ColumnType::Int8
            | ColumnType::Numeric(_)
            | ColumnType::Float4
            | ColumnType::Float8
            | ColumnType::Date
            | ColumnType::Time
            | ColumnType::Timetz
            | ColumnType::Timestamp
            | ColumnType::Timestamptz
            | ColumnType::Interval
    )
}

fn range_offset_supported(column: ColumnType, offset: ColumnType) -> bool {
    let column = range_offset_base_type(column);
    let offset = range_offset_base_type(offset);
    let integral = matches!(
        offset,
        ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8
    );
    match column {
        // PostgreSQL's integer `in_range` support functions take an integer
        // offset only — `RANGE 1.5 PRECEDING` over an `integer` column is an
        // error there, not a rounded offset.
        ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 => integral,
        ColumnType::Numeric(_) => integral || matches!(offset, ColumnType::Numeric(_)),
        ColumnType::Float4 | ColumnType::Float8 => {
            integral
                || matches!(
                    offset,
                    ColumnType::Numeric(_) | ColumnType::Float4 | ColumnType::Float8
                )
        }
        ColumnType::Date
        | ColumnType::Time
        | ColumnType::Timetz
        | ColumnType::Timestamp
        | ColumnType::Timestamptz
        | ColumnType::Interval => offset == ColumnType::Interval,
        _ => false,
    }
}

/// A temporal typmod does not change which `in_range` support function a
/// `RANGE` frame uses.
fn range_offset_base_type(column: ColumnType) -> ColumnType {
    column.temporal_base().map_or(column, |(base, _)| base)
}

fn mode_word(mode: FrameMode) -> &'static str {
    match mode {
        FrameMode::Rows => "ROWS",
        FrameMode::Range => "RANGE",
        FrameMode::Groups => "GROUPS",
    }
}

fn resolve_bound(
    bound: &FrameBound,
    mode: FrameMode,
    which: &str,
    unknown_ty: Option<ColumnType>,
    ctx: &EvalCtx,
) -> Result<ResolvedBound, ExecError> {
    let (offset, following) = match bound {
        FrameBound::UnboundedPreceding => return Ok(ResolvedBound::UnboundedPreceding),
        FrameBound::CurrentRow => return Ok(ResolvedBound::CurrentRow),
        FrameBound::UnboundedFollowing => return Ok(ResolvedBound::UnboundedFollowing),
        FrameBound::Preceding(offset) => (offset, false),
        FrameBound::Following(offset) => (offset, true),
    };
    // A frame offset may not reference the row, so it is evaluated against an
    // empty scope exactly once per statement — a column reference there is
    // PostgreSQL's "must not contain variables", not an unknown column.
    let mut value =
        crate::eval::eval(offset, &Scope::empty(), &[], ctx).map_err(|error| match error {
            ExecError::UndefinedColumn(_) | ExecError::MissingFromEntry(_) => {
                ExecError::InvalidColumnReference(format!(
                    "argument of {} must not contain variables",
                    mode_word(mode)
                ))
            }
            other => other,
        })?;
    // An `unknown` literal offset carries the type its `in_range` support
    // function declares, so it is converted here rather than being compared as
    // text: `RANGE 'x' PRECEDING` over an `integer` column is that type's input
    // error, not an unsupported offset type.
    if let Some(ty) = unknown_ty
        && crate::eval::is_unknown_literal(offset)
        && !value.is_null()
    {
        value = crabka_pgtypes::cast::cast(&value, ty, &ctx.time_zone)?;
    }
    if value.is_null() {
        return Err(ExecError::FunctionError {
            sqlstate: "22004",
            message: format!("frame {which} offset must not be null"),
        });
    }
    let value = match mode {
        FrameMode::Rows | FrameMode::Groups => {
            let count = crabka_pgtypes::cast::cast(&value, ColumnType::Int8, &ctx.time_zone)?;
            if matches!(count, Datum::Int8(n) if n < 0) {
                return Err(ExecError::FunctionError {
                    sqlstate: "22013",
                    message: format!("frame {which} offset must not be negative"),
                });
            }
            count
        }
        // A RANGE offset is only rejected where PostgreSQL's `in_range` support
        // function sees it — per row, and never for a row whose ordering value
        // is NULL. `RANGE BETWEEN -1 PRECEDING …` over an empty partition is
        // therefore not an error there, so it must not be one here either.
        FrameMode::Range => value,
    };
    Ok(if following {
        ResolvedBound::Following(value)
    } else {
        ResolvedBound::Preceding(value)
    })
}

/// `PostgreSQL`'s `in_range` rejection of a negative or `NaN` offset, raised
/// where `in_range` itself raises it: at the first row that consults the offset.
fn validate_range_offset(offset: &Datum, ctx: &EvalCtx) -> Result<(), ExecError> {
    if !(is_nan(offset) || is_negative(offset, ctx)?) {
        return Ok(());
    }
    Err(ExecError::FunctionError {
        sqlstate: "22013",
        message: "invalid preceding or following size in window function".into(),
    })
}

fn is_negative(value: &Datum, ctx: &EvalCtx) -> Result<bool, ExecError> {
    if let Datum::Interval(interval) = value {
        return Ok(interval.canonical_micros() < 0);
    }
    Ok(crate::eval::apply_binary(BinaryOp::Lt, value, &Datum::Int4(0), ctx)? == Datum::Bool(true))
}

/// Is `value` a floating-point or `numeric` `NaN`? `NaN` sorts above every other
/// value, so a row ordered by one has no arithmetic neighbourhood and `PostgreSQL`
/// frames it exactly like a NULL. `PostgreSQL` never admits it to another row's
/// frame.
fn is_nan(value: &Datum) -> bool {
    match value {
        Datum::Float4(f) => f.is_nan(),
        Datum::Float8(f) => f.is_nan(),
        Datum::Numeric(n) => n.is_nan(),
        _ => false,
    }
}

/// Is `value` an infinity of the type the ordering column carries?
fn is_infinite(value: &Datum) -> bool {
    match value {
        Datum::Float4(f) => f.is_infinite(),
        Datum::Float8(f) => f.is_infinite(),
        Datum::Numeric(n) => n.is_infinite(),
        _ => false,
    }
}

/// Is `value` a negative infinity? Only reached for a value [`is_infinite`]
/// already accepted.
fn is_negative_infinity(value: &Datum) -> bool {
    match value {
        Datum::Float4(f) => *f < 0.0,
        Datum::Float8(f) => *f < 0.0,
        Datum::Numeric(n) => matches!(n, crabka_pgtypes::numeric::NumericValue::NegInfinity),
        _ => false,
    }
}

fn offset_count(value: &Datum) -> usize {
    match value {
        Datum::Int8(n) => usize::try_from(*n).unwrap_or(usize::MAX),
        _ => 0,
    }
}

/// One planned window call together with the frame its `OVER` clause resolved
/// to. Every position of the partition is evaluated against this pair.
struct FramedCall<'a> {
    call: &'a PlannedCall,
    frame: &'a ResolvedFrame,
    statement_memory: &'a crate::scanner::StatementMemory,
}

fn evaluate_position(
    framed: &FramedCall<'_>,
    partition: &Partition<'_>,
    position: usize,
    buckets: &mut NtileState,
    scope: &Scope,
    rows: &[Vec<Datum>],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let FramedCall {
        call,
        frame,
        statement_memory,
    } = framed;
    let ascending = call.spec.order_by.first().is_none_or(|item| item.asc);
    let total = partition.ordered.len();
    let place = RowPlace::of(partition, position);
    let (group, peer_first, peer_last) = (place.group, place.peer_first, place.peer_last);
    match call.func {
        WindowFunc::RowNumber => return Ok(Datum::Int8(as_i64(position + 1))),
        WindowFunc::Rank => return Ok(Datum::Int8(as_i64(peer_first + 1))),
        WindowFunc::DenseRank => return Ok(Datum::Int8(as_i64(group + 1))),
        WindowFunc::PercentRank => {
            let value = if total <= 1 {
                0.0
            } else {
                as_f64(peer_first) / as_f64(total - 1)
            };
            return Ok(Datum::Float8(value));
        }
        WindowFunc::CumeDist => {
            return Ok(Datum::Float8(as_f64(peer_last + 1) / as_f64(total)));
        }
        WindowFunc::Ntile => {
            let source_row = &rows[partition.ordered[position]];
            let requested = crate::eval::eval(&call.args()[0], scope, source_row, ctx)?;
            return buckets.next_bucket(&requested, total, &ctx.time_zone);
        }
        WindowFunc::Lag | WindowFunc::Lead => {
            return offset_value(call, partition, position, scope, rows, ctx);
        }
        _ => {}
    }
    let frame_rows = frame_positions(frame, partition, place, ascending, ctx)?;
    let frame_rows = apply_exclusion(frame_rows, frame.exclusion(), place);
    let source_row = &rows[partition.ordered[position]];
    match call.func {
        WindowFunc::FirstValue => {
            nth_frame_value(call, partition, &frame_rows, 0, scope, rows, ctx)
        }
        WindowFunc::LastValue => {
            let last = frame_rows.len().checked_sub(1);
            match last {
                Some(last) => nth_frame_value(call, partition, &frame_rows, last, scope, rows, ctx),
                None => Ok(Datum::Null),
            }
        }
        WindowFunc::NthValue => {
            let n = crate::eval::eval(&call.args()[1], scope, source_row, ctx)?;
            let Some(n) = positive_count(&n, "nth_value", "22016", &ctx.time_zone)? else {
                return Ok(Datum::Null);
            };
            match usize::try_from(n - 1) {
                Ok(index) if index < frame_rows.len() => {
                    nth_frame_value(call, partition, &frame_rows, index, scope, rows, ctx)
                }
                _ => Ok(Datum::Null),
            }
        }
        _ => aggregate_over_frame(
            call,
            partition,
            &frame_rows,
            scope,
            rows,
            ctx,
            statement_memory,
        ),
    }
}

fn nth_frame_value(
    call: &PlannedCall,
    partition: &Partition<'_>,
    frame_rows: &[usize],
    index: usize,
    scope: &Scope,
    rows: &[Vec<Datum>],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    match frame_rows.get(index) {
        Some(&position) => crate::eval::eval(
            &call.args()[0],
            scope,
            &rows[partition.ordered[position]],
            ctx,
        ),
        None => Ok(Datum::Null),
    }
}

/// `lag`/`lead`: the value `offset` rows behind/ahead in the partition,
/// independent of the frame. It falls back to the `default` argument.
fn offset_value(
    call: &PlannedCall,
    partition: &Partition<'_>,
    position: usize,
    scope: &Scope,
    rows: &[Vec<Datum>],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let source_row = &rows[partition.ordered[position]];
    let args = call.args();
    let offset = match args.get(1) {
        Some(expr) => {
            let value = crate::eval::eval(expr, scope, source_row, ctx)?;
            if value.is_null() {
                return Ok(Datum::Null);
            }
            match crabka_pgtypes::cast::cast(&value, ColumnType::Int8, &ctx.time_zone)? {
                Datum::Int8(n) => n,
                _ => 1,
            }
        }
        None => 1,
    };
    let signed = if call.func == WindowFunc::Lag {
        -offset
    } else {
        offset
    };
    let target = i64::try_from(position)
        .ok()
        .and_then(|position| position.checked_add(signed))
        .and_then(|target| usize::try_from(target).ok())
        .filter(|target| *target < partition.ordered.len());
    let value = match target {
        Some(target) => crate::eval::eval(&args[0], scope, &rows[partition.ordered[target]], ctx)?,
        None => match args.get(2) {
            Some(default) => crate::eval::eval(default, scope, source_row, ctx)?,
            None => Datum::Null,
        },
    };
    // `anycompatible` makes ONE type of the value and the default, so the column
    // never carries a datum of the other's type — a `RowDescription` saying
    // `integer` must not be followed by the text a `lag(v, 1, 'zzz')` default
    // would otherwise emit.
    if value.is_null() || value.column_type() == Some(call.result_ty) {
        return Ok(value);
    }
    Ok(crabka_pgtypes::cast::cast(
        &value,
        call.result_ty,
        &ctx.time_zone,
    )?)
}

/// `ntile`'s bucket run over one partition, which reproduces `PostgreSQL`'s
/// streaming `window_ntile` exactly.
///
/// The bucket count is read from the argument ONCE per partition and reused for
/// every later row, so `ntile(<non-constant>)` follows the partition's FIRST row
/// in window order and a zero on any later row is never even looked at. A NULL
/// there is the one case that does not arm the run. That row alone is NULL and
/// the next row re-reads the argument, which is what `PostgreSQL`'s
/// "first call" test does when it returns NULL before storing any state.
#[derive(Default)]
struct NtileState {
    /// The bucket the run is currently in, or 0 while the run is unarmed.
    bucket: i32,
    rows_in_bucket: i64,
    boundary: i64,
    remainder: i64,
}

impl NtileState {
    fn next_bucket(
        &mut self,
        requested: &Datum,
        total: usize,
        tz: &jiff::tz::TimeZone,
    ) -> Result<Datum, ExecError> {
        if self.bucket == 0 {
            let Some(buckets) = positive_count(requested, "ntile", "22014", tz)? else {
                return Ok(Datum::Null);
            };
            let total = as_i64(total);
            self.bucket = 1;
            self.rows_in_bucket = 0;
            self.boundary = total / buckets;
            if self.boundary <= 0 {
                // More buckets than rows: every row is its own bucket.
                self.boundary = 1;
            } else {
                // Rows spread evenly over the buckets, with the leftover going
                // one each to the leading `total % buckets` buckets.
                self.remainder = total % buckets;
                self.boundary += i64::from(self.remainder != 0);
            }
        }
        self.rows_in_bucket += 1;
        if self.boundary < self.rows_in_bucket {
            if self.remainder != 0 {
                self.remainder -= 1;
                if self.remainder == 0 {
                    self.boundary -= 1;
                }
            }
            self.bucket += 1;
            self.rows_in_bucket = 1;
        }
        Ok(Datum::Int4(self.bucket))
    }
}

/// An `ntile`/`nth_value` count argument: NULL yields NULL, zero or negative is
/// that function's own SQLSTATE.
fn positive_count(
    value: &Datum,
    func: &str,
    sqlstate: &'static str,
    tz: &jiff::tz::TimeZone,
) -> Result<Option<i64>, ExecError> {
    if value.is_null() {
        return Ok(None);
    }
    let count = match crabka_pgtypes::cast::cast(value, ColumnType::Int8, tz)? {
        Datum::Int8(n) => n,
        _ => 0,
    };
    if count <= 0 {
        return Err(ExecError::FunctionError {
            sqlstate,
            message: format!("argument of {func} must be greater than zero"),
        });
    }
    Ok(Some(count))
}

/// Fold an ordinary aggregate over the frame's rows, and run it as a bare
/// aggregate query. Every aggregate the engine implements then works over a
/// window with no per-aggregate code here, including its empty-input value.
fn aggregate_over_frame(
    call: &PlannedCall,
    partition: &Partition<'_>,
    frame_rows: &[usize],
    scope: &Scope,
    rows: &[Vec<Datum>],
    ctx: &EvalCtx,
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<Datum, ExecError> {
    let mut input = Vec::with_capacity(frame_rows.len());
    for position in frame_rows {
        let row = &rows[partition.ordered[*position]];
        if let Some(filter) = &call.filter
            && crate::eval::eval(filter, scope, row, ctx)? != Datum::Bool(true)
        {
            continue;
        }
        input.push(row.clone());
    }
    let folded = crate::agg::aggregate_rows_with_memory(
        &bare_aggregate_select(&call.call),
        scope,
        input,
        ctx,
        statement_memory,
    )?;
    Ok(folded
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
        .unwrap_or(Datum::Null))
}

/// `SELECT <aggregate>` over the frame rows. There is no FROM, because the
/// caller supplies the rows directly. There is no grouping, and there are no
/// result-level modifiers.
fn bare_aggregate_select(call: &FuncCall) -> SelectStmt {
    SelectStmt {
        projection: vec![SelectItem::Expr {
            expr: Expr::Func(call.clone()),
            alias: None,
        }],
        from: Vec::new(),
        filter: None,
        distinct: DistinctClause::All,
        group_by: Vec::new(),
        grouping: None,
        having: None,
        windows: Vec::new(),
        window_calls: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        with_ties: false,
        locking: None,
    }
}

/// The positions within the ordered partition that the frame spans, before
/// `EXCLUDE` is applied.
fn frame_positions(
    frame: &ResolvedFrame,
    partition: &Partition<'_>,
    place: RowPlace,
    ascending: bool,
    ctx: &EvalCtx,
) -> Result<Vec<usize>, ExecError> {
    let total = partition.ordered.len();
    let side = |is_start| BoundSide {
        is_start,
        ascending,
    };
    let (start, end) = match frame {
        // The default frame runs from the partition start through the current
        // row's last peer.
        ResolvedFrame::Default => (0isize, as_isize(place.peer_last)),
        ResolvedFrame::Explicit {
            mode, start, end, ..
        } => (
            bound_position(*mode, start, side(true), partition, place, ctx)?,
            bound_position(*mode, end, side(false), partition, place, ctx)?,
        ),
    };
    let low = start.max(0);
    let high = end.min(as_isize(total) - 1);
    if low > high {
        return Ok(Vec::new());
    }
    let (low, high) = (
        usize::try_from(low).unwrap_or(0),
        usize::try_from(high).unwrap_or(0),
    );
    Ok((low..=high).collect())
}

fn as_isize(value: usize) -> isize {
    isize::try_from(value).unwrap_or(isize::MAX)
}

fn as_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// A row count as `f64` for `percent_rank`/`cume_dist`. The conversion is
/// lossless through `u32`. A partition wider than that cannot be materialized.
fn as_f64(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

/// Resolve one frame bound to a position. The position may fall outside
/// `0..total`, and the caller clamps it into an empty or truncated frame.
fn bound_position(
    mode: FrameMode,
    bound: &ResolvedBound,
    side: BoundSide,
    partition: &Partition<'_>,
    place: RowPlace,
    ctx: &EvalCtx,
) -> Result<isize, ExecError> {
    let total = partition.ordered.len();
    let RowPlace {
        position,
        group,
        peer_first,
        peer_last,
    } = place;
    let is_start = side.is_start;
    let groups = partition.peers.bounds.len();
    Ok(match bound {
        ResolvedBound::UnboundedPreceding => 0,
        ResolvedBound::UnboundedFollowing => as_isize(total) - 1,
        ResolvedBound::CurrentRow => match mode {
            // In ROWS mode CURRENT ROW is the row itself; in RANGE and GROUPS it
            // is the whole peer group.
            FrameMode::Rows => as_isize(position),
            FrameMode::Range | FrameMode::Groups => {
                if is_start {
                    as_isize(peer_first)
                } else {
                    as_isize(peer_last)
                }
            }
        },
        ResolvedBound::Preceding(offset) | ResolvedBound::Following(offset) => {
            let following = matches!(bound, ResolvedBound::Following(_));
            match mode {
                FrameMode::Rows => {
                    // Offsets are unbounded bigints; a step past either end of
                    // the partition saturates rather than wrapping.
                    let step = as_isize(offset_count(offset));
                    if following {
                        as_isize(position).saturating_add(step)
                    } else {
                        as_isize(position).saturating_sub(step)
                    }
                }
                FrameMode::Groups => {
                    let step = as_isize(offset_count(offset));
                    let target = if following {
                        as_isize(group).saturating_add(step)
                    } else {
                        as_isize(group).saturating_sub(step)
                    };
                    if target < 0 {
                        // Before the first group: an empty end, an unbounded start.
                        if is_start { 0 } else { -1 }
                    } else if target >= as_isize(groups) {
                        if is_start {
                            as_isize(total)
                        } else {
                            as_isize(total) - 1
                        }
                    } else {
                        let index = usize::try_from(target).unwrap_or(0);
                        let (first, last) = partition.peers.bounds[index];
                        as_isize(if is_start { first } else { last })
                    }
                }
                FrameMode::Range => {
                    range_bound_position(offset, following, side, partition, place, ctx)?
                }
            }
        }
    })
}

/// `RANGE <offset> PRECEDING/FOLLOWING`: the bound is the first (for a start) or
/// last (for an end) row whose ordering value has reached `current ± offset` in
/// the direction the `ORDER BY` sorts. A NULL ordering value has no arithmetic,
/// so its frame is exactly its own peer group, which is the run of NULLs.
/// `current ∓ offset` for a `RANGE` bound.
///
/// An integer ordering column computes in a wider domain and CLAMPS on overflow,
/// as `PostgreSQL`'s integer `in_range` support functions do. `RANGE BETWEEN
/// 2147483647 PRECEDING AND 2147483647 FOLLOWING` over an `integer` column is a
/// whole-partition frame there, not an out-of-range error.
fn offset_limit(
    ordering: &Datum,
    offset: &Datum,
    subtract: bool,
    ctx: &EvalCtx,
) -> Result<RangeLimit, ExecError> {
    // PostgreSQL treats an infinite interval frame offset as unbounded. This
    // must happen before time/timetz arithmetic, whose ordinary operators
    // correctly reject an infinite shift.
    if matches!(offset, Datum::Interval(interval) if interval.is_infinite()) {
        return Ok(RangeLimit::EveryOrderedValue);
    }
    // `+inf` infinitely precedes `+inf` and `-inf` infinitely follows `-inf`:
    // the arithmetic would be NaN, and PostgreSQL's float and numeric `in_range`
    // instead admit every finite and infinite value (but not NaN) to the frame.
    if is_infinite(ordering) && is_infinite(offset) && subtract != is_negative_infinity(ordering) {
        return Ok(RangeLimit::EveryOrderedValue);
    }
    if let (Some(base), Some(step)) = (as_i128(ordering), as_i128(offset)) {
        let limit = if subtract { base - step } else { base + step };
        let clamped = if limit < 0 { i64::MIN } else { i64::MAX };
        return Ok(RangeLimit::Value(Datum::Int8(
            i64::try_from(limit).unwrap_or(clamped),
        )));
    }
    crate::eval::apply_binary(
        if subtract {
            BinaryOp::Sub
        } else {
            BinaryOp::Add
        },
        ordering,
        offset,
        ctx,
    )
    .map(RangeLimit::Value)
}

/// What `current ± offset` bounds a `RANGE` frame at.
enum RangeLimit {
    /// The bound reaches past every ordering value there is. This is
    /// `PostgreSQL`'s infinity-against-infinity case, which admits everything
    /// except `NaN`.
    EveryOrderedValue,
    Value(Datum),
}

fn as_i128(value: &Datum) -> Option<i128> {
    match value {
        Datum::Int2(n) => Some(i128::from(*n)),
        Datum::Int4(n) => Some(i128::from(*n)),
        Datum::Int8(n) => Some(i128::from(*n)),
        _ => None,
    }
}

fn range_bound_position(
    offset: &Datum,
    following: bool,
    side: BoundSide,
    partition: &Partition<'_>,
    place: RowPlace,
    ctx: &EvalCtx,
) -> Result<isize, ExecError> {
    let BoundSide {
        is_start,
        ascending,
    } = side;
    let ordering = partition.sort_keys[partition.ordered[place.position]]
        .first()
        .cloned()
        .unwrap_or(Datum::Null);
    let own_peers = as_isize(if is_start {
        place.peer_first
    } else {
        place.peer_last
    });
    // PostgreSQL never consults `in_range` for a NULL ordering value, so a NULL
    // row is neither an offset check nor an arithmetic neighbourhood.
    if ordering.is_null() {
        return Ok(own_peers);
    }
    // This is where PostgreSQL calls `in_range`, so this is where it rejects a
    // negative or NaN offset — for a NaN ordering value too.
    validate_range_offset(offset, ctx)?;
    // NaN sorts above every value and equals only itself, so `in_range` admits
    // exactly this row's peers to its own frame.
    if is_nan(&ordering) {
        return Ok(own_peers);
    }
    // ASC counts PRECEDING downwards and FOLLOWING upwards; DESC mirrors both.
    let subtract = ascending != following;
    let limit = offset_limit(&ordering, offset, subtract, ctx)?;
    // The partition is already in window order, so the start bound is the first
    // row that has reached the limit and the end bound is the last one that has
    // not passed it. Rows whose ordering value is NULL or NaN are never in range.
    let mut found = None;
    for (index, row) in partition.ordered.iter().enumerate() {
        let value = &partition.sort_keys[*row][0];
        if value.is_null() || is_nan(value) {
            continue;
        }
        let order = match &limit {
            RangeLimit::EveryOrderedValue => Ordering::Equal,
            RangeLimit::Value(limit) => {
                let Some(order) = crabka_pgtypes::ops::compare(value, limit)? else {
                    continue;
                };
                if ascending { order } else { order.reverse() }
            }
        };
        if is_start {
            if order != Ordering::Less {
                found = Some(index);
                break;
            }
        } else if order != Ordering::Greater {
            found = Some(index);
        }
    }
    Ok(match found {
        Some(index) => as_isize(index),
        // Nothing is in range: an empty frame on whichever side asked.
        None if is_start => as_isize(partition.ordered.len()),
        None => -1,
    })
}

fn apply_exclusion(
    frame_rows: Vec<usize>,
    exclusion: FrameExclusion,
    place: RowPlace,
) -> Vec<usize> {
    let outside_peers = |p: &usize| *p < place.peer_first || *p > place.peer_last;
    match exclusion {
        FrameExclusion::NoOthers => frame_rows,
        FrameExclusion::CurrentRow => frame_rows
            .into_iter()
            .filter(|p| *p != place.position)
            .collect(),
        FrameExclusion::Group => frame_rows.into_iter().filter(outside_peers).collect(),
        FrameExclusion::Ties => frame_rows
            .into_iter()
            .filter(|p| *p == place.position || outside_peers(p))
            .collect(),
    }
}
