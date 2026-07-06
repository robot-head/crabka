#[cfg(feature = "experimental-functions")]
use promql_parser::parser::token::{T_LIMIT_RATIO, T_LIMITK};
use promql_parser::parser::{
    AggregateExpr, Call, Expr, LabelModifier, MatrixSelector, SubqueryExpr,
    token::{
        T_AVG, T_BOTTOMK, T_COUNT, T_COUNT_VALUES, T_GROUP, T_MAX, T_MIN, T_QUANTILE, T_STDDEV,
        T_STDVAR, T_SUM, T_TOPK, TokenType,
    },
    value::ValueType,
};

use super::{
    aggregation::AggregateOp,
    histogram::histogram_accessor_from_function_name,
    range_functions::{IrateFn, OuterRangeFn, OverTimeFn, RangeFn},
    scalar::calendar_fn_from_function_name,
};
use crate::{
    PromqlError,
    error::Result,
    functions::{OverTimeFamily, ScalarMathOp},
    planner::{
        ExtendedSelectorExpr, ExtendedSelectorModifier,
        aggregate::{Grouping, SimpleAggregateOp},
        label_ops::SortOrder,
        over_time_range::over_time_family_from_function_name,
        rate_range::RateUdfKind,
    },
};

pub(super) fn validate_extended_selector_modifier(
    function_name: &str,
    modifier: ExtendedSelectorModifier,
) -> Result<()> {
    let allowed = match modifier {
        ExtendedSelectorModifier::Anchored => matches!(
            function_name,
            "changes" | "delta" | "increase" | "rate" | "resets"
        ),
        ExtendedSelectorModifier::Smoothed => {
            matches!(function_name, "delta" | "increase" | "rate")
        }
    };
    if allowed {
        return Ok(());
    }

    let allowed_functions = match modifier {
        ExtendedSelectorModifier::Anchored => "changes, delta, increase, rate, resets",
        ExtendedSelectorModifier::Smoothed => "delta, increase, rate",
    };
    Err(PromqlError::Plan(format!(
        "{} modifier can only be used with: {allowed_functions} - not with {function_name}",
        modifier.keyword()
    )))
}

/// Structural predicate: true when every node of `expr` dispatches to a shape
/// the operator planner (`PromqlEngine::plan_instant_expr`) handles, so a
/// range query over `expr` can be routed through the per-step planner.
///
/// This mirrors `plan_instant_expr`'s **dispatch** (which node kinds and
/// function names route to the operator path), recursing into vector-typed
/// inner expressions the same way. It is purely structural - it never touches
/// the store - because planner support is structural: which constructs the
/// operator path understands does not change with the evaluation timestamp.
///
/// It deliberately does **not** model the data-dependent fallbacks
/// (histogram-bearing or empty-valued-label series, wrong call arity, a
/// non-scalar bound argument, an invalid `label_replace` regex). Those still
/// surface at evaluation time as a
/// `plan_instant_expr` returning `None` (or an `Err`), and the per-step range
/// driver treats *any* such per-step `None` as a whole-query fallback to the
/// interpreter - so this predicate only needs to gate out the node kinds that
/// cannot be nested as an operand or stitched across a step grid (string
/// literals, raw matrix selectors, and subqueries - whose results are not a
/// numeric scalar / instant vector).
///
/// Scalar-typed sub-expressions (a bound arg, a scalar binary operand) are
/// always treated as plannable: the planner evaluates them through the
/// interpreter's pure scalar path, which carries no staleness/NaN subtlety.
///
/// The arms returning a bare `true` (`VectorSelector`, `NumberLiteral`,
/// `Extension`) are kept separate for their per-variant documentation rather
/// than merged.
#[allow(clippy::match_same_arms)]
pub(super) fn instant_expr_is_plannable(expr: &Expr) -> bool {
    match expr {
        Expr::Paren(paren) => instant_expr_is_plannable(&paren.expr),
        // A bare instant-vector selector. A histogram-bearing series falls back
        // per-step; an empty-valued-label series now rides the operator leaf
        // (NULL = absent, `""` = present-empty).
        Expr::VectorSelector(_) => true,
        Expr::Call(call) => {
            // Rate-family or `*_over_time` range call (incl. the experimental
            // `mad`/`first`/`ts_of_*_over_time` members) over a bare matrix
            // selector. The matchers already require a plain `MatrixSelector`
            // argument; histogram inputs fall back per-step. A bad
            // `quantile_over_time` phi no longer falls back - it evaluates to
            // signed `+/-Inf` / `NaN` plus an `InvalidQuantileWarning`.
            if match_rate_range_call(expr).is_some()
                || match_over_time_range_call(expr).is_some()
                || match_experimental_over_time_range_call(expr).is_some()
            {
                return true;
            }
            // A RESIDUAL range-vector fold the fast matchers don't claim:
            // `changes`/`resets`/`deriv`/`predict_linear`/
            // `double_exponential_smoothing` over a plain matrix, or ANY rate /
            // `*_over_time` fold over an `anchored`/`smoothed` extended selector.
            // These route through `plan_extended_range_fold_call` (delegating to
            // the shared interpreter dispatch), so they are plannable - including
            // nested under an aggregate / binary and range-stitched per step.
            if is_extended_range_fold_call(call) {
                return true;
            }
            // The EXPERIMENTAL scalar-returning helpers handled by
            // `plan_experimental_call`: the duration helpers `range`/`step`/
            // `start`/`end` (which read the scoped range context, also scoped by the
            // per-step planner range driver) and `max_of`/`min_of` (scalar of scalar
            // extrema). These fold to a `PrecomputedScalar`, so they nest and
            // range-stitch like any scalar expression.
            #[cfg(feature = "experimental-functions")]
            if matches!(
                call.func.name,
                "range" | "step" | "start" | "end" | "max_of" | "min_of"
            ) {
                return true;
            }
            // A per-row scalar-math call: the inner vector argument (the first
            // positional arg) must itself be plannable. The trailing bound
            // args are scalars resolved through the interpreter.
            if scalar_math_op_from_function_name(call.func.name).is_some() {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // A label-rewrite / ordering call: the inner vector argument (the
            // first positional arg) must be plannable; the rest are string
            // literals validated per-step.
            if label_ops_kind_from_function_name(call.func.name).is_some() {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // A `histogram_quantile(phi, v)` (classic OR native): the inner bucket
            // vector (the second positional arg) must be plannable. `phi` (the
            // first arg) is a scalar resolved through the interpreter.
            if call.func.name == "histogram_quantile" {
                return call
                    .args
                    .args
                    .get(1)
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // The experimental `histogram_quantiles(label, v, phi...)`: the inner
            // bucket vector (the FIRST positional arg) must be plannable. The label
            // name and the trailing scalar `phi`s are resolved per-step.
            #[cfg(feature = "experimental-functions")]
            if call.func.name == "histogram_quantiles" {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // A native accessor (`histogram_count`/`sum`/`avg`/`stddev`/`stdvar`):
            // the single instant-vector operand must be plannable.
            if histogram_accessor_from_function_name(call.func.name).is_some() {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // `histogram_fraction(lower, upper, v)`: the inner vector (the third
            // positional arg) must be plannable. The two scalar bounds are
            // resolved through the interpreter.
            if call.func.name == "histogram_fraction" {
                return call
                    .args
                    .args
                    .get(2)
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // `info(v [, data_label_selector])`: the input vector `v` (the first
            // positional arg) must be plannable. The data-label selector is a
            // vector-selector literal validated at eval time (a non-vector-selector
            // arg / wrong arity surfaces as an `Err` from `plan_info_call`, which
            // the per-step driver treats as a whole-query fallback).
            if call.func.name == "info" {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // A range/`*_over_time` call whose argument is a subquery: the
            // subquery's inner instant expression must itself be plannable. The
            // outer scalar params (quantile/predict_linear/double_exp) are
            // resolved through the interpreter; a non-positive step / invalid
            // param falls back inside `plan_subquery_range_call`. This is what lets
            // nested subqueries and subquery calls inside an aggregate/binary route
            // through the planner.
            if let Some((subquery, _)) = match_subquery_range_call(call) {
                return instant_expr_is_plannable(&subquery.expr);
            }
            // The float UTILITY functions handled by `plan_util_call`.
            util_call_is_plannable(call)
        }
        // A simple (no-param) float aggregation, or a parameterized aggregation
        // (topk/bottomk/quantile/count_values/stddev/stdvar), over a plannable
        // inner vector. `limitk`/`limit_ratio` are not plannable.
        Expr::Aggregate(aggregate) => {
            let simple = simple_aggregate_op(aggregate.op).is_some() && aggregate.param.is_none();
            (simple || param_aggregate_op_is_plannable(aggregate))
                && instant_expr_is_plannable(&aggregate.expr)
        }
        // A binary op: each operand must itself be plannable; scalar operands are
        // always fine (folded via the interpreter's pure scalar path). A
        // scalar of scalar fold is carried through `PrecomputedScalar`.
        Expr::Binary(binary) => {
            binary_operand_is_plannable(&binary.lhs) && binary_operand_is_plannable(&binary.rhs)
        }
        // A unary `-`/`+` over a plannable operand. A scalar operand folds to a
        // scalar; a vector operand to a vector. Both nest and range-stitch.
        Expr::Unary(unary) => instant_expr_is_plannable(&unary.expr),
        // A bare numeric literal is a scalar carried through `PrecomputedScalar`.
        Expr::NumberLiteral(_) => true,
        // An `anchored`/`smoothed` extended selector is handled by
        // `plan_extension_expr` (the `smoothed` kernel, or the `anchored`-on-
        // instant hard error). Structurally plannable so nested forms
        // (`sum(smoothed(m))`) route too; a non-selector / unknown extension falls
        // back inside the planner.
        Expr::Extension(_) => true,
        // A string literal (no numeric/vector result to nest or range-stitch) and
        // a raw matrix selector / subquery (range-vector result, only meaningful
        // at the top level of an instant query) are handled directly in the
        // top-level `plan_instant_expr` dispatch, not through this nesting /
        // range-stitching predicate.
        Expr::StringLiteral(_) | Expr::MatrixSelector(_) | Expr::Subquery(_) => false,
    }
}

/// Gate for routing a **range** query through the per-step operator planner.
///
/// A range query routes through the per-step operator driver iff its top-level
/// shape is per-step planner-supported (`instant_expr_is_plannable`). This
/// includes a **bare** instant-vector selector and a top-level **scalar-typed**
/// expression:
///
/// - **Bare instant-vector selector.** The selector chain uses Prometheus'
///   left-**open**, right-closed lookback window `(eval - lookbackDelta, eval]`
///   (`promql/engine.go::vectorSelectorSingle` rejects `t <= eval - lookback`),
///   so a sample landing exactly on the lookback boundary is excluded.
///
/// - **Scalar-typed expression** (`time()`, `1 + 2`, the argless calendar
///   forms). The driver folds a no-label scalar series per step
///   (empty-labelset / `SampleValue::Float`).
///
/// Aggregations over a rate-family or `*_over_time` range call
/// (`sum(rate(m[5m]))`, `avg by(l)(increase(...))`, ...) route through the planner
/// too: the rate/`*_over_time` UDF emits **NULL** (not a NaN sentinel) for a
/// no-value window, the aggregate planner drops those NULL rows before grouping,
/// and the built-in / NaN-ignoring aggregates skip NULL - so a no-value series
/// is excluded from the group (and an all-no-value group yields no result row).
/// A genuine NaN value is non-null and propagates through the aggregate.
///
/// A top-level raw **matrix selector** / **subquery** is *not* plannable here
/// (it yields a range vector, owned by the dedicated matrix/subquery range
/// paths), so `instant_expr_is_plannable` already excludes it.
///
/// A **top-level bare selector** carrying an `@ start()`/`@ end()` modifier also
/// routes through the planner: the per-step planner range driver scopes the
/// query's `[start, end]` bounds in `AT_MODIFIER_BOUNDS`, and the selector planner
/// (`PromqlEngine::plan_instant_selector`) resolves `@ start()`/`@ end()` to
/// those bounds per Prometheus (a fixed eval instant repeated across every grid
/// step).
pub(super) fn range_expr_routes_through_planner(expr: &Expr) -> bool {
    instant_expr_is_plannable(expr)
}

/// True when one binary operand can be carried through the operator path: a
/// scalar operand is folded via the interpreter's pure scalar path (always
/// fine), and a vector operand must itself be structurally plannable. A
/// matrix/string operand is never plannable.
fn binary_operand_is_plannable(operand: &Expr) -> bool {
    match operand.value_type() {
        ValueType::Scalar => true,
        ValueType::Vector => instant_expr_is_plannable(operand),
        ValueType::Matrix | ValueType::String => false,
    }
}

/// Structural gate for the float UTILITY functions handled by
/// `PromqlEngine::plan_util_call`: `time`/`pi` (argless), `scalar`/`vector`,
/// `timestamp`, the calendar family (argless or one vector arg), and
/// `absent`/`absent_over_time`. The inner instant-vector argument (where one
/// exists) must itself be structurally plannable; data-dependent shapes
/// (histogram series, etc.) fall back per-step inside the planner. A `vector`
/// argument must be scalar-typed. Any other function (or a non-matching arity)
/// returns `false` so the dispatch falls through to the interpreter.
fn util_call_is_plannable(call: &Call) -> bool {
    match call.func.name {
        // Argless scalar utilities.
        "time" | "pi" => call.args.args.is_empty(),
        // The lone inner instant-vector argument must be plannable.
        "scalar" | "timestamp" | "absent" => call
            .args
            .args
            .first()
            .is_some_and(|arg| call.args.args.len() == 1 && instant_expr_is_plannable(arg)),
        // `vector(s)` takes a scalar argument resolved through the interpreter.
        "vector" => {
            call.args.args.len() == 1 && call.args.args[0].value_type() == ValueType::Scalar
        }
        // `absent_over_time(v[range])`: a plain float-only matrix selector rides
        // the fast `eval_range_arg` path; a histogram-bearing matrix, a subquery
        // range, or an anchored/smoothed selector delegates to the interpreter's
        // `eval_absent_over_time_call` (parity-exact). All range-vector shapes are
        // plannable; the per-shape / wrong-arity error is raised inside
        // `plan_absent_over_time_call`.
        "absent_over_time" => {
            let [arg] = call.args.args.as_slice() else {
                return false;
            };
            let mut inner = arg.as_ref();
            while let Expr::Paren(paren) = inner {
                inner = paren.expr.as_ref();
            }
            matches!(
                inner,
                Expr::MatrixSelector(_) | Expr::Subquery(_) | Expr::Extension(_)
            )
        }
        // The calendar family: argless (operates on `time()`) or one plannable
        // inner vector argument.
        other if calendar_fn_from_function_name(other).is_some() => match call.args.args.as_slice()
        {
            [] => true,
            [arg] => instant_expr_is_plannable(arg),
            _ => false,
        },
        _ => false,
    }
}

/// Recognize a top-level `f(selector[range])` rate-family call eligible for the
/// operator path, returning the inner [`MatrixSelector`] and the UDF kind.
///
/// Eligible iff `expr` is a [`Call`] whose function is one of
/// `rate|increase|delta|irate|idelta`, called with exactly one argument that -
/// after unwrapping parentheses - is a plain [`Expr::MatrixSelector`]. An
/// `anchored`/`smoothed` selector parses to [`Expr::Extension`], not a plain
/// `MatrixSelector`, so it is rejected here and stays on the interpreter, as do
/// nested forms (`sum(rate(...))`), `_over_time`, subqueries, and every other
/// function.
pub(super) fn match_rate_range_call(expr: &Expr) -> Option<(&MatrixSelector, RateUdfKind)> {
    let Expr::Call(call) = expr else {
        return None;
    };
    let kind = RateUdfKind::from_function_name(call.func.name)?;
    let [arg] = call.args.args.as_slice() else {
        return None;
    };
    let mut arg = arg.as_ref();
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    let Expr::MatrixSelector(selector) = arg else {
        return None;
    };
    Some((selector, kind))
}

/// Match a top-level `*_over_time` range call eligible for the operator path.
///
/// Eligible iff `expr` is a [`Call`] whose function is one of the
/// non-experimental members (`sum|avg|count|min|max|stddev|stdvar|
/// last|present_over_time`, or `quantile_over_time`), whose range argument -
/// after unwrapping parentheses - is a plain [`Expr::MatrixSelector`]. For
/// `quantile_over_time` the leading `phi` argument is returned for separate
/// scalar resolution; for every other family it is `None`.
///
/// The experimental members (`mad_over_time`, `first_over_time`, the
/// `ts_of_*_over_time` family) are matched separately by
/// [`match_experimental_over_time_range_call`] (they route through the shared
/// kernel, not this float UDF-chain path). `absent_over_time`, subquery range
/// arguments, `anchored`/`smoothed` selectors (which parse to [`Expr::Extension`]),
/// and nested forms stay on the interpreter (return `None`).
pub(super) fn match_over_time_range_call(
    expr: &Expr,
) -> Option<(&MatrixSelector, OverTimeFamily, Option<&Expr>)> {
    let Expr::Call(call) = expr else {
        return None;
    };
    let family = over_time_family_from_function_name(call.func.name)?;
    let (range_arg, phi_arg) = if matches!(family, OverTimeFamily::Quantile) {
        let [phi, range] = call.args.args.as_slice() else {
            return None;
        };
        (range.as_ref(), Some(phi.as_ref()))
    } else {
        let [range] = call.args.args.as_slice() else {
            return None;
        };
        (range.as_ref(), None)
    };
    let mut arg = range_arg;
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    let Expr::MatrixSelector(selector) = arg else {
        return None;
    };
    Some((selector, family, phi_arg))
}

/// The range-vector argument position of a single-range-vector-argument
/// range/`*_over_time` fold, by function name. This is the residual set of
/// range-fold functions whose operator routing is NOT a fast UDF chain: either
/// the function has no operator-leaf lowering (`changes`/`resets`/`deriv`/
/// `predict_linear`/`double_exponential_smoothing`), or its argument is an
/// `anchored`/`smoothed` extended selector (which `match_rate_range_call` /
/// `match_over_time_range_call` reject because they require a plain
/// `MatrixSelector`). Returns the index of the range-vector argument; the
/// parameter args (if any) are resolved by the delegated interpreter method.
pub(super) fn range_fold_range_arg_index(call: &Call) -> Option<usize> {
    match call.func.name {
        // One argument: the range vector.
        "rate" | "increase" | "delta" | "irate" | "idelta" | "changes" | "resets" | "deriv"
        | "sum_over_time" | "avg_over_time" | "count_over_time" | "min_over_time"
        | "max_over_time" | "stddev_over_time" | "stdvar_over_time" | "last_over_time"
        | "present_over_time" => (call.args.args.len() == 1).then_some(0),
        #[cfg(feature = "experimental-functions")]
        "mad_over_time"
        | "first_over_time"
        | "ts_of_first_over_time"
        | "ts_of_last_over_time"
        | "ts_of_min_over_time"
        | "ts_of_max_over_time" => (call.args.args.len() == 1).then_some(0),
        // `quantile_over_time(phi, range)`: the range vector is the SECOND arg.
        "quantile_over_time" => (call.args.args.len() == 2).then_some(1),
        // `predict_linear(range, t)`: the range vector is the FIRST arg.
        "predict_linear" => (call.args.args.len() == 2).then_some(0),
        // `double_exponential_smoothing(range, sf, tf)`: range is the FIRST arg.
        #[cfg(feature = "experimental-functions")]
        "double_exponential_smoothing" => (call.args.args.len() == 3).then_some(0),
        _ => None,
    }
}

/// Recognize a residual range-vector fold call (see `range_fold_range_arg_index`)
/// whose range-vector argument - after unwrapping parentheses - is a plain
/// [`Expr::MatrixSelector`] or an `anchored`/`smoothed` [`Expr::Extension`] over a
/// selector. Subquery range arguments are already claimed by
/// [`match_subquery_range_call`], and the fast plain-matrix `rate`/`*_over_time`
/// paths are already claimed by [`match_rate_range_call`] /
/// [`match_over_time_range_call`] earlier in the dispatch; this matcher is what
/// makes the planner TOTAL over the remaining shapes (`changes`/`resets`/`deriv`
/// over a plain matrix, and ANY of these folds over an anchored/smoothed
/// selector) by routing them into the SHARED interpreter `eval_*_call` (parity-
/// exact). Returns `true` when the call should route through
/// `PromqlEngine::plan_extended_range_fold_call`.
pub(super) fn is_extended_range_fold_call(call: &Call) -> bool {
    let Some(index) = range_fold_range_arg_index(call) else {
        return false;
    };
    let Some(range_arg) = call.args.args.get(index) else {
        return false;
    };
    let mut arg = range_arg.as_ref();
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    match arg {
        Expr::MatrixSelector(_) => true,
        // An `anchored`/`smoothed` extended selector wraps a `MatrixSelector`
        // child (`anchored(m[5m])`), so the interpreter's `eval_range_arg` can
        // build its windowed range vector.
        Expr::Extension(extension) => extension
            .expr
            .as_any()
            .downcast_ref::<ExtendedSelectorExpr>()
            .is_some_and(|extended| matches!(extended.child(), Some(Expr::MatrixSelector(_)))),
        _ => false,
    }
}

/// Match a top-level EXPERIMENTAL `*_over_time` member range call eligible for the
/// shared-kernel operator path.
///
/// Eligible iff `expr` is a [`Call`] whose function is one of `mad_over_time`,
/// `first_over_time`, or the `ts_of_{first,last,min,max}_over_time` family, called
/// with exactly one argument that - after unwrapping parentheses - is a plain
/// [`Expr::MatrixSelector`], returning the selector and the matching
/// [`OverTimeFn`]. These members have no operator-leaf UDF, so they route through
/// the shared `apply_outer_range_fn` kernel rather than the float UDF chain.
///
/// `absent_over_time`, subquery range arguments, `anchored`/`smoothed` selectors
/// (which parse to [`Expr::Extension`]), and nested forms stay on the interpreter
/// (return `None`). The non-experimental members are matched by
/// [`match_over_time_range_call`] instead.
pub(super) fn match_experimental_over_time_range_call(
    expr: &Expr,
) -> Option<(&MatrixSelector, OverTimeFn)> {
    let Expr::Call(call) = expr else {
        return None;
    };
    let kind = match call.func.name {
        "mad_over_time" => OverTimeFn::Mad,
        "first_over_time" => OverTimeFn::First,
        "ts_of_first_over_time" => OverTimeFn::TsOfFirst,
        "ts_of_last_over_time" => OverTimeFn::TsOfLast,
        "ts_of_min_over_time" => OverTimeFn::TsOfMin,
        "ts_of_max_over_time" => OverTimeFn::TsOfMax,
        _ => return None,
    };
    let [range] = call.args.args.as_slice() else {
        return None;
    };
    let mut arg = range.as_ref();
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    let Expr::MatrixSelector(selector) = arg else {
        return None;
    };
    Some((selector, kind))
}

/// Map a [`RateUdfKind`] (the rate-family matcher's output) to the shared
/// [`OuterRangeFn`] the interpreter's `eval_*_call` applies for the same name.
/// `rate`/`increase`/`delta` are extrapolated range folds; `irate`/`idelta` are
/// instant-delta folds. This is the seam that lets a histogram-bearing rate-family
/// call route through the shared `apply_outer_range_fn` kernel instead of the
/// float-only UDF chain.
pub(super) fn rate_udf_kind_to_outer_range_fn(kind: RateUdfKind) -> OuterRangeFn {
    match kind {
        RateUdfKind::Rate => OuterRangeFn::Range(RangeFn::Rate),
        RateUdfKind::Increase => OuterRangeFn::Range(RangeFn::Increase),
        RateUdfKind::Delta => OuterRangeFn::Range(RangeFn::Delta),
        RateUdfKind::Irate => OuterRangeFn::InstantDelta(IrateFn::Irate),
        RateUdfKind::Idelta => OuterRangeFn::InstantDelta(IrateFn::Idelta),
    }
}

/// Map an [`OverTimeFamily`] (the `*_over_time` matcher's output) to the shared
/// [`OuterRangeFn`] the interpreter's `eval_over_time_call` applies for the same
/// name. `quantile_over_time` carries its resolved `phi`; every other member maps
/// to the matching [`OverTimeFn`]. The matcher only yields the non-experimental
/// members, so the experimental [`OverTimeFn`] variants (`Mad`/`First`/`TsOf*`)
/// are unreachable here.
pub(super) fn over_time_family_to_outer_range_fn(family: OverTimeFamily, phi: f64) -> OuterRangeFn {
    match family {
        OverTimeFamily::Sum => OuterRangeFn::OverTime(OverTimeFn::Sum),
        OverTimeFamily::Avg => OuterRangeFn::OverTime(OverTimeFn::Avg),
        OverTimeFamily::Count => OuterRangeFn::OverTime(OverTimeFn::Count),
        OverTimeFamily::Min => OuterRangeFn::OverTime(OverTimeFn::Min),
        OverTimeFamily::Max => OuterRangeFn::OverTime(OverTimeFn::Max),
        OverTimeFamily::Stddev => OuterRangeFn::OverTime(OverTimeFn::Stddev),
        OverTimeFamily::Stdvar => OuterRangeFn::OverTime(OverTimeFn::Stdvar),
        OverTimeFamily::Last => OuterRangeFn::OverTime(OverTimeFn::Last),
        OverTimeFamily::Present => OuterRangeFn::OverTime(OverTimeFn::Present),
        OverTimeFamily::Quantile => OuterRangeFn::QuantileOverTime(phi),
    }
}

/// The outer range/`*_over_time` function of a `f(inner[range:res] ...)` subquery
/// call, with any scalar parameters still **unresolved** (the parameter argument
/// `Expr`s are resolved through the interpreter inside the async planner method,
/// matching what the corresponding `eval_*_call` does).
pub(super) enum SubqueryOuterFn<'a> {
    /// A function whose only argument is the range vector; the [`OuterRangeFn`] is
    /// fully determined by the name.
    NoParam(OuterRangeFn),
    /// `quantile_over_time(phi, inner[...])`: resolve `phi` (the leading arg).
    QuantileOverTime { phi: &'a Expr },
    /// `predict_linear(inner[...], t)`: resolve the trailing duration arg.
    PredictLinear { duration: &'a Expr },
    /// `double_exponential_smoothing(inner[...], sf, tf)`: resolve both factors.
    #[cfg(feature = "experimental-functions")]
    DoubleExponentialSmoothing {
        smoothing: &'a Expr,
        trend: &'a Expr,
    },
}

/// Map a range/`*_over_time` function name to its [`OuterRangeFn`] when the
/// function takes exactly one argument (the range vector). Parameterized
/// functions (`quantile_over_time`/`predict_linear`/`double_exponential_smoothing`)
/// and the non-fold helpers (`absent_over_time`/`time`/...) return `None` here and
/// are matched separately.
fn no_param_outer_range_fn(name: &str) -> Option<OuterRangeFn> {
    Some(match name {
        "rate" => OuterRangeFn::Range(RangeFn::Rate),
        "increase" => OuterRangeFn::Range(RangeFn::Increase),
        "delta" => OuterRangeFn::Range(RangeFn::Delta),
        "changes" => OuterRangeFn::Range(RangeFn::Changes),
        "resets" => OuterRangeFn::Range(RangeFn::Resets),
        "irate" => OuterRangeFn::InstantDelta(IrateFn::Irate),
        "idelta" => OuterRangeFn::InstantDelta(IrateFn::Idelta),
        "deriv" => OuterRangeFn::Deriv,
        "sum_over_time" => OuterRangeFn::OverTime(OverTimeFn::Sum),
        "avg_over_time" => OuterRangeFn::OverTime(OverTimeFn::Avg),
        "count_over_time" => OuterRangeFn::OverTime(OverTimeFn::Count),
        "min_over_time" => OuterRangeFn::OverTime(OverTimeFn::Min),
        "max_over_time" => OuterRangeFn::OverTime(OverTimeFn::Max),
        "stddev_over_time" => OuterRangeFn::OverTime(OverTimeFn::Stddev),
        "stdvar_over_time" => OuterRangeFn::OverTime(OverTimeFn::Stdvar),
        "mad_over_time" => OuterRangeFn::OverTime(OverTimeFn::Mad),
        "first_over_time" => OuterRangeFn::OverTime(OverTimeFn::First),
        "last_over_time" => OuterRangeFn::OverTime(OverTimeFn::Last),
        "ts_of_first_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfFirst),
        "ts_of_last_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfLast),
        "ts_of_min_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfMin),
        "ts_of_max_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfMax),
        "present_over_time" => OuterRangeFn::OverTime(OverTimeFn::Present),
        _ => return None,
    })
}

/// Recognize a `f(inner[range:resolution] ...)` call whose range argument is a
/// **subquery** and whose outer `f` is a planner-supported range/`*_over_time`
/// fold, returning the [`SubqueryExpr`] and the (param-unresolved) outer-fn spec.
///
/// Eligible iff `expr` is a [`Call`] whose function `f` is one of the supported
/// folds and whose range argument - after unwrapping parentheses - is an
/// [`Expr::Subquery`]. `absent_over_time` (synthesizes absent labels) and every
/// non-fold function return `None` and stay on the interpreter, as does a
/// matrix-selector range argument (matched by [`match_rate_range_call`] /
/// [`match_over_time_range_call`] instead).
pub(super) fn match_subquery_range_call(
    call: &Call,
) -> Option<(&SubqueryExpr, SubqueryOuterFn<'_>)> {
    // Resolve the range-vector argument's position and the parameter args by the
    // function's arity, exactly as the corresponding `eval_*_call` does.
    let (range_arg, spec) = match call.func.name {
        "quantile_over_time" => {
            let [phi, range] = call.args.args.as_slice() else {
                return None;
            };
            (range.as_ref(), SubqueryOuterFn::QuantileOverTime { phi })
        }
        "predict_linear" => {
            let [range, duration] = call.args.args.as_slice() else {
                return None;
            };
            (range.as_ref(), SubqueryOuterFn::PredictLinear { duration })
        }
        #[cfg(feature = "experimental-functions")]
        "double_exponential_smoothing" => {
            let [range, smoothing, trend] = call.args.args.as_slice() else {
                return None;
            };
            (
                range.as_ref(),
                SubqueryOuterFn::DoubleExponentialSmoothing { smoothing, trend },
            )
        }
        name => {
            let outer = no_param_outer_range_fn(name)?;
            let [range] = call.args.args.as_slice() else {
                return None;
            };
            (range.as_ref(), SubqueryOuterFn::NoParam(outer))
        }
    };
    let mut arg = range_arg;
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    let Expr::Subquery(subquery) = arg else {
        return None;
    };
    Some((subquery, spec))
}

/// Map a `PromQL` function name to its per-row scalar-math op, or `None` for any
/// function outside the scalar-math set (which stays on the interpreter). `pi`
/// is a 0-arg literal, not a per-row op, so it is intentionally excluded.
pub(super) fn scalar_math_op_from_function_name(name: &str) -> Option<ScalarMathOp> {
    Some(match name {
        "abs" => ScalarMathOp::Abs,
        "ceil" => ScalarMathOp::Ceil,
        "floor" => ScalarMathOp::Floor,
        "sqrt" => ScalarMathOp::Sqrt,
        "exp" => ScalarMathOp::Exp,
        "ln" => ScalarMathOp::Ln,
        "log2" => ScalarMathOp::Log2,
        "log10" => ScalarMathOp::Log10,
        "sgn" => ScalarMathOp::Sgn,
        "sin" => ScalarMathOp::Sin,
        "cos" => ScalarMathOp::Cos,
        "tan" => ScalarMathOp::Tan,
        "asin" => ScalarMathOp::Asin,
        "acos" => ScalarMathOp::Acos,
        "atan" => ScalarMathOp::Atan,
        "sinh" => ScalarMathOp::Sinh,
        "cosh" => ScalarMathOp::Cosh,
        "tanh" => ScalarMathOp::Tanh,
        "asinh" => ScalarMathOp::Asinh,
        "acosh" => ScalarMathOp::Acosh,
        "atanh" => ScalarMathOp::Atanh,
        "deg" => ScalarMathOp::Deg,
        "rad" => ScalarMathOp::Rad,
        "round" => ScalarMathOp::Round,
        "clamp_min" => ScalarMathOp::ClampMin,
        "clamp_max" => ScalarMathOp::ClampMax,
        "clamp" => ScalarMathOp::Clamp,
        _ => return None,
    })
}

/// The label-rewrite / ordering functions handled by the operator-path
/// `PromqlEngine::plan_label_ops_call`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LabelOpsKind {
    LabelReplace,
    LabelJoin,
    Sort(SortOrder),
    SortByLabel(SortOrder),
}

/// Map a `PromQL` function name to its label-rewrite / ordering kind, or `None`
/// for any function outside this set.
pub(super) fn label_ops_kind_from_function_name(name: &str) -> Option<LabelOpsKind> {
    Some(match name {
        "label_replace" => LabelOpsKind::LabelReplace,
        "label_join" => LabelOpsKind::LabelJoin,
        "sort" => LabelOpsKind::Sort(SortOrder::Ascending),
        "sort_desc" => LabelOpsKind::Sort(SortOrder::Descending),
        "sort_by_label" => LabelOpsKind::SortByLabel(SortOrder::Ascending),
        "sort_by_label_desc" => LabelOpsKind::SortByLabel(SortOrder::Descending),
        _ => return None,
    })
}

/// The value of a string-literal call argument at `index`, or `None` when the
/// argument is absent or not a string literal. Unlike `string_literal_arg`,
/// this never errors: the label-ops planner uses it to *probe* the call shape
/// and falls back to the interpreter (which raises the canonical error) on any
/// mismatch.
pub(super) fn string_literal_value(call: &Call, index: usize) -> Option<String> {
    match call.args.args.get(index).map(Box::as_ref) {
        Some(Expr::StringLiteral(value)) => Some(value.val.clone()),
        _ => None,
    }
}

/// Map an aggregation token to its simple-aggregation lowering, or `None` for
/// ops that are not in the simple set (param ops, `stddev`/`stdvar`, etc.).
pub(super) fn simple_aggregate_op(token: TokenType) -> Option<SimpleAggregateOp> {
    match token.id() {
        T_SUM => Some(SimpleAggregateOp::Sum),
        T_AVG => Some(SimpleAggregateOp::Avg),
        T_MIN => Some(SimpleAggregateOp::Min),
        T_MAX => Some(SimpleAggregateOp::Max),
        T_COUNT => Some(SimpleAggregateOp::Count),
        T_GROUP => Some(SimpleAggregateOp::Group),
        _ => None,
    }
}

/// Map the planner's [`SimpleAggregateOp`] (which shapes the `DataFusion` plan)
/// to the interpreter's [`AggregateOp`] (which drives the shared
/// `apply_simple_aggregate` kernel). Both enumerate the same six simple ops, so
/// the mapping is total; this is the seam that lets the histogram-bearing
/// operator path reuse the interpreter's reduction core.
pub(super) fn simple_aggregate_op_to_aggregate_op(op: SimpleAggregateOp) -> AggregateOp {
    match op {
        SimpleAggregateOp::Sum => AggregateOp::Sum,
        SimpleAggregateOp::Avg => AggregateOp::Avg,
        SimpleAggregateOp::Min => AggregateOp::Min,
        SimpleAggregateOp::Max => AggregateOp::Max,
        SimpleAggregateOp::Count => AggregateOp::Count,
        SimpleAggregateOp::Group => AggregateOp::Group,
    }
}

/// True when a parameterized / non-simple aggregation routes through the
/// operator path (`plan_param_aggregate_expr`): `topk`/`bottomk`/`quantile`
/// (numeric-literal param), `count_values` (string-literal param),
/// `stddev`/`stdvar` (no param), and the experimental `limitk`/`limit_ratio`
/// (scalar param, resolved through the SAME interpreter helpers - including
/// `limit_ratio`'s deduplicated `InvalidRatioWarning`). The structural param
/// shape is checked here so the range gate matches the per-step planner's own
/// param requirement; a malformed-but-right-kind param still falls back at eval
/// time and the interpreter raises the canonical error.
fn param_aggregate_op_is_plannable(aggregate: &AggregateExpr) -> bool {
    match aggregate.op.id() {
        T_TOPK | T_BOTTOMK | T_QUANTILE => {
            matches!(aggregate.param.as_deref(), Some(Expr::NumberLiteral(_)))
        }
        T_COUNT_VALUES => matches!(aggregate.param.as_deref(), Some(Expr::StringLiteral(_))),
        T_STDDEV | T_STDVAR => aggregate.param.is_none(),
        // `limitk`/`limit_ratio` carry a scalar parameter resolved through the
        // interpreter helpers; the planner short-circuits a 0 param and applies
        // the shared selection kernel.
        #[cfg(feature = "experimental-functions")]
        T_LIMITK | T_LIMIT_RATIO => aggregate
            .param
            .as_deref()
            .is_some_and(|param| param.value_type() == ValueType::Scalar),
        _ => false,
    }
}

/// Map an aggregation `by`/`without` modifier to the planner [`Grouping`].
/// `None` means the aggregation has no modifier (which the caller treats as
/// `by ()`, collapsing all series into one group).
pub(super) fn aggregate_grouping(modifier: Option<&LabelModifier>) -> Option<Grouping> {
    match modifier? {
        LabelModifier::Include(include) => Some(Grouping::By(include.labels.clone())),
        LabelModifier::Exclude(exclude) => Some(Grouping::Without(exclude.labels.clone())),
    }
}
