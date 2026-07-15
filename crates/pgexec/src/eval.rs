//! Expression evaluation over Datums, plus static result-type inference (used
//! to build a stable RowDescription before any row is produced).

use std::cmp::Ordering;

use crabka_pgparser::ast::{BinaryOp, Expr, UnaryOp};
use crabka_pgtypes::{ColumnType, Datum, TypeError, ops};

use crate::{clock::EvalCtx, error::ExecError, scope::Scope};

/// The maximum expression-tree depth `eval` will recurse before returning
/// `54001` (statement_too_complex). This is DEFENSE-IN-DEPTH: the parser already
/// caps the AST depth at `crabka_pgparser::parser::MAX_DEPTH` (50) at parse time, so a
/// tree deeper than 50 can never reach here in practice — `150` leaves 3x
/// headroom above that cap so the guard never wrongly rejects a parser-admitted
/// tree. The value also stays well below the depth at which `eval` itself would
/// overflow: in production (tokio's ~2 MiB worker stack) `eval` handles many
/// thousands of frames, and even on the SMALLER stack a `cargo nextest` test
/// thread gets, the at-limit `eval_accepts_a_tree_at_the_limit` test (≈150
/// frames) runs safely below the ~350-frame overflow point — so a hypothetical
/// over-deep tree returns a clean error rather than aborting the process.
const MAX_EVAL_DEPTH: usize = 150;

/// Evaluate `expr` against a row (`values`, aligned to `scope.columns`). `ctx`
/// carries the session time zone and the transaction/statement clock; non-temporal
/// evaluation ignores it (UTC/epoch reproduces prior behavior).
pub(crate) fn eval(
    expr: &Expr,
    scope: &Scope,
    values: &[Datum],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    eval_depth(expr, scope, values, ctx, 0)
}

/// Depth-tracking core of [`eval`]. `depth` is the current recursion level; every
/// recursive descent (direct calls AND the child-evaluation closures handed to
/// the shared `eval_*`/`func::*` combinators) increments it, so a runaway tree is
/// bounded on every path. Returns `54001` once it exceeds `MAX_EVAL_DEPTH`.
fn eval_depth(
    expr: &Expr,
    scope: &Scope,
    values: &[Datum],
    ctx: &EvalCtx,
    depth: usize,
) -> Result<Datum, ExecError> {
    if depth > MAX_EVAL_DEPTH {
        return Err(ExecError::StackDepthExceeded);
    }
    // One level deeper for every child this frame evaluates.
    let d = depth + 1;
    match expr {
        Expr::IntLiteral(s) => Ok(ops::int_literal(s)?),
        // SP32: a bare decimal/exponent literal is `numeric` (arbitrary precision —
        // no overflow; the lexer already guaranteed a well-formed decimal lexeme).
        Expr::NumericLiteral(s) => crabka_pgtypes::numeric::parse(s)
            .map(Datum::Numeric)
            .ok_or_else(|| {
                ExecError::Type(TypeError::InvalidText {
                    type_name: "numeric",
                    value: s.clone(),
                })
            }),
        Expr::StringLiteral(s) => Ok(Datum::Text(s.clone())),
        Expr::BoolLiteral(b) => Ok(Datum::Bool(*b)),
        Expr::NullLiteral => Ok(Datum::Null),
        Expr::Param(_) => Err(ExecError::Unsupported(
            "query parameters ($n) are not supported".into(),
        )),
        Expr::Default => Err(ExecError::Unsupported(
            "DEFAULT is only supported in INSERT target values".into(),
        )),
        Expr::Column { table, name } => {
            let idx = scope.resolve(table.as_deref(), name)?;
            Ok(values[idx].clone())
        }
        Expr::Unary { op, expr } => {
            let v = eval_depth(expr, scope, values, ctx, d)?;
            apply_unary(*op, &v, ctx)
        }
        Expr::Binary { op, left, right } => {
            let l = eval_depth(left, scope, values, ctx, d)?;
            let r = eval_depth(right, scope, values, ctx, d)?;
            apply_binary(*op, &l, &r, ctx)
        }
        // A function call reached scalar `eval`: a SP29 scalar function evaluates
        // here (its arguments recurse through this same `eval`). Otherwise it is
        // NOT in a valid aggregate position (the aggregate path resolves
        // aggregates from accumulators) — a known aggregate here is misplaced /
        // nested (42803); any other name is undefined (42883).
        Expr::Func(fc) if crate::func::is_scalar(&fc.name) => {
            crate::func::eval_scalar(fc, ctx, |e| eval_depth(e, scope, values, ctx, d))
        }
        // SP37: a date/time function (clock family, extract/date_part, date_trunc,
        // age, timezone). Tried after scalar, before the aggregate-context error.
        Expr::Func(fc) if crate::datetime_fn::is_datetime_func(&fc.name) => {
            crate::datetime_fn::eval_datetime(fc, ctx, |e| eval_depth(e, scope, values, ctx, d))
        }
        // SP38: date/time formatting + constructors + numeric to_char
        // (to_char/to_timestamp/to_date/make_*/justify_*). Tried after scalar +
        // datetime, before the aggregate-context error.
        Expr::Func(fc) if crate::format_fn::is_format_func(&fc.name) => {
            crate::format_fn::eval_format(fc, ctx, |e| eval_depth(e, scope, values, ctx, d))
        }
        Expr::Func(fc) => Err(crate::agg::func_in_scalar_context_error(fc)),
        // SP28: predicate + conditional expressions. The pure-Datum combinators
        // (`eval_in_list`/`eval_between`/`eval_like`/`eval_case`) are shared with
        // the grouped evaluator (`agg::eval_grouped`); only the child-evaluation
        // closure differs.
        Expr::IsNull { expr, negated } => {
            let v = eval_depth(expr, scope, values, ctx, d)?;
            Ok(Datum::Bool(v.is_null() ^ *negated))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let x = eval_depth(expr, scope, values, ctx, d)?;
            eval_in_list(&x, list, *negated, |e| eval_depth(e, scope, values, ctx, d))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let x = eval_depth(expr, scope, values, ctx, d)?;
            let lo = eval_depth(low, scope, values, ctx, d)?;
            let hi = eval_depth(high, scope, values, ctx, d)?;
            eval_between(&x, &lo, &hi, *negated, ctx)
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            let s = eval_depth(expr, scope, values, ctx, d)?;
            let p = eval_depth(pattern, scope, values, ctx, d)?;
            eval_like(&s, &p, *negated, *case_insensitive)
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => eval_case(operand.as_deref(), whens, else_result.as_deref(), |e| {
            eval_depth(e, scope, values, ctx, d)
        }),
        // SP31: explicit cast — evaluate the operand, then convert. A text-parse
        // failure (22P02), numeric overflow (22003), or undefined cast (42846)
        // surfaces here; NULL casts to NULL. The session zone comes from `ctx`.
        Expr::Cast { expr, ty } => {
            let v = eval_depth(expr, scope, values, ctx, d)?;
            // `'name'::regclass`: a non-numeric string is a relation name only
            // the catalog can resolve (PostgreSQL's regclassin); numeric and
            // NULL inputs take the pure cast below.
            if *ty == crabka_pgtypes::ColumnType::Regclass
                && let Datum::Text(name) = &v
                && name.trim().parse::<i32>().is_err()
                && let Some(sequence) = &ctx.sequence
            {
                return crate::exec::resolve_regclass(sequence.kv.as_ref(), name).map(Datum::Int4);
            }
            Ok(crabka_pgtypes::cast::cast(&v, *ty, &ctx.time_zone)?)
        }
        // SP34: a resolved subquery folded to a constant.
        Expr::Const { value, .. } => Ok(value.clone()),
        // SP34: a raw subquery node here means the read pre-pass did not run — only
        // SELECT goes through `resolve_in_select`. (Subqueries in INSERT/UPDATE/DELETE
        // are a documented non-goal of this slice.)
        Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery { .. }
        | Expr::Quantified { .. } => Err(ExecError::Unsupported(
            "subqueries are only supported in SELECT".into(),
        )),
    }
}

/// `x IN (list)` / `x NOT IN (list)` with three-valued NULL logic. `eval_child`
/// evaluates each list element. Truth table for `IN`: NULL lhs → NULL; an
/// element comparing Equal → true; otherwise NULL if any element was NULL, else
/// false. `NOT IN` is the boolean negation (NULL stays NULL).
pub(crate) fn eval_in_list(
    x: &Datum,
    list: &[Expr],
    negated: bool,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    if x.is_null() {
        return Ok(Datum::Null);
    }
    let mut saw_null = false;
    for item in list {
        let v = eval_child(item)?;
        match ops::compare(x, &v)? {
            Some(Ordering::Equal) => return Ok(Datum::Bool(!negated)),
            Some(_) => {}
            None => saw_null = true,
        }
    }
    if saw_null {
        Ok(Datum::Null)
    } else {
        Ok(Datum::Bool(negated))
    }
}

/// `x BETWEEN lo AND hi` ≡ `x >= lo AND x <= hi`; `NOT BETWEEN` negates it. NULL
/// propagates exactly as three-valued AND/NOT define.
pub(crate) fn eval_between(
    x: &Datum,
    lo: &Datum,
    hi: &Datum,
    negated: bool,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let ge = apply_binary(BinaryOp::Ge, x, lo, ctx)?;
    let le = apply_binary(BinaryOp::Le, x, hi, ctx)?;
    let res = ops::and(&ge, &le)?;
    Ok(if negated { ops::not(&res)? } else { res })
}

/// `s LIKE pat` / `ILIKE` (and their negations). NULL operand → NULL; a non-text
/// operand → 42804.
pub(crate) fn eval_like(
    s: &Datum,
    pat: &Datum,
    negated: bool,
    case_insensitive: bool,
) -> Result<Datum, ExecError> {
    if s.is_null() || pat.is_null() {
        return Ok(Datum::Null);
    }
    let m = like_match(as_text(s)?, as_text(pat)?, case_insensitive)?;
    Ok(Datum::Bool(m ^ negated))
}

fn as_text(d: &Datum) -> Result<&str, ExecError> {
    match d {
        Datum::Text(s) => Ok(s),
        _ => Err(ExecError::TypeMismatch(
            "LIKE/ILIKE operands must be type text".into(),
        )),
    }
}

/// SQL `LIKE` matcher over Unicode scalar values: `%` matches zero-or-more
/// characters, `_` exactly one, and `\` escapes the next pattern character.
/// `ci` folds ASCII case (the `ILIKE` form). A pattern ending in a lone `\` is
/// an invalid escape sequence (22025). Iterative backtracking to the last `%`,
/// O(n·m) worst case.
pub(crate) fn like_match(s: &str, p: &str, ci: bool) -> Result<bool, ExecError> {
    let fold = |c: char| if ci { c.to_ascii_lowercase() } else { c };
    let sb: Vec<char> = s.chars().map(fold).collect();
    let pb: Vec<char> = p.chars().collect();
    let (mut si, mut pi) = (0usize, 0usize);
    // The last `%` seen: pattern index just past it, and the `s` index to resume
    // from (advanced by one on each backtrack).
    let mut star: Option<usize> = None;
    let mut star_si = 0usize;
    while si < sb.len() {
        if pi < pb.len() {
            match pb[pi] {
                '\\' => {
                    let lit = *pb
                        .get(pi + 1)
                        .ok_or(ExecError::Type(TypeError::InvalidEscape))?;
                    if sb[si] == fold(lit) {
                        si += 1;
                        pi += 2;
                        continue;
                    }
                }
                '%' => {
                    star = Some(pi);
                    star_si = si;
                    pi += 1;
                    continue;
                }
                '_' => {
                    si += 1;
                    pi += 1;
                    continue;
                }
                c => {
                    if sb[si] == fold(c) {
                        si += 1;
                        pi += 1;
                        continue;
                    }
                }
            }
        }
        // Mismatch (or pattern exhausted while `s` remains): backtrack to the
        // last `%`, consuming one more subject character under it.
        if let Some(sp) = star {
            pi = sp + 1;
            star_si += 1;
            si = star_si;
        } else {
            return Ok(false);
        }
    }
    // `s` is consumed; the remaining pattern must be only `%` to match (and a
    // trailing lone `\` is still an invalid escape).
    while pi < pb.len() {
        match pb[pi] {
            '%' => pi += 1,
            '\\' => {
                pb.get(pi + 1)
                    .ok_or(ExecError::Type(TypeError::InvalidEscape))?;
                return Ok(false);
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}

/// A `CASE` expression. Searched form (`operand` None): the first WHEN whose
/// condition is TRUE wins (false/NULL skip; non-boolean → 42804). Simple form:
/// the first WHEN value comparing Equal to the operand wins (NULL never
/// matches). Falls through to ELSE, or NULL. Branches are evaluated lazily and
/// in order, so a later branch's error/side-effect is never reached early.
pub(crate) fn eval_case(
    operand: Option<&Expr>,
    whens: &[(Expr, Expr)],
    else_result: Option<&Expr>,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    match operand {
        None => {
            for (cond, result) in whens {
                match eval_child(cond)? {
                    Datum::Bool(true) => return eval_child(result),
                    Datum::Bool(false) | Datum::Null => {}
                    _ => {
                        return Err(ExecError::TypeMismatch(
                            "argument of CASE/WHEN must be type boolean".into(),
                        ));
                    }
                }
            }
        }
        Some(op) => {
            let ov = eval_child(op)?;
            for (val, result) in whens {
                let vv = eval_child(val)?;
                if matches!(ops::compare(&ov, &vv)?, Some(Ordering::Equal)) {
                    return eval_child(result);
                }
            }
        }
    }
    match else_result {
        Some(e) => eval_child(e),
        None => Ok(Datum::Null),
    }
}

/// Apply a unary operator to an already-evaluated operand. Shared by scalar
/// `eval` and the SP27 grouped evaluator (`agg::eval_grouped`). `ctx` is threaded
/// uniformly (no unary operator consumes it yet).
pub(crate) fn apply_unary(op: UnaryOp, v: &Datum, _ctx: &EvalCtx) -> Result<Datum, ExecError> {
    match op {
        UnaryOp::Not => Ok(ops::not(v)?),
        // SP37: unary minus on an interval negates each field (`0 - interval` has no
        // defined operator). Everything else is `0 - v` (int/numeric/float negation).
        UnaryOp::Neg => match v {
            Datum::Interval(i) => Ok(Datum::Interval(crabka_pgtypes::datetime::neg_interval(*i)?)),
            _ => Ok(ops::sub(&Datum::Int4(0), v)?),
        },
    }
}

/// Apply a binary operator to two already-evaluated operands. Shared by scalar
/// `eval` and the SP27 grouped evaluator (`agg::eval_grouped`). `ctx` supplies the
/// session zone used by `||`'s text rendering.
pub(crate) fn apply_binary(
    op: BinaryOp,
    l: &Datum,
    r: &Datum,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    // SP37: tz-AWARE temporal arithmetic involving `timestamptz` is computed here
    // (where `ctx.time_zone` is available) — `crabka_pgtypes::ops` would `TypeMismatch` on
    // a `Timestamptz` operand. A non-timestamptz pair falls through to `ops`, so all
    // existing (tz-free) behavior is unchanged.
    if matches!(op, BinaryOp::Add | BinaryOp::Sub)
        && let Some(result) = apply_timestamptz_arith(op, l, r, ctx)?
    {
        return Ok(result);
    }
    match op {
        BinaryOp::Add => Ok(ops::add(l, r)?),
        BinaryOp::Sub => Ok(ops::sub(l, r)?),
        BinaryOp::Mul => Ok(ops::mul(l, r)?),
        BinaryOp::Div => Ok(ops::div(l, r)?),
        BinaryOp::And => Ok(ops::and(l, r)?),
        BinaryOp::Or => Ok(ops::or(l, r)?),
        BinaryOp::Concat => Ok(ops::concat(l, r, &ctx.time_zone)?),
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            let ord = ops::compare(l, r)?;
            Ok(cmp_result(op, ord))
        }
    }
}

/// SP37: tz-AWARE `timestamptz` arithmetic — the cells deferred from
/// `crabka_pgtypes::ops` because they need the session zone (`ctx.time_zone`):
/// `timestamptz ± interval → timestamptz` (calendar-aware in the zone) and
/// `timestamptz − timestamptz → interval` (absolute-instant difference). Returns
/// `Ok(None)` when neither operand is a `Timestamptz` (so the caller falls through
/// to `crabka_pgtypes::ops`), and propagates NULL like `ops` does. Result types match
/// `datetime_result_type`'s `Timestamptz`/`Interval` predictions, so plan-time
/// inference and runtime never disagree.
fn apply_timestamptz_arith(
    op: BinaryOp,
    l: &Datum,
    r: &Datum,
    ctx: &EvalCtx,
) -> Result<Option<Datum>, ExecError> {
    use crabka_pgtypes::datetime::{timestamptz_diff, timestamptz_plus_interval};
    // Only engage when a Timestamptz operand is present.
    if !matches!(l, Datum::Timestamptz(_)) && !matches!(r, Datum::Timestamptz(_)) {
        return Ok(None);
    }
    // NULL propagates (mirrors `ops::add`/`ops::sub`).
    if l.is_null() || r.is_null() {
        return Ok(Some(Datum::Null));
    }
    let tz = &ctx.time_zone;
    let result = match (op, l, r) {
        // timestamptz + interval → timestamptz; interval + timestamptz → timestamptz.
        (BinaryOp::Add, Datum::Timestamptz(ts), Datum::Interval(iv))
        | (BinaryOp::Add, Datum::Interval(iv), Datum::Timestamptz(ts)) => {
            Datum::Timestamptz(timestamptz_plus_interval(*ts, *iv, tz)?)
        }
        // timestamptz - interval → timestamptz.
        (BinaryOp::Sub, Datum::Timestamptz(ts), Datum::Interval(iv)) => {
            let neg = crabka_pgtypes::datetime::neg_interval(*iv)?;
            Datum::Timestamptz(timestamptz_plus_interval(*ts, neg, tz)?)
        }
        // timestamptz - timestamptz → interval (absolute-instant difference).
        (BinaryOp::Sub, Datum::Timestamptz(a), Datum::Timestamptz(b)) => {
            Datum::Interval(timestamptz_diff(*a, *b))
        }
        // Any other combination with a timestamptz operand is undefined — surface
        // the genuine type error via `crabka_pgtypes::ops` (which yields TypeMismatch).
        _ => return Ok(None),
    };
    Ok(Some(result))
}

fn cmp_result(op: BinaryOp, ord: Option<Ordering>) -> Datum {
    match ord {
        None => Datum::Null,
        Some(o) => {
            let holds = match op {
                BinaryOp::Eq => o == Ordering::Equal,
                BinaryOp::Ne => o != Ordering::Equal,
                BinaryOp::Lt => o == Ordering::Less,
                BinaryOp::Le => o != Ordering::Greater,
                BinaryOp::Gt => o == Ordering::Greater,
                BinaryOp::Ge => o != Ordering::Less,
                _ => unreachable!("cmp_result called with non-comparison op"),
            };
            Datum::Bool(holds)
        }
    }
}

/// Statically infer the result column type of an expression, for RowDescription.
pub(crate) fn infer_type(expr: &Expr, scope: &Scope) -> Result<ColumnType, ExecError> {
    match expr {
        Expr::IntLiteral(s) => match ops::int_literal(s)? {
            Datum::Int4(_) => Ok(ColumnType::Int4),
            Datum::Int8(_) => Ok(ColumnType::Int8),
            _ => unreachable!(),
        },
        // SP32: a decimal/exponent literal types as unconstrained `numeric`.
        Expr::NumericLiteral(_) => Ok(ColumnType::Numeric(None)),
        Expr::StringLiteral(_) => Ok(ColumnType::Text),
        Expr::BoolLiteral(_) => Ok(ColumnType::Bool),
        // PostgreSQL types a bare NULL as "unknown"; the slice uses text as a
        // concrete stand-in so RowDescription has a real OID.
        Expr::NullLiteral => Ok(ColumnType::Text),
        Expr::Param(_) => Err(ExecError::Unsupported(
            "query parameters ($n) are not supported".into(),
        )),
        Expr::Default => Err(ExecError::Unsupported(
            "DEFAULT is only supported in INSERT target values".into(),
        )),
        Expr::Column { table, name } => {
            let idx = scope.resolve(table.as_deref(), name)?;
            Ok(scope.ty_at(idx))
        }
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => Ok(ColumnType::Bool),
            UnaryOp::Neg => infer_type(expr, scope),
        },
        Expr::Binary { op, left, right } => {
            match op {
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
                    let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
                    // SP37: a temporal operand resolves via PG's date/time arithmetic
                    // matrix first; a non-temporal pair falls through to the numeric tower.
                    Ok(datetime_result_type(*op, lt, rt)
                        .unwrap_or_else(|| numeric_result_type(lt, rt)))
                }
                // SP29: `||` yields text. PostgreSQL resolves the operator at plan
                // time and requires at least one operand to be text (`text || anynonarray`
                // / `anynonarray || text`); neither-text (e.g. `int || int`) is 42883.
                BinaryOp::Concat => {
                    let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
                    if lt != ColumnType::Text && rt != ColumnType::Text {
                        return Err(ExecError::UndefinedFunction(format!(
                            "operator does not exist: {} || {}",
                            lt.name(),
                            rt.name()
                        )));
                    }
                    Ok(ColumnType::Text)
                }
                _ => Ok(ColumnType::Bool),
            }
        }
        // SP29: a scalar function's result type; otherwise an aggregate result
        // type for RowDescription (count/sum -> int8, min/max -> the argument's
        // type); unknown names / bad arity / bad argument type -> 42883.
        Expr::Func(fc) if crate::func::is_scalar(&fc.name) => {
            crate::func::scalar_result_type(fc, scope)
        }
        // SP37: a date/time function's static result type.
        Expr::Func(fc) if crate::datetime_fn::is_datetime_func(&fc.name) => {
            crate::datetime_fn::datetime_func_result_type(fc, scope)
        }
        // SP38: a formatting/constructor function's static result type.
        Expr::Func(fc) if crate::format_fn::is_format_func(&fc.name) => {
            crate::format_fn::format_func_result_type(fc, scope)
        }
        Expr::Func(fc) => crate::agg::func_result_type(fc, scope),
        // SP28: predicates are boolean; CASE unifies its branch result types.
        Expr::IsNull { .. } | Expr::InList { .. } | Expr::Between { .. } | Expr::Like { .. } => {
            Ok(ColumnType::Bool)
        }
        Expr::Case {
            whens, else_result, ..
        } => infer_case_type(whens, else_result.as_deref(), scope),
        // SP31: a cast's static result type is the target type — but only if the
        // cast is defined; an undefined `(from, to)` pair is 42846 at plan time
        // (so it is rejected before any row is produced). A bare `NULL` infers as
        // text, and text → anything is defined, so `NULL::<any>` is accepted.
        Expr::Cast { expr, ty } => {
            let from = infer_type(expr, scope)?;
            if crabka_pgtypes::cast::cast_allowed(from, *ty) {
                Ok(*ty)
            } else {
                Err(ExecError::Type(TypeError::CannotCast {
                    from: from.name(),
                    to: ty.name(),
                }))
            }
        }
        // SP34: a resolved subquery's static type is recorded on the node.
        Expr::Const { ty, .. } => Ok(*ty),
        // SP34: EXISTS / IN-subquery / quantified comparison are always boolean
        // (typeable without executing — used by `describe`).
        Expr::Exists(_) | Expr::InSubquery { .. } | Expr::Quantified { .. } => Ok(ColumnType::Bool),
        // A scalar subquery's type needs the catalog; both the exec and describe
        // paths substitute it to `Const` before `infer_type` runs, so this is
        // unreachable in practice (defensive).
        Expr::ScalarSubquery(_) => Err(ExecError::Unsupported(
            "internal: scalar subquery must be resolved before type inference".into(),
        )),
    }
}

/// Infer a `CASE`'s result type by unifying every THEN result and the ELSE. A
/// bare `NULL` branch imposes no constraint; an all-NULL CASE is `text` (PG's
/// "unknown" → text); incompatible branch types are 42804.
fn infer_case_type(
    whens: &[(Expr, Expr)],
    else_result: Option<&Expr>,
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    let mut acc: Option<ColumnType> = None;
    for (_, result) in whens {
        acc = unify_branch(acc, result, scope)?;
    }
    if let Some(e) = else_result {
        acc = unify_branch(acc, e, scope)?;
    }
    Ok(acc.unwrap_or(ColumnType::Text))
}

/// Fold one branch/argument into a running unified type. A bare `NULL` is
/// type-neutral (imposes no constraint). Shared by `CASE` type inference and
/// SP29's `coalesce`/`greatest`/`least`.
pub(crate) fn unify_branch(
    acc: Option<ColumnType>,
    expr: &Expr,
    scope: &Scope,
) -> Result<Option<ColumnType>, ExecError> {
    if matches!(expr, Expr::NullLiteral) {
        return Ok(acc); // a bare NULL branch is type-neutral
    }
    let t = infer_type(expr, scope)?;
    match acc {
        None => Ok(Some(t)),
        Some(a) => Ok(Some(unify_types(a, t)?)),
    }
}

pub(crate) fn unify_types(a: ColumnType, b: ColumnType) -> Result<ColumnType, ExecError> {
    use ColumnType::{Float8, Int4, Int8, Numeric};
    // The numeric tower: int4/int8 < numeric < float8.
    let num_family = |t: ColumnType| matches!(t, Int4 | Int8 | Float8) || t.is_numeric();
    Ok(match (a, b) {
        (x, y) if x == y => x,
        // Mirror the arithmetic int4->int8 promotion rule.
        (Int4, Int8) | (Int8, Int4) => Int8,
        // SP30/SP32: any float8 wins; else (a numeric in the mix) → numeric.
        _ if a == Float8 || b == Float8 => Float8,
        _ if num_family(a) && num_family(b) => Numeric(None),
        _ => {
            return Err(ExecError::TypeMismatch(format!(
                "types {} and {} cannot be matched",
                a.name(),
                b.name()
            )));
        }
    })
}

/// Whether a column type is one of the SP37 date/time types.
fn is_temporal(t: ColumnType) -> bool {
    use ColumnType::{Date, Interval, Time, Timestamp, Timestamptz};
    matches!(t, Date | Time | Timestamp | Timestamptz | Interval)
}

/// PostgreSQL's date/time arithmetic result-type matrix. Returns `Some(result)`
/// for a defined `(op, lt, rt)` combination where at least one operand is
/// temporal; `None` otherwise — including a temporal operand in an UNdefined
/// combination, so the caller falls through to `numeric_result_type` and the real
/// type error surfaces at evaluation (it never invents a numeric result for a
/// temporal pair that PG would reject — eval is the authority).
fn datetime_result_type(op: BinaryOp, lt: ColumnType, rt: ColumnType) -> Option<ColumnType> {
    use BinaryOp::{Add, Div, Mul, Sub};
    use ColumnType::{Date, Float8, Int4, Int8, Interval, Numeric, Time, Timestamp, Timestamptz};
    // Only engage the matrix when a temporal operand is present; a purely numeric
    // pair belongs to the numeric tower.
    if !is_temporal(lt) && !is_temporal(rt) {
        return None;
    }
    let is_int = |t: ColumnType| matches!(t, Int4 | Int8);
    let is_number = |t: ColumnType| matches!(t, Int4 | Int8 | Float8 | Numeric(_));
    Some(match (op, lt, rt) {
        // date ± integer → date; integer + date → date.
        (Add, Date, r) | (Sub, Date, r) if is_int(r) => Date,
        (Add, l, Date) if is_int(l) => Date,
        // date − date → int4 (number of days).
        (Sub, Date, Date) => Int4,
        // date ± interval → timestamp; interval + date → timestamp.
        (Add | Sub, Date, Interval) | (Add, Interval, Date) => Timestamp,
        // timestamp ± interval → timestamp; interval + timestamp → timestamp.
        (Add | Sub, Timestamp, Interval) | (Add, Interval, Timestamp) => Timestamp,
        // timestamptz ± interval → timestamptz; interval + timestamptz → timestamptz.
        (Add | Sub, Timestamptz, Interval) | (Add, Interval, Timestamptz) => Timestamptz,
        // timestamp − timestamp / timestamptz − timestamptz → interval.
        (Sub, Timestamp, Timestamp) | (Sub, Timestamptz, Timestamptz) => Interval,
        // interval ± interval → interval.
        (Add | Sub, Interval, Interval) => Interval,
        // interval * / number → interval; number * interval → interval.
        (Mul | Div, Interval, r) if is_number(r) => Interval,
        (Mul, l, Interval) if is_number(l) => Interval,
        // time ± interval → time; interval + time → time.
        (Add | Sub, Time, Interval) | (Add, Interval, Time) => Time,
        // date + time / time + date → timestamp (combine the calendar date and
        // the wall-clock time).
        (Add, Date, Time) | (Add, Time, Date) => Timestamp,
        // Any other combination with a temporal operand is undefined here — fall
        // through so eval raises the genuine type error.
        _ => return None,
    })
}

/// The result type of `+ - * /` on two operand types. The numeric tower is
/// int < numeric < float8: any float8 makes the result float8; else any numeric
/// makes it numeric; else int4 only if both are int4, else int8. Permissive about
/// non-numeric operands (a real type error surfaces at evaluation).
fn numeric_result_type(lt: ColumnType, rt: ColumnType) -> ColumnType {
    use ColumnType::{Float8, Int4};
    if lt == Float8 || rt == Float8 {
        Float8
    } else if lt.is_numeric() || rt.is_numeric() {
        ColumnType::Numeric(None)
    } else if lt == Int4 && rt == Int4 {
        Int4
    } else {
        ColumnType::Int8
    }
}

#[cfg(test)]
mod tests {
    use crabka_pgcatalog::{Column, Table};
    use crabka_pgparser::parser::parse_expr_for_test as pexpr;
    use crabka_pgtypes::{ColumnType, Datum};

    use super::*;

    fn table() -> Table {
        Table {
            id: 1,
            name: "t".into(),
            columns: vec![
                Column::new("a", ColumnType::Int4),
                Column::new("b", ColumnType::Int4),
            ],
            sharded: false,
            sharding: None,
            foreign: None,
        }
    }

    /// Build the `Scope` the tests evaluate against: the table's single-relation
    /// scope, or the empty scope (FROM-less expressions).
    fn scope_of(t: Option<&Table>) -> Scope {
        match t {
            Some(t) => Scope::single(t, &t.name),
            None => Scope::empty(),
        }
    }

    fn ev(sql: &str, t: Option<&Table>, vals: &[Datum]) -> Datum {
        let ctx = crate::clock::EvalCtx::test_default();
        eval(&pexpr(sql).expect("parse"), &scope_of(t), vals, &ctx).expect("eval")
    }

    /// Defense-in-depth: an expression tree deeper than `MAX_EVAL_DEPTH` — built
    /// DIRECTLY here, bypassing the parser's parse-time cap — must return a clean
    /// `54001` from `eval`, never overflow the stack. (In production the parser
    /// cap means such a tree can't be built, but the guard must still hold.)
    #[test]
    fn eval_rejects_an_over_deep_tree_with_54001() {
        let mut e = Expr::BoolLiteral(true);
        for _ in 0..(MAX_EVAL_DEPTH + 50) {
            e = Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(e),
            };
        }
        let ctx = crate::clock::EvalCtx::test_default();
        let err = eval(&e, &Scope::empty(), &[], &ctx).expect_err("must reject");
        assert_eq!(err, ExecError::StackDepthExceeded);
        assert_eq!(err.into_pg().code, "54001");
    }

    /// A tree right at the limit still evaluates (the guard does not fire early).
    #[test]
    fn eval_accepts_a_tree_at_the_limit() {
        // `Not` chains of even length evaluate back to the base value.
        let depth = MAX_EVAL_DEPTH - 1; // safely under, even count from `true`
        let depth = depth - (depth % 2);
        let mut e = Expr::BoolLiteral(true);
        for _ in 0..depth {
            e = Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(e),
            };
        }
        let ctx = crate::clock::EvalCtx::test_default();
        assert_eq!(
            eval(&e, &Scope::empty(), &[], &ctx).expect("at-limit tree evaluates"),
            Datum::Bool(true),
        );
    }

    #[test]
    fn eval_takes_ctx_and_ignores_it_for_non_temporal() {
        let ctx = crate::clock::EvalCtx::test_default();
        let e = crabka_pgparser::parser::parse_expr_for_test("1 + 2").expect("parse");
        assert_eq!(
            eval(&e, &Scope::empty(), &[], &ctx).expect("eval"),
            Datum::Int4(3)
        );
    }

    #[test]
    fn arithmetic_and_columns() {
        let t = table();
        assert_eq!(
            ev("a + b * 2", Some(&t), &[Datum::Int4(3), Datum::Int4(4)]),
            Datum::Int4(11)
        );
    }

    #[test]
    fn comparison_yields_bool_and_null() {
        let t = table();
        assert_eq!(
            ev("a > b", Some(&t), &[Datum::Int4(2), Datum::Int4(1)]),
            Datum::Bool(true)
        );
        assert_eq!(
            ev("a > b", Some(&t), &[Datum::Null, Datum::Int4(1)]),
            Datum::Null
        );
    }

    #[test]
    fn literals_no_table() {
        assert_eq!(ev("1 + 1", None, &[]), Datum::Int4(2));
        assert_eq!(ev("'x'", None, &[]), Datum::Text("x".into()));
        assert_eq!(ev("not true", None, &[]), Datum::Bool(false));
    }

    #[test]
    fn numeric_literals_arithmetic_and_inference() {
        let num = |s: &str| Datum::Numeric(crabka_pgtypes::numeric::parse(s).expect("n"));
        // SP32: a bare decimal literal evaluates and types as `numeric`.
        assert_eq!(ev("1.5", None, &[]), num("1.5"));
        assert_eq!(
            infer_type(&pexpr("1.5").expect("parse"), &scope_of(None)).expect("infer"),
            ColumnType::Numeric(None)
        );
        // int ⊕ numeric promotes to numeric (exact). `3 / 2.0` uses PG's div scale.
        assert_eq!(ev("1 + 0.5", None, &[]), num("1.5"));
        assert_eq!(ev("3 / 2.0", None, &[]), num("1.5000000000000000"));
        assert_eq!(ev("- 2.5", None, &[]), num("-2.5"));
        assert_eq!(
            infer_type(&pexpr("a + 1.0").expect("parse"), &scope_of(Some(&table())))
                .expect("infer"),
            ColumnType::Numeric(None)
        );
        // CASE/coalesce unify int and numeric to numeric.
        assert_eq!(
            infer_type(
                &pexpr("case when a > 0 then 1 else 2.5 end").expect("parse"),
                &scope_of(Some(&table()))
            )
            .expect("infer"),
            ColumnType::Numeric(None)
        );
        // float8 is still reachable via an explicit cast (and wins over numeric).
        assert_eq!(ev("1.5::float8", None, &[]), Datum::Float8(1.5));
        assert_eq!(ev("3 / 2.0::float8", None, &[]), Datum::Float8(1.5));
    }

    /// SP37 §8: the tz-AWARE temporal cells that live in `apply_binary` (because
    /// they need the session zone) — `timestamptz ± interval → timestamptz` and
    /// `timestamptz − timestamptz → interval`. Each asserts BOTH the produced value
    /// AND that `infer_type` predicts the same type (no infer/eval mismatch).
    #[test]
    fn timestamptz_arithmetic_is_tz_aware_in_apply_binary() {
        use crabka_pgtypes::datetime;
        // A non-UTC session zone proves the tz path is actually exercised
        // (a `timestamptz` literal without an explicit offset is interpreted in it,
        // and the calendar shift is applied in it).
        let tz = jiff::tz::TimeZone::get("America/New_York").expect("tzdb has NY");
        let ctx = crate::clock::EvalCtx {
            time_zone: tz.clone(),
            ..crate::clock::EvalCtx::test_default()
        };
        let tstz = |s: &str| Datum::Timestamptz(datetime::parse_timestamptz(s, &tz).expect("tstz"));
        let iv = |s: &str| Datum::Interval(datetime::parse_interval(s).expect("iv"));

        // timestamptz + interval '1 hour' → an absolute-instant shift of +1h.
        // 2024-01-15 12:00:00 in NY (EST, -05) is the instant 17:00:00 UTC;
        // + 1 hour → 18:00:00 UTC = 2024-01-15 13:00:00 NY.
        let base = tstz("2024-01-15 12:00:00");
        let got = apply_binary(BinaryOp::Add, &base, &iv("1 hour"), &ctx).expect("add");
        assert_eq!(got, tstz("2024-01-15 13:00:00"));
        assert!(matches!(got, Datum::Timestamptz(_)));

        // The same via a calendar-aware `+ 1 day` ACROSS the US spring-forward DST
        // boundary (2024-03-10 02:00 → 03:00 in NY): a wall-clock `+1 day` keeps the
        // same wall-clock hour, so 2024-03-09 12:00 NY + 1 day = 2024-03-10 12:00 NY
        // even though only 23 absolute hours elapsed.
        let pre_dst = tstz("2024-03-09 12:00:00");
        let after_day = apply_binary(BinaryOp::Add, &pre_dst, &iv("1 day"), &ctx).expect("add day");
        assert_eq!(after_day, tstz("2024-03-10 12:00:00"));

        // timestamptz - interval → timestamptz (the reverse).
        let back = apply_binary(BinaryOp::Sub, &got, &iv("1 hour"), &ctx).expect("sub");
        assert_eq!(back, base);

        // timestamptz - timestamptz → interval (absolute-instant difference: the two
        // instants are 1 h apart, which PG stores as `01:00:00`).
        let diff = apply_binary(BinaryOp::Sub, &got, &base, &ctx).expect("diff");
        assert_eq!(diff, iv("1 hour"));
        assert!(matches!(diff, Datum::Interval(_)));

        // NULL propagates on either operand.
        assert_eq!(
            apply_binary(BinaryOp::Add, &Datum::Null, &iv("1 hour"), &ctx).expect("null"),
            Datum::Null
        );

        // infer_type agrees on the result types for these cells (no plan/eval drift).
        let tstz_col = Table {
            id: 9,
            name: "tz".into(),
            columns: vec![
                Column::new("ts", ColumnType::Timestamptz),
                Column::new("iv", ColumnType::Interval),
            ],
            sharded: false,
            sharding: None,
            foreign: None,
        };
        let tstz_scope = scope_of(Some(&tstz_col));
        assert_eq!(
            infer_type(&pexpr("ts + iv").expect("parse"), &tstz_scope).expect("infer"),
            ColumnType::Timestamptz
        );
        assert_eq!(
            infer_type(&pexpr("ts - iv").expect("parse"), &tstz_scope).expect("infer"),
            ColumnType::Timestamptz
        );
        assert_eq!(
            infer_type(&pexpr("ts - ts").expect("parse"), &tstz_scope).expect("infer"),
            ColumnType::Interval
        );
    }

    #[test]
    fn undefined_column_is_42703() {
        let t = table();
        let ctx = crate::clock::EvalCtx::test_default();
        let err = eval(
            &pexpr("zzz").expect("parse"),
            &scope_of(Some(&t)),
            &[Datum::Int4(1), Datum::Int4(1)],
            &ctx,
        )
        .expect_err("eval zzz should fail");
        assert_eq!(err.into_pg().code, "42703");
    }

    #[test]
    fn parameter_is_0a000() {
        let ctx = crate::clock::EvalCtx::test_default();
        let err = eval(&pexpr("$1").expect("parse"), &scope_of(None), &[], &ctx)
            .expect_err("eval $1 should fail");
        assert_eq!(err.into_pg().code, "0A000");
    }

    #[test]
    fn type_inference_is_static() {
        let t = table();
        assert_eq!(
            infer_type(&pexpr("a + b").expect("parse"), &scope_of(Some(&t))).expect("infer"),
            ColumnType::Int4
        );
        assert_eq!(
            infer_type(&pexpr("a > b").expect("parse"), &scope_of(Some(&t))).expect("infer"),
            ColumnType::Bool
        );
        assert_eq!(
            infer_type(&pexpr("'x'").expect("parse"), &scope_of(None)).expect("infer"),
            ColumnType::Text
        );
        assert_eq!(
            infer_type(&pexpr("2147483648").expect("parse"), &scope_of(None)).expect("infer"),
            ColumnType::Int8
        );
    }

    // ---- SP28: predicate + conditional expression breadth ----

    fn err_code(sql: &str, t: Option<&Table>, vals: &[Datum]) -> String {
        let ctx = crate::clock::EvalCtx::test_default();
        eval(&pexpr(sql).expect("parse"), &scope_of(t), vals, &ctx)
            .expect_err("expected error")
            .into_pg()
            .code
    }

    #[test]
    fn is_null_is_never_null() {
        assert_eq!(ev("null is null", None, &[]), Datum::Bool(true));
        assert_eq!(ev("1 is null", None, &[]), Datum::Bool(false));
        assert_eq!(ev("1 is not null", None, &[]), Datum::Bool(true));
        assert_eq!(ev("null is not null", None, &[]), Datum::Bool(false));
    }

    #[test]
    fn in_list_three_valued_null_logic() {
        assert_eq!(ev("1 in (1, 2)", None, &[]), Datum::Bool(true));
        assert_eq!(ev("3 in (1, 2)", None, &[]), Datum::Bool(false));
        assert_eq!(ev("null in (1, 2)", None, &[]), Datum::Null);
        // no equal match but a NULL element present -> unknown (NULL).
        assert_eq!(ev("3 in (1, null)", None, &[]), Datum::Null);
        // an equal match short-circuits past the NULL element -> true.
        assert_eq!(ev("1 in (1, null)", None, &[]), Datum::Bool(true));
        // NOT IN is the negation; NULL stays NULL.
        assert_eq!(ev("3 not in (1, null)", None, &[]), Datum::Null);
        assert_eq!(ev("3 not in (1, 2)", None, &[]), Datum::Bool(true));
        assert_eq!(ev("1 not in (1, 2)", None, &[]), Datum::Bool(false));
    }

    #[test]
    fn between_null_propagates() {
        assert_eq!(ev("5 between 1 and 10", None, &[]), Datum::Bool(true));
        assert_eq!(ev("5 not between 1 and 10", None, &[]), Datum::Bool(false));
        assert_eq!(ev("5 between 1 and null", None, &[]), Datum::Null);
        assert_eq!(ev("null between 1 and 2", None, &[]), Datum::Null);
    }

    #[test]
    fn like_matcher_wildcards_escape_and_ilike() {
        assert!(like_match("abc", "a%", false).expect("m"));
        assert!(like_match("abc", "a_c", false).expect("m"));
        assert!(!like_match("ac", "a_c", false).expect("m"));
        assert!(like_match("anything", "%", false).expect("m"));
        assert!(like_match("", "%", false).expect("m"));
        assert!(like_match("axyzc", "a%c", false).expect("m"));
        assert!(!like_match("abd", "a%c", false).expect("m"));
        // `\` escapes the next pattern char: `a\%b` matches a literal `%`.
        assert!(like_match("a%b", "a\\%b", false).expect("m"));
        assert!(!like_match("axb", "a\\%b", false).expect("m"));
        // ILIKE folds ASCII case.
        assert!(like_match("ABC", "a%", true).expect("m"));
        assert!(!like_match("ABC", "a%", false).expect("m"));
        // a pattern ending in a lone `\` is an invalid escape (22025).
        assert_eq!(
            like_match("a", "a\\", false)
                .expect_err("invalid escape")
                .into_pg()
                .code,
            "22025"
        );
    }

    #[test]
    fn like_eval_null_and_type_errors() {
        assert_eq!(ev("null like 'a'", None, &[]), Datum::Null);
        assert_eq!(ev("'a' like null", None, &[]), Datum::Null);
        // a non-text operand is a 42804.
        assert_eq!(err_code("1 like 'a'", None, &[]), "42804");
    }

    #[test]
    fn case_searched_simple_and_lazy() {
        // searched: first TRUE wins; false/NULL skip.
        assert_eq!(
            ev(
                "case when false then 'a' when true then 'b' else 'c' end",
                None,
                &[]
            ),
            Datum::Text("b".into())
        );
        assert_eq!(
            ev("case when null then 'a' else 'z' end", None, &[]),
            Datum::Text("z".into())
        );
        // no match, no ELSE -> NULL.
        assert_eq!(ev("case when false then 'a' end", None, &[]), Datum::Null);
        // simple form: equality; NULL never matches.
        assert_eq!(
            ev("case 1 when 1 then 'one' else 'other' end", None, &[]),
            Datum::Text("one".into())
        );
        assert_eq!(
            ev("case null when null then 'x' else 'y' end", None, &[]),
            Datum::Text("y".into())
        );
        // lazy: the unreached `1/0` branch must not raise division-by-zero.
        assert_eq!(
            ev("case when false then 1/0 else 0 end", None, &[]),
            Datum::Int4(0)
        );
    }

    #[test]
    fn case_when_non_boolean_condition_is_42804() {
        assert_eq!(err_code("case when 1 then 'x' end", None, &[]), "42804");
    }

    // ---- SP31: explicit casts ----

    #[test]
    fn cast_evaluates_each_supported_conversion() {
        // text → numeric/bool.
        assert_eq!(ev("'42'::int4", None, &[]), Datum::Int4(42));
        assert_eq!(
            ev("'9000000000'::int8", None, &[]),
            Datum::Int8(9_000_000_000)
        );
        assert_eq!(ev("'1.5'::float8", None, &[]), Datum::Float8(1.5));
        assert_eq!(ev("'true'::bool", None, &[]), Datum::Bool(true));
        // numeric → numeric (float8 → int rounds half-to-even).
        assert_eq!(ev("1.5::int4", None, &[]), Datum::Int4(2));
        assert_eq!(ev("(5::int8)::int4", None, &[]), Datum::Int4(5));
        // bool ↔ int4, and → text (`true`/`false`, not `t`/`f`).
        assert_eq!(ev("true::int4", None, &[]), Datum::Int4(1));
        assert_eq!(ev("5::bool", None, &[]), Datum::Bool(true));
        assert_eq!(ev("0::bool", None, &[]), Datum::Bool(false));
        assert_eq!(ev("42::text", None, &[]), Datum::Text("42".into()));
        assert_eq!(ev("true::text", None, &[]), Datum::Text("true".into()));
        // NULL casts to NULL; the CAST() spelling is identical to `::`.
        assert_eq!(ev("null::int4", None, &[]), Datum::Null);
        assert_eq!(ev("CAST('7' AS int4)", None, &[]), Datum::Int4(7));
    }

    #[test]
    fn cast_infers_target_type_and_rejects_undefined_at_plan_time() {
        let t = table();
        // The static result type is the target type; a column operand resolves too.
        assert_eq!(
            infer_type(&pexpr("'42'::int8").expect("parse"), &scope_of(None)).expect("infer"),
            ColumnType::Int8
        );
        assert_eq!(
            infer_type(&pexpr("a::text").expect("parse"), &scope_of(Some(&t))).expect("infer"),
            ColumnType::Text
        );
        // A bare NULL infers as text, and text → anything is defined.
        assert_eq!(
            infer_type(&pexpr("null::bool").expect("parse"), &scope_of(None)).expect("infer"),
            ColumnType::Bool
        );
        // An undefined cast is rejected at plan time (42846), before evaluation:
        // a float8 column → bool has no defined cast.
        let ft = Table {
            id: 1,
            name: "t".into(),
            columns: vec![Column::new("a", ColumnType::Float8)],
            sharded: false,
            sharding: None,
            foreign: None,
        };
        let err = infer_type(&pexpr("a::bool").expect("parse"), &scope_of(Some(&ft)))
            .expect_err("float8->bool is undefined");
        assert_eq!(err.into_pg().code, "42846");
    }

    #[test]
    fn cast_runtime_error_surface() {
        // Undefined cast at eval (42846), bad text syntax (22P02), overflow (22003).
        assert_eq!(err_code("1.5::bool", None, &[]), "42846");
        assert_eq!(err_code("'abc'::int4", None, &[]), "22P02");
        assert_eq!(err_code("'99999999999'::int4", None, &[]), "22003");
    }

    #[test]
    fn infer_predicate_and_case_result_types() {
        let t = table();
        let scope = scope_of(Some(&t));
        for sql in ["a is null", "a in (1,2)", "a between 1 and 2", "a like 'x'"] {
            // `a` is int4; `a like 'x'` infers Bool statically regardless.
            let got = infer_type(&pexpr(sql).expect("parse"), &scope).expect("infer");
            assert_eq!(got, ColumnType::Bool, "for {sql}");
        }
        // CASE unifies int4 + int8 -> int8.
        assert_eq!(
            infer_type(
                &pexpr("case when a > 0 then 1 else 2147483648 end").expect("parse"),
                &scope
            )
            .expect("infer"),
            ColumnType::Int8
        );
        // a bare NULL branch is type-neutral -> int4 from the other branch.
        assert_eq!(
            infer_type(
                &pexpr("case when a > 0 then 1 else null end").expect("parse"),
                &scope
            )
            .expect("infer"),
            ColumnType::Int4
        );
        // incompatible branch types -> 42804.
        let err = infer_type(
            &pexpr("case when a > 0 then 1 else 'x' end").expect("parse"),
            &scope,
        )
        .expect_err("incompatible");
        assert_eq!(err.into_pg().code, "42804");
    }

    // ---- SP37 Task 11: temporal result-type inference + unary-minus interval ----

    #[test]
    fn datetime_literal_eval_and_infer() {
        let ctx = crate::clock::EvalCtx::test_default();
        let scope = Scope::empty();
        let p = |s: &str| crabka_pgparser::parser::parse_expr_for_test(s).expect("parse");
        assert_eq!(
            eval(&p("DATE '2024-01-15'"), &scope, &[], &ctx).expect("eval"),
            Datum::Date(crabka_pgtypes::datetime::parse_date("2024-01-15").expect("d"))
        );
        assert_eq!(
            infer_type(&p("DATE '2024-01-15'"), &scope).expect("inf"),
            ColumnType::Date
        );
        assert_eq!(
            infer_type(&p("DATE '2024-02-01' - DATE '2024-01-01'"), &scope).expect("inf"),
            ColumnType::Int4
        );
        assert_eq!(
            infer_type(&p("DATE '2024-01-01' + INTERVAL '1 day'"), &scope).expect("inf"),
            ColumnType::Timestamp
        );
    }

    #[test]
    fn unary_minus_interval() {
        let ctx = crate::clock::EvalCtx::test_default();
        let scope = Scope::empty();
        let p = crabka_pgparser::parser::parse_expr_for_test("- INTERVAL '1 day'").expect("parse");
        assert_eq!(
            eval(&p, &scope, &[], &ctx).expect("eval"),
            Datum::Interval(crabka_pgtypes::datetime::Interval {
                months: 0,
                days: -1,
                micros: 0
            })
        );
    }
}
