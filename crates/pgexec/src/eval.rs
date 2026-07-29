//! Expression evaluation over Datums, plus static result-type inference (used
//! to build a stable RowDescription before any row is produced).

use std::cmp::Ordering;

use crabka_pgparser::ast::{BinaryOp, Expr, UnaryOp};
use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType, TypeError, ops};

use crate::{
    array_fn::{self, ConcatForm, Quantifier},
    clock::EvalCtx,
    error::ExecError,
    json_fn::{self, JsonOp},
    scope::Scope,
};

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
            apply_binary_of(*op, left, right, &l, &r, scope, ctx)
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
        // The jsonb + array function families, tried after the older families and
        // before the aggregate-context error, exactly like the arms above.
        Expr::Func(fc) if json_fn::is_json_func(&fc.name) => {
            json_fn::eval_json(fc, ctx, |e| eval_depth(e, scope, values, ctx, d))
        }
        Expr::Func(fc) if array_fn::is_array_func(&fc.name) => {
            array_fn::eval_array(fc, ctx, |e| eval_depth(e, scope, values, ctx, d))
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
            // `ARRAY[]::int[]`: the cast supplies the element type the empty
            // constructor cannot infer, so it never reaches the operand eval.
            if let Some(empty) = empty_array_cast(expr, *ty) {
                return Ok(empty);
            }
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
        // `ARRAY[e1, e2, …]`: every element is coerced to the constructor's
        // unified element type, so the built array is homogeneous.
        Expr::ArrayLiteral(items) => {
            let elem = array_literal_elem_type(items, scope)?;
            let target = elem.column_type();
            let mut elems = Vec::with_capacity(items.len());
            for item in items {
                let v = eval_depth(item, scope, values, ctx, d)?;
                elems.push(crabka_pgtypes::cast::cast(&v, target, &ctx.time_zone)?);
            }
            Ok(Datum::Array(ArrayValue::new(elem, elems)))
        }
        // `base[index]`: 1-based, and out-of-range / NULL is SQL NULL (not an error).
        Expr::Subscript { base, index } => {
            let b = eval_depth(base, scope, values, ctx, d)?;
            let i = eval_depth(index, scope, values, ctx, d)?;
            array_fn::array_subscript(&b, &i)
        }
        // `x <op> ANY|ALL (array)` — the array form of a quantified comparison,
        // with three-valued logic supplied by `array_fn::eval_quantified`.
        Expr::QuantifiedArray {
            expr,
            op,
            all,
            array,
        } => {
            let x = eval_depth(expr, scope, values, ctx, d)?;
            let a = eval_depth(array, scope, values, ctx, d)?;
            array_fn::eval_quantified(&a, quantifier_of(*all), |elem| {
                apply_binary(*op, &x, elem, ctx)
            })
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

/// Apply a binary operator when the operand *expressions* and the scope are also
/// in hand — the front door both evaluators use.
///
/// Only `||` needs more than the values: PostgreSQL resolves which concatenation
/// operator applies (text, jsonb, or one of the three array forms) from the
/// operands' STATIC types, and those are indistinguishable once an operand has
/// evaluated to SQL NULL. Every other operator resolves from the values alone and
/// goes straight to [`apply_binary`].
pub(crate) fn apply_binary_of(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    l: &Datum,
    r: &Datum,
    scope: &Scope,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let (lc, rc) = coerce_untyped_literal_operands(op, left, right, l, r, ctx)?;
    let (l, r) = (lc.as_ref().unwrap_or(l), rc.as_ref().unwrap_or(r));
    if op == BinaryOp::Concat {
        let (kind, _) = resolve_concat(left, right, scope)?;
        return apply_concat(kind, l, r, ctx);
    }
    apply_binary(op, l, r, ctx)
}

/// Convert an `unknown` string-literal operand's *value* to the type the
/// operator resolved it to. Typing it is not enough on its own: the literal
/// still evaluates to a `Datum::Text`, while the jsonb operators need a
/// `Datum::Jsonb` (or a `text[]` path). Returns `None` per side when nothing
/// needs converting, so the common case copies nothing.
fn coerce_untyped_literal_operands(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    l: &Datum,
    r: &Datum,
    ctx: &EvalCtx,
) -> Result<(Option<Datum>, Option<Datum>), ExecError> {
    // Only a jsonb counterpart resolves a bare literal; an array must not, so
    // that `ARRAY['a'] || 'b'` still appends an element (PostgreSQL's
    // `anyarray || anyelement`).
    let target = |other: &Datum| -> Option<ColumnType> {
        if !matches!(other, Datum::Jsonb(_)) {
            return None;
        }
        match op {
            BinaryOp::JsonGetPath
            | BinaryOp::JsonGetPathText
            | BinaryOp::KeyExistsAny
            | BinaryOp::KeyExistsAll => ColumnType::array_of(ColumnType::Text),
            BinaryOp::Contains
            | BinaryOp::ContainedBy
            | BinaryOp::Concat
            | BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::Gt
            | BinaryOp::Ge => Some(ColumnType::Jsonb),
            _ => None,
        }
    };
    let convert = |e: &Expr, v: &Datum, other: &Datum| -> Result<Option<Datum>, ExecError> {
        if !matches!(e, Expr::StringLiteral(_)) || !matches!(v, Datum::Text(_)) {
            return Ok(None);
        }
        match target(other) {
            Some(ty) => Ok(Some(crabka_pgtypes::cast::cast(v, ty, &ctx.time_zone)?)),
            None => Ok(None),
        }
    };
    Ok((convert(left, l, r)?, convert(right, r, l)?))
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
        // jsonb `-` (delete a key, an index, or a set of keys) overloads the
        // arithmetic `-`; a jsonb LEFT operand is what selects it, so every
        // numeric/date pair keeps its existing behavior.
        BinaryOp::Sub if matches!(l, Datum::Jsonb(_)) => {
            json_fn::eval_json_operator(JsonOp::Delete, l, r)
        }
        BinaryOp::Sub => Ok(ops::sub(l, r)?),
        BinaryOp::Mul => Ok(ops::mul(l, r)?),
        BinaryOp::Div => Ok(ops::div(l, r)?),
        BinaryOp::And => Ok(ops::and(l, r)?),
        BinaryOp::Or => Ok(ops::or(l, r)?),
        // Resolved from the runtime values' types, which agrees with the static
        // resolution for every non-NULL pair. A NULL operand carries no type, so
        // this falls back to text concatenation — and `||` propagates NULL for
        // every family, so only the reported *type* could differ. Callers with
        // the operand expressions use [`apply_binary_of`], which is exact.
        BinaryOp::Concat => {
            let kind = l
                .column_type()
                .zip(r.column_type())
                .and_then(|(lt, rt)| concat_kind(lt, rt))
                .map_or(ConcatKind::Text, |(kind, _)| kind);
            apply_concat(kind, l, r, ctx)
        }
        BinaryOp::JsonGet => json_fn::eval_json_operator(JsonOp::Get, l, r),
        BinaryOp::JsonGetText => json_fn::eval_json_operator(JsonOp::GetText, l, r),
        BinaryOp::JsonGetPath => json_fn::eval_json_operator(JsonOp::GetPath, l, r),
        BinaryOp::JsonGetPathText => json_fn::eval_json_operator(JsonOp::GetPathText, l, r),
        BinaryOp::KeyExists => json_fn::eval_json_operator(JsonOp::KeyExists, l, r),
        BinaryOp::KeyExistsAny => json_fn::eval_json_operator(JsonOp::KeyExistsAny, l, r),
        BinaryOp::KeyExistsAll => json_fn::eval_json_operator(JsonOp::KeyExistsAll, l, r),
        // `@>` / `<@` are defined for BOTH jsonb and arrays.
        BinaryOp::Contains | BinaryOp::ContainedBy => apply_containment(op, l, r),
        // `&&` is array-only; `array_overlap` already yields NULL for a NULL side.
        BinaryOp::Overlaps => array_fn::array_overlap(l, r),
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            let ord = ops::compare(l, r)?;
            Ok(cmp_result(op, ord))
        }
    }
}

/// `@>` / `<@`: the operand values pick the jsonb or the array family (the
/// static types already agreed at plan time). Both families are strict, so two
/// SQL NULLs need no family at all.
fn apply_containment(op: BinaryOp, l: &Datum, r: &Datum) -> Result<Datum, ExecError> {
    let contains = op == BinaryOp::Contains;
    if matches!(l, Datum::Jsonb(_)) || matches!(r, Datum::Jsonb(_)) {
        let json_op = if contains {
            JsonOp::Contains
        } else {
            JsonOp::ContainedBy
        };
        return json_fn::eval_json_operator(json_op, l, r);
    }
    if matches!(l, Datum::Array(_)) || matches!(r, Datum::Array(_)) {
        return if contains {
            array_fn::array_contains(l, r)
        } else {
            array_fn::array_contained_by(l, r)
        };
    }
    if l.is_null() && r.is_null() {
        return Ok(Datum::Null);
    }
    Err(undefined_operator_for(op, l, r))
}

/// Which of PostgreSQL's concatenation operators a `||` resolves to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConcatKind {
    /// `text || anynonarray` / `anynonarray || text`.
    Text,
    /// `jsonb || jsonb`.
    Jsonb,
    /// One of the three array forms (`anyarray || anyarray`, `anyarray ||
    /// anyelement`, `anyelement || anyarray`).
    Array(ConcatForm),
}

/// Resolve `left || right` from the operands' STATIC types: the operator that
/// applies plus its result type. 42883 when no `||` is defined for the pair.
fn resolve_concat(
    left: &Expr,
    right: &Expr,
    scope: &Scope,
) -> Result<(ConcatKind, ColumnType), ExecError> {
    let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
    let (lt, rt) = adopt_null_literal_type(left, right, lt, rt);
    concat_kind(lt, rt).ok_or_else(|| undefined_operator("||", lt, rt))
}

/// This codebase types a bare `NULL` literal as `text`, but PostgreSQL resolves
/// the literal's `unknown` type against the other operand. That matters for `||`:
/// `ARRAY[1,2] || NULL` must pick `array_cat` (yielding `{1,2}`), not
/// `array_append` (which would yield `{1,2,NULL}`). Only the two families whose
/// `||` a text fallback would silently steal — jsonb and arrays — are adopted.
fn adopt_null_literal_type(
    left: &Expr,
    right: &Expr,
    lt: ColumnType,
    rt: ColumnType,
) -> (ColumnType, ColumnType) {
    let adopts = |t: ColumnType| t == ColumnType::Jsonb || t.array_element().is_some();
    if matches!(left, Expr::NullLiteral) && adopts(rt) {
        (rt, rt)
    } else if matches!(right, Expr::NullLiteral) && adopts(lt) {
        (lt, lt)
    } else {
        adopt_string_literal_type(left, right, lt, rt)
    }
}

/// PostgreSQL leaves a bare string literal `unknown` and resolves it against the
/// other operand; this codebase types it `text` at once. Adopt it into `jsonb`
/// when the other side is jsonb, so `j @> '{"a":1}'` and `j || '{"b":2}'` mean
/// what they do in PostgreSQL. Without this, `||` is the dangerous one: a
/// jsonb/text pair falls through to *string* concatenation and returns a
/// plausible-looking wrong answer instead of merging.
///
/// Deliberately jsonb-only. An array must NOT adopt, because PostgreSQL's
/// `anyarray || anyelement` is what makes `ARRAY['a'] || 'b'` append `'b'` as an
/// element rather than concatenating two arrays.
fn adopt_string_literal_type(
    left: &Expr,
    right: &Expr,
    lt: ColumnType,
    rt: ColumnType,
) -> (ColumnType, ColumnType) {
    let untyped = |e: &Expr| matches!(e, Expr::StringLiteral(_));
    if untyped(left) && rt == ColumnType::Jsonb {
        (ColumnType::Jsonb, rt)
    } else if untyped(right) && lt == ColumnType::Jsonb {
        (lt, ColumnType::Jsonb)
    } else {
        (lt, rt)
    }
}

/// Resolve an `unknown` string-literal operand of a jsonb operator to the type
/// that operator expects on that side: `text[]` for the path and multi-key
/// operators, `jsonb` for containment.
fn adopt_json_operand_types(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    lt: ColumnType,
    rt: ColumnType,
) -> (ColumnType, ColumnType) {
    if !matches!(right, Expr::StringLiteral(_)) || lt != ColumnType::Jsonb {
        return adopt_string_literal_type(left, right, lt, rt);
    }
    let expected = match op {
        BinaryOp::JsonGetPath
        | BinaryOp::JsonGetPathText
        | BinaryOp::KeyExistsAny
        | BinaryOp::KeyExistsAll => ColumnType::array_of(ColumnType::Text),
        BinaryOp::Contains | BinaryOp::ContainedBy => Some(ColumnType::Jsonb),
        _ => None,
    };
    (lt, expected.unwrap_or(rt))
}

// ---- `unknown` literals as FUNCTION ARGUMENTS ----
//
// The operator side of PostgreSQL's `unknown` resolution is above
// (`adopt_string_literal_type` / `adopt_json_operand_types`). The three items
// below are its function-argument counterpart, shared by the families that need
// it (`json_fn`, `array_fn`): each family writes its parameter types down once,
// as a `param_types` rule over [`ArgType`]s, and both the plan-time result-type
// resolver and the run-time evaluator drive that one rule.

/// What one argument contributes to PostgreSQL's function-argument type
/// resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArgType {
    /// A bare literal PostgreSQL leaves `unknown`, so it adopts the type of the
    /// parameter it is passed to. This codebase types both a string literal and
    /// a bare `NULL` as `text` immediately, so the question has to be asked
    /// syntactically.
    Unknown,
    /// An argument carrying a type of its own.
    Known(ColumnType),
    /// An argument that carries a type, but not one visible here — a run-time
    /// SQL NULL. Like [`ArgType::Unknown`] it resolves no polymorphic
    /// parameter, but unlike it, it is never the *reason* one is unresolvable
    /// (`to_jsonb(j)` on a NULL row value is not `to_jsonb('a')`).
    Opaque,
}

impl ArgType {
    /// The type this argument contributes, if any.
    pub(crate) fn known(self) -> Option<ColumnType> {
        match self {
            ArgType::Known(t) => Some(t),
            ArgType::Unknown | ArgType::Opaque => None,
        }
    }

    /// Is this an `unknown` literal awaiting its parameter's type?
    pub(crate) fn is_unknown(self) -> bool {
        self == ArgType::Unknown
    }
}

/// The argument types a family resolves its parameters from at PLAN time.
pub(crate) fn static_arg_types(args: &[Expr], scope: &Scope) -> Result<Vec<ArgType>, ExecError> {
    args.iter()
        .map(|e| {
            if is_unknown_literal(e) {
                Ok(ArgType::Unknown)
            } else {
                infer_type(e, scope).map(ArgType::Known)
            }
        })
        .collect()
}

/// The same, at RUN time: an argument's type comes from its value, and a SQL
/// NULL carries none.
pub(crate) fn value_arg_types(args: &[Expr], vals: &[Datum]) -> Vec<ArgType> {
    args.iter()
        .zip(vals)
        .map(|(e, v)| {
            if is_unknown_literal(e) {
                ArgType::Unknown
            } else {
                v.column_type().map_or(ArgType::Opaque, ArgType::Known)
            }
        })
        .collect()
}

/// Each argument's type once the `unknown` literals have adopted their
/// parameter's type — what a family's plan-time checks and result type are
/// computed from. A parameter a literal cannot adopt (a `"any"` parameter, where
/// PostgreSQL resolves `unknown` to `text`) leaves it `text`.
pub(crate) fn effective_arg_types(
    given: &[ArgType],
    params: &[Option<ColumnType>],
) -> Vec<ColumnType> {
    given
        .iter()
        .enumerate()
        .map(|(i, g)| {
            if g.is_unknown() {
                params.get(i).copied().flatten().unwrap_or(ColumnType::Text)
            } else {
                g.known().unwrap_or(ColumnType::Text)
            }
        })
        .collect()
}

/// Convert each `unknown` literal argument's `text` value to the type its
/// parameter resolved to. Typing the literal is not enough on its own: it still
/// evaluates to a `Datum::Text`, while `jsonb_set`'s path parameter needs a
/// `Datum::Array` and its value parameter a `Datum::Jsonb`. A NULL value has
/// nothing to convert.
pub(crate) fn coerce_unknown_args(
    args: &[Expr],
    vals: &mut [Datum],
    params: &[Option<ColumnType>],
    ctx: &EvalCtx,
) -> Result<(), ExecError> {
    for (i, v) in vals.iter_mut().enumerate() {
        let Some(Some(ty)) = args.get(i).map(|e| {
            is_unknown_literal(e)
                .then(|| params.get(i).copied().flatten())
                .flatten()
        }) else {
            continue;
        };
        if v.is_null() || v.column_type() == Some(ty) {
            continue;
        }
        *v = crabka_pgtypes::cast::cast(v, ty, &ctx.time_zone)?;
    }
    Ok(())
}

/// Is `e` a literal PostgreSQL would still call `unknown`?
fn is_unknown_literal(e: &Expr) -> bool {
    matches!(e, Expr::StringLiteral(_) | Expr::NullLiteral)
}

/// PostgreSQL's 42804 for a polymorphic parameter (`anyarray`, `anyelement`)
/// that nothing in the call resolves, because every argument that could have is
/// an `unknown` literal — `cardinality('{1,2}')`, `to_jsonb('a')`.
pub(crate) fn undetermined_polymorphic_type() -> ExecError {
    ExecError::TypeMismatch(
        "could not determine polymorphic type because input has type unknown".into(),
    )
}

/// The `||` operator for a type pair, and its result type. Arrays are resolved
/// before the text fallback so `text[] || text` appends rather than stringifies —
/// PostgreSQL's `anyarray || anyelement` outranks `anynonarray || text` there.
fn concat_kind(lt: ColumnType, rt: ColumnType) -> Option<(ConcatKind, ColumnType)> {
    if lt == ColumnType::Jsonb && rt == ColumnType::Jsonb {
        return Some((ConcatKind::Jsonb, ColumnType::Jsonb));
    }
    if let (Some(form), Some(ty)) = (
        array_fn::concat_form(lt, rt),
        array_fn::concat_result_type(lt, rt),
    ) {
        return Some((ConcatKind::Array(form), ty));
    }
    // SP29: `||` yields text, and PostgreSQL requires at least one operand to be
    // text (`text || anynonarray` / `anynonarray || text`); neither-text (e.g.
    // `int || int`) is 42883.
    (lt == ColumnType::Text || rt == ColumnType::Text)
        .then_some((ConcatKind::Text, ColumnType::Text))
}

fn apply_concat(kind: ConcatKind, l: &Datum, r: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    match kind {
        ConcatKind::Text => Ok(ops::concat(l, r, &ctx.time_zone)?),
        ConcatKind::Jsonb => json_fn::eval_json_operator(JsonOp::Concat, l, r),
        ConcatKind::Array(form) => array_fn::array_concat(form, l, r, ctx),
    }
}

/// The `JsonOp` a `BinaryOp` spells, for the operators jsonb defines.
fn json_op_of(op: BinaryOp) -> Option<JsonOp> {
    Some(match op {
        BinaryOp::JsonGet => JsonOp::Get,
        BinaryOp::JsonGetText => JsonOp::GetText,
        BinaryOp::JsonGetPath => JsonOp::GetPath,
        BinaryOp::JsonGetPathText => JsonOp::GetPathText,
        BinaryOp::Contains => JsonOp::Contains,
        BinaryOp::ContainedBy => JsonOp::ContainedBy,
        BinaryOp::KeyExists => JsonOp::KeyExists,
        BinaryOp::KeyExistsAny => JsonOp::KeyExistsAny,
        BinaryOp::KeyExistsAll => JsonOp::KeyExistsAll,
        _ => return None,
    })
}

/// A binary operator's SQL spelling, for error messages.
fn op_spelling(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Concat => "||",
        BinaryOp::JsonGet => "->",
        BinaryOp::JsonGetText => "->>",
        BinaryOp::JsonGetPath => "#>",
        BinaryOp::JsonGetPathText => "#>>",
        BinaryOp::Contains => "@>",
        BinaryOp::ContainedBy => "<@",
        BinaryOp::KeyExists => "?",
        BinaryOp::KeyExistsAny => "?|",
        BinaryOp::KeyExistsAll => "?&",
        BinaryOp::Overlaps => "&&",
        BinaryOp::Eq => "=",
        BinaryOp::Ne => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::Le => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Ge => ">=",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
    }
}

fn undefined_operator(op: &str, lt: ColumnType, rt: ColumnType) -> ExecError {
    ExecError::UndefinedFunction(format!(
        "operator does not exist: {} {op} {}",
        lt.name(),
        rt.name()
    ))
}

/// The same 42883, reported from the runtime values (an untyped SQL NULL has no
/// type to name).
fn undefined_operator_for(op: BinaryOp, l: &Datum, r: &Datum) -> ExecError {
    let name = |d: &Datum| d.column_type().map_or("unknown", ColumnType::name);
    ExecError::UndefinedFunction(format!(
        "operator does not exist: {} {} {}",
        name(l),
        op_spelling(op),
        name(r)
    ))
}

pub(crate) fn quantifier_of(all: bool) -> Quantifier {
    if all {
        Quantifier::All
    } else {
        Quantifier::Any
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
        Expr::Binary { op, left, right } => infer_binary_type(*op, left, right, scope),
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
        // The jsonb + array function families' static result types.
        Expr::Func(fc) if json_fn::is_json_func(&fc.name) => {
            json_fn::json_func_result_type(fc, scope)
        }
        Expr::Func(fc) if array_fn::is_array_func(&fc.name) => {
            array_fn::array_func_result_type(fc, scope)
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
            // `ARRAY[]::int[]`: the empty constructor has no element type of its
            // own — the cast supplies it (PostgreSQL pushes the type context down
            // into the constructor), so the operand is not inferred at all.
            if empty_array_cast(expr, *ty).is_some() {
                return Ok(*ty);
            }
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
        // (typeable without executing — used by `describe`). The array form of a
        // quantified comparison (`= ANY(arr)`) is boolean for the same reason.
        Expr::Exists(_)
        | Expr::InSubquery { .. }
        | Expr::Quantified { .. }
        | Expr::QuantifiedArray { .. } => Ok(ColumnType::Bool),
        // `ARRAY[…]` types as an array of its unified element type.
        Expr::ArrayLiteral(items) => Ok(ColumnType::Array(array_literal_elem_type(items, scope)?)),
        // `base[index]` yields the base array's element type; anything else has no
        // subscripting operator.
        Expr::Subscript { base, .. } => {
            let bt = infer_type(base, scope)?;
            bt.array_element()
                .map(ElemType::column_type)
                .ok_or_else(|| {
                    ExecError::TypeMismatch(format!(
                        "cannot subscript type {} because it does not support subscripting",
                        bt.name()
                    ))
                })
        }
        // A scalar subquery's type needs the catalog; both the exec and describe
        // paths substitute it to `Const` before `infer_type` runs, so this is
        // unreachable in practice (defensive).
        Expr::ScalarSubquery(_) => Err(ExecError::Unsupported(
            "internal: scalar subquery must be resolved before type inference".into(),
        )),
    }
}

/// Statically infer a binary expression's result type.
fn infer_binary_type(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
            // jsonb `-` (delete) overloads the arithmetic `-`. Only a jsonb LEFT
            // operand selects it, so every numeric/date pair below is unchanged;
            // a jsonb left operand with an unsupported right operand is 42883
            // rather than falling through to the numeric tower.
            if op == BinaryOp::Sub && lt == ColumnType::Jsonb {
                return json_fn::json_operator_result_type(JsonOp::Delete, lt, rt)
                    .ok_or_else(|| undefined_operator("-", lt, rt));
            }
            // SP37: a temporal operand resolves via PG's date/time arithmetic
            // matrix first; a non-temporal pair falls through to the numeric tower.
            Ok(datetime_result_type(op, lt, rt).unwrap_or_else(|| numeric_result_type(lt, rt)))
        }
        // `||` is text, jsonb, or one of the three array concatenations, resolved
        // from the operand types (42883 when no `||` applies).
        BinaryOp::Concat => Ok(resolve_concat(left, right, scope)?.1),
        // The jsonb/array operators: `->` and `#>` yield jsonb, `->>` and `#>>`
        // yield text, and the containment/existence/overlap tests yield boolean.
        BinaryOp::JsonGet
        | BinaryOp::JsonGetText
        | BinaryOp::JsonGetPath
        | BinaryOp::JsonGetPathText
        | BinaryOp::Contains
        | BinaryOp::ContainedBy
        | BinaryOp::KeyExists
        | BinaryOp::KeyExistsAny
        | BinaryOp::KeyExistsAll
        | BinaryOp::Overlaps => {
            let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
            let (alt, art) = adopt_json_operand_types(op, left, right, lt, rt);
            json_or_array_operator_result_type(op, alt, art)
                .ok_or_else(|| undefined_operator(op_spelling(op), lt, rt))
        }
        _ => Ok(ColumnType::Bool),
    }
}

/// The static result type of a jsonb or array operator, or `None` when the
/// operand types resolve neither (the caller reports 42883 at plan time). The
/// jsonb rules live in `json_fn`; the array rules are `@>` / `<@` / `&&` over two
/// arrays sharing one element type.
fn json_or_array_operator_result_type(
    op: BinaryOp,
    lt: ColumnType,
    rt: ColumnType,
) -> Option<ColumnType> {
    if let Some(json_op) = json_op_of(op)
        && let Some(t) = json_fn::json_operator_result_type(json_op, lt, rt)
    {
        return Some(t);
    }
    if matches!(
        op,
        BinaryOp::Contains | BinaryOp::ContainedBy | BinaryOp::Overlaps
    ) && let (Some(le), Some(re)) = (lt.array_element(), rt.array_element())
        && le == re
    {
        return Some(ColumnType::Bool);
    }
    None
}

/// The element type of an `ARRAY[…]` constructor: its elements unified exactly as
/// `CASE`/`COALESCE` unify their branches, so a bare `NULL` element imposes no
/// constraint and an all-NULL list falls back to `text` (PostgreSQL's `unknown` →
/// `text`). `ARRAY[]` has nothing to unify — 42P18, and a cast supplies the type.
/// An element type crabka has no array type for is 0A000.
pub(crate) fn array_literal_elem_type(
    items: &[Expr],
    scope: &Scope,
) -> Result<ElemType, ExecError> {
    if items.is_empty() {
        return Err(indeterminate_type("cannot determine type of empty array"));
    }
    let mut acc: Option<ColumnType> = None;
    for item in items {
        acc = unify_branch(acc, item, scope)?;
    }
    let elem = acc.unwrap_or(ColumnType::Text);
    ElemType::from_column_type(elem).ok_or_else(|| {
        ExecError::Unsupported(format!("arrays of {} are not supported", elem.name()))
    })
}

/// `ARRAY[]::int[]`: an empty array constructor has no element type of its own,
/// so PostgreSQL pushes the cast's type context down into it. Returns the typed
/// empty array when `expr`/`ty` are exactly that shape.
pub(crate) fn empty_array_cast(expr: &Expr, ty: ColumnType) -> Option<Datum> {
    match (expr, ty.array_element()) {
        (Expr::ArrayLiteral(items), Some(elem)) if items.is_empty() => {
            Some(Datum::Array(ArrayValue::new(elem, Vec::new())))
        }
        _ => None,
    }
}

/// 42P18 (`indeterminate_datatype`).
fn indeterminate_type(message: &str) -> ExecError {
    ExecError::IndeterminateType(message.to_string())
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

    // ---- jsonb + array operators, constructors, and quantified comparisons ----

    /// A relation covering every operand family the new operators resolve over.
    fn jt() -> Table {
        Table {
            id: 2,
            name: "jt".into(),
            columns: vec![
                Column::new("j", ColumnType::Jsonb),
                Column::new("ia", ColumnType::Array(ElemType::Int4)),
                Column::new("ta", ColumnType::Array(ElemType::Text)),
                Column::new("i", ColumnType::Int4),
                Column::new("s", ColumnType::Text),
            ],
            sharded: false,
            sharding: None,
            foreign: None,
        }
    }

    fn jb(text: &str) -> Datum {
        Datum::Jsonb(crabka_pgtypes::jsonb::parse(text).expect("jsonb literal"))
    }

    fn int_array(elems: &[i32]) -> Datum {
        Datum::Array(ArrayValue::new(
            ElemType::Int4,
            elems.iter().copied().map(Datum::Int4).collect(),
        ))
    }

    /// The row bound to [`jt`] for the evaluation tables below.
    fn jt_row() -> Vec<Datum> {
        vec![
            jb(r#"{"a": 1, "b": [10, 20]}"#),
            int_array(&[1, 2, 3]),
            Datum::Array(ArrayValue::new(
                ElemType::Text,
                vec![Datum::Text("a".into()), Datum::Text("x".into())],
            )),
            Datum::Int4(2),
            Datum::Text("a".into()),
        ]
    }

    fn infer_jt(sql: &str) -> Result<ColumnType, ExecError> {
        infer_type(&pexpr(sql).expect("parse"), &scope_of(Some(&jt())))
    }

    fn eval_jt(sql: &str) -> Result<Datum, ExecError> {
        let ctx = crate::clock::EvalCtx::test_default();
        eval(
            &pexpr(sql).expect("parse"),
            &scope_of(Some(&jt())),
            &jt_row(),
            &ctx,
        )
    }

    /// Every new operator's STATIC result type, routed through the `json_fn` /
    /// `array_fn` rules so plan-time typing and evaluation never disagree.
    #[test]
    fn new_operators_infer_their_result_types() {
        let text_array = ColumnType::Array(ElemType::Text);
        let int_array_ty = ColumnType::Array(ElemType::Int4);
        let cases: &[(&str, ColumnType)] = &[
            ("j -> 'a'", ColumnType::Jsonb),
            ("j -> 1", ColumnType::Jsonb),
            ("j ->> 'a'", ColumnType::Text),
            ("j #> ARRAY['b', '0']", ColumnType::Jsonb),
            ("j #>> ARRAY['b', '0']", ColumnType::Text),
            ("j @> j", ColumnType::Bool),
            ("j <@ j", ColumnType::Bool),
            ("j ? 'a'", ColumnType::Bool),
            ("j ?| ARRAY['a']", ColumnType::Bool),
            ("j ?& ARRAY['a']", ColumnType::Bool),
            ("ia @> ia", ColumnType::Bool),
            ("ia <@ ia", ColumnType::Bool),
            ("ia && ia", ColumnType::Bool),
            ("ta && ta", ColumnType::Bool),
            // `||` and `-` are overloads of the existing operators.
            ("j || j", ColumnType::Jsonb),
            ("j - 'a'", ColumnType::Jsonb),
            ("j - 1", ColumnType::Jsonb),
            ("j - ARRAY['a']", ColumnType::Jsonb),
            ("ia || ia", int_array_ty),
            ("ia || i", int_array_ty),
            ("i || ia", int_array_ty),
            ("ta || s", text_array),
            ("s || ta", text_array),
            // The existing text/numeric behavior is untouched.
            ("s || i", ColumnType::Text),
            ("i - i", ColumnType::Int4),
            // The array expression forms.
            ("ARRAY[1, 2]", int_array_ty),
            ("ia[1]", ColumnType::Int4),
            ("ta[1]", ColumnType::Text),
            ("i = ANY(ia)", ColumnType::Bool),
            ("i > ALL(ia)", ColumnType::Bool),
        ];
        for (sql, want) in cases {
            assert2::assert!(infer_jt(sql).expect("infer") == *want, "for {sql}");
        }
    }

    /// Operand combinations no operator resolves are 42883 at PLAN time.
    #[test]
    fn unresolvable_operator_operands_are_42883() {
        for sql in [
            "i -> 'a'",      // `->` needs a jsonb left operand
            "j -> true",     // no jsonb subscript for boolean
            "j @> ia",       // jsonb against an array
            "ia && ta",      // mismatched element types
            "j && j",        // `&&` is array-only
            "i && i",        // neither side is an array
            "j || i",        // jsonb concatenates only with jsonb (or text)
            "i || i",        // the pre-existing neither-text rule still holds
            "j - true",      // no jsonb delete for boolean
            "ia || ta",      // mismatched element types
            "j ?| ARRAY[1]", // `?|` needs text[]
        ] {
            let err = infer_jt(sql).expect_err("must not resolve");
            assert2::assert!(err.into_pg().code == "42883", "for {sql}");
        }
    }

    /// Every new operator's VALUE, including the jsonb-null / SQL-NULL and
    /// three-valued cells that distinguish them.
    #[test]
    fn new_operators_evaluate() {
        let cases: &[(&str, Datum)] = &[
            ("j -> 'a'", jb("1")),
            ("j -> 'zz'", Datum::Null),
            ("j ->> 'a'", Datum::Text("1".into())),
            ("j -> 'b' -> 0", jb("10")),
            ("j #> ARRAY['b', '1']", jb("20")),
            ("j #>> ARRAY['b', '1']", Datum::Text("20".into())),
            (r#"j @> '{"a": 1}'::jsonb"#, Datum::Bool(true)),
            (r#"j @> '{"a": 2}'::jsonb"#, Datum::Bool(false)),
            (r#"'{"a": 1}'::jsonb <@ j"#, Datum::Bool(true)),
            ("j ? 'a'", Datum::Bool(true)),
            ("j ? 'zz'", Datum::Bool(false)),
            ("j ?| ARRAY['zz', 'a']", Datum::Bool(true)),
            ("j ?& ARRAY['a', 'zz']", Datum::Bool(false)),
            ("ia @> ARRAY[1, 2]", Datum::Bool(true)),
            ("ia @> ARRAY[9]", Datum::Bool(false)),
            ("ARRAY[1] <@ ia", Datum::Bool(true)),
            ("ia && ARRAY[9, 3]", Datum::Bool(true)),
            ("ia && ARRAY[9]", Datum::Bool(false)),
            // Every jsonb operator is strict.
            ("null::jsonb -> 'a'", Datum::Null),
            ("j -> null", Datum::Null),
            ("null::jsonb @> j", Datum::Null),
            ("null::int[] && ia", Datum::Null),
            ("null::int[] && null::int[]", Datum::Null),
        ];
        for (sql, want) in cases {
            assert2::assert!(eval_jt(sql).expect("eval") == *want, "for {sql}");
        }
    }

    /// `||` resolves to five different operators; the choice is made from the
    /// operands' STATIC types, which is the only way `ARRAY[1,2] || NULL` (a
    /// concatenation, `{1,2}`) can differ from `ARRAY[1,2] || NULL::int` (an
    /// append, `{1,2,NULL}`) once both right sides have evaluated to SQL NULL.
    #[test]
    fn concat_resolves_text_jsonb_and_the_three_array_forms() {
        let with_null = Datum::Array(ArrayValue::new(
            ElemType::Int4,
            vec![Datum::Int4(1), Datum::Int4(2), Datum::Null],
        ));
        let cases: &[(&str, Datum)] = &[
            ("s || i", Datum::Text("a2".into())),
            (
                r#"j || '{"c": 3}'::jsonb"#,
                jb(r#"{"a": 1, "b": [10, 20], "c": 3}"#),
            ),
            ("ARRAY[1, 2] || ARRAY[3]", int_array(&[1, 2, 3])),
            ("ARRAY[1, 2] || 3", int_array(&[1, 2, 3])),
            ("1 || ARRAY[2, 3]", int_array(&[1, 2, 3])),
            // A bare NULL literal resolves to `array_cat`, as PostgreSQL's
            // `unknown` resolution does — NOT to `array_append`.
            ("ARRAY[1, 2] || null", int_array(&[1, 2])),
            ("null || ARRAY[1, 2]", int_array(&[1, 2])),
            // A NULL typed as the ELEMENT still appends.
            ("ARRAY[1, 2] || null::int4", with_null),
            // A typed NULL array is still a concatenation of an unknown array.
            ("ARRAY[1, 2] || null::int[]", int_array(&[1, 2])),
            // jsonb `||` is strict.
            ("j || null::jsonb", Datum::Null),
        ];
        for (sql, want) in cases {
            assert2::assert!(eval_jt(sql).expect("eval") == *want, "for {sql}");
        }
        // The NULL-literal adoption also shows up in the static type.
        assert2::assert!(
            infer_jt("ARRAY[1, 2] || null").expect("infer") == ColumnType::Array(ElemType::Int4)
        );
        assert2::assert!(infer_jt("j || null").expect("infer") == ColumnType::Jsonb);
    }

    /// jsonb `-` shares the `Sub` operator with arithmetic; the LEFT operand's
    /// type disambiguates, and every numeric/temporal pair is untouched.
    #[test]
    fn jsonb_delete_overloads_subtraction() {
        let cases: &[(&str, Datum)] = &[
            ("j - 'a'", jb(r#"{"b": [10, 20]}"#)),
            ("j - 'zz'", jb(r#"{"a": 1, "b": [10, 20]}"#)),
            ("j - ARRAY['a', 'b']", jb("{}")),
            (r"'[1, 2, 3]'::jsonb - 1", jb("[1, 3]")),
            ("null::jsonb - 'a'", Datum::Null),
            // Arithmetic subtraction is unchanged.
            ("i - 1", Datum::Int4(1)),
        ];
        for (sql, want) in cases {
            assert2::assert!(eval_jt(sql).expect("eval") == *want, "for {sql}");
        }
    }

    /// `ARRAY[…]` unifies its elements exactly as `CASE` does; `ARRAY[]` has
    /// nothing to unify and needs a cast (42P18); an all-NULL list falls back to
    /// `text`, matching PostgreSQL's `unknown` → `text`.
    #[test]
    fn array_literal_typing_and_construction() {
        let text_array = ColumnType::Array(ElemType::Text);
        assert2::assert!(eval_jt("ARRAY[1, 2, 3]").expect("eval") == int_array(&[1, 2, 3]));
        // int4 + int8 unify to int8, and the int4 element is coerced.
        assert2::assert!(
            infer_jt("ARRAY[1, 2147483648]").expect("infer") == ColumnType::Array(ElemType::Int8)
        );
        assert2::assert!(
            eval_jt("ARRAY[1, 2147483648]").expect("eval")
                == Datum::Array(ArrayValue::new(
                    ElemType::Int8,
                    vec![Datum::Int8(1), Datum::Int8(2_147_483_648)]
                ))
        );
        // A NULL element is type-neutral but is kept as an array element.
        assert2::assert!(
            eval_jt("ARRAY[1, null]").expect("eval")
                == Datum::Array(ArrayValue::new(
                    ElemType::Int4,
                    vec![Datum::Int4(1), Datum::Null]
                ))
        );
        // An all-NULL list is text[].
        assert2::assert!(infer_jt("ARRAY[null]").expect("infer") == text_array);
        assert2::assert!(
            eval_jt("ARRAY[null]").expect("eval")
                == Datum::Array(ArrayValue::new(ElemType::Text, vec![Datum::Null]))
        );
        // `ARRAY[]` alone cannot be typed; the cast supplies the element type.
        assert2::assert!(infer_jt("ARRAY[]").expect_err("empty array").into_pg().code == "42P18");
        assert2::assert!(eval_jt("ARRAY[]").expect_err("empty array").into_pg().code == "42P18");
        assert2::assert!(
            infer_jt("ARRAY[]::int[]").expect("infer") == ColumnType::Array(ElemType::Int4)
        );
        assert2::assert!(
            eval_jt("ARRAY[]::int[]").expect("eval")
                == Datum::Array(ArrayValue::new(ElemType::Int4, Vec::new()))
        );
        // Incompatible elements are the same 42804 `CASE` reports.
        assert2::assert!(
            infer_jt("ARRAY[1, 'x']")
                .expect_err("incompatible")
                .into_pg()
                .code
                == "42804"
        );
    }

    /// Subscripting is 1-based, and out-of-range / NULL is SQL NULL rather than
    /// an error. A non-array base has no subscripting operator at all.
    #[test]
    fn array_subscripting() {
        let cases: &[(&str, Datum)] = &[
            ("ia[1]", Datum::Int4(1)),
            ("ia[3]", Datum::Int4(3)),
            ("ia[0]", Datum::Null),
            ("ia[4]", Datum::Null),
            ("ia[null]", Datum::Null),
            ("ta[2]", Datum::Text("x".into())),
            ("(null::int[])[1]", Datum::Null),
            ("ia[i]", Datum::Int4(2)),
        ];
        for (sql, want) in cases {
            assert2::assert!(eval_jt(sql).expect("eval") == *want, "for {sql}");
        }
        let err = infer_jt("i[1]").expect_err("int is not subscriptable");
        assert2::assert!(err.into_pg().code == "42804");
    }

    /// `ANY`/`ALL` over an array, including the three-valued cells: an unmatched
    /// `ANY` over an array containing NULL is UNKNOWN, not false; an empty array
    /// is false for `ANY` and true for `ALL`; a NULL array is NULL for both.
    #[test]
    fn quantified_array_three_valued_logic() {
        let cases: &[(&str, Datum)] = &[
            ("2 = ANY(ARRAY[1, 2, 3])", Datum::Bool(true)),
            ("9 = ANY(ARRAY[1, 2, 3])", Datum::Bool(false)),
            ("2 = SOME(ARRAY[1, 2, 3])", Datum::Bool(true)),
            // A match short-circuits past the NULL element.
            ("2 = ANY(ARRAY[2, null])", Datum::Bool(true)),
            // No match with a NULL element present is UNKNOWN.
            ("9 = ANY(ARRAY[1, null])", Datum::Null),
            ("null = ANY(ARRAY[1, 2])", Datum::Null),
            ("1 = ANY(null::int[])", Datum::Null),
            ("1 = ANY(ARRAY[]::int[])", Datum::Bool(false)),
            ("9 > ALL(ARRAY[1, 2, 3])", Datum::Bool(true)),
            ("2 > ALL(ARRAY[1, 2, 3])", Datum::Bool(false)),
            // A mismatch short-circuits past the NULL element.
            ("1 > ALL(ARRAY[2, null])", Datum::Bool(false)),
            ("9 > ALL(ARRAY[1, null])", Datum::Null),
            ("1 > ALL(null::int[])", Datum::Null),
            ("1 > ALL(ARRAY[]::int[])", Datum::Bool(true)),
            // The array may be a column, and the operator any comparison.
            ("i = ANY(ia)", Datum::Bool(true)),
            ("s = ANY(ta)", Datum::Bool(true)),
        ];
        for (sql, want) in cases {
            assert2::assert!(eval_jt(sql).expect("eval") == *want, "for {sql}");
        }
    }

    /// The jsonb + array FUNCTION families are reachable from scalar `eval` and
    /// from static inference (the guard chains wire them in beside the older
    /// scalar/datetime/format families).
    #[test]
    fn json_and_array_function_families_are_wired_into_eval() {
        assert2::assert!(eval_jt("jsonb_typeof(j)").expect("eval") == Datum::Text("object".into()));
        assert2::assert!(infer_jt("jsonb_typeof(j)").expect("infer") == ColumnType::Text);
        assert2::assert!(eval_jt("jsonb_build_object('k', i)").expect("eval") == jb(r#"{"k": 2}"#));
        assert2::assert!(
            infer_jt("jsonb_build_object('k', i)").expect("infer") == ColumnType::Jsonb
        );
        assert2::assert!(eval_jt("cardinality(ia)").expect("eval") == Datum::Int4(3));
        assert2::assert!(infer_jt("cardinality(ia)").expect("infer") == ColumnType::Int4);
        assert2::assert!(eval_jt("array_append(ia, 4)").expect("eval") == int_array(&[1, 2, 3, 4]));
        assert2::assert!(
            infer_jt("array_append(ia, 4)").expect("infer") == ColumnType::Array(ElemType::Int4)
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
