//! Expression evaluation over Datums, plus static result-type inference.
//!
//! Static result-type inference builds a stable RowDescription before any row
//! is produced.

use std::cmp::Ordering;

use crabka_pgparser::ast::{BinaryOp, Expr, MatchKind, UnaryOp};
use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType, TypeError, ops};

use crate::{
    array_fn::{self, ConcatForm, Quantifier},
    clock::EvalCtx,
    error::ExecError,
    json_fn::{self, JsonOp},
    rowexpr,
    scope::Scope,
};

/// The maximum expression-tree depth `eval` will recurse before it returns
/// `54001` (statement_too_complex).
///
/// This limit is DEFENSE-IN-DEPTH. The parser already caps the AST depth at
/// `crabka_pgparser::parser::MAX_DEPTH` (50) at parse time, so a tree deeper
/// than 50 can never reach here in practice. `150` leaves 3x headroom above
/// that cap, so the guard never wrongly rejects a parser-admitted tree. The
/// value also stays well below the depth at which `eval` itself would overflow.
/// A hypothetical over-deep tree must return a clean error and must not abort
/// the process.
///
/// It is deliberately **2×** the parser's `MAX_DEPTH` of 50, not more. The
/// parser caps AST depth at parse time, so no tree the parser produces can
/// exceed 50, and this guard only ever fires on a hand-built tree. The multiple
/// is headroom, not capacity. It was 3× (150), and that slack caused an
/// aborting stack overflow three separate times as waves widened `Datum` and
/// `ExecError`. The recursive frame carries both, so this constant multiplies
/// every byte added to them. Raise it only together with a measurement of the
/// frame size on the smallest stack the tests run on.
const MAX_EVAL_DEPTH: usize = 100;

/// Parser-produced trees stop at depth 50. A deeper tree can only have been
/// built programmatically, so finish that defense-in-depth evaluation on a
/// stack large enough to reach [`MAX_EVAL_DEPTH`] even in an unoptimized test
/// build (where this function's frame is much larger than in production).
const EVAL_STACK_SWITCH_DEPTH: usize = 51;
const DEEP_EVAL_STACK_BYTES: usize = 8 * 1024 * 1024;

/// The relation a failed column reference may still name as a *whole row*.
///
/// `PostgreSQL` tries the range table only after the column search comes up
/// empty, and only for an unqualified name — `s.t` is read as "column `t` of
/// range-table entry `s`" and reports a missing FROM entry for `s`, never as the
/// whole row of `s.t`. An ambiguous name (42702) is already an error there too,
/// so only 42703 opens this door.
fn whole_row_reference<'a>(
    qualifier: Option<&str>,
    name: &'a str,
    error: &ExecError,
) -> Option<&'a str> {
    (qualifier.is_none() && matches!(error, ExecError::UndefinedColumn(_))).then_some(name)
}

/// Evaluate `expr` against a row (`values`, aligned to `scope.columns`). `ctx`
/// carries the session time zone and the transaction/statement clock; non-temporal
/// evaluation ignores it (UTC/epoch reproduces prior behavior).
pub(crate) fn eval(
    expr: &Expr,
    scope: &Scope,
    values: &[Datum],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    crate::session::check_query_canceled()?;
    eval_depth(expr, scope, values, ctx, 0)
}

/// Executor-level cast, adding the `jsonpath` input function to the pure type
/// layer's Datum-preserving cast table.
///
/// Every style but the zone is left at its default, so a `text → date`/
/// `timestamp` arm reads an ambiguous all-numeric literal as `MDY` whatever the
/// session says. Prefer [`cast_value_in`] wherever the context is at hand.
pub(crate) fn cast_value(
    value: &Datum,
    target: ColumnType,
    time_zone: &jiff::tz::TimeZone,
) -> Result<Datum, ExecError> {
    cast_value_in(
        value,
        target,
        crabka_pgtypes::encoding::OutputStyle::with_zone(time_zone),
    )
}

/// Apply `PostgreSQL`'s `bpchar → text` cast to a value whose *static* type is
/// `character(n)`.
///
/// The value layer cannot do this on its own. `character`, `character varying`
/// and `text` all reach the row as a bare [`Datum::Text`], so nothing about the
/// padded string says the padding is a `character(n)` artifact rather than data
/// a `text` column really holds. Only the static type knows, and only the
/// executor holds the static type — so the cast that
/// [`crabka_pgtypes::string::bpchar_to_text`] describes is applied here, at each
/// point a `character` expression enters a `text` context.
///
/// Both guards are load-bearing. The trailing-space test settles the ordinary
/// case without a type inference at all, which matters because this sits inside
/// the per-row expression loop; and the inference itself is what keeps a `text`
/// column that genuinely ends in a space (`'a '::text = 'a '`) intact.
/// `Ok(None)` means the value is not a padded `character`, so the caller keeps
/// the one it already has rather than paying for a copy.
pub(crate) fn bpchar_to_text_value(
    expr: &Expr,
    scope: &Scope,
    value: &Datum,
) -> Result<Option<Datum>, ExecError> {
    let Datum::Text(text) = value else {
        return Ok(None);
    };
    if !text.ends_with(' ') || !matches!(infer_type(expr, scope)?, ColumnType::Char(_)) {
        return Ok(None);
    }
    Ok(Some(Datum::Text(
        crabka_pgtypes::string::bpchar_to_text(text).to_owned(),
    )))
}

/// [`bpchar_to_text_value`] over an operand pair, for the comparison and
/// concatenation operators — the families with no `bpchar` overload of their
/// own, so `PostgreSQL` resolves them through the implicit cast to `text`.
///
/// `LIKE` and the regular-expression operators are deliberately absent:
/// `bpcharlike` and `bpcharregexeq` read the padded datum, so `'a'::char(3) LIKE
/// 'a'` is false on `PostgreSQL` exactly as it is here.
///
/// Each side is judged separately, because `'x'::char(8) = 'x '::text` is false
/// on `PostgreSQL` — only the `character` side is trimmed.
fn bpchar_to_text_operands(
    op: BinaryOp,
    left: (&Expr, &Datum),
    right: (&Expr, &Datum),
    scope: &Scope,
) -> Result<(Option<Datum>, Option<Datum>), ExecError> {
    if !matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::Gt
            | BinaryOp::Ge
            | BinaryOp::Concat
    ) {
        return Ok((None, None));
    }
    Ok((
        bpchar_to_text_value(left.0, scope, left.1)?,
        bpchar_to_text_value(right.0, scope, right.1)?,
    ))
}

/// [`cast_value`] in the session's styles, so an ambiguous all-numeric date
/// literal is read under the session's `DateStyle` field order.
pub(crate) fn cast_value_in(
    value: &Datum,
    target: ColumnType,
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> Result<Datum, ExecError> {
    match target.storage_type() {
        ColumnType::JsonPath => crate::jsonpath::cast_datum(value),
        ColumnType::Array(ElemType::JsonPath) => crate::jsonpath::cast_array_datum(value),
        _ => crabka_pgtypes::cast::cast_in(value, target, style).map_err(ExecError::from),
    }
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
    if depth == EVAL_STACK_SWITCH_DEPTH {
        return std::thread::scope(|thread_scope| {
            let handle = std::thread::Builder::new()
                .name("crabka-deep-expression".into())
                .stack_size(DEEP_EVAL_STACK_BYTES)
                .spawn_scoped(thread_scope, || {
                    eval_depth_inner(expr, scope, values, ctx, depth)
                })
                .map_err(|_| ExecError::StackDepthExceeded)?;
            match handle.join() {
                Ok(result) => result,
                Err(payload) => std::panic::resume_unwind(payload),
            }
        });
    }
    eval_depth_inner(expr, scope, values, ctx, depth)
}

fn eval_depth_inner(
    expr: &Expr,
    scope: &Scope,
    values: &[Datum],
    ctx: &EvalCtx,
    depth: usize,
) -> Result<Datum, ExecError> {
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
        // `B'…'` / `X'…'` — already decoded to binary digits by the parser,
        // which also ran `bit_in`, so the value cannot fail here.
        Expr::BitStringLiteral(bits) => Ok(Datum::BitString(
            crabka_pgtypes::BitString::parse(bits, false)
                .expect("the parser validated the bit-string literal"),
        )),
        Expr::BoolLiteral(b) => Ok(Datum::Bool(*b)),
        Expr::NullLiteral => Ok(Datum::Null),
        // A parameter that reached evaluation was never bound. The simple
        // protocol supplies none, so PostgreSQL reports the placeholder as
        // undefined rather than as an unimplemented feature -- 42P02, the same
        // code and wording a view body already raises.
        Expr::Param(number) => Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
            "42P02",
            format!("there is no parameter ${number}"),
        ))),
        Expr::Default => Err(ExecError::Syntax(
            "DEFAULT is not allowed in this context".into(),
        )),
        // A collation derivation never changes the value: every collation this
        // engine has orders text by byte value. What survives is PostgreSQL's
        // type rule — `COLLATE` on a non-collatable operand is 42804.
        Expr::Collate { expr, .. } => {
            let value = eval_depth(expr, scope, values, ctx, d)?;
            if let Some(ty) = value.column_type() {
                require_collatable(ty)?;
            }
            Ok(value)
        }
        Expr::Column { table, name } => {
            if table.is_none()
                && let Some(call) = crate::func::niladic_keyword_call(name)
            {
                return crate::func::eval_scalar(&call, Some(scope), ctx, |e| {
                    eval_depth(e, scope, values, ctx, d)
                });
            }
            match scope.resolve(table.as_deref(), name) {
                Ok(idx) => Ok(values[idx].clone()),
                // A bare name that is no column may still name a relation in the
                // FROM clause, and then it is that relation's whole row.
                Err(error) => whole_row_reference(table.as_deref(), name, &error)
                    .and_then(|q| scope.refs_value(q, values))
                    .ok_or(error),
            }
        }
        Expr::Unary { op, expr } => {
            let v = eval_depth(expr, scope, values, ctx, d)?;
            apply_unary(*op, &v, ctx)
        }
        Expr::Binary { op, left, right } => {
            // A comparison of two row constructors is evaluated field by field
            // and never reaches the scalar operator path.
            if let Some(result) =
                rowexpr::eval_binary(*op, left, right, |e| eval_depth(e, scope, values, ctx, d))?
            {
                return Ok(result);
            }
            let l = eval_depth(left, scope, values, ctx, d)?;
            // The boolean connectives stop once the left operand settles the
            // answer: the three-valued tables give `false AND anything` = false
            // and `true OR anything` = true, so the right operand cannot change
            // it. Evaluating it regardless is not merely wasted work — a
            // conjunct like `WHERE k < 3 AND EXISTS (…)` runs the correlated
            // subquery for every row rather than the three that pass, which is
            // how one upstream test came to take 892 seconds.
            match (op, &l) {
                (BinaryOp::And, Datum::Bool(false)) => return Ok(Datum::Bool(false)),
                (BinaryOp::Or, Datum::Bool(true)) => return Ok(Datum::Bool(true)),
                _ => {}
            }
            let r = eval_depth(right, scope, values, ctx, d)?;
            apply_binary_of(*op, left, right, &l, &r, scope, ctx)
        }
        // A function call reached scalar `eval`: a SP29 scalar function evaluates
        // here (its arguments recurse through this same `eval`). Otherwise it is
        // NOT in a valid aggregate position (the aggregate path resolves
        // aggregates from accumulators) — a known aggregate here is misplaced /
        // nested (42803); any other name is undefined (42883).
        // F-2: the pg_catalog introspection family (`pg_get_viewdef`,
        // `pg_get_indexdef`, `obj_description`, the `has_*_privilege` family,
        // …), tried alongside the other post-SP29 families.
        Expr::Func(fc)
            if let Some(result) = crate::routine::eval_plpgsql_scalar(fc, scope, values, ctx) =>
        {
            result
        }
        Expr::Func(fc) if crate::catalog_fn::is_catalog_func(&fc.name) => {
            crate::catalog_fn::eval_catalog(fc, ctx, |e| eval_depth(e, scope, values, ctx, d))
        }
        // The object-identifier family: `to_regclass(…)` and the type-as-function
        // spelling `regclass(…)`, both of which resolve against the catalog.
        Expr::Func(fc) if crate::reg_fn::is_reg_func(&fc.name) => {
            crate::reg_fn::eval_reg_func(fc, ctx, |e| eval_depth(e, scope, values, ctx, d))
        }
        Expr::Func(fc) if crate::func::is_scalar(&fc.name) => {
            crate::func::eval_scalar(fc, Some(scope), ctx, |e| {
                eval_depth(e, scope, values, ctx, d)
            })
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
        // `json_populate_record` and its `jsonb_` twin, ahead of the general
        // JSON arm because they are the one family whose result *shape* comes
        // from an argument's declared type rather than from its value —
        // `NULL::jpop` is a bare `Datum::Null` by the time the general arm sees
        // it, and the composite it was cast to is only in the scope.
        Expr::Func(fc) if json_fn::is_record_func(&fc.name) => {
            json_fn::eval_record_func(fc, scope, ctx, |e| eval_depth(e, scope, values, ctx, d))
        }
        // The jsonb + array function families, tried after the older families and
        // before the aggregate-context error, exactly like the arms above.
        Expr::Func(fc) if json_fn::is_json_func(&fc.name) => {
            json_fn::eval_json(fc, ctx, |e| eval_depth(e, scope, values, ctx, d))
        }
        // The SQL/JSON standard expression forms.
        Expr::SqlJson(json) => {
            json_fn::eval_sql_json(json, ctx, |e| eval_depth(e, scope, values, ctx, d))
        }
        Expr::Func(fc) if array_fn::is_array_func(&fc.name) => {
            array_fn::eval_array(fc, ctx, |e| eval_depth(e, scope, values, ctx, d))
        }
        Expr::Func(fc) if crate::window::is_window_only_function(&fc.name) => {
            Err(crate::window::requires_over_clause(&fc.name))
        }
        Expr::Func(fc) => Err(crate::agg::func_in_scalar_context_error(fc)),
        // SP28: predicate + conditional expressions. The pure-Datum combinators
        // (`eval_in_list`/`eval_between`/`eval_like`/`eval_case`) are shared with
        // the grouped evaluator (`agg::eval_grouped`); only the child-evaluation
        // closure differs.
        Expr::IsNull { expr, negated } => {
            if let Some(result) =
                rowexpr::eval_is_null(expr, *negated, |e| eval_depth(e, scope, values, ctx, d))?
            {
                return Ok(result);
            }
            let v = eval_depth(expr, scope, values, ctx, d)?;
            // Every composite operand is tested field by field, not just a
            // whole-row reference: `PostgreSQL` sets `argisrow` from the
            // operand's type, so a column of a composite type obeys the same
            // rule. Reading the shape off the value settles it without the two
            // scope scans naming the operand used to cost per row.
            if let Some(result) = rowexpr::composite_is_null(&v, *negated) {
                return Ok(result);
            }
            Ok(Datum::Bool(v.is_null() ^ *negated))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            reject_uncomparable_in_list(expr, list, scope)?;
            if let Some(result) = rowexpr::eval_in_list(expr, list, *negated, |e| {
                eval_depth(e, scope, values, ctx, d)
            })? {
                return Ok(result);
            }
            // `x IN (a, b)` is `x = a OR x = b`, so every operand takes the same
            // `character → text` cast the `=` operator takes.
            let x = eval_depth(expr, scope, values, ctx, d)?;
            let x = bpchar_to_text_value(expr, scope, &x)?.unwrap_or(x);
            eval_in_list(expr, &x, list, *negated, ctx, |e| {
                let item = eval_depth(e, scope, values, ctx, d)?;
                Ok(bpchar_to_text_value(e, scope, &item)?.unwrap_or(item))
            })
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            reject_uncomparable_comparison(BinaryOp::Ge, expr, low, scope)?;
            reject_uncomparable_comparison(BinaryOp::Le, expr, high, scope)?;
            // `x BETWEEN lo AND hi` expands to two comparisons, which take the
            // `character → text` cast as `>=` and `<=` do.
            let x = eval_depth(expr, scope, values, ctx, d)?;
            let x = bpchar_to_text_value(expr, scope, &x)?.unwrap_or(x);
            let lo = eval_depth(low, scope, values, ctx, d)?;
            let lo = bpchar_to_text_value(low, scope, &lo)?.unwrap_or(lo);
            let hi = eval_depth(high, scope, values, ctx, d)?;
            let hi = bpchar_to_text_value(high, scope, &hi)?.unwrap_or(hi);
            eval_between((expr, &x), (low, &lo), (high, &hi), *negated, ctx)
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            kind,
            escape,
        } => {
            let s = eval_depth(expr, scope, values, ctx, d)?;
            let p = eval_depth(pattern, scope, values, ctx, d)?;
            let e = escape
                .as_deref()
                .map(|e| eval_depth(e, scope, values, ctx, d))
                .transpose()?;
            eval_like(&s, &p, *negated, *kind, e.as_ref())
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            reject_uncomparable_simple_case(operand.as_deref(), whens, scope)?;
            eval_case(
                operand.as_deref(),
                whens,
                else_result.as_deref(),
                infer_case_type(whens, else_result.as_deref(), scope)?,
                ctx,
                |e| eval_depth(e, scope, values, ctx, d),
            )
        }
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
            // `character → text`/`varchar` (and `name`, which shares `Text`
            // here) is the written spelling of the one cast function every
            // implicit coercion also goes through, so it drops the blank padding
            // here too: `'x'::char(8)::text` is `'x'`. A cast BACK to
            // `character` is not this cast — `bpchar(bpchar)` re-pads to the new
            // length — so those targets are excluded.
            let v = match ty {
                ColumnType::Text | ColumnType::Varchar(_) => {
                    bpchar_to_text_value(expr, scope, &v)?.unwrap_or(v)
                }
                _ => v,
            };
            // The `reg*` family is the one set of casts that needs the catalog:
            // a name has to be resolved to its oid (PostgreSQL's `reg*in`), and
            // an oid has to be resolved *back* to the name `reg*out` prints,
            // because the value layer that renders it has no catalog handle.
            // This is the point where one is in scope, so the name is attached
            // here and travels with the value — and it is also where the
            // session's search path is in scope, which is what decides the
            // schema a bare name lands in. Without a catalog — a planning
            // context or a unit test — the pure cast below yields the bare-oid
            // rendering.
            if let Some(kind) = crate::reg_fn::RegKind::of(*ty)
                && let Some(resolved) = crate::reg_fn::reg_cast(kind, &v, ctx)?
            {
                return Ok(resolved);
            }
            if *ty == crabka_pgtypes::ColumnType::Array(crabka_pgtypes::ElemType::Regtype)
                && let Datum::OidVector(arguments) = &v
            {
                let elems = arguments
                    .elems
                    .iter()
                    .map(|argument| {
                        crate::reg_fn::reg_cast(crate::reg_fn::RegKind::Type, argument, ctx)?
                            .ok_or_else(|| {
                                ExecError::TypeMismatch("cannot cast oid to regtype".into())
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(Datum::Array(crabka_pgtypes::ArrayValue::with_dims(
                    crabka_pgtypes::ElemType::Regtype,
                    elems,
                    arguments.dims.clone(),
                )));
            }
            let cast = if matches!(
                ty.storage_type(),
                ColumnType::JsonPath | ColumnType::Array(ElemType::JsonPath)
            ) {
                cast_value(&v, *ty, &ctx.time_zone)?
            } else {
                crabka_pgtypes::cast::cast_in(&v, *ty, ctx.output_style())?
            };
            // A cast to a domain converts through the base type and then has to
            // satisfy the domain's own NOT NULL and CHECK constraints.
            crate::usertype::check_domain(*ty, &cast, ctx)?;
            Ok(cast)
        }
        // `ARRAY[e1, e2, …]`: every element is coerced to the constructor's
        // unified element type, so the built array is homogeneous.
        Expr::ArrayLiteral(items) => eval_array_constructor(items, scope, values, ctx, d),
        // `ARRAY(subquery)` is resolved to a `Const` by the read pre-pass, so
        // reaching here means the pre-pass did not run.
        Expr::ArraySubquery(_) => Err(ExecError::Unsupported(
            "ARRAY(subquery) is only supported in a query context".into(),
        )),
        // A row constructor reaching an ordinary value position renders to
        // PostgreSQL's composite text form; the row-WISE operations were already
        // taken by the arms above, before their fields were flattened.
        Expr::Row(items) => rowexpr::eval_row(items, |e| eval_depth(e, scope, values, ctx, d)),
        // `(composite).field` — the attribute's value, or NULL when the whole
        // composite is NULL (PostgreSQL's field selection over a NULL row).
        Expr::FieldSelect { base, field } => {
            let value = eval_depth(base, scope, values, ctx, d)?;
            select_field(&value, field)
        }
        Expr::FieldSelectAll(_) => Err(ExecError::Unsupported(
            "(row).* is only supported in a SELECT output list".into(),
        )),
        // `base[index]`: 1-based over an array, and out-of-range / NULL is SQL
        // NULL (not an error). A jsonb base subscripts by key or by 0-based
        // index instead — `json_fn::jsonb_subscript` owns that rule.
        Expr::Subscript { base, index } => {
            let b = eval_depth(base, scope, values, ctx, d)?;
            let i = eval_depth(index, scope, values, ctx, d)?;
            match subscript_kind(&b, base, scope)? {
                SubscriptKind::Json => Err(cannot_subscript(ColumnType::Json)),
                SubscriptKind::Jsonb => json_fn::jsonb_subscript(&b, &i),
                SubscriptKind::Geometric => geometric_subscript(&b, &i),
                SubscriptKind::Array => array_fn::array_subscript(&b, &i),
            }
        }
        // `base[s1][s2]…` — a multi-subscript or sliced reference, which
        // PostgreSQL resolves as ONE array reference rather than a chain.
        Expr::ArrayRef { base, subscripts } => {
            eval_array_ref(base, subscripts, scope, values, ctx, d)
        }
        // `x <op> ANY|ALL (array)` — the array form of a quantified comparison,
        // with three-valued logic supplied by `array_fn::eval_quantified`.
        Expr::QuantifiedArray {
            expr,
            op,
            all,
            array,
        } => {
            reject_uncomparable_quantified(*op, expr, array, scope)?;
            let x = eval_depth(expr, scope, values, ctx, d)?;
            let a = eval_depth(array, scope, values, ctx, d)?;
            // `33 = ANY('{1,2,3}')`: a bare literal on the array side is
            // `unknown`, and PostgreSQL resolves it to the array type over the
            // LEFT operand's type.
            let a = match (&a, matches!(array.as_ref(), Expr::StringLiteral(_))) {
                (Datum::Text(_), true) => {
                    let elem = x.column_type().unwrap_or(ColumnType::Text);
                    let target = ColumnType::array_of(elem).ok_or_else(|| {
                        ExecError::Unsupported(format!(
                            "arrays of {} are not supported",
                            elem.name()
                        ))
                    })?;
                    // `array_in` runs the element type's input function, so
                    // the session's `DateStyle` order decides how an ambiguous
                    // all-numeric element is read.
                    crabka_pgtypes::cast::cast_in(&a, target, ctx.output_style())?
                }
                _ => a,
            };
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

/// `x IN (list)` / `x NOT IN (list)` with three-valued NULL logic.
///
/// `eval_child` evaluates each list element. The truth table for `IN` is:
///
/// - An empty list is false for every `x`, NULL included.
/// - A NULL left-hand side against a non-empty list is NULL.
/// - An element that compares Equal makes the result true.
/// - Otherwise the result is NULL if any element was NULL, and false if not.
///
/// `NOT IN` is the boolean negation. NULL stays NULL.
pub(crate) fn eval_in_list(
    operand: &Expr,
    x: &Datum,
    list: &[Expr],
    negated: bool,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    // An empty list is decided before the operand is even considered: `x IN ()`
    // is false and `x NOT IN ()` is true for every `x`, NULL included. Only a
    // non-empty list lets a NULL operand make the answer unknown.
    if list.is_empty() {
        return Ok(Datum::Bool(negated));
    }
    if x.is_null() {
        return Ok(Datum::Null);
    }
    let mut saw_null = false;
    for item in list {
        let v = eval_child(item)?;
        // `x IN (a, b)` is `x = a OR x = b`, and each of those equalities
        // resolves its own `unknown` literal — so `c.oid IN ('1','2')` compares
        // as oids while `'10' IN ('9', 20)` still compares '10' to '9' as text.
        let (xc, vc) = coerce_untyped_literal_operands(BinaryOp::Eq, operand, item, x, &v, ctx)?;
        let (x, v) = (xc.as_ref().unwrap_or(x), vc.as_ref().unwrap_or(&v));
        if runtime_equality_short_circuit(x, v) == Some(false) {
            continue;
        }
        require_runtime_equality(x, v)?;
        match ops::compare(x, v)? {
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
/// propagates exactly as three-valued AND/NOT define. Each operand is paired
/// with the expression it came from because the two comparisons resolve their
/// `unknown` literals independently, exactly as PostgreSQL's expansion does.
pub(crate) fn eval_between(
    x: (&Expr, &Datum),
    lo: (&Expr, &Datum),
    hi: (&Expr, &Datum),
    negated: bool,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let ge = apply_comparison_of(BinaryOp::Ge, x, lo, ctx)?;
    let le = apply_comparison_of(BinaryOp::Le, x, hi, ctx)?;
    let res = ops::and(&ge, &le)?;
    Ok(if negated { ops::not(&res)? } else { res })
}

/// `s LIKE|ILIKE|SIMILAR TO pat [ESCAPE e]` and their negations.
///
/// A NULL subject, pattern, or escape string gives NULL. A non-text operand
/// gives 42804. `escape` is the evaluated `ESCAPE` clause. Without an `ESCAPE`
/// clause, each pattern language uses `\`.
pub(crate) fn eval_like(
    s: &Datum,
    pat: &Datum,
    negated: bool,
    kind: MatchKind,
    escape: Option<&Datum>,
) -> Result<Datum, ExecError> {
    if s.is_null() || pat.is_null() || escape.is_some_and(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let escape = match escape {
        Some(e) => crate::pattern::escape_char(e)?,
        None => Some('\\'),
    };
    let (subject, pattern) = (as_text(s)?, as_text(pat)?);
    let m = match kind {
        MatchKind::Like => like_match(subject, pattern, false, escape)?,
        MatchKind::ILike => like_match(subject, pattern, true, escape)?,
        MatchKind::Similar => crate::pattern::similar_match(subject, pattern, escape)?,
    };
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

/// SQL `LIKE` matcher over Unicode scalar values.
///
/// `%` matches zero or more characters and `_` matches exactly one. `escape` is
/// the `ESCAPE` clause's character. It is `\` by default and `None` for
/// `ESCAPE ''`, and it makes the next pattern character literal. The matcher
/// tests the escape BEFORE the wildcards, so `ESCAPE '%'` stops `%` from acting
/// as a wildcard, as it does in `PostgreSQL`.
///
/// `ci` folds ASCII case, which is the `ILIKE` form. A pattern that ends in a
/// lone escape character is an invalid escape sequence (22025). The matcher
/// backtracks iteratively to the last `%`, with an O(n·m) worst case.
pub(crate) fn like_match(
    s: &str,
    p: &str,
    ci: bool,
    escape: Option<char>,
) -> Result<bool, ExecError> {
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
                c if Some(c) == escape => {
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
    // `s` is consumed; the remaining pattern must be only `%` to match. A lone
    // trailing escape character is NOT an error here — PostgreSQL raises 22025
    // only while subject characters remain, and simply fails to match once the
    // subject is exhausted (`'a' LIKE 'a\'` is false, `'ab' LIKE 'a\'` is 22025).
    while pi < pb.len() {
        match pb[pi] {
            c if Some(c) == escape => return Ok(false),
            '%' => pi += 1,
            _ => return Ok(false),
        }
    }
    Ok(true)
}

/// A `CASE` expression.
///
/// In the searched form, where `operand` is None, the first WHEN whose
/// condition is TRUE wins. A false or NULL condition is skipped, and a
/// non-boolean condition gives 42804. In the simple form, the first WHEN value
/// that compares Equal to the operand wins, and NULL never matches. When no
/// WHEN matches, the result is the ELSE branch, or NULL.
///
/// This function evaluates branches lazily and in order, so it never reaches a
/// later branch's error or side effect early.
pub(crate) fn eval_case(
    operand: Option<&Expr>,
    whens: &[(Expr, Expr)],
    else_result: Option<&Expr>,
    result_type: ColumnType,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    match operand {
        None => {
            for (cond, result) in whens {
                match eval_child(cond)? {
                    Datum::Bool(true) => {
                        let value = eval_child(result)?;
                        return cast_value(&value, result_type, &ctx.time_zone);
                    }
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
                // A simple `CASE` is a chain of equalities, so `CASE oid WHEN
                // '1' …` resolves the bare literal from the operand just as
                // `oid = '1'` would.
                let (oc, vc) =
                    coerce_untyped_literal_operands(BinaryOp::Eq, op, val, &ov, &vv, ctx)?;
                let (ov, vv) = (oc.as_ref().unwrap_or(&ov), vc.as_ref().unwrap_or(&vv));
                if runtime_equality_short_circuit(ov, vv) == Some(false) {
                    continue;
                }
                require_runtime_equality(ov, vv)?;
                if matches!(ops::compare(ov, vv)?, Some(Ordering::Equal)) {
                    let value = eval_child(result)?;
                    return cast_value(&value, result_type, &ctx.time_zone);
                }
            }
        }
    }
    match else_result {
        Some(e) => {
            let value = eval_child(e)?;
            cast_value(&value, result_type, &ctx.time_zone)
        }
        None => Ok(Datum::Null),
    }
}

/// Apply a unary operator to an already-evaluated operand.
///
/// Scalar `eval` and the SP27 grouped evaluator `agg::eval_grouped` share this
/// function. `ctx` is threaded uniformly, and no unary operator consumes it
/// yet.
pub(crate) fn apply_unary(op: UnaryOp, v: &Datum, _ctx: &EvalCtx) -> Result<Datum, ExecError> {
    match op {
        UnaryOp::Not => Ok(ops::not(v)?),
        UnaryOp::TsNot => match v {
            Datum::Null => Ok(Datum::Null),
            Datum::TsQuery(query) => Ok(Datum::TsQuery(crabka_pgtypes::TsQuery::Not(Box::new(
                query.clone(),
            )))),
            other => Err(undefined_prefix_operator(op, other)),
        },
        // SP37: unary minus on an interval negates each field (`0 - interval` has no
        // defined operator). Everything else is `0 - v` (int/numeric/float negation).
        UnaryOp::Neg => match v {
            Datum::Interval(i) => Ok(Datum::Interval(crabka_pgtypes::datetime::neg_interval(*i)?)),
            // Negation stays at the operand's own width, so `-((-32768)::int2)`
            // is 22003 rather than a silently widened 32768.
            Datum::Int2(_) => Ok(ops::sub(&Datum::Int2(0), v)?),
            // The float widths flip the sign bit rather than subtracting from
            // zero, matching `float4um`/`float8um`: `-('0'::float4)` is `-0`,
            // which `0 - 0` would render as `0`.
            Datum::Float4(f) => Ok(Datum::Float4(-f)),
            Datum::Float8(f) => Ok(Datum::Float8(-f)),
            // `numeric` has its own `numeric_uminus`, which flips the sign
            // without inventing a zero operand — so `-'NaN'::numeric` is `NaN`
            // and the display scale is the operand's own.
            Datum::Numeric(n) => Ok(Datum::Numeric(crabka_pgtypes::numeric::neg(n))),
            // `money` has no unary minus at all in PostgreSQL, and the generic
            // `0 - v` fallback would otherwise report an integer-operand
            // mismatch instead of the missing operator.
            // Neither `money` nor any system identifier type has a unary
            // minus in PostgreSQL, and the generic `0 - v` fallback would
            // otherwise report an integer-operand mismatch (42804) instead of
            // the missing operator (42883).
            Datum::Money(_)
            | Datum::BitString(_)
            | Datum::Oid(_)
            | Datum::Xid(_)
            | Datum::Xid8(_)
            | Datum::Cid(_)
            | Datum::Tid(_)
            | Datum::PgLsn(_) => Err(undefined_prefix_operator(op, v)),
            _ => Ok(ops::sub(&Datum::Int4(0), v)?),
        },
        UnaryOp::Plus
        | UnaryOp::BitNot
        | UnaryOp::Abs
        | UnaryOp::Sqrt
        | UnaryOp::Cbrt
        | UnaryOp::NPoints
        | UnaryOp::Length
        | UnaryOp::Center
        | UnaryOp::IsHorizontal
        | UnaryOp::IsVertical => apply_prefix_op(op, v),
        // `IS [NOT] DOCUMENT` is the one postfix test that is NOT total: a
        // NULL `xml` yields NULL, because unlike the boolean tests it asks a
        // question about a value rather than about its definedness.
        UnaryOp::IsDocument => crate::xml_fn::is_document(v, false),
        UnaryOp::IsNotDocument => crate::xml_fn::is_document(v, true),
        // The postfix boolean tests. Each is total over its operand — the whole
        // point of `IS TRUE` over `= TRUE` is that a NULL operand yields FALSE
        // rather than NULL — so none of them can return NULL.
        UnaryOp::IsTrue
        | UnaryOp::IsNotTrue
        | UnaryOp::IsFalse
        | UnaryOp::IsNotFalse
        | UnaryOp::IsUnknown
        | UnaryOp::IsNotUnknown => {
            let state = boolean_test_operand(op, v)?;
            let (want, negated) = match op {
                UnaryOp::IsTrue => (Some(true), false),
                UnaryOp::IsNotTrue => (Some(true), true),
                UnaryOp::IsFalse => (Some(false), false),
                UnaryOp::IsNotFalse => (Some(false), true),
                UnaryOp::IsUnknown => (None, false),
                _ => (None, true),
            };
            Ok(Datum::Bool((state == want) ^ negated))
        }
    }
}

/// The operand of a boolean test as a three-valued boolean (`None` is UNKNOWN).
///
/// A string is still `unknown` to `PostgreSQL` at this point. This function
/// therefore parses the string as a boolean literal and reports 22P02 when it
/// is not one, as in `'x' IS TRUE`. Any other non-boolean operand is 42804,
/// worded as `PostgreSQL` words it.
fn boolean_test_operand(op: UnaryOp, v: &Datum) -> Result<Option<bool>, ExecError> {
    match v {
        Datum::Null => Ok(None),
        Datum::Bool(b) => Ok(Some(*b)),
        Datum::Text(_) => {
            match crabka_pgtypes::cast::cast(v, ColumnType::Bool, &jiff::tz::TimeZone::UTC)? {
                Datum::Bool(b) => Ok(Some(b)),
                _ => Ok(None),
            }
        }
        other => Err(ExecError::TypeMismatch(format!(
            "argument of {} must be type boolean, not type {}",
            boolean_test_spelling(op),
            other.column_type().map_or("unknown", ColumnType::name)
        ))),
    }
}

/// The SQL spelling of a boolean test, for its 42804 message.
fn boolean_test_spelling(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::IsTrue => "IS TRUE",
        UnaryOp::IsNotTrue => "IS NOT TRUE",
        UnaryOp::IsFalse => "IS FALSE",
        UnaryOp::IsNotFalse => "IS NOT FALSE",
        UnaryOp::IsUnknown => "IS UNKNOWN",
        _ => "IS NOT UNKNOWN",
    }
}

/// The generic prefix operators `~` (bitwise NOT), `@` (absolute value), `|/`
/// (square root) and `||/` (cube root).
///
/// `~` and `@` keep their operand's type, so `@ 5.5` is `numeric`. `|/` and
/// `||/` are `PostgreSQL`'s `dsqrt`/`dcbrt`. They give `float8` whatever the
/// operand's type is, so `|/ 16.0` is `4`, not `4.0000000000000000`.
fn apply_prefix_op(op: UnaryOp, v: &Datum) -> Result<Datum, ExecError> {
    if v.is_null() {
        return Ok(Datum::Null);
    }
    match op {
        // `+` is identity, but only on the numeric types: PostgreSQL defines no
        // `+ text` / `+ boolean` / `+ interval`, so those are 42883 rather than
        // a silent pass-through.
        UnaryOp::Plus => match v {
            Datum::Int2(_)
            | Datum::Int4(_)
            | Datum::Int8(_)
            | Datum::Float4(_)
            | Datum::Float8(_)
            | Datum::Numeric(_) => Ok(v.clone()),
            other => Err(undefined_prefix_operator(op, other)),
        },
        UnaryOp::BitNot => match v {
            Datum::Int2(x) => Ok(Datum::Int2(!x)),
            Datum::Int4(x) => Ok(Datum::Int4(!x)),
            Datum::Int8(x) => Ok(Datum::Int8(!x)),
            // `~inet` inverts every address bit and keeps the netmask; a `cidr`
            // widens to `inet` first, so the result is always an `inet`.
            // `bitnot` inverts every bit and keeps the length; a `bit varying`
            // becomes a `bit`, because the operator is declared over `bit`.
            Datum::BitString(value) => Ok(Datum::BitString(value.not())),
            Datum::Inet(value) => Ok(Datum::Inet(value.not())),
            Datum::MacAddr(value) => Ok(Datum::MacAddr(value.not())),
            Datum::MacAddr8(value) => Ok(Datum::MacAddr8(value.not())),
            other => Err(undefined_prefix_operator(op, other)),
        },
        UnaryOp::Abs => match v {
            // `@ (-32768)::int2` has no int2 result — 22003, like PostgreSQL.
            Datum::Int2(x) => x
                .checked_abs()
                .map(Datum::Int2)
                .ok_or_else(|| TypeError::out_of_range_for("smallint"))
                .map_err(ExecError::Type),
            // `@ (-2147483648)::int4` has no int4 result — 22003, like PostgreSQL.
            Datum::Int4(x) => x
                .checked_abs()
                .map(Datum::Int4)
                .ok_or(ExecError::Type(TypeError::Overflow)),
            Datum::Int8(x) => x
                .checked_abs()
                .map(Datum::Int8)
                .ok_or(ExecError::Type(TypeError::Overflow)),
            Datum::Float4(f) => Ok(Datum::Float4(f.abs())),
            Datum::Float8(f) => Ok(Datum::Float8(f.abs())),
            Datum::Numeric(d) => Ok(Datum::Numeric(crabka_pgtypes::numeric::abs(d))),
            other => Err(undefined_prefix_operator(op, other)),
        },
        UnaryOp::Sqrt | UnaryOp::Cbrt => {
            let Some(x) = to_f64(v) else {
                return Err(undefined_prefix_operator(op, v));
            };
            if op == UnaryOp::Sqrt {
                if x < 0.0 {
                    return Err(domain_error(
                        "2201F",
                        "cannot take square root of a negative number",
                    ));
                }
                Ok(Datum::Float8(x.sqrt()))
            } else {
                Ok(Datum::Float8(x.cbrt()))
            }
        }
        UnaryOp::NPoints
        | UnaryOp::Length
        | UnaryOp::Center
        | UnaryOp::IsHorizontal
        | UnaryOp::IsVertical => apply_geometric_prefix(op, v),
        UnaryOp::Not
        | UnaryOp::Neg
        | UnaryOp::IsTrue
        | UnaryOp::IsNotTrue
        | UnaryOp::IsFalse
        | UnaryOp::IsNotFalse
        | UnaryOp::IsUnknown
        | UnaryOp::IsNotUnknown
        | UnaryOp::IsDocument
        | UnaryOp::IsNotDocument
        | UnaryOp::TsNot => Err(undefined_prefix_operator(op, v)),
    }
}

/// The five geometric prefix operators: `#` (the number of points in a path or
/// polygon), `@-@` (the length of an lseg or path), `@@` (the centre of a box,
/// circle, lseg or polygon — but NOT a path, which has none), and `?-` / `?|`
/// (is this line or lseg horizontal / vertical?).
///
/// Every one is 42883 on any other operand, including on `integer`: PostgreSQL
/// lexes `@-@ 5` as one operator and finds no `@-@ integer`.
fn apply_geometric_prefix(op: UnaryOp, v: &Datum) -> Result<Datum, ExecError> {
    match (op, v) {
        (UnaryOp::NPoints, Datum::Path(path)) => Ok(Datum::Int4(path.npoints())),
        (UnaryOp::NPoints, Datum::Polygon(polygon)) => Ok(Datum::Int4(polygon.npoints())),
        (UnaryOp::Length, Datum::Lseg(lseg)) => Ok(Datum::Float8(lseg.length())),
        (UnaryOp::Length, Datum::Path(path)) => Ok(Datum::Float8(path.length())),
        (UnaryOp::Center, Datum::Box(value)) => Ok(Datum::Point(value.center())),
        (UnaryOp::Center, Datum::Circle(circle)) => Ok(Datum::Point(circle.to_point())),
        (UnaryOp::Center, Datum::Lseg(lseg)) => Ok(Datum::Point(lseg.center())),
        (UnaryOp::Center, Datum::Polygon(polygon)) => Ok(Datum::Point(polygon.center())),
        (UnaryOp::IsHorizontal, Datum::Line(line)) => Ok(Datum::Bool(line.is_horizontal())),
        (UnaryOp::IsHorizontal, Datum::Lseg(lseg)) => Ok(Datum::Bool(lseg.is_horizontal())),
        (UnaryOp::IsVertical, Datum::Line(line)) => Ok(Datum::Bool(line.is_vertical())),
        (UnaryOp::IsVertical, Datum::Lseg(lseg)) => Ok(Datum::Bool(lseg.is_vertical())),
        _ => Err(undefined_prefix_operator(op, v)),
    }
}

/// The static result type of a geometric prefix operator over its operand, or
/// `None` when PostgreSQL declares no such operator. `@@` over a `path` is the
/// one that surprises: `path` has a length and a point count but no centre.
fn geometric_prefix_result_type(op: UnaryOp, ty: ColumnType) -> Option<ColumnType> {
    Some(match (op, ty) {
        (UnaryOp::NPoints, ColumnType::Path | ColumnType::Polygon) => ColumnType::Int4,
        (UnaryOp::Length, ColumnType::Lseg | ColumnType::Path) => ColumnType::Float8,
        (
            UnaryOp::Center,
            ColumnType::Box | ColumnType::Circle | ColumnType::Lseg | ColumnType::Polygon,
        ) => ColumnType::Point,
        (UnaryOp::IsHorizontal | UnaryOp::IsVertical, ColumnType::Line | ColumnType::Lseg) => {
            ColumnType::Bool
        }
        _ => return None,
    })
}

/// Promote a numeric-tower Datum to `f64`, for the operators `PostgreSQL`
/// defines only over `float8`.
fn to_f64(d: &Datum) -> Option<f64> {
    match d {
        Datum::Int2(n) => Some(f64::from(*n)),
        Datum::Float4(f) => Some(f64::from(*f)),
        Datum::Int4(n) => Some(f64::from(*n)),
        Datum::Int8(n) => Some(crabka_pgtypes::numeric::to_f64(
            &crabka_pgtypes::numeric::from_i64(*n),
        )),
        Datum::Float8(f) => Some(*f),
        Datum::Numeric(d) => Some(crabka_pgtypes::numeric::to_f64(d)),
        _ => None,
    }
}

/// A math/string domain error that carries its own `PostgreSQL` SQLSTATE.
fn domain_error(sqlstate: &'static str, message: &'static str) -> ExecError {
    ExecError::Type(TypeError::Domain { sqlstate, message })
}

/// A prefix operator's SQL spelling, for error messages.
fn prefix_spelling(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::BitNot => "~",
        UnaryOp::Abs => "@",
        UnaryOp::Sqrt => "|/",
        UnaryOp::Cbrt => "||/",
        UnaryOp::Neg => "-",
        UnaryOp::Plus => "+",
        UnaryOp::TsNot => "!!",
        UnaryOp::NPoints => "#",
        UnaryOp::Length => "@-@",
        UnaryOp::Center => "@@",
        UnaryOp::IsHorizontal => "?-",
        UnaryOp::IsVertical => "?|",
        _ => "NOT",
    }
}

/// 42883 for a prefix operator applied to a type it is not defined for.
fn undefined_prefix_operator(op: UnaryOp, v: &Datum) -> ExecError {
    undefined_prefix_operator_named(op, v.column_type().map_or("unknown", ColumnType::name))
}

/// [`undefined_prefix_operator`] with the operand type already spelled, for the
/// plan-time rejections that have a `ColumnType` rather than a value.
fn undefined_prefix_operator_named(op: UnaryOp, ty: &str) -> ExecError {
    ExecError::UndefinedFunction(format!(
        "operator does not exist: {} {ty}",
        prefix_spelling(op)
    ))
}

/// The POSIX regex-match operators `~`, `~*`, `!~` and `!~*`.
///
/// # `PostgreSQL` divergence
///
/// `PostgreSQL` matches with its own POSIX *advanced* regular expressions. This
/// implementation uses the `regex` crate. The two agree across the ERE core
/// that SQL predicates are written in: literals, bracket expressions and POSIX
/// classes (`[[:alpha:]]`), alternation, greedy and non-greedy quantifiers,
/// anchors, capture groups, and the same leftmost-unanchored match semantics.
///
/// The two differ where `PostgreSQL`'s dialect exceeds what a finite automaton
/// can express. BACK-REFERENCES (`'aa' ~ '(a)\1'`) and LOOKAROUND compile in
/// `PostgreSQL`, but this implementation reports them as an invalid regular
/// expression (2201B).
fn apply_regex_match(op: BinaryOp, l: &Datum, r: &Datum) -> Result<Datum, ExecError> {
    if l.is_null() || r.is_null() {
        return Ok(Datum::Null);
    }
    let (Datum::Text(subject), Datum::Text(pattern)) = (l, r) else {
        return Err(undefined_operator_for(op, l, r));
    };
    let regex = regex::RegexBuilder::new(pattern)
        .case_insensitive(matches!(op, BinaryOp::MatchCi | BinaryOp::NotMatchCi))
        .build()
        .map_err(|_| domain_error("2201B", "invalid regular expression"))?;
    let negated = matches!(op, BinaryOp::NotMatch | BinaryOp::NotMatchCi);
    Ok(Datum::Bool(regex.is_match(subject) != negated))
}

/// The integer bitwise operators `&`, `|`, `#` (XOR), `<<` and `>>`.
///
/// This function reduces a shift count modulo the LEFT operand's width, which
/// is what `PostgreSQL`'s `int4shl`/`int8shl` do. `1::int4 << 32` is 1,
/// `1::int4 << 31` is −2147483648, and a negative count wraps, so
/// `1::int4 << -1` is −2147483648. `>>` is an ARITHMETIC shift, so
/// `(-1)::int4 >> 1` stays −1.
fn apply_bitwise(op: BinaryOp, l: &Datum, r: &Datum) -> Result<Datum, ExecError> {
    if l.is_null() || r.is_null() {
        return Ok(Datum::Null);
    }
    if matches!(op, BinaryOp::Shl | BinaryOp::Shr) {
        // Only the left operand's width decides the result type; the count is
        // always taken as an ordinary integer.
        let (Some(count), true) = (as_int(r), as_int(l).is_some()) else {
            return Err(undefined_operator_for(op, l, r));
        };
        let left = matches!(op, BinaryOp::Shl);
        return match l {
            Datum::Int2(x) => {
                let n = shift_count(count, 16);
                Ok(Datum::Int2(if left {
                    x.wrapping_shl(n)
                } else {
                    x.wrapping_shr(n)
                }))
            }
            Datum::Int4(x) => {
                let n = shift_count(count, 32);
                Ok(Datum::Int4(if left {
                    x.wrapping_shl(n)
                } else {
                    x.wrapping_shr(n)
                }))
            }
            _ => {
                let x = as_int(l).unwrap_or_default();
                let n = shift_count(count, 64);
                Ok(Datum::Int8(if left {
                    x.wrapping_shl(n)
                } else {
                    x.wrapping_shr(n)
                }))
            }
        };
    }
    let combine = |a: i64, b: i64| match op {
        BinaryOp::BitAnd => a & b,
        BinaryOp::BitOr => a | b,
        _ => a ^ b,
    };
    let (Some(a), Some(b)) = (as_int(l), as_int(r)) else {
        return Err(undefined_operator_for(op, l, r));
    };
    // `&`/`|`/`#` over two operands of a given width always land back inside
    // that width, so combining in i64 and narrowing to the operand width — the
    // width `infer_binary_type` reports — is exact.
    let combined = combine(a, b);
    Ok(match (l, r) {
        (Datum::Int2(_), Datum::Int2(_)) => Datum::Int2(
            i16::try_from(combined).expect("a bitwise result of two int2 operands fits int2"),
        ),
        (Datum::Int2(_) | Datum::Int4(_), Datum::Int2(_) | Datum::Int4(_)) => Datum::Int4(
            i32::try_from(combined).expect("a bitwise result of two int4 operands fits int4"),
        ),
        _ => Datum::Int8(combined),
    })
}

/// A shift count reduced to `width`.
///
/// The reduction matches two's-complement masking of the low `log2(width)`
/// bits. `-1` over 32 bits is 31, as `PostgreSQL` produces.
fn shift_count(count: i64, width: i64) -> u32 {
    u32::try_from(count.rem_euclid(width)).unwrap_or(0)
}

/// An integer Datum as `i64`.
///
/// This conversion is deliberately narrow. `numeric` and `float8` have no
/// bitwise operators in `PostgreSQL`, so they must reach 42883 and must not be
/// truncated.
fn as_int(d: &Datum) -> Option<i64> {
    match d {
        Datum::Int2(n) => Some(i64::from(*n)),
        Datum::Int4(n) => Some(i64::from(*n)),
        Datum::Int8(n) => Some(*n),
        _ => None,
    }
}

/// `^`, exponentiation.
///
/// There is no `integer ^ integer` in `PostgreSQL`, so an all-integer `2^3`
/// resolves to the `float8` operator and gives `8`. A `numeric` operand with no
/// `float8` operand selects the exact `numeric` form, so `5.0^2` is
/// `25.000000000000000`.
fn apply_pow(l: &Datum, r: &Datum) -> Result<Datum, ExecError> {
    if l.is_null() || r.is_null() {
        return Ok(Datum::Null);
    }
    let numeric_pair = |d: &Datum| match d {
        Datum::Int2(n) => Some(crabka_pgtypes::numeric::from_i64(i64::from(*n))),
        Datum::Int4(n) => Some(crabka_pgtypes::numeric::from_i64(i64::from(*n))),
        Datum::Int8(n) => Some(crabka_pgtypes::numeric::from_i64(*n)),
        Datum::Numeric(d) => Some(d.clone()),
        _ => None,
    };
    if (matches!(l, Datum::Numeric(_)) || matches!(r, Datum::Numeric(_)))
        && let (Some(base), Some(exp)) = (numeric_pair(l), numeric_pair(r))
    {
        return crabka_pgtypes::numeric::num_power(&base, &exp)
            .map(Datum::Numeric)
            .map_err(ExecError::Type);
    }
    let (Some(base), Some(exp)) = (to_f64(l), to_f64(r)) else {
        return Err(undefined_operator_for(BinaryOp::Pow, l, r));
    };
    if base == 0.0 && exp < 0.0 {
        return Err(domain_error(
            "2201F",
            "zero raised to a negative power is undefined",
        ));
    }
    if base < 0.0 && exp.fract() != 0.0 {
        return Err(domain_error(
            "2201F",
            "a negative number raised to a non-integer power yields a complex result",
        ));
    }
    let result = base.powf(exp);
    if result.is_infinite() && base.is_finite() && exp.is_finite() {
        return Err(ExecError::Type(TypeError::Overflow));
    }
    Ok(Datum::Float8(result))
}

/// Apply a binary operator when the operand *expressions* and the scope are also
/// available.
///
/// This is the entry point that both evaluators use. Only `||` needs more than
/// the values. PostgreSQL resolves which concatenation operator applies, text,
/// jsonb, or one of the three array forms, from the operands' STATIC types, and
/// those types are indistinguishable once an operand has evaluated to SQL NULL.
/// Every other operator resolves from the values alone and goes directly to
/// [`apply_binary`].
pub(crate) fn apply_binary_of(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    l: &Datum,
    r: &Datum,
    scope: &Scope,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    reject_uncomparable_comparison(op, left, right, scope)?;
    // These overloaded strict operators must still be resolved before value-
    // time NULL handling. Keep the runtime fast paths from accepting an invalid
    // typed pair such as `int4range @> NULL::text` in a predicate that did not
    // go through projection type inference.
    if matches!(
        op,
        BinaryOp::Contains
            | BinaryOp::ContainedBy
            | BinaryOp::Overlaps
            | BinaryOp::DoesNotExtendRight
            | BinaryOp::DoesNotExtendLeft
            | BinaryOp::Adjacent
            | BinaryOp::Shl
            | BinaryOp::Shr
            | BinaryOp::ContainedByOrEq
            | BinaryOp::ContainsOrEq
            | BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
    ) {
        infer_binary_type(op, left, right, scope)?;
    }
    // A `character` operand is cast to `text` first: `=`, the other five
    // comparisons and `||` have no `bpchar` overload, so PostgreSQL reaches them
    // through the implicit cast, and the padding never takes part. `LIKE` and
    // the regular-expression operators are NOT here — `bpcharlike` and
    // `bpcharregexeq` read the padded datum, so `'a'::char(3) LIKE 'a'` is false
    // on PostgreSQL exactly as it is here.
    let (lb, rb) = bpchar_to_text_operands(op, (left, l), (right, r), scope)?;
    let (l, r) = (lb.as_ref().unwrap_or(l), rb.as_ref().unwrap_or(r));
    let (lc, rc) = coerce_untyped_literal_operands(op, left, right, l, r, ctx)?;
    let (l, r) = (lc.as_ref().unwrap_or(l), rc.as_ref().unwrap_or(r));
    if op == BinaryOp::Concat {
        let (kind, _) = resolve_concat(left, right, scope)?;
        return apply_concat(kind, l, r, ctx);
    }
    apply_binary(op, l, r, ctx)
}

/// Apply a comparison to two already-evaluated operands, resolving a bare
/// `unknown` literal from the other side first. `BETWEEN` and `IN` expand into
/// comparisons that PostgreSQL resolves one at a time, and neither has a
/// [`BinaryOp`] expression of its own to hand to [`apply_binary_of`], so each
/// operand arrives here paired with the expression that produced it.
pub(crate) fn apply_comparison_of(
    op: BinaryOp,
    (le, l): (&Expr, &Datum),
    (re, r): (&Expr, &Datum),
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let (lc, rc) = coerce_untyped_literal_operands(op, le, re, l, r, ctx)?;
    apply_binary(op, lc.as_ref().unwrap_or(l), rc.as_ref().unwrap_or(r), ctx)
}

/// Convert an `unknown` string-literal operand's *value* to the type the
/// operator resolved it to.
///
/// A resolved type alone is not enough. The literal still evaluates to a
/// `Datum::Text`, while the jsonb operators need a `Datum::Jsonb` or a `text[]`
/// path. This function returns `None` per side when that side needs no
/// conversion, so the common case copies nothing.
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
        // A comparison against a date/time value resolves the literal to that
        // type, which is how `f1 < '05:06:07'` works on a `time` column.
        if matches!(
            other,
            Datum::Date(_)
                | Datum::Time(_)
                | Datum::Timetz(_)
                | Datum::Timestamp(_)
                | Datum::Timestamptz(_)
                | Datum::Interval(_)
        ) {
            return match op {
                BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge => other.column_type(),
                _ => None,
            };
        }
        // An array counterpart resolves the literal to that array type for the
        // array-only operators and the comparisons — but NOT for `||`, where
        // `ARRAY['a'] || 'b'` must stay `anyarray || anyelement`.
        let array_type = match other {
            Datum::Array(array) => Some(array.column_type()),
            Datum::OidVector(v) => Some(if v.elem == crabka_pgtypes::ElemType::Int2 {
                ColumnType::Int2Vector
            } else {
                ColumnType::OidVector
            }),
            _ => None,
        };
        if let Some(array_type) = array_type {
            return match op {
                BinaryOp::Contains
                | BinaryOp::ContainedBy
                | BinaryOp::Overlaps
                | BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge => Some(array_type),
                _ => None,
            };
        }
        if let Datum::Range(range) = other {
            return match op {
                BinaryOp::Contains
                | BinaryOp::ContainedBy
                | BinaryOp::Overlaps
                | BinaryOp::Add
                | BinaryOp::Sub
                | BinaryOp::Mul
                | BinaryOp::Shl
                | BinaryOp::Shr
                | BinaryOp::DoesNotExtendRight
                | BinaryOp::DoesNotExtendLeft
                | BinaryOp::Adjacent
                | BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge => Some(range.column_type()),
                _ => None,
            };
        }
        if let Datum::Multirange(multirange) = other {
            return match op {
                BinaryOp::Contains
                | BinaryOp::ContainedBy
                | BinaryOp::Overlaps
                | BinaryOp::Add
                | BinaryOp::Sub
                | BinaryOp::Mul
                | BinaryOp::Shl
                | BinaryOp::Shr
                | BinaryOp::DoesNotExtendRight
                | BinaryOp::DoesNotExtendLeft
                | BinaryOp::Adjacent
                | BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge => Some(multirange.column_type()),
                _ => None,
            };
        }
        // A system identifier counterpart resolves the literal to its own type,
        // which is how `f1 = '1234'` compares two `oid`s and `t = '(0,1)'` two
        // `tid`s. `pg_lsn` also takes `-` here, and deliberately: PostgreSQL
        // has both `pg_lsn - pg_lsn` and `pg_lsn - numeric`, and its preference
        // for the exact-match candidate is what makes `lsn - '16'` a 22P02
        // rather than an offset. `+` is left out because `pg_lsn + numeric` is
        // its only candidate, so the literal there is a `numeric`.
        let sysid_type = match other {
            Datum::Oid(_) => Some(ColumnType::Oid),
            Datum::Xid(_) => Some(ColumnType::Xid),
            Datum::Xid8(_) => Some(ColumnType::Xid8),
            Datum::Cid(_) => Some(ColumnType::Cid),
            Datum::Tid(_) => Some(ColumnType::Tid),
            Datum::PgLsn(_) => Some(ColumnType::PgLsn),
            _ => None,
        };
        if let Some(sysid_type) = sysid_type {
            return match op {
                BinaryOp::Sub if sysid_type == ColumnType::PgLsn => Some(sysid_type),
                // `pg_lsn + numeric` is the only `+` candidate, so an unknown
                // literal there is a `numeric` and `lsn + '16'` is an offset.
                BinaryOp::Add if sysid_type == ColumnType::PgLsn => Some(ColumnType::Numeric(None)),
                BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge
                | BinaryOp::IsDistinctFrom
                | BinaryOp::IsNotDistinctFrom => Some(sysid_type),
                _ => None,
            };
        }
        // A network counterpart resolves the literal to its own type, which is
        // how `b < '08:00:2b:01:02:04'` compares two `macaddr`s and
        // `i <<= '192.168.1.0/24'` is a containment test. An `inet`/`cidr`
        // counterpart resolves the literal to `inet`, the category's preferred
        // type, exactly as PostgreSQL's operator resolution does.
        // A `money` counterpart resolves the literal to `money`: `m + '123'`
        // adds $123.00 and `m = '$123.00'` compares two cash values.
        if matches!(other, Datum::Money(_)) {
            return match op {
                BinaryOp::Add
                | BinaryOp::Sub
                | BinaryOp::Div
                | BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge
                | BinaryOp::IsDistinctFrom
                | BinaryOp::IsNotDistinctFrom => Some(ColumnType::Money),
                // `money * x` has candidates for every numeric width, so the
                // literal's meaning is not decidable here — as in PostgreSQL.
                _ => None,
            };
        }
        // A bit-string counterpart resolves the literal to a bit string, which
        // is how `b = '1010'` compares two `bit`s and `v || '01'` concatenates.
        if let Datum::BitString(bits) = other {
            return match op {
                BinaryOp::Concat
                | BinaryOp::BitAnd
                | BinaryOp::BitOr
                | BinaryOp::BitXor
                | BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge
                | BinaryOp::IsDistinctFrom
                | BinaryOp::IsNotDistinctFrom => Some(if bits.varying {
                    ColumnType::VarBit(None)
                } else {
                    ColumnType::Bit(None)
                }),
                // `bit << int` is a shift, so the literal beside one is an
                // integer rather than a bit string.
                _ => None,
            };
        }
        let network_type = match other {
            Datum::Inet(_) => Some(ColumnType::Inet),
            Datum::MacAddr(_) => Some(ColumnType::MacAddr),
            Datum::MacAddr8(_) => Some(ColumnType::MacAddr8),
            _ => None,
        };
        if let Some(network_type) = network_type {
            return match op {
                BinaryOp::Shl
                | BinaryOp::Shr
                | BinaryOp::ContainedByOrEq
                | BinaryOp::ContainsOrEq
                | BinaryOp::Overlaps
                | BinaryOp::BitAnd
                | BinaryOp::BitOr
                | BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge
                | BinaryOp::IsDistinctFrom
                | BinaryOp::IsNotDistinctFrom => Some(network_type),
                // `inet - inet` is a difference, but `inet - bigint` is an
                // offset, so the literal's meaning is not decidable here.
                _ => None,
            };
        }
        // A geometric counterpart resolves the literal to whichever type the
        // operator's one surviving candidate wants — its own for
        // `p.f1 << '(0,0)'`, but a `point` for `b.f1 + '(1,2)'`, whose only
        // candidate is `box + point`.
        if let Some(geometric_type) = other.column_type().filter(|ty| is_geometric_type(*ty)) {
            return geometric_literal_type(op, geometric_type);
        }
        // A scalar counterpart resolves the literal to its own type, which is how
        // psql's `c.oid = '20001'` finds an `oid` comparison and `1 + '1'` stays
        // integer arithmetic. `regclass` is deliberately absent: PostgreSQL's
        // `regclass =` is `oideq`, so its bare literal is read as an *oid* and
        // `'pg_class'::regclass = 'pg_class'` is an oid syntax error, not a name
        // lookup.
        let scalar_type = match other {
            Datum::Bool(_)
            | Datum::Int2(_)
            | Datum::Int4(_)
            | Datum::Int8(_)
            | Datum::Float4(_)
            | Datum::Float8(_)
            | Datum::Numeric(_)
            | Datum::Bytea(_) => other.column_type(),
            _ => None,
        };
        if let Some(scalar_type) = scalar_type {
            let comparison = matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::Lt
                    | BinaryOp::Le
                    | BinaryOp::Gt
                    | BinaryOp::Ge
                    | BinaryOp::IsDistinctFrom
                    | BinaryOp::IsNotDistinctFrom
            );
            let resolves = comparison
                || match other {
                    Datum::Bool(_) => matches!(op, BinaryOp::And | BinaryOp::Or),
                    Datum::Int2(_) | Datum::Int4(_) | Datum::Int8(_) => matches!(
                        op,
                        BinaryOp::Add
                            | BinaryOp::Sub
                            | BinaryOp::Mul
                            | BinaryOp::Div
                            | BinaryOp::Mod
                            | BinaryOp::Pow
                            | BinaryOp::BitAnd
                            | BinaryOp::BitOr
                            | BinaryOp::BitXor
                            | BinaryOp::Shl
                            | BinaryOp::Shr
                    ),
                    Datum::Float4(_) | Datum::Float8(_) | Datum::Numeric(_) => matches!(
                        op,
                        BinaryOp::Add
                            | BinaryOp::Sub
                            | BinaryOp::Mul
                            | BinaryOp::Div
                            | BinaryOp::Mod
                            | BinaryOp::Pow
                    ),
                    _ => false,
                };
            return resolves.then_some(scalar_type);
        }
        // A `json` counterpart resolves a literal for the two PATH operators
        // only. PostgreSQL gives `json` six operators and no more; of those,
        // `->` and `->>` take `text` (which the literal already is) or
        // `integer`, and `#>`/`#>>` take `text[]`, which a `Datum::Text` is not.
        if matches!(other, Datum::Json(_)) {
            return match op {
                BinaryOp::JsonGetPath | BinaryOp::JsonGetPathText => {
                    ColumnType::array_of(ColumnType::Text)
                }
                _ => None,
            };
        }
        if !matches!(other, Datum::Jsonb(_)) {
            return match (op, other) {
                (BinaryOp::JsonPathMatch, Datum::TsVector(_)) => Some(ColumnType::TsQuery),
                (BinaryOp::Concat, Datum::TsVector(_)) => Some(ColumnType::TsVector),
                (BinaryOp::Concat, Datum::TsQuery(_)) => Some(ColumnType::TsQuery),
                (
                    BinaryOp::Contains | BinaryOp::ContainedBy | BinaryOp::Overlaps,
                    Datum::TsQuery(_),
                ) => Some(ColumnType::TsQuery),
                _ => None,
            };
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
            BinaryOp::JsonPathExists | BinaryOp::JsonPathMatch => Some(ColumnType::JsonPath),
            _ => None,
        }
    };
    let convert = |e: &Expr, v: &Datum, other: &Datum| -> Result<Option<Datum>, ExecError> {
        if !matches!(e, Expr::StringLiteral(_)) || !matches!(v, Datum::Text(_)) {
            return Ok(None);
        }
        match target(other) {
            Some(ty) => Ok(Some(cast_value(v, ty, &ctx.time_zone)?)),
            None => Ok(None),
        }
    };
    Ok((convert(left, l, r)?, convert(right, r, l)?))
}

/// Apply a binary operator to two already-evaluated operands.
///
/// Scalar `eval` and the SP27 grouped evaluator `agg::eval_grouped` share this
/// function. `ctx` gives the session zone that `||`'s text rendering uses.
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
    // Every geometric operator is resolved by the EXACT (operator, left type,
    // right type) triple PostgreSQL declares, so this claims nothing but a
    // geometric operand pair and the families below keep their spellings.
    //
    // The operand test is written out here rather than left to the callee: this
    // is a PER-ROW path, and hoisting it keeps a row of integers to two inlined
    // discriminant comparisons with no call at all.
    if (is_geometric_datum(l) || is_geometric_datum(r))
        && let Some(result) = apply_geometric_operator(op, l, r)
    {
        return result;
    }
    // `<<`, `>>`, `&&`, `&`, `|`, `+` and `-` are shared with the bitwise,
    // range and arithmetic families; a network operand is what selects the
    // inet/cidr meaning, so this must run before the `match` below claims them.
    if let Some(result) = crate::network_fn::apply_network_operator(op, l, r)? {
        return Ok(result);
    }
    // `||`, `&`, `|`, `#`, `<<` and `>>` are shared with the string, bitwise
    // and network families; a bit-string operand is what selects this meaning,
    // so this must run before the `match` below claims them.
    if let Some(result) = crate::bit_fn::apply_bit_operator(op, l, r)? {
        return Ok(result);
    }
    // `money` shares `+ - * /` with the numeric family, so a `money` operand is
    // what selects the cash arithmetic — including its truncating integer
    // division, which the numeric rules would otherwise round.
    if let Some(result) = crate::money_fn::apply_money_operator(op, l, r)? {
        return Ok(result);
    }
    // `pg_lsn` shares `+` and `-` with the numeric family, and its `-` against
    // another `pg_lsn` leaves the type entirely (it is a `numeric` byte count),
    // so a `pg_lsn` operand has to select this meaning before the numeric rules
    // below claim it.
    if let Some(result) = crate::sysid_fn::apply_sysid_operator(op, l, r)? {
        return Ok(result);
    }
    // A `json` operand has to be turned away from every family below before one
    // of them claims it: `ops::compare` would order two documents as text and
    // `ops::sub` would report a 42804 where PostgreSQL reports a 42883.
    if let Some(error) = json_operator_rejection(op, l, r) {
        return Err(error);
    }
    match op {
        // `<<=` / `>>=` exist only for the network family, so anything that
        // reaches here is a type error rather than another overload.
        BinaryOp::ContainedByOrEq | BinaryOp::ContainsOrEq => {
            if l.is_null() || r.is_null() {
                Ok(Datum::Null)
            } else {
                Err(undefined_operator_for(op, l, r))
            }
        }
        BinaryOp::Add if matches!((l, r), (Datum::Range(_), Datum::Range(_))) => {
            let (Datum::Range(a), Datum::Range(b)) = (l, r) else {
                unreachable!()
            };
            Ok(Datum::Range(crabka_pgtypes::range::union(a, b)?))
        }
        BinaryOp::Add if matches!((l, r), (Datum::Multirange(_), Datum::Multirange(_))) => {
            let (Datum::Multirange(a), Datum::Multirange(b)) = (l, r) else {
                unreachable!()
            };
            Ok(Datum::Multirange(crabka_pgtypes::multirange::union(a, b)?))
        }
        BinaryOp::Add => Ok(ops::add(l, r)?),
        // jsonb `-` (delete a key, an index, or a set of keys) overloads the
        // arithmetic `-`; a jsonb LEFT operand is what selects it, so every
        // numeric/date pair keeps its existing behavior.
        BinaryOp::Sub if matches!(l, Datum::Jsonb(_)) => {
            json_fn::eval_json_operator(JsonOp::Delete, l, r)
        }
        BinaryOp::Sub if matches!((l, r), (Datum::Range(_), Datum::Range(_))) => {
            let (Datum::Range(a), Datum::Range(b)) = (l, r) else {
                unreachable!()
            };
            Ok(Datum::Range(crabka_pgtypes::range::difference(a, b)?))
        }
        BinaryOp::Sub if matches!((l, r), (Datum::Multirange(_), Datum::Multirange(_))) => {
            let (Datum::Multirange(a), Datum::Multirange(b)) = (l, r) else {
                unreachable!()
            };
            Ok(Datum::Multirange(crabka_pgtypes::multirange::difference(
                a, b,
            )?))
        }
        BinaryOp::Sub => Ok(ops::sub(l, r)?),
        BinaryOp::Mul if matches!((l, r), (Datum::Range(_), Datum::Range(_))) => {
            let (Datum::Range(a), Datum::Range(b)) = (l, r) else {
                unreachable!()
            };
            Ok(Datum::Range(crabka_pgtypes::range::intersection(a, b)?))
        }
        BinaryOp::Mul if matches!((l, r), (Datum::Multirange(_), Datum::Multirange(_))) => {
            let (Datum::Multirange(a), Datum::Multirange(b)) = (l, r) else {
                unreachable!()
            };
            Ok(Datum::Multirange(crabka_pgtypes::multirange::intersection(
                a, b,
            )?))
        }
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
        BinaryOp::JsonPathExists => json_fn::eval_json_operator(JsonOp::PathExists, l, r),
        BinaryOp::JsonPathMatch
            if matches!(l, Datum::TsVector(_) | Datum::TsQuery(_) | Datum::Text(_))
                || matches!(r, Datum::TsVector(_) | Datum::TsQuery(_)) =>
        {
            apply_text_search_match(l, r, ctx)
        }
        BinaryOp::JsonPathMatch => json_fn::eval_json_operator(JsonOp::PathMatch, l, r),
        // `@>` / `<@` are defined for BOTH jsonb and arrays.
        BinaryOp::Contains | BinaryOp::ContainedBy => apply_containment(op, l, r),
        BinaryOp::Overlaps if matches!((l, r), (Datum::Range(_), Datum::Range(_))) => {
            let (Datum::Range(left), Datum::Range(right)) = (l, r) else {
                unreachable!()
            };
            Ok(Datum::Bool(crabka_pgtypes::range::overlaps(left, right)?))
        }
        BinaryOp::Overlaps if matches!((l, r), (Datum::TsQuery(_), Datum::TsQuery(_))) => {
            let (Datum::TsQuery(left), Datum::TsQuery(right)) = (l, r) else {
                unreachable!()
            };
            Ok(Datum::TsQuery(crabka_pgtypes::TsQuery::And(
                Box::new(left.clone()),
                Box::new(right.clone()),
            )))
        }
        BinaryOp::Overlaps if l.is_null() || r.is_null() => Ok(Datum::Null),
        BinaryOp::Overlaps => match (l, r) {
            (Datum::Range(range), Datum::Multirange(multirange))
            | (Datum::Multirange(multirange), Datum::Range(range)) => Ok(Datum::Bool(
                crabka_pgtypes::multirange::overlaps_range(multirange, range)?,
            )),
            (Datum::Multirange(left), Datum::Multirange(right)) => Ok(Datum::Bool(
                crabka_pgtypes::multirange::overlaps(left, right)?,
            )),
            _ => array_fn::array_overlap(l, r),
        },
        BinaryOp::Same
        | BinaryOp::StrictlyBelow
        | BinaryOp::StrictlyAbove
        | BinaryOp::DoesNotExtendAbove
        | BinaryOp::DoesNotExtendBelow
        | BinaryOp::DoesNotExtendRight
        | BinaryOp::DoesNotExtendLeft
        | BinaryOp::Adjacent
        | BinaryOp::Shl
        | BinaryOp::Shr
            if matches!(l, Datum::Range(_) | Datum::Multirange(_))
                && matches!(r, Datum::Range(_) | Datum::Multirange(_)) =>
        {
            Ok(Datum::Bool(apply_range_directional(op, l, r)?))
        }
        BinaryOp::Same
        | BinaryOp::StrictlyBelow
        | BinaryOp::StrictlyAbove
        | BinaryOp::DoesNotExtendAbove
        | BinaryOp::DoesNotExtendBelow
        | BinaryOp::DoesNotExtendRight
        | BinaryOp::DoesNotExtendLeft
        | BinaryOp::Adjacent => {
            if l.is_null() || r.is_null() {
                Ok(Datum::Null)
            } else {
                Err(undefined_operator_for(op, l, r))
            }
        }
        // `<->` is the tsquery phrase operator and the geometric distance
        // operator; the operand types pick the overload, and the geometric one
        // was already claimed by `apply_geometric_operator` above.
        BinaryOp::Phrase => match (l, r) {
            (Datum::TsQuery(left), Datum::TsQuery(right)) => Ok(Datum::TsQuery(
                crabka_pgtypes::TsQuery::Phrase(Box::new(left.clone()), Box::new(right.clone()), 1),
            )),
            (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
            _ => Err(undefined_operator_for(op, l, r)),
        },
        // The seven operators PostgreSQL declares for geometry ALONE. Anything
        // reaching here had no geometric operand, so there is no overload left.
        BinaryOp::ClosestPoint
        | BinaryOp::Intersects
        | BinaryOp::Horizontal
        | BinaryOp::Perpendicular
        | BinaryOp::Parallel
        | BinaryOp::BelowEq
        | BinaryOp::AboveEq => {
            if l.is_null() || r.is_null() {
                Ok(Datum::Null)
            } else {
                Err(undefined_operator_for(op, l, r))
            }
        }
        BinaryOp::Match | BinaryOp::MatchCi | BinaryOp::NotMatch | BinaryOp::NotMatchCi => {
            apply_regex_match(op, l, r)
        }
        BinaryOp::BitAnd | BinaryOp::BitOr | BinaryOp::BitXor | BinaryOp::Shl | BinaryOp::Shr => {
            apply_bitwise(op, l, r)
        }
        BinaryOp::Pow => apply_pow(l, r),
        BinaryOp::Mod => Ok(ops::rem(l, r)?),
        // Null-safe (in)equality: two NULLs are not distinct, a NULL and a
        // non-NULL are, and the result is never NULL.
        BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom => {
            if let Some(equal) = runtime_equality_short_circuit(l, r) {
                let distinct = !equal;
                return Ok(Datum::Bool(distinct ^ (op == BinaryOp::IsNotDistinctFrom)));
            }
            // Both forms are resolved through the type's `=`, so a geometric
            // operand with no equality operator is named that way — which
            // `require_runtime_equality` does for every construct that expands
            // into an equality.
            require_runtime_equality(l, r)?;
            let distinct = rowexpr::is_distinct(l, r)?;
            Ok(Datum::Bool(distinct ^ (op == BinaryOp::IsNotDistinctFrom)))
        }
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            if (is_geometric_datum(l) || is_geometric_datum(r))
                && let Some(result) = apply_geometric_comparison(op, l, r)
            {
                return result;
            }
            if matches!(op, BinaryOp::Eq | BinaryOp::Ne) {
                if let Some(equal) = runtime_equality_short_circuit(l, r) {
                    return Ok(Datum::Bool(equal ^ (op == BinaryOp::Ne)));
                }
                require_runtime_equality(l, r)?;
            } else {
                require_runtime_comparison(l, r)?;
            }
            let ord = ops::compare(l, r)?;
            Ok(cmp_result(op, ord))
        }
    }
}

/// `@>` / `<@`: the operand values pick the jsonb, array, range, or text-search
/// family (the static types already agreed at plan time). Every family is
/// strict, so a SQL NULL operand produces SQL NULL before runtime dispatch.
fn apply_containment(op: BinaryOp, l: &Datum, r: &Datum) -> Result<Datum, ExecError> {
    if l.is_null() || r.is_null() {
        return Ok(Datum::Null);
    }
    let contains = op == BinaryOp::Contains;
    if let (Datum::Range(left), Datum::Range(right)) = (l, r) {
        return Ok(Datum::Bool(if contains {
            crabka_pgtypes::range::contains_range(left, right)?
        } else {
            crabka_pgtypes::range::contains_range(right, left)?
        }));
    }
    match (l, r, contains) {
        (Datum::Range(range), Datum::Multirange(multirange), true)
        | (Datum::Multirange(multirange), Datum::Range(range), false) => {
            return Ok(Datum::Bool(crabka_pgtypes::multirange::range_contains(
                range, multirange,
            )?));
        }
        (Datum::Multirange(multirange), Datum::Range(range), true)
        | (Datum::Range(range), Datum::Multirange(multirange), false) => {
            return Ok(Datum::Bool(crabka_pgtypes::multirange::contains_range(
                multirange, range,
            )?));
        }
        (Datum::Multirange(left), Datum::Multirange(right), true) => {
            return Ok(Datum::Bool(crabka_pgtypes::multirange::contains(
                left, right,
            )?));
        }
        (Datum::Multirange(left), Datum::Multirange(right), false) => {
            return Ok(Datum::Bool(crabka_pgtypes::multirange::contains(
                right, left,
            )?));
        }
        _ => {}
    }
    if contains {
        if let Datum::Range(range) = l {
            return Ok(Datum::Bool(crabka_pgtypes::range::contains_element(
                range, r,
            )?));
        }
        if let Datum::Multirange(multirange) = l {
            return Ok(Datum::Bool(crabka_pgtypes::multirange::contains_element(
                multirange, r,
            )?));
        }
    } else if let Datum::Range(range) = r {
        return Ok(Datum::Bool(crabka_pgtypes::range::contains_element(
            range, l,
        )?));
    } else if let Datum::Multirange(multirange) = r {
        return Ok(Datum::Bool(crabka_pgtypes::multirange::contains_element(
            multirange, l,
        )?));
    }
    if matches!(l, Datum::Jsonb(_)) || matches!(r, Datum::Jsonb(_)) {
        let json_op = if contains {
            JsonOp::Contains
        } else {
            JsonOp::ContainedBy
        };
        return json_fn::eval_json_operator(json_op, l, r);
    }
    if matches!(l, Datum::Array(_) | Datum::OidVector(_))
        || matches!(r, Datum::Array(_) | Datum::OidVector(_))
    {
        return if contains {
            array_fn::array_contains(l, r)
        } else {
            array_fn::array_contained_by(l, r)
        };
    }
    if let (Datum::TsQuery(left), Datum::TsQuery(right)) = (l, r) {
        return Ok(Datum::Bool(if contains {
            left.contains(right)
        } else {
            right.contains(left)
        }));
    }
    if l.is_null() && r.is_null() {
        return Ok(Datum::Null);
    }
    Err(undefined_operator_for(op, l, r))
}

fn apply_range_directional(op: BinaryOp, l: &Datum, r: &Datum) -> Result<bool, ExecError> {
    type RangeRelation = fn(
        &crabka_pgtypes::RangeValue,
        &crabka_pgtypes::RangeValue,
    ) -> Result<bool, crabka_pgtypes::TypeError>;
    let (left_last, right_last, relation): (bool, bool, RangeRelation) = match op {
        BinaryOp::DoesNotExtendRight => (
            true,
            true,
            crabka_pgtypes::range::does_not_extend_right as _,
        ),
        BinaryOp::DoesNotExtendLeft => (
            false,
            false,
            crabka_pgtypes::range::does_not_extend_left as _,
        ),
        BinaryOp::Shl => (true, false, crabka_pgtypes::range::strictly_left as _),
        BinaryOp::Shr => (false, true, crabka_pgtypes::range::strictly_right as _),
        BinaryOp::Adjacent => {
            if let (Datum::Multirange(multirange), Datum::Range(range))
            | (Datum::Range(range), Datum::Multirange(multirange)) = (l, r)
            {
                return Ok(crabka_pgtypes::multirange::adjacent_range(
                    multirange, range,
                )?);
            }
            let Some(left_first) = range_boundary(l, false) else {
                return Ok(false);
            };
            let Some(left_last) = range_boundary(l, true) else {
                return Ok(false);
            };
            let Some(right_first) = range_boundary(r, false) else {
                return Ok(false);
            };
            let Some(right_last) = range_boundary(r, true) else {
                return Ok(false);
            };
            return Ok(crabka_pgtypes::range::adjacent(left_last, right_first)?
                || crabka_pgtypes::range::adjacent(left_first, right_last)?);
        }
        _ => unreachable!(),
    };
    let (Some(left), Some(right)) = (range_boundary(l, left_last), range_boundary(r, right_last))
    else {
        return Ok(false);
    };
    Ok(relation(left, right)?)
}

fn range_boundary(value: &Datum, last: bool) -> Option<&crabka_pgtypes::RangeValue> {
    match value {
        Datum::Range(range) if !range.empty => Some(range),
        Datum::Multirange(multirange) if last => multirange.ranges.last(),
        Datum::Multirange(multirange) => multirange.ranges.first(),
        _ => None,
    }
}

/// Which of PostgreSQL's concatenation operators a `||` resolves to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConcatKind {
    /// `text || anynonarray` / `anynonarray || text`.
    Text,
    /// `jsonb || jsonb`.
    Jsonb,
    /// One of the three array forms: `anyarray || anyarray`,
    /// `anyarray || anyelement`, or `anyelement || anyarray`.
    Array(ConcatForm),
    /// `tsvector || tsvector`.
    TsVector,
    /// `tsquery || tsquery` (boolean OR).
    TsQuery,
    /// `bitcat` — `bit varying || bit varying`, which `bit` reaches through its
    /// binary coercion and whose result is always `bit varying`.
    BitString,
}

/// Resolve `left || right` from the operands' STATIC types.
///
/// The result is the operator that applies plus its result type. The result is
/// 42883 when no `||` is defined for the pair.
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
/// `json` has no `||`, so there is nothing for a literal beside one to steal.
fn adopt_null_literal_type(
    left: &Expr,
    right: &Expr,
    lt: ColumnType,
    rt: ColumnType,
) -> (ColumnType, ColumnType) {
    let adopts = |t: ColumnType| {
        matches!(
            t,
            ColumnType::Jsonb | ColumnType::TsVector | ColumnType::TsQuery
        ) || crate::bit_fn::is_bit_type(t)
            || t.array_element().is_some()
    };
    if matches!(left, Expr::NullLiteral) && adopts(rt) {
        (rt, rt)
    } else if matches!(right, Expr::NullLiteral) && adopts(lt) {
        (lt, lt)
    } else {
        adopt_string_literal_type(left, right, lt, rt)
    }
}

/// PostgreSQL leaves a bare string literal `unknown` and resolves it against the
/// other operand. This codebase types it `text` at once.
///
/// Deliberately jsonb-only. An array must NOT adopt, because PostgreSQL's
/// `anyarray || anyelement` is what makes `ARRAY['a'] || 'b'` append `'b'` as an
/// element rather than concatenating two arrays. `json` must not adopt either,
/// and for the opposite reason: it has no `||` of its own, so `j || 'b'` is
/// `anynonarray || text` and really does concatenate the document's text.
fn adopt_string_literal_type(
    left: &Expr,
    right: &Expr,
    lt: ColumnType,
    rt: ColumnType,
) -> (ColumnType, ColumnType) {
    let untyped = |e: &Expr| matches!(e, Expr::StringLiteral(_));
    let adopts = |t: ColumnType| {
        matches!(
            t,
            ColumnType::Jsonb | ColumnType::TsVector | ColumnType::TsQuery | ColumnType::Range(_)
        )
    };
    if untyped(left) && adopts(rt) {
        (rt, rt)
    } else if untyped(right) && adopts(lt) {
        (lt, lt)
    } else {
        (lt, rt)
    }
}

/// Resolve an `unknown` string-literal operand of a jsonb operator to the type
/// that operator expects on that side.
///
/// The type is `text[]` for the path and multi-key operators, and `jsonb` for
/// containment.
fn adopt_json_operand_types(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    lt: ColumnType,
    rt: ColumnType,
) -> (ColumnType, ColumnType) {
    if lt == ColumnType::Jsonb
        && matches!(op, BinaryOp::JsonPathExists | BinaryOp::JsonPathMatch)
        && is_unknown_literal(right)
    {
        return (lt, ColumnType::JsonPath);
    }
    // The array containment/overlap operators have no text overload, so an
    // `unknown` literal on either side adopts the other's array type — which is
    // what makes `array_shuffle(a) <@ '{1,2,3}'` resolve in PostgreSQL.
    if matches!(
        op,
        BinaryOp::Contains | BinaryOp::ContainedBy | BinaryOp::Overlaps
    ) {
        // A bare NULL is PostgreSQL's `unknown`, so the other operand selects
        // the containment family before strict evaluation returns NULL. Typed
        // NULLs deliberately do not adopt: their declared type participates in
        // operator lookup and can make the pair invalid.
        if matches!(left, Expr::NullLiteral) {
            return (rt, rt);
        }
        if matches!(right, Expr::NullLiteral) {
            return (lt, lt);
        }
        if matches!(right, Expr::StringLiteral(_)) && lt.array_element().is_some() {
            return (lt, lt);
        }
        if matches!(left, Expr::StringLiteral(_)) && rt.array_element().is_some() {
            return (rt, rt);
        }
        if matches!(right, Expr::StringLiteral(_)) && matches!(lt, ColumnType::Range(_)) {
            return (lt, lt);
        }
        if matches!(left, Expr::StringLiteral(_)) && matches!(rt, ColumnType::Range(_)) {
            return (rt, rt);
        }
        if let ColumnType::Multirange(_) = lt
            && matches!(right, Expr::StringLiteral(_))
        {
            return (lt, lt);
        }
        if let ColumnType::Multirange(_) = rt
            && matches!(left, Expr::StringLiteral(_))
        {
            return (rt, rt);
        }
    }
    // `json` resolves a literal for its two PATH operators only. Its `->` and
    // `->>` take `text` (which the literal already is) or `integer`, and it has
    // no `@>`, `?|`, `?&`, `@?` or `@@` for a literal to adopt a type for — so
    // the literal stays `text` and the operator does not resolve, which is
    // exactly PostgreSQL's outcome.
    if matches!(right, Expr::StringLiteral(_)) && lt == ColumnType::Json {
        let expected = match op {
            BinaryOp::JsonGetPath | BinaryOp::JsonGetPathText => {
                ColumnType::array_of(ColumnType::Text)
            }
            _ => None,
        };
        return (lt, expected.unwrap_or(rt));
    }
    if !matches!(right, Expr::StringLiteral(_)) || lt != ColumnType::Jsonb {
        return adopt_string_literal_type(left, right, lt, rt);
    }
    let expected = match op {
        BinaryOp::JsonGetPath
        | BinaryOp::JsonGetPathText
        | BinaryOp::KeyExistsAny
        | BinaryOp::KeyExistsAll => ColumnType::array_of(ColumnType::Text),
        BinaryOp::Contains | BinaryOp::ContainedBy => Some(ColumnType::Jsonb),
        BinaryOp::JsonPathExists | BinaryOp::JsonPathMatch => Some(ColumnType::JsonPath),
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
    /// a bare `NULL` as `text` immediately, so this code must ask the question
    /// syntactically.
    Unknown,
    /// An argument that carries a type of its own.
    Known(ColumnType),
    /// An argument that carries a type, but not one visible here. It is a
    /// run-time SQL NULL. Like [`ArgType::Unknown`] it resolves no polymorphic
    /// parameter, but unlike it, it is never the *reason* one is unresolvable.
    /// `to_jsonb(j)` on a NULL row value is not `to_jsonb('a')`.
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

    /// Is this an `unknown` literal that waits for its parameter's type?
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

/// The same at RUN time.
///
/// An argument's type comes from its value, and a SQL NULL carries no type.
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
/// parameter's type.
///
/// A family computes its plan-time checks and its result type from these types.
/// A literal cannot adopt a `"any"` parameter, where PostgreSQL resolves
/// `unknown` to `text`, so that literal stays `text`.
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
/// parameter resolved to.
///
/// A type on the literal alone is not enough. The literal still evaluates to a
/// `Datum::Text`, while `jsonb_set`'s path parameter needs a `Datum::Array` and
/// its value parameter needs a `Datum::Jsonb`. A NULL value has nothing to
/// convert.
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
        *v = cast_value(v, ty, &ctx.time_zone)?;
    }
    Ok(())
}

/// Type-check a whole `CHECK` predicate the way `PostgreSQL`'s parse analysis
/// does when the constraint is created.
///
/// *Every* subexpression has to resolve, not only the ones whose result type
/// depends on their operands.
///
/// A query can leave an operator's operands to the values, so `infer_type`
/// reports `boolean` for a comparison and a numeric-tower type for arithmetic
/// without a check that the operands make sense. DDL cannot do that. The table
/// fixes the operand types, so an operator or function that does not resolve
/// for them has to fail the DDL instead of every later write to the table.
pub(crate) fn check_predicate_resolves(expr: &Expr, scope: &Scope) -> Result<(), ExecError> {
    let mut failure: Option<ExecError> = None;
    crate::grouping::visit_expr(expr, &mut |node| {
        if failure.is_some() {
            return;
        }
        // `ARRAY[]` carries no element type of its own — the enclosing cast
        // supplies it, and that cast node types fine — so it is the one node
        // that cannot be asked for a type on its own.
        if matches!(node, Expr::ArrayLiteral(items) if items.is_empty()) {
            return;
        }
        failure = comparison_mismatch(node, scope).or_else(|| infer_type(node, scope).err());
    });
    failure.map_or(Ok(()), Err)
}

/// The 42883 for a comparison whose operands belong to different families.
///
/// `infer_binary_type` types a comparison `boolean` without a look at its
/// operands. That is right for a query, where the values decide, but wrong for
/// DDL. A stored `CHECK` that compares `text` to `integer` would be accepted
/// and would then fail every write to the table. This function skips an
/// `unknown` literal on either side, because that literal adopts the other
/// operand's type.
fn comparison_mismatch(node: &Expr, scope: &Scope) -> Option<ExecError> {
    let Expr::Binary { op, left, right } = node else {
        return None;
    };
    if !matches!(
        op,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
    ) || is_unknown_literal(left)
        || is_unknown_literal(right)
    {
        return None;
    }
    let (Ok(lt), Ok(rt)) = (infer_type(left, scope), infer_type(right, scope)) else {
        return None;
    };
    let (lc, rc) = (comparison_category(lt)?, comparison_category(rt)?);
    (lc != rc).then(|| undefined_operator(op_spelling(*op), lt, rt))
}

/// The families whose members compare with one another.
///
/// Two types in *different* families have no comparison operator in
/// `PostgreSQL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ComparisonFamily {
    Number,
    String,
    Date,
    TimeOfDay,
    Boolean,
    Json,
}

/// The comparison family a type belongs to, or `None` when crabka has no
/// confident answer.
///
/// The values then decide the comparison.
fn comparison_category(ty: ColumnType) -> Option<ComparisonFamily> {
    match ty {
        ColumnType::Int2
        | ColumnType::Int4
        | ColumnType::Int8
        | ColumnType::Float4
        | ColumnType::Float8 => Some(ComparisonFamily::Number),
        _ if ty.is_numeric() => Some(ComparisonFamily::Number),
        _ if ty.is_string() => Some(ComparisonFamily::String),
        ColumnType::Date | ColumnType::Timestamp | ColumnType::Timestamptz => {
            Some(ComparisonFamily::Date)
        }
        ColumnType::Time | ColumnType::Timetz => Some(ComparisonFamily::TimeOfDay),
        ColumnType::Bool => Some(ComparisonFamily::Boolean),
        ColumnType::Jsonb => Some(ComparisonFamily::Json),
        // `json` is deliberately absent — it is not a *family* of one, it has
        // no comparison operator at all. Putting it here would say
        // `json = jsonb` resolves; [`reject_uncomparable_comparison`] turns
        // every comparison over it away before this is consulted.
        _ => None,
    }
}

/// Is `e` a literal PostgreSQL would still call `unknown`?
pub(crate) fn is_unknown_literal(e: &Expr) -> bool {
    matches!(e, Expr::StringLiteral(_) | Expr::NullLiteral)
}

/// The operand types an operator resolves against, with a bare `unknown`
/// literal's placeholder `text` replaced by its sibling's type. This is what
/// keeps `1 + '1'` an `integer` sum rather than widening through an unresolved
/// `text` operand. Two bare literals resolve each other to nothing, so the pair
/// is returned unchanged and the caller's own `unknown`/`text` rule applies.
fn adopt_unknown_literal_types(
    left: &Expr,
    right: &Expr,
    lt: ColumnType,
    rt: ColumnType,
) -> (ColumnType, ColumnType) {
    match (is_unknown_literal(left), is_unknown_literal(right)) {
        (true, false) => (rt, rt),
        (false, true) => (lt, lt),
        _ => (lt, rt),
    }
}

/// PostgreSQL's 42804 for a polymorphic parameter (`anyarray`, `anyelement`)
/// that nothing in the call resolves, because every argument that could have is
/// an `unknown` literal — `cardinality('{1,2}')`, `to_jsonb('a')`.
pub(crate) fn undetermined_polymorphic_type() -> ExecError {
    ExecError::TypeMismatch(
        "could not determine polymorphic type because input has type unknown".into(),
    )
}

/// The `||` operator for a type pair, and its result type.
///
/// This function resolves arrays before the text fallback, so `text[] || text`
/// appends instead of stringifies. PostgreSQL's `anyarray || anyelement`
/// outranks `anynonarray || text` there.
fn concat_kind(lt: ColumnType, rt: ColumnType) -> Option<(ConcatKind, ColumnType)> {
    if lt == ColumnType::TsVector && rt == ColumnType::TsVector {
        return Some((ConcatKind::TsVector, ColumnType::TsVector));
    }
    if lt == ColumnType::TsQuery && rt == ColumnType::TsQuery {
        return Some((ConcatKind::TsQuery, ColumnType::TsQuery));
    }
    if lt == ColumnType::Jsonb && rt == ColumnType::Jsonb {
        return Some((ConcatKind::Jsonb, ColumnType::Jsonb));
    }
    // `bitcat` must be reached before the text fallback below: `bit || bit`
    // has no text operand at all, and a bit string beside an `unknown` literal
    // resolves to `bitcat` rather than to string concatenation.
    if crate::bit_fn::is_bit_type(lt) && crate::bit_fn::is_bit_type(rt) {
        return Some((ConcatKind::BitString, ColumnType::VarBit(None)));
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
        ConcatKind::BitString => match (l, r) {
            (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
            _ => crate::bit_fn::apply_bit_operator(BinaryOp::Concat, l, r)?
                .ok_or_else(|| undefined_operator_for(BinaryOp::Concat, l, r)),
        },
        ConcatKind::Text => Ok(ops::concat(l, r, ctx.output_style())?),
        ConcatKind::Jsonb => json_fn::eval_json_operator(JsonOp::Concat, l, r),
        ConcatKind::Array(form) => array_fn::array_concat(form, l, r, ctx),
        ConcatKind::TsVector => match (l, r) {
            (Datum::TsVector(left), Datum::TsVector(right)) => {
                Ok(Datum::TsVector(left.concat(right)))
            }
            (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
            _ => Err(undefined_operator_for(BinaryOp::Concat, l, r)),
        },
        ConcatKind::TsQuery => match (l, r) {
            (Datum::TsQuery(left), Datum::TsQuery(right)) => Ok(Datum::TsQuery(
                crabka_pgtypes::TsQuery::Or(Box::new(left.clone()), Box::new(right.clone())),
            )),
            (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
            _ => Err(undefined_operator_for(BinaryOp::Concat, l, r)),
        },
    }
}

fn apply_text_search_match(l: &Datum, r: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    if l.is_null() || r.is_null() {
        return Ok(Datum::Null);
    }
    let matches = match (l, r) {
        (Datum::TsVector(vector), Datum::TsQuery(query))
        | (Datum::TsQuery(query), Datum::TsVector(vector)) => vector.matches(query),
        (Datum::Text(text), Datum::TsQuery(query)) | (Datum::TsQuery(query), Datum::Text(text)) => {
            let config =
                crate::session::current_setting_runtime("default_text_search_config", false)?
                    .expect("registered GUC has a value");
            crate::text_search_fn::to_tsvector(&config, text, ctx.catalog())?.matches(query)
        }
        (Datum::Text(document), Datum::Text(query)) => {
            let config =
                crate::session::current_setting_runtime("default_text_search_config", false)?
                    .expect("registered GUC has a value");
            let vector = crate::text_search_fn::to_tsvector(&config, document, ctx.catalog())?;
            let query = crate::text_search_fn::plain_query(&config, query, false, ctx.catalog())?;
            vector.matches(&query)
        }
        _ => return Err(undefined_operator_for(BinaryOp::JsonPathMatch, l, r)),
    };
    Ok(Datum::Bool(matches))
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
        BinaryOp::JsonPathExists => JsonOp::PathExists,
        BinaryOp::JsonPathMatch => JsonOp::PathMatch,
        _ => return None,
    })
}

/// One of `PostgreSQL`'s seven geometric types, as an operator-resolution key.
///
/// `pg_operator` declares the geometric operators pair by pair — `box <-> lseg`
/// exists and `box <-> circle` does not — so resolving one means naming both
/// operand types exactly, not asking whether each is "geometric enough".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeoType {
    Point,
    Box,
    Circle,
    Line,
    Lseg,
    Path,
    Polygon,
}

impl GeoType {
    fn of_column(ty: ColumnType) -> Option<Self> {
        Some(match ty {
            ColumnType::Point => Self::Point,
            ColumnType::Box => Self::Box,
            ColumnType::Circle => Self::Circle,
            ColumnType::Line => Self::Line,
            ColumnType::Lseg => Self::Lseg,
            ColumnType::Path => Self::Path,
            ColumnType::Polygon => Self::Polygon,
            _ => return None,
        })
    }

    fn of_datum(value: &Datum) -> Option<Self> {
        Some(match value {
            Datum::Point(_) => Self::Point,
            Datum::Box(_) => Self::Box,
            Datum::Circle(_) => Self::Circle,
            Datum::Line(_) => Self::Line,
            Datum::Lseg(_) => Self::Lseg,
            Datum::Path(_) => Self::Path,
            Datum::Polygon(_) => Self::Polygon,
            _ => return None,
        })
    }
}

/// Is this one of the seven geometric types?
fn is_geometric_type(ty: ColumnType) -> bool {
    GeoType::of_column(ty).is_some()
}

/// Is this value one of the seven geometric types? A single discriminant test,
/// and the guard [`apply_binary`] inlines at its own call site so a row with no
/// geometric operand never enters (or even calls into) the geometric dispatch.
#[inline]
fn is_geometric_datum(value: &Datum) -> bool {
    matches!(
        value,
        Datum::Point(_)
            | Datum::Box(_)
            | Datum::Circle(_)
            | Datum::Line(_)
            | Datum::Lseg(_)
            | Datum::Path(_)
            | Datum::Polygon(_)
    )
}

/// The operators `PostgreSQL` gives a geometric overload. `=`, `<>` and the
/// four orderings are deliberately absent: they are declared type by type over
/// a *different* table and answered by [`apply_geometric_comparison`].
fn is_geometric_operator(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::BitXor
            | BinaryOp::ClosestPoint
            | BinaryOp::Overlaps
            | BinaryOp::DoesNotExtendRight
            | BinaryOp::DoesNotExtendLeft
            | BinaryOp::DoesNotExtendAbove
            | BinaryOp::DoesNotExtendBelow
            | BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Phrase
            | BinaryOp::Shl
            | BinaryOp::Shr
            | BinaryOp::StrictlyBelow
            | BinaryOp::StrictlyAbove
            | BinaryOp::Contains
            | BinaryOp::ContainedBy
            | BinaryOp::BelowEq
            | BinaryOp::AboveEq
            | BinaryOp::Intersects
            | BinaryOp::Horizontal
            | BinaryOp::KeyExistsAny
            | BinaryOp::Perpendicular
            | BinaryOp::Parallel
            | BinaryOp::Same
    )
}

/// The type an `unknown` literal adopts beside a geometric operand, or `None`
/// when nothing resolves it. `PostgreSQL` runs full operator resolution here;
/// this reproduces the cases where exactly one candidate survives, so
/// `p.f1 << '(0,0)'` picks `point << point` and `b.f1 + '(1,2)'` picks
/// `box + point` rather than a second box.
fn geometric_literal_type(op: BinaryOp, sibling: ColumnType) -> Option<ColumnType> {
    let geo = GeoType::of_column(sibling)?;
    Some(match op {
        // `path + path` is `path_add`, an exact match beside a path; every
        // other geometric arithmetic operator's only partner is a point.
        BinaryOp::Add if geo == GeoType::Path => ColumnType::Path,
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => ColumnType::Point,
        // The same-type operators, where the sibling's own type is the only
        // candidate — plus the comparisons, which are same-type throughout.
        BinaryOp::Shl
        | BinaryOp::Shr
        | BinaryOp::StrictlyBelow
        | BinaryOp::StrictlyAbove
        | BinaryOp::DoesNotExtendRight
        | BinaryOp::DoesNotExtendLeft
        | BinaryOp::DoesNotExtendAbove
        | BinaryOp::DoesNotExtendBelow
        | BinaryOp::Overlaps
        | BinaryOp::Same
        | BinaryOp::BitXor
        | BinaryOp::Intersects
        | BinaryOp::Horizontal
        | BinaryOp::KeyExistsAny
        | BinaryOp::Perpendicular
        | BinaryOp::Parallel
        | BinaryOp::BelowEq
        | BinaryOp::AboveEq
        | BinaryOp::Contains
        | BinaryOp::ContainedBy
        | BinaryOp::Phrase
        | BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Le
        | BinaryOp::Gt
        | BinaryOp::Ge => sibling,
        // `##` has two candidates beside every operand it takes, and
        // PostgreSQL calls that ambiguous rather than picking one.
        _ => return None,
    })
}

/// The result type when a geometric operator has one bare `unknown` literal.
/// PostgreSQL resolves the literal from its sibling, so `p.f1 << '(0,0)'` picks
/// the `point` operator; the literal's *value* is converted to the same type
/// later, by `coerce_untyped_literal_operands`.
fn geometric_literal_operator(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    scope: &Scope,
) -> Result<Option<ColumnType>, ExecError> {
    let unknown_left = is_unknown_literal(left);
    if unknown_left == is_unknown_literal(right) {
        return Ok(None);
    }
    let sibling = if unknown_left {
        infer_type(right, scope)?
    } else {
        infer_type(left, scope)?
    };
    let Some(literal) = geometric_literal_type(op, sibling) else {
        return Ok(None);
    };
    let (lt, rt) = if unknown_left {
        (literal, sibling)
    } else {
        (sibling, literal)
    };
    Ok(geometric_operator_result_type(op, lt, rt))
}

/// The static result type of a geometric operator over the two operand types,
/// or `None` when `pg_operator` declares no such pair. This is the plan-time
/// twin of [`apply_geometric_operator`]; a test cross-checks that the two agree
/// on every one of the 7×7 pairs for every operator.
fn geometric_operator_result_type(
    op: BinaryOp,
    lt: ColumnType,
    rt: ColumnType,
) -> Option<ColumnType> {
    let (left, right) = (GeoType::of_column(lt)?, GeoType::of_column(rt)?);
    geometric_pair_result(op, left, right)
}

/// `Some(result)` — or `Some(Err)` for an undeclared pair — when at least one
/// operand of a geometric-capable operator is geometric, so the operator can
/// only be the geometric one. `None` leaves the operator's other overloads
/// (bitwise, network, range, jsonb, …) to the caller.
fn geometric_operand_result(
    op: BinaryOp,
    lt: ColumnType,
    rt: ColumnType,
) -> Option<Result<ColumnType, ExecError>> {
    if !is_geometric_operator(op) || !(is_geometric_type(lt) || is_geometric_type(rt)) {
        return None;
    }
    Some(
        geometric_operator_result_type(op, lt, rt)
            .ok_or_else(|| undefined_operator(op_spelling(op), lt, rt)),
    )
}

/// The declared (operator, left, right) matrix, as `pg_operator` records it.
fn geometric_pair_result(op: BinaryOp, left: GeoType, right: GeoType) -> Option<ColumnType> {
    use GeoType::{Box, Circle, Line, Lseg, Path, Point, Polygon};
    let pair = (left, right);
    match op {
        // `#` — `box_intersect`, `line_interpt` and `lseg_interpt`.
        BinaryOp::BitXor => match pair {
            (Box, Box) => Some(ColumnType::Box),
            (Line, Line) | (Lseg, Lseg) => Some(ColumnType::Point),
            _ => None,
        },
        // `##` — the point of the RIGHT operand closest to the left one.
        BinaryOp::ClosestPoint => matches!(
            pair,
            (Line, Lseg)
                | (Lseg, Box)
                | (Lseg, Lseg)
                | (Point, Box)
                | (Point, Line)
                | (Point, Lseg)
        )
        .then_some(ColumnType::Point),
        // `&&`, `&<`, `&>`, `&<|` and `|&>` — the five tests `point` has no
        // spelling of.
        BinaryOp::Overlaps
        | BinaryOp::DoesNotExtendRight
        | BinaryOp::DoesNotExtendLeft
        | BinaryOp::DoesNotExtendAbove
        | BinaryOp::DoesNotExtendBelow => {
            matches!(pair, (Box, Box) | (Circle, Circle) | (Polygon, Polygon))
                .then_some(ColumnType::Bool)
        }
        // `<<`, `>>`, `<<|`, `|>>` and `~=` — the same four families plus
        // `point`.
        BinaryOp::Shl
        | BinaryOp::Shr
        | BinaryOp::StrictlyBelow
        | BinaryOp::StrictlyAbove
        | BinaryOp::Same => matches!(
            pair,
            (Box, Box) | (Circle, Circle) | (Point, Point) | (Polygon, Polygon)
        )
        .then_some(ColumnType::Bool),
        // `<^` / `>^` — `box_below_eq` and `point_below`, which despite the
        // shared spelling are not the same relation.
        BinaryOp::BelowEq | BinaryOp::AboveEq => {
            matches!(pair, (Box, Box) | (Point, Point)).then_some(ColumnType::Bool)
        }
        BinaryOp::Contains => matches!(
            pair,
            (Box, Box)
                | (Box, Point)
                | (Circle, Circle)
                | (Circle, Point)
                | (Path, Point)
                | (Polygon, Point)
                | (Polygon, Polygon)
        )
        .then_some(ColumnType::Bool),
        // `<@` has four pairs `@>` has no commutator for — `lseg <@ box`,
        // `lseg <@ line`, `point <@ line` and `point <@ lseg`.
        BinaryOp::ContainedBy => matches!(
            pair,
            (Box, Box)
                | (Circle, Circle)
                | (Lseg, Box)
                | (Lseg, Line)
                | (Point, Box)
                | (Point, Circle)
                | (Point, Line)
                | (Point, Lseg)
                | (Point, Path)
                | (Point, Polygon)
                | (Polygon, Polygon)
        )
        .then_some(ColumnType::Bool),
        BinaryOp::Intersects => matches!(
            pair,
            (Box, Box)
                | (Line, Box)
                | (Line, Line)
                | (Lseg, Box)
                | (Lseg, Line)
                | (Lseg, Lseg)
                | (Path, Path)
        )
        .then_some(ColumnType::Bool),
        // `?-` and `?|` between two points ask whether they share a horizontal
        // or a vertical; the same spellings are the prefix tests on a line.
        BinaryOp::Horizontal | BinaryOp::KeyExistsAny => {
            matches!(pair, (Point, Point)).then_some(ColumnType::Bool)
        }
        BinaryOp::Perpendicular | BinaryOp::Parallel => {
            matches!(pair, (Line, Line) | (Lseg, Lseg)).then_some(ColumnType::Bool)
        }
        BinaryOp::Phrase => geometric_distance_pair(left, right).then_some(ColumnType::Float8),
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            geometric_arithmetic_result(op, left, right)
        }
        _ => None,
    }
}

/// The 24 pairs `<->` is declared over. Every other pair — `box <-> circle`
/// and `point <-> path`'s missing siblings among them — is 42883, NOT the
/// bounding-box distance that would otherwise answer them.
fn geometric_distance_pair(left: GeoType, right: GeoType) -> bool {
    use GeoType::{Box, Circle, Line, Lseg, Path, Point, Polygon};
    matches!(
        (left, right),
        (Box, Box)
            | (Box, Lseg)
            | (Box, Point)
            | (Circle, Circle)
            | (Circle, Point)
            | (Circle, Polygon)
            | (Line, Line)
            | (Line, Lseg)
            | (Line, Point)
            | (Lseg, Box)
            | (Lseg, Line)
            | (Lseg, Lseg)
            | (Lseg, Point)
            | (Path, Path)
            | (Path, Point)
            | (Point, Box)
            | (Point, Circle)
            | (Point, Line)
            | (Point, Lseg)
            | (Point, Path)
            | (Point, Point)
            | (Point, Polygon)
            | (Polygon, Circle)
            | (Polygon, Point)
            | (Polygon, Polygon)
    )
}

/// `+`, `-`, `*` and `/`. Every one translates (or rotates and scales) a shape
/// by a POINT and keeps the shape's type; `path + path` alone concatenates.
fn geometric_arithmetic_result(op: BinaryOp, left: GeoType, right: GeoType) -> Option<ColumnType> {
    if op == BinaryOp::Add && (left, right) == (GeoType::Path, GeoType::Path) {
        return Some(ColumnType::Path);
    }
    if right != GeoType::Point {
        return None;
    }
    Some(match left {
        GeoType::Box => ColumnType::Box,
        GeoType::Circle => ColumnType::Circle,
        GeoType::Path => ColumnType::Path,
        GeoType::Point => ColumnType::Point,
        // `lseg`, `line` and `polygon` have no arithmetic operator at all.
        GeoType::Lseg | GeoType::Line | GeoType::Polygon => return None,
    })
}

/// The bounding box a geometric operand compares as for the eight DIRECTIONAL
/// positional tests, which is the one place `PostgreSQL` genuinely reduces to
/// one: `box_left`, `circle_left` and `poly_left` are each written as a
/// comparison of the operands' extents. A point bounds to itself.
fn bounding_box(value: &Datum) -> Option<crabka_pgtypes::geometry::Box2> {
    use crabka_pgtypes::geometry::Box2;
    match value {
        Datum::Point(point) => Some(Box2::of_point(*point)),
        Datum::Box(value) => Some(*value),
        Datum::Circle(circle) => Some(Box2::of_circle(*circle)),
        Datum::Polygon(polygon) => Some(polygon.bounding_box()),
        _ => None,
    }
}

/// Every geometric operator except the comparisons, resolved EXACTLY by the
/// (operator, left type, right type) triple `pg_operator` declares.
///
/// Returns `None` when neither operand is geometric, leaving the operator's
/// bitwise / network / range / jsonb / text-search overloads to the caller —
/// this runs early in [`apply_binary`], so it must claim nothing that is not
/// geometric. A geometric operand with an undeclared partner is 42883 rather
/// than a bounding-box approximation, and a NULL operand is SQL NULL.
fn apply_geometric_operator(
    op: BinaryOp,
    left: &Datum,
    right: &Datum,
) -> Option<Result<Datum, ExecError>> {
    if !is_geometric_operator(op) || !(is_geometric_datum(left) || is_geometric_datum(right)) {
        return None;
    }
    // Every geometric operator is strict.
    if left.is_null() || right.is_null() {
        return Some(Ok(Datum::Null));
    }
    Some(
        geometric_predicate(op, left, right)
            .map(|held| Ok(Datum::Bool(held)))
            .or_else(|| geometric_distance(op, left, right))
            .or_else(|| geometric_construction(op, left, right).map(Ok))
            .or_else(|| geometric_arithmetic(op, left, right))
            .unwrap_or_else(|| Err(undefined_operator_for(op, left, right))),
    )
}

/// The boolean-valued geometric operators.
fn geometric_predicate(op: BinaryOp, left: &Datum, right: &Datum) -> Option<bool> {
    geometric_positional(op, left, right)
        .or_else(|| geometric_containment(op, left, right))
        .or_else(|| geometric_relation(op, left, right))
}

/// `~=`, `&&`, `<^`, `>^` and the eight directional tests.
fn geometric_positional(op: BinaryOp, left: &Datum, right: &Datum) -> Option<bool> {
    match (op, left, right) {
        // `~=` is structural for all four families it is declared over —
        // `box_same` compares corners, not the areas `box_eq` compares.
        (BinaryOp::Same, Datum::Box(a), Datum::Box(b)) => Some(a.same(*b)),
        (BinaryOp::Same, Datum::Circle(a), Datum::Circle(b)) => Some(a.same(*b)),
        (BinaryOp::Same, Datum::Point(a), Datum::Point(b)) => Some(a.eq_point(*b)),
        (BinaryOp::Same, Datum::Polygon(a), Datum::Polygon(b)) => Some(a.same(b)),
        // `&&` is a true overlap test, not a bounding-box one: `circle_overlap`
        // measures centre distance against the radii.
        (BinaryOp::Overlaps, Datum::Box(a), Datum::Box(b)) => Some(a.overlaps(*b)),
        (BinaryOp::Overlaps, Datum::Circle(a), Datum::Circle(b)) => Some(a.overlaps(*b)),
        (BinaryOp::Overlaps, Datum::Polygon(a), Datum::Polygon(b)) => Some(a.overlaps(b)),
        // `<^` / `>^` are `box_below_eq` for boxes and the STRICT `point_below`
        // for points, which is why they cannot share the box reduction.
        (BinaryOp::BelowEq, Datum::Box(a), Datum::Box(b)) => Some(a.below_or_equal(*b)),
        (BinaryOp::BelowEq, Datum::Point(a), Datum::Point(b)) => Some(a.is_below(*b)),
        (BinaryOp::AboveEq, Datum::Box(a), Datum::Box(b)) => Some(a.above_or_equal(*b)),
        (BinaryOp::AboveEq, Datum::Point(a), Datum::Point(b)) => Some(a.is_above(*b)),
        _ => geometric_directional(op, left, right),
    }
}

/// `<<`, `>>`, `<<|`, `|>>`, `&<`, `&>`, `&<|` and `|&>`.
///
/// `point` has only the four STRICT spellings; the four "does not extend"
/// tests are declared for `box`, `circle` and `polygon` alone.
fn geometric_directional(op: BinaryOp, left: &Datum, right: &Datum) -> Option<bool> {
    let strict = matches!(
        op,
        BinaryOp::Shl | BinaryOp::Shr | BinaryOp::StrictlyBelow | BinaryOp::StrictlyAbove
    );
    let declared = match (left, right) {
        (Datum::Box(_), Datum::Box(_))
        | (Datum::Circle(_), Datum::Circle(_))
        | (Datum::Polygon(_), Datum::Polygon(_)) => true,
        (Datum::Point(_), Datum::Point(_)) => strict,
        _ => false,
    };
    if !declared {
        return None;
    }
    let (a, b) = (bounding_box(left)?, bounding_box(right)?);
    Some(match op {
        BinaryOp::Shl => a.strictly_left_of(b),
        BinaryOp::Shr => a.strictly_right_of(b),
        BinaryOp::StrictlyBelow => a.strictly_below(b),
        BinaryOp::StrictlyAbove => a.strictly_above(b),
        BinaryOp::DoesNotExtendRight => a.does_not_extend_right(b),
        BinaryOp::DoesNotExtendLeft => a.does_not_extend_left(b),
        BinaryOp::DoesNotExtendAbove => a.does_not_extend_above(b),
        BinaryOp::DoesNotExtendBelow => a.does_not_extend_below(b),
        _ => return None,
    })
}

/// `@>` and `<@`. The two are NOT mirror images: `PostgreSQL` declares
/// `lseg <@ box`, `lseg <@ line`, `point <@ line` and `point <@ lseg` with no
/// commutator, so `box @> lseg` is 42883.
fn geometric_containment(op: BinaryOp, left: &Datum, right: &Datum) -> Option<bool> {
    let contained_by = match op {
        BinaryOp::Contains => false,
        BinaryOp::ContainedBy => true,
        _ => return None,
    };
    let (outer, inner) = if contained_by {
        (right, left)
    } else {
        (left, right)
    };
    Some(match (outer, inner) {
        (Datum::Box(a), Datum::Box(b)) => a.contains(*b),
        (Datum::Box(a), Datum::Point(p)) => a.contains_point(*p),
        (Datum::Circle(a), Datum::Circle(b)) => a.contains(*b),
        (Datum::Circle(a), Datum::Point(p)) => a.contains_point(*p),
        (Datum::Path(a), Datum::Point(p)) => a.contains_point(*p),
        (Datum::Polygon(a), Datum::Point(p)) => a.contains_point(*p),
        (Datum::Polygon(a), Datum::Polygon(b)) => a.contains_polygon(b),
        (Datum::Box(a), Datum::Lseg(s)) if contained_by => a.contains_lseg(*s),
        (Datum::Line(a), Datum::Lseg(s)) if contained_by => a.contains_lseg(*s),
        (Datum::Line(a), Datum::Point(p)) if contained_by => a.contains_point(*p),
        (Datum::Lseg(a), Datum::Point(p)) if contained_by => a.contains_point(*p),
        _ => return None,
    })
}

/// `?#`, `?-`, `?|`, `?-|` and `?||`.
fn geometric_relation(op: BinaryOp, left: &Datum, right: &Datum) -> Option<bool> {
    match (op, left, right) {
        (BinaryOp::Intersects, Datum::Box(a), Datum::Box(b)) => Some(a.overlaps(*b)),
        (BinaryOp::Intersects, Datum::Line(a), Datum::Box(b)) => Some(a.intersects_box(*b)),
        (BinaryOp::Intersects, Datum::Line(a), Datum::Line(b)) => Some(a.intersects(*b)),
        (BinaryOp::Intersects, Datum::Lseg(s), Datum::Box(b)) => Some(b.intersects_lseg(*s)),
        (BinaryOp::Intersects, Datum::Lseg(s), Datum::Line(a)) => Some(s.intersects_line(*a)),
        (BinaryOp::Intersects, Datum::Lseg(a), Datum::Lseg(b)) => Some(a.intersects(*b)),
        (BinaryOp::Intersects, Datum::Path(a), Datum::Path(b)) => Some(a.intersects(b)),
        (BinaryOp::Horizontal, Datum::Point(a), Datum::Point(b)) => Some(a.is_horizontal_with(*b)),
        (BinaryOp::KeyExistsAny, Datum::Point(a), Datum::Point(b)) => Some(a.is_vertical_with(*b)),
        (BinaryOp::Perpendicular, Datum::Line(a), Datum::Line(b)) => {
            Some(a.is_perpendicular_to(*b))
        }
        (BinaryOp::Perpendicular, Datum::Lseg(a), Datum::Lseg(b)) => {
            Some(a.is_perpendicular_to(*b))
        }
        (BinaryOp::Parallel, Datum::Line(a), Datum::Line(b)) => Some(a.is_parallel_to(*b)),
        (BinaryOp::Parallel, Datum::Lseg(a), Datum::Lseg(b)) => Some(a.is_parallel_to(*b)),
        _ => None,
    }
}

/// `<->` over the 24 pairs `pg_operator` declares. `path <-> path` and
/// `polygon <-> polygon` are the two that can be NULL (an empty operand).
fn geometric_distance(
    op: BinaryOp,
    left: &Datum,
    right: &Datum,
) -> Option<Result<Datum, ExecError>> {
    if op != BinaryOp::Phrase {
        return None;
    }
    let float = |value: f64| Some(Ok(Datum::Float8(value)));
    let nullable = |value: Option<f64>| Some(Ok(value.map_or(Datum::Null, Datum::Float8)));
    match (left, right) {
        (Datum::Box(a), Datum::Box(b)) => float(a.distance(*b)),
        (Datum::Box(a), Datum::Lseg(s)) | (Datum::Lseg(s), Datum::Box(a)) => {
            float(a.distance_to_lseg(*s))
        }
        (Datum::Box(a), Datum::Point(p)) | (Datum::Point(p), Datum::Box(a)) => {
            float(a.distance_to_point(*p))
        }
        (Datum::Circle(a), Datum::Circle(b)) => float(a.distance(*b)),
        (Datum::Circle(a), Datum::Point(p)) | (Datum::Point(p), Datum::Circle(a)) => {
            float(a.distance_to_point(*p))
        }
        (Datum::Circle(a), Datum::Polygon(g)) | (Datum::Polygon(g), Datum::Circle(a)) => {
            float(a.distance_to_polygon(g))
        }
        (Datum::Line(a), Datum::Line(b)) => float(a.distance(*b)),
        (Datum::Line(a), Datum::Lseg(s)) | (Datum::Lseg(s), Datum::Line(a)) => {
            float(a.distance_to_lseg(*s))
        }
        (Datum::Line(a), Datum::Point(p)) | (Datum::Point(p), Datum::Line(a)) => {
            float(a.distance_to_point(*p))
        }
        (Datum::Lseg(a), Datum::Lseg(b)) => float(a.distance(*b)),
        (Datum::Lseg(s), Datum::Point(p)) | (Datum::Point(p), Datum::Lseg(s)) => {
            float(s.distance_to_point(*p))
        }
        (Datum::Path(a), Datum::Path(b)) => nullable(a.distance(b)),
        (Datum::Path(a), Datum::Point(p)) | (Datum::Point(p), Datum::Path(a)) => {
            float(a.distance_to_point(*p))
        }
        (Datum::Point(a), Datum::Point(b)) => float(a.distance(*b)),
        (Datum::Point(p), Datum::Polygon(g)) | (Datum::Polygon(g), Datum::Point(p)) => {
            float(g.distance_to_point(*p))
        }
        (Datum::Polygon(a), Datum::Polygon(b)) => nullable(a.distance(b)),
        _ => None,
    }
}

/// `#` (the intersection of two boxes, lines or segments) and `##` (the point
/// on the RIGHT operand closest to the left one). Both are NULL wherever the
/// construction is undefined — disjoint boxes, parallel lines.
fn geometric_construction(op: BinaryOp, left: &Datum, right: &Datum) -> Option<Datum> {
    let point = match (op, left, right) {
        (BinaryOp::BitXor, Datum::Box(a), Datum::Box(b)) => {
            return Some(a.intersection(*b).map_or(Datum::Null, Datum::Box));
        }
        (BinaryOp::BitXor, Datum::Line(a), Datum::Line(b)) => a.intersection_point(*b),
        (BinaryOp::BitXor, Datum::Lseg(a), Datum::Lseg(b)) => a.intersection_point(*b),
        (BinaryOp::ClosestPoint, Datum::Line(a), Datum::Lseg(s)) => a.closest_point_to_lseg(*s),
        (BinaryOp::ClosestPoint, Datum::Lseg(s), Datum::Box(b)) => b.closest_point_to_lseg(*s),
        (BinaryOp::ClosestPoint, Datum::Lseg(a), Datum::Lseg(b)) => a.closest_point_to_lseg(*b),
        (BinaryOp::ClosestPoint, Datum::Point(p), Datum::Box(b)) => b.closest_point_to(*p),
        (BinaryOp::ClosestPoint, Datum::Point(p), Datum::Line(a)) => a.closest_point_to(*p),
        (BinaryOp::ClosestPoint, Datum::Point(p), Datum::Lseg(s)) => s.closest_point_to(*p),
        _ => return None,
    };
    Some(point.map_or(Datum::Null, Datum::Point))
}

/// `+`, `-`, `*` and `/`. `geometry.rs` already reports 22003 (overflow /
/// underflow) and 22012 (division by zero) as a `TypeError`.
fn geometric_arithmetic(
    op: BinaryOp,
    left: &Datum,
    right: &Datum,
) -> Option<Result<Datum, ExecError>> {
    if !matches!(
        op,
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div
    ) {
        return None;
    }
    // `path + path` is `path_add`: plain concatenation, and NULL when either
    // operand is a closed path.
    if let (BinaryOp::Add, Datum::Path(a), Datum::Path(b)) = (op, left, right) {
        return Some(Ok(a.concat(b).map_or(Datum::Null, Datum::Path)));
    }
    let Datum::Point(point) = right else {
        return None;
    };
    let point = *point;
    let value = match left {
        Datum::Box(a) => match op {
            BinaryOp::Add => a.add_point(point),
            BinaryOp::Sub => a.sub_point(point),
            BinaryOp::Mul => a.mul_point(point),
            _ => a.div_point(point),
        }
        .map(Datum::Box),
        Datum::Circle(a) => match op {
            BinaryOp::Add => a.add_point(point),
            BinaryOp::Sub => a.sub_point(point),
            BinaryOp::Mul => a.mul_point(point),
            _ => a.div_point(point),
        }
        .map(Datum::Circle),
        Datum::Path(a) => match op {
            BinaryOp::Add => a.add_point(point),
            BinaryOp::Sub => a.sub_point(point),
            BinaryOp::Mul => a.mul_point(point),
            _ => a.div_point(point),
        }
        .map(Datum::Path),
        Datum::Point(a) => match op {
            BinaryOp::Add => a.add_point(point),
            BinaryOp::Sub => a.sub_point(point),
            BinaryOp::Mul => a.mul_point(point),
            _ => a.div_point(point),
        }
        .map(Datum::Point),
        _ => return None,
    };
    Some(value.map_err(ExecError::Type))
}

/// `=`, `<>` and the four orderings over a geometric operand.
///
/// `PostgreSQL` declares these one type at a time and inconsistently: `point`
/// has `<>` but no `=`, `box` has `=` but no `<>`, `polygon` has neither, and
/// `line` has `=` alone. Where the operator exists, what it MEANS also varies —
/// `box = box` compares areas while `lseg = lseg` compares endpoints. Returns
/// `None` when neither operand is geometric.
fn apply_geometric_comparison(
    op: BinaryOp,
    left: &Datum,
    right: &Datum,
) -> Option<Result<Datum, ExecError>> {
    if !is_geometric_datum(left) && !is_geometric_datum(right) {
        return None;
    }
    if left.is_null() || right.is_null() {
        return Some(Ok(Datum::Null));
    }
    if let Some(error) = geometric_comparison_rejection(op, left, right) {
        return Some(Err(error));
    }
    Some(match (op, left, right) {
        // `lseg_eq` / `lseg_ne` compare ENDPOINTS, where `lseg_lt` and friends
        // compare length — so equality cannot go through `ops::compare`.
        (BinaryOp::Eq, Datum::Lseg(a), Datum::Lseg(b)) => Ok(Datum::Bool(a.eq_lseg(*b))),
        (BinaryOp::Ne, Datum::Lseg(a), Datum::Lseg(b)) => Ok(Datum::Bool(a.ne_lseg(*b))),
        // `point` and `polygon` order nothing; `<>` is all `point` has.
        (BinaryOp::Ne, Datum::Point(a), Datum::Point(b)) => Ok(Datum::Bool(a.ne_point(*b))),
        // `circle_ne` is not the negation of `circle_eq` for a NaN area, so it
        // is answered by its own function rather than by inverting `=`.
        (BinaryOp::Ne, Datum::Circle(a), Datum::Circle(b)) => Ok(Datum::Bool(a.ne_circle(*b))),
        _ => ops::compare(left, right)
            .map(|ord| cmp_result(op, ord))
            .map_err(ExecError::from),
    })
}

/// The 42883 for a geometric comparison `pg_operator` does not declare, or
/// `None` when the pair is one it does. `IS [NOT] DISTINCT FROM` resolves
/// through the type's `=`, so its caller passes [`BinaryOp::Eq`].
fn geometric_comparison_rejection(op: BinaryOp, left: &Datum, right: &Datum) -> Option<ExecError> {
    // A NULL carries no type, so it resolves the operator from its sibling and
    // there is nothing to refuse: `box IS DISTINCT FROM NULL` is `t`.
    if left.is_null() || right.is_null() {
        return None;
    }
    if !is_geometric_datum(left) && !is_geometric_datum(right) {
        return None;
    }
    let declared = GeoType::of_datum(left)
        .zip(GeoType::of_datum(right))
        .is_some_and(|(a, b)| a == b && geometric_comparison_declared(op, a));
    (!declared).then(|| undefined_operator_for(op, left, right))
}

/// The btree surface `pg_operator` gives each geometric type, verified against
/// PostgreSQL 18.4:
///
/// | type | `=` | `<>` | `<` `<=` `>` `>=` |
/// |---|---|---|---|
/// | `point` | no | yes | no |
/// | `box` | yes | no | yes |
/// | `circle` | yes | yes | yes |
/// | `line` | yes | no | no |
/// | `lseg` | yes | yes | yes |
/// | `path` | yes | no | yes |
/// | `polygon` | no | no | no |
fn geometric_comparison_declared(op: BinaryOp, ty: GeoType) -> bool {
    use GeoType::{Box, Circle, Line, Lseg, Path, Point};
    match op {
        BinaryOp::Eq => matches!(ty, Box | Circle | Line | Lseg | Path),
        BinaryOp::Ne => matches!(ty, Circle | Lseg | Point),
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            matches!(ty, Box | Circle | Lseg | Path)
        }
        _ => false,
    }
}

/// A binary operator's SQL spelling, for error messages.
pub(crate) fn op_spelling(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Same => "~=",
        BinaryOp::DoesNotExtendAbove => "&<|",
        BinaryOp::DoesNotExtendBelow => "|&>",
        BinaryOp::StrictlyBelow => "<<|",
        BinaryOp::StrictlyAbove => "|>>",
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
        BinaryOp::JsonPathExists => "@?",
        BinaryOp::JsonPathMatch => "@@",
        BinaryOp::Overlaps => "&&",
        BinaryOp::DoesNotExtendRight => "&<",
        BinaryOp::DoesNotExtendLeft => "&>",
        BinaryOp::Adjacent => "-|-",
        BinaryOp::ClosestPoint => "##",
        BinaryOp::Intersects => "?#",
        BinaryOp::Horizontal => "?-",
        BinaryOp::Perpendicular => "?-|",
        BinaryOp::Parallel => "?||",
        BinaryOp::BelowEq => "<^",
        BinaryOp::AboveEq => ">^",
        BinaryOp::Phrase => "<->",
        BinaryOp::Match => "~",
        BinaryOp::MatchCi => "~*",
        BinaryOp::NotMatch => "!~",
        BinaryOp::NotMatchCi => "!~*",
        BinaryOp::BitAnd => "&",
        BinaryOp::BitOr => "|",
        BinaryOp::BitXor => "#",
        BinaryOp::Shl => "<<",
        BinaryOp::Shr => ">>",
        BinaryOp::ContainedByOrEq => "<<=",
        BinaryOp::ContainsOrEq => ">>=",
        BinaryOp::Pow => "^",
        BinaryOp::Mod => "%",
        BinaryOp::Eq => "=",
        BinaryOp::Ne => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::Le => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Ge => ">=",
        BinaryOp::IsDistinctFrom => "IS DISTINCT FROM",
        BinaryOp::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
    }
}

pub(crate) fn undefined_operator(op: &str, lt: ColumnType, rt: ColumnType) -> ExecError {
    undefined_operator_named(op, lt.name(), rt.name())
}

/// The same 42883 with the operand types already spelled. The `json` rejections
/// need it because PostgreSQL names an unresolved literal operand `unknown`
/// (`json = unknown`), and this codebase has no `ColumnType` for that.
fn undefined_operator_named(op: &str, lt: &str, rt: &str) -> ExecError {
    ExecError::UndefinedFunction(format!("operator does not exist: {lt} {op} {rt}"))
}

/// The 42883 for an operator `json` does not have.
///
/// PostgreSQL gives `json` exactly six operators — `->` and `->>` (by object
/// key and by array index) and `#>` / `#>>` — and nothing else: no `=`, no
/// ordering, no `@>`/`<@`, no `?`/`?|`/`?&`, no `||`, no `-`, no `@?`/`@@`.
/// Every one of those spellings is `jsonb`'s alone. Without this, a `json`
/// operand reaching the value-time families below would be *answered* rather
/// than rejected: `ops::compare` renders both sides as text and would order
/// `'{"a":1}'` against `'{"a": 1}'`, and `ops::sub` reports 42804 where
/// PostgreSQL reports 42883.
///
/// `||` is deliberately absent: `json || 'x'` is PostgreSQL's
/// `anynonarray || text`, which renders the document and really does apply.
fn json_operator_rejection(op: BinaryOp, l: &Datum, r: &Datum) -> Option<ExecError> {
    if !matches!(l, Datum::Json(_)) && !matches!(r, Datum::Json(_)) {
        return None;
    }
    let spelled = match op {
        // `IS DISTINCT FROM` is resolved through the type's `=` operator, so
        // that is the spelling PostgreSQL names when there is none.
        BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom => "=",
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Le
        | BinaryOp::Gt
        | BinaryOp::Ge
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::Mod
        | BinaryOp::Pow
        | BinaryOp::Contains
        | BinaryOp::ContainedBy
        | BinaryOp::Overlaps
        | BinaryOp::KeyExists
        | BinaryOp::KeyExistsAny
        | BinaryOp::KeyExistsAll
        | BinaryOp::JsonPathExists
        | BinaryOp::JsonPathMatch => op_spelling(op),
        _ => return None,
    };
    let name = |d: &Datum| d.column_type().map_or("unknown", ColumnType::name);
    Some(undefined_operator_named(spelled, name(l), name(r)))
}

fn ambiguous_operator(op: &str) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42725",
        message: format!("operator is not unique: unknown {op} unknown"),
    }
}

/// The types crabka supports that PostgreSQL gives no default btree operator
/// class — `jsonpath` and `json`, and the array type over each.
///
/// `json` is in the list for the reason the type exists at all: it stores the
/// document's original text, so two values that differ only in whitespace or
/// key order are the same document but not the same bytes, and PostgreSQL
/// refuses to say which of `GROUP BY`, `DISTINCT`, `ORDER BY`, `UNION` and the
/// quantified comparisons meant. `jsonb`, which normalizes on input, has the
/// opclass.
fn has_no_operator_class(ty: ColumnType) -> bool {
    matches!(
        ty.storage_type(),
        ColumnType::JsonPath
            | ColumnType::Array(ElemType::JsonPath)
            | ColumnType::Json
            | ColumnType::Array(ElemType::Json)
            | ColumnType::Xml
            | ColumnType::Array(ElemType::Xml)
    )
}

/// The types with a **hash** operator class but no **btree** one, so equality,
/// `GROUP BY`, `DISTINCT` and the set operations all work while `ORDER BY` and
/// a btree index do not.
///
/// `xid` and `cid` are the pair PostgreSQL puts here, and `xid.c` says why:
/// transaction ids compare with modular arithmetic, which does not respect the
/// triangle inequality, so there is no total order for a btree to sort by. The
/// consequence a corpus notices is that `SELECT DISTINCT x` over an `xid`
/// column succeeds and `ORDER BY x` is 42883.
fn has_no_btree_operator_class(ty: ColumnType) -> bool {
    has_no_operator_class(ty) || has_no_btree_opclass(ty)
}

/// The types with a hash opclass but no btree one — see
/// [`has_no_btree_operator_class`], which is this plus the types with no
/// operator class at all.
pub(crate) fn has_no_btree_opclass(ty: ColumnType) -> bool {
    matches!(ty.storage_type(), ColumnType::Xid | ColumnType::Cid)
}

pub(crate) fn is_scalar_jsonpath(ty: ColumnType) -> bool {
    ty.storage_type() == ColumnType::JsonPath
}

/// Is this `json` or `xml` itself (through any domain over either)? An ARRAY of
/// one is deliberately not: `json[] = json[]` resolves `array_eq` at parse
/// analysis and only fails when it looks for the ELEMENT operator, so its error
/// is `could not identify an equality operator for type json`, not
/// `operator does not exist`.
/// `json` and `xml` are the two types `PostgreSQL` declares with no comparison
/// operator of any kind, so every construct that reaches for one has to refuse
/// them both and name whichever it was given.
pub(crate) fn is_uncomparable_scalar(ty: ColumnType) -> bool {
    matches!(ty.storage_type(), ColumnType::Json | ColumnType::Xml)
}

pub(crate) fn require_runtime_equality(left: &Datum, right: &Datum) -> Result<(), ExecError> {
    // `point` and `polygon` have no `=` operator at all, and every construct
    // that expands into one — `IN (…)`, a simple `CASE`, a row comparison,
    // `IS DISTINCT FROM` — names the missing OPERATOR, not a missing opclass.
    // The operand test is inlined here because this is a per-row path.
    if (is_geometric_datum(left) || is_geometric_datum(right))
        && let Some(error) = geometric_comparison_rejection(BinaryOp::Eq, left, right)
    {
        return Err(error);
    }
    let needs_operator = match (left, right) {
        (Datum::JsonPath(_), Datum::JsonPath(_)) => true,
        (Datum::Array(left), Datum::Array(right))
            if left.elem == ElemType::JsonPath && right.elem == ElemType::JsonPath =>
        {
            left.dims == right.dims
        }
        _ => false,
    };
    if needs_operator {
        return Err(ExecError::UndefinedFunction(
            "could not identify an equality operator for type jsonpath".into(),
        ));
    }
    Ok(())
}

/// `array_eq` compares dimensions before looking up its element equality
/// operator. Differently-shaped jsonpath arrays are therefore simply unequal,
/// while equal-shaped ones reach the missing jsonpath operator.
pub(crate) fn runtime_equality_short_circuit(left: &Datum, right: &Datum) -> Option<bool> {
    match (left, right) {
        (Datum::Array(left), Datum::Array(right))
            if left.elem == ElemType::JsonPath
                && right.elem == ElemType::JsonPath
                && left.dims != right.dims =>
        {
            Some(false)
        }
        _ => None,
    }
}

pub(crate) fn require_runtime_comparison(left: &Datum, right: &Datum) -> Result<(), ExecError> {
    // The types with equality (or, for `point` and `polygon`, not even that)
    // but no ordering operator at all. `line` has `=`; `point` has only `<>`;
    // `polygon` has neither. `xid` and `cid` are the same situation outside the
    // geometric family — transaction ids compare modularly, which has no total
    // order. The four geometric types PostgreSQL DOES order (`box`, `circle`,
    // `lseg` and `path`) fall through, as does every other pair.
    //
    // The spelling is `<` because this is the ordering gate: the callers that
    // reach it are `ORDER BY`, `min`/`max`, array and row comparison, all of
    // which PostgreSQL resolves through the type's `<`. A written `>` or `<=`
    // over these types is named exactly by [`apply_geometric_comparison`],
    // which runs first.
    let unordered = match (left, right) {
        (Datum::Line(_), Datum::Line(_)) => Some("line"),
        (Datum::Point(_), Datum::Point(_)) => Some("point"),
        (Datum::Polygon(_), Datum::Polygon(_)) => Some("polygon"),
        (Datum::Xid(_), Datum::Xid(_)) => Some("xid"),
        (Datum::Cid(_), Datum::Cid(_)) => Some("cid"),
        _ => None,
    };
    if let Some(name) = unordered {
        return Err(ExecError::UndefinedFunction(format!(
            "operator does not exist: {name} < {name}"
        )));
    }
    if matches!((left, right), (Datum::JsonPath(_), Datum::JsonPath(_)))
        || matches!(
            (left, right),
            (Datum::Array(left), Datum::Array(right))
                if left.elem == ElemType::JsonPath && right.elem == ElemType::JsonPath
        )
    {
        return Err(ExecError::UndefinedFunction(
            "could not identify a comparison function for type jsonpath".into(),
        ));
    }
    Ok(())
}

/// Reject operations whose implementation requires a PostgreSQL equality
/// operator class. `Datum` still implements Rust equality for storage
/// invariants; SQL must not accidentally expose that internal relation.
pub(crate) fn require_equality_operator(ty: ColumnType) -> Result<(), ExecError> {
    if has_no_operator_class(ty) {
        return Err(ExecError::UndefinedFunction(format!(
            "could not identify an equality operator for type {}",
            ty.name()
        )));
    }
    Ok(())
}

/// Reject ORDER BY and other btree-dependent operations for the types with no
/// btree opclass ([`has_no_operator_class`]).
pub(crate) fn require_ordering_operator(ty: ColumnType) -> Result<(), ExecError> {
    if has_no_btree_operator_class(ty) {
        return Err(ExecError::UndefinedFunction(format!(
            "could not identify an ordering operator for type {}",
            ty.name()
        )));
    }
    Ok(())
}

/// Reject `greatest`/`least` over a type with no btree opclass.
///
/// PostgreSQL resolves these two through `btree`'s **comparison function**
/// rather than through an ordering operator, and names the missing thing
/// accordingly: `greatest('1'::xid, '2'::xid)` is `could not identify a
/// comparison function for type xid`, not the `ordering operator` wording
/// `ORDER BY` uses.
pub(crate) fn require_comparison_function(ty: ColumnType) -> Result<(), ExecError> {
    if has_no_btree_operator_class(ty) {
        return Err(ExecError::UndefinedFunction(format!(
            "could not identify a comparison function for type {}",
            ty.name()
        )));
    }
    Ok(())
}

/// The same 42883, reported from the runtime values (an untyped SQL NULL has no
/// type to name).
pub(crate) fn undefined_operator_for(op: BinaryOp, l: &Datum, r: &Datum) -> ExecError {
    let name = |d: &Datum| d.column_type().map_or("unknown", ColumnType::name);
    undefined_operator_named(op_spelling(op), name(l), name(r))
}

pub(crate) fn quantifier_of(all: bool) -> Quantifier {
    if all {
        Quantifier::All
    } else {
        Quantifier::Any
    }
}

/// SP37: tz-AWARE `timestamptz` arithmetic.
///
/// These are the cells deferred from `crabka_pgtypes::ops` because they need
/// the session zone `ctx.time_zone`. They are
/// `timestamptz ± interval → timestamptz`, which is calendar-aware in the zone,
/// and `timestamptz − timestamptz → interval`, which is an absolute-instant
/// difference.
///
/// This function returns `Ok(None)` when neither operand is a `Timestamptz`, so
/// the caller then uses `crabka_pgtypes::ops`. It propagates NULL as `ops`
/// does. Result types match `datetime_result_type`'s `Timestamptz`/`Interval`
/// predictions, so plan-time inference and runtime never disagree.
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
            Datum::Interval(timestamptz_diff(*a, *b)?)
        }
        // Any other combination with a timestamptz operand is undefined — surface
        // the genuine type error via `crabka_pgtypes::ops` (which yields TypeMismatch).
        _ => return Ok(None),
    };
    Ok(Some(result))
}

pub(crate) fn cmp_result(op: BinaryOp, ord: Option<Ordering>) -> Datum {
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

/// Only the string types carry a collation. `PostgreSQL` rejects `COLLATE` on
/// anything else at parse analysis, naming the offending type.
///
/// A domain carries whatever its base type carries, and an array carries
/// whatever its elements carry — `text[] COLLATE "C"` is legal there and
/// `pg_type.typcollation` of `_text` is the same `100` as `text`'s — so both
/// are answered by the type they wrap rather than refused outright.
pub(crate) fn require_collatable(ty: ColumnType) -> Result<(), ExecError> {
    if is_collatable(ty) {
        return Ok(());
    }
    Err(ExecError::TypeMismatch(format!(
        "collations are not supported by type {}",
        ty.name()
    )))
}

/// Whether a value of `ty` can carry a collation at all.
fn is_collatable(ty: ColumnType) -> bool {
    match ty {
        ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Char(_) => true,
        ColumnType::Domain(domain) => is_collatable(*domain.base),
        ColumnType::Array(elem) => is_collatable(elem.column_type()),
        _ => false,
    }
}

/// `(composite).field` at run time: the attribute's value, or NULL when the
/// whole composite is NULL — `PostgreSQL` propagates a NULL row through field
/// selection rather than failing.
pub(crate) fn select_field(value: &Datum, field: &str) -> Result<Datum, ExecError> {
    match value {
        Datum::Null => Ok(Datum::Null),
        Datum::Record(record) => record.field(field).cloned().ok_or_else(|| {
            ExecError::UndefinedColumn(format!(
                "column \"{field}\" not found in data type {}",
                record.column_type().name()
            ))
        }),
        other => Err(ExecError::TypeMismatch(format!(
            "column notation .{field} applied to type {}, which is not a composite type",
            other
                .column_type()
                .map_or("unknown", crabka_pgtypes::ColumnType::name)
        ))),
    }
}

/// The declared type of `field` in the composite type `base`.
fn field_type(base: ColumnType, field: &str) -> Result<ColumnType, ExecError> {
    let ColumnType::Record(named) = base.storage_type() else {
        return Err(ExecError::TypeMismatch(format!(
            "column notation .{field} applied to type {}, which is not a composite type",
            base.name()
        )));
    };
    // The anonymous `record` carries no attribute list, so a field of one has
    // no static type; PostgreSQL says so with the same "not a composite type"
    // complaint only when it cannot resolve the record, and accepts `f1`…`fn`
    // when it can. Crabka resolves the value's own names at run time and
    // reports `text` here, which is what RowDescription needs to be stable.
    let Some(named) = named else {
        return Ok(ColumnType::Text);
    };
    let Some(ty) = crabka_pgtypes::usertype::lookup_oid(named.oid) else {
        return Err(ExecError::UndefinedObject(format!(
            "type \"{}\" does not exist",
            named.name
        )));
    };
    ty.fields()
        .unwrap_or(&[])
        .iter()
        .find(|attribute| attribute.name == field)
        .map(|attribute| attribute.ty)
        .ok_or_else(|| {
            ExecError::UndefinedColumn(format!(
                "column \"{field}\" not found in data type {}",
                named.name
            ))
        })
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
        // A bit-string literal is `bit` with no length modifier — PostgreSQL's
        // `make_const` types `T_BitString` as `BITOID` with typmod -1, which is
        // why storing `B'10'` in a `bit(11)` column complains about the value's
        // own length rather than the literal's declared one.
        Expr::BitStringLiteral(_) => Ok(ColumnType::Bit(None)),
        Expr::BoolLiteral(_) => Ok(ColumnType::Bool),
        // PostgreSQL types a bare NULL as "unknown"; the slice uses text as a
        // concrete stand-in so RowDescription has a real OID.
        Expr::NullLiteral => Ok(ColumnType::Text),
        // A parameter that reached evaluation was never bound. The simple
        // protocol supplies none, so PostgreSQL reports the placeholder as
        // undefined rather than as an unimplemented feature -- 42P02, the same
        // code and wording a view body already raises.
        Expr::Param(number) => Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
            "42P02",
            format!("there is no parameter ${number}"),
        ))),
        Expr::Default => Err(ExecError::Syntax(
            "DEFAULT is not allowed in this context".into(),
        )),
        Expr::Collate { expr, .. } => {
            let ty = infer_type(expr, scope)?;
            require_collatable(ty)?;
            Ok(ty)
        }
        Expr::Column { table, name } => {
            if table.is_none()
                && let Some(call) = crate::func::niladic_keyword_call(name)
            {
                return crate::func::scalar_result_type(&call, scope);
            }
            match scope.resolve(table.as_deref(), name) {
                Ok(idx) => Ok(scope.ty_at(idx)),
                Err(error) => whole_row_reference(table.as_deref(), name, &error)
                    .filter(|q| scope.whole_row(q).is_some())
                    // The relation's composite type is not registered in
                    // `pg_type` here, so a whole row reports the anonymous
                    // `record` rather than PostgreSQL's per-relation row type.
                    .map(|_| ColumnType::Record(None))
                    .ok_or(error),
            }
        }
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => Ok(ColumnType::Bool),
            UnaryOp::TsNot => {
                let operand = infer_type(expr, scope)?;
                if operand == ColumnType::TsQuery || is_unknown_literal(expr) {
                    Ok(ColumnType::TsQuery)
                } else {
                    Err(undefined_operator("!!", operand, operand))
                }
            }
            // `~` and `@` keep the operand's type; `|/` and `||/` are the
            // `float8`-only `dsqrt`/`dcbrt`, so they report `float8` whatever
            // the operand is.
            UnaryOp::Plus | UnaryOp::Neg | UnaryOp::BitNot | UnaryOp::Abs => {
                let operand = infer_type(expr, scope)?;
                // `~cidr` is `~inet` after the implicit widening, so it reports
                // `inet` rather than the operand's own type.
                if *op == UnaryOp::BitNot && matches!(operand, ColumnType::Inet | ColumnType::Cidr)
                {
                    return Ok(ColumnType::Inet);
                }
                Ok(operand)
            }
            UnaryOp::Sqrt | UnaryOp::Cbrt => Ok(ColumnType::Float8),
            // The geometric prefix operators are declared operand type by
            // operand type, so PostgreSQL rejects `@-@ 5` at parse analysis —
            // which is here — rather than on the first row.
            UnaryOp::NPoints
            | UnaryOp::Length
            | UnaryOp::Center
            | UnaryOp::IsHorizontal
            | UnaryOp::IsVertical => {
                let operand = infer_type(expr, scope)?;
                geometric_prefix_result_type(*op, operand)
                    .ok_or_else(|| undefined_prefix_operator_named(*op, operand.name()))
            }
            // `IS DOCUMENT` is boolean-valued over an `xml` operand, and
            // PostgreSQL rejects any other operand type right here.
            UnaryOp::IsDocument | UnaryOp::IsNotDocument => {
                crate::xml_fn::check_is_document_operand(
                    expr,
                    scope,
                    *op == UnaryOp::IsNotDocument,
                )?;
                Ok(ColumnType::Bool)
            }
            // The boolean tests are boolean-valued, but only over a boolean
            // operand: PostgreSQL rejects `1 IS TRUE` at parse analysis, which
            // is here. A bare literal is still `unknown` and adopts `boolean`,
            // so `'t' IS TRUE` is accepted and coerced when it evaluates.
            UnaryOp::IsTrue
            | UnaryOp::IsNotTrue
            | UnaryOp::IsFalse
            | UnaryOp::IsNotFalse
            | UnaryOp::IsUnknown
            | UnaryOp::IsNotUnknown => {
                let operand = infer_type(expr, scope)?;
                if operand != ColumnType::Bool && !is_unknown_literal(expr) {
                    return Err(ExecError::TypeMismatch(format!(
                        "argument of {} must be type boolean, not type {}",
                        boolean_test_spelling(*op),
                        operand.name()
                    )));
                }
                Ok(ColumnType::Bool)
            }
        },
        Expr::Binary { op, left, right } => infer_binary_type(*op, left, right, scope),
        // SP29: a scalar function's result type; otherwise an aggregate result
        // type for RowDescription (count/sum -> int8, min/max -> the argument's
        // type); unknown names / bad arity / bad argument type -> 42883.
        // F-2: the pg_catalog introspection family's static result type, tried
        // in the same order as `eval` above.
        // Q3: `GROUPING(…)` is `int4` — the grouping-set rewrite folds it to a
        // `CASE` over the grouping-set ordinal before anything evaluates it, so
        // the type pass has to accept it where a scalar function would not be.
        Expr::Func(fc)
            if let Some(result) = crate::routine::plpgsql_scalar_result_type(fc, scope) =>
        {
            result
        }
        Expr::Func(fc) if crate::grouping::is_grouping_call(fc) => Ok(ColumnType::Int4),
        Expr::Func(fc) if crate::catalog_fn::is_catalog_func(&fc.name) => {
            crate::catalog_fn::catalog_func_result_type(fc, scope)
        }
        Expr::Func(fc) if crate::reg_fn::is_reg_func(&fc.name) => {
            crate::reg_fn::reg_func_result_type(fc, scope)
        }
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
        // A SQL/JSON expression's type is fixed by its form and its `RETURNING`
        // clause; the operands still have to type-check.
        Expr::SqlJson(json) => {
            let mut types = Vec::new();
            for child in json.children() {
                types.push(infer_type(child, scope)?);
            }
            if let crabka_pgparser::ast::SqlJsonExpr::IsJson { .. } = json.as_ref() {
                // `IS JSON` is the one form whose operand type is constrained.
                json_fn::is_json_operand_type(types[0])?;
            }
            Ok(json.result_type())
        }
        Expr::Func(fc) if array_fn::is_array_func(&fc.name) => {
            array_fn::array_func_result_type(fc, scope)
        }
        // A window-only function written without OVER is not an undefined
        // function: PostgreSQL says so with its own 42809.
        Expr::Func(fc) if crate::window::is_window_only_function(&fc.name) => {
            Err(crate::window::requires_over_clause(&fc.name))
        }
        Expr::Func(fc) => crate::agg::func_result_type(fc, scope),
        // SP28: predicates are boolean; CASE unifies its branch result types.
        Expr::IsNull { .. } | Expr::Like { .. } => Ok(ColumnType::Bool),
        Expr::InList { expr, list, .. } => {
            reject_uncomparable_in_list(expr, list, scope)?;
            Ok(ColumnType::Bool)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            reject_uncomparable_comparison(BinaryOp::Ge, expr, low, scope)?;
            reject_uncomparable_comparison(BinaryOp::Le, expr, high, scope)?;
            Ok(ColumnType::Bool)
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            reject_uncomparable_simple_case(operand.as_deref(), whens, scope)?;
            infer_case_type(whens, else_result.as_deref(), scope)
        }
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
        Expr::Exists(_) | Expr::InSubquery { .. } | Expr::Quantified { .. } => Ok(ColumnType::Bool),
        Expr::QuantifiedArray {
            expr, op, array, ..
        } => {
            reject_uncomparable_quantified(*op, expr, array, scope)?;
            Ok(ColumnType::Bool)
        }
        // `ARRAY[…]` types as an array of its unified element type.
        Expr::ArrayLiteral(items) => Ok(ColumnType::Array(array_literal_elem_type(items, scope)?)),
        // Like a scalar subquery, resolved to `Const` before inference runs.
        Expr::ArraySubquery(_) => Err(ExecError::Unsupported(
            "internal: ARRAY(subquery) must be resolved before type inference".into(),
        )),
        // A row value in a value position is the anonymous `record` (OID 2249).
        // Every field must still type-check, so they are inferred and discarded
        // — the record's own OID does not depend on them.
        Expr::Row(items) => {
            for item in items {
                infer_type(item, scope)?;
            }
            Ok(ColumnType::Record(None))
        }
        // `(composite).field` reports the attribute's declared type; the
        // composite has to be a known one for the attribute to have a type at
        // all, which is exactly PostgreSQL's rule.
        Expr::FieldSelect { base, field } => {
            let base_type = infer_type(base, scope)?;
            field_type(base_type, field)
        }
        Expr::FieldSelectAll(_) => Err(ExecError::Unsupported(
            "(row).* is only supported in a SELECT output list".into(),
        )),
        // `base[index]` yields the base array's element type; anything else has no
        // subscripting operator.
        Expr::Subscript { base, .. } => {
            let bt = infer_type(base, scope)?;
            // PostgreSQL's jsonb subscripting yields jsonb at every level, so
            // `j['a']['b']` type-checks without the base being an array. `json`
            // deliberately does NOT follow it here: the subscript handler is
            // jsonb's alone, so a `json` base falls through to the error below.
            if bt == ColumnType::Jsonb {
                return Ok(ColumnType::Jsonb);
            }
            // `point[i]` and `line[i]` are `float8`; `box[i]` and `lseg[i]` are
            // `point`. `circle`, `path` and `polygon` have no handler and fall
            // through to the 42804 below.
            if let Some(elem) = geometric_subscript_element(bt) {
                return Ok(elem);
            }
            bt.array_element()
                .map(ElemType::column_type)
                .ok_or_else(|| cannot_subscript(bt))
        }
        // A subscript chain containing a slice yields the ARRAY type; one made
        // only of indexes reaches an element.
        Expr::ArrayRef { base, subscripts } => {
            let bt = infer_type(base, scope)?;
            if bt == ColumnType::Jsonb {
                return Ok(ColumnType::Jsonb);
            }
            // A fixed-length geometric type has one dimension and cannot be
            // sliced, so every chain over one reports the element type; the
            // slice itself is refused when the reference evaluates.
            if let Some(elem) = geometric_subscript_element(bt) {
                return Ok(elem);
            }
            let elem = bt.array_element().ok_or_else(|| cannot_subscript(bt))?;
            if subscripts
                .iter()
                .any(crabka_pgparser::ast::ArraySubscript::is_slice)
            {
                Ok(bt)
            } else {
                Ok(elem.column_type())
            }
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
    reject_uncomparable_comparison(op, left, right, scope)?;
    if let Some(resolved) = geometric_literal_operator(op, left, right, scope)? {
        return Ok(resolved);
    }
    if let Some(resolved) = crate::network_fn::network_operator_result_type(op, left, right, scope)?
    {
        return Ok(resolved);
    }
    if let Some(resolved) = crate::bit_fn::bit_operator_result_type(op, left, right, scope)? {
        return Ok(resolved);
    }
    if let Some(resolved) = crate::money_fn::money_operator_result_type(op, left, right, scope)? {
        return Ok(resolved);
    }
    if let Some(resolved) = crate::sysid_fn::sysid_operator_result_type(op, left, right, scope)? {
        return Ok(resolved);
    }
    match op {
        // `<<=` and `>>=` have no non-network overload.
        BinaryOp::ContainedByOrEq | BinaryOp::ContainsOrEq => {
            let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
            Err(undefined_operator(op_spelling(op), lt, rt))
        }
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            if is_unknown_literal(left) && is_unknown_literal(right) {
                return Err(ambiguous_operator(op_spelling(op)));
            }
            let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
            // A geometric operand selects `box + point`, `path + path` and
            // their siblings; every undeclared pair (`polygon + point`,
            // `lseg + point`, `point + box`) is 42883 right here.
            if let Some(resolved) = geometric_operand_result(op, lt, rt) {
                return resolved;
            }
            if matches!(op, BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul) {
                match (lt, rt) {
                    (ColumnType::Range(a), ColumnType::Range(b)) if a == b => return Ok(lt),
                    (ColumnType::Multirange(a), ColumnType::Multirange(b)) if a == b => {
                        return Ok(lt);
                    }
                    (ColumnType::Range(_), _) if is_unknown_literal(right) => return Ok(lt),
                    (_, ColumnType::Range(_)) if is_unknown_literal(left) => return Ok(rt),
                    (ColumnType::Multirange(_), _) if is_unknown_literal(right) => return Ok(lt),
                    (_, ColumnType::Multirange(_)) if is_unknown_literal(left) => return Ok(rt),
                    _ => {}
                }
            }
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
            if let Some(ty) = datetime_result_type(op, lt, rt) {
                return Ok(ty);
            }
            // PostgreSQL resolves `+ - * /` from the operand types, so an
            // operand no arithmetic operator is defined over is 42883 at parse
            // analysis rather than a value-time failure on every row. An
            // `unknown` literal is exempt — it adopts the other operand's type.
            let usable = |e: &Expr, t: ColumnType| {
                is_unknown_literal(e) || is_arithmetic_type(t) || is_temporal(t)
            };
            if !usable(left, lt) || !usable(right, rt) {
                return Err(undefined_operator(op_spelling(op), lt, rt));
            }
            let (lt, rt) = adopt_unknown_literal_types(left, right, lt, rt);
            Ok(numeric_result_type(lt, rt))
        }
        // `||` is text, jsonb, or one of the three array concatenations, resolved
        // from the operand types (42883 when no `||` applies).
        BinaryOp::Concat => Ok(resolve_concat(left, right, scope)?.1),
        BinaryOp::Phrase => {
            let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
            if lt == ColumnType::TsQuery && rt == ColumnType::TsQuery {
                return Ok(ColumnType::TsQuery);
            }
            geometric_operand_result(op, lt, rt)
                .unwrap_or_else(|| Err(undefined_operator("<->", lt, rt)))
        }
        // `@@` is shared by jsonpath and full-text search. A text-search
        // operand selects the latter overload; a bare query literal adopts
        // `tsquery` when paired with a vector.
        BinaryOp::JsonPathMatch => {
            let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
            let (lt, rt) = if lt == ColumnType::TsVector && is_unknown_literal(right) {
                (lt, ColumnType::TsQuery)
            } else if lt == ColumnType::Jsonb && is_unknown_literal(right) {
                (lt, ColumnType::JsonPath)
            } else {
                (lt, rt)
            };
            if matches!(
                (lt, rt),
                (ColumnType::TsVector | ColumnType::Text, ColumnType::TsQuery)
                    | (ColumnType::TsQuery, ColumnType::TsVector | ColumnType::Text)
                    | (ColumnType::Text, ColumnType::Text)
            ) {
                return Ok(ColumnType::Bool);
            }
            json_fn::json_operator_result_type(JsonOp::PathMatch, lt, rt)
                .ok_or_else(|| undefined_operator("@@", lt, rt))
        }
        BinaryOp::Overlaps => {
            if is_unknown_literal(left) && is_unknown_literal(right) {
                return Err(ambiguous_operator(op_spelling(op)));
            }
            let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
            let (alt, art) = adopt_json_operand_types(op, left, right, lt, rt);
            if alt == ColumnType::TsQuery && art == ColumnType::TsQuery {
                return Ok(ColumnType::TsQuery);
            }
            json_or_array_operator_result_type(op, alt, art)
                .ok_or_else(|| undefined_operator("&&", lt, rt))
        }
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
        | BinaryOp::JsonPathExists => {
            if matches!(op, BinaryOp::Contains | BinaryOp::ContainedBy)
                && is_unknown_literal(left)
                && is_unknown_literal(right)
            {
                return Err(ambiguous_operator(op_spelling(op)));
            }
            let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
            let (alt, art) = adopt_json_operand_types(op, left, right, lt, rt);
            if matches!(op, BinaryOp::Contains | BinaryOp::ContainedBy)
                && alt == ColumnType::TsQuery
                && art == ColumnType::TsQuery
            {
                return Ok(ColumnType::Bool);
            }
            json_or_array_operator_result_type(op, alt, art)
                .ok_or_else(|| undefined_operator(op_spelling(op), lt, rt))
        }
        // The bitwise operators are integer-only. `&`/`|`/`#` widen to the wider
        // operand; a shift keeps the LEFT operand's width (its count is an
        // ordinary integer, not part of the result type).
        BinaryOp::DoesNotExtendRight | BinaryOp::DoesNotExtendLeft | BinaryOp::Adjacent => {
            if is_unknown_literal(left) && is_unknown_literal(right) {
                return Err(ambiguous_operator(op_spelling(op)));
            }
            let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
            if let Some(resolved) = geometric_operand_result(op, lt, rt) {
                return resolved;
            }
            if range_family_compatible(lt, rt)
                || matches!(lt, ColumnType::Range(_) | ColumnType::Multirange(_))
                    && is_unknown_literal(right)
                || matches!(rt, ColumnType::Range(_) | ColumnType::Multirange(_))
                    && is_unknown_literal(left)
            {
                Ok(ColumnType::Bool)
            } else {
                Err(undefined_operator(op_spelling(op), lt, rt))
            }
        }
        BinaryOp::BitAnd | BinaryOp::BitOr | BinaryOp::BitXor | BinaryOp::Shl | BinaryOp::Shr => {
            if is_unknown_literal(left) && is_unknown_literal(right) {
                return Err(ambiguous_operator(op_spelling(op)));
            }
            let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
            // `<<`, `>>` and `#` all double as geometric operators, and a
            // geometric operand is what selects that meaning.
            if let Some(resolved) = geometric_operand_result(op, lt, rt) {
                return resolved;
            }
            if matches!(op, BinaryOp::Shl | BinaryOp::Shr)
                && (range_family_compatible(lt, rt)
                    || matches!(lt, ColumnType::Range(_) | ColumnType::Multirange(_))
                        && is_unknown_literal(right)
                    || matches!(rt, ColumnType::Range(_) | ColumnType::Multirange(_))
                        && is_unknown_literal(left))
            {
                return Ok(ColumnType::Bool);
            }
            let is_int =
                |t: ColumnType| matches!(t, ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8);
            let (lt, rt) = adopt_unknown_literal_types(left, right, lt, rt);
            if !is_int(lt) || !is_int(rt) {
                return Err(undefined_operator(op_spelling(op), lt, rt));
            }
            let both = |want: ColumnType| lt == want && rt == want;
            Ok(match op {
                BinaryOp::Shl | BinaryOp::Shr => lt,
                _ if both(ColumnType::Int2) => ColumnType::Int2,
                _ if matches!(lt, ColumnType::Int2 | ColumnType::Int4)
                    && matches!(rt, ColumnType::Int2 | ColumnType::Int4) =>
                {
                    ColumnType::Int4
                }
                _ => ColumnType::Int8,
            })
        }
        // `^` has no integer form in PostgreSQL: an all-integer pair resolves to
        // the `float8` operator, and a `numeric` operand (with no `float8` one)
        // selects the exact `numeric` operator.
        BinaryOp::Pow => {
            let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
            let (lt, rt) = adopt_unknown_literal_types(left, right, lt, rt);
            if lt == ColumnType::Float8 || rt == ColumnType::Float8 {
                Ok(ColumnType::Float8)
            } else if lt.is_numeric() || rt.is_numeric() {
                Ok(ColumnType::Numeric(None))
            } else {
                Ok(ColumnType::Float8)
            }
        }
        // `%` is defined for integers and `numeric` only — `float8` has no
        // modulo in PostgreSQL, so it is 42883 at plan time rather than a
        // runtime type error.
        BinaryOp::Mod => {
            let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
            let (lt, rt) = adopt_unknown_literal_types(left, right, lt, rt);
            let modulo_able = |t: ColumnType| {
                matches!(t, ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8)
                    || t.is_numeric()
            };
            if !modulo_able(lt) || !modulo_able(rt) {
                return Err(undefined_operator("%", lt, rt));
            }
            Ok(numeric_result_type(lt, rt))
        }
        // The geometry-only operators, plus the five whose other overload is
        // the range family. `##` alone is not a predicate — it yields the
        // closest point — so none of these can fall through to the `boolean`
        // default below without being resolved first.
        BinaryOp::ClosestPoint
        | BinaryOp::Intersects
        | BinaryOp::Horizontal
        | BinaryOp::Perpendicular
        | BinaryOp::Parallel
        | BinaryOp::BelowEq
        | BinaryOp::AboveEq
        | BinaryOp::Same
        | BinaryOp::StrictlyBelow
        | BinaryOp::StrictlyAbove
        | BinaryOp::DoesNotExtendAbove
        | BinaryOp::DoesNotExtendBelow => {
            let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
            match geometric_operand_result(op, lt, rt) {
                Some(resolved) => resolved,
                // `~=`, `<<|`, `|>>`, `&<|` and `|&>` are also range operators,
                // which the value layer answers; `##` and the other five are
                // geometry's alone and are 42883 without a geometric operand.
                None if matches!(
                    op,
                    BinaryOp::Same
                        | BinaryOp::StrictlyBelow
                        | BinaryOp::StrictlyAbove
                        | BinaryOp::DoesNotExtendAbove
                        | BinaryOp::DoesNotExtendBelow
                ) =>
                {
                    Ok(ColumnType::Bool)
                }
                None => Err(undefined_operator(op_spelling(op), lt, rt)),
            }
        }
        _ => Ok(ColumnType::Bool),
    }
}

/// PLAN-time 42883 for a comparison over a type that has no comparison
/// operator: `jsonpath`, and `json` (see [`has_no_operator_class`]).
///
/// Both reach here through every syntax that resolves to one — `=`, the
/// ordering operators, `IS DISTINCT FROM`, `IN (…)`, `BETWEEN` and the simple
/// `CASE` — because PostgreSQL rejects them all at parse analysis, before a
/// single row is fetched.
fn reject_uncomparable_comparison(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    scope: &Scope,
) -> Result<(), ExecError> {
    if !matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::Gt
            | BinaryOp::Ge
            | BinaryOp::IsDistinctFrom
            | BinaryOp::IsNotDistinctFrom
    ) {
        return Ok(());
    }
    crate::rowexpr::validate_comparison(op, left, right, scope)?;
    let (mut left_type, mut right_type) = (infer_type(left, scope)?, infer_type(right, scope)?);
    // `IS DISTINCT FROM` is resolved through the type's `=`, so that is the
    // spelling PostgreSQL names when the type has none.
    let spelled = if matches!(op, BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom) {
        "="
    } else {
        op_spelling(op)
    };
    // `json` and `xml` have no comparison operator at ANY width, so an
    // `unknown` literal beside one adopts nothing — PostgreSQL leaves it
    // `unknown` and names it that way (`operator does not exist: xml =
    // unknown`).
    if is_uncomparable_scalar(left_type) || is_uncomparable_scalar(right_type) {
        let name = |e: &Expr, t: ColumnType| {
            if is_unknown_literal(e) {
                "unknown"
            } else {
                t.name()
            }
        };
        return Err(undefined_operator_named(
            spelled,
            name(left, left_type),
            name(right, right_type),
        ));
    }
    // The system identifier family's comparison partners are exactly what
    // `pg_operator` declares plus what `pg_cast` marks implicit — a much
    // shorter list than the value layer would otherwise accept. `oid` takes the
    // integer widths and the `reg*` types (all implicit coercions), `xid` takes
    // `int4` alone (`xideqint4`), and the other four take only themselves. An
    // `unknown` literal beside any of them adopts it.
    if let Some(error) = sysid_comparison_rejection(spelled, left, left_type, right, right_type) {
        return Err(error);
    }
    // `xid` and `cid` have no ordering operator at any width, and `cid` has no
    // `<>` either — `pg_operator` gives it `cideq` alone. Like `json`, an
    // `unknown` literal beside one adopts nothing, so `'1'::xid < '2'` is
    // `operator does not exist: xid < unknown`, naming the literal `unknown`.
    let missing_operator = |ty: ColumnType| match ty.storage_type() {
        ColumnType::Xid => matches!(
            op,
            BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
        ),
        ColumnType::Cid => matches!(
            op,
            BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge | BinaryOp::Ne
        ),
        _ => false,
    };
    let unordered = |e: &Expr, ty: ColumnType| !is_unknown_literal(e) && missing_operator(ty);
    if unordered(left, left_type) || unordered(right, right_type) {
        let name = |e: &Expr, t: ColumnType| {
            if is_unknown_literal(e) {
                "unknown"
            } else {
                t.name()
            }
        };
        return Err(undefined_operator_named(
            spelled,
            name(left, left_type),
            name(right, right_type),
        ));
    }
    if !is_scalar_jsonpath(left_type) && !is_scalar_jsonpath(right_type) {
        return Ok(());
    }
    if is_unknown_literal(left) {
        left_type = right_type;
    }
    if is_unknown_literal(right) {
        right_type = left_type;
    }
    Err(undefined_operator(spelled, left_type, right_type))
}

/// The 42883 for a system identifier compared with a type it has no operator
/// against, or `None` when the pair is one PostgreSQL declares.
fn sysid_comparison_rejection(
    spelled: &str,
    left: &Expr,
    left_type: ColumnType,
    right: &Expr,
    right_type: ColumnType,
) -> Option<ExecError> {
    let sysid = |e: &Expr, ty: ColumnType| {
        (!is_unknown_literal(e)
            && matches!(
                ty.storage_type(),
                ColumnType::Oid
                    | ColumnType::Xid
                    | ColumnType::Xid8
                    | ColumnType::Cid
                    | ColumnType::Tid
                    | ColumnType::PgLsn
            ))
        .then(|| ty.storage_type())
    };
    let (family, other, other_expr, reflected) =
        match (sysid(left, left_type), sysid(right, right_type)) {
            (Some(family), _) => (family, right_type, right, false),
            (None, Some(family)) => (family, left_type, left, true),
            (None, None) => return None,
        };
    // An `unknown` literal takes the identifier's own type, so `f1 = '1234'`
    // never reaches the partner check.
    if is_unknown_literal(other_expr) {
        return None;
    }
    let allowed = match family {
        ColumnType::Oid => matches!(
            other.storage_type(),
            ColumnType::Oid
                | ColumnType::Int2
                | ColumnType::Int4
                | ColumnType::Int8
                | ColumnType::Regclass
                | ColumnType::Regtype
                | ColumnType::Regprocedure
                | ColumnType::Regnamespace
        ),
        // `xideqint4` / `xidneqint4` have no commutator, so the integer must be
        // on the RIGHT: `'1'::xid = 1` resolves and `1 = '1'::xid` does not.
        // `int2` reaches it through the implicit widening to `int4`.
        ColumnType::Xid => {
            other.storage_type() == ColumnType::Xid
                || (!reflected
                    && matches!(other.storage_type(), ColumnType::Int2 | ColumnType::Int4))
        }
        other_family => other.storage_type() == other_family,
    };
    (!allowed).then(|| undefined_operator_named(spelled, left_type.name(), right_type.name()))
}

fn reject_uncomparable_in_list(expr: &Expr, list: &[Expr], scope: &Scope) -> Result<(), ExecError> {
    for item in list {
        reject_uncomparable_comparison(BinaryOp::Eq, expr, item, scope)?;
    }
    Ok(())
}

fn reject_uncomparable_simple_case(
    operand: Option<&Expr>,
    whens: &[(Expr, Expr)],
    scope: &Scope,
) -> Result<(), ExecError> {
    let Some(operand) = operand else {
        return Ok(());
    };
    for (when, _) in whens {
        reject_uncomparable_comparison(BinaryOp::Eq, operand, when, scope)?;
    }
    Ok(())
}

/// `x <op> ANY|ALL (array)` over an element type with no comparison operator —
/// `'{}'::json = ANY(ARRAY['{}'::json])` is `operator does not exist: json = json`.
fn reject_uncomparable_quantified(
    op: BinaryOp,
    expr: &Expr,
    array: &Expr,
    scope: &Scope,
) -> Result<(), ExecError> {
    let mut left = infer_type(expr, scope)?;
    let array_type = infer_type(array, scope)?;
    let mut right = array_type
        .array_element()
        .map(ElemType::column_type)
        .unwrap_or(array_type);
    if is_unknown_literal(expr) {
        left = right;
    }
    if is_unknown_literal(array) {
        right = left;
    }
    if has_no_operator_class(left) || has_no_operator_class(right) {
        return Err(undefined_operator(op_spelling(op), left, right));
    }
    Ok(())
}

/// The static result type of a jsonb or array operator, or `None` when the
/// operand types resolve neither.
///
/// The caller reports 42883 at plan time. `json_fn` holds the jsonb rules. The
/// array rules are `@>` / `<@` / `&&` over two arrays that share one element
/// type.
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
    ) {
        let same_array_family = match (lt, rt) {
            (ColumnType::Array(left), ColumnType::Array(right)) => left == right,
            (ColumnType::OidVector, ColumnType::OidVector)
            | (ColumnType::Int2Vector, ColumnType::Int2Vector) => true,
            _ => false,
        };
        if same_array_family {
            return Some(ColumnType::Bool);
        }
    }
    // The geometric operators resolve by the exact operand pair; an undeclared
    // one is `None` here and the caller reports 42883.
    if let Some(resolved) = geometric_operator_result_type(op, lt, rt) {
        return Some(resolved);
    }
    match (op, lt, rt) {
        (
            BinaryOp::Contains | BinaryOp::ContainedBy | BinaryOp::Overlaps,
            ColumnType::Range(left),
            ColumnType::Range(right),
        ) if left == right => return Some(ColumnType::Bool),
        (BinaryOp::Contains | BinaryOp::ContainedBy | BinaryOp::Overlaps, left, right)
            if range_family_compatible(left, right) =>
        {
            return Some(ColumnType::Bool);
        }
        (BinaryOp::Contains, ColumnType::Range(range), element) if *range.subtype == element => {
            return Some(ColumnType::Bool);
        }
        (BinaryOp::ContainedBy, element, ColumnType::Range(range)) if element == *range.subtype => {
            return Some(ColumnType::Bool);
        }
        (BinaryOp::Contains, ColumnType::Multirange(multirange), element)
            if *multirange.range.subtype == element =>
        {
            return Some(ColumnType::Bool);
        }
        (BinaryOp::ContainedBy, element, ColumnType::Multirange(multirange))
            if element == *multirange.range.subtype =>
        {
            return Some(ColumnType::Bool);
        }
        _ => {}
    }
    let storage = (lt.storage_type(), rt.storage_type());
    (storage != (lt, rt))
        .then(|| json_or_array_operator_result_type(op, storage.0, storage.1))
        .flatten()
}

fn range_family_compatible(left: ColumnType, right: ColumnType) -> bool {
    match (left, right) {
        (ColumnType::Range(a), ColumnType::Range(b)) => a == b,
        (ColumnType::Range(range), ColumnType::Multirange(multirange))
        | (ColumnType::Multirange(multirange), ColumnType::Range(range)) => {
            range == multirange.range
        }
        (ColumnType::Multirange(a), ColumnType::Multirange(b)) => a == b,
        _ => false,
    }
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
        // A nested constructor contributes its own element type, not an array
        // type: `ARRAY[ARRAY[1],ARRAY[2]]` is `integer[]` with two dimensions.
        match item {
            Expr::ArrayLiteral(inner) => {
                let elem = array_literal_elem_type(inner, scope)?.column_type();
                acc = Some(match acc {
                    None => elem,
                    Some(seen) => unify_types(seen, elem)?,
                });
            }
            _ => acc = unify_branch(acc, item, scope)?,
        }
    }
    let elem = acc.unwrap_or(ColumnType::Text);
    ElemType::from_column_type(elem).ok_or_else(|| {
        ExecError::Unsupported(format!("arrays of {} are not supported", elem.name()))
    })
}

/// `ARRAY[…]`'s evaluation, kept out of [`eval_depth`]'s own frame so that this
/// arm's locals do not spend the recursion budget.
#[inline(never)]
fn eval_array_constructor(
    items: &[Expr],
    scope: &Scope,
    values: &[Datum],
    ctx: &EvalCtx,
    depth: usize,
) -> Result<Datum, ExecError> {
    let elem = array_literal_elem_type(items, scope)?;
    let target = elem.column_type();
    let mut parts = Vec::with_capacity(items.len());
    for item in items {
        // A nested constructor contributes a whole sub-array — the dimension it
        // adds is what makes `ARRAY[[1,2],[3,4]]` two-dimensional.
        let value = eval_depth(item, scope, values, ctx, depth)?;
        parts.push(match item {
            Expr::ArrayLiteral(_) => value,
            _ => cast_value(&value, target, &ctx.time_zone)?,
        });
    }
    array_fn::build_constructor(elem, parts)
}

/// `base[s1][s2]…`'s evaluation, kept out of [`eval_depth`]'s own frame for the
/// same reason as [`eval_array_constructor`].
#[inline(never)]
fn eval_array_ref(
    base: &Expr,
    subscripts: &[crabka_pgparser::ast::ArraySubscript],
    scope: &Scope,
    values: &[Datum],
    ctx: &EvalCtx,
    depth: usize,
) -> Result<Datum, ExecError> {
    let evaluated = eval_depth(base, scope, values, ctx, depth)?;
    let args = eval_subscripts(subscripts, scope, values, ctx, depth)?;
    match subscript_kind(&evaluated, base, scope)? {
        SubscriptKind::Json => Err(cannot_subscript(ColumnType::Json)),
        SubscriptKind::Jsonb => eval_jsonb_subscript_chain(&evaluated, subscripts, &args),
        SubscriptKind::Geometric => geometric_array_ref(&evaluated, &args),
        SubscriptKind::Array => array_fn::array_ref(&evaluated, &args),
    }
}

/// Which subscripting rule `base[…]` follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscriptKind {
    /// The SQL array rule: 1-based, sliceable, out-of-range is SQL NULL.
    Array,
    /// `jsonb`'s `subscript_handler`: by key or by 0-based index.
    Jsonb,
    /// `json`, which has NO subscript handler at all — the reference is an
    /// error rather than a different rule.
    Json,
    /// `point`, `box`, `lseg` and `line`: a fixed-length, **0-based** reference
    /// into the value's own fields, which cannot be sliced.
    Geometric,
}

/// The rule a base value (or, when the value carries no type of its own, the
/// base expression's static type) selects.
///
/// `circle`, `path` and `polygon` are the three geometric types with no
/// subscript handler at all, so they are rejected here rather than falling
/// through to `array_fn`'s "assume it is an array" default — which would have
/// produced the same 42804 message, but only for a non-NULL value.
fn subscript_kind(value: &Datum, base: &Expr, scope: &Scope) -> Result<SubscriptKind, ExecError> {
    let ty = match value {
        Datum::Jsonb(_) => return Ok(SubscriptKind::Jsonb),
        Datum::Json(_) => return Ok(SubscriptKind::Json),
        Datum::Point(_) => ColumnType::Point,
        Datum::Box(_) => ColumnType::Box,
        Datum::Lseg(_) => ColumnType::Lseg,
        Datum::Line(_) => ColumnType::Line,
        Datum::Circle(_) => ColumnType::Circle,
        Datum::Path(_) => ColumnType::Path,
        Datum::Polygon(_) => ColumnType::Polygon,
        _ => infer_type(base, scope)?,
    };
    Ok(match ty {
        ColumnType::Jsonb => SubscriptKind::Jsonb,
        ColumnType::Json => SubscriptKind::Json,
        ColumnType::Point | ColumnType::Box | ColumnType::Lseg | ColumnType::Line => {
            SubscriptKind::Geometric
        }
        ColumnType::Circle | ColumnType::Path | ColumnType::Polygon => {
            return Err(cannot_subscript(ty));
        }
        _ => SubscriptKind::Array,
    })
}

/// The element type `base[i]` yields for the four subscriptable geometric
/// types, or `None` for every other type.
fn geometric_subscript_element(ty: ColumnType) -> Option<ColumnType> {
    match ty {
        ColumnType::Point | ColumnType::Line => Some(ColumnType::Float8),
        ColumnType::Box | ColumnType::Lseg => Some(ColumnType::Point),
        _ => None,
    }
}

/// `point[i]`, `box[i]`, `lseg[i]` and `line[i]` — `PostgreSQL`'s fixed-length
/// subscripting, which is **0-based** where the array rule is 1-based. Every
/// out-of-range index is SQL NULL, a negative one included.
fn geometric_subscript(base: &Datum, index: &Datum) -> Result<Datum, ExecError> {
    let Some(index) = array_fn::subscript_int(index)? else {
        return Ok(Datum::Null);
    };
    Ok(match (base, index) {
        (Datum::Point(point), 0) => Datum::Float8(point.x),
        (Datum::Point(point), 1) => Datum::Float8(point.y),
        // `box_subscript` reads the HIGH corner first, which is also the order
        // `box_out` prints them in.
        (Datum::Box(value), 0) => Datum::Point(value.high),
        (Datum::Box(value), 1) => Datum::Point(value.low),
        (Datum::Lseg(lseg), 0) => Datum::Point(lseg.start),
        (Datum::Lseg(lseg), 1) => Datum::Point(lseg.end),
        (Datum::Line(line), 0) => Datum::Float8(line.a),
        (Datum::Line(line), 1) => Datum::Float8(line.b),
        (Datum::Line(line), 2) => Datum::Float8(line.c),
        _ => Datum::Null,
    })
}

/// [`geometric_subscript`] over a whole subscript chain. A fixed-length type
/// has exactly one dimension, so a longer chain is SQL NULL (`box[0][1]` is a
/// two-dimensional reference, not `(box[0])[1]`), and a slice is 0A000.
fn geometric_array_ref(
    base: &Datum,
    subscripts: &[array_fn::SubscriptArg],
) -> Result<Datum, ExecError> {
    if subscripts.iter().any(array_fn::SubscriptArg::is_slice) {
        return Err(ExecError::Unsupported(
            "slices of fixed-length arrays not implemented".into(),
        ));
    }
    let [array_fn::SubscriptArg::Index(index)] = subscripts else {
        return Ok(Datum::Null);
    };
    geometric_subscript(base, index)
}

/// PostgreSQL's 42804 for a base type with no subscripting operator. `json` is
/// one: only `jsonb` has a `subscript_handler`, so `('{"a":1}'::json)['a']` is
/// this error rather than a field lookup.
fn cannot_subscript(ty: ColumnType) -> ExecError {
    ExecError::TypeMismatch(format!(
        "cannot subscript type {} because it does not support subscripting",
        ty.name()
    ))
}

/// [`eval_subscripts`] for an assignment target, whose bounds are evaluated
/// against the joined row instead of a projection scope.
pub(crate) fn eval_assignment_subscripts(
    subscripts: &[crabka_pgparser::ast::ArraySubscript],
    scope: &Scope,
    values: &[Datum],
    ctx: &EvalCtx,
) -> Result<Vec<array_fn::SubscriptArg>, ExecError> {
    eval_subscripts(subscripts, scope, values, ctx, 0)
}

/// Evaluate each bound of a subscript chain into the executor's
/// [`array_fn::SubscriptArg`] form.
fn eval_subscripts(
    subscripts: &[crabka_pgparser::ast::ArraySubscript],
    scope: &Scope,
    values: &[Datum],
    ctx: &EvalCtx,
    depth: usize,
) -> Result<Vec<array_fn::SubscriptArg>, ExecError> {
    use crabka_pgparser::ast::ArraySubscript;

    let bound = |e: &Option<Expr>| -> Result<Option<Datum>, ExecError> {
        e.as_ref()
            .map(|e| eval_depth(e, scope, values, ctx, depth))
            .transpose()
    };
    subscripts
        .iter()
        .map(|s| match s {
            ArraySubscript::Index(e) => Ok(array_fn::SubscriptArg::Index(eval_depth(
                e, scope, values, ctx, depth,
            )?)),
            ArraySubscript::Slice { lower, upper } => Ok(array_fn::SubscriptArg::Slice {
                lower: bound(lower)?,
                upper: bound(upper)?,
            }),
        })
        .collect()
}

/// A jsonb base subscripts by key or index at every level, so a chain folds
/// from left to right. `jsonb` has no slice operator.
fn eval_jsonb_subscript_chain(
    base: &Datum,
    subscripts: &[crabka_pgparser::ast::ArraySubscript],
    args: &[array_fn::SubscriptArg],
) -> Result<Datum, ExecError> {
    if subscripts
        .iter()
        .any(crabka_pgparser::ast::ArraySubscript::is_slice)
    {
        return Err(ExecError::TypeMismatch(
            "jsonb subscript does not support slices".into(),
        ));
    }
    let mut value = base.clone();
    for arg in args {
        let array_fn::SubscriptArg::Index(index) = arg else {
            unreachable!("the slice case returned above")
        };
        value = json_fn::jsonb_subscript(&value, index)?;
    }
    Ok(value)
}

/// `ARRAY[]::int[]`, where an empty array constructor has no element type of
/// its own.
///
/// PostgreSQL pushes the cast's type context down into the constructor. This
/// function returns the typed empty array when `expr`/`ty` are exactly that
/// shape.
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
pub(crate) fn infer_case_type(
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

/// Fold one branch/argument into a running unified type. An untyped string or
/// `NULL` is type-neutral and adopts the concrete branch type. Shared by `CASE`
/// type inference and SP29's `coalesce`/`greatest`/`least`.
pub(crate) fn unify_branch(
    acc: Option<ColumnType>,
    expr: &Expr,
    scope: &Scope,
) -> Result<Option<ColumnType>, ExecError> {
    if is_unknown_literal(expr) {
        return Ok(acc);
    }
    let t = infer_type(expr, scope)?;
    match acc {
        None => Ok(Some(t)),
        Some(a) => Ok(Some(unify_types(a, t)?)),
    }
}

pub(crate) fn unify_types(a: ColumnType, b: ColumnType) -> Result<ColumnType, ExecError> {
    use ColumnType::{Float4, Float8, Int2, Int4, Int8, Numeric};
    // The numeric tower: int2 < int4/int8 < numeric < float4 < float8. This is
    // PostgreSQL's `select_common_type` over category N restricted to crabka's
    // types: the winner is the one every other member casts to IMPLICITLY, and
    // `numeric → float4` is implicit while `float4 → numeric` is only
    // assignment, which is why `int2 UNION float4` is `real`, not `numeric`.
    let num_family =
        |t: ColumnType| matches!(t, Int2 | Int4 | Int8 | Float4 | Float8) || t.is_numeric();
    Ok(match (a, b) {
        (x, y) if x == y => x,
        // Mirror the arithmetic int2->int4->int8 promotion rule.
        (Int2, Int4) | (Int4, Int2) => Int4,
        (Int2 | Int4, Int8) | (Int8, Int2 | Int4) => Int8,
        // SP30/SP32: any float8 wins; then float4; else (a numeric in the mix) → numeric.
        _ if a == Float8 || b == Float8 => Float8,
        _ if (a == Float4 || b == Float4) && num_family(a) && num_family(b) => Float4,
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
    use ColumnType::{Date, Interval, Time, Timestamp, Timestamptz, Timetz};
    matches!(t, Date | Time | Timetz | Timestamp | Timestamptz | Interval)
}

/// PostgreSQL's date/time arithmetic result-type matrix.
///
/// This function returns `Some(result)` for a defined `(op, lt, rt)`
/// combination where at least one operand is temporal. It returns `None` in
/// every other case, including a temporal operand in an UNdefined combination.
/// The caller then uses `numeric_result_type`, and the real type error appears
/// at evaluation. This function never invents a numeric result for a temporal
/// pair that PG would reject, because eval is the authority.
fn datetime_result_type(op: BinaryOp, lt: ColumnType, rt: ColumnType) -> Option<ColumnType> {
    use BinaryOp::{Add, Div, Mul, Sub};
    use ColumnType::{
        Date, Float8, Int4, Int8, Interval, Numeric, Time, Timestamp, Timestamptz, Timetz,
    };
    // Only engage the matrix when a temporal operand is present; a purely numeric
    // pair belongs to the numeric tower.
    if !is_temporal(lt) && !is_temporal(rt) {
        return None;
    }
    // `int2` reaches the `date ± integer` and `interval * number` operators
    // through PostgreSQL's implicit widening cast, so it belongs to both sets.
    let is_int = |t: ColumnType| matches!(t, ColumnType::Int2 | Int4 | Int8);
    let is_number = |t: ColumnType| {
        matches!(
            t,
            ColumnType::Int2 | Int4 | Int8 | ColumnType::Float4 | Float8 | Numeric(_)
        )
    };
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
        // timetz ± interval → timetz; the shift wraps mod 24 h and keeps the
        // operand's zone offset, which `ops.rs` already implements — only this
        // plan-time rule was missing, so the call was 42883 before it could run.
        (Add | Sub, Timetz, Interval) | (Add, Interval, Timetz) => Timetz,
        // date + time / time + date → timestamp (combine the calendar date and
        // the wall-clock time).
        (Add, Date, Time) | (Add, Time, Date) => Timestamp,
        // Any other combination with a temporal operand is undefined here — fall
        // through so eval raises the genuine type error.
        _ => return None,
    })
}

/// The result type of `+ - * /` on two operand types.
///
/// The numeric tower is int < numeric < float8. Any float8 makes the result
/// float8. If there is no float8, any numeric makes the result numeric. If
/// there is neither, the result is int4 only when both operands are int4, and
/// int8 otherwise. This function is permissive about non-numeric operands, and
/// a real type error appears at evaluation.
/// The types `PostgreSQL` defines `+ - * /` over once the date/time matrix has
/// been applied. These are the numeric tower, and nothing else.
fn is_arithmetic_type(ty: ColumnType) -> bool {
    matches!(
        ty,
        ColumnType::Int2
            | ColumnType::Int4
            | ColumnType::Int8
            | ColumnType::Float4
            | ColumnType::Float8
    ) || ty.is_numeric()
}

fn numeric_result_type(lt: ColumnType, rt: ColumnType) -> ColumnType {
    use ColumnType::{Float4, Float8, Int2, Int4};
    // `float4 ⊕ float4` is the ONLY single-precision pairing: PostgreSQL has no
    // `float4 ⊕ int` / `float4 ⊕ numeric` operator, so any other mix widens both
    // sides to the category's preferred type, `float8`.
    if lt == Float4 && rt == Float4 {
        Float4
    } else if lt == Float8 || rt == Float8 || lt == Float4 || rt == Float4 {
        Float8
    } else if lt.is_numeric() || rt.is_numeric() {
        ColumnType::Numeric(None)
    } else if lt == Int2 && rt == Int2 {
        Int2
    } else if matches!(lt, Int2 | Int4) && matches!(rt, Int2 | Int4) {
        Int4
    } else {
        ColumnType::Int8
    }
}

#[cfg(test)]
mod tests {

    /// PostgreSQL decides an empty `IN` list before it looks at the operand:
    /// `NULL IN (SELECT 1 WHERE false)` is `f`, not NULL, and `NOT IN` is `t`.
    #[test]
    fn an_empty_in_list_is_decided_without_the_operand() {
        use assert2::assert;

        let no_children = |_: &Expr| -> Result<Datum, ExecError> {
            panic!("an empty list must not evaluate any child")
        };
        let ctx = crate::clock::EvalCtx::test_default();
        let operand = Expr::IntLiteral("1".into());

        assert!(
            eval_in_list(&operand, &Datum::Null, &[], false, &ctx, no_children).expect("empty IN")
                == Datum::Bool(false)
        );
        assert!(
            eval_in_list(&operand, &Datum::Null, &[], true, &ctx, no_children)
                .expect("empty NOT IN")
                == Datum::Bool(true)
        );
        assert!(
            eval_in_list(&operand, &Datum::Int4(1), &[], false, &ctx, no_children)
                .expect("empty IN")
                == Datum::Bool(false)
        );
        // A NULL operand against a NON-empty list is still unknown.
        assert!(
            eval_in_list(
                &operand,
                &Datum::Null,
                &[Expr::IntLiteral("1".into())],
                false,
                &ctx,
                |_| Ok(Datum::Int4(1))
            )
            .expect("NULL IN (1)")
                == Datum::Null
        );
    }
    use crabka_pgcatalog::{Column, RelationName, Table};
    use crabka_pgparser::parser::parse_expr_for_test as pexpr;
    use crabka_pgtypes::{ColumnType, Datum};

    use super::*;

    fn table() -> Table {
        Table {
            id: 1,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public("t"),
            columns: vec![
                Column::new("a", ColumnType::Int4),
                Column::new("b", ColumnType::Int4),
            ],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            materialized: None,
            checks: Vec::new(),
        }
    }

    /// Build the `Scope` the tests evaluate against.
    ///
    /// This is the table's single-relation scope, or the empty scope for
    /// FROM-less expressions.
    fn scope_of(t: Option<&Table>) -> Scope {
        match t {
            Some(t) => Scope::single(t, &t.name.name),
            None => Scope::empty(),
        }
    }

    fn ev(sql: &str, t: Option<&Table>, vals: &[Datum]) -> Datum {
        let ctx = crate::clock::EvalCtx::test_default();
        eval(&pexpr(sql).expect("parse"), &scope_of(t), vals, &ctx).expect("eval")
    }

    fn ev_err(sql: &str) -> ExecError {
        let ctx = crate::clock::EvalCtx::test_default();
        eval(&pexpr(sql).expect("parse"), &Scope::empty(), &[], &ctx).expect_err("must fail")
    }

    /// The static-type pass runs before any row is produced, so it is what
    /// rejects a non-boolean boolean test.
    fn infer_err(sql: &str) -> ExecError {
        infer_type(&pexpr(sql).expect("parse"), &Scope::empty()).expect_err("must fail")
    }

    /// PostgreSQL resolves an `unknown` literal from its geometric sibling, so
    /// `p.f1 << '(0,0)'` picks the `point` operator rather than failing to find
    /// one. The literal adopts the sibling's own type — `circle @> '(0.5,0)'`
    /// reads the literal as a *circle*, not as the point it looks like.
    #[test]
    fn a_bare_literal_adopts_its_geometric_siblings_type() {
        // (expression, result) — verified against PostgreSQL 18.4.
        let cases: &[(&str, Datum)] = &[
            ("point '(1,2)' << '(3,4)'", Datum::Bool(true)),
            ("'(3,4)' >> point '(1,2)'", Datum::Bool(true)),
            ("point '(1,2)' >> '(3,4)'", Datum::Bool(false)),
            ("point '(1,2)' <<| '(3,4)'", Datum::Bool(true)),
            ("point '(1,2)' |>> '(3,4)'", Datum::Bool(false)),
            ("point '(5.1,34.5)' ~= '(5.1,34.5)'", Datum::Bool(true)),
            ("point '(0,0)' <-> '(3,4)'", Datum::Float8(5.0)),
            ("box '((0,0),(2,2))' && '((1,1),(3,3))'", Datum::Bool(true)),
        ];
        for (sql, expected) in cases {
            assert2::assert!(ev(sql, None, &[]) == *expected, "{sql}");
        }
        // The literal is read as the sibling's type, so a point-shaped literal
        // against a circle is a circle syntax error, not a silent point.
        assert2::assert!(
            ev_err("circle '<(0,0),1>' @> '(0.5,0)'")
                .into_pg()
                .message
                .contains("invalid input syntax for type circle")
        );
    }

    /// A quoted literal with no cast is type `unknown` and adopts the type of
    /// the operand it is compared against — the rule psql's `\d` leans on when
    /// it writes `WHERE c.oid = '20001'`. Every cell was verified against
    /// PostgreSQL 18.4.
    #[test]
    fn a_bare_literal_adopts_its_scalar_siblings_type() {
        // (expression, result)
        let cases: &[(&str, Datum)] = &[
            // The integer family, in both operand orders.
            ("1 = '1'", Datum::Bool(true)),
            ("'1' = 1", Datum::Bool(true)),
            ("1::int2 = '1'", Datum::Bool(true)),
            ("1::int8 = '1'", Datum::Bool(true)),
            ("1::oid = '1'", Datum::Bool(true)),
            ("1 = '01'", Datum::Bool(true)),
            ("1 = '+1'", Datum::Bool(true)),
            ("5 = ' 5'", Datum::Bool(true)),
            ("-1 = '-1'", Datum::Bool(true)),
            // Every comparison, not just equality.
            ("1 < '2'", Datum::Bool(true)),
            ("1 >= '2'", Datum::Bool(false)),
            ("1 <> '2'", Datum::Bool(true)),
            // numeric and the floats.
            ("1.5 = '1.5'", Datum::Bool(true)),
            ("1.5 = '1.50'", Datum::Bool(true)),
            ("1.5::float8 = '1.5'", Datum::Bool(true)),
            ("1.5::float4 = '1.5'", Datum::Bool(true)),
            ("'NaN'::float8 = 'NaN'", Datum::Bool(true)),
            // boolean, whose input syntax accepts far more than `t`/`f`.
            ("true = 't'", Datum::Bool(true)),
            ("'t' = true", Datum::Bool(true)),
            ("true = 'yes'", Datum::Bool(true)),
            ("true > 'f'", Datum::Bool(true)),
            ("true AND 't'", Datum::Bool(true)),
            // bytea and the date/time family.
            ("'\\x0102'::bytea > '\\x01'", Datum::Bool(true)),
            ("'2020-01-01'::date = '2020-01-01'", Datum::Bool(true)),
            ("'01:02:03'::time = '01:02:03'", Datum::Bool(true)),
            ("'1 day'::interval = '1 day'", Datum::Bool(true)),
            // `BETWEEN`, `IN` and a simple `CASE` expand into comparisons that
            // each resolve their own literal — so a pair of bare literals still
            // compares as text ('10' < '9') even with a typed sibling nearby.
            ("2 BETWEEN '1' AND '3'", Datum::Bool(true)),
            ("'2' BETWEEN 1 AND 3", Datum::Bool(true)),
            ("'10' BETWEEN '9' AND 20", Datum::Bool(false)),
            ("1 IN ('1','2')", Datum::Bool(true)),
            ("'1' IN (1,2)", Datum::Bool(true)),
            ("1 NOT IN ('2')", Datum::Bool(true)),
            ("'10' IN ('9', 20)", Datum::Bool(false)),
            ("'2020-01-01'::date IN ('2020-01-01')", Datum::Bool(true)),
            (
                "CASE 1 WHEN '1' THEN 'y' ELSE 'n' END",
                Datum::Text("y".into()),
            ),
            (
                "CASE 1 WHEN '2' THEN 'y' ELSE 'n' END",
                Datum::Text("n".into()),
            ),
            ("1 IS DISTINCT FROM '1'", Datum::Bool(false)),
            ("1 IS NOT DISTINCT FROM '1'", Datum::Bool(true)),
            // Arithmetic and the bitwise operators resolve the same way.
            ("1 + '1'", Datum::Int4(2)),
            ("'1' + 1", Datum::Int4(2)),
            ("1 - '1'", Datum::Int4(0)),
            ("6 / '2'", Datum::Int4(3)),
            ("7 % '3'", Datum::Int4(1)),
            ("2 ^ '3'", Datum::Float8(8.0)),
            ("1::int8 * '3'", Datum::Int8(3)),
            ("1.5 * '2' = 3.0", Datum::Bool(true)),
            ("1 & '3'", Datum::Int4(1)),
            ("1 | '2'", Datum::Int4(3)),
            ("1 # '3'", Datum::Int4(2)),
            ("1 << '2'", Datum::Int4(4)),
            ("8 >> '2'", Datum::Int4(2)),
        ];
        for (sql, expected) in cases {
            assert2::assert!(ev(sql, None, &[]) == *expected, "{sql}");
        }
    }

    /// Resolving the literal fixes the operator's *result* type too: `1 + '1'`
    /// is an `integer` sum in PostgreSQL, not a pair widened through an
    /// unresolved operand.
    #[test]
    fn an_adopted_literal_sets_the_operators_result_type() {
        // (expression, inferred type)
        let cases: &[(&str, ColumnType)] = &[
            ("1 + '1'", ColumnType::Int4),
            ("'1' + 1", ColumnType::Int4),
            ("1::int2 + '1'", ColumnType::Int2),
            ("1::int8 + '1'", ColumnType::Int8),
            ("1.5 + '1'", ColumnType::Numeric(None)),
            ("1::float8 / '4'", ColumnType::Float8),
            ("7 % '3'", ColumnType::Int4),
            ("2 ^ '3'", ColumnType::Float8),
            ("1 & '3'", ColumnType::Int4),
            ("1 << '2'", ColumnType::Int4),
        ];
        for (sql, expected) in cases {
            let got = infer_type(&pexpr(sql).expect("parse"), &Scope::empty()).expect("infer");
            assert2::assert!(got == *expected, "{sql}");
        }
    }

    /// Adopting a type is not the same as accepting anything. A literal that is
    /// not valid input for the type it adopted fails with that type's own input
    /// error, and a value that is *genuinely* `text` never adopts at all —
    /// PostgreSQL has no `integer = text` operator and neither does crabka.
    #[test]
    fn only_an_unknown_literal_adopts_and_only_when_it_parses() {
        // (expression, message fragment)
        let cases: &[(&str, &str)] = &[
            (
                "1 = 'abc'",
                "invalid input syntax for type integer: \"abc\"",
            ),
            ("1 = ''", "invalid input syntax for type integer: \"\""),
            ("1 > ''", "invalid input syntax for type integer: \"\""),
            (
                "true = 'xyz'",
                "invalid input syntax for type boolean: \"xyz\"",
            ),
            (
                "1.5 = 'abc'",
                "invalid input syntax for type numeric: \"abc\"",
            ),
            (
                "1::int2 = '99999'",
                "value \"99999\" is out of range for type smallint",
            ),
            (
                "1 IN ('abc')",
                "invalid input syntax for type integer: \"abc\"",
            ),
            (
                "2 BETWEEN '1' AND 'abc'",
                "invalid input syntax for type integer: \"abc\"",
            ),
            (
                "CASE 1 WHEN 'abc' THEN 'y' END",
                "invalid input syntax for type integer: \"abc\"",
            ),
            // Genuinely `text`: an explicit cast, and a text-returning function.
            ("1 = '1'::text", "cannot compare integer and text"),
            ("1 = upper('1')", "cannot compare integer and text"),
            ("1 + '1'::text", "operator does not exist: integer + text"),
        ];
        for (sql, fragment) in cases {
            let message = ev_err(sql).into_pg().message;
            assert2::assert!(message.contains(fragment), "{sql} produced {message}");
        }
    }

    #[test]
    fn boolean_tests_are_total_over_their_operand() {
        // (expression, result) — every cell of PostgreSQL 18's truth table.
        let cases: &[(&str, bool)] = &[
            ("true IS TRUE", true),
            ("false IS TRUE", false),
            ("null IS TRUE", false),
            ("true IS NOT TRUE", false),
            ("false IS NOT TRUE", true),
            ("null IS NOT TRUE", true),
            ("true IS FALSE", false),
            ("false IS FALSE", true),
            ("null IS FALSE", false),
            ("true IS NOT FALSE", true),
            ("false IS NOT FALSE", false),
            ("null IS NOT FALSE", true),
            ("true IS UNKNOWN", false),
            ("null IS UNKNOWN", true),
            ("null IS NOT UNKNOWN", false),
            ("true IS NOT UNKNOWN", true),
            // A bare string literal is still `unknown` and adopts boolean.
            ("'t' IS TRUE", true),
            ("'f' IS UNKNOWN", false),
        ];
        for (sql, expected) in cases {
            assert2::assert!(ev(sql, None, &[]) == Datum::Bool(*expected), "{sql}");
        }
    }

    #[test]
    fn a_non_boolean_boolean_test_is_42804_and_a_bad_literal_is_22p02() {
        for (sql, spelling) in [
            ("1 IS TRUE", "IS TRUE"),
            ("1 IS NOT TRUE", "IS NOT TRUE"),
            ("1 IS FALSE", "IS FALSE"),
            ("1 IS NOT FALSE", "IS NOT FALSE"),
            ("1 IS UNKNOWN", "IS UNKNOWN"),
            ("1 IS NOT UNKNOWN", "IS NOT UNKNOWN"),
        ] {
            let error = infer_err(sql).into_pg();
            assert2::assert!(error.code == "42804", "{sql}");
            assert2::assert!(
                error.message
                    == format!("argument of {spelling} must be type boolean, not type integer"),
                "{sql}"
            );
        }
        // An `unknown` literal type-checks, then fails to convert at run time —
        // exactly as it does in PostgreSQL.
        assert2::assert!(ev_err("'x' IS TRUE").into_pg().code == "22P02");
    }

    #[test]
    fn is_distinct_from_is_null_safe_and_never_returns_null() {
        let cases: &[(&str, bool)] = &[
            ("null IS DISTINCT FROM null", false),
            ("1 IS DISTINCT FROM null", true),
            ("null IS DISTINCT FROM 1", true),
            ("1 IS DISTINCT FROM 1", false),
            ("1 IS DISTINCT FROM 2", true),
            ("null IS NOT DISTINCT FROM null", true),
            ("1 IS NOT DISTINCT FROM null", false),
            ("1 IS NOT DISTINCT FROM 1", true),
        ];
        for (sql, expected) in cases {
            assert2::assert!(ev(sql, None, &[]) == Datum::Bool(*expected), "{sql}");
        }
    }

    #[test]
    fn typed_literal_constants_evaluate_as_their_cast() {
        let cases: &[(&str, Datum)] = &[
            ("bool 't'", Datum::Bool(true)),
            ("int4 '0'", Datum::Int4(0)),
            ("text 'x'", Datum::Text("x".into())),
            ("date '2024-01-01'", ev("'2024-01-01'::date", None, &[])),
            ("numeric '1.50'", ev("'1.50'::numeric", None, &[])),
        ];
        for (sql, expected) in cases {
            assert2::assert!(ev(sql, None, &[]) == *expected, "{sql}");
        }
        // The error is the cast's error, code and message alike.
        let literal = ev_err("bool 'test'").into_pg();
        let cast = ev_err("'test'::bool").into_pg();
        assert2::assert!(literal.code == cast.code && literal.message == cast.message);
        assert2::assert!(literal.code == "22P02");
    }

    #[test]
    fn similar_to_and_the_escape_clause_evaluate() {
        let cases: &[(&str, Datum)] = &[
            ("'abc' SIMILAR TO 'a%'", Datum::Bool(true)),
            ("'abc' SIMILAR TO 'a|b'", Datum::Bool(false)),
            ("'abc' SIMILAR TO '(a|b)bc'", Datum::Bool(true)),
            ("'abc' NOT SIMILAR TO 'a%'", Datum::Bool(false)),
            ("'abc' SIMILAR TO 'a.c'", Datum::Bool(false)),
            ("null SIMILAR TO 'a'", Datum::Null),
            ("'abc' SIMILAR TO null", Datum::Null),
            ("'a%c' SIMILAR TO 'a#%c' ESCAPE '#'", Datum::Bool(true)),
            ("'a_c' LIKE 'aX_c' ESCAPE 'X'", Datum::Bool(true)),
            ("'aXc' LIKE 'aXc' ESCAPE 'X'", Datum::Bool(false)),
            ("'ABC' ILIKE 'aXbc' ESCAPE 'X'", Datum::Bool(true)),
            ("'a_c' LIKE 'a_c' ESCAPE ''", Datum::Bool(true)),
        ];
        for (sql, expected) in cases {
            assert2::assert!(ev(sql, None, &[]) == *expected, "{sql}");
        }
        for sql in [
            "'abc' LIKE 'abc' ESCAPE 'xy'",
            "'a' SIMILAR TO 'a' ESCAPE 'xy'",
        ] {
            let error = ev_err(sql).into_pg();
            assert2::assert!(error.code == "22025", "{sql}");
            assert2::assert!(error.message == "invalid escape string", "{sql}");
        }
    }

    #[test]
    fn row_expressions_compare_field_wise_and_render_as_composite_text() {
        // A bare row value is a `record` (OID 2249), as `pg_typeof(ROW(1,2))`
        // reports on the oracle — not text. What the SQL contract fixes is the
        // *rendering*, so these four assert the composite text the wire sends
        // rather than the datum's representation.
        for (sql, expected) in [
            ("ROW(1, 2)", "(1,2)"),
            ("(1, 2)", "(1,2)"),
            ("ROW(1, 'a', null, true)", "(1,a,,t)"),
            ("ROW('a b', '')", "(\"a b\",\"\")"),
        ] {
            let value = ev(sql, None, &[]);
            let bytes = crabka_pgtypes::encoding::encode_text(&value, &jiff::tz::TimeZone::UTC);
            let rendered = String::from_utf8(bytes).expect("composite text is utf-8");
            assert!(rendered == expected, "{sql}: {rendered} != {expected}");
        }

        let cases: &[(&str, Datum)] = &[
            ("(1,2) = (1,2)", Datum::Bool(true)),
            ("(1,2) < (1,3)", Datum::Bool(true)),
            ("(2,1) > (1,9)", Datum::Bool(true)),
            // A field that already decided outranks a later NULL; one reached
            // first makes the whole comparison NULL.
            ("(1,null) < (2,2)", Datum::Bool(true)),
            ("(1,2) < (1,null)", Datum::Null),
            ("(1,null) = (1,2)", Datum::Null),
            ("(1,null) = (2,2)", Datum::Bool(false)),
            // IS NULL / IS NOT NULL are field-wise, not negations of each other.
            ("ROW(1,null) IS NULL", Datum::Bool(false)),
            ("ROW(1,null) IS NOT NULL", Datum::Bool(false)),
            ("ROW(null,null) IS NULL", Datum::Bool(true)),
            ("ROW(1,2) IS NOT NULL", Datum::Bool(true)),
            ("(1,2) IN ((1,2),(3,4))", Datum::Bool(true)),
            ("(5,6) IN ((1,2),(3,4))", Datum::Bool(false)),
            ("(1,2) NOT IN ((1,2),(3,4))", Datum::Bool(false)),
            ("ROW(1,2) IS DISTINCT FROM ROW(1,null)", Datum::Bool(true)),
            ("(1,null) IS NOT DISTINCT FROM (1,null)", Datum::Bool(true)),
        ];
        for (sql, expected) in cases {
            assert2::assert!(ev(sql, None, &[]) == *expected, "{sql}");
        }
    }

    /// Defense-in-depth. An expression tree deeper than `MAX_EVAL_DEPTH`, built
    /// DIRECTLY here to get past the parser's parse-time cap, must return a
    /// clean `54001` from `eval` and must never overflow the stack. In
    /// production the parser cap means such a tree cannot be built, but the
    /// guard must still hold.
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

    /// A tree exactly at the limit still evaluates, because the guard does not
    /// fire early.
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

    /// The static half of the `int2`/`float4` promotion rules, which is what
    /// `RowDescription` reports. Every expectation is `pg_typeof(...)` on
    /// PostgreSQL 18.4.
    #[test]
    fn int2_and_float4_result_types_match_postgres() {
        use assert2::assert;
        let f4 = ColumnType::Float4;
        let f8 = ColumnType::Float8;
        let num = ColumnType::Numeric(None);
        let cases: &[(&str, ColumnType)] = &[
            // Arithmetic keeps int2 only when BOTH operands are int2.
            ("1::int2 + 1::int2", ColumnType::Int2),
            ("1::int2 - 1::int2", ColumnType::Int2),
            ("1::int2 * 1::int2", ColumnType::Int2),
            ("1::int2 / 1::int2", ColumnType::Int2),
            ("1::int2 % 1::int2", ColumnType::Int2),
            ("1::int2 + 1::int4", ColumnType::Int4),
            ("1::int4 + 1::int2", ColumnType::Int4),
            ("1::int2 + 1::int8", ColumnType::Int8),
            ("1::int2 + 1.5", num),
            ("1::int2 + 1::float8", f8),
            ("- (1::int2)", ColumnType::Int2),
            ("@ (1::int2)", ColumnType::Int2),
            ("abs(1::int2)", ColumnType::Int2),
            // `float4 ⊕ float4` is the only single-precision pairing.
            ("1::float4 + 1::float4", f4),
            ("1::float4 / 1::float4", f4),
            ("- (1::float4)", f4),
            ("abs(1::float4)", f4),
            ("1::float4 + 1::int4", f8),
            ("1::float4 + 1::int2", f8),
            ("1::float4 + 1::int8", f8),
            ("1::float4 + 1::float8", f8),
            ("1::float4 + 1.5", f8),
            // `real` has no overload of these, so they resolve to `float8`.
            ("floor(1::float4)", f8),
            ("round(1::float4)", f8),
            ("sign(1::float4)", f8),
            ("sqrt(4::float4)", f8),
            // Bitwise ops follow the same width ladder as arithmetic.
            ("1::int2 & 3::int2", ColumnType::Int2),
            ("1::int2 & 3::int4", ColumnType::Int4),
            ("1::int2 << 2", ColumnType::Int2),
            ("~ (1::int2)", ColumnType::Int2),
            // CASE / COALESCE unification (PostgreSQL `select_common_type`).
            ("coalesce(1::int2, 2::int2)", ColumnType::Int2),
            ("coalesce(1::int2, 2::int4)", ColumnType::Int4),
            ("coalesce(1::int2, 2::int8)", ColumnType::Int8),
            ("coalesce(1::int2, 2.5)", num),
            // `numeric → float4` is implicit but `float4 → numeric` is not, so
            // `real` wins the unification against `numeric` and `int2`.
            ("coalesce(1::int2, 2::float4)", f4),
            ("coalesce(1::float4, 2.5)", f4),
            ("coalesce(1::float4, 2::float8)", f8),
            ("greatest(1::int2, 2::int2)", ColumnType::Int2),
            ("least(1::int2, 2::int4)", ColumnType::Int4),
        ];
        for (sql, expected) in cases {
            let got = infer_type(&pexpr(sql).expect("parse"), &scope_of(None)).expect("infer");
            assert!(got == *expected, "{sql}");
        }
    }

    /// The runtime half, cross-checked against the static half above. The value
    /// an expression evaluates to carries the type `infer_type` promised.
    #[test]
    fn int2_and_float4_evaluation_agrees_with_inference() {
        use assert2::assert;
        let cases: &[(&str, Datum)] = &[
            ("1::int2 + 2::int2", Datum::Int2(3)),
            ("1::int2 + 2::int4", Datum::Int4(3)),
            ("7::int2 % 2::int2", Datum::Int2(1)),
            ("- (100::int2)", Datum::Int2(-100)),
            ("abs((-100)::int2)", Datum::Int2(100)),
            ("1::int2 & 3::int2", Datum::Int2(1)),
            ("1::int2 << 2", Datum::Int2(4)),
            ("1.5::float4 + 2.5::float4", Datum::Float4(4.0)),
            ("1.5::float4 + 2::int4", Datum::Float8(3.5)),
            ("abs((-1.5)::float4)", Datum::Float4(1.5)),
            ("floor(2.9::float4)", Datum::Float8(2.0)),
            ("'5'::int2", Datum::Int2(5)),
            ("int2 '5'", Datum::Int2(5)),
            ("real '1.5'", Datum::Float4(1.5)),
        ];
        for (sql, expected) in cases {
            let value = ev(sql, None, &[]);
            assert!(value == *expected, "{sql}");
            let inferred = infer_type(&pexpr(sql).expect("parse"), &scope_of(None)).expect("infer");
            assert!(
                value.column_type() == Some(inferred),
                "{sql}: evaluated {value:?} but RowDescription says {inferred:?}"
            );
        }
        // `-((-32768)::int2)` and `32767::int2 + 1::int2` are 22003, not widened.
        for sql in ["- ((-32768)::int2)", "32767::int2 + 1::int2"] {
            let err = ev_err(sql);
            assert!(err.into_pg().code == "22003", "{sql}");
        }
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
            ev("-1 * 11528652096115048448::numeric", None, &[]),
            num("-11528652096115048448")
        );
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

    /// SP37 §8: the tz-AWARE temporal cells in `apply_binary`, which are there
    /// because they need the session zone. They are
    /// `timestamptz ± interval → timestamptz` and
    /// `timestamptz − timestamptz → interval`. Each case asserts BOTH the
    /// produced value AND that `infer_type` predicts the same type, so there is
    /// no infer/eval mismatch.
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
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public("tz"),
            columns: vec![
                Column::new("ts", ColumnType::Timestamptz),
                Column::new("iv", ColumnType::Interval),
            ],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            materialized: None,
            checks: Vec::new(),
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
    fn an_unbound_parameter_is_42p02() {
        let ctx = crate::clock::EvalCtx::test_default();
        let err = eval(&pexpr("$1").expect("parse"), &scope_of(None), &[], &ctx)
            .expect_err("eval $1 should fail");
        let err = err.into_pg();
        // PostgreSQL reports an unbound placeholder as undefined, not as an
        // unimplemented feature: the simple protocol supplies no parameters, so
        // `$1` names nothing rather than asking for something gres cannot do.
        assert_eq!(err.code, "42P02");
        assert_eq!(err.message, "there is no parameter $1");
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
        let bs = Some('\\');
        assert!(like_match("abc", "a%", false, bs).expect("m"));
        assert!(like_match("abc", "a_c", false, bs).expect("m"));
        assert!(!like_match("ac", "a_c", false, bs).expect("m"));
        assert!(like_match("anything", "%", false, bs).expect("m"));
        assert!(like_match("", "%", false, bs).expect("m"));
        assert!(like_match("axyzc", "a%c", false, bs).expect("m"));
        assert!(!like_match("abd", "a%c", false, bs).expect("m"));
        // `\` escapes the next pattern char: `a\%b` matches a literal `%`.
        assert!(like_match("a%b", "a\\%b", false, bs).expect("m"));
        assert!(!like_match("axb", "a\\%b", false, bs).expect("m"));
        // ILIKE folds ASCII case.
        assert!(like_match("ABC", "a%", true, bs).expect("m"));
        assert!(!like_match("ABC", "a%", false, bs).expect("m"));
        // An `ESCAPE` clause replaces the escape character outright, so the
        // default `\` is then an ordinary literal and the new one escapes.
        assert!(like_match("a%b", "a#%b", false, Some('#')).expect("m"));
        assert!(like_match("a\\b", "a\\b", false, Some('#')).expect("m"));
        // `ESCAPE ''` disables escaping entirely.
        assert!(like_match("a\\b", "a\\b", false, None).expect("m"));
        // A pattern ending in a lone escape character is an invalid escape
        // (22025) — but only while subject characters remain; once the subject
        // is exhausted the pattern simply fails to match.
        assert_eq!(
            like_match("ab", "a\\", false, bs)
                .expect_err("invalid escape")
                .into_pg()
                .code,
            "22025"
        );
        assert!(!like_match("a", "a\\", false, bs).expect("m"));
        assert!(!like_match("a", "a%", false, Some('%')).expect("m"));
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
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public("t"),
            columns: vec![Column::new("a", ColumnType::Float8)],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            materialized: None,
            checks: Vec::new(),
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
        // A bare string is `unknown`, adopts int4 from the other branch, and
        // is parsed only if that branch is selected.
        assert_eq!(
            infer_type(
                &pexpr("case when a > 0 then 1 else 'x' end").expect("parse"),
                &scope,
            )
            .expect("infer"),
            ColumnType::Int4,
        );
        assert_eq!(
            err_code("case when false then 1 else 'x' end", None, &[]),
            "22P02"
        );
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

    /// A relation that covers every operand family the new operators resolve
    /// over.
    fn jt() -> Table {
        Table {
            id: 2,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public("jt"),
            columns: vec![
                Column::new("j", ColumnType::Jsonb),
                Column::new("ia", ColumnType::Array(ElemType::Int4)),
                Column::new("ta", ColumnType::Array(ElemType::Text)),
                Column::new("i", ColumnType::Int4),
                Column::new("s", ColumnType::Text),
                Column::new("jn", ColumnType::Json),
            ],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            materialized: None,
            checks: Vec::new(),
        }
    }

    fn jb(text: &str) -> Datum {
        Datum::Jsonb(crabka_pgtypes::jsonb::parse(text).expect("jsonb literal"))
    }

    /// A `json` value, which is its input text and nothing else.
    fn jn(text: &str) -> Datum {
        crabka_pgtypes::json::validate(text).expect("json literal");
        Datum::Json(text.to_string())
    }

    /// The `json` document bound to `jt.jn`. Its spacing, key order and escape
    /// are all load-bearing: `jsonb` would erase every one of them, so a `json`
    /// operator that answered from a re-serialized document would show up here.
    const JSON_ROW: &str = r#"{"a":{"b":  1}, "c": [1,  2], "s": "x\ty"}"#;

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
            jn(JSON_ROW),
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
    /// `array_fn` rules, so plan-time typing and evaluation never disagree.
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
    /// three-valued cells that tell them apart.
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

    // ---- `json`, which is a different type from `jsonb` ----

    /// The 42883 every `json` rejection carries, verbatim from PostgreSQL 18.4.
    const NO_OPERATOR_HINT: &str = "No operator matches the given name and argument types. \
                                    You might need to add explicit type casts.";

    /// The SQLSTATE, message and HINT a plan-time failure reports.
    fn plan_failure(sql: &str) -> (String, String, Option<String>) {
        let error = infer_jt(sql).expect_err("must not resolve").into_pg();
        let hint = error.diagnostics.as_ref().and_then(|d| d.hint.clone());
        (error.code, error.message, hint)
    }

    /// PostgreSQL gives `json` six operators and no more: `->` and `->>` by
    /// object key and by array index, and `#>` / `#>>` by path. `->`/`#>` yield
    /// `json`; `->>`/`#>>` yield `text`.
    #[test]
    fn json_infers_the_result_types_of_its_six_operators() {
        let cases: &[(&str, ColumnType)] = &[
            ("jn -> 'a'", ColumnType::Json),
            ("jn -> 0", ColumnType::Json),
            ("jn ->> 'a'", ColumnType::Text),
            ("jn ->> 0", ColumnType::Text),
            ("jn #> ARRAY['a']", ColumnType::Json),
            ("jn #>> ARRAY['a']", ColumnType::Text),
            // An `unknown` literal beside `#>`/`#>>` adopts `text[]` — the only
            // adoption `json` makes, since its other two operators take the
            // `text` the literal already is.
            ("jn #> '{a}'", ColumnType::Json),
            ("jn #>> '{a}'", ColumnType::Text),
            // `json` has NO `||`; `anynonarray || text` renders the document.
            ("jn || s", ColumnType::Text),
            ("s || jn", ColumnType::Text),
            // The `jsonb` column in the same table still resolves to `jsonb`,
            // so neither type is answering for the other.
            ("j -> 'a'", ColumnType::Jsonb),
            ("j ->> 'a'", ColumnType::Text),
        ];
        for (sql, want) in cases {
            assert2::assert!(infer_jt(sql).expect("infer") == *want, "for {sql}");
        }
    }

    /// `json` returns the ORIGINAL text of the sub-document, spacing and escapes
    /// intact — the one property that makes it a different type from `jsonb`,
    /// which would return `{"b": 1}` for every one of these.
    #[test]
    fn json_operators_return_the_original_sub_text() {
        let cases: &[(&str, Datum)] = &[
            ("jn -> 'a'", jn(r#"{"b":  1}"#)),
            ("jn ->> 'a'", Datum::Text(r#"{"b":  1}"#.into())),
            ("jn -> 'c' -> 0", jn("1")),
            // A negative array index counts from the end.
            ("jn -> 'c' -> -1", jn("2")),
            // `->` keeps a JSON string quoted and escaped; `->>` de-escapes it.
            ("jn -> 's'", jn(r#""x\ty""#)),
            ("jn ->> 's'", Datum::Text("x\ty".into())),
            ("jn #> ARRAY['a', 'b']", jn("1")),
            ("jn #>> ARRAY['a', 'b']", Datum::Text("1".into())),
            ("jn #> '{a,b}'", jn("1")),
            // A missing key, and an array index into an object, are SQL NULL
            // rather than an error.
            ("jn -> 'zz'", Datum::Null),
            ("jn ->> 0", Datum::Null),
            // All six are strict.
            ("null::json -> 'a'", Datum::Null),
            ("jn -> null", Datum::Null),
            ("jn #> null", Datum::Null),
            // `json || text` renders the document, which is `anynonarray || text`.
            ("jn || s", Datum::Text(format!("{JSON_ROW}a"))),
        ];
        for (sql, want) in cases {
            assert2::assert!(eval_jt(sql).expect("eval") == *want, "for {sql}");
        }
    }

    /// Every operator PostgreSQL does NOT give `json`, with its exact message
    /// and HINT. Without this the fall-throughs are silent rather than wrong:
    /// `=` would order two documents as text and `-` would report a 42804.
    #[test]
    fn json_reports_42883_for_every_operator_it_does_not_have() {
        let cases: &[(&str, &str)] = &[
            // No equality and no ordering, at any spelling that resolves one.
            ("jn = jn", "operator does not exist: json = json"),
            ("jn <> jn", "operator does not exist: json <> json"),
            ("jn < jn", "operator does not exist: json < json"),
            ("jn <= jn", "operator does not exist: json <= json"),
            ("jn > jn", "operator does not exist: json > json"),
            ("jn >= jn", "operator does not exist: json >= json"),
            (
                "jn IS DISTINCT FROM jn",
                "operator does not exist: json = json",
            ),
            (
                "jn IS NOT DISTINCT FROM jn",
                "operator does not exist: json = json",
            ),
            ("jn IN (jn)", "operator does not exist: json = json"),
            (
                "jn BETWEEN jn AND jn",
                "operator does not exist: json >= json",
            ),
            (
                "CASE jn WHEN jn THEN 1 END",
                "operator does not exist: json = json",
            ),
            (
                "jn = ANY(ARRAY[jn])",
                "operator does not exist: json = json",
            ),
            // `json` is not `jsonb` and not `text`, in either direction.
            ("jn = j", "operator does not exist: json = jsonb"),
            ("jn = s", "operator does not exist: json = text"),
            ("jn @> j", "operator does not exist: json @> jsonb"),
            ("j @> jn", "operator does not exist: jsonb @> json"),
            // PostgreSQL leaves a literal beside a `json` operand `unknown`,
            // because there is no operator for it to adopt a type from.
            ("jn = '{}'", "operator does not exist: json = unknown"),
            ("'{}' = jn", "operator does not exist: unknown = json"),
            // The jsonb-only containment, existence and jsonpath operators.
            ("jn @> jn", "operator does not exist: json @> json"),
            ("jn <@ jn", "operator does not exist: json <@ json"),
            ("jn && jn", "operator does not exist: json && json"),
            ("jn ? s", "operator does not exist: json ? text"),
            (
                "jn ?| ARRAY['a']",
                "operator does not exist: json ?| text[]",
            ),
            (
                "jn ?& ARRAY['a']",
                "operator does not exist: json ?& text[]",
            ),
            // `||` and `-` resolve for `jsonb` and for nothing about `json`.
            ("jn || jn", "operator does not exist: json || json"),
            ("jn - s", "operator does not exist: json - text"),
            ("jn - 1", "operator does not exist: json - integer"),
            // The six that DO exist still reject an operand they have no
            // overload for.
            ("jn -> true", "operator does not exist: json -> boolean"),
            (
                "jn #> ARRAY[1]",
                "operator does not exist: json #> integer[]",
            ),
            // …and no other left operand acquires them.
            ("s -> s", "operator does not exist: text -> text"),
            ("i #> ta", "operator does not exist: integer #> text[]"),
            ("s #>> ta", "operator does not exist: text #>> text[]"),
        ];
        for (sql, message) in cases {
            assert2::assert!(
                plan_failure(sql)
                    == (
                        "42883".to_string(),
                        (*message).to_string(),
                        Some(NO_OPERATOR_HINT.to_string())
                    ),
                "for {sql}"
            );
        }
    }

    /// Only `jsonb` has a subscript handler, so a `json` base is 42804 — at
    /// plan time and at value time, for a single subscript and for a chain.
    #[test]
    fn json_does_not_support_subscripting() {
        let want = (
            "42804".to_string(),
            "cannot subscript type json because it does not support subscripting".to_string(),
        );
        for sql in ["jn['a']", "jn[1]", "jn['a']['b']"] {
            let planned = infer_jt(sql).expect_err("no subscripting").into_pg();
            assert2::assert!((planned.code, planned.message) == want, "planning {sql}");
            let evaluated = eval_jt(sql).expect_err("no subscripting").into_pg();
            assert2::assert!(
                (evaluated.code, evaluated.message) == want,
                "evaluating {sql}"
            );
        }
        // `jsonb`'s subscripting is untouched.
        assert2::assert!(eval_jt("j['a']").expect("jsonb subscript") == jb("1"));
    }

    /// `json` has no default btree operator class, so everything that needs one
    /// — GROUP BY, DISTINCT, ORDER BY, the set operations, a window PARTITION —
    /// names the missing operator rather than inventing a text order.
    #[test]
    fn json_has_no_btree_operator_class() {
        let cases: &[(ColumnType, &str)] = &[
            (ColumnType::Json, "json"),
            (ColumnType::Array(ElemType::Json), "json[]"),
        ];
        for (ty, name) in cases {
            let equality = require_equality_operator(*ty)
                .expect_err("no equality operator")
                .into_pg();
            assert2::assert!(
                (equality.code.as_str(), equality.message.as_str())
                    == (
                        "42883",
                        format!("could not identify an equality operator for type {name}").as_str()
                    ),
                "for {name}"
            );
            let ordering = require_ordering_operator(*ty)
                .expect_err("no ordering operator")
                .into_pg();
            assert2::assert!(
                (ordering.code.as_str(), ordering.message.as_str())
                    == (
                        "42883",
                        format!("could not identify an ordering operator for type {name}").as_str()
                    ),
                "for {name}"
            );
        }
        // `jsonb` normalizes on input and therefore HAS the opclass.
        assert2::assert!(require_equality_operator(ColumnType::Jsonb).is_ok());
        assert2::assert!(require_ordering_operator(ColumnType::Jsonb).is_ok());
    }

    /// The same rejections from the VALUES alone — the path `agg`'s grouped
    /// evaluator takes, which has no operand expressions to type first.
    #[test]
    fn json_values_reject_the_operators_json_does_not_have() {
        let ctx = crate::clock::EvalCtx::test_default();
        let doc = jn("{}");
        let text = Datum::Text("a".into());
        let cases: &[(BinaryOp, &Datum, &str)] = &[
            (BinaryOp::Eq, &doc, "operator does not exist: json = json"),
            (BinaryOp::Ne, &doc, "operator does not exist: json <> json"),
            (BinaryOp::Lt, &doc, "operator does not exist: json < json"),
            (BinaryOp::Ge, &doc, "operator does not exist: json >= json"),
            (
                BinaryOp::IsDistinctFrom,
                &doc,
                "operator does not exist: json = json",
            ),
            (
                BinaryOp::Contains,
                &doc,
                "operator does not exist: json @> json",
            ),
            (
                BinaryOp::ContainedBy,
                &doc,
                "operator does not exist: json <@ json",
            ),
            (BinaryOp::Sub, &text, "operator does not exist: json - text"),
            (BinaryOp::Add, &text, "operator does not exist: json + text"),
            (
                BinaryOp::KeyExists,
                &text,
                "operator does not exist: json ? text",
            ),
            (
                BinaryOp::JsonPathMatch,
                &doc,
                "operator does not exist: json @@ json",
            ),
        ];
        for (op, right, message) in cases {
            let error = apply_binary(*op, &doc, right, &ctx)
                .expect_err("no operator")
                .into_pg();
            assert2::assert!(
                (error.code.as_str(), error.message.as_str()) == ("42883", *message),
                "for {}",
                op_spelling(*op)
            );
        }
        // `||` is the one shared spelling `json` really resolves, and it must
        // NOT be turned away with the rest.
        assert2::assert!(
            apply_binary(BinaryOp::Concat, &doc, &text, &ctx).expect("concat")
                == Datum::Text("{}a".into())
        );
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

    /// jsonb `-` shares the `Sub` operator with arithmetic. The LEFT operand's
    /// type decides which one applies, and every numeric/temporal pair is
    /// untouched.
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

    /// `ARRAY[…]` unifies its elements exactly as `CASE` does. `ARRAY[]` has
    /// nothing to unify and needs a cast, which is 42P18. An all-NULL list
    /// falls back to `text`, which matches PostgreSQL's `unknown` → `text`.
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
        // A bare string is `unknown` and adopts int4 from the typed element.
        assert2::assert!(
            infer_jt("ARRAY[1, 'x']").expect("infer") == ColumnType::Array(ElemType::Int4)
        );
        assert2::assert!(eval_jt("ARRAY[1, '2']").expect("eval") == int_array(&[1, 2]));
        assert2::assert!(
            eval_jt("ARRAY[1, 'x']")
                .expect_err("invalid integer input")
                .into_pg()
                .code
                == "22P02"
        );
    }

    /// Subscripting is 1-based, and an out-of-range or NULL subscript gives SQL
    /// NULL instead of an error. A non-array base has no subscripting operator
    /// at all.
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

    /// `ANY`/`ALL` over an array, including the three-valued cells. An
    /// unmatched `ANY` over an array that contains NULL is UNKNOWN, not false.
    /// An empty array is false for `ANY` and true for `ALL`. A NULL array is
    /// NULL for both.
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
    /// from static inference. The guard chains wire them in beside the older
    /// scalar/datetime/format families.
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

    #[test]
    fn regex_match_operators_evaluate_against_posix_patterns() {
        use assert2::assert;

        let cases: &[(&str, Datum)] = &[
            ("'abc' ~ 'b'", Datum::Bool(true)),
            ("'abc' ~ 'B'", Datum::Bool(false)),
            ("'abc' ~* 'B'", Datum::Bool(true)),
            ("'abc' !~ 'B'", Datum::Bool(true)),
            ("'abc' !~* 'B'", Datum::Bool(false)),
            ("'abc' ~ '^a'", Datum::Bool(true)),
            ("'abc' ~ 'c$'", Datum::Bool(true)),
            ("'abc' ~ '^b'", Datum::Bool(false)),
            ("'abc' ~ '(b|z)'", Datum::Bool(true)),
            ("'abc' ~ '[[:alpha:]]+'", Datum::Bool(true)),
            ("'ABC' ~* 'a.c'", Datum::Bool(true)),
            ("'a' ~ ''", Datum::Bool(true)),
            // A match is unanchored, so a pattern that only matches part of the
            // subject still matches — the same rule PostgreSQL uses.
            ("'xxbyy' ~ 'b'", Datum::Bool(true)),
            // Strict: either NULL operand yields NULL, never false.
            ("null ~ 'a'", Datum::Null),
            ("'a' ~ null", Datum::Null),
        ];
        for (sql, want) in cases {
            assert!(ev(sql, None, &[]) == *want, "{sql}");
        }
        // A non-text operand has no `~` operator (42883).
        assert!(err_code("'abc' ~ 1", None, &[]) == "42883");
        // An uncompilable pattern is 2201B, PostgreSQL's invalid_regular_expression.
        assert!(err_code("'abc' ~ '['", None, &[]) == "2201B");
    }

    #[test]
    fn bitwise_operators_evaluate_with_postgres_width_and_shift_rules() {
        use assert2::assert;

        let cases: &[(&str, Datum)] = &[
            ("5 & 3", Datum::Int4(1)),
            ("5 | 3", Datum::Int4(7)),
            ("5 # 3", Datum::Int4(6)),
            ("~5", Datum::Int4(-6)),
            ("1 << 3", Datum::Int4(8)),
            ("16 >> 2", Datum::Int4(4)),
            ("0 & 0", Datum::Int4(0)),
            // A mixed-width pair widens to int8, as PostgreSQL's implicit
            // int4 -> int8 coercion does.
            ("5::int8 & 3", Datum::Int8(1)),
            ("~5::int8", Datum::Int8(-6)),
            // The shift count wraps to the LEFT operand's width...
            ("1::int4 << 31", Datum::Int4(i32::MIN)),
            ("1::int4 << 32", Datum::Int4(1)),
            ("1::int4 >> 33", Datum::Int4(0)),
            ("-1::int4 << 31", Datum::Int4(i32::MIN)),
            ("1::int8 << 63", Datum::Int8(i64::MIN)),
            ("1::int8 << 64", Datum::Int8(1)),
            // ...and `>>` is arithmetic, so the sign bit is replicated.
            ("(-8)::int4 >> 1", Datum::Int4(-4)),
            ("(-1)::int4 >> 1", Datum::Int4(-1)),
            ("null & 1", Datum::Null),
            ("1 & null", Datum::Null),
        ];
        for (sql, want) in cases {
            assert!(ev(sql, None, &[]) == *want, "{sql}");
        }
        // Only integers have bitwise operators (42883).
        for sql in ["2.5 & 1", "1.5::float8 | 1", "'a'::text # 1"] {
            assert!(infer_err(sql).into_pg().code == "42883", "{sql}");
        }
    }

    #[test]
    fn exponentiation_and_modulo_evaluate_like_postgres() {
        use assert2::assert;

        let cases: &[(&str, Datum)] = &[
            // No integer `^` exists, so an all-integer pair is float8.
            ("2^3", Datum::Float8(8.0)),
            ("2^0", Datum::Float8(1.0)),
            ("2 ^ -2", Datum::Float8(0.25)),
            // Left-associative: (2^3)^2, not 2^(3^2).
            ("2^3^2", Datum::Float8(64.0)),
            // Tighter than `*`, looser than unary minus.
            ("2 ^ 2 * 3", Datum::Float8(12.0)),
            ("-2^2", Datum::Float8(4.0)),
            ("4 % 3", Datum::Int4(1)),
            ("-7 % 3", Datum::Int4(-1)),
            ("7 % -3", Datum::Int4(1)),
            ("10 % 5", Datum::Int4(0)),
            ("7 % 3 + 1", Datum::Int4(2)),
        ];
        for (sql, want) in cases {
            assert!(ev(sql, None, &[]) == *want, "{sql}");
        }
        // A numeric operand selects the exact numeric `^`.
        assert!(matches!(ev("5.0 ^ 2", None, &[]), Datum::Numeric(_)));
        // Domain errors are 2201F; `% 0` is 22012; float8 has no `%` at all.
        assert!(err_code("0 ^ -1", None, &[]) == "2201F");
        assert!(err_code("(-2) ^ 0.5", None, &[]) == "2201F");
        assert!(err_code("5 % 0", None, &[]) == "22012");
        assert!(infer_err("1.5::float8 % 2").into_pg().code == "42883");
    }

    #[test]
    fn generic_prefix_operators_evaluate_like_postgres() {
        use assert2::assert;

        let cases: &[(&str, Datum)] = &[
            ("@ -5", Datum::Int4(5)),
            ("@ 5", Datum::Int4(5)),
            ("@ -5::int8", Datum::Int8(5)),
            ("@ -5::float8", Datum::Float8(5.0)),
            // `|/` and `||/` are float8 whatever the operand's type is.
            ("|/ 16.0", Datum::Float8(4.0)),
            ("|/ 25", Datum::Float8(5.0)),
            ("||/ 8.0", Datum::Float8(2.0)),
            ("||/ -8.0", Datum::Float8(-2.0)),
            // The prefix operators bind LOOSELY, so the operand takes the `+`/`-`.
            ("~ 5 + 1", Datum::Int4(-7)),
            ("@ 5 - 8", Datum::Int4(3)),
            // ...but stop at their own level, so `&` sees the finished `~5`.
            ("~ 5 & 3", Datum::Int4(2)),
            ("@ null", Datum::Null),
        ];
        for (sql, want) in cases {
            assert!(ev(sql, None, &[]) == *want, "{sql}");
        }
        assert!(matches!(ev("@ -5.5", None, &[]), Datum::Numeric(_)));
        // `|/` of a negative number is 2201F; `@`/`~` of a non-number is 42883.
        assert!(err_code("|/ -1", None, &[]) == "2201F");
        assert!(err_code("@ 'abc'::text", None, &[]) == "42883");
        assert!(err_code("~ 'abc'::text", None, &[]) == "42883");
        // int4's most negative value has no int4 absolute value (22003).
        assert!(err_code("@ (-2147483648)::int4", None, &[]) == "22003");
    }

    #[test]
    fn new_operators_infer_their_postgres_result_types() {
        use assert2::assert;

        let cases: &[(&str, ColumnType)] = &[
            ("'a' ~ 'b'", ColumnType::Bool),
            ("'a' !~* 'b'", ColumnType::Bool),
            ("5 & 3", ColumnType::Int4),
            ("5::int8 & 3", ColumnType::Int8),
            ("5 & 3::int8", ColumnType::Int8),
            // A shift keeps the LEFT operand's width; its count is not part of
            // the result type.
            ("1::int4 << 3::int8", ColumnType::Int4),
            ("1::int8 << 3", ColumnType::Int8),
            ("~5", ColumnType::Int4),
            ("~5::int8", ColumnType::Int8),
            ("2^3", ColumnType::Float8),
            ("2^3::float8", ColumnType::Float8),
            ("5.0^2", ColumnType::Numeric(None)),
            ("4 % 3", ColumnType::Int4),
            ("4::int8 % 3", ColumnType::Int8),
            ("@ -5", ColumnType::Int4),
            ("@ -5::int8", ColumnType::Int8),
            // `|/`/`||/` report float8 even for a numeric operand.
            ("|/ 16.0", ColumnType::Float8),
            ("||/ 27.0", ColumnType::Float8),
        ];
        for (sql, want) in cases {
            assert!(
                infer_type(&pexpr(sql).expect("parse"), &Scope::empty()).expect("infer") == *want,
                "{sql}"
            );
        }
    }

    #[test]
    fn range_operators_resolve_domains_by_their_storage_type() {
        let base = Box::leak(Box::new(
            ColumnType::builtin_multirange(crabka_pgtypes::oids::INT4MULTIRANGE)
                .expect("int4multirange"),
        ));
        let domain = ColumnType::Domain(crabka_pgtypes::usertype::DomainRef {
            oid: 900_001,
            name: "restrictedmultirange_test",
            base,
        });
        assert_eq!(
            json_or_array_operator_result_type(BinaryOp::Contains, domain, ColumnType::Int4),
            Some(ColumnType::Bool)
        );

        let subtype = Box::leak(Box::new(ColumnType::Domain(
            crabka_pgtypes::usertype::DomainRef {
                oid: 900_002,
                name: "range_subtype_domain_test",
                base: Box::leak(Box::new(ColumnType::Int4)),
            },
        )));
        let range = crabka_pgtypes::usertype::RangeRef {
            oid: 900_003,
            name: "range_over_domain_test",
            subtype,
        };
        let multirange = ColumnType::Multirange(crabka_pgtypes::usertype::MultirangeRef {
            oid: 900_004,
            name: "multirange_over_domain_test",
            range,
        });
        assert_eq!(
            json_or_array_operator_result_type(BinaryOp::Contains, multirange, *subtype),
            Some(ColumnType::Bool)
        );
    }

    #[test]
    fn a_range_is_not_adjacent_to_an_internal_multirange_edge() {
        use assert2::assert;

        assert!(
            ev(
                "int4range(20,25) -|- int4multirange(int4range(10,20),int4range(30,40))",
                None,
                &[]
            ) == Datum::Bool(false)
        );
        assert!(
            ev(
                "int4range(40,50) -|- int4multirange(int4range(10,20),int4range(30,40))",
                None,
                &[]
            ) == Datum::Bool(true)
        );
    }

    /// The plan-time table and the value-time dispatch answer the SAME set of
    /// (operator, left type, right type) triples.
    ///
    /// [`geometric_operator_result_type`] is what decides at plan time whether
    /// `box <-> circle` exists, and [`apply_geometric_operator`] is what decides
    /// it per row. A pair one of them accepts and the other does not is a
    /// divergence a corpus would see as a query that describes and then fails,
    /// or the other way round — so this walks all 26 geometric operators over
    /// every one of the 7×7 pairs and requires the two to agree 1274 times.
    #[test]
    fn plan_time_and_value_time_agree_on_every_geometric_pair() {
        use assert2::assert;

        let ctx = crate::clock::EvalCtx::test_default();
        for op in GEOMETRIC_BINARY_OPERATORS {
            for (lt, left) in geometric_samples() {
                for (rt, right) in geometric_samples() {
                    let planned = geometric_operator_result_type(op, lt, rt);
                    let evaluated = apply_geometric_operator(op, &left, &right)
                        .expect("a geometric operand pair is always claimed");
                    assert!(
                        planned.is_some() == evaluated.is_ok(),
                        "{} {} {}: plan {:?}, value {:?}",
                        lt.name(),
                        op_spelling(op),
                        rt.name(),
                        planned,
                        evaluated
                    );
                    // A declared pair's value really has the type the plan
                    // promised, so a RowDescription built from the plan
                    // describes the rows the executor then sends.
                    if let (Some(ty), Ok(value)) = (planned, &evaluated)
                        && !value.is_null()
                    {
                        assert!(
                            value.column_type() == Some(ty),
                            "{} {} {} produced {value:?}, not {}",
                            lt.name(),
                            op_spelling(op),
                            rt.name(),
                            ty.name()
                        );
                    }
                    // The full `apply_binary` path agrees with the dispatch it
                    // delegates to, so nothing downstream re-claims a pair.
                    let whole = apply_binary(op, &left, &right, &ctx);
                    assert!(whole.is_ok() == evaluated.is_ok());
                }
            }
        }
    }

    /// Every geometric operator is strict: one NULL operand is one NULL result,
    /// whichever side it is on and whether or not the pair is declared.
    #[test]
    fn a_null_operand_makes_every_geometric_operator_null() {
        use assert2::assert;

        for op in GEOMETRIC_BINARY_OPERATORS {
            for (ty, value) in geometric_samples() {
                for pair in [(value.clone(), Datum::Null), (Datum::Null, value.clone())] {
                    let result = apply_geometric_operator(op, &pair.0, &pair.1);
                    assert!(
                        result.map(Result::ok) == Some(Some(Datum::Null)),
                        "{} {}",
                        op_spelling(op),
                        ty.name()
                    );
                }
            }
        }
    }

    /// The geometric dispatch runs SECOND in `apply_binary`'s prelude, ahead of
    /// the network, bit-string, money and system-identifier families. It must
    /// therefore claim nothing that has no geometric operand — including two
    /// NULLs, which carry no type at all.
    #[test]
    fn the_geometric_dispatch_claims_nothing_without_a_geometric_operand() {
        use assert2::assert;

        let others = [
            Datum::Null,
            Datum::Int4(5),
            Datum::Int8(5),
            Datum::Float8(1.5),
            Datum::Text("x".into()),
            Datum::Bool(true),
        ];
        for op in GEOMETRIC_BINARY_OPERATORS {
            for left in &others {
                for right in &others {
                    assert!(
                        apply_geometric_operator(op, left, right).is_none(),
                        "{op:?} claimed {left:?} and {right:?}"
                    );
                    assert!(apply_geometric_comparison(op, left, right).is_none());
                }
            }
        }
    }

    /// Every `BinaryOp` PostgreSQL gives a geometric overload.
    const GEOMETRIC_BINARY_OPERATORS: [BinaryOp; 26] = [
        BinaryOp::BitXor,
        BinaryOp::ClosestPoint,
        BinaryOp::Overlaps,
        BinaryOp::DoesNotExtendRight,
        BinaryOp::DoesNotExtendLeft,
        BinaryOp::DoesNotExtendAbove,
        BinaryOp::DoesNotExtendBelow,
        BinaryOp::Add,
        BinaryOp::Sub,
        BinaryOp::Mul,
        BinaryOp::Div,
        BinaryOp::Phrase,
        BinaryOp::Shl,
        BinaryOp::Shr,
        BinaryOp::StrictlyBelow,
        BinaryOp::StrictlyAbove,
        BinaryOp::Contains,
        BinaryOp::ContainedBy,
        BinaryOp::BelowEq,
        BinaryOp::AboveEq,
        BinaryOp::Intersects,
        BinaryOp::Horizontal,
        BinaryOp::KeyExistsAny,
        BinaryOp::Perpendicular,
        BinaryOp::Parallel,
        BinaryOp::Same,
    ];

    /// One well-formed value of each geometric type. The coordinates are chosen
    /// so no pair overflows, divides by zero or leaves an operand empty — the
    /// point of the sweep is which pairs RESOLVE, not what they answer.
    fn geometric_samples() -> [(ColumnType, Datum); 7] {
        use crabka_pgtypes::{
            Path, Point, Polygon,
            geometry::{Box2, Circle, Line, Lseg},
        };

        let point = Point { x: 1.0, y: 2.0 };
        let other = Point { x: 3.0, y: 4.0 };
        [
            (ColumnType::Point, Datum::Point(point)),
            (ColumnType::Box, Datum::Box(Box2::normalized(point, other))),
            (
                ColumnType::Circle,
                Datum::Circle(Circle {
                    center: point,
                    radius: 1.0,
                }),
            ),
            (
                ColumnType::Line,
                Datum::Line(Line {
                    a: 1.0,
                    b: -1.0,
                    c: 0.0,
                }),
            ),
            (
                ColumnType::Lseg,
                Datum::Lseg(Lseg {
                    start: point,
                    end: other,
                }),
            ),
            (
                ColumnType::Path,
                Datum::Path(Path {
                    closed: false,
                    points: vec![point, other],
                }),
            ),
            (
                ColumnType::Polygon,
                Datum::Polygon(Polygon {
                    points: vec![point, other, Point { x: 5.0, y: 1.0 }],
                }),
            ),
        ]
    }
}
