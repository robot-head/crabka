//! The array function family, the array operator semantics, and `ANY`/`ALL`
//! over an array.
//!
//! This module follows the existing scalar families `func.rs`, `datetime_fn.rs`
//! and `format_fn.rs`. It holds an `array_func(name)` classifier, an
//! `is_array_func` dispatch predicate, an `array_func_result_type` static
//! resolver for RowDescription, and an `eval_array` value evaluator that takes
//! the caller's child-evaluation closure. So scalar `eval` and the grouped
//! evaluator share the math.
//!
//! The operator helpers for `||`, `@>`, `<@`, `&&`, subscripting and
//! `= ANY(...)` live here too rather than in `eval.rs`. So all one-dimensional
//! array semantics, and their PostgreSQL corner cases, sit in one file.
//! Everything here is a pure, deterministic transform over a single row's
//! already-resolved `Datum`s, so it introduces no lock, visibility, or
//! interleaving rule.

use crabka_pgparser::ast::{Expr, FuncArgs, FuncCall};
use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType, TypeError, cast};

use crate::{clock::EvalCtx, error::ExecError, eval::ArgType, scope::Scope};

/// The array functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArrayFunc {
    /// `array_length(anyarray, int)`: length of dimension `n` (only 1 exists).
    Length,
    /// `cardinality(anyarray)`: total element count (0, not NULL, when empty).
    Cardinality,
    /// `array_append(anyarray, anyelement)`.
    Append,
    /// `array_prepend(anyelement, anyarray)`.
    Prepend,
    /// `array_cat(anyarray, anyarray)`.
    Cat,
    /// `array_to_string(anyarray, text [, null_string])`.
    ToString,
    /// `string_to_array(text, delimiter [, null_string])`.
    StringTo,
    /// `array_ndims(anyarray)`: NULL for the empty array.
    Ndims,
    /// `array_dims(anyarray)`: the `[l:u][l:u]` text, NULL for the empty array.
    Dims,
    /// `array_lower(anyarray, int)`.
    Lower,
    /// `array_upper(anyarray, int)`.
    Upper,
    /// `array_fill(anyelement, int[] [, int[]])`.
    Fill,
    /// `array_position(anyarray, anyelement [, int])`.
    Position,
    /// `array_positions(anyarray, anyelement)`.
    Positions,
    /// `array_remove(anyarray, anyelement)`.
    Remove,
    /// `array_replace(anyarray, anyelement, anyelement)`.
    Replace,
    /// `trim_array(anyarray, int)`.
    Trim,
    /// `array_sample(anyarray, int)`.
    Sample,
    /// `array_shuffle(anyarray)`.
    Shuffle,
    /// `array_sort(anyarray [, descending [, nulls_first]])` (`PostgreSQL` 18).
    Sort,
    /// `array_reverse(anyarray)` (`PostgreSQL` 18).
    Reverse,
}

/// Classify a lowercased function name. The lexer lowercases unquoted idents.
/// `None` means "not an array function".
fn array_func(name: &str) -> Option<ArrayFunc> {
    Some(match name {
        "array_length" => ArrayFunc::Length,
        "cardinality" => ArrayFunc::Cardinality,
        "array_append" => ArrayFunc::Append,
        "array_prepend" => ArrayFunc::Prepend,
        "array_cat" => ArrayFunc::Cat,
        "array_to_string" => ArrayFunc::ToString,
        "string_to_array" => ArrayFunc::StringTo,
        "array_ndims" => ArrayFunc::Ndims,
        "array_dims" => ArrayFunc::Dims,
        "array_lower" => ArrayFunc::Lower,
        "array_upper" => ArrayFunc::Upper,
        "array_fill" => ArrayFunc::Fill,
        "array_position" => ArrayFunc::Position,
        "array_positions" => ArrayFunc::Positions,
        "array_remove" => ArrayFunc::Remove,
        "array_replace" => ArrayFunc::Replace,
        "trim_array" => ArrayFunc::Trim,
        "array_sample" => ArrayFunc::Sample,
        "array_shuffle" => ArrayFunc::Shuffle,
        "array_sort" => ArrayFunc::Sort,
        "array_reverse" => ArrayFunc::Reverse,
        _ => return None,
    })
}

/// Is `name` an array function? This is the dispatch point for the eval guard
/// chains.
pub(crate) fn is_array_func(name: &str) -> bool {
    array_func(name).is_some()
}

// ---- argument-type resolution ----

/// The type an `unknown` literal argument adopts, per position. This is the ONE
/// place the array family's parameter types are written down.
///
/// PostgreSQL leaves a bare `'…'` / `NULL` literal `unknown` and resolves it
/// against the parameter it is passed to. For the polymorphic pairs, such as
/// `array_append(anyarray, anyelement)`, the side which *does* carry a type
/// resolves both, so `array_append('{1,2}', 3)` is `int4[]`. When neither side
/// carries one, PostgreSQL falls back to `text`, so `array_cat('{1,2}', '{3}')`
/// is `text[]`. For `array_length`, `cardinality` and `array_to_string` nothing
/// can resolve the `anyarray` parameter, so an `unknown` literal there is 42804
/// rather than a guess.
///
/// Two callers drive this one rule: [`array_func_result_type`] at plan time,
/// over statically inferred argument types, and [`eval_array`] at run time, over
/// the evaluated values' types. So one decision types and converts a literal.
fn param_types(f: ArrayFunc, given: &[ArgType]) -> Result<Vec<Option<ColumnType>>, ExecError> {
    let at = |i: usize| given.get(i).copied().unwrap_or(ArgType::Opaque);
    let text = Some(ColumnType::Text);
    Ok(match f {
        ArrayFunc::Length => {
            require_resolvable(at(0))?;
            vec![None, Some(ColumnType::Int4)]
        }
        ArrayFunc::Cardinality => {
            require_resolvable(at(0))?;
            vec![None]
        }
        ArrayFunc::ToString => {
            require_resolvable(at(0))?;
            vec![None, text, text]
        }
        ArrayFunc::StringTo => vec![text, text, text],
        ArrayFunc::Append => {
            let elem = pair_element(at(0), at(1));
            vec![Some(ColumnType::Array(elem)), Some(elem.column_type())]
        }
        ArrayFunc::Prepend => {
            let elem = pair_element(at(1), at(0));
            vec![Some(elem.column_type()), Some(ColumnType::Array(elem))]
        }
        ArrayFunc::Cat => {
            let elem = pair_element(at(0), at(1));
            let array = Some(ColumnType::Array(elem));
            vec![array, array]
        }
        ArrayFunc::Ndims | ArrayFunc::Dims => {
            require_resolvable(at(0))?;
            vec![None]
        }
        ArrayFunc::Lower | ArrayFunc::Upper => {
            require_resolvable(at(0))?;
            vec![None, Some(ColumnType::Int4)]
        }
        // `array_fill` types its result from its *element* argument, so the
        // element is what an unknown literal there cannot be resolved from.
        ArrayFunc::Fill => {
            require_resolvable(at(0))?;
            let ints = Some(ColumnType::Array(ElemType::Int4));
            vec![None, ints, ints]
        }
        ArrayFunc::Position => {
            let elem = pair_element(at(0), at(1));
            vec![
                Some(ColumnType::Array(elem)),
                Some(elem.column_type()),
                Some(ColumnType::Int4),
            ]
        }
        ArrayFunc::Positions | ArrayFunc::Remove => {
            let elem = pair_element(at(0), at(1));
            vec![Some(ColumnType::Array(elem)), Some(elem.column_type())]
        }
        ArrayFunc::Replace => {
            let elem = pair_element(at(0), at(1));
            vec![
                Some(ColumnType::Array(elem)),
                Some(elem.column_type()),
                Some(elem.column_type()),
            ]
        }
        ArrayFunc::Trim | ArrayFunc::Sample => {
            require_resolvable(at(0))?;
            vec![None, Some(ColumnType::Int4)]
        }
        ArrayFunc::Shuffle | ArrayFunc::Reverse => {
            require_resolvable(at(0))?;
            vec![None]
        }
        ArrayFunc::Sort => {
            require_resolvable(at(0))?;
            vec![None, Some(ColumnType::Bool), Some(ColumnType::Bool)]
        }
    })
}

/// The element type an `anyarray`/`anyelement` pair resolves to. It comes from
/// the array side when it carries one, else from the element side, else `text`.
/// PostgreSQL settles on `text` when every polymorphic input is `unknown`.
fn pair_element(array_side: ArgType, elem_side: ArgType) -> ElemType {
    array_side
        .known()
        .and_then(ColumnType::array_element)
        .or_else(|| elem_side.known().and_then(ElemType::from_column_type))
        .unwrap_or(ElemType::Text)
}

/// 42804 when an `anyarray` parameter no other argument can resolve was given an
/// `unknown` literal.
fn require_resolvable(arg: ArgType) -> Result<(), ExecError> {
    if arg.is_unknown() {
        return Err(crate::eval::undetermined_polymorphic_type());
    }
    Ok(())
}

/// The argument types the RUN-time resolver drives [`param_types`] from: each
/// value's own type, with a SQL NULL falling back to the type its expression
/// states on its face.
///
/// PostgreSQL resolves `anyarray`/`anyelement` from the *static* argument types,
/// so `array_append(NULL::int4[], NULL::int4)` is `{NULL}` typed `int4[]` even
/// though neither operand carries a type once evaluated. [`ArgType::Opaque`]
/// means "a run-time NULL, type unknown here". That is exactly the case where
/// the syntax still knows, so this function recovers the cast target from it.
fn runtime_arg_types(args: &[Expr], vals: &[Datum]) -> Vec<ArgType> {
    let mut given = crate::eval::value_arg_types(args, vals);
    for (arg, expr) in given.iter_mut().zip(args) {
        if *arg == ArgType::Opaque
            && let Some(ty) = stated_type(expr)
        {
            *arg = ArgType::Known(ty);
        }
    }
    given
}

/// The type an expression states without consulting a `Scope`: a cast's target
/// and a resolved constant's own type. A column reference states nothing here,
/// which is why the recovery above is best-effort rather than complete.
fn stated_type(e: &Expr) -> Option<ColumnType> {
    match e {
        Expr::Cast { ty, .. } | Expr::Const { ty, .. } => Some(*ty),
        _ => None,
    }
}

/// The element type [`param_types`] resolved for the `anyarray` parameter at
/// position `i`. This is what `array_append`/`array_prepend` build a singleton
/// with when their array operand is a run-time NULL.
fn resolved_element(params: &[Option<ColumnType>], i: usize) -> ElemType {
    params
        .get(i)
        .copied()
        .flatten()
        .and_then(ColumnType::array_element)
        .unwrap_or(ElemType::Text)
}

// ---- result-type inference ----

/// Statically infer an array call's result type (for RowDescription). Arity and
/// argument-type mismatches surface as 42883 here, at plan time.
pub(crate) fn array_func_result_type(
    fc: &FuncCall,
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    let f = array_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = exprs_of(fc)?;
    let n = args.len();
    let given = crate::eval::static_arg_types(args, scope)?;
    let types = crate::eval::effective_arg_types(&given, &param_types(f, &given)?);
    Ok(match f {
        ArrayFunc::Length => {
            require_arity(fc, n == 2)?;
            require_array_type(fc, types[0])?;
            ColumnType::Int4
        }
        ArrayFunc::Cardinality => {
            require_arity(fc, n == 1)?;
            require_array_type(fc, types[0])?;
            ColumnType::Int4
        }
        ArrayFunc::Append => {
            require_arity(fc, n == 2)?;
            ColumnType::Array(require_array_type(fc, types[0])?)
        }
        ArrayFunc::Prepend => {
            require_arity(fc, n == 2)?;
            ColumnType::Array(require_array_type(fc, types[1])?)
        }
        ArrayFunc::Cat => {
            require_arity(fc, n == 2)?;
            let left = require_array_type(fc, types[0])?;
            let right = require_array_type(fc, types[1])?;
            if left != right {
                return Err(undefined_function(&fc.name));
            }
            ColumnType::Array(left)
        }
        ArrayFunc::ToString => {
            require_arity(fc, n == 2 || n == 3)?;
            require_array_type(fc, types[0])?;
            ColumnType::Text
        }
        ArrayFunc::StringTo => {
            require_arity(fc, n == 2 || n == 3)?;
            ColumnType::Array(ElemType::Text)
        }
        ArrayFunc::Ndims => {
            require_arity(fc, n == 1)?;
            require_array_type(fc, types[0])?;
            ColumnType::Int4
        }
        ArrayFunc::Dims => {
            require_arity(fc, n == 1)?;
            require_array_type(fc, types[0])?;
            ColumnType::Text
        }
        ArrayFunc::Lower | ArrayFunc::Upper => {
            require_arity(fc, n == 2)?;
            require_array_type(fc, types[0])?;
            ColumnType::Int4
        }
        ArrayFunc::Fill => {
            require_arity(fc, n == 2 || n == 3)?;
            ColumnType::Array(
                ElemType::from_column_type(types[0]).ok_or_else(|| undefined_function(&fc.name))?,
            )
        }
        ArrayFunc::Position => {
            require_arity(fc, n == 2 || n == 3)?;
            require_array_type(fc, types[0])?;
            ColumnType::Int4
        }
        ArrayFunc::Positions => {
            require_arity(fc, n == 2)?;
            require_array_type(fc, types[0])?;
            ColumnType::Array(ElemType::Int4)
        }
        ArrayFunc::Remove => {
            require_arity(fc, n == 2)?;
            ColumnType::Array(require_array_type(fc, types[0])?)
        }
        ArrayFunc::Replace => {
            require_arity(fc, n == 3)?;
            ColumnType::Array(require_array_type(fc, types[0])?)
        }
        ArrayFunc::Trim | ArrayFunc::Sample => {
            require_arity(fc, n == 2)?;
            ColumnType::Array(require_array_type(fc, types[0])?)
        }
        ArrayFunc::Shuffle | ArrayFunc::Reverse => {
            require_arity(fc, n == 1)?;
            ColumnType::Array(require_array_type(fc, types[0])?)
        }
        ArrayFunc::Sort => {
            require_arity(fc, (1..=3).contains(&n))?;
            ColumnType::Array(require_array_type(fc, types[0])?)
        }
    })
}

/// The element type of an array-typed argument. Reports 42883 when it is not an
/// array.
fn require_array_type(fc: &FuncCall, t: ColumnType) -> Result<ElemType, ExecError> {
    t.array_element()
        .ok_or_else(|| undefined_function(&fc.name))
}

// ---- evaluation ----

/// Evaluate an array function call.
///
/// `array_length`/`cardinality`/`array_to_string`/`string_to_array` are STRICT,
/// so any NULL argument yields NULL. `array_append`/`array_prepend`/`array_cat`
/// are deliberately **not** strict, which matches PostgreSQL. A NULL array
/// behaves like an empty array of the other operand's type, and a NULL element
/// appends a NULL element.
pub(crate) fn eval_array(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let f = array_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = exprs_of(fc)?;
    let mut vals: Vec<Datum> = args.iter().map(&mut eval_child).collect::<Result<_, _>>()?;
    // Give every `unknown` literal argument the value its parameter's type calls
    // for, by the same rule the plan-time resolver typed it with.
    let given = runtime_arg_types(args, &vals);
    let params = param_types(f, &given)?;
    crate::eval::coerce_unknown_args(args, &mut vals, &params, ctx)?;
    let n = vals.len();
    match f {
        ArrayFunc::Length => {
            require_arity(fc, n == 2)?;
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Datum::Null);
            }
            let array = array_value(&vals[0], &fc.name)?;
            let dim = int_arg(&vals[1], &fc.name)?;
            Ok(dimension(array, dim).map_or(Datum::Null, |d| Datum::Int4(d.len)))
        }
        ArrayFunc::Cardinality => {
            require_arity(fc, n == 1)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Int4(element_count(array_value(
                &vals[0], &fc.name,
            )?)?))
        }
        ArrayFunc::Append => {
            require_arity(fc, n == 2)?;
            array_append(&vals[0], &vals[1], resolved_element(&params, 0), ctx)
        }
        ArrayFunc::Prepend => {
            require_arity(fc, n == 2)?;
            array_prepend(&vals[0], &vals[1], resolved_element(&params, 1), ctx)
        }
        ArrayFunc::Cat => {
            require_arity(fc, n == 2)?;
            array_cat(&vals[0], &vals[1])
        }
        ArrayFunc::ToString => {
            require_arity(fc, n == 2 || n == 3)?;
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Datum::Null);
            }
            let array = array_value(&vals[0], &fc.name)?;
            let sep = text_arg(&vals[1], &fc.name)?;
            let null_text = match vals.get(2) {
                Some(Datum::Null) | None => None,
                Some(d) => Some(text_arg(d, &fc.name)?),
            };
            Ok(Datum::Text(array_to_string(array, sep, null_text, ctx)))
        }
        ArrayFunc::StringTo => {
            require_arity(fc, n == 2 || n == 3)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            let input = text_arg(&vals[0], &fc.name)?;
            let sep = match &vals[1] {
                Datum::Null => None,
                d => Some(text_arg(d, &fc.name)?),
            };
            let null_text = match vals.get(2) {
                Some(Datum::Null) | None => None,
                Some(d) => Some(text_arg(d, &fc.name)?),
            };
            Ok(string_to_array(input, sep, null_text))
        }
        ArrayFunc::Ndims => {
            require_arity(fc, n == 1)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            let array = array_value(&vals[0], &fc.name)?;
            Ok(match i32::try_from(array.ndims()) {
                Ok(0) => Datum::Null,
                Ok(ndims) => Datum::Int4(ndims),
                Err(_) => return Err(ExecError::Type(TypeError::Overflow)),
            })
        }
        ArrayFunc::Dims => {
            require_arity(fc, n == 1)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            let array = array_value(&vals[0], &fc.name)?;
            if array.dims.is_empty() {
                return Ok(Datum::Null);
            }
            let mut text = String::new();
            for dim in &array.dims {
                text.push('[');
                text.push_str(&dim.lower.to_string());
                text.push(':');
                text.push_str(&dim.upper().to_string());
                text.push(']');
            }
            Ok(Datum::Text(text))
        }
        ArrayFunc::Lower | ArrayFunc::Upper => {
            require_arity(fc, n == 2)?;
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Datum::Null);
            }
            let array = array_value(&vals[0], &fc.name)?;
            let dim = int_arg(&vals[1], &fc.name)?;
            Ok(dimension(array, dim).map_or(Datum::Null, |d| {
                Datum::Int4(if f == ArrayFunc::Lower {
                    d.lower
                } else {
                    d.upper()
                })
            }))
        }
        ArrayFunc::Fill => {
            require_arity(fc, n == 2 || n == 3)?;
            let elem = resolved_fill_element(&vals[0], &params);
            array_fill(&vals[0], &vals[1], vals.get(2), elem)
        }
        ArrayFunc::Position => {
            require_arity(fc, n == 2 || n == 3)?;
            array_position(&vals[0], &vals[1], vals.get(2), &fc.name)
        }
        ArrayFunc::Positions => {
            require_arity(fc, n == 2)?;
            array_positions(&vals[0], &vals[1], &fc.name)
        }
        ArrayFunc::Remove => {
            require_arity(fc, n == 2)?;
            array_remove(&vals[0], &vals[1], &fc.name)
        }
        ArrayFunc::Replace => {
            require_arity(fc, n == 3)?;
            array_replace(&vals[0], &vals[1], &vals[2], &fc.name)
        }
        ArrayFunc::Trim => {
            require_arity(fc, n == 2)?;
            trim_array(&vals[0], &vals[1], &fc.name)
        }
        ArrayFunc::Sample => {
            require_arity(fc, n == 2)?;
            array_sample(&vals[0], &vals[1], &fc.name)
        }
        ArrayFunc::Shuffle => {
            require_arity(fc, n == 1)?;
            array_shuffle(&vals[0], &fc.name)
        }
        ArrayFunc::Sort => {
            require_arity(fc, (1..=3).contains(&n))?;
            array_sort(&vals[0], vals.get(1), vals.get(2), &fc.name)
        }
        ArrayFunc::Reverse => {
            require_arity(fc, n == 1)?;
            array_reverse(&vals[0], &fc.name)
        }
    }
}

/// `cardinality`/`array_length` report an `int4`. A longer array is 22003.
fn element_count(array: &ArrayValue) -> Result<i32, ExecError> {
    i32::try_from(array.elems.len()).map_err(|_| ExecError::Type(TypeError::Overflow))
}

/// `array_to_string`: render each element with its own output function, and join
/// with `sep`. This function omits NULL elements unless the caller supplies
/// `null_text`.
fn array_to_string(
    array: &ArrayValue,
    sep: &str,
    null_text: Option<&str>,
    ctx: &EvalCtx,
) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(array.elems.len());
    for e in &array.elems {
        if e.is_null() {
            if let Some(text) = null_text {
                parts.push(text.to_string());
            }
        } else {
            parts.push(datum_text(e, ctx));
        }
    }
    parts.join(sep)
}

/// `string_to_array`: split `input` on `sep` into a `text[]`. A NULL separator
/// splits into single characters. An empty separator yields the whole string as
/// one element. An empty input yields an empty array. Elements equal to
/// `null_text` become NULL elements.
fn string_to_array(input: &str, sep: Option<&str>, null_text: Option<&str>) -> Datum {
    let parts: Vec<String> = match sep {
        None => input.chars().map(|c| c.to_string()).collect(),
        Some("") => {
            if input.is_empty() {
                Vec::new()
            } else {
                vec![input.to_string()]
            }
        }
        Some(sep) => {
            if input.is_empty() {
                Vec::new()
            } else {
                input.split(sep).map(ToString::to_string).collect()
            }
        }
    };
    let elems = parts
        .into_iter()
        .map(|p| {
            if null_text == Some(p.as_str()) {
                Datum::Null
            } else {
                Datum::Text(p)
            }
        })
        .collect();
    Datum::Array(ArrayValue::new(ElemType::Text, elems))
}

// ---- operator semantics (wired into `apply_binary` by the eval layer) ----

/// `array_append(anyarray, anyelement)`: a NULL array behaves like the EMPTY
/// array of the resolved element type, so the result is always a one-element
/// array. `array_append(NULL::int4[], NULL::int4)` is `{NULL}`, not SQL NULL.
/// `into` is the element type the call's `anyarray`/`anyelement` pair resolved
/// to. This function uses it only when neither operand carries one at run time.
pub(crate) fn array_append(
    array: &Datum,
    elem: &Datum,
    into: ElemType,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    match array {
        Datum::Array(a) => {
            require_flat(a)?;
            let mut elems = a.elems.clone();
            elems.push(coerce_element(elem, a.elem, ctx)?);
            let lower = a.dims.first().map_or(1, |d| d.lower);
            Ok(Datum::Array(ArrayValue::with_dims(
                a.elem,
                elems.clone(),
                vec![crabka_pgtypes::ArrayDim::new(
                    lower,
                    i32::try_from(elems.len()).unwrap_or(i32::MAX),
                )],
            )))
        }
        Datum::Null => Ok(singleton_from_element(elem, into)),
        other => Err(not_an_array(other)),
    }
}

/// `array_append`/`array_prepend` and the element `||` forms only accept an
/// empty or one-dimensional array. Anything else is 22000, exactly as
/// PostgreSQL reports it.
fn require_flat(a: &ArrayValue) -> Result<(), ExecError> {
    if a.ndims() > 1 {
        return Err(ExecError::Type(TypeError::Coded {
            sqlstate: "22000",
            message: "argument must be empty or one-dimensional array".into(),
        }));
    }
    Ok(())
}

/// `array_prepend(anyelement, anyarray)`: the mirror of [`array_append`].
pub(crate) fn array_prepend(
    elem: &Datum,
    array: &Datum,
    into: ElemType,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    match array {
        Datum::Array(a) => {
            require_flat(a)?;
            let mut elems = Vec::with_capacity(a.elems.len() + 1);
            elems.push(coerce_element(elem, a.elem, ctx)?);
            elems.extend(a.elems.iter().cloned());
            // Prepending keeps the array's own lower bound and grows upward,
            // exactly as PostgreSQL's `array_cat` of a one-element array does.
            let lower = a.dims.first().map_or(1, |d| d.lower);
            let len = i32::try_from(elems.len()).unwrap_or(i32::MAX);
            Ok(Datum::Array(ArrayValue::with_dims(
                a.elem,
                elems,
                vec![crabka_pgtypes::ArrayDim::new(lower, len)],
            )))
        }
        Datum::Null => Ok(singleton_from_element(elem, into)),
        other => Err(not_an_array(other)),
    }
}

/// `array_cat(anyarray, anyarray)`: a NULL array behaves like the empty array of
/// the other side's type. This is PostgreSQL's non-strict `array_cat`.
pub(crate) fn array_cat(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    match (left, right) {
        (Datum::Null, Datum::Null) => Ok(Datum::Null),
        (Datum::Null, other) | (other, Datum::Null) => match other {
            Datum::Array(_) => Ok(other.clone()),
            _ => Err(not_an_array(other)),
        },
        (Datum::Array(a), Datum::Array(b)) => {
            if a.elem != b.elem {
                return Err(operator_undefined("||", left, right));
            }
            if a.dims.is_empty() {
                return Ok(right.clone());
            }
            if b.dims.is_empty() {
                return Ok(left.clone());
            }
            let mut elems = a.elems.clone();
            elems.extend(b.elems.iter().cloned());
            // PostgreSQL joins along the OUTERMOST dimension. Equal
            // dimensionality sums the outer extents; one fewer dimension on
            // either side makes that side a single extra outer slice.
            let dims = match (a.ndims(), b.ndims()) {
                (x, y) if x == y => {
                    if a.dims[1..] != b.dims[1..] {
                        return Err(incompatible_arrays());
                    }
                    let mut dims = a.dims.clone();
                    dims[0].len = a.dims[0].len.saturating_add(b.dims[0].len);
                    dims
                }
                (x, y) if x + 1 == y => {
                    if a.dims[..] != b.dims[1..] {
                        return Err(incompatible_arrays());
                    }
                    let mut dims = b.dims.clone();
                    dims[0].len = dims[0].len.saturating_add(1);
                    dims
                }
                (x, y) if x == y + 1 => {
                    if a.dims[1..] != b.dims[..] {
                        return Err(incompatible_arrays());
                    }
                    let mut dims = a.dims.clone();
                    dims[0].len = dims[0].len.saturating_add(1);
                    dims
                }
                _ => return Err(incompatible_arrays()),
            };
            Ok(Datum::Array(ArrayValue::with_dims(a.elem, elems, dims)))
        }
        _ => Err(operator_undefined("||", left, right)),
    }
}

fn incompatible_arrays() -> ExecError {
    ExecError::Type(TypeError::array_subscript(
        "cannot concatenate incompatible arrays",
    ))
}

/// Which of PostgreSQL's three `||` array operators a call resolves to.
///
/// This function makes the choice from the operands' **static** types, exactly
/// as PostgreSQL's operator resolution does. It cannot make the choice from the
/// runtime values, because `int[] || NULL::int[]` concatenates to `{1,2}` and
/// `int[] || NULL::int` appends to `{1,2,NULL}`, and the two are
/// indistinguishable once both right-hand sides have evaluated to SQL NULL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConcatForm {
    /// `anyarray || anyarray`.
    ArrayArray,
    /// `anyarray || anyelement`, carrying the element type the pair resolved to.
    /// So `NULL::int4[] || NULL::int4` still builds an `int4[]` `{NULL}`.
    ArrayElement(ElemType),
    /// `anyelement || anyarray`: the mirror of [`ConcatForm::ArrayElement`].
    ElementArray(ElemType),
}

/// Resolve `left || right` from the operand types, or `None` when no array `||`
/// applies. The caller then falls through to the text/jsonb `||`, or reports
/// 42883.
///
/// A bare `NULL` literal types as `text` here, so a caller with the syntactic
/// expression in hand should resolve `x || NULL` to [`ConcatForm::ArrayArray`].
/// That is what PostgreSQL's `unknown` resolution picks, and it makes
/// `ARRAY[1,2] || NULL` yield `{1,2}` rather than `{1,2,NULL}`.
pub(crate) fn concat_form(left: ColumnType, right: ColumnType) -> Option<ConcatForm> {
    match (left.array_element(), right.array_element()) {
        (Some(a), Some(b)) if a == b => Some(ConcatForm::ArrayArray),
        (Some(_), Some(_)) => None,
        (Some(a), None) if ElemType::from_column_type(right) == Some(a) => {
            Some(ConcatForm::ArrayElement(a))
        }
        (None, Some(b)) if ElemType::from_column_type(left) == Some(b) => {
            Some(ConcatForm::ElementArray(b))
        }
        _ => None,
    }
}

/// The static result type of an array `||`.
pub(crate) fn concat_result_type(left: ColumnType, right: ColumnType) -> Option<ColumnType> {
    match concat_form(left, right)? {
        ConcatForm::ArrayArray | ConcatForm::ArrayElement(_) => Some(left),
        ConcatForm::ElementArray(_) => Some(right),
    }
}

/// The `||` operator over arrays, in the form [`concat_form`] resolved.
pub(crate) fn array_concat(
    form: ConcatForm,
    left: &Datum,
    right: &Datum,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    match form {
        ConcatForm::ArrayArray => array_cat(left, right),
        ConcatForm::ArrayElement(elem) => array_append(left, right, elem, ctx),
        ConcatForm::ElementArray(elem) => array_prepend(left, right, elem, ctx),
    }
}

/// `left @> right`: every element of `right` appears in `left`.
///
/// PostgreSQL's `array_contain_compare` assumes a strict equality operator. So a
/// NULL element on the *contained* side can never be matched, and it makes the
/// whole test false. A NULL element on the containing side is never a match.
pub(crate) fn array_contains(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let (Some(l), Some(r)) = (array_or_null(left, "@>")?, array_or_null(right, "@>")?) else {
        return Ok(Datum::Null);
    };
    require_same_element(l, r, "@>", left, right)?;
    Ok(Datum::Bool(r.elems.iter().all(|needle| {
        !needle.is_null() && l.elems.iter().any(|e| e == needle)
    })))
}

/// `left <@ right`: [`array_contains`] with the operands swapped.
pub(crate) fn array_contained_by(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let (Some(l), Some(r)) = (array_or_null(left, "<@")?, array_or_null(right, "<@")?) else {
        return Ok(Datum::Null);
    };
    require_same_element(l, r, "<@", left, right)?;
    Ok(Datum::Bool(l.elems.iter().all(|needle| {
        !needle.is_null() && r.elems.iter().any(|e| e == needle)
    })))
}

/// `left && right`: the arrays share at least one element. NULL elements never
/// overlap. This operator skips them, and does not falsify the test.
pub(crate) fn array_overlap(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let (Some(l), Some(r)) = (array_or_null(left, "&&")?, array_or_null(right, "&&")?) else {
        return Ok(Datum::Null);
    };
    require_same_element(l, r, "&&", left, right)?;
    Ok(Datum::Bool(l.elems.iter().any(|a| {
        !a.is_null() && r.elems.iter().any(|b| !b.is_null() && a == b)
    })))
}

/// `array[index]`: 1-based subscripting. A NULL array or NULL index yields NULL.
/// An out-of-range subscript is also NULL, not an error, unlike most languages.
pub(crate) fn array_subscript(base: &Datum, index: &Datum) -> Result<Datum, ExecError> {
    let array = match base {
        Datum::Null => return Ok(Datum::Null),
        Datum::Array(a) | Datum::OidVector(a) => a,
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "cannot subscript type {} because it does not support subscripting",
                type_name(other)
            )));
        }
    };
    let i = match index {
        Datum::Null => return Ok(Datum::Null),
        Datum::Int4(n) => i64::from(*n),
        Datum::Int8(n) => *n,
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "array subscript must be type integer, not type {}",
                type_name(other)
            )));
        }
    };
    // A single subscript reaches an element only of a one-dimensional array;
    // PostgreSQL returns NULL when the subscript count is not the dimension
    // count, so `('{{1,2},{3,4}}'::int[])[1]` is NULL rather than a row.
    let [dim] = array.dims[..] else {
        return Ok(Datum::Null);
    };
    let offset = i - i64::from(dim.lower);
    if offset < 0 || offset >= i64::from(dim.len) {
        return Ok(Datum::Null);
    }
    let idx = usize::try_from(offset).map_err(|_| ExecError::Type(TypeError::Overflow))?;
    Ok(array.elems.get(idx).cloned().unwrap_or(Datum::Null))
}

/// `ARRAY[…]`: assemble one constructor level from its already-evaluated
/// items, where an item that is itself an array came from a nested constructor.
///
/// A mix of arrays and scalars, or sub-arrays of different shape, is
/// `PostgreSQL`'s 2202E "multidimensional arrays must have array expressions
/// with matching dimensions".
pub(crate) fn build_constructor(elem: ElemType, items: Vec<Datum>) -> Result<Datum, ExecError> {
    let arrays = items
        .iter()
        .filter(|i| matches!(i, Datum::Array(_)))
        .count();
    if arrays == 0 {
        return Ok(Datum::Array(ArrayValue::new(elem, items)));
    }
    if arrays != items.len() {
        return Err(mismatched_constructor_dims());
    }
    let mut inner: Option<Vec<crabka_pgtypes::ArrayDim>> = None;
    let mut elems = Vec::new();
    for item in &items {
        let Datum::Array(a) = item else {
            unreachable!("every item is an array here")
        };
        match &inner {
            None => inner = Some(a.dims.clone()),
            Some(seen) if *seen == a.dims => {}
            Some(_) => return Err(mismatched_constructor_dims()),
        }
        elems.extend(a.elems.iter().cloned());
    }
    let mut dims = vec![crabka_pgtypes::ArrayDim::from_len(items.len())];
    dims.extend(inner.unwrap_or_default());
    if dims.len() > crabka_pgtypes::MAX_ARRAY_DIM {
        return Err(ExecError::Type(TypeError::Coded {
            sqlstate: "54000",
            message: format!(
                "number of array dimensions ({}) exceeds the maximum allowed ({})",
                dims.len(),
                crabka_pgtypes::MAX_ARRAY_DIM
            ),
        }));
    }
    if elems.is_empty() {
        return Ok(Datum::Array(ArrayValue::new(elem, elems)));
    }
    Ok(Datum::Array(ArrayValue::with_dims(elem, elems, dims)))
}

fn mismatched_constructor_dims() -> ExecError {
    ExecError::Type(TypeError::array_subscript(
        "multidimensional arrays must have array expressions with matching dimensions",
    ))
}

/// `ARRAY(subquery)`: one array over the subquery's single column, in the order
/// the subquery produced its rows.
pub(crate) fn array_from_rows(elem: ElemType, rows: Vec<Datum>) -> Datum {
    Datum::Array(ArrayValue::new(elem, rows))
}

// ---- multi-subscript references and slices ----

/// One evaluated entry of a subscript chain. This is
/// [`crabka_pgparser::ast::ArraySubscript`] with its bound expressions already
/// reduced to values.
#[derive(Debug, Clone)]
pub(crate) enum SubscriptArg {
    /// `a[i]`.
    Index(Datum),
    /// `a[lo:hi]`. An omitted bound takes the array's own bound for that
    /// dimension.
    Slice {
        lower: Option<Datum>,
        upper: Option<Datum>,
    },
}

impl SubscriptArg {
    fn is_slice(&self) -> bool {
        matches!(self, SubscriptArg::Slice { .. })
    }
}

/// `base[s1][s2]…`: `PostgreSQL`'s array reference over a whole subscript chain.
///
/// A chain with no slice selects one element. It yields NULL unless the chain is
/// exactly as long as the array has dimensions and every subscript is in range.
/// A chain with at least one slice yields an **array**. Unsupplied trailing
/// dimensions are whole slices, a plain index in a slice chain means `[i:i]`,
/// every range is clipped to the array's own bounds, and the result is
/// renumbered from lower bound 1, exactly as `array_get_slice` does.
pub(crate) fn array_ref(base: &Datum, subscripts: &[SubscriptArg]) -> Result<Datum, ExecError> {
    let array = match base {
        Datum::Null => return Ok(Datum::Null),
        Datum::Array(a) | Datum::OidVector(a) => a,
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "cannot subscript type {} because it does not support subscripting",
                type_name(other)
            )));
        }
    };
    if subscripts.iter().any(SubscriptArg::is_slice) {
        return array_ref_slice(array, subscripts);
    }
    if subscripts.len() != array.ndims() {
        return Ok(Datum::Null);
    }
    let mut offset = 0usize;
    for (sub, (dim, stride)) in subscripts
        .iter()
        .zip(array.dims.iter().zip(array.strides()))
    {
        let SubscriptArg::Index(value) = sub else {
            unreachable!("the slice case returned above")
        };
        let Some(i) = subscript_int(value)? else {
            return Ok(Datum::Null);
        };
        let within = i - i64::from(dim.lower);
        if within < 0 || within >= i64::from(dim.len) {
            return Ok(Datum::Null);
        }
        let within = usize::try_from(within).map_err(|_| ExecError::Type(TypeError::Overflow))?;
        offset += within * stride;
    }
    Ok(array.elems.get(offset).cloned().unwrap_or(Datum::Null))
}

/// The slice half of [`array_ref`].
fn array_ref_slice(array: &ArrayValue, subscripts: &[SubscriptArg]) -> Result<Datum, ExecError> {
    if array.dims.is_empty() {
        return Ok(Datum::Array(ArrayValue::new(array.elem, Vec::new())));
    }
    // Ranges are half-open in the flat vector; `ranges[d]` is the selected
    // `[start, end)` offset inside dimension `d`.
    let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(array.dims.len());
    for (i, dim) in array.dims.iter().enumerate() {
        let (lower, upper) = match subscripts.get(i) {
            // Dimensions past the end of the chain are whole slices.
            None => (dim.lower, dim.upper()),
            // In a chain that contains a slice, a plain subscript `i` means
            // `1:i` — PostgreSQL treats EVERY subscript of such a chain as a
            // slice, taking the missing lower bound as 1 rather than as `i`.
            Some(SubscriptArg::Index(value)) => {
                let Some(i) = subscript_int(value)? else {
                    return Ok(Datum::Null);
                };
                (1, clamp_i32(i))
            }
            Some(SubscriptArg::Slice { lower, upper }) => {
                let lower = match lower {
                    None => dim.lower,
                    Some(value) => match subscript_int(value)? {
                        None => return Ok(Datum::Null),
                        Some(i) => clamp_i32(i),
                    },
                };
                let upper = match upper {
                    None => dim.upper(),
                    Some(value) => match subscript_int(value)? {
                        None => return Ok(Datum::Null),
                        Some(i) => clamp_i32(i),
                    },
                };
                (lower, upper)
            }
        };
        // An empty range anywhere makes the whole slice the empty array.
        if lower > upper || upper < dim.lower || lower > dim.upper() {
            return Ok(Datum::Array(ArrayValue::new(array.elem, Vec::new())));
        }
        let start = lower.max(dim.lower) - dim.lower;
        let end = upper.min(dim.upper()) - dim.lower + 1;
        let start = usize::try_from(start).map_err(|_| ExecError::Type(TypeError::Overflow))?;
        let end = usize::try_from(end).map_err(|_| ExecError::Type(TypeError::Overflow))?;
        ranges.push((start, end));
    }
    let strides = array.strides();
    let dims: Vec<crabka_pgtypes::ArrayDim> = ranges
        .iter()
        .map(|(start, end)| crabka_pgtypes::ArrayDim::from_len(end - start))
        .collect();
    let mut elems = Vec::new();
    collect_slice(array, &ranges, &strides, 0, 0, &mut elems);
    Ok(Datum::Array(ArrayValue::with_dims(array.elem, elems, dims)))
}

/// Walk the selected sub-box in row-major order, and append its elements.
fn collect_slice(
    array: &ArrayValue,
    ranges: &[(usize, usize)],
    strides: &[usize],
    depth: usize,
    base: usize,
    out: &mut Vec<Datum>,
) {
    let (start, end) = ranges[depth];
    for i in start..end {
        let offset = base + i * strides[depth];
        if depth + 1 == ranges.len() {
            out.push(array.elems.get(offset).cloned().unwrap_or(Datum::Null));
        } else {
            collect_slice(array, ranges, strides, depth + 1, offset, out);
        }
    }
}

/// One subscript value as an integer. `None` is a NULL subscript.
fn subscript_int(value: &Datum) -> Result<Option<i64>, ExecError> {
    Ok(match value {
        Datum::Null => None,
        Datum::Int2(n) => Some(i64::from(*n)),
        Datum::Int4(n) => Some(i64::from(*n)),
        Datum::Int8(n) => Some(*n),
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "array subscript must be type integer, not type {}",
                type_name(other)
            )));
        }
    })
}

/// Saturate a subscript into `i32`, the width `PostgreSQL` stores bounds at.
fn clamp_i32(value: i64) -> i32 {
    i32::try_from(value).unwrap_or(if value < 0 { i32::MIN } else { i32::MAX })
}

/// The `PostgreSQL` dimension `n` of `array`, counted from 1, or `None` when the
/// array has no such dimension. That is what makes `array_length('{}', 1)` NULL.
fn dimension(array: &ArrayValue, n: i32) -> Option<crabka_pgtypes::ArrayDim> {
    usize::try_from(n)
        .ok()
        .and_then(|n| n.checked_sub(1))
        .and_then(|i| array.dims.get(i))
        .copied()
}

// ---- subscripted assignment ----

/// `SET a[i] = v` / `SET a[lo:hi] = v`: `PostgreSQL`'s `array_set_element` and
/// `array_set_slice` over the column's current value.
///
/// A NULL target starts from the empty array of `into`. A one-dimensional array
/// **extends** to cover a subscript past either end, and fills the gap with
/// NULLs. A multidimensional one does not, and reports 2202E instead.
pub(crate) fn array_assign(
    current: &Datum,
    subscripts: &[SubscriptArg],
    value: &Datum,
    into: ElemType,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let array = match current {
        Datum::Null => ArrayValue::new(into, Vec::new()),
        Datum::Array(a) => a.clone(),
        other => return Err(not_an_array(other)),
    };
    let bounds = resolved_bounds(subscripts, &array)?;
    if subscripts.iter().any(SubscriptArg::is_slice) {
        assign_slice(array, &bounds, value, ctx)
    } else {
        assign_element(array, &bounds, value, ctx)
    }
}

/// Each subscript's `[lower, upper]` after NULL rejection and slice defaulting.
fn resolved_bounds(
    subscripts: &[SubscriptArg],
    array: &ArrayValue,
) -> Result<Vec<(i32, i32)>, ExecError> {
    let mut out = Vec::with_capacity(subscripts.len());
    for (i, sub) in subscripts.iter().enumerate() {
        let dim = array.dims.get(i).copied();
        let pair = match sub {
            SubscriptArg::Index(value) => {
                let i = clamp_i32(assignment_subscript(value)?);
                (i, i)
            }
            SubscriptArg::Slice { lower, upper } => {
                let lower = match lower {
                    None => dim.map_or(1, |d| d.lower),
                    Some(value) => clamp_i32(assignment_subscript(value)?),
                };
                let upper = match upper {
                    None => dim.map_or(1, crabka_pgtypes::ArrayDim::upper),
                    Some(value) => clamp_i32(assignment_subscript(value)?),
                };
                (lower, upper)
            }
        };
        out.push(pair);
    }
    Ok(out)
}

/// A subscript in an assignment must not be NULL. That is 22004, unlike a read.
fn assignment_subscript(value: &Datum) -> Result<i64, ExecError> {
    subscript_int(value)?.ok_or_else(|| {
        ExecError::Type(TypeError::Coded {
            sqlstate: "22004",
            message: "array subscript in assignment must not be null".into(),
        })
    })
}

/// `array_set_element`: write one element, and extend a one-dimensional array.
fn assign_element(
    mut array: ArrayValue,
    bounds: &[(i32, i32)],
    value: &Datum,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let elem = coerce_element(value, array.elem, ctx)?;
    if array.dims.is_empty() {
        // A fresh array takes exactly the shape the subscripts describe.
        let dims = bounds
            .iter()
            .map(|(lower, _)| crabka_pgtypes::ArrayDim::new(*lower, 1))
            .collect();
        return Ok(Datum::Array(ArrayValue::with_dims(
            array.elem,
            vec![elem],
            dims,
        )));
    }
    if bounds.len() != array.dims.len() {
        return Err(wrong_number_of_subscripts());
    }
    let index = bounds[0].0;
    if array.dims.len() == 1 {
        extend_to_cover(&mut array, index, index)?;
    }
    let mut offset = 0usize;
    for ((lower, _), (dim, stride)) in bounds.iter().zip(array.dims.iter().zip(array.strides())) {
        let within = i64::from(*lower) - i64::from(dim.lower);
        if within < 0 || within >= i64::from(dim.len) {
            return Err(subscript_out_of_range());
        }
        let within = usize::try_from(within).map_err(|_| ExecError::Type(TypeError::Overflow))?;
        offset += within * stride;
    }
    array.elems[offset] = elem;
    Ok(Datum::Array(array))
}

/// `array_set_slice`: write a whole sub-box, and extend a one-dimensional array.
fn assign_slice(
    mut array: ArrayValue,
    bounds: &[(i32, i32)],
    value: &Datum,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let source = match value {
        // Assigning NULL to a slice leaves the array untouched, as PostgreSQL's
        // `array_set_slice` does for a null source.
        Datum::Null => return Ok(Datum::Array(array)),
        Datum::Array(a) => a.clone(),
        // `SET a[1:2] = '{1,2}'` — a bare literal on the value side is
        // `unknown`, and the target column's array type resolves it.
        Datum::Text(_) => match cast::cast(value, ColumnType::Array(array.elem), &ctx.time_zone)? {
            Datum::Array(a) => a,
            other => return Err(not_an_array(&other)),
        },
        other => return Err(not_an_array(other)),
    };
    if array.dims.is_empty() {
        let dims = bounds
            .iter()
            .map(|(lower, upper)| {
                crabka_pgtypes::ArrayDim::new(*lower, upper.saturating_sub(*lower) + 1)
            })
            .collect();
        let slots = slot_count(bounds)?;
        check_array_size(slots)?;
        array = ArrayValue::with_dims(array.elem, vec![Datum::Null; slots], dims);
    } else if bounds.len() != array.dims.len() {
        return Err(wrong_number_of_subscripts());
    } else if array.dims.len() == 1 {
        extend_to_cover(&mut array, bounds[0].0, bounds[0].1)?;
    }
    if slot_count(bounds)? > source.elems.len() {
        return Err(ExecError::Type(TypeError::array_subscript(
            "source array too small",
        )));
    }
    let strides = array.strides();
    let mut ranges = Vec::with_capacity(bounds.len());
    for ((lower, upper), dim) in bounds.iter().zip(&array.dims) {
        let start = i64::from(*lower) - i64::from(dim.lower);
        let end = i64::from(*upper) - i64::from(dim.lower) + 1;
        if start < 0 || end > i64::from(dim.len) {
            return Err(subscript_out_of_range());
        }
        let start = usize::try_from(start).map_err(|_| ExecError::Type(TypeError::Overflow))?;
        let end = usize::try_from(end).map_err(|_| ExecError::Type(TypeError::Overflow))?;
        ranges.push((start, end));
    }
    let mut offsets = Vec::new();
    slice_offsets(&ranges, &strides, 0, 0, &mut offsets);
    for (offset, replacement) in offsets.into_iter().zip(&source.elems) {
        array.elems[offset] = coerce_element(replacement, array.elem, ctx)?;
    }
    Ok(Datum::Array(array))
}

/// The flat offsets the slice `ranges` covers, in row-major order.
fn slice_offsets(
    ranges: &[(usize, usize)],
    strides: &[usize],
    depth: usize,
    base: usize,
    out: &mut Vec<usize>,
) {
    let (start, end) = ranges[depth];
    for i in start..end {
        let offset = base + i * strides[depth];
        if depth + 1 == ranges.len() {
            out.push(offset);
        } else {
            slice_offsets(ranges, strides, depth + 1, offset, out);
        }
    }
}

/// How many slots a subscript box covers.
fn slot_count(bounds: &[(i32, i32)]) -> Result<usize, ExecError> {
    let mut total = 1usize;
    for (lower, upper) in bounds {
        let len = i64::from(*upper) - i64::from(*lower) + 1;
        let len = usize::try_from(len).map_err(|_| subscript_out_of_range())?;
        total = total
            .checked_mul(len)
            .ok_or(ExecError::Type(TypeError::Overflow))?;
    }
    Ok(total)
}

/// `PostgreSQL`'s `MaxArraySize`, that is `MaxAllocSize / sizeof(Datum)`. An
/// array larger than this is 54000 rather than an out-of-memory failure.
const MAX_ARRAY_SIZE: usize = 134_217_727;

/// Reject an element count `PostgreSQL` would refuse to allocate.
fn check_array_size(elements: usize) -> Result<(), ExecError> {
    if elements > MAX_ARRAY_SIZE {
        return Err(ExecError::Type(TypeError::Coded {
            sqlstate: "54000",
            message: format!("array size exceeds the maximum allowed ({MAX_ARRAY_SIZE})"),
        }));
    }
    Ok(())
}

/// Grow a one-dimensional array so `[lower, upper]` is inside it. This function
/// fills the new slots with NULL. It moves the lower bound down when the write
/// is below the array's start, as in `UPDATE t SET a[0] = …`.
fn extend_to_cover(array: &mut ArrayValue, lower: i32, upper: i32) -> Result<(), ExecError> {
    let Some(dim) = array.dims.first().copied() else {
        return Ok(());
    };
    let below = usize::try_from(i64::from(dim.lower) - i64::from(lower)).unwrap_or(0);
    let above = usize::try_from(i64::from(upper) - i64::from(dim.upper())).unwrap_or(0);
    if below == 0 && above == 0 {
        return Ok(());
    }
    check_array_size(
        below
            .saturating_add(array.elems.len())
            .saturating_add(above),
    )?;
    let mut elems = vec![Datum::Null; below];
    elems.extend(array.elems.iter().cloned());
    elems.extend(std::iter::repeat_n(Datum::Null, above));
    let new_lower = dim.lower.min(lower);
    let len = i32::try_from(elems.len()).unwrap_or(i32::MAX);
    array.dims = vec![crabka_pgtypes::ArrayDim::new(new_lower, len)];
    array.elems = elems;
    Ok(())
}

fn wrong_number_of_subscripts() -> ExecError {
    ExecError::Type(TypeError::array_subscript(
        "wrong number of array subscripts",
    ))
}

fn subscript_out_of_range() -> ExecError {
    ExecError::Type(TypeError::array_subscript("array subscript out of range"))
}

/// The `ANY`/`SOME` and `ALL` quantifiers over an array.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Quantifier {
    /// `x <op> ANY (array)` / `SOME`.
    Any,
    /// `x <op> ALL (array)`.
    All,
}

/// Evaluate `x <op> ANY/ALL (array)` with SQL three-valued logic.
///
/// `compare` applies the operator to one element and returns `Bool` or NULL. The
/// caller passes the ordinary binary-operator evaluator, so every operator gets
/// the same quantified semantics.
///
/// - `ANY`: true as soon as one element compares true. If no element compares
///   true, the result is NULL when any comparison was NULL, because a
///   false-with-NULL result is **unknown**, not false. Otherwise the result is
///   false. An empty array is false, even for a NULL left operand.
/// - `ALL`: false as soon as one element compares false. If no element compares
///   false, the result is NULL when any comparison was NULL. Otherwise the
///   result is true. An empty array is true.
/// - A NULL array is NULL for both.
pub(crate) fn eval_quantified(
    array: &Datum,
    quantifier: Quantifier,
    mut compare: impl FnMut(&Datum) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let array = match array {
        Datum::Null => return Ok(Datum::Null),
        Datum::Array(a) | Datum::OidVector(a) => a,
        other => return Err(not_an_array(other)),
    };
    let mut saw_null = false;
    for elem in &array.elems {
        match compare(elem)? {
            Datum::Bool(true) => {
                if quantifier == Quantifier::Any {
                    return Ok(Datum::Bool(true));
                }
            }
            Datum::Bool(false) => {
                if quantifier == Quantifier::All {
                    return Ok(Datum::Bool(false));
                }
            }
            Datum::Null => saw_null = true,
            other => {
                return Err(ExecError::TypeMismatch(format!(
                    "argument of quantified comparison must be type boolean, not type {}",
                    type_name(&other)
                )));
            }
        }
    }
    if saw_null {
        return Ok(Datum::Null);
    }
    Ok(Datum::Bool(quantifier == Quantifier::All))
}

// ---- argument helpers ----

fn undefined_function(name: &str) -> ExecError {
    ExecError::UndefinedFunction(format!("function {name}(...) does not exist"))
}

fn operator_undefined(op: &str, left: &Datum, right: &Datum) -> ExecError {
    ExecError::UndefinedFunction(format!(
        "operator does not exist: {} {op} {}",
        type_name(left),
        type_name(right)
    ))
}

fn not_an_array(d: &Datum) -> ExecError {
    ExecError::TypeMismatch(format!(
        "expected an array value, not type {}",
        type_name(d)
    ))
}

fn type_name(d: &Datum) -> &'static str {
    d.column_type().map_or("unknown", ColumnType::name)
}

/// The positional argument list. Array functions never accept `f(*)`.
fn exprs_of(fc: &FuncCall) -> Result<&[Expr], ExecError> {
    match &fc.args {
        FuncArgs::Exprs(v) => Ok(v),
        FuncArgs::Star => Err(undefined_function(&fc.name)),
    }
}

fn require_arity(fc: &FuncCall, ok: bool) -> Result<(), ExecError> {
    if ok {
        Ok(())
    } else {
        Err(undefined_function(&fc.name))
    }
}

fn array_value<'a>(d: &'a Datum, name: &str) -> Result<&'a ArrayValue, ExecError> {
    match d {
        Datum::Array(a) | Datum::OidVector(a) => Ok(a),
        _ => Err(undefined_function(name)),
    }
}

/// An array operand of a binary operator. Returns `Ok(None)` for SQL NULL,
/// because the operator is strict, and an error for a non-array operand.
fn array_or_null<'a>(d: &'a Datum, op: &str) -> Result<Option<&'a ArrayValue>, ExecError> {
    match d {
        Datum::Null => Ok(None),
        Datum::Array(a) | Datum::OidVector(a) => Ok(Some(a)),
        other => Err(ExecError::UndefinedFunction(format!(
            "operator does not exist: {} {op} ...",
            type_name(other)
        ))),
    }
}

fn require_same_element(
    left: &ArrayValue,
    right: &ArrayValue,
    op: &str,
    l: &Datum,
    r: &Datum,
) -> Result<(), ExecError> {
    if left.elem == right.elem {
        Ok(())
    } else {
        Err(operator_undefined(op, l, r))
    }
}

fn text_arg<'a>(d: &'a Datum, name: &str) -> Result<&'a str, ExecError> {
    match d {
        Datum::Text(s) => Ok(s),
        _ => Err(undefined_function(name)),
    }
}

fn int_arg(d: &Datum, name: &str) -> Result<i32, ExecError> {
    match d {
        Datum::Int4(n) => Ok(*n),
        Datum::Int8(n) => i32::try_from(*n).map_err(|_| ExecError::Type(TypeError::Overflow)),
        _ => Err(undefined_function(name)),
    }
}

/// A single-element array. This is the `array_append`/`array_prepend` path where
/// the array operand is NULL and so behaves like the empty array. The element's
/// own type wins when it has one. A NULL element falls back to `into`, the type
/// the call's polymorphic pair resolved to.
fn singleton_from_element(elem: &Datum, into: ElemType) -> Datum {
    let t = elem
        .column_type()
        .and_then(ElemType::from_column_type)
        .unwrap_or(into);
    Datum::Array(ArrayValue::new(t, vec![elem.clone()]))
}

/// Coerce an element into an array's element type. For example,
/// `array_append(bigint[], 1)` stores an `int8`. An undefined conversion is
/// 42804.
fn coerce_element(elem: &Datum, to: ElemType, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    if elem.is_null() || elem.column_type() == Some(to.column_type()) {
        return Ok(elem.clone());
    }
    // A string element goes through the target type's INPUT function, so a
    // malformed one keeps that function's 22P02 rather than becoming 42804.
    if matches!(elem, Datum::Text(_)) {
        return cast::cast(elem, to.column_type(), &ctx.time_zone).map_err(ExecError::Type);
    }
    cast::cast(elem, to.column_type(), &ctx.time_zone).map_err(|_| {
        ExecError::TypeMismatch(format!(
            "cannot store a value of type {} in an array of {}",
            type_name(elem),
            to.name()
        ))
    })
}

// ---- the remaining array functions ----

/// The verbs PostgreSQL names in its one-dimensional-only refusals.
const SEARCH_ACTION: &str = "searching for elements in";
const REMOVE_ACTION: &str = "removing elements from";

/// The element type `array_fill` builds with. It is the value's own type when it
/// has one, else what its `anyelement` parameter resolved to.
fn resolved_fill_element(value: &Datum, params: &[Option<ColumnType>]) -> ElemType {
    value
        .column_type()
        .and_then(ElemType::from_column_type)
        .or_else(|| {
            params
                .first()
                .copied()
                .flatten()
                .and_then(ElemType::from_column_type)
        })
        .unwrap_or(ElemType::Text)
}

/// `array_fill(value, dims [, lower_bounds])`: an array of `value` repeated
/// over the given shape. Both shape arguments must be non-NULL `int[]`s of the
/// same length. A zero-length dimension yields the empty array.
fn array_fill(
    value: &Datum,
    dims: &Datum,
    lower_bounds: Option<&Datum>,
    elem: ElemType,
) -> Result<Datum, ExecError> {
    let null_shape = ExecError::Type(TypeError::Coded {
        sqlstate: "22004",
        message: "dimension array or low bound array cannot be null".into(),
    });
    let lengths = int_array_arg(dims).ok_or(null_shape)?;
    let lowers = match lower_bounds {
        None | Some(Datum::Null) if lower_bounds.is_some() => {
            return Err(ExecError::Type(TypeError::Coded {
                sqlstate: "22004",
                message: "dimension array or low bound array cannot be null".into(),
            }));
        }
        None => vec![1; lengths.len()],
        Some(d) => {
            let lowers = int_array_arg(d).ok_or_else(|| {
                ExecError::Type(TypeError::Coded {
                    sqlstate: "22004",
                    message: "dimension array or low bound array cannot be null".into(),
                })
            })?;
            if lowers.len() != lengths.len() {
                return Err(wrong_number_of_subscripts());
            }
            lowers
        }
    };
    if lengths.len() > crabka_pgtypes::MAX_ARRAY_DIM {
        return Err(ExecError::Type(TypeError::Coded {
            sqlstate: "54000",
            message: format!(
                "number of array dimensions ({}) exceeds the maximum allowed ({})",
                lengths.len(),
                crabka_pgtypes::MAX_ARRAY_DIM
            ),
        }));
    }
    let mut total = 1usize;
    let mut shape = Vec::with_capacity(lengths.len());
    for (len, lower) in lengths.iter().zip(&lowers) {
        if *len < 0 {
            return Err(subscript_out_of_range());
        }
        total = total
            .checked_mul(usize::try_from(*len).unwrap_or(0))
            .ok_or(ExecError::Type(TypeError::Overflow))?;
        shape.push(crabka_pgtypes::ArrayDim::new(*lower, *len));
    }
    if total == 0 {
        return Ok(Datum::Array(ArrayValue::new(elem, Vec::new())));
    }
    check_array_size(total)?;
    Ok(Datum::Array(ArrayValue::with_dims(
        elem,
        vec![value.clone(); total],
        shape,
    )))
}

/// The `int[]` shape arguments of `array_fill`. Returns `None` for a NULL
/// argument or a NULL element. PostgreSQL rejects both the same way.
fn int_array_arg(d: &Datum) -> Option<Vec<i32>> {
    let Datum::Array(a) = d else {
        return None;
    };
    a.elems
        .iter()
        .map(|e| match e {
            Datum::Int2(n) => Some(i32::from(*n)),
            Datum::Int4(n) => Some(*n),
            Datum::Int8(n) => i32::try_from(*n).ok(),
            _ => None,
        })
        .collect()
}

/// `array_position(array, value [, start])`: the 1-based offset of the first
/// occurrence at or after `start`, or NULL when there is none.
fn array_position(
    array: &Datum,
    needle: &Datum,
    start: Option<&Datum>,
    name: &str,
) -> Result<Datum, ExecError> {
    if array.is_null() {
        return Ok(Datum::Null);
    }
    let a = require_at_most_one_dimension(array_value(array, name)?, SEARCH_ACTION)?;
    let from = match start {
        None => 1,
        Some(Datum::Null) => return Ok(Datum::Null),
        Some(d) => int_arg(d, name)?,
    };
    let skip = usize::try_from(from.max(1)).unwrap_or(1).saturating_sub(1);
    for (i, e) in a.elems.iter().enumerate().skip(skip) {
        if element_matches(e, needle) {
            return Ok(Datum::Int4(i32::try_from(i + 1).unwrap_or(i32::MAX)));
        }
    }
    Ok(Datum::Null)
}

/// `array_positions(array, value)`: every 1-based offset, as an `int[]`.
fn array_positions(array: &Datum, needle: &Datum, name: &str) -> Result<Datum, ExecError> {
    if array.is_null() {
        return Ok(Datum::Null);
    }
    let a = require_at_most_one_dimension(array_value(array, name)?, SEARCH_ACTION)?;
    let found: Vec<Datum> = a
        .elems
        .iter()
        .enumerate()
        .filter(|(_, e)| element_matches(e, needle))
        .map(|(i, _)| Datum::Int4(i32::try_from(i + 1).unwrap_or(i32::MAX)))
        .collect();
    Ok(Datum::Array(ArrayValue::new(ElemType::Int4, found)))
}

/// `array_remove(array, value)`: every matching element dropped. A NULL `value`
/// removes the NULL elements, which matches PostgreSQL's `IS NOT DISTINCT FROM`
/// treatment here.
fn array_remove(array: &Datum, needle: &Datum, name: &str) -> Result<Datum, ExecError> {
    if array.is_null() {
        return Ok(Datum::Null);
    }
    let a = require_at_most_one_dimension(array_value(array, name)?, REMOVE_ACTION)?;
    let kept: Vec<Datum> = a
        .elems
        .iter()
        .filter(|e| !element_matches(e, needle))
        .cloned()
        .collect();
    Ok(Datum::Array(ArrayValue::new(a.elem, kept)))
}

/// `array_replace(array, from, to)`: every matching element replaced, over any
/// number of dimensions. The shape does not change, unlike `array_remove`.
fn array_replace(array: &Datum, from: &Datum, to: &Datum, name: &str) -> Result<Datum, ExecError> {
    if array.is_null() {
        return Ok(Datum::Null);
    }
    let a = array_value(array, name)?;
    let elems = a
        .elems
        .iter()
        .map(|e| {
            if element_matches(e, from) {
                to.clone()
            } else {
                e.clone()
            }
        })
        .collect();
    Ok(Datum::Array(ArrayValue::with_dims(
        a.elem,
        elems,
        a.dims.clone(),
    )))
}

/// `trim_array(array, n)`: the array without its last `n` elements. `n` outside
/// `0..=cardinality` is 2202E.
fn trim_array(array: &Datum, count: &Datum, name: &str) -> Result<Datum, ExecError> {
    if array.is_null() || count.is_null() {
        return Ok(Datum::Null);
    }
    let a = array_value(array, name)?;
    let n = int_arg(count, name)?;
    let total = i32::try_from(a.elems.len()).unwrap_or(i32::MAX);
    if n < 0 || n > total {
        return Err(ExecError::Type(TypeError::array_subscript(format!(
            "number of elements to trim must be between 0 and {total}"
        ))));
    }
    let keep = usize::try_from(total - n).unwrap_or(0);
    Ok(Datum::Array(ArrayValue::new(
        a.elem,
        a.elems[..keep].to_vec(),
    )))
}

/// `array_sample(array, n)`: `n` of the outermost slices, chosen at random.
fn array_sample(array: &Datum, count: &Datum, name: &str) -> Result<Datum, ExecError> {
    if array.is_null() || count.is_null() {
        return Ok(Datum::Null);
    }
    let a = array_value(array, name)?;
    let n = int_arg(count, name)?;
    let slices = outer_slices(a);
    let total = i32::try_from(slices.len()).unwrap_or(i32::MAX);
    if n < 0 || n > total {
        return Err(ExecError::Type(TypeError::Coded {
            sqlstate: "22023",
            message: format!("sample size must be between 0 and {total}"),
        }));
    }
    let mut order: Vec<usize> = (0..slices.len()).collect();
    shuffle_in_place(&mut order);
    order.truncate(usize::try_from(n).unwrap_or(0));
    Ok(rebuild_from_slices(a, &order, &slices))
}

/// `array_shuffle(array)`: the outermost slices in a random order.
fn array_shuffle(array: &Datum, name: &str) -> Result<Datum, ExecError> {
    if array.is_null() {
        return Ok(Datum::Null);
    }
    let a = array_value(array, name)?;
    let slices = outer_slices(a);
    let mut order: Vec<usize> = (0..slices.len()).collect();
    shuffle_in_place(&mut order);
    Ok(rebuild_from_slices(a, &order, &slices))
}

/// `array_reverse(array)`: the outermost slices in the opposite order.
fn array_reverse(array: &Datum, name: &str) -> Result<Datum, ExecError> {
    if array.is_null() {
        return Ok(Datum::Null);
    }
    let a = array_value(array, name)?;
    let slices = outer_slices(a);
    let order: Vec<usize> = (0..slices.len()).rev().collect();
    Ok(rebuild_from_slices(a, &order, &slices))
}

/// `array_sort(array [, descending [, nulls_first]])`: the outermost slices in
/// btree order. `nulls_first` defaults to `descending`, as `PostgreSQL` 18 does.
fn array_sort(
    array: &Datum,
    descending: Option<&Datum>,
    nulls_first: Option<&Datum>,
    name: &str,
) -> Result<Datum, ExecError> {
    if array.is_null() {
        return Ok(Datum::Null);
    }
    let desc = match descending {
        None => false,
        Some(Datum::Null) => return Ok(Datum::Null),
        Some(Datum::Bool(b)) => *b,
        Some(_) => return Err(undefined_function(name)),
    };
    let nulls_first = match nulls_first {
        None => desc,
        Some(Datum::Null) => return Ok(Datum::Null),
        Some(Datum::Bool(b)) => *b,
        Some(_) => return Err(undefined_function(name)),
    };
    let a = array_value(array, name)?;
    let slices = outer_slices(a);
    let mut order: Vec<usize> = (0..slices.len()).collect();
    let mut failure = None;
    order.sort_by(|x, y| {
        compare_slices(&slices[*x], &slices[*y], nulls_first, &mut failure).then_with(|| x.cmp(y))
    });
    if let Some(error) = failure {
        return Err(error);
    }
    if desc {
        order.reverse();
    }
    Ok(rebuild_from_slices(a, &order, &slices))
}

/// The outermost slices of an array as flat element runs. There is one run per
/// index of dimension 1, and each run holds the whole sub-box beneath it. So a
/// one-dimensional array yields one single-element run per element.
fn outer_slices(array: &ArrayValue) -> Vec<Vec<Datum>> {
    let Some(first) = array.dims.first() else {
        return Vec::new();
    };
    let stride = array.strides().first().copied().unwrap_or(1).max(1);
    let count = usize::try_from(first.len).unwrap_or(0);
    (0..count)
        .map(|i| {
            let start = i * stride;
            let end = (start + stride).min(array.elems.len());
            array.elems.get(start..end).unwrap_or(&[]).to_vec()
        })
        .collect()
}

/// Reassemble an array from a permutation or a subset of its outermost slices.
/// This function keeps every inner dimension and lower bound.
fn rebuild_from_slices(array: &ArrayValue, order: &[usize], slices: &[Vec<Datum>]) -> Datum {
    let mut elems = Vec::new();
    for i in order {
        if let Some(slice) = slices.get(*i) {
            elems.extend(slice.iter().cloned());
        }
    }
    if elems.is_empty() {
        return Datum::Array(ArrayValue::new(array.elem, Vec::new()));
    }
    let mut dims = array.dims.clone();
    if let Some(first) = dims.first_mut() {
        first.len = i32::try_from(order.len()).unwrap_or(i32::MAX);
    }
    Datum::Array(ArrayValue::with_dims(array.elem, elems, dims))
}

/// Compare two outermost slices element-wise. This function records the first
/// comparison failure, and does not propagate it out of the sort comparator.
fn compare_slices(
    left: &[Datum],
    right: &[Datum],
    nulls_first: bool,
    failure: &mut Option<ExecError>,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    for (x, y) in left.iter().zip(right) {
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) | (false, true) => {
                let null_side = if x.is_null() {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
                if nulls_first {
                    null_side
                } else {
                    null_side.reverse()
                }
            }
            (false, false) => match crabka_pgtypes::ops::compare(x, y) {
                Ok(Some(ord)) => ord,
                Ok(None) => Ordering::Equal,
                Err(e) => {
                    failure.get_or_insert(ExecError::Type(e));
                    Ordering::Equal
                }
            },
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    left.len().cmp(&right.len())
}

/// A deterministic-per-process Fisher-Yates shuffle. `array_shuffle` and
/// `array_sample` are volatile in PostgreSQL, so only the *set* of elements is
/// observable. Nothing in the corpus depends on a particular permutation.
fn shuffle_in_place(order: &mut [usize]) {
    use std::{
        cell::Cell,
        hash::{BuildHasher, Hasher, RandomState},
    };

    thread_local! {
        static SEED: Cell<u64> = const { Cell::new(0) };
    }
    let mut state = SEED.with(|seed| {
        let current = seed.get();
        if current == 0 {
            let mut hasher = RandomState::new().build_hasher();
            hasher.write_u64(0x9E37_79B9_7F4A_7C15);
            let fresh = hasher.finish() | 1;
            seed.set(fresh);
            fresh
        } else {
            current
        }
    });
    for i in (1..order.len()).rev() {
        // xorshift64* — enough mixing for a shuffle nothing asserts on.
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = usize::try_from(state % (i as u64 + 1)).unwrap_or(0);
        order.swap(i, j);
    }
    SEED.with(|seed| seed.set(state | 1));
}

/// `array_position`/`array_positions`/`array_remove` are one-dimensional only.
/// `PostgreSQL` reports 0A000 with the verb the caller is running.
fn require_at_most_one_dimension<'a>(
    a: &'a ArrayValue,
    action: &str,
) -> Result<&'a ArrayValue, ExecError> {
    if a.ndims() > 1 {
        return Err(ExecError::Type(TypeError::Coded {
            sqlstate: "0A000",
            message: format!("{action} multidimensional arrays is not supported"),
        }));
    }
    Ok(a)
}

/// Element identity for the search functions is `IS NOT DISTINCT FROM`, so a
/// NULL needle finds the NULL elements.
fn element_matches(element: &Datum, needle: &Datum) -> bool {
    match (element.is_null(), needle.is_null()) {
        (true, true) => true,
        (true, false) | (false, true) => false,
        (false, false) => element == needle,
    }
}

/// A non-NULL datum's PostgreSQL text output.
fn datum_text(d: &Datum, ctx: &EvalCtx) -> String {
    String::from_utf8(crabka_pgtypes::encoding::encode_text(d, &ctx.time_zone))
        .expect("a Datum's text encoding is always valid UTF-8")
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn ctx() -> EvalCtx {
        EvalCtx::test_default()
    }

    /// A `'{…}'::T[]` expression built directly, so these tests do not depend on
    /// the parser's array-literal syntax.
    fn array_expr(literal: &str, elem: ElemType) -> Expr {
        Expr::Cast {
            expr: Box::new(Expr::StringLiteral(literal.to_string())),
            ty: ColumnType::Array(elem),
        }
    }

    fn null_array_expr(elem: ElemType) -> Expr {
        Expr::Cast {
            expr: Box::new(Expr::NullLiteral),
            ty: ColumnType::Array(elem),
        }
    }

    /// A `NULL::T` expression: a run-time SQL NULL that still states its type.
    fn null_expr(ty: ColumnType) -> Expr {
        Expr::Cast {
            expr: Box::new(Expr::NullLiteral),
            ty,
        }
    }

    fn text_expr(s: &str) -> Expr {
        Expr::StringLiteral(s.to_string())
    }

    fn int_expr(n: i32) -> Expr {
        Expr::IntLiteral(n.to_string())
    }

    fn func(name: &str, args: Vec<Expr>) -> FuncCall {
        FuncCall {
            name: name.to_string(),
            distinct: false,
            args: FuncArgs::Exprs(args),
            filter: None,
        }
    }

    fn call(name: &str, args: Vec<Expr>) -> Result<Datum, ExecError> {
        let ctx = ctx();
        eval_array(&func(name, args), &ctx, |e| {
            crate::eval::eval(e, &Scope::empty(), &[], &ctx)
        })
    }

    fn result_type(name: &str, args: Vec<Expr>) -> Result<ColumnType, ExecError> {
        array_func_result_type(&func(name, args), &Scope::empty())
    }

    fn sqlstate(e: ExecError) -> String {
        e.into_pg().code
    }

    /// `PostgreSQL` resolves an `unknown` literal argument against the parameter
    /// it is passed to. For the polymorphic pairs that means the side which does
    /// carry a type resolves both, and when neither does the pair settles on
    /// `text`. Type and value on each row were taken from `PostgreSQL` 18.4.
    #[test]
    fn unknown_literal_arguments_adopt_their_parameter_type() {
        let int_array = ColumnType::Array(ElemType::Int4);
        let text_array = ColumnType::Array(ElemType::Text);
        let cases: [(&str, Vec<Expr>, ColumnType, Datum); 8] = [
            // The typed side of an `anyarray`/`anyelement` pair resolves both.
            (
                "array_append",
                vec![text_expr("{1,2}"), int_expr(3)],
                int_array,
                ints(&[Some(1), Some(2), Some(3)]),
            ),
            (
                "array_prepend",
                vec![int_expr(1), text_expr("{2,3}")],
                int_array,
                ints(&[Some(1), Some(2), Some(3)]),
            ),
            (
                "array_cat",
                vec![array_expr("{1}", ElemType::Int4), text_expr("{2,3}")],
                int_array,
                ints(&[Some(1), Some(2), Some(3)]),
            ),
            // Neither side carries a type: PostgreSQL settles on `text`.
            (
                "array_cat",
                vec![text_expr("{1,2}"), text_expr("{3}")],
                text_array,
                texts(&[Some("1"), Some("2"), Some("3")]),
            ),
            // A bare NULL is `unknown` too, so it resolves nothing either.
            (
                "array_append",
                vec![text_expr("{1,2}"), Expr::NullLiteral],
                text_array,
                texts(&[Some("1"), Some("2"), None]),
            ),
            // Non-polymorphic parameters: the delimiter and null-string are text.
            (
                "array_to_string",
                vec![array_expr("{1,2}", ElemType::Int4), text_expr(",")],
                ColumnType::Text,
                Datum::Text("1,2".to_string()),
            ),
            (
                "string_to_array",
                vec![text_expr("a,b"), text_expr(",")],
                text_array,
                texts(&[Some("a"), Some("b")]),
            ),
            (
                "array_length",
                vec![array_expr("{1,2}", ElemType::Int4), int_expr(1)],
                ColumnType::Int4,
                Datum::Int4(2),
            ),
        ];
        for (name, args, ty, value) in cases {
            assert!(result_type(name, args.clone()).expect(name) == ty, "{name}");
            assert!(call(name, args).expect(name) == value, "{name}");
        }
    }

    /// `array_length`, `cardinality` and `array_to_string` have nothing that can
    /// resolve their `anyarray` parameter, so an `unknown` literal there is
    /// `PostgreSQL`'s 42804. It is not a guess, and it is not the 42883 a plain
    /// "that is not an array" check would report.
    #[test]
    fn an_unknown_literal_array_argument_that_resolves_to_nothing_is_rejected() {
        for (name, args) in [
            ("array_length", vec![text_expr("{1,2}"), int_expr(1)]),
            ("cardinality", vec![text_expr("{1,2}")]),
            ("cardinality", vec![Expr::NullLiteral]),
            ("array_to_string", vec![text_expr("{1,2}"), text_expr(",")]),
        ] {
            assert!(
                sqlstate(result_type(name, args.clone()).expect_err(name)) == "42804",
                "{name}"
            );
            assert!(
                sqlstate(call(name, args).expect_err(name)) == "42804",
                "{name}"
            );
        }
        // The resolved element type is applied to the literal, so an element it
        // cannot parse as is 22P02.
        assert!(
            sqlstate(
                call(
                    "array_append",
                    vec![array_expr("{1,2}", ElemType::Int4), text_expr("x")],
                )
                .expect_err("element")
            ) == "22P02"
        );
    }

    fn ints(values: &[Option<i32>]) -> Datum {
        Datum::Array(ArrayValue::new(
            ElemType::Int4,
            values
                .iter()
                .map(|v| v.map_or(Datum::Null, Datum::Int4))
                .collect(),
        ))
    }

    fn texts(values: &[Option<&str>]) -> Datum {
        Datum::Array(ArrayValue::new(
            ElemType::Text,
            values
                .iter()
                .map(|v| v.map_or(Datum::Null, |s| Datum::Text(s.to_string())))
                .collect(),
        ))
    }

    #[test]
    fn classifier_covers_the_family_only() {
        for name in [
            "array_length",
            "cardinality",
            "array_append",
            "array_prepend",
            "array_cat",
            "array_to_string",
            "string_to_array",
        ] {
            assert!(is_array_func(name));
        }
        assert!(!is_array_func("array_agg"));
        assert!(!is_array_func("length"));
        assert!(!is_array_func("unnest"));
    }

    #[test]
    fn length_and_cardinality_differ_on_the_empty_array() {
        let three = || array_expr("{1,2,3}", ElemType::Int4);
        let empty = || array_expr("{}", ElemType::Int4);
        assert!(call("array_length", vec![three(), int_expr(1)]).expect("len") == Datum::Int4(3));
        // Only dimension 1 exists, and an empty array has no dimension at all.
        assert!(call("array_length", vec![three(), int_expr(2)]).expect("len") == Datum::Null);
        assert!(call("array_length", vec![empty(), int_expr(1)]).expect("len") == Datum::Null);
        assert!(
            call(
                "array_length",
                vec![null_array_expr(ElemType::Int4), int_expr(1)]
            )
            .expect("len")
                == Datum::Null
        );
        assert!(
            call("array_length", vec![three(), Expr::NullLiteral]).expect("len") == Datum::Null
        );
        // cardinality counts elements and is 0 (not NULL) for the empty array.
        assert!(call("cardinality", vec![three()]).expect("card") == Datum::Int4(3));
        assert!(call("cardinality", vec![empty()]).expect("card") == Datum::Int4(0));
        assert!(
            call("cardinality", vec![null_array_expr(ElemType::Int4)]).expect("card")
                == Datum::Null
        );
    }

    #[test]
    fn append_prepend_and_cat_treat_a_null_array_as_empty() {
        let ctx = ctx();
        let int4 = ElemType::Int4;
        assert!(
            array_append(&ints(&[Some(1)]), &Datum::Int4(2), int4, &ctx).expect("append")
                == ints(&[Some(1), Some(2)])
        );
        // A NULL element is stored as a NULL element, not dropped.
        assert!(
            array_append(&ints(&[Some(1)]), &Datum::Null, int4, &ctx).expect("append")
                == ints(&[Some(1), None])
        );
        // A NULL array behaves like an empty array of the element's type.
        assert!(
            array_append(&Datum::Null, &Datum::Int4(7), int4, &ctx).expect("append")
                == ints(&[Some(7)])
        );
        // Two NULLs still append: the resolved element type carries the result,
        // so `array_append(NULL::int4[], NULL::int4)` is `{NULL}`, not SQL NULL.
        assert!(
            array_append(&Datum::Null, &Datum::Null, int4, &ctx).expect("append") == ints(&[None])
        );
        assert!(
            array_prepend(&Datum::Null, &Datum::Null, int4, &ctx).expect("prepend")
                == ints(&[None])
        );
        assert!(
            array_prepend(&Datum::Int4(0), &ints(&[Some(1)]), int4, &ctx).expect("prepend")
                == ints(&[Some(0), Some(1)])
        );
        assert!(
            array_cat(&ints(&[Some(1)]), &ints(&[Some(2), Some(3)])).expect("cat")
                == ints(&[Some(1), Some(2), Some(3)])
        );
        assert!(array_cat(&Datum::Null, &ints(&[Some(1)])).expect("cat") == ints(&[Some(1)]));
        assert!(array_cat(&ints(&[Some(1)]), &Datum::Null).expect("cat") == ints(&[Some(1)]));
        assert!(array_cat(&Datum::Null, &Datum::Null).expect("cat") == Datum::Null);
        // Mismatched element types have no operator.
        assert!(
            sqlstate(array_cat(&ints(&[Some(1)]), &texts(&[Some("a")])).expect_err("mismatch"))
                == "42883"
        );
    }

    /// A typed NULL array is the EMPTY array of its element type, so an append
    /// to it always builds a one-element array. That holds even when the element
    /// is a typed NULL too and nothing carries a type at run time. `array_cat`
    /// is the one that stays NULL, because two empty arrays concatenate to
    /// nothing.
    /// Type and value on each row were taken from `PostgreSQL` 18.4.
    #[test]
    fn a_typed_null_array_keeps_its_element_type() {
        let int4 = ColumnType::Int4;
        let int_array = ColumnType::Array(ElemType::Int4);
        let text_array = ColumnType::Array(ElemType::Text);
        let cases: [(&str, Vec<Expr>, ColumnType, Datum); 6] = [
            (
                "array_append",
                vec![null_expr(int_array), null_expr(int4)],
                int_array,
                ints(&[None]),
            ),
            (
                "array_append",
                vec![null_expr(int_array), int_expr(1)],
                int_array,
                ints(&[Some(1)]),
            ),
            (
                "array_append",
                vec![array_expr("{1}", ElemType::Int4), Expr::NullLiteral],
                int_array,
                ints(&[Some(1), None]),
            ),
            (
                "array_prepend",
                vec![null_expr(int4), null_expr(int_array)],
                int_array,
                ints(&[None]),
            ),
            // Neither operand states a type: PostgreSQL settles the pair on text.
            (
                "array_append",
                vec![Expr::NullLiteral, Expr::NullLiteral],
                text_array,
                texts(&[None]),
            ),
            (
                "array_cat",
                vec![null_expr(int_array), null_expr(int_array)],
                int_array,
                Datum::Null,
            ),
        ];
        for (name, args, ty, value) in cases {
            assert!(result_type(name, args.clone()).expect(name) == ty, "{name}");
            assert!(call(name, args).expect(name) == value, "{name}");
        }
    }

    #[test]
    fn concat_resolves_its_form_from_the_operand_types() {
        let int_array = ColumnType::Array(ElemType::Int4);
        let text_array = ColumnType::Array(ElemType::Text);
        assert!(concat_form(int_array, int_array) == Some(ConcatForm::ArrayArray));
        assert!(
            concat_form(int_array, ColumnType::Int4)
                == Some(ConcatForm::ArrayElement(ElemType::Int4))
        );
        assert!(
            concat_form(ColumnType::Int4, int_array)
                == Some(ConcatForm::ElementArray(ElemType::Int4))
        );
        // Mismatched element types, and two non-arrays, resolve to no array `||`.
        assert!(concat_form(int_array, text_array).is_none());
        assert!(concat_form(int_array, ColumnType::Text).is_none());
        assert!(concat_form(ColumnType::Text, ColumnType::Text).is_none());
        // The result type follows the array side.
        assert!(concat_result_type(int_array, ColumnType::Int4) == Some(int_array));
        assert!(concat_result_type(ColumnType::Int4, int_array) == Some(int_array));
    }

    #[test]
    fn concat_applies_the_resolved_form() {
        let ctx = ctx();
        assert!(
            array_concat(
                ConcatForm::ArrayArray,
                &ints(&[Some(1)]),
                &ints(&[Some(2)]),
                &ctx
            )
            .expect("cat")
                == ints(&[Some(1), Some(2)])
        );
        assert!(
            array_concat(
                ConcatForm::ArrayElement(ElemType::Int4),
                &ints(&[Some(1)]),
                &Datum::Int4(2),
                &ctx
            )
            .expect("append")
                == ints(&[Some(1), Some(2)])
        );
        assert!(
            array_concat(
                ConcatForm::ElementArray(ElemType::Int4),
                &Datum::Int4(0),
                &ints(&[Some(1)]),
                &ctx
            )
            .expect("prepend")
                == ints(&[Some(0), Some(1)])
        );
        // The two NULL readings are what the form distinguishes: a NULL *array*
        // concatenates to nothing, a NULL *element* appends a NULL.
        assert!(
            array_concat(
                ConcatForm::ArrayArray,
                &ints(&[Some(1)]),
                &Datum::Null,
                &ctx
            )
            .expect("cat")
                == ints(&[Some(1)])
        );
        assert!(
            array_concat(
                ConcatForm::ArrayElement(ElemType::Int4),
                &ints(&[Some(1)]),
                &Datum::Null,
                &ctx
            )
            .expect("append")
                == ints(&[Some(1), None])
        );
        // An int4 element coerces into an int8 array rather than failing.
        let big = Datum::Array(ArrayValue::new(ElemType::Int8, vec![Datum::Int8(1)]));
        assert!(
            array_concat(
                ConcatForm::ArrayElement(ElemType::Int8),
                &big,
                &Datum::Int4(2),
                &ctx
            )
            .expect("append")
                == Datum::Array(ArrayValue::new(
                    ElemType::Int8,
                    vec![Datum::Int8(1), Datum::Int8(2)]
                ))
        );
    }

    #[test]
    fn containment_and_overlap_handle_null_elements() {
        // (left, right, l @> r, l <@ r, l && r)
        let cases: [(Datum, Datum, Datum, Datum, Datum); 5] = [
            (
                ints(&[Some(1), Some(2), Some(3)]),
                ints(&[Some(3), Some(1)]),
                Datum::Bool(true),
                Datum::Bool(false),
                Datum::Bool(true),
            ),
            (
                ints(&[Some(1)]),
                ints(&[Some(2)]),
                Datum::Bool(false),
                Datum::Bool(false),
                Datum::Bool(false),
            ),
            // The empty array is contained by everything and contains nothing but itself.
            (
                ints(&[Some(1)]),
                ints(&[]),
                Datum::Bool(true),
                Datum::Bool(false),
                Datum::Bool(false),
            ),
            // A NULL on the contained side can never be matched (strict equality).
            (
                ints(&[Some(1), None]),
                ints(&[None]),
                Datum::Bool(false),
                Datum::Bool(false),
                Datum::Bool(false),
            ),
            // ... but a NULL on the containing side is merely never a match.
            (
                ints(&[Some(1), None]),
                ints(&[Some(1)]),
                Datum::Bool(true),
                Datum::Bool(false),
                Datum::Bool(true),
            ),
        ];
        for (left, right, contains, contained, overlap) in cases {
            assert!(array_contains(&left, &right).expect("@>") == contains);
            assert!(array_contained_by(&left, &right).expect("<@") == contained);
            assert!(array_overlap(&left, &right).expect("&&") == overlap);
        }
        // Strict on a NULL array.
        assert!(array_contains(&Datum::Null, &ints(&[Some(1)])).expect("@>") == Datum::Null);
        assert!(array_overlap(&ints(&[Some(1)]), &Datum::Null).expect("&&") == Datum::Null);
        // Mismatched element types have no operator.
        assert!(
            sqlstate(array_contains(&ints(&[Some(1)]), &texts(&[Some("1")])).expect_err("types"))
                == "42883"
        );
    }

    #[test]
    fn subscripting_is_one_based_and_null_outside_the_bounds() {
        let a = ints(&[Some(10), Some(20), None]);
        let cases = [
            (Datum::Int4(1), Datum::Int4(10)),
            (Datum::Int4(2), Datum::Int4(20)),
            (Datum::Int4(3), Datum::Null),
            (Datum::Int4(0), Datum::Null),
            (Datum::Int4(-1), Datum::Null),
            (Datum::Int4(4), Datum::Null),
            (Datum::Null, Datum::Null),
        ];
        for (index, expected) in cases {
            assert!(array_subscript(&a, &index).expect("subscript") == expected);
        }
        assert!(array_subscript(&Datum::Null, &Datum::Int4(1)).expect("subscript") == Datum::Null);
        assert!(
            sqlstate(array_subscript(&Datum::Int4(1), &Datum::Int4(1)).expect_err("not an array"))
                == "42804"
        );
    }

    #[test]
    fn quantified_comparison_is_three_valued() {
        // `compare` stands in for the caller's binary-operator evaluator: NULL for
        // a NULL element, mirroring `=`'s strictness.
        let eq = |needle: i32| {
            move |e: &Datum| -> Result<Datum, ExecError> {
                Ok(match e {
                    Datum::Null => Datum::Null,
                    Datum::Int4(n) => Datum::Bool(*n == needle),
                    other => panic!("unexpected element {other:?}"),
                })
            }
        };
        let with_null = ints(&[Some(1), None]);
        let plain = ints(&[Some(1), Some(2)]);
        let empty = ints(&[]);

        // ANY: a match wins over the NULL element; a miss is unknown, not false.
        let any_cases: [(&Datum, i32, Datum); 4] = [
            (&with_null, 1, Datum::Bool(true)),
            (&with_null, 9, Datum::Null),
            (&plain, 9, Datum::Bool(false)),
            (&plain, 2, Datum::Bool(true)),
        ];
        for (array, needle, expected) in any_cases {
            assert!(eval_quantified(array, Quantifier::Any, eq(needle)).expect("any") == expected);
        }

        // ALL: a definite false wins over the NULL; otherwise unknown.
        let all_cases: [(&Datum, i32, Datum); 3] = [
            (&with_null, 9, Datum::Bool(false)),
            (&with_null, 1, Datum::Null),
            (&plain, 1, Datum::Bool(false)),
        ];
        for (array, needle, expected) in all_cases {
            assert!(eval_quantified(array, Quantifier::All, eq(needle)).expect("all") == expected);
        }

        // The empty array short-circuits both quantifiers without comparing.
        assert!(
            eval_quantified(&empty, Quantifier::Any, eq(1)).expect("any") == Datum::Bool(false)
        );
        assert!(eval_quantified(&empty, Quantifier::All, eq(1)).expect("all") == Datum::Bool(true));
        // A NULL array is unknown for both.
        assert!(eval_quantified(&Datum::Null, Quantifier::Any, eq(1)).expect("any") == Datum::Null);
        assert!(eval_quantified(&Datum::Null, Quantifier::All, eq(1)).expect("all") == Datum::Null);
    }

    #[test]
    fn to_string_and_from_string_cover_the_null_forms() {
        let with_null = || array_expr("{1,NULL,3}", ElemType::Int4);
        assert!(
            call(
                "array_to_string",
                vec![array_expr("{1,2,3}", ElemType::Int4), text_expr(",")]
            )
            .expect("join")
                == Datum::Text("1,2,3".into())
        );
        // NULL elements are dropped without a null string, rendered with one.
        assert!(
            call("array_to_string", vec![with_null(), text_expr(",")]).expect("join")
                == Datum::Text("1,3".into())
        );
        assert!(
            call(
                "array_to_string",
                vec![with_null(), text_expr(","), text_expr("NIL")]
            )
            .expect("join")
                == Datum::Text("1,NIL,3".into())
        );
        assert!(
            call(
                "array_to_string",
                vec![null_array_expr(ElemType::Int4), text_expr(",")]
            )
            .expect("join")
                == Datum::Null
        );

        assert!(
            call("string_to_array", vec![text_expr("a,b,c"), text_expr(",")]).expect("split")
                == texts(&[Some("a"), Some("b"), Some("c")])
        );
        // A NULL delimiter splits into characters; an empty one keeps the string whole.
        assert!(
            call("string_to_array", vec![text_expr("abc"), Expr::NullLiteral]).expect("split")
                == texts(&[Some("a"), Some("b"), Some("c")])
        );
        assert!(
            call("string_to_array", vec![text_expr("abc"), text_expr("")]).expect("split")
                == texts(&[Some("abc")])
        );
        assert!(
            call("string_to_array", vec![text_expr(""), text_expr(",")]).expect("split")
                == texts(&[])
        );
        assert!(
            call("string_to_array", vec![Expr::NullLiteral, text_expr(",")]).expect("split")
                == Datum::Null
        );
        assert!(
            call(
                "string_to_array",
                vec![text_expr("a,NIL,c"), text_expr(","), text_expr("NIL")]
            )
            .expect("split")
                == texts(&[Some("a"), None, Some("c")])
        );
    }

    #[test]
    fn result_types_for_row_description() {
        let int_array = || array_expr("{1}", ElemType::Int4);
        let cases: [(&str, Vec<Expr>, ColumnType); 7] = [
            (
                "array_length",
                vec![int_array(), int_expr(1)],
                ColumnType::Int4,
            ),
            ("cardinality", vec![int_array()], ColumnType::Int4),
            (
                "array_append",
                vec![int_array(), int_expr(2)],
                ColumnType::Array(ElemType::Int4),
            ),
            (
                "array_prepend",
                vec![int_expr(2), int_array()],
                ColumnType::Array(ElemType::Int4),
            ),
            (
                "array_cat",
                vec![int_array(), int_array()],
                ColumnType::Array(ElemType::Int4),
            ),
            (
                "array_to_string",
                vec![int_array(), text_expr(",")],
                ColumnType::Text,
            ),
            (
                "string_to_array",
                vec![text_expr("a"), text_expr(",")],
                ColumnType::Array(ElemType::Text),
            ),
        ];
        for (name, args, expected) in cases {
            assert!(result_type(name, args).expect(name) == expected);
        }
        // Arity, non-array arguments and mixed element types are 42883 at plan time.
        assert!(
            sqlstate(
                result_type("cardinality", vec![int_array(), int_expr(1)]).expect_err("arity")
            ) == "42883"
        );
        // A TYPED non-array argument is 42883; an `unknown` literal is instead
        // 42804 (see `an_unknown_literal_array_argument_that_resolves_to_nothing_is_rejected`).
        assert!(
            sqlstate(result_type("cardinality", vec![int_expr(1)]).expect_err("type")) == "42883"
        );
        assert!(
            sqlstate(
                result_type(
                    "array_cat",
                    vec![int_array(), array_expr("{a}", ElemType::Text)]
                )
                .expect_err("mixed")
            ) == "42883"
        );
    }
    /// A `Datum` array built from a literal, so the dimension tests state their
    /// input the way `PostgreSQL` spells it.
    fn arr(literal: &str, elem: ElemType) -> Datum {
        crate::eval::eval(&array_expr(literal, elem), &Scope::empty(), &[], &ctx())
            .expect("array literal")
    }

    fn int_arr(literal: &str) -> Datum {
        arr(literal, ElemType::Int4)
    }

    fn idx(i: i32) -> SubscriptArg {
        SubscriptArg::Index(Datum::Int4(i))
    }

    fn slice(lower: Option<i32>, upper: Option<i32>) -> SubscriptArg {
        SubscriptArg::Slice {
            lower: lower.map(Datum::Int4),
            upper: upper.map(Datum::Int4),
        }
    }

    /// Every row is the value `PostgreSQL` 18.4 returns for the same reference.
    #[test]
    fn array_references_match_postgres_over_dimensions_and_bounds() {
        let cases: &[(&str, Vec<SubscriptArg>, &str)] = &[
            // A chain of plain subscripts reaches an element only when it is as
            // long as the array has dimensions.
            ("{1,2,3}", vec![idx(2)], "2"),
            ("{1,2,3}", vec![idx(0)], "NULL"),
            ("{1,2,3}", vec![idx(4)], "NULL"),
            ("{{1,2,3},{4,5,6}}", vec![idx(2)], "NULL"),
            ("{{1,2,3},{4,5,6}}", vec![idx(2), idx(3)], "6"),
            ("{{1,2,3},{4,5,6}}", vec![idx(1), idx(1), idx(1)], "NULL"),
            ("[2:4]={1,2,3}", vec![idx(1)], "NULL"),
            ("[2:4]={1,2,3}", vec![idx(2)], "1"),
            ("[2:4]={1,2,3}", vec![idx(4)], "3"),
            // Slices clip to the array, renumber from 1, and never error.
            ("{1,2,3,4,5}", vec![slice(Some(2), Some(4))], "{2,3,4}"),
            ("{1,2,3,4,5}", vec![slice(None, Some(2))], "{1,2}"),
            ("{1,2,3,4,5}", vec![slice(Some(3), None)], "{3,4,5}"),
            ("{1,2,3,4,5}", vec![slice(None, None)], "{1,2,3,4,5}"),
            ("{1,2,3,4,5}", vec![slice(Some(0), Some(2))], "{1,2}"),
            ("{1,2,3,4,5}", vec![slice(Some(4), Some(2))], "{}"),
            ("{1,2,3}", vec![slice(Some(10), Some(12))], "{}"),
            (
                "[5:9]={1,2,3,4,5}",
                vec![slice(Some(6), Some(8))],
                "{2,3,4}",
            ),
            // Missing trailing dimensions of a slice chain are whole slices.
            (
                "{{1,2,3},{4,5,6}}",
                vec![slice(Some(2), Some(2))],
                "{{4,5,6}}",
            ),
            (
                "{{1,2,3},{4,5,6},{7,8,9}}",
                vec![slice(Some(2), Some(3)), slice(Some(1), Some(2))],
                "{{4,5},{7,8}}",
            ),
            // A plain subscript inside a slice chain means `1:i`.
            (
                "{{1,2,3},{4,5,6}}",
                vec![slice(Some(1), Some(2)), idx(2)],
                "{{1,2},{4,5}}",
            ),
        ];
        for (literal, subscripts, expected) in cases {
            let got = array_ref(&int_arr(literal), subscripts).expect("reference");
            let text = match &got {
                Datum::Null => "NULL".to_string(),
                other => String::from_utf8(crabka_pgtypes::encoding::encode_text(
                    other,
                    &ctx().time_zone,
                ))
                .expect("utf8"),
            };
            assert!(text == *expected, "{literal} {subscripts:?}");
        }
    }

    #[test]
    fn a_null_subscript_reads_as_null_and_a_non_array_base_is_rejected() {
        let null_index = vec![SubscriptArg::Index(Datum::Null), idx(1)];
        assert!(array_ref(&int_arr("{{1,2}}"), &null_index).expect("null") == Datum::Null);
        assert!(array_ref(&Datum::Null, &[idx(1), idx(2)]).expect("null base") == Datum::Null);
        assert!(
            sqlstate(array_ref(&Datum::Int4(1), &[idx(1), idx(2)]).expect_err("not an array"))
                == "42804"
        );
    }

    /// `PostgreSQL` 18.4 values for `UPDATE t SET a[…] = v` over each starting
    /// array, including the extension and NULL-filling rules.
    #[test]
    fn subscripted_assignment_matches_postgres() {
        let cases: &[(Option<&str>, Vec<SubscriptArg>, Datum, &str)] = &[
            (Some("{1,2,3}"), vec![idx(2)], Datum::Int4(99), "{1,99,3}"),
            (
                Some("{1,2,3}"),
                vec![idx(6)],
                Datum::Int4(7),
                "{1,2,3,NULL,NULL,7}",
            ),
            (
                Some("{1,2,3}"),
                vec![idx(0)],
                Datum::Int4(-1),
                "[0:3]={-1,1,2,3}",
            ),
            (None, vec![idx(3)], Datum::Int4(5), "[3:3]={5}"),
            (None, vec![idx(2)], Datum::Null, "[2:2]={NULL}"),
            (
                Some("{1,2,3}"),
                vec![slice(Some(2), Some(3))],
                int_arr("{50,60}"),
                "{1,50,60}",
            ),
            (
                None,
                vec![slice(Some(2), Some(3))],
                int_arr("{1,2}"),
                "[2:3]={1,2}",
            ),
            (
                Some("{{1,2},{3,4}}"),
                vec![idx(1), idx(2)],
                Datum::Int4(9),
                "{{1,9},{3,4}}",
            ),
            (
                Some("{{1,2},{3,4}}"),
                vec![slice(Some(1), Some(2)), slice(Some(1), Some(1))],
                int_arr("{{7},{8}}"),
                "{{7,2},{8,4}}",
            ),
        ];
        for (start, subscripts, value, expected) in cases {
            let current = start.map_or(Datum::Null, int_arr);
            let got = array_assign(&current, subscripts, value, ElemType::Int4, &ctx())
                .expect("assignment");
            let text = String::from_utf8(crabka_pgtypes::encoding::encode_text(
                &got,
                &ctx().time_zone,
            ))
            .expect("utf8");
            assert!(text == *expected, "{start:?} {subscripts:?}");
        }
    }

    #[test]
    fn assignment_reports_postgres_sqlstates_for_every_refusal() {
        let cases: &[(Option<&str>, Vec<SubscriptArg>, Datum, &str)] = &[
            // A NULL subscript is an error in an assignment, unlike in a read.
            (
                Some("{1,2,3}"),
                vec![SubscriptArg::Index(Datum::Null)],
                Datum::Int4(1),
                "22004",
            ),
            // A multidimensional target neither extends nor accepts the wrong
            // number of subscripts.
            (
                Some("{{1,2},{3,4}}"),
                vec![idx(3), idx(1)],
                Datum::Int4(9),
                "2202E",
            ),
            (Some("{{1,2},{3,4}}"), vec![idx(2)], Datum::Int4(5), "2202E"),
            // A slice source shorter than the slice.
            (
                Some("{1,2,3}"),
                vec![slice(Some(2), Some(3))],
                int_arr("{1}"),
                "2202E",
            ),
            // An array larger than PostgreSQL will allocate.
            (
                Some("{1,2,3}"),
                vec![idx(2_147_483_647)],
                Datum::Int4(42),
                "54000",
            ),
        ];
        for (start, subscripts, value, code) in cases {
            let current = start.map_or(Datum::Null, int_arr);
            let error = array_assign(&current, subscripts, value, ElemType::Int4, &ctx())
                .expect_err("refused");
            assert!(sqlstate(error) == *code, "{start:?} {subscripts:?}");
        }
    }

    /// The dimension-reporting functions over the shapes that distinguish them.
    #[test]
    fn dimension_functions_match_postgres() {
        let cases: &[(&str, Vec<Expr>, Datum)] = &[
            (
                "array_ndims",
                vec![array_expr("{1,2}", ElemType::Int4)],
                Datum::Int4(1),
            ),
            (
                "array_ndims",
                vec![array_expr("{{1,2},{3,4}}", ElemType::Int4)],
                Datum::Int4(2),
            ),
            (
                "array_ndims",
                vec![array_expr("{}", ElemType::Int4)],
                Datum::Null,
            ),
            (
                "array_dims",
                vec![array_expr("{{1,2},{3,4}}", ElemType::Int4)],
                Datum::Text("[1:2][1:2]".into()),
            ),
            (
                "array_dims",
                vec![array_expr("[2:4]={1,2,3}", ElemType::Int4)],
                Datum::Text("[2:4]".into()),
            ),
            (
                "array_dims",
                vec![array_expr("{}", ElemType::Int4)],
                Datum::Null,
            ),
            (
                "array_length",
                vec![array_expr("{{1,2,3},{4,5,6}}", ElemType::Int4), int_expr(2)],
                Datum::Int4(3),
            ),
            (
                "array_length",
                vec![array_expr("{{1,2,3},{4,5,6}}", ElemType::Int4), int_expr(3)],
                Datum::Null,
            ),
            (
                "array_length",
                vec![array_expr("{1,2}", ElemType::Int4), int_expr(0)],
                Datum::Null,
            ),
            (
                "array_lower",
                vec![array_expr("[2:4]={1,2,3}", ElemType::Int4), int_expr(1)],
                Datum::Int4(2),
            ),
            (
                "array_upper",
                vec![array_expr("[2:4]={1,2,3}", ElemType::Int4), int_expr(1)],
                Datum::Int4(4),
            ),
            (
                "cardinality",
                vec![array_expr("{{1,2},{3,4}}", ElemType::Int4)],
                Datum::Int4(4),
            ),
        ];
        for (name, args, expected) in cases {
            assert!(
                call(name, args.clone()).expect(name) == *expected,
                "{name} {args:?}"
            );
        }
    }

    /// The search, reshaping and generating functions. Values are `PostgreSQL`
    /// 18.4's.
    #[test]
    fn the_remaining_array_functions_match_postgres() {
        let ints = |literal: &str| array_expr(literal, ElemType::Int4);
        let cases: &[(&str, Vec<Expr>, &str)] = &[
            (
                "array_fill",
                vec![int_expr(7), ints("{2,2}")],
                "{{7,7},{7,7}}",
            ),
            (
                "array_fill",
                vec![int_expr(7), ints("{3}"), ints("{2}")],
                "[2:4]={7,7,7}",
            ),
            ("array_fill", vec![int_expr(1), ints("{0}")], "{}"),
            (
                "array_position",
                vec![ints("{1,2,3,4,5}"), int_expr(4)],
                "4",
            ),
            ("array_position", vec![ints("{1,2,3}"), int_expr(9)], "NULL"),
            (
                "array_position",
                vec![ints("{1,2,3,2}"), int_expr(2), int_expr(3)],
                "4",
            ),
            (
                "array_positions",
                vec![ints("{1,2,3,2}"), int_expr(2)],
                "{2,4}",
            ),
            ("array_positions", vec![ints("{1,2}"), int_expr(9)], "{}"),
            (
                "array_remove",
                vec![ints("{1,2,2,3}"), int_expr(2)],
                "{1,3}",
            ),
            (
                "array_replace",
                vec![ints("{1,2,5,4}"), int_expr(5), int_expr(3)],
                "{1,2,3,4}",
            ),
            (
                "array_replace",
                vec![ints("{{1,2},{2,3}}"), int_expr(2), int_expr(9)],
                "{{1,9},{9,3}}",
            ),
            ("trim_array", vec![ints("{1,2,3}"), int_expr(2)], "{1}"),
            ("trim_array", vec![ints("{1,2,3}"), int_expr(0)], "{1,2,3}"),
            ("trim_array", vec![ints("{1,2,3}"), int_expr(3)], "{}"),
            ("array_sort", vec![ints("{3,1,2}")], "{1,2,3}"),
            ("array_sort", vec![ints("{{3,4},{1,2}}")], "{{1,2},{3,4}}"),
            ("array_reverse", vec![ints("{1,2,3}")], "{3,2,1}"),
            (
                "array_reverse",
                vec![ints("{{1,2},{3,4}}")],
                "{{3,4},{1,2}}",
            ),
        ];
        for (name, args, expected) in cases {
            let got = call(name, args.clone()).expect(name);
            let text = match &got {
                Datum::Null => "NULL".to_string(),
                other => String::from_utf8(crabka_pgtypes::encoding::encode_text(
                    other,
                    &ctx().time_zone,
                ))
                .expect("utf8"),
            };
            assert!(text == *expected, "{name} {args:?}");
        }
    }

    #[test]
    fn the_remaining_array_functions_report_postgres_sqlstates() {
        let ints = |literal: &str| array_expr(literal, ElemType::Int4);
        let cases: &[(&str, Vec<Expr>, &str)] = &[
            (
                "array_fill",
                vec![int_expr(1), null_array_expr(ElemType::Int4)],
                "22004",
            ),
            (
                "array_fill",
                vec![int_expr(1), ints("{2,2}"), ints("{1}")],
                "2202E",
            ),
            ("trim_array", vec![ints("{1,2,3}"), int_expr(-1)], "2202E"),
            ("trim_array", vec![ints("{1,2,3}"), int_expr(4)], "2202E"),
            ("array_sample", vec![ints("{1,2,3}"), int_expr(-1)], "22023"),
            ("array_sample", vec![ints("{1,2,3}"), int_expr(5)], "22023"),
            (
                "array_position",
                vec![ints("{{1,2},{3,4}}"), int_expr(3)],
                "0A000",
            ),
            (
                "array_remove",
                vec![ints("{{1,2},{3,4}}"), int_expr(3)],
                "0A000",
            ),
            (
                "array_append",
                vec![ints("{{1,2},{3,4}}"), int_expr(5)],
                "22000",
            ),
            ("array_prepend", vec![int_expr(1), ints("{{2,3}}")], "22000"),
        ];
        for (name, args, code) in cases {
            let error = call(name, args.clone()).expect_err(name);
            assert!(sqlstate(error) == *code, "{name} {args:?}");
        }
    }

    /// `array_cat` and `||` join along the OUTERMOST dimension, so the operand
    /// dimensionalities may differ by one.
    #[test]
    fn concatenation_joins_the_outermost_dimension() {
        let cases: &[(&str, &str, &str)] = &[
            ("{{1,2},{3,4}}", "{{5,6}}", "{{1,2},{3,4},{5,6}}"),
            ("{1,2}", "{{5,6}}", "{{1,2},{5,6}}"),
            ("{{5,6}}", "{1,2}", "{{5,6},{1,2}}"),
            ("{}", "{1}", "{1}"),
            ("{1}", "{}", "{1}"),
        ];
        for (left, right, expected) in cases {
            let got = array_cat(&int_arr(left), &int_arr(right)).expect("cat");
            let text = String::from_utf8(crabka_pgtypes::encoding::encode_text(
                &got,
                &ctx().time_zone,
            ))
            .expect("utf8");
            assert!(text == *expected, "{left} || {right}");
        }
        // Inner dimensions (bounds included) must agree.
        assert!(
            sqlstate(
                array_cat(&int_arr("{{1,2}}"), &int_arr("{{1,2,3}}")).expect_err("incompatible")
            ) == "2202E"
        );
    }

    /// `ARRAY[…]` with nested constructors adds a dimension; a ragged or mixed
    /// list is PostgreSQL's 2202E.
    #[test]
    fn the_array_constructor_stacks_sub_arrays() {
        let built = build_constructor(ElemType::Int4, vec![int_arr("{1,2}"), int_arr("{3,4}")])
            .expect("constructor");
        assert!(built == int_arr("{{1,2},{3,4}}"));
        let flat = build_constructor(ElemType::Int4, vec![Datum::Int4(1), Datum::Int4(2)])
            .expect("constructor");
        assert!(flat == int_arr("{1,2}"));
        for items in [
            vec![int_arr("{1,2}"), int_arr("{3}")],
            vec![int_arr("{1,2}"), Datum::Int4(3)],
        ] {
            let error = build_constructor(ElemType::Int4, items).expect_err("ragged");
            assert!(sqlstate(error) == "2202E");
        }
    }

    #[test]
    fn oidvector_reuses_zero_based_array_semantics() {
        let value = Datum::OidVector(ArrayValue::with_dims(
            ElemType::Int4,
            vec![Datum::Int4(23), Datum::Int4(25)],
            vec![crabka_pgtypes::ArrayDim::new(0, 2)],
        ));
        assert!(
            dimension(array_value(&value, "array_length").expect("oidvector"), 1)
                == Some(crabka_pgtypes::ArrayDim::new(0, 2))
        );
        assert!(array_subscript(&value, &Datum::Int4(0)).expect("subscript") == Datum::Int4(23));
        assert!(
            eval_quantified(&value, Quantifier::Any, |item| {
                Ok(Datum::Bool(*item == Datum::Int4(25)))
            })
            .expect("any")
                == Datum::Bool(true)
        );
        assert!(
            crabka_pgtypes::encoding::encode_text(&value, &jiff::tz::TimeZone::UTC) == b"23 25"
        );
        assert!(
            crabka_pgtypes::ops::compare(&value, &value).expect("compare")
                == Some(std::cmp::Ordering::Equal)
        );
    }
}
