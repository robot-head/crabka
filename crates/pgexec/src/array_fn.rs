//! The array function family, the array operator semantics, and `ANY`/`ALL`
//! over an array.
//!
//! Mirrors the existing scalar families (`func.rs`, `datetime_fn.rs`,
//! `format_fn.rs`): an `array_func(name)` classifier, an `is_array_func`
//! dispatch predicate, an `array_func_result_type` static resolver for
//! RowDescription, and an `eval_array` value evaluator that takes the caller's
//! child-evaluation closure (so scalar `eval` and the grouped evaluator share
//! the math).
//!
//! The operator helpers (`||`, `@>`, `<@`, `&&`, subscripting, `= ANY(...)`)
//! live here too rather than in `eval.rs`, so all one-dimensional array
//! semantics — and their PostgreSQL corner cases — sit in one file. Everything
//! here is a pure, deterministic transform over a single row's already-resolved
//! `Datum`s, so it introduces no lock, visibility, or interleaving rule.

use crabka_pgparser::ast::{Expr, FuncArgs, FuncCall};
use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType, TypeError, cast};

use crate::{clock::EvalCtx, error::ExecError, eval::ArgType, scope::Scope};

/// The array functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArrayFunc {
    /// `array_length(anyarray, int)` — length of dimension `n` (only 1 exists).
    Length,
    /// `cardinality(anyarray)` — total element count (0, not NULL, when empty).
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
}

/// Classify a (lowercased — the lexer lowercases unquoted idents) function name.
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
        _ => return None,
    })
}

/// Is `name` an array function? (The dispatch point for the eval guard chains.)
pub(crate) fn is_array_func(name: &str) -> bool {
    array_func(name).is_some()
}

// ---- argument-type resolution ----

/// The type an `unknown` literal argument adopts, per position — the ONE place
/// the array family's parameter types are written down.
///
/// PostgreSQL leaves a bare `'…'` / `NULL` literal `unknown` and resolves it
/// against the parameter it is passed to. For the polymorphic pairs
/// (`array_append(anyarray, anyelement)` and friends) that means the side which
/// *does* carry a type resolves both, so `array_append('{1,2}', 3)` is
/// `int4[]`; when neither side carries one, PostgreSQL falls back to `text`, so
/// `array_cat('{1,2}', '{3}')` is `text[]`. For `array_length`, `cardinality`
/// and `array_to_string` nothing can resolve the `anyarray` parameter, so an
/// `unknown` literal there is 42804 rather than a guess.
///
/// Both [`array_func_result_type`] (plan time, over statically inferred
/// argument types) and [`eval_array`] (run time, over the evaluated values'
/// types) drive this one rule, so a literal is typed and converted by the same
/// decision.
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
    })
}

/// The element type an `anyarray`/`anyelement` pair resolves to: from the array
/// side when it carries one, else from the element side, else `text` (which is
/// what PostgreSQL settles on when every polymorphic input is `unknown`).
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
    })
}

/// The element type of an array-typed argument (42883 when it is not an array).
fn require_array_type(fc: &FuncCall, t: ColumnType) -> Result<ElemType, ExecError> {
    t.array_element()
        .ok_or_else(|| undefined_function(&fc.name))
}

// ---- evaluation ----

/// Evaluate an array function call.
///
/// `array_length`/`cardinality`/`array_to_string`/`string_to_array` are STRICT
/// (any NULL argument yields NULL). `array_append`/`array_prepend`/`array_cat`
/// are deliberately **not** strict, matching PostgreSQL: a NULL array behaves
/// like an empty array of the other operand's type, and a NULL element appends
/// a NULL element.
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
    let given = crate::eval::value_arg_types(args, &vals);
    crate::eval::coerce_unknown_args(args, &mut vals, &param_types(f, &given)?, ctx)?;
    let n = vals.len();
    match f {
        ArrayFunc::Length => {
            require_arity(fc, n == 2)?;
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Datum::Null);
            }
            let array = array_value(&vals[0], &fc.name)?;
            let dim = int_arg(&vals[1], &fc.name)?;
            // Only one dimension exists, and an empty array has none at all.
            if dim != 1 || array.elems.is_empty() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Int4(element_count(array)?))
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
            array_append(&vals[0], &vals[1], ctx)
        }
        ArrayFunc::Prepend => {
            require_arity(fc, n == 2)?;
            array_prepend(&vals[0], &vals[1], ctx)
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
    }
}

/// `cardinality`/`array_length` report an `int4`; a longer array is 22003.
fn element_count(array: &ArrayValue) -> Result<i32, ExecError> {
    i32::try_from(array.elems.len()).map_err(|_| ExecError::Type(TypeError::Overflow))
}

/// `array_to_string`: render each element with its own output function, joining
/// with `sep`. NULL elements are omitted unless `null_text` is supplied.
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
/// splits into single characters; an empty separator yields the whole string as
/// one element; an empty input yields an empty array. Elements equal to
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

/// `array_append(anyarray, anyelement)`: NULL array + non-NULL element yields a
/// one-element array of that element's type (PostgreSQL's non-strict behavior);
/// NULL on both sides is NULL, since no element type can be derived.
pub(crate) fn array_append(array: &Datum, elem: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    match array {
        Datum::Array(a) => {
            let mut elems = a.elems.clone();
            elems.push(coerce_element(elem, a.elem, ctx)?);
            Ok(Datum::Array(ArrayValue::new(a.elem, elems)))
        }
        Datum::Null => Ok(singleton_from_element(elem)),
        other => Err(not_an_array(other)),
    }
}

/// `array_prepend(anyelement, anyarray)` — the mirror of [`array_append`].
pub(crate) fn array_prepend(
    elem: &Datum,
    array: &Datum,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    match array {
        Datum::Array(a) => {
            let mut elems = Vec::with_capacity(a.elems.len() + 1);
            elems.push(coerce_element(elem, a.elem, ctx)?);
            elems.extend(a.elems.iter().cloned());
            Ok(Datum::Array(ArrayValue::new(a.elem, elems)))
        }
        Datum::Null => Ok(singleton_from_element(elem)),
        other => Err(not_an_array(other)),
    }
}

/// `array_cat(anyarray, anyarray)`: a NULL array is treated as the empty array
/// of the other side's type (PostgreSQL's non-strict `array_cat`).
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
            let mut elems = a.elems.clone();
            elems.extend(b.elems.iter().cloned());
            Ok(Datum::Array(ArrayValue::new(a.elem, elems)))
        }
        _ => Err(operator_undefined("||", left, right)),
    }
}

/// Which of PostgreSQL's three `||` array operators a call resolves to.
///
/// The choice is made from the operands' **static** types, exactly as
/// PostgreSQL's operator resolution does — it cannot be made from the runtime
/// values, because `int[] || NULL::int[]` (concatenation, `{1,2}`) and
/// `int[] || NULL::int` (append, `{1,2,NULL}`) are indistinguishable once both
/// right-hand sides have evaluated to SQL NULL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConcatForm {
    /// `anyarray || anyarray`.
    ArrayArray,
    /// `anyarray || anyelement`.
    ArrayElement,
    /// `anyelement || anyarray`.
    ElementArray,
}

/// Resolve `left || right` from the operand types, or `None` when no array `||`
/// applies (the caller falls through to the text/jsonb `||` or reports 42883).
///
/// A bare `NULL` literal types as `text` here, so a caller with the syntactic
/// expression in hand should resolve `x || NULL` to [`ConcatForm::ArrayArray`]
/// — that is what PostgreSQL's `unknown` resolution picks, and it makes
/// `ARRAY[1,2] || NULL` yield `{1,2}` rather than `{1,2,NULL}`.
pub(crate) fn concat_form(left: ColumnType, right: ColumnType) -> Option<ConcatForm> {
    match (left.array_element(), right.array_element()) {
        (Some(a), Some(b)) if a == b => Some(ConcatForm::ArrayArray),
        (Some(_), Some(_)) => None,
        (Some(a), None) if ElemType::from_column_type(right) == Some(a) => {
            Some(ConcatForm::ArrayElement)
        }
        (None, Some(b)) if ElemType::from_column_type(left) == Some(b) => {
            Some(ConcatForm::ElementArray)
        }
        _ => None,
    }
}

/// The static result type of an array `||`.
pub(crate) fn concat_result_type(left: ColumnType, right: ColumnType) -> Option<ColumnType> {
    match concat_form(left, right)? {
        ConcatForm::ArrayArray | ConcatForm::ArrayElement => Some(left),
        ConcatForm::ElementArray => Some(right),
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
        ConcatForm::ArrayElement => array_append(left, right, ctx),
        ConcatForm::ElementArray => array_prepend(left, right, ctx),
    }
}

/// `left @> right`: every element of `right` appears in `left`.
///
/// PostgreSQL's `array_contain_compare` assumes a strict equality operator, so a
/// NULL element on the *contained* side can never be matched and makes the whole
/// test false; a NULL element on the containing side is simply never a match.
pub(crate) fn array_contains(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let (Some(l), Some(r)) = (array_or_null(left, "@>")?, array_or_null(right, "@>")?) else {
        return Ok(Datum::Null);
    };
    require_same_element(l, r, "@>", left, right)?;
    Ok(Datum::Bool(r.elems.iter().all(|needle| {
        !needle.is_null() && l.elems.iter().any(|e| e == needle)
    })))
}

/// `left <@ right` — [`array_contains`] with the operands swapped.
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
/// overlap (they are skipped, not falsified).
pub(crate) fn array_overlap(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let (Some(l), Some(r)) = (array_or_null(left, "&&")?, array_or_null(right, "&&")?) else {
        return Ok(Datum::Null);
    };
    require_same_element(l, r, "&&", left, right)?;
    Ok(Datum::Bool(l.elems.iter().any(|a| {
        !a.is_null() && r.elems.iter().any(|b| !b.is_null() && a == b)
    })))
}

/// `array[index]`: 1-based subscripting. A NULL array or NULL index yields NULL,
/// and — unlike most languages — an out-of-range subscript is NULL, not an
/// error.
pub(crate) fn array_subscript(base: &Datum, index: &Datum) -> Result<Datum, ExecError> {
    let array = match base {
        Datum::Null => return Ok(Datum::Null),
        Datum::Array(a) => a,
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
    if i < 1 {
        return Ok(Datum::Null);
    }
    let idx = usize::try_from(i - 1).map_err(|_| ExecError::Type(TypeError::Overflow))?;
    Ok(array.elems.get(idx).cloned().unwrap_or(Datum::Null))
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
/// `compare` applies the operator to one element and returns `Bool` or NULL (the
/// caller passes the ordinary binary-operator evaluator, so every operator gets
/// the same quantified semantics).
///
/// - `ANY`: true as soon as one element compares true; otherwise NULL if any
///   comparison was NULL (a false-with-NULL result is **unknown**, not false);
///   otherwise false. An empty array is false — even for a NULL left operand.
/// - `ALL`: false as soon as one element compares false; otherwise NULL if any
///   comparison was NULL; otherwise true. An empty array is true.
/// - A NULL array is NULL for both.
pub(crate) fn eval_quantified(
    array: &Datum,
    quantifier: Quantifier,
    mut compare: impl FnMut(&Datum) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let array = match array {
        Datum::Null => return Ok(Datum::Null),
        Datum::Array(a) => a,
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
        Datum::Array(a) => Ok(a),
        _ => Err(undefined_function(name)),
    }
}

/// An array operand of a binary operator: `Ok(None)` for SQL NULL (the operator
/// is strict), an error for a non-array operand.
fn array_or_null<'a>(d: &'a Datum, op: &str) -> Result<Option<&'a ArrayValue>, ExecError> {
    match d {
        Datum::Null => Ok(None),
        Datum::Array(a) => Ok(Some(a)),
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

/// A single-element array typed from the element itself — the `array_append`
/// path where the array operand is NULL. Two NULLs carry no type at all, so the
/// result is NULL.
fn singleton_from_element(elem: &Datum) -> Datum {
    match elem.column_type().and_then(ElemType::from_column_type) {
        Some(t) => Datum::Array(ArrayValue::new(t, vec![elem.clone()])),
        None => Datum::Null,
    }
}

/// Coerce an element into an array's element type (`array_append(bigint[], 1)`
/// stores an `int8`). An undefined conversion is 42804.
fn coerce_element(elem: &Datum, to: ElemType, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    if elem.is_null() || elem.column_type() == Some(to.column_type()) {
        return Ok(elem.clone());
    }
    cast::cast(elem, to.column_type(), &ctx.time_zone).map_err(|_| {
        ExecError::TypeMismatch(format!(
            "cannot store a value of type {} in an array of {}",
            type_name(elem),
            to.name()
        ))
    })
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
    /// `PostgreSQL`'s 42804 — not a guess, and not the 42883 a plain
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
        assert!(
            array_append(&ints(&[Some(1)]), &Datum::Int4(2), &ctx).expect("append")
                == ints(&[Some(1), Some(2)])
        );
        // A NULL element is stored as a NULL element, not dropped.
        assert!(
            array_append(&ints(&[Some(1)]), &Datum::Null, &ctx).expect("append")
                == ints(&[Some(1), None])
        );
        // A NULL array behaves like an empty array of the element's type.
        assert!(
            array_append(&Datum::Null, &Datum::Int4(7), &ctx).expect("append") == ints(&[Some(7)])
        );
        // Two NULLs carry no element type, so there is no array to build.
        assert!(array_append(&Datum::Null, &Datum::Null, &ctx).expect("append") == Datum::Null);
        assert!(
            array_prepend(&Datum::Int4(0), &ints(&[Some(1)]), &ctx).expect("prepend")
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

    #[test]
    fn concat_resolves_its_form_from_the_operand_types() {
        let int_array = ColumnType::Array(ElemType::Int4);
        let text_array = ColumnType::Array(ElemType::Text);
        assert!(concat_form(int_array, int_array) == Some(ConcatForm::ArrayArray));
        assert!(concat_form(int_array, ColumnType::Int4) == Some(ConcatForm::ArrayElement));
        assert!(concat_form(ColumnType::Int4, int_array) == Some(ConcatForm::ElementArray));
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
                ConcatForm::ArrayElement,
                &ints(&[Some(1)]),
                &Datum::Int4(2),
                &ctx
            )
            .expect("append")
                == ints(&[Some(1), Some(2)])
        );
        assert!(
            array_concat(
                ConcatForm::ElementArray,
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
                ConcatForm::ArrayElement,
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
            array_concat(ConcatForm::ArrayElement, &big, &Datum::Int4(2), &ctx).expect("append")
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
}
