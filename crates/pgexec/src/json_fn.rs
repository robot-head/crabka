//! The `jsonb` function family and the semantics of every `jsonb` operator.
//!
//! This module follows the existing scalar families `func.rs`, `datetime_fn.rs`
//! and `format_fn.rs`. It holds a `json_func(name)` classifier, an
//! `is_json_func` dispatch predicate, a `json_func_result_type` static resolver
//! for RowDescription, and an `eval_json` value evaluator that takes the
//! caller's child-evaluation closure. So scalar `eval` and the grouped evaluator
//! share the math.
//!
//! The operator semantics for `->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`,
//! `?&`, `||` and `-` live here as well. [`JsonOp`] and [`eval_json_operator`]
//! expose them, and each operator is also exposed on its own, so the eval layer
//! only has to map its `BinaryOp` variants onto [`JsonOp`]. Keeping them beside
//! the functions puts every PostgreSQL corner case in one file: jsonb null
//! against SQL NULL, the raw-scalar containment exception, and right-wins object
//! merge.
//!
//! Everything here is a pure, deterministic transform over a single row's
//! already-resolved `Datum`s, so it introduces no lock, visibility, or
//! interleaving rule.

use std::borrow::Cow;

use bigdecimal::BigDecimal;
use crabka_pgparser::ast::{Expr, FuncArgs, FuncCall, SqlJsonExpr};
use crabka_pgtypes::{
    ArrayValue, ColumnType, Datum, ElemType, JsonbValue, TypeError, jsonb, numeric,
};

use crate::{clock::EvalCtx, error::ExecError, eval::ArgType, scope::Scope};

/// The `jsonb` functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonFunc {
    /// `jsonb_build_object(VARIADIC k, v, …)`: an even-length key/value list.
    BuildObject,
    /// `jsonb_build_array(VARIADIC …)`.
    BuildArray,
    /// `jsonb_array_length(jsonb)`.
    ArrayLength,
    /// `jsonb_typeof(jsonb)`.
    Typeof,
    /// `jsonb_extract_path(jsonb, VARIADIC path text)`: the `#>` function form.
    ExtractPath,
    /// `jsonb_extract_path_text(jsonb, VARIADIC path text)`: the `#>>` form.
    ExtractPathText,
    /// `jsonb_set(target, path, new_value [, create_if_missing])`.
    Set,
    /// `to_jsonb(anyelement)`.
    ToJsonb,
    /// `row_to_json(record [, pretty])`.
    RowToJson,
    /// `jsonb_strip_nulls(jsonb)` — drop every object field whose value is the
    /// JSON `null` literal, recursively. Array nulls are kept.
    StripNulls,
    /// `jsonb_pretty(jsonb)`: the indented rendering, as `text`.
    Pretty,
    /// `jsonb_insert(target, path, new_value [, insert_after])`.
    Insert,
    /// `jsonb_delete_path(target, path)`: the `#-` operator's function form.
    DeletePath,
    /// `jsonb_set_lax(target, path, new_value [, create_if_missing [, null_value_treatment]])`.
    SetLax,
    /// `json_object(text[])` / `json_object(text[], text[])`: an object built
    /// from a flat or two-column key/value array. Every value is a JSON string.
    Object,
    /// `jsonb_path_exists(target, path [, vars [, silent]])`: the `@?` function form.
    PathExists,
    /// `jsonb_path_match(target, path [, vars [, silent]])`: the `@@` function form.
    PathMatch,
    /// `jsonb_path_query_array(target, path [, vars [, silent]])`.
    PathQueryArray,
    /// `jsonb_path_query_first(target, path [, vars [, silent]])`.
    PathQueryFirst,
    /// The function spellings of the operators, which `\df` and the regress
    /// corpus both use: `jsonb_contains`, `jsonb_contained`, `jsonb_exists`,
    /// `jsonb_exists_any`, `jsonb_exists_all`, `jsonb_delete` and
    /// `jsonb_concat`.
    Operator(JsonOp),
}

/// Classify a lowercased function name. The lexer lowercases unquoted idents.
/// `None` means "not a jsonb function".
///
/// The `json_*` spellings resolve to the same implementations as their `jsonb_*`
/// counterparts, because crabka stores `json` as `jsonb`. See the compatibility
/// matrix row. The `_tz` jsonpath variants also share their implementation:
/// crabka's jsonpath datetime items are rendered strings, so no comparison in
/// them depends on the session time zone.
fn json_func(name: &str) -> Option<JsonFunc> {
    Some(match name {
        "jsonb_build_object" | "json_build_object" => JsonFunc::BuildObject,
        "jsonb_build_array" | "json_build_array" => JsonFunc::BuildArray,
        "jsonb_array_length" | "json_array_length" => JsonFunc::ArrayLength,
        "jsonb_typeof" | "json_typeof" => JsonFunc::Typeof,
        "jsonb_extract_path" | "json_extract_path" => JsonFunc::ExtractPath,
        "jsonb_extract_path_text" | "json_extract_path_text" => JsonFunc::ExtractPathText,
        "jsonb_set" => JsonFunc::Set,
        "jsonb_set_lax" => JsonFunc::SetLax,
        "to_jsonb" | "to_json" => JsonFunc::ToJsonb,
        "row_to_json" => JsonFunc::RowToJson,
        "jsonb_strip_nulls" | "json_strip_nulls" => JsonFunc::StripNulls,
        "jsonb_pretty" => JsonFunc::Pretty,
        "jsonb_insert" => JsonFunc::Insert,
        "jsonb_delete_path" => JsonFunc::DeletePath,
        "jsonb_object" | "json_object" => JsonFunc::Object,
        "jsonb_path_exists" | "jsonb_path_exists_tz" => JsonFunc::PathExists,
        "jsonb_path_match" | "jsonb_path_match_tz" => JsonFunc::PathMatch,
        "jsonb_path_query_array" | "jsonb_path_query_array_tz" => JsonFunc::PathQueryArray,
        "jsonb_path_query_first" | "jsonb_path_query_first_tz" => JsonFunc::PathQueryFirst,
        "jsonb_contains" => JsonFunc::Operator(JsonOp::Contains),
        "jsonb_contained" => JsonFunc::Operator(JsonOp::ContainedBy),
        "jsonb_exists" => JsonFunc::Operator(JsonOp::KeyExists),
        "jsonb_exists_any" => JsonFunc::Operator(JsonOp::KeyExistsAny),
        "jsonb_exists_all" => JsonFunc::Operator(JsonOp::KeyExistsAll),
        "jsonb_delete" => JsonFunc::Operator(JsonOp::Delete),
        "jsonb_concat" => JsonFunc::Operator(JsonOp::Concat),
        _ => return None,
    })
}

/// Is `name` a jsonb function? This is the dispatch point for the eval guard
/// chains.
pub(crate) fn is_json_func(name: &str) -> bool {
    json_func(name).is_some()
}

// ---- argument-type resolution ----

/// The type an `unknown` literal argument adopts, per position. This is the ONE
/// place the jsonb family's parameter types are written down.
///
/// PostgreSQL leaves a bare `'…'` / `NULL` literal `unknown` and resolves it
/// against the parameter it is passed to, so `jsonb_set('{"a":1}', '{a}', '2')`
/// passes a `jsonb`, a `text[]` and a `jsonb`. `None` in the result means the
/// literal adopts nothing and stays `text`, which is PostgreSQL's own rule for a
/// `"any"` parameter. That is why `jsonb_build_object('a', '{"x":1}')` stores
/// the JSON *string* `"{\"x\":1}"` rather than a nested object.
///
/// Two callers drive this one rule: [`json_func_result_type`] at plan time, over
/// statically inferred argument types, and [`eval_json`] at run time, over the
/// evaluated values' types. So one decision types and converts a literal.
fn param_types(f: JsonFunc, given: &[ArgType]) -> Result<Vec<Option<ColumnType>>, ExecError> {
    let n = given.len();
    let jsonb = Some(ColumnType::Jsonb);
    Ok(match f {
        // `VARIADIC "any"`: an `unknown` literal resolves to `text`.
        JsonFunc::BuildObject | JsonFunc::BuildArray => vec![None; n],
        JsonFunc::ArrayLength | JsonFunc::Typeof => vec![jsonb],
        // `(jsonb, VARIADIC text[])`.
        JsonFunc::ExtractPath | JsonFunc::ExtractPathText => std::iter::once(jsonb)
            .chain(std::iter::repeat_n(
                Some(ColumnType::Text),
                n.saturating_sub(1),
            ))
            .collect(),
        JsonFunc::Set | JsonFunc::Insert => vec![
            jsonb,
            ColumnType::array_of(ColumnType::Text),
            jsonb,
            Some(ColumnType::Bool),
        ],
        JsonFunc::SetLax => vec![
            jsonb,
            ColumnType::array_of(ColumnType::Text),
            jsonb,
            Some(ColumnType::Bool),
            Some(ColumnType::Text),
        ],
        // PG18's `strip_nulls(jsonb, strip_in_arrays boolean)`.
        JsonFunc::StripNulls => vec![jsonb, Some(ColumnType::Bool)],
        JsonFunc::Pretty => vec![jsonb],
        JsonFunc::DeletePath => vec![jsonb, ColumnType::array_of(ColumnType::Text)],
        JsonFunc::Object => vec![ColumnType::array_of(ColumnType::Text); n.max(1)],
        // `(jsonb, jsonpath [, jsonb vars [, boolean silent]])`. crabka spells
        // `jsonpath` `text`, so the second parameter takes an `unknown` literal
        // as text and the path is compiled at evaluation.
        JsonFunc::PathExists
        | JsonFunc::PathMatch
        | JsonFunc::PathQueryArray
        | JsonFunc::PathQueryFirst => {
            vec![jsonb, Some(ColumnType::Text), jsonb, Some(ColumnType::Bool)]
        }
        JsonFunc::Operator(op) => match op {
            JsonOp::KeyExists => vec![jsonb, Some(ColumnType::Text)],
            JsonOp::KeyExistsAny | JsonOp::KeyExistsAll => {
                vec![jsonb, ColumnType::array_of(ColumnType::Text)]
            }
            JsonOp::Delete => vec![jsonb, None],
            _ => vec![jsonb, jsonb],
        },
        // `to_jsonb(anyelement)`: nothing else in the call can resolve the
        // parameter, so an `unknown` literal there is 42804 — not a JSON string.
        JsonFunc::ToJsonb => {
            if given.first().is_some_and(|a| a.is_unknown()) {
                return Err(crate::eval::undetermined_polymorphic_type());
            }
            vec![None; n]
        }
        JsonFunc::RowToJson => vec![None, Some(ColumnType::Bool)],
    })
}

// ---- result-type inference ----

/// Statically infer a jsonb call's result type (for RowDescription). Arity and
/// argument-type mismatches surface as 42883 here, at plan time.
pub(crate) fn json_func_result_type(fc: &FuncCall, scope: &Scope) -> Result<ColumnType, ExecError> {
    let f = json_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = exprs_of(fc)?;
    let n = args.len();
    let given = crate::eval::static_arg_types(args, scope)?;
    let types = crate::eval::effective_arg_types(&given, &param_types(f, &given)?);
    Ok(match f {
        JsonFunc::BuildObject => {
            // PostgreSQL reports the odd-length list at run time (22023); the
            // arity itself is unconstrained here.
            ColumnType::Jsonb
        }
        JsonFunc::BuildArray => ColumnType::Jsonb,
        JsonFunc::RowToJson => {
            require_arity(fc, n == 1 || n == 2)?;
            if !matches!(types[0], ColumnType::Record(_))
                || types.get(1).is_some_and(|ty| *ty != ColumnType::Bool)
            {
                return Err(undefined_function(&fc.name));
            }
            ColumnType::Text
        }
        JsonFunc::ArrayLength => {
            require_arity(fc, n == 1)?;
            require_jsonb_arg(fc, types[0])?;
            ColumnType::Int4
        }
        JsonFunc::Typeof => {
            require_arity(fc, n == 1)?;
            require_jsonb_arg(fc, types[0])?;
            ColumnType::Text
        }
        JsonFunc::ExtractPath | JsonFunc::ExtractPathText => {
            require_arity(fc, n >= 1)?;
            require_jsonb_arg(fc, types[0])?;
            if types[1..].iter().any(|t| !t.is_string()) {
                return Err(undefined_function(&fc.name));
            }
            if f == JsonFunc::ExtractPath {
                ColumnType::Jsonb
            } else {
                ColumnType::Text
            }
        }
        JsonFunc::Set | JsonFunc::Insert => {
            require_arity(fc, n == 3 || n == 4)?;
            require_jsonb_arg(fc, types[0])?;
            if types[1] != ColumnType::Array(ElemType::Text) {
                return Err(undefined_function(&fc.name));
            }
            require_jsonb_arg(fc, types[2])?;
            ColumnType::Jsonb
        }
        JsonFunc::ToJsonb => {
            require_arity(fc, n == 1)?;
            ColumnType::Jsonb
        }
        JsonFunc::StripNulls => {
            require_arity(fc, n == 1 || n == 2)?;
            require_jsonb_arg(fc, types[0])?;
            ColumnType::Jsonb
        }
        JsonFunc::Pretty => {
            require_arity(fc, n == 1)?;
            require_jsonb_arg(fc, types[0])?;
            ColumnType::Text
        }
        JsonFunc::DeletePath => {
            require_arity(fc, n == 2)?;
            require_jsonb_arg(fc, types[0])?;
            if types[1] != ColumnType::Array(ElemType::Text) {
                return Err(undefined_function(&fc.name));
            }
            ColumnType::Jsonb
        }
        JsonFunc::SetLax => {
            require_arity(fc, (3..=5).contains(&n))?;
            require_jsonb_arg(fc, types[0])?;
            if types[1] != ColumnType::Array(ElemType::Text) {
                return Err(undefined_function(&fc.name));
            }
            require_jsonb_arg(fc, types[2])?;
            ColumnType::Jsonb
        }
        JsonFunc::Object => {
            require_arity(fc, n == 1 || n == 2)?;
            if types
                .iter()
                .any(|t| *t != ColumnType::Array(ElemType::Text))
            {
                return Err(undefined_function(&fc.name));
            }
            ColumnType::Jsonb
        }
        JsonFunc::PathExists | JsonFunc::PathMatch => {
            require_arity(fc, (2..=4).contains(&n))?;
            require_jsonb_arg(fc, types[0])?;
            ColumnType::Bool
        }
        JsonFunc::PathQueryArray | JsonFunc::PathQueryFirst => {
            require_arity(fc, (2..=4).contains(&n))?;
            require_jsonb_arg(fc, types[0])?;
            ColumnType::Jsonb
        }
        JsonFunc::Operator(op) => {
            require_arity(fc, n == 2)?;
            json_operator_result_type(op, types[0], types[1])
                .ok_or_else(|| undefined_function(&fc.name))?
        }
    })
}

/// A `jsonb`-typed argument. An unadorned literal such as
/// `jsonb_typeof('{"a":1}')` has already adopted `jsonb` in [`param_types`]. So,
/// as in PostgreSQL, a genuinely `text`-typed argument is 42883 and needs an
/// explicit cast.
fn require_jsonb_arg(fc: &FuncCall, t: ColumnType) -> Result<(), ExecError> {
    if t == ColumnType::Jsonb {
        Ok(())
    } else {
        Err(undefined_function(&fc.name))
    }
}

// ---- evaluation ----

/// Evaluate a jsonb function call.
///
/// Every function except `jsonb_build_object`/`jsonb_build_array` is STRICT: a
/// NULL argument yields SQL NULL. The two builders are deliberately not strict.
/// A NULL *value* becomes the JSON `null` literal, and a NULL *key* is an error.
pub(crate) fn eval_json(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let f = json_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = exprs_of(fc)?;
    let mut vals: Vec<Datum> = args.iter().map(&mut eval_child).collect::<Result<_, _>>()?;
    // Give every `unknown` literal argument the value its parameter's type calls
    // for, by the same rule the plan-time resolver typed it with.
    let given = crate::eval::value_arg_types(args, &vals);
    crate::eval::coerce_unknown_args(args, &mut vals, &param_types(f, &given)?, ctx)?;
    let n = vals.len();
    match f {
        JsonFunc::BuildObject => build_object(&vals, ctx),
        JsonFunc::BuildArray => {
            let items = vals
                .iter()
                .map(|v| to_jsonb(v, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Datum::Jsonb(JsonbValue::Array(items)))
        }
        JsonFunc::ArrayLength => {
            require_arity(fc, n == 1)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            let value = jsonb_operand(&vals[0], &fc.name)?;
            match value.as_ref() {
                JsonbValue::Array(items) => Ok(Datum::Int4(
                    i32::try_from(items.len()).map_err(|_| ExecError::Type(TypeError::Overflow))?,
                )),
                JsonbValue::Object(_) => {
                    Err(invalid_parameter("cannot get array length of a non-array"))
                }
                _ => Err(invalid_parameter("cannot get array length of a scalar")),
            }
        }
        JsonFunc::Typeof => {
            require_arity(fc, n == 1)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            let value = jsonb_operand(&vals[0], &fc.name)?;
            Ok(Datum::Text(value.type_name().to_string()))
        }
        JsonFunc::ExtractPath | JsonFunc::ExtractPathText => {
            require_arity(fc, n >= 1)?;
            if vals.iter().any(Datum::is_null) {
                return Ok(Datum::Null);
            }
            let value = jsonb_operand(&vals[0], &fc.name)?;
            let mut path = Vec::with_capacity(n - 1);
            for v in &vals[1..] {
                path.push(Some(text_arg(v, &fc.name)?.to_string()));
            }
            let found = navigate(value.as_ref(), &path);
            Ok(match (found, f == JsonFunc::ExtractPath) {
                (None, _) => Datum::Null,
                (Some(v), true) => Datum::Jsonb(v.clone()),
                (Some(v), false) => jsonb_as_sql_text(v),
            })
        }
        JsonFunc::Set => {
            require_arity(fc, n == 3 || n == 4)?;
            if vals.iter().any(Datum::is_null) {
                return Ok(Datum::Null);
            }
            let target = jsonb_operand(&vals[0], &fc.name)?;
            let path = text_path(&vals[1], &fc.name)?;
            let new_value = jsonb_operand(&vals[2], &fc.name)?;
            let create = match vals.get(3) {
                Some(Datum::Bool(b)) => *b,
                None => true,
                Some(other) => return Err(type_error(&fc.name, other)),
            };
            jsonb_set(target.as_ref(), &path, new_value.as_ref(), create)
        }
        JsonFunc::ToJsonb => {
            require_arity(fc, n == 1)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Jsonb(to_jsonb(&vals[0], ctx)?))
        }
        JsonFunc::RowToJson => {
            require_arity(fc, n == 1 || n == 2)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            if !matches!(vals[0], Datum::Record(_)) {
                return Err(undefined_function(&fc.name));
            }
            let value = to_jsonb(&vals[0], ctx)?;
            match vals.get(1) {
                None | Some(Datum::Bool(false)) => Ok(Datum::Text(compact_json(&value))),
                Some(Datum::Bool(true)) => Ok(Datum::Text(pretty(&value, 0))),
                Some(Datum::Null) => Ok(Datum::Null),
                Some(other) => Err(type_error(&fc.name, other)),
            }
        }
        JsonFunc::StripNulls => {
            require_arity(fc, n == 1 || n == 2)?;
            if vals.iter().any(Datum::is_null) {
                return Ok(Datum::Null);
            }
            let in_arrays = match vals.get(1) {
                None => false,
                Some(Datum::Bool(b)) => *b,
                Some(other) => return Err(type_error(&fc.name, other)),
            };
            let value = jsonb_operand(&vals[0], &fc.name)?;
            Ok(Datum::Jsonb(strip_nulls(value.as_ref(), in_arrays)))
        }
        JsonFunc::Pretty => {
            require_arity(fc, n == 1)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            let value = jsonb_operand(&vals[0], &fc.name)?;
            Ok(Datum::Text(pretty(value.as_ref(), 0)))
        }
        JsonFunc::Insert => {
            require_arity(fc, n == 3 || n == 4)?;
            if vals.iter().any(Datum::is_null) {
                return Ok(Datum::Null);
            }
            let target = jsonb_operand(&vals[0], &fc.name)?;
            let path = text_path(&vals[1], &fc.name)?;
            let new_value = jsonb_operand(&vals[2], &fc.name)?;
            let after = match vals.get(3) {
                Some(Datum::Bool(b)) => *b,
                None => false,
                Some(other) => return Err(type_error(&fc.name, other)),
            };
            if !matches!(
                target.as_ref(),
                JsonbValue::Object(_) | JsonbValue::Array(_)
            ) {
                return Err(invalid_parameter("cannot set path in scalar"));
            }
            Ok(Datum::Jsonb(insert_path(
                target.as_ref(),
                &path,
                new_value.as_ref(),
                after,
            )?))
        }
        JsonFunc::DeletePath => {
            require_arity(fc, n == 2)?;
            if vals.iter().any(Datum::is_null) {
                return Ok(Datum::Null);
            }
            let target = jsonb_operand(&vals[0], &fc.name)?;
            let path = text_path(&vals[1], &fc.name)?;
            if !matches!(
                target.as_ref(),
                JsonbValue::Object(_) | JsonbValue::Array(_)
            ) {
                return Err(invalid_parameter("cannot delete path in scalar"));
            }
            Ok(Datum::Jsonb(delete_path(target.as_ref(), &path)))
        }
        JsonFunc::SetLax => {
            require_arity(fc, (3..=5).contains(&n))?;
            eval_set_lax(fc, &vals)
        }
        JsonFunc::Object => {
            require_arity(fc, n == 1 || n == 2)?;
            if vals.iter().any(Datum::is_null) {
                return Ok(Datum::Null);
            }
            eval_json_object(&fc.name, &vals)
        }
        JsonFunc::PathExists
        | JsonFunc::PathMatch
        | JsonFunc::PathQueryArray
        | JsonFunc::PathQueryFirst => {
            require_arity(fc, (2..=4).contains(&n))?;
            eval_path_func(f, &fc.name, &vals)
        }
        JsonFunc::Operator(op) => {
            require_arity(fc, n == 2)?;
            eval_json_operator(op, &vals[0], &vals[1])
        }
    }
}

// ---- jsonpath ----

/// One resolved `jsonb_path_*` call: the target document, the compiled path,
/// the optional `vars` object and the `silent` flag.
struct PathCall {
    target: JsonbValue,
    path: crate::jsonpath::JsonPath,
    vars: Option<JsonbValue>,
    silent: bool,
}

/// Resolve the arguments every `jsonb_path_*` function shares. `None` means the
/// whole call is SQL NULL, because all of them are STRICT in the target and the
/// path.
fn path_args(name: &str, args: &[Datum]) -> Result<Option<PathCall>, ExecError> {
    if args[0].is_null() || args[1].is_null() {
        return Ok(None);
    }
    let target = jsonb_operand(&args[0], name)?.into_owned();
    let path = crate::jsonpath::JsonPath::parse(text_arg(&args[1], name)?)?;
    let vars = match args.get(2) {
        None | Some(Datum::Null) => None,
        Some(given) => {
            let object = jsonb_operand(given, name)?.into_owned();
            crate::jsonpath::check_vars(&object)?;
            Some(object)
        }
    };
    let silent = match args.get(3) {
        None | Some(Datum::Null) => false,
        Some(Datum::Bool(b)) => *b,
        Some(other) => return Err(type_error(name, other)),
    };
    Ok(Some(PathCall {
        target,
        path,
        vars,
        silent,
    }))
}

fn eval_path_func(f: JsonFunc, name: &str, args: &[Datum]) -> Result<Datum, ExecError> {
    let Some(call) = path_args(name, args)? else {
        return Ok(Datum::Null);
    };
    let (target, path, silent) = (&call.target, &call.path, call.silent);
    let vars = call.vars.as_ref();
    Ok(match f {
        JsonFunc::PathExists => match path.exists(target, vars, silent)? {
            Some(b) => Datum::Bool(b),
            None => Datum::Null,
        },
        JsonFunc::PathMatch => match path.predicate(target, vars, silent)? {
            Some(b) => Datum::Bool(b),
            None => Datum::Null,
        },
        JsonFunc::PathQueryArray => {
            Datum::Jsonb(JsonbValue::Array(path.query(target, vars, silent)?))
        }
        // `jsonb_path_query_first` keeps only the first item; an empty result is
        // SQL NULL, not an empty array.
        _ => match path.query(target, vars, silent)?.into_iter().next() {
            Some(v) => Datum::Jsonb(v),
            None => Datum::Null,
        },
    })
}

/// `jsonb @? jsonpath` / `jsonb @@ jsonpath`. Both operators run `silent`, so a
/// structural error is SQL NULL rather than a raised error.
fn json_path_operator(left: &Datum, right: &Datum, predicate: bool) -> Result<Datum, ExecError> {
    let op = if predicate {
        JsonOp::PathMatch
    } else {
        JsonOp::PathExists
    };
    if left.is_null() || right.is_null() {
        return Ok(Datum::Null);
    }
    let Some(target) = jsonb_or_null(left, op)? else {
        return Ok(Datum::Null);
    };
    let path = crate::jsonpath::JsonPath::parse(text_arg(right, op.spelling())?)?;
    let result = if predicate {
        path.predicate(target.as_ref(), None, true)?
    } else {
        path.exists(target.as_ref(), None, true)?
    };
    Ok(match result {
        Some(b) => Datum::Bool(b),
        None => Datum::Null,
    })
}

/// `jsonb_set_lax(target, path, new_value, create_if_missing, null_value_treatment)`,
/// which is `jsonb_set` plus an explicit policy for a SQL NULL `new_value`.
fn eval_set_lax(fc: &FuncCall, vals: &[Datum]) -> Result<Datum, ExecError> {
    let treatment = match vals.get(4) {
        None => "use_json_null".to_string(),
        Some(Datum::Null) => return Ok(Datum::Null),
        Some(v) => text_arg(v, &fc.name)?.to_ascii_lowercase(),
    };
    if !matches!(
        treatment.as_str(),
        "raise_exception" | "use_json_null" | "delete_key" | "return_target"
    ) {
        return Err(ExecError::FunctionError {
            sqlstate: "22023",
            message: "null_value_treatment must be \"delete_key\", \"return_target\", \"use_json_null\", or \"raise_exception\"".into(),
        });
    }
    if vals[0].is_null() || vals[1].is_null() {
        return Ok(Datum::Null);
    }
    let create = match vals.get(3) {
        None => true,
        Some(Datum::Null) => return Ok(Datum::Null),
        Some(Datum::Bool(b)) => *b,
        Some(other) => return Err(type_error(&fc.name, other)),
    };
    let target = jsonb_operand(&vals[0], &fc.name)?;
    let path = text_path(&vals[1], &fc.name)?;
    if vals[2].is_null() {
        return match treatment.as_str() {
            "raise_exception" => Err(ExecError::FunctionError {
                sqlstate: "22004",
                message: "JSON value must not be null".into(),
            }),
            "return_target" => Ok(Datum::Jsonb(target.into_owned())),
            "delete_key" => {
                if !matches!(
                    target.as_ref(),
                    JsonbValue::Object(_) | JsonbValue::Array(_)
                ) {
                    return Err(invalid_parameter("cannot delete path in scalar"));
                }
                Ok(Datum::Jsonb(delete_path(target.as_ref(), &path)))
            }
            _ => jsonb_set(target.as_ref(), &path, &JsonbValue::Null, create),
        };
    }
    let new_value = jsonb_operand(&vals[2], &fc.name)?;
    jsonb_set(target.as_ref(), &path, new_value.as_ref(), create)
}

/// `json_object(text[])` / `json_object(text[], text[])`: an object whose values
/// are all JSON strings. A NULL element becomes the JSON `null` literal.
fn eval_json_object(name: &str, vals: &[Datum]) -> Result<Datum, ExecError> {
    let flat = |d: &Datum| -> Result<Vec<Datum>, ExecError> {
        match d {
            Datum::Array(a) => Ok(a.elems.clone()),
            other => Err(type_error(name, other)),
        }
    };
    let element = |d: &Datum| -> JsonbValue {
        match d {
            Datum::Null => JsonbValue::Null,
            Datum::Text(s) => JsonbValue::String(s.clone()),
            other => JsonbValue::String(
                String::from_utf8(crabka_pgtypes::encoding::encode_text(
                    other,
                    &jiff::tz::TimeZone::UTC,
                ))
                .unwrap_or_default(),
            ),
        }
    };
    let key_text = |d: &Datum| -> Result<String, ExecError> {
        match d {
            Datum::Null => Err(invalid_parameter("null value not allowed for object key")),
            Datum::Text(s) => Ok(s.clone()),
            other => Err(type_error(name, other)),
        }
    };
    let mut pairs = Vec::new();
    if vals.len() == 1 {
        let items = flat(&vals[0])?;
        if items.len() % 2 != 0 {
            return Err(ExecError::FunctionError {
                sqlstate: "2202E",
                message: "array must have even number of elements".into(),
            });
        }
        for pair in items.chunks_exact(2) {
            pairs.push((key_text(&pair[0])?, element(&pair[1])));
        }
    } else {
        let keys = flat(&vals[0])?;
        let values = flat(&vals[1])?;
        if keys.len() != values.len() {
            return Err(ExecError::FunctionError {
                sqlstate: "2202E",
                message: "mismatched array dimensions".into(),
            });
        }
        for (k, v) in keys.iter().zip(&values) {
            pairs.push((key_text(k)?, element(v)));
        }
    }
    Ok(Datum::Jsonb(JsonbValue::object_from_pairs(pairs)))
}

// ---- the SQL/JSON standard expressions ----

/// Evaluate one SQL/JSON standard expression (`IS JSON`, `JSON_OBJECT`,
/// `JSON_ARRAY`, `JSON_SCALAR`, `JSON_SERIALIZE`, `JSON()`, `JSON_EXISTS`,
/// `JSON_VALUE`, `JSON_QUERY`).
pub(crate) fn eval_sql_json(
    node: &SqlJsonExpr,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    match node {
        SqlJsonExpr::IsJson {
            expr,
            negated,
            item,
            unique_keys,
        } => {
            let value = eval_child(expr)?;
            // A NULL operand makes the whole predicate NULL, unlike `IS NULL`.
            if value.is_null() {
                return Ok(Datum::Null);
            }
            let holds = is_json(&value, *item, *unique_keys)?;
            Ok(Datum::Bool(holds ^ *negated))
        }
        SqlJsonExpr::Object {
            entries,
            absent_on_null,
            unique_keys,
            returning,
        } => {
            let mut pairs: Vec<(String, JsonbValue)> = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                let value = eval_child(value)?;
                if *absent_on_null && value.is_null() {
                    continue;
                }
                let key = eval_child(key)?;
                if key.is_null() {
                    return Err(invalid_parameter_owned(format!(
                        "argument {}: key must not be null",
                        pairs.len() * 2 + 1
                    )));
                }
                let key = object_key_text(&key, ctx)?;
                if *unique_keys && pairs.iter().any(|(k, _)| *k == key) {
                    return Err(ExecError::FunctionError {
                        sqlstate: "22030",
                        message: "duplicate JSON object key value".into(),
                    });
                }
                pairs.push((key, to_jsonb(&value, ctx)?));
            }
            returning_json(JsonbValue::object_from_pairs(pairs), *returning, ctx)
        }
        SqlJsonExpr::Array {
            items,
            absent_on_null,
            returning,
        } => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let value = eval_child(item)?;
                if *absent_on_null && value.is_null() {
                    continue;
                }
                out.push(to_jsonb(&value, ctx)?);
            }
            returning_json(JsonbValue::Array(out), *returning, ctx)
        }
        SqlJsonExpr::Scalar(expr) => {
            let value = eval_child(expr)?;
            if value.is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Jsonb(to_jsonb(&value, ctx)?))
        }
        SqlJsonExpr::Serialize { expr, returning } => {
            let value = eval_child(expr)?;
            if value.is_null() {
                return Ok(Datum::Null);
            }
            let text = Datum::Text(json_document(&value)?.to_text());
            match returning {
                Some(ty) => Ok(crabka_pgtypes::cast::cast(&text, *ty, &ctx.time_zone)?),
                None => Ok(text),
            }
        }
        SqlJsonExpr::Parse { expr, unique_keys } => {
            let value = eval_child(expr)?;
            if value.is_null() {
                return Ok(Datum::Null);
            }
            let (document, duplicate) = match &value {
                Datum::Jsonb(j) => (j.clone(), false),
                Datum::Text(text) => {
                    jsonb::parse_with_options(text, false).map_err(ExecError::Type)?
                }
                other => return Err(type_error("json", other)),
            };
            if *unique_keys && duplicate {
                return Err(ExecError::FunctionError {
                    sqlstate: "22030",
                    message: "duplicate JSON object key value".into(),
                });
            }
            Ok(Datum::Jsonb(document))
        }
        SqlJsonExpr::Query(q) => eval_json_query(q, ctx, eval_child),
    }
}

/// `RETURNING <type>` on a constructor: the document is produced as `jsonb` and
/// converted, so `RETURNING text` renders it and `RETURNING jsonb` is identity.
fn returning_json(
    value: JsonbValue,
    returning: Option<ColumnType>,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let datum = Datum::Jsonb(value);
    match returning {
        None | Some(ColumnType::Jsonb) => Ok(datum),
        Some(ty) => Ok(crabka_pgtypes::cast::cast(&datum, ty, &ctx.time_zone)?),
    }
}

/// `IS [NOT] JSON [<item>] [WITH UNIQUE KEYS]` over an already-evaluated,
/// non-NULL value. Only the string family and `jsonb` have a JSON reading at
/// all. Every other type is 42804, as in `PostgreSQL`.
fn is_json(
    value: &Datum,
    item: crabka_pgparser::ast::JsonItemType,
    unique_keys: bool,
) -> Result<bool, ExecError> {
    use crabka_pgparser::ast::JsonItemType;

    let (document, duplicate) = match value {
        Datum::Jsonb(j) => (j.clone(), false),
        // Text that does not parse is simply not JSON.
        Datum::Text(text) => match jsonb::parse_with_options(text, false) {
            Ok(pair) => pair,
            Err(_) => return Ok(false),
        },
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "cannot use type {} in IS JSON predicate",
                type_name(other)
            )));
        }
    };
    if unique_keys && duplicate {
        return Ok(false);
    }
    Ok(match item {
        JsonItemType::Value => true,
        JsonItemType::Object => matches!(document, JsonbValue::Object(_)),
        JsonItemType::Array => matches!(document, JsonbValue::Array(_)),
        JsonItemType::Scalar => !matches!(document, JsonbValue::Object(_) | JsonbValue::Array(_)),
    })
}

/// The plan-time counterpart: `IS JSON` accepts only the string family and
/// `jsonb`, so `1 IS JSON` is 42804 before a row is ever read.
pub(crate) fn is_json_operand_type(ty: ColumnType) -> Result<(), ExecError> {
    if ty.is_string() || ty == ColumnType::Jsonb {
        Ok(())
    } else {
        Err(ExecError::TypeMismatch(format!(
            "cannot use type {} in IS JSON predicate",
            ty.name()
        )))
    }
}

/// The jsonb document an argument denotes: a `jsonb` value as-is, `text` parsed.
fn json_document(value: &Datum) -> Result<Cow<'_, JsonbValue>, ExecError> {
    match value {
        Datum::Jsonb(j) => Ok(Cow::Borrowed(j)),
        Datum::Text(text) => jsonb::parse(text).map(Cow::Owned).map_err(ExecError::Type),
        other => Err(type_error("json", other)),
    }
}

/// `JSON_EXISTS` / `JSON_VALUE` / `JSON_QUERY`.
fn eval_json_query(
    q: &crabka_pgparser::ast::JsonQuery,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    use crabka_pgparser::ast::{JsonQueryOp, JsonWrapper};

    let context = eval_child(&q.context)?;
    let path_text = eval_child(&q.path)?;
    if context.is_null() || path_text.is_null() {
        return Ok(Datum::Null);
    }
    let mut vars = Vec::with_capacity(q.passing.len());
    for (name, expr) in &q.passing {
        let value = eval_child(expr)?;
        vars.push((name.clone(), to_jsonb(&value, ctx)?));
    }
    let vars = JsonbValue::object_from_pairs(vars);
    // Everything from here on is subject to `ON ERROR`, so it is computed inside
    // a closure whose error the behavior clause decides what to do with.
    let computed = (|| -> Result<Option<Datum>, ExecError> {
        let document = json_document(&context)?;
        let path = crate::jsonpath::JsonPath::parse(text_arg(&path_text, "jsonpath")?)?;
        let items = path.query(document.as_ref(), Some(&vars), false)?;
        Ok(match q.op {
            JsonQueryOp::Exists => Some(Datum::Bool(!items.is_empty())),
            JsonQueryOp::Value => match items.as_slice() {
                [] => None,
                [JsonbValue::Null] => Some(Datum::Null),
                [JsonbValue::Object(_) | JsonbValue::Array(_)] => {
                    return Err(json_value_not_scalar());
                }
                [single] => Some(sql_json_value(single, q.returning, ctx)?),
                _ => return Err(json_value_not_scalar()),
            },
            JsonQueryOp::Query => {
                let wrapped = match (q.wrapper, items.len()) {
                    (JsonWrapper::Unconditional, _) | (JsonWrapper::Conditional, 0) => {
                        Some(JsonbValue::Array(items))
                    }
                    (JsonWrapper::Conditional, 1) => items.into_iter().next(),
                    (JsonWrapper::Conditional, _) => Some(JsonbValue::Array(items)),
                    (JsonWrapper::Without, 0) => None,
                    (JsonWrapper::Without, 1) => items.into_iter().next(),
                    (JsonWrapper::Without, _) => {
                        return Err(ExecError::FunctionError {
                            sqlstate: "22034",
                            message: "JSON path expression in JSON_QUERY must return single item when no wrapper is requested".into(),
                        });
                    }
                };
                match wrapped {
                    None => None,
                    Some(value) => {
                        // `OMIT QUOTES` unwraps a JSON string, which then has to
                        // parse as a document again for a `jsonb` result.
                        let rendered = match (&value, q.omit_quotes) {
                            (JsonbValue::String(s), true) => s.clone(),
                            (other, _) => other.to_text(),
                        };
                        Some(sql_json_text(&rendered, q.returning, ctx)?)
                    }
                }
            }
        })
    })();
    let empty_default = match q.op {
        // `JSON_EXISTS` has no empty case: no items is simply false.
        JsonQueryOp::Exists => Datum::Bool(false),
        _ => Datum::Null,
    };
    match computed {
        Ok(Some(value)) => Ok(value),
        Ok(None) => match &q.on_empty {
            None => Ok(empty_default),
            Some(behavior) => apply_behavior(behavior, q, ctx, eval_child, true),
        },
        Err(error) => match &q.on_error {
            // PostgreSQL's defaults: `JSON_EXISTS` is FALSE ON ERROR, the other
            // two are NULL ON ERROR.
            None => Ok(match q.op {
                JsonQueryOp::Exists => Datum::Bool(false),
                _ => Datum::Null,
            }),
            Some(crabka_pgparser::ast::JsonBehavior::Error) => Err(error),
            Some(behavior) => apply_behavior(behavior, q, ctx, eval_child, false),
        },
    }
}

/// What an `ON EMPTY` / `ON ERROR` clause produces.
fn apply_behavior(
    behavior: &crabka_pgparser::ast::JsonBehavior,
    q: &crabka_pgparser::ast::JsonQuery,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
    on_empty: bool,
) -> Result<Datum, ExecError> {
    use crabka_pgparser::ast::JsonBehavior;

    Ok(match behavior {
        JsonBehavior::Error => {
            return Err(ExecError::FunctionError {
                sqlstate: "22035",
                message: if on_empty {
                    "no SQL/JSON item found for specified path".into()
                } else {
                    "SQL/JSON member not found".into()
                },
            });
        }
        JsonBehavior::Null => Datum::Null,
        JsonBehavior::True => Datum::Bool(true),
        JsonBehavior::False => Datum::Bool(false),
        JsonBehavior::Unknown => Datum::Null,
        JsonBehavior::EmptyArray => sql_json_text("[]", q.returning, ctx)?,
        JsonBehavior::EmptyObject => sql_json_text("{}", q.returning, ctx)?,
        JsonBehavior::Default(expr) => {
            let value = eval_child(expr)?;
            let target = q.returning.unwrap_or(match q.op {
                crabka_pgparser::ast::JsonQueryOp::Exists => ColumnType::Bool,
                crabka_pgparser::ast::JsonQueryOp::Value => ColumnType::Text,
                crabka_pgparser::ast::JsonQueryOp::Query => ColumnType::Jsonb,
            });
            crabka_pgtypes::cast::cast(&value, target, &ctx.time_zone)?
        }
    })
}

fn json_value_not_scalar() -> ExecError {
    ExecError::FunctionError {
        sqlstate: "2203F",
        message: "JSON path expression in JSON_VALUE must return single scalar item".into(),
    }
}

/// `JSON_VALUE`'s scalar unwrapping: a JSON string loses its quotes, every other
/// scalar keeps its canonical rendering, and the result is cast to `RETURNING`.
fn sql_json_value(
    item: &JsonbValue,
    returning: Option<ColumnType>,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let text = match item {
        JsonbValue::String(s) => s.clone(),
        other => other.to_text(),
    };
    let datum = Datum::Text(text);
    match returning {
        None | Some(ColumnType::Text) => Ok(datum),
        Some(ty) => Ok(crabka_pgtypes::cast::cast(&datum, ty, &ctx.time_zone)?),
    }
}

/// `JSON_QUERY`'s result: JSON text converted to the `RETURNING` type (`jsonb`
/// by default).
fn sql_json_text(
    text: &str,
    returning: Option<ColumnType>,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let target = returning.unwrap_or(ColumnType::Jsonb);
    crabka_pgtypes::cast::cast(&Datum::Text(text.to_string()), target, &ctx.time_zone)
        .map_err(ExecError::Type)
}

// ---- subscripting ----

/// One jsonb subscript as `PostgreSQL` stores it: a `text` path element. An
/// integer subscript is converted to its decimal text, which is why
/// `('{"0": 1}'::jsonb)[0]` finds the key `"0"` and `('[1, 2]'::jsonb)['1']`
/// finds element 1.
///
/// `PostgreSQL` accepts only text-ish and `integer` subscripts. Everything else,
/// `bigint` and `numeric` included, is 42804 at parse-analysis time.
fn subscript_path_element(index: &Datum) -> Result<Option<String>, ExecError> {
    Ok(match index {
        Datum::Null => None,
        Datum::Text(s) => Some(s.clone()),
        Datum::Int2(n) => Some(n.to_string()),
        Datum::Int4(n) => Some(n.to_string()),
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "subscript type {} is not supported",
                type_name(other)
            )));
        }
    })
}

/// `j[subscript]`: `PostgreSQL`'s jsonb subscripting *read*. A missing key, an
/// out-of-range index, a NULL subscript and a scalar container are all SQL NULL.
/// Nothing here is an error.
pub(crate) fn jsonb_subscript(base: &Datum, index: &Datum) -> Result<Datum, ExecError> {
    let step = subscript_path_element(index)?;
    if base.is_null() {
        return Ok(Datum::Null);
    }
    let value = jsonb_operand(base, "jsonb subscript")?;
    Ok(
        match navigate(value.as_ref(), std::slice::from_ref(&step)) {
            Some(v) => Datum::Jsonb(v.clone()),
            None => Datum::Null,
        },
    )
}

/// `UPDATE t SET j[s1][s2] = v`: `PostgreSQL`'s `jsonb_set_element`, which is
/// `setPath` with *create*, *fill gaps* and *consistent position*. This function
/// builds the missing intermediate levels, where an integer step makes an array
/// and a text step makes an object. It pads a positive index past the end with
/// JSON nulls. A negative index that reaches before the start is an error rather
/// than a prepend.
pub(crate) fn jsonb_subscript_assign(
    target: &Datum,
    subscripts: &[Datum],
    new_value: &Datum,
) -> Result<Datum, ExecError> {
    let mut path = Vec::with_capacity(subscripts.len());
    for s in subscripts {
        let Some(step) = subscript_path_element(s)? else {
            return Err(ExecError::FunctionError {
                sqlstate: "22004",
                message: "jsonb subscript in assignment must not be null".into(),
            });
        };
        path.push(step);
    }
    // A SQL NULL new value writes the JSON `null` literal.
    let value = match new_value {
        Datum::Null => JsonbValue::Null,
        other => jsonb_operand(other, "jsonb subscript")?.into_owned(),
    };
    // A NULL container is created from scratch: an array when the first
    // subscript was written as an integer, an object otherwise.
    let container = match target {
        Datum::Null => {
            if matches!(subscripts.first(), Some(Datum::Int2(_) | Datum::Int4(_))) {
                JsonbValue::Array(Vec::new())
            } else {
                JsonbValue::Object(Vec::new())
            }
        }
        other => jsonb_operand(other, "jsonb subscript")?.into_owned(),
    };
    Ok(Datum::Jsonb(set_element(&container, &path, 0, &value)?))
}

fn cannot_replace_existing_key() -> ExecError {
    invalid_parameter("cannot replace existing key")
}

/// `PostgreSQL`'s `setPath`, specialized to the subscript-assignment flags.
fn set_element(
    value: &JsonbValue,
    path: &[String],
    level: usize,
    new_value: &JsonbValue,
) -> Result<JsonbValue, ExecError> {
    match value {
        JsonbValue::Object(pairs) => set_element_object(pairs, path, level, new_value),
        JsonbValue::Array(items) => set_element_array(items, path, level, new_value),
        // A scalar cannot hold a further path step. `PostgreSQL` raises the same
        // error for a nested scalar and for a raw-scalar document.
        scalar => {
            if level < path.len() {
                Err(cannot_replace_existing_key())
            } else {
                Ok(scalar.clone())
            }
        }
    }
}

fn set_element_object(
    pairs: &[(String, JsonbValue)],
    path: &[String],
    level: usize,
    new_value: &JsonbValue,
) -> Result<JsonbValue, ExecError> {
    let key = &path[level];
    let last = level == path.len() - 1;
    let mut out: Vec<(String, JsonbValue)> = Vec::with_capacity(pairs.len() + 1);
    let mut done = false;
    // An empty object at the final step takes the new key directly.
    if pairs.is_empty() && last {
        out.push((key.clone(), new_value.clone()));
        done = true;
    }
    for (i, (k, v)) in pairs.iter().enumerate() {
        if !done && k == key {
            done = true;
            if last {
                out.push((k.clone(), new_value.clone()));
            } else {
                out.push((k.clone(), set_element(v, path, level + 1, new_value)?));
            }
            continue;
        }
        // A missing key at the final step is appended after the last member.
        if !done && last && i == pairs.len() - 1 {
            out.push((key.clone(), new_value.clone()));
            done = true;
        }
        out.push((k.clone(), v.clone()));
    }
    if !done && !last {
        out.push((key.clone(), build_path(path, level, new_value)));
    }
    Ok(JsonbValue::object_from_pairs(out))
}

fn set_element_array(
    items: &[JsonbValue],
    path: &[String],
    level: usize,
    new_value: &JsonbValue,
) -> Result<JsonbValue, ExecError> {
    let nelems = i64::try_from(items.len()).unwrap_or(i64::MAX);
    let raw: i64 = path[level]
        .parse::<i32>()
        .map_err(|_| {
            invalid_parameter_owned(format!(
                "path element at position {} is not an integer",
                level + 1
            ))
        })?
        .into();
    let mut idx = raw;
    if idx < 0 {
        if -idx > nelems {
            // `JB_PATH_CONSISTENT_POSITION`: a negative index that would land
            // before the start is refused rather than prepended.
            return Err(invalid_parameter_owned(format!(
                "path element at position {} is out of range: {raw}",
                level + 1
            )));
        }
        idx += nelems;
    }
    let last = level == path.len() - 1;
    let mut out: Vec<JsonbValue> = Vec::with_capacity(items.len() + 1);
    let mut done = false;
    if items.is_empty() && last {
        push_nulls(&mut out, idx);
        out.push(new_value.clone());
        done = true;
    }
    for (i, item) in items.iter().enumerate() {
        if !done && i64::try_from(i).unwrap_or(i64::MAX) == idx {
            done = true;
            if last {
                out.push(new_value.clone());
            } else {
                out.push(set_element(item, path, level + 1, new_value)?);
            }
            continue;
        }
        out.push(item.clone());
    }
    if !done && last {
        push_nulls(&mut out, idx - nelems);
        out.push(new_value.clone());
        done = true;
    }
    if !done {
        push_nulls(&mut out, idx - nelems);
        out.push(build_path(path, level, new_value));
    }
    Ok(JsonbValue::Array(out))
}

fn push_nulls(out: &mut Vec<JsonbValue>, count: i64) {
    for _ in 0..count.max(0) {
        out.push(JsonbValue::Null);
    }
}

/// `PostgreSQL`'s `push_path`: the nested containers `path[level + 1 ..]`
/// describes, with `new_value` at the bottom. An integer step builds an array,
/// padded with JSON nulls up to that index. A text step builds an object.
fn build_path(path: &[String], level: usize, new_value: &JsonbValue) -> JsonbValue {
    let mut value = new_value.clone();
    for step in path[level + 1..].iter().rev() {
        value = match step.parse::<i32>() {
            Ok(index) => {
                let mut items = Vec::new();
                push_nulls(&mut items, i64::from(index));
                items.push(value);
                JsonbValue::Array(items)
            }
            Err(_) => JsonbValue::object_from_pairs(vec![(step.clone(), value)]),
        };
    }
    value
}

// ---- the jsonb set-returning functions ----

/// The `jsonb` set-returning functions, expanded by `srf.rs` through the same
/// registry every other SRF goes through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonbSrf {
    /// `jsonb_each(jsonb)` → `(key text, value jsonb)`.
    Each,
    /// `jsonb_each_text(jsonb)` → `(key text, value text)`.
    EachText,
    /// `jsonb_object_keys(jsonb)` → `text`.
    ObjectKeys,
    /// `jsonb_array_elements(jsonb)` → `value jsonb`.
    ArrayElements,
    /// `jsonb_array_elements_text(jsonb)` → `value text`.
    ArrayElementsText,
}

/// `jsonb_path_query(target, path [, vars [, silent]])`: one row per item the
/// jsonpath produces.
pub(crate) fn jsonb_path_query_rows(
    name: &str,
    args: &[Datum],
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let Some(call) = path_args(name, args)? else {
        return Ok(Vec::new());
    };
    Ok(call
        .path
        .query(&call.target, call.vars.as_ref(), call.silent)?
        .into_iter()
        .map(|item| vec![Datum::Jsonb(item)])
        .collect())
}

/// Expand a jsonb set-returning function over its single already-evaluated
/// argument. Object keys come out in the canonical jsonb order the value is
/// already stored in. The `_text` flavours unquote a JSON string and turn the
/// JSON `null` literal into SQL NULL, exactly as `->>` does.
pub(crate) fn jsonb_srf_rows(kind: JsonbSrf, vals: &[Datum]) -> Result<Vec<Vec<Datum>>, ExecError> {
    let name = match kind {
        JsonbSrf::Each => "jsonb_each",
        JsonbSrf::EachText => "jsonb_each_text",
        JsonbSrf::ObjectKeys => "jsonb_object_keys",
        JsonbSrf::ArrayElements => "jsonb_array_elements",
        JsonbSrf::ArrayElementsText => "jsonb_array_elements_text",
    };
    let value = jsonb_operand(&vals[0], name)?;
    Ok(match kind {
        JsonbSrf::Each | JsonbSrf::EachText => {
            let JsonbValue::Object(pairs) = value.as_ref() else {
                return Err(invalid_parameter(match kind {
                    JsonbSrf::Each => "cannot call jsonb_each on a non-object",
                    _ => "cannot call jsonb_each_text on a non-object",
                }));
            };
            pairs
                .iter()
                .map(|(key, item)| {
                    let cell = if kind == JsonbSrf::Each {
                        Datum::Jsonb(item.clone())
                    } else {
                        jsonb_as_sql_text(item)
                    };
                    vec![Datum::Text(key.clone()), cell]
                })
                .collect()
        }
        JsonbSrf::ObjectKeys => match value.as_ref() {
            JsonbValue::Object(pairs) => pairs
                .iter()
                .map(|(key, _)| vec![Datum::Text(key.clone())])
                .collect(),
            JsonbValue::Array(_) => {
                return Err(invalid_parameter(
                    "cannot call jsonb_object_keys on an array",
                ));
            }
            _ => {
                return Err(invalid_parameter(
                    "cannot call jsonb_object_keys on a scalar",
                ));
            }
        },
        JsonbSrf::ArrayElements | JsonbSrf::ArrayElementsText => match value.as_ref() {
            JsonbValue::Array(items) => items
                .iter()
                .map(|item| {
                    let cell = if kind == JsonbSrf::ArrayElements {
                        Datum::Jsonb(item.clone())
                    } else {
                        jsonb_as_sql_text(item)
                    };
                    vec![cell]
                })
                .collect(),
            JsonbValue::Object(_) => {
                return Err(invalid_parameter("cannot extract elements from an object"));
            }
            _ => {
                return Err(invalid_parameter("cannot extract elements from a scalar"));
            }
        },
    })
}

/// `jsonb_strip_nulls`: an object field whose value is the JSON `null` literal
/// disappears, recursively. A `null` ARRAY element is kept. PostgreSQL only
/// strips fields, because dropping an element would renumber the array.
fn strip_nulls(value: &JsonbValue, in_arrays: bool) -> JsonbValue {
    match value {
        JsonbValue::Object(pairs) => JsonbValue::Object(
            pairs
                .iter()
                .filter(|(_, v)| !matches!(v, JsonbValue::Null))
                .map(|(k, v)| (k.clone(), strip_nulls(v, in_arrays)))
                .collect(),
        ),
        JsonbValue::Array(items) => JsonbValue::Array(
            items
                .iter()
                .filter(|v| !in_arrays || !matches!(v, JsonbValue::Null))
                .map(|v| strip_nulls(v, in_arrays))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// `jsonb_pretty`: four-space indentation, one member/element per line, and an
/// empty container rendered as its two brackets on consecutive lines. A scalar
/// keeps its canonical one-line rendering.
fn pretty(value: &JsonbValue, indent: usize) -> String {
    let pad = |n: usize| " ".repeat(n);
    match value {
        JsonbValue::Object(pairs) if pairs.is_empty() => format!("{{\n{}}}", pad(indent)),
        JsonbValue::Object(pairs) => {
            let body = pairs
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}{}: {}",
                        pad(indent + 4),
                        JsonbValue::String(k.clone()).to_text(),
                        pretty(v, indent + 4)
                    )
                })
                .collect::<Vec<_>>()
                .join(",\n");
            format!("{{\n{body}\n{}}}", pad(indent))
        }
        JsonbValue::Array(items) if items.is_empty() => format!("[\n{}]", pad(indent)),
        JsonbValue::Array(items) => {
            let body = items
                .iter()
                .map(|v| format!("{}{}", pad(indent + 4), pretty(v, indent + 4)))
                .collect::<Vec<_>>()
                .join(",\n");
            format!("[\n{body}\n{}]", pad(indent))
        }
        scalar => scalar.to_text(),
    }
}

/// `jsonb_insert(target, path, new_value, insert_after)`.
///
/// The path's final step names the position to insert AT, before it, or after it
/// when `after` is set. In an object the key must not already exist. Replacing
/// one is `jsonb_set`'s job and is 22023 here. In an array an out-of-range index
/// appends, or prepends for a negative one.
fn insert_path(
    target: &JsonbValue,
    path: &[String],
    new_value: &JsonbValue,
    after: bool,
) -> Result<JsonbValue, ExecError> {
    let Some((step, rest)) = path.split_first() else {
        return Ok(target.clone());
    };
    Ok(match target {
        JsonbValue::Object(pairs) => {
            let existing = pairs.iter().position(|(k, _)| k == step);
            match (existing, rest.is_empty()) {
                (Some(_), true) => return Err(invalid_parameter("cannot replace existing key")),
                (Some(at), false) => {
                    let mut pairs = pairs.clone();
                    pairs[at].1 = insert_path(&pairs[at].1, rest, new_value, after)?;
                    JsonbValue::Object(pairs)
                }
                (None, true) => {
                    let mut pairs = pairs.clone();
                    pairs.push((step.clone(), new_value.clone()));
                    JsonbValue::object_from_pairs(pairs)
                }
                (None, false) => JsonbValue::Object(pairs.clone()),
            }
        }
        JsonbValue::Array(items) => {
            let index = step.parse::<i64>().map_err(|_| {
                ExecError::Type(TypeError::Domain {
                    sqlstate: "22P02",
                    message: "path element is not an integer",
                })
            })?;
            match (resolve_index(index, items.len()), rest.is_empty()) {
                (Some(at), true) => {
                    let mut items = items.clone();
                    items.insert(if after { at + 1 } else { at }, new_value.clone());
                    JsonbValue::Array(items)
                }
                (Some(at), false) => {
                    let mut items = items.clone();
                    items[at] = insert_path(&items[at], rest, new_value, after)?;
                    JsonbValue::Array(items)
                }
                (None, true) => {
                    let mut items = items.clone();
                    if index < 0 {
                        items.insert(0, new_value.clone());
                    } else {
                        items.push(new_value.clone());
                    }
                    JsonbValue::Array(items)
                }
                (None, false) => JsonbValue::Array(items.clone()),
            }
        }
        other => other.clone(),
    })
}

/// `jsonb_delete_path(target, path)`: the `#-` operator's function form. A path
/// that does not resolve leaves the target unchanged. An empty path is a no-op.
fn delete_path(target: &JsonbValue, path: &[String]) -> JsonbValue {
    let Some((step, rest)) = path.split_first() else {
        return target.clone();
    };
    match target {
        JsonbValue::Object(pairs) => {
            let Some(at) = pairs.iter().position(|(k, _)| k == step) else {
                return JsonbValue::Object(pairs.clone());
            };
            let mut pairs = pairs.clone();
            if rest.is_empty() {
                pairs.remove(at);
            } else {
                pairs[at].1 = delete_path(&pairs[at].1, rest);
            }
            JsonbValue::Object(pairs)
        }
        JsonbValue::Array(items) => {
            let Some(at) = step
                .parse::<i64>()
                .ok()
                .and_then(|index| resolve_index(index, items.len()))
            else {
                return JsonbValue::Array(items.clone());
            };
            let mut items = items.clone();
            if rest.is_empty() {
                items.remove(at);
            } else {
                items[at] = delete_path(&items[at], rest);
            }
            JsonbValue::Array(items)
        }
        other => other.clone(),
    }
}

/// `jsonb_build_object(k1, v1, k2, v2, …)`: an odd-length list is 22023, a NULL
/// key is 22023, a NULL value becomes the JSON `null` literal, and a duplicate
/// key keeps the last value through `JsonbValue::object_from_pairs`.
fn build_object(vals: &[Datum], ctx: &EvalCtx) -> Result<Datum, ExecError> {
    if !vals.len().is_multiple_of(2) {
        return Err(invalid_parameter(
            "argument list must have even number of elements",
        ));
    }
    let mut pairs = Vec::with_capacity(vals.len() / 2);
    for pair in vals.chunks_exact(2) {
        let key = match &pair[0] {
            Datum::Null => return Err(invalid_parameter("argument: key must not be null")),
            Datum::Jsonb(_) | Datum::Array(_) => {
                return Err(invalid_parameter(
                    "key value must be scalar, not array, composite, or json",
                ));
            }
            other => object_key_text(other, ctx)?,
        };
        pairs.push((key, to_jsonb(&pair[1], ctx)?));
    }
    Ok(Datum::Jsonb(JsonbValue::object_from_pairs(pairs)))
}

/// A scalar's spelling as a JSON object key.
///
/// PostgreSQL renders a key through the same conversion as a value and then
/// takes its unquoted text, so a key follows the *JSON* spelling rather than the
/// SQL one: `true` not `t`, and `2020-01-02T03:04:05` not the space-separated
/// form. The caller rejects container and NULL keys, so the arms below cover
/// every key that reaches here.
fn object_key_text(d: &Datum, ctx: &EvalCtx) -> Result<String, ExecError> {
    Ok(match to_jsonb(d, ctx)? {
        JsonbValue::String(s) => s,
        JsonbValue::Bool(b) => b.to_string(),
        JsonbValue::Number(n) => numeric::finite_to_text(&n),
        // `build_object` rejects null and container keys before calling this.
        other => other.to_text(),
    })
}

/// `to_jsonb(anyelement)`: the value's JSON rendering.
///
/// Numbers stay numbers and keep their scale, because `jsonb` is
/// numeric-backed. Strings and every stringly type become JSON strings, `jsonb`
/// is the identity, and an array becomes a JSON array. Date/time values use
/// PostgreSQL's JSON spelling, that is ISO 8601 with a `T` separator and an
/// `hh:mm` offset, not their SQL output.
fn to_jsonb(d: &Datum, ctx: &EvalCtx) -> Result<JsonbValue, ExecError> {
    Ok(match d {
        Datum::Null => JsonbValue::Null,
        Datum::Bool(b) => JsonbValue::Bool(*b),
        Datum::Int2(n) => JsonbValue::Number(BigDecimal::from(*n)),
        Datum::Int4(n) => JsonbValue::Number(BigDecimal::from(*n)),
        Datum::Int8(n) => JsonbValue::Number(BigDecimal::from(*n)),
        // JSON has no `NaN`/`Infinity` spelling either, so a special `numeric`
        // becomes a JSON string exactly like a non-finite float below.
        Datum::Numeric(n) => match n.as_finite() {
            Some(bd) => JsonbValue::Number(bd.clone()),
            None => JsonbValue::String(numeric::to_text(n)),
        },
        // JSON has no non-finite number, so PostgreSQL renders `NaN`/`Infinity`
        // as JSON *strings* rather than failing.
        Datum::Float4(f) if !f.is_finite() => JsonbValue::String(datum_text(d, ctx)),
        // PostgreSQL runs the value through its OUTPUT function and then
        // `numeric_in`, so a `real` keeps every digit `float4out` prints
        // (`to_jsonb(16777216::float4)` is `16777216`). That is deliberately
        // NOT the `::numeric` cast, which loses digits through `%.6g`.
        Datum::Float4(_) => JsonbValue::Number(
            numeric::parse_finite(&datum_text(d, ctx))
                .ok_or(ExecError::Type(crabka_pgtypes::TypeError::Overflow))?,
        ),
        Datum::Float8(f) if !f.is_finite() => JsonbValue::String(datum_text(d, ctx)),
        Datum::Float8(f) => JsonbValue::Number(
            numeric::from_f64(*f)
                .as_finite()
                .cloned()
                .ok_or(ExecError::Type(crabka_pgtypes::TypeError::Overflow))?,
        ),
        Datum::Jsonb(j) => j.clone(),
        Datum::Array(a) => JsonbValue::Array(
            a.elems
                .iter()
                .map(|e| to_jsonb(e, ctx))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        // A composite becomes a JSON object keyed by its field names — the same
        // rendering `row_to_json` produces, which is why that function is this
        // arm applied to its argument.
        Datum::Record(r) => JsonbValue::Object(record_pairs(r, ctx)?),
        Datum::Timestamp(_) | Datum::Timestamptz(_) => {
            JsonbValue::String(iso_8601_datetime(&datum_text(d, ctx)))
        }
        // `regclass` joins the stringly group, not the numbers: PostgreSQL's
        // `to_jsonb('pp'::regclass)` is `"pp"`, its output function's text.
        Datum::Text(_)
        | Datum::Point(_)
        | Datum::Path(_)
        | Datum::Date(_)
        | Datum::Time(_)
        | Datum::Timetz(_)
        | Datum::Interval(_)
        | Datum::Enum(_)
        | Datum::Regclass(_)
        | Datum::TsVector(_)
        | Datum::TsQuery(_)
        | Datum::Range(_)
        | Datum::Multirange(_)
        | Datum::Bytea(_) => JsonbValue::String(datum_text(d, ctx)),
    })
}

fn compact_json(value: &JsonbValue) -> String {
    let text = value.to_text();
    let mut quoted = false;
    let mut escaped = false;
    text.chars()
        .filter(|ch| {
            if quoted {
                if escaped {
                    escaped = false;
                } else if *ch == '\\' {
                    escaped = true;
                } else if *ch == '"' {
                    quoted = false;
                }
                true
            } else if *ch == '"' {
                quoted = true;
                true
            } else {
                *ch != ' '
            }
        })
        .collect()
}

/// A composite's fields as JSON object pairs, in declaration order.
///
/// `PostgreSQL` names an anonymous record's fields `f1`…`fn`. A named composite
/// uses its attribute names. Duplicate names are possible in a record built from
/// a join. `PostgreSQL` keeps both pairs in `row_to_json`, because it does not
/// de-duplicate a `json` value. `jsonb` keeps only the last, which is what
/// [`JsonbValue::Object`] does on construction.
pub(crate) fn record_pairs(
    r: &crabka_pgtypes::RecordValue,
    ctx: &EvalCtx,
) -> Result<Vec<(String, JsonbValue)>, ExecError> {
    r.values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let name = r
                .names
                .get(index)
                .cloned()
                .unwrap_or_else(|| format!("f{}", index + 1));
            Ok((name, to_jsonb(value, ctx)?))
        })
        .collect()
}

/// PostgreSQL's JSON date/time spelling from its SQL (ISO `DateStyle`) spelling:
/// `T` in place of the space, and a two-part `+hh:mm` offset.
fn iso_8601_datetime(sql_text: &str) -> String {
    let mut text = sql_text.replacen(' ', "T", 1);
    let Some(time_at) = text.find('T') else {
        return text;
    };
    // The only `+`/`-` after the `T` starts the zone offset (`13:45:06+02`).
    if let Some(sign_at) = text[time_at..].rfind(['+', '-']).map(|i| i + time_at)
        && text.len() - sign_at == 3
    {
        text.push_str(":00");
    }
    text
}

// ---- operator semantics ----

/// The `jsonb` operators, so the eval layer maps one `BinaryOp` onto one
/// [`JsonOp`] and gets both the static result type and the value semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonOp {
    /// `->`: field/element as `jsonb`.
    Get,
    /// `->>`: field/element as `text`.
    GetText,
    /// `#>`: path as `jsonb`.
    GetPath,
    /// `#>>`: path as `text`.
    GetPathText,
    /// `@>`: containment.
    Contains,
    /// `<@`: reverse containment.
    ContainedBy,
    /// `?`: key/element existence.
    KeyExists,
    /// `?|`: any key exists.
    KeyExistsAny,
    /// `?&`: all keys exist.
    KeyExistsAll,
    /// `||`: concatenation / object merge.
    Concat,
    /// `-`: delete a key, an index, or a set of keys.
    Delete,
    /// `@?`: the jsonpath finds at least one item.
    PathExists,
    /// `@@`: the jsonpath predicate, as a three-valued boolean.
    PathMatch,
}

impl JsonOp {
    /// The operator's SQL spelling (for error messages).
    pub(crate) fn spelling(self) -> &'static str {
        match self {
            JsonOp::Get => "->",
            JsonOp::GetText => "->>",
            JsonOp::GetPath => "#>",
            JsonOp::GetPathText => "#>>",
            JsonOp::Contains => "@>",
            JsonOp::ContainedBy => "<@",
            JsonOp::KeyExists => "?",
            JsonOp::KeyExistsAny => "?|",
            JsonOp::KeyExistsAll => "?&",
            JsonOp::Concat => "||",
            JsonOp::Delete => "-",
            JsonOp::PathExists => "@?",
            JsonOp::PathMatch => "@@",
        }
    }
}

/// The static result type of `left <op> right`, or `None` when the operand types
/// do not resolve the operator. The caller then reports 42883 at plan time.
pub(crate) fn json_operator_result_type(
    op: JsonOp,
    left: ColumnType,
    right: ColumnType,
) -> Option<ColumnType> {
    if left != ColumnType::Jsonb {
        return None;
    }
    let text_array = ColumnType::Array(ElemType::Text);
    let integral = matches!(right, ColumnType::Int4 | ColumnType::Int8);
    match op {
        JsonOp::Get if right.is_string() || integral => Some(ColumnType::Jsonb),
        JsonOp::GetText if right.is_string() || integral => Some(ColumnType::Text),
        JsonOp::GetPath if right == text_array => Some(ColumnType::Jsonb),
        JsonOp::GetPathText if right == text_array => Some(ColumnType::Text),
        JsonOp::Contains | JsonOp::ContainedBy if right == ColumnType::Jsonb => {
            Some(ColumnType::Bool)
        }
        JsonOp::KeyExists if right.is_string() => Some(ColumnType::Bool),
        JsonOp::KeyExistsAny | JsonOp::KeyExistsAll if right == text_array => {
            Some(ColumnType::Bool)
        }
        JsonOp::Concat if right == ColumnType::Jsonb => Some(ColumnType::Jsonb),
        JsonOp::Delete if right.is_string() || integral || right == text_array => {
            Some(ColumnType::Jsonb)
        }
        // The jsonpath operand is a `jsonpath` in PostgreSQL; crabka spells
        // that type `text` (see the module divergence note).
        JsonOp::PathExists | JsonOp::PathMatch if right.is_string() => Some(ColumnType::Bool),
        _ => None,
    }
}

/// Evaluate `left <op> right`. Every jsonb operator is strict: a SQL NULL on
/// either side yields SQL NULL.
pub(crate) fn eval_json_operator(
    op: JsonOp,
    left: &Datum,
    right: &Datum,
) -> Result<Datum, ExecError> {
    match op {
        JsonOp::Get => json_get(left, right),
        JsonOp::GetText => json_get_text(left, right),
        JsonOp::GetPath => json_get_path(left, right),
        JsonOp::GetPathText => json_get_path_text(left, right),
        JsonOp::Contains => json_contains(left, right),
        JsonOp::ContainedBy => json_contained_by(left, right),
        JsonOp::KeyExists => json_key_exists(left, right),
        JsonOp::KeyExistsAny => json_key_exists_any(left, right),
        JsonOp::KeyExistsAll => json_key_exists_all(left, right),
        JsonOp::Concat => json_concat(left, right),
        JsonOp::Delete => json_delete(left, right),
        JsonOp::PathExists => json_path_operator(left, right, false),
        JsonOp::PathMatch => json_path_operator(left, right, true),
    }
}

/// `jsonb -> text` / `jsonb -> integer`: an object field or an array element.
/// Negative indexes count from the end. A missing field or index is SQL NULL. A
/// JSON `null` value is the JSON null, which is *not* SQL NULL.
pub(crate) fn json_get(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let Some((value, key)) = operands(JsonOp::Get, left, right)? else {
        return Ok(Datum::Null);
    };
    Ok(match extract(value.as_ref(), &key) {
        Some(v) => Datum::Jsonb(v.clone()),
        None => Datum::Null,
    })
}

/// `jsonb ->> text` / `jsonb ->> integer`: the same extraction rendered as
/// `text`. A JSON `null` becomes SQL NULL, a JSON string loses its quotes, and
/// every other value keeps its canonical `jsonb` rendering.
pub(crate) fn json_get_text(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let Some((value, key)) = operands(JsonOp::GetText, left, right)? else {
        return Ok(Datum::Null);
    };
    Ok(match extract(value.as_ref(), &key) {
        Some(v) => jsonb_as_sql_text(v),
        None => Datum::Null,
    })
}

/// `jsonb #> text[]`: follow a path of object keys / array indexes.
pub(crate) fn json_get_path(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let Some((value, path)) = path_operands(JsonOp::GetPath, left, right)? else {
        return Ok(Datum::Null);
    };
    Ok(match navigate(value.as_ref(), &path) {
        Some(v) => Datum::Jsonb(v.clone()),
        None => Datum::Null,
    })
}

/// `jsonb #>> text[]`: [`json_get_path`] rendered as `text`.
pub(crate) fn json_get_path_text(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let Some((value, path)) = path_operands(JsonOp::GetPathText, left, right)? else {
        return Ok(Datum::Null);
    };
    Ok(match navigate(value.as_ref(), &path) {
        Some(v) => jsonb_as_sql_text(v),
        None => Datum::Null,
    })
}

/// `left @> right`: does `left` contain `right`?
pub(crate) fn json_contains(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let (Some(l), Some(r)) = (
        jsonb_or_null(left, JsonOp::Contains)?,
        jsonb_or_null(right, JsonOp::Contains)?,
    ) else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Bool(contains(l.as_ref(), r.as_ref())))
}

/// `left <@ right`: [`json_contains`] with the operands swapped.
pub(crate) fn json_contained_by(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let (Some(l), Some(r)) = (
        jsonb_or_null(left, JsonOp::ContainedBy)?,
        jsonb_or_null(right, JsonOp::ContainedBy)?,
    ) else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Bool(contains(r.as_ref(), l.as_ref())))
}

/// `jsonb ? text`: an object key, an array string element, or a string equal to
/// the operand.
pub(crate) fn json_key_exists(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let Some(value) = jsonb_or_null(left, JsonOp::KeyExists)? else {
        return Ok(Datum::Null);
    };
    let key = match right {
        Datum::Null => return Ok(Datum::Null),
        Datum::Text(s) => s.as_str(),
        other => return Err(operator_undefined(JsonOp::KeyExists, left, other)),
    };
    Ok(Datum::Bool(key_exists(value.as_ref(), key)))
}

/// `jsonb ?| text[]`: any of the keys exists. This operator skips NULL elements,
/// as PostgreSQL's `jsonb_exists_any` does.
pub(crate) fn json_key_exists_any(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let Some((value, keys)) = path_operands(JsonOp::KeyExistsAny, left, right)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Bool(
        keys.iter().flatten().any(|k| key_exists(value.as_ref(), k)),
    ))
}

/// `jsonb ?& text[]`: all of the keys exist. This operator skips NULL elements,
/// so an all-NULL array is true. This is PostgreSQL's `jsonb_exists_all`.
pub(crate) fn json_key_exists_all(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let Some((value, keys)) = path_operands(JsonOp::KeyExistsAll, left, right)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Bool(
        keys.iter().flatten().all(|k| key_exists(value.as_ref(), k)),
    ))
}

/// `jsonb || jsonb`: two objects merge with the right side winning, and two
/// arrays concatenate. Every other combination converts each non-array operand
/// into a one-element array and concatenates. This is PostgreSQL's documented
/// rule.
pub(crate) fn json_concat(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let (Some(l), Some(r)) = (
        jsonb_or_null(left, JsonOp::Concat)?,
        jsonb_or_null(right, JsonOp::Concat)?,
    ) else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Jsonb(match (l.as_ref(), r.as_ref()) {
        (JsonbValue::Object(a), JsonbValue::Object(b)) => {
            let mut pairs = a.clone();
            pairs.extend(b.iter().cloned());
            // `object_from_pairs` resolves duplicates last-wins, so appending the
            // right-hand pairs is exactly PostgreSQL's right-wins merge.
            JsonbValue::object_from_pairs(pairs)
        }
        (a, b) => {
            let mut items = as_array_items(a);
            items.extend(as_array_items(b));
            JsonbValue::Array(items)
        }
    }))
}

/// `jsonb - text` deletes a key or matching string elements. `jsonb - integer`
/// deletes an array element, counted from the end when negative.
/// `jsonb - text[]` deletes each key or element. A delete from a scalar, or a
/// delete by index from an object, is 22023, as in PostgreSQL.
pub(crate) fn json_delete(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    let Some(value) = jsonb_or_null(left, JsonOp::Delete)? else {
        return Ok(Datum::Null);
    };
    match right {
        Datum::Null => Ok(Datum::Null),
        Datum::Text(key) => delete_keys(value.as_ref(), std::slice::from_ref(key)),
        Datum::Array(a) if a.elem == ElemType::Text => {
            let keys: Vec<String> = a
                .elems
                .iter()
                .filter_map(|e| match e {
                    Datum::Text(s) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            delete_keys(value.as_ref(), &keys)
        }
        Datum::Int4(_) | Datum::Int8(_) => {
            let index = match right {
                Datum::Int4(n) => i64::from(*n),
                Datum::Int8(n) => *n,
                _ => unreachable!("guarded by the match arm"),
            };
            match value.as_ref() {
                JsonbValue::Array(items) => {
                    let Some(at) = resolve_index(index, items.len()) else {
                        return Ok(Datum::Jsonb(JsonbValue::Array(items.clone())));
                    };
                    let mut items = items.clone();
                    items.remove(at);
                    Ok(Datum::Jsonb(JsonbValue::Array(items)))
                }
                JsonbValue::Object(_) => Err(invalid_parameter(
                    "cannot delete from object using integer index",
                )),
                _ => Err(invalid_parameter("cannot delete from scalar")),
            }
        }
        other => Err(operator_undefined(JsonOp::Delete, left, other)),
    }
}

/// Delete each of `keys` from an object by key, or from an array by every string
/// element equal to it. A delete from a scalar is 22023.
fn delete_keys(value: &JsonbValue, keys: &[String]) -> Result<Datum, ExecError> {
    Ok(Datum::Jsonb(match value {
        JsonbValue::Object(pairs) => JsonbValue::Object(
            pairs
                .iter()
                .filter(|(k, _)| !keys.iter().any(|key| key == k))
                .cloned()
                .collect(),
        ),
        JsonbValue::Array(items) => JsonbValue::Array(
            items
                .iter()
                .filter(|item| match item {
                    JsonbValue::String(s) => !keys.iter().any(|key| key == s),
                    _ => true,
                })
                .cloned()
                .collect(),
        ),
        _ => return Err(invalid_parameter("cannot delete from scalar")),
    }))
}

/// `jsonb_set(target, path, new_value, create_if_missing)`.
///
/// A path that does not resolve leaves `target` unchanged. At the final step
/// this function creates a missing object key, and appends or prepends a missing
/// array index, only when `create` is set. An empty path returns `target`, and a
/// scalar target is 22023.
fn jsonb_set(
    target: &JsonbValue,
    path: &[String],
    new_value: &JsonbValue,
    create: bool,
) -> Result<Datum, ExecError> {
    if !matches!(target, JsonbValue::Object(_) | JsonbValue::Array(_)) {
        return Err(invalid_parameter("cannot set path in scalar"));
    }
    Ok(Datum::Jsonb(set_path(target, path, new_value, create)?))
}

fn set_path(
    target: &JsonbValue,
    path: &[String],
    new_value: &JsonbValue,
    create: bool,
) -> Result<JsonbValue, ExecError> {
    let Some((step, rest)) = path.split_first() else {
        return Ok(target.clone());
    };
    Ok(match target {
        JsonbValue::Object(pairs) => {
            let existing = pairs.iter().position(|(k, _)| k == step);
            match (existing, rest.is_empty()) {
                (Some(at), true) => {
                    let mut pairs = pairs.clone();
                    pairs[at].1 = new_value.clone();
                    JsonbValue::Object(pairs)
                }
                (Some(at), false) => {
                    let mut pairs = pairs.clone();
                    pairs[at].1 = set_path(&pairs[at].1, rest, new_value, create)?;
                    JsonbValue::Object(pairs)
                }
                // Only the *final* step is created; PostgreSQL never invents
                // intermediate levels.
                (None, true) if create => {
                    let mut pairs = pairs.clone();
                    pairs.push((step.clone(), new_value.clone()));
                    JsonbValue::object_from_pairs(pairs)
                }
                (None, _) => JsonbValue::Object(pairs.clone()),
            }
        }
        JsonbValue::Array(items) => {
            let index = step.parse::<i64>().map_err(|_| {
                // PostgreSQL reports a non-integer array subscript in a
                // `jsonb_set` path as 22P02, not as the 22023 the rest of the
                // jsonb domain errors use.
                ExecError::Type(TypeError::Domain {
                    sqlstate: "22P02",
                    message: "path element is not an integer",
                })
            })?;
            match (resolve_index(index, items.len()), rest.is_empty()) {
                (Some(at), true) => {
                    let mut items = items.clone();
                    items[at] = new_value.clone();
                    JsonbValue::Array(items)
                }
                (Some(at), false) => {
                    let mut items = items.clone();
                    items[at] = set_path(&items[at], rest, new_value, create)?;
                    JsonbValue::Array(items)
                }
                (None, true) if create => {
                    let mut items = items.clone();
                    if index < 0 {
                        items.insert(0, new_value.clone());
                    } else {
                        items.push(new_value.clone());
                    }
                    JsonbValue::Array(items)
                }
                (None, _) => JsonbValue::Array(items.clone()),
            }
        }
        // A scalar in the middle of a path simply does not resolve.
        other => other.clone(),
    })
}

// ---- the pure jsonb combinators ----

/// One `->`/`->>` subscript: an object key or an array index.
enum Subscript {
    Key(String),
    Index(i64),
}

/// Extract by subscript: object keys match by name, and array indexes count from
/// the end when negative. A subscript of the wrong shape for the value simply
/// misses. Those are a key on an array or a scalar, and an index on an object.
///
/// A jsonb SCALAR answers an integer subscript as if it were a one-element
/// array, so `'"s"'::jsonb -> 0` is `"s"`, `->> 0` is `s`, `-> -1` is `"s"` and
/// every other index misses. That is not a special case in PostgreSQL. A scalar
/// jsonb *is* stored as a one-element array, in a container that carries the
/// `JB_FSCALAR` flag, so `jsonb_array_element` walks it like any other array.
/// The path operators do not share the behavior. `jsonb_get_path` rejects a
/// scalar root outright, so `'"s"'::jsonb #> '{0}'` is NULL. See [`navigate`].
fn extract<'a>(value: &'a JsonbValue, subscript: &Subscript) -> Option<&'a JsonbValue> {
    match (value, subscript) {
        (JsonbValue::Object(_), Subscript::Key(key)) => value.object_get(key),
        (JsonbValue::Array(items), Subscript::Index(i)) => {
            items.get(resolve_index(*i, items.len())?)
        }
        (
            JsonbValue::Null | JsonbValue::Bool(_) | JsonbValue::Number(_) | JsonbValue::String(_),
            Subscript::Index(i),
        ) => resolve_index(*i, 1).map(|_| value),
        _ => None,
    }
}

/// Follow a `#>`/`jsonb_extract_path` path. A NULL path element makes the whole
/// lookup miss, which matches PostgreSQL's `array_contains_nulls` short circuit.
fn navigate<'a>(value: &'a JsonbValue, path: &[Option<String>]) -> Option<&'a JsonbValue> {
    let mut current = value;
    for step in path {
        let step = step.as_deref()?;
        current = match current {
            JsonbValue::Object(_) => current.object_get(step)?,
            JsonbValue::Array(items) => {
                let index = step.parse::<i64>().ok()?;
                items.get(resolve_index(index, items.len())?)?
            }
            _ => return None,
        };
    }
    Some(current)
}

/// A signed index resolved against a length. A negative index counts from the
/// end. Returns `None` when it falls outside the array.
fn resolve_index(index: i64, len: usize) -> Option<usize> {
    let len = i64::try_from(len).ok()?;
    let at = if index < 0 { len + index } else { index };
    if at < 0 || at >= len {
        return None;
    }
    usize::try_from(at).ok()
}

/// The `->>`/`#>>` rendering: the JSON `null` literal becomes SQL NULL, a JSON
/// string is unquoted, everything else keeps its canonical rendering.
fn jsonb_as_sql_text(value: &JsonbValue) -> Datum {
    match value {
        JsonbValue::Null => Datum::Null,
        JsonbValue::String(s) => Datum::Text(s.clone()),
        other => Datum::Text(other.to_text()),
    }
}

/// PostgreSQL's `jsonb_contains`: the two roots must agree on object-ness, then
/// containment is structural and recursive.
fn contains(lhs: &JsonbValue, rhs: &JsonbValue) -> bool {
    if matches!(lhs, JsonbValue::Object(_)) != matches!(rhs, JsonbValue::Object(_)) {
        return false;
    }
    deep_contains(lhs, rhs)
}

fn deep_contains(lhs: &JsonbValue, rhs: &JsonbValue) -> bool {
    match (lhs, rhs) {
        (JsonbValue::Object(_), JsonbValue::Object(rpairs)) => rpairs
            .iter()
            .all(|(k, rv)| lhs.object_get(k).is_some_and(|lv| contains_member(lv, rv))),
        (JsonbValue::Array(litems), JsonbValue::Array(ritems)) => ritems.iter().all(|rv| {
            if is_container(rv) {
                litems
                    .iter()
                    .any(|lv| same_container_kind(lv, rv) && deep_contains(lv, rv))
            } else {
                litems.iter().any(|lv| lv == rv)
            }
        }),
        // PostgreSQL's documented exception: an array contains a bare scalar.
        (JsonbValue::Array(litems), scalar) => litems.iter().any(|lv| lv == scalar),
        // ... which is not reciprocal: a raw scalar never contains an array.
        (_, JsonbValue::Array(_) | JsonbValue::Object(_)) => false,
        (scalar_l, scalar_r) => scalar_l == scalar_r,
    }
}

/// The object-member rule: a scalar member must be equal, a container member
/// must be a container of the same kind and be contained in turn.
fn contains_member(lv: &JsonbValue, rv: &JsonbValue) -> bool {
    if is_container(rv) {
        same_container_kind(lv, rv) && deep_contains(lv, rv)
    } else {
        lv == rv
    }
}

fn is_container(v: &JsonbValue) -> bool {
    matches!(v, JsonbValue::Array(_) | JsonbValue::Object(_))
}

fn same_container_kind(a: &JsonbValue, b: &JsonbValue) -> bool {
    matches!(
        (a, b),
        (JsonbValue::Array(_), JsonbValue::Array(_))
            | (JsonbValue::Object(_), JsonbValue::Object(_))
    )
}

/// `?`-family existence: an object key, an array string element, or a top-level
/// string equal to the operand.
fn key_exists(value: &JsonbValue, key: &str) -> bool {
    match value {
        JsonbValue::Object(_) => value.object_get(key).is_some(),
        JsonbValue::Array(items) => items
            .iter()
            .any(|i| matches!(i, JsonbValue::String(s) if s == key)),
        JsonbValue::String(s) => s == key,
        _ => false,
    }
}

/// The `||` view of a value as array items: an array contributes its elements,
/// anything else contributes itself as a single element.
fn as_array_items(value: &JsonbValue) -> Vec<JsonbValue> {
    match value {
        JsonbValue::Array(items) => items.clone(),
        other => vec![other.clone()],
    }
}

// ---- argument helpers ----

fn undefined_function(name: &str) -> ExecError {
    ExecError::UndefinedFunction(format!("function {name}(...) does not exist"))
}

fn operator_undefined(op: JsonOp, left: &Datum, right: &Datum) -> ExecError {
    ExecError::UndefinedFunction(format!(
        "operator does not exist: {} {} {}",
        type_name(left),
        op.spelling(),
        type_name(right)
    ))
}

/// A PostgreSQL `invalid_parameter_value` (22023) with a fixed message. This is
/// the SQLSTATE every jsonb domain error uses.
fn invalid_parameter(message: &'static str) -> ExecError {
    ExecError::Type(TypeError::Domain {
        sqlstate: "22023",
        message,
    })
}

/// [`invalid_parameter`] for a message that names a value, so it cannot be a
/// `&'static str`.
fn invalid_parameter_owned(message: String) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "22023",
        message,
    }
}

fn type_error(what: &str, got: &Datum) -> ExecError {
    ExecError::TypeMismatch(format!(
        "{what} does not accept an argument of type {}",
        type_name(got)
    ))
}

fn type_name(d: &Datum) -> &'static str {
    d.column_type().map_or("unknown", ColumnType::name)
}

/// The positional argument list. jsonb functions never accept `f(*)`.
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

/// A `jsonb` value argument. [`param_types`] has already converted an unadorned
/// literal, so a `text` value only reaches here from a path that never ran the
/// plan-time check. Parsing it keeps that path working, rather than failing on a
/// value PostgreSQL would have coerced.
fn jsonb_operand<'a>(d: &'a Datum, name: &str) -> Result<Cow<'a, JsonbValue>, ExecError> {
    match d {
        Datum::Jsonb(j) => Ok(Cow::Borrowed(j)),
        Datum::Text(s) => jsonb::parse(s).map(Cow::Owned).map_err(ExecError::Type),
        other => Err(type_error(name, other)),
    }
}

/// A `jsonb` operand of a binary operator. Returns `Ok(None)` for SQL NULL,
/// because every jsonb operator is strict, and an error for an operand of the
/// wrong type.
fn jsonb_or_null(d: &Datum, op: JsonOp) -> Result<Option<Cow<'_, JsonbValue>>, ExecError> {
    match d {
        Datum::Null => Ok(None),
        Datum::Jsonb(j) => Ok(Some(Cow::Borrowed(j))),
        Datum::Text(s) => jsonb::parse(s)
            .map(|v| Some(Cow::Owned(v)))
            .map_err(ExecError::Type),
        other => Err(ExecError::UndefinedFunction(format!(
            "operator does not exist: {} {} ...",
            type_name(other),
            op.spelling()
        ))),
    }
}

/// A `(jsonb, subscript)` operand pair, absent when either side is SQL NULL.
type SubscriptOperands<'a> = Option<(Cow<'a, JsonbValue>, Subscript)>;

/// A `(jsonb, text[])` operand pair, absent when either side is SQL NULL.
type PathOperands<'a> = Option<(Cow<'a, JsonbValue>, Vec<Option<String>>)>;

/// The `(jsonb, subscript)` operands of `->`/`->>`; `Ok(None)` when either is
/// SQL NULL.
fn operands<'a>(
    op: JsonOp,
    left: &'a Datum,
    right: &Datum,
) -> Result<SubscriptOperands<'a>, ExecError> {
    let Some(value) = jsonb_or_null(left, op)? else {
        return Ok(None);
    };
    let subscript = match right {
        Datum::Null => return Ok(None),
        Datum::Text(s) => Subscript::Key(s.clone()),
        Datum::Int4(n) => Subscript::Index(i64::from(*n)),
        Datum::Int8(n) => Subscript::Index(*n),
        other => return Err(operator_undefined(op, left, other)),
    };
    Ok(Some((value, subscript)))
}

/// The `(jsonb, text[])` operands of `#>`/`#>>`/`?|`/`?&`. Returns `Ok(None)`
/// when either is SQL NULL.
fn path_operands<'a>(
    op: JsonOp,
    left: &'a Datum,
    right: &Datum,
) -> Result<PathOperands<'a>, ExecError> {
    let Some(value) = jsonb_or_null(left, op)? else {
        return Ok(None);
    };
    let path = match right {
        Datum::Null => return Ok(None),
        Datum::Array(a) if a.elem == ElemType::Text => array_path(a),
        other => return Err(operator_undefined(op, left, other)),
    };
    Ok(Some((value, path)))
}

fn array_path(a: &ArrayValue) -> Vec<Option<String>> {
    a.elems
        .iter()
        .map(|e| match e {
            Datum::Text(s) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

/// A `text[]` function argument with no NULL elements, as `jsonb_set`'s path is.
fn text_path(d: &Datum, name: &str) -> Result<Vec<String>, ExecError> {
    match d {
        Datum::Array(a) if a.elem == ElemType::Text => a
            .elems
            .iter()
            .map(|e| match e {
                Datum::Text(s) => Ok(s.clone()),
                _ => Err(ExecError::Type(TypeError::Domain {
                    sqlstate: "22004",
                    message: "path element is null",
                })),
            })
            .collect(),
        other => Err(type_error(name, other)),
    }
}

fn text_arg<'a>(d: &'a Datum, name: &str) -> Result<&'a str, ExecError> {
    match d {
        Datum::Text(s) => Ok(s),
        other => Err(type_error(name, other)),
    }
}

/// A non-NULL datum's PostgreSQL text output, as JSON spells it.
///
/// JSON always uses the ISO date spelling whatever `DateStyle` says. The
/// separate `json_datetime_text` step turns it into the RFC 3339 form. But an
/// `interval` inside JSON *does* follow `IntervalStyle`, so
/// `to_json(interval '1 day')` is `"@ 1 day"` under `postgres_verbose`.
fn datum_text(d: &Datum, ctx: &EvalCtx) -> String {
    let style = crabka_pgtypes::encoding::OutputStyle {
        time_zone: &ctx.time_zone,
        date_style: crabka_pgtypes::datetime::DateStyle::Iso,
        date_order: ctx.date_order,
        interval_style: ctx.interval_style,
    };
    String::from_utf8(crabka_pgtypes::encoding::encode_text_in(d, style))
        .expect("a Datum's text encoding is always valid UTF-8")
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn ctx() -> EvalCtx {
        EvalCtx::test_default()
    }

    fn j(text: &str) -> Datum {
        Datum::Jsonb(jsonb::parse(text).expect("jsonb literal"))
    }

    fn t(s: &str) -> Datum {
        Datum::Text(s.to_string())
    }

    /// A JSON object key follows the JSON spelling, not the SQL one. Rows
    /// measured against PostgreSQL 18.4.
    #[test]
    fn object_keys_use_the_json_spelling_not_the_sql_one() {
        let stamp = crabka_pgtypes::datetime::parse_timestamp("2020-01-02 03:04:05")
            .expect("timestamp literal");
        for (key, want) in [
            (Datum::Bool(true), r#"{"true": "a"}"#),
            (Datum::Bool(false), r#"{"false": "a"}"#),
            (Datum::Int4(2), r#"{"2": "a"}"#),
            (
                Datum::Numeric(numeric::parse("1.50").expect("numeric literal")),
                r#"{"1.50": "a"}"#,
            ),
            (Datum::Timestamp(stamp), r#"{"2020-01-02T03:04:05": "a"}"#),
            (t("plain"), r#"{"plain": "a"}"#),
        ] {
            let got = build_object(&[key.clone(), t("a")], &ctx()).expect("build_object");
            let Datum::Jsonb(value) = got else {
                panic!("expected jsonb for key {key:?}")
            };
            assert!(value.to_text() == want, "key {key:?}");
        }
    }

    fn text_array(items: &[&str]) -> Datum {
        Datum::Array(ArrayValue::new(
            ElemType::Text,
            items
                .iter()
                .map(|s| Datum::Text((*s).to_string()))
                .collect(),
        ))
    }

    fn jsonb_expr(text: &str) -> Expr {
        Expr::Cast {
            expr: Box::new(Expr::StringLiteral(text.to_string())),
            ty: ColumnType::Jsonb,
        }
    }

    fn text_array_expr(items: &[&str]) -> Expr {
        Expr::Cast {
            expr: Box::new(Expr::StringLiteral(format!("{{{}}}", items.join(",")))),
            ty: ColumnType::Array(ElemType::Text),
        }
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
        eval_json(&func(name, args), &ctx, |e| {
            crate::eval::eval(e, &Scope::empty(), &[], &ctx)
        })
    }

    fn result_type(name: &str, args: Vec<Expr>) -> Result<ColumnType, ExecError> {
        json_func_result_type(&func(name, args), &Scope::empty())
    }

    fn sqlstate(e: ExecError) -> String {
        e.into_pg().code
    }

    /// A bare literal, which `PostgreSQL` leaves `unknown`.
    fn u(text: &str) -> Expr {
        Expr::StringLiteral(text.to_string())
    }

    /// `PostgreSQL` resolves an `unknown` literal argument against the parameter
    /// it is passed to, so `jsonb_set('{"a":1}', '{a}', '2')` passes a `jsonb`, a
    /// `text[]` and a `jsonb`. This codebase used to reject that call as 42883,
    /// because it typed every literal `text` on sight. Each row's type and value
    /// were taken from `PostgreSQL` 18.4.
    #[test]
    fn unknown_literal_arguments_adopt_their_parameter_type() {
        let doc = r#"{"a":{"b":1}}"#;
        let cases: [(&str, Vec<Expr>, ColumnType, Datum); 11] = [
            // The three jsonb_set parameters: jsonb, text[], jsonb (+ a bool).
            (
                "jsonb_set",
                vec![u(r#"{"a":1}"#), u("{a}"), u("2")],
                ColumnType::Jsonb,
                j(r#"{"a":2}"#),
            ),
            (
                "jsonb_set",
                vec![jsonb_expr(r#"{"a":1}"#), u("{a}"), u("2")],
                ColumnType::Jsonb,
                j(r#"{"a":2}"#),
            ),
            (
                "jsonb_set",
                vec![u(r#"{"a":1}"#), u("{b}"), u("2"), u("f")],
                ColumnType::Jsonb,
                j(r#"{"a":1}"#),
            ),
            (
                "jsonb_typeof",
                vec![u(r#"{"a":1}"#)],
                ColumnType::Text,
                t("object"),
            ),
            (
                "jsonb_array_length",
                vec![u("[1,2,3]")],
                ColumnType::Int4,
                Datum::Int4(3),
            ),
            (
                "jsonb_extract_path",
                vec![u(doc), u("a"), u("b")],
                ColumnType::Jsonb,
                j("1"),
            ),
            (
                "jsonb_extract_path_text",
                vec![u(doc), u("a"), u("b")],
                ColumnType::Text,
                t("1"),
            ),
            // The builders take `"any"`, where PostgreSQL resolves `unknown` to
            // `text` — so a bare literal becomes a JSON *string*, not an object.
            (
                "jsonb_build_object",
                vec![u("a"), u(r#"{"x":1}"#)],
                ColumnType::Jsonb,
                j(r#"{"a":"{\"x\":1}"}"#),
            ),
            (
                "jsonb_build_array",
                vec![u("a"), u(r#"{"x":1}"#)],
                ColumnType::Jsonb,
                j(r#"["a","{\"x\":1}"]"#),
            ),
            // A bare NULL is `unknown` too: it adopts jsonb, and the call is strict.
            (
                "jsonb_typeof",
                vec![Expr::NullLiteral],
                ColumnType::Text,
                Datum::Null,
            ),
            (
                "jsonb_set",
                vec![u(r#"{"a":1}"#), Expr::NullLiteral, u("2")],
                ColumnType::Jsonb,
                Datum::Null,
            ),
        ];
        for (name, args, ty, value) in cases {
            assert!(result_type(name, args.clone()).expect(name) == ty, "{name}");
            assert!(call(name, args).expect(name) == value, "{name}");
        }
    }

    /// The limits of that resolution, again as `PostgreSQL` 18.4 draws them.
    #[test]
    fn an_unknown_literal_argument_that_resolves_to_nothing_is_rejected() {
        // `to_jsonb(anyelement)` has no other argument to resolve its parameter
        // from, so an `unknown` literal is 42804 rather than a JSON string.
        for args in [vec![u("a")], vec![Expr::NullLiteral]] {
            assert!(
                sqlstate(result_type("to_jsonb", args.clone()).expect_err("to_jsonb")) == "42804"
            );
            assert!(sqlstate(call("to_jsonb", args).expect_err("to_jsonb")) == "42804");
        }
        // A genuinely `text`-typed argument is NOT an `unknown` literal, and
        // PostgreSQL has no jsonb function taking text: it needs an explicit cast.
        let as_text = Expr::Cast {
            expr: Box::new(u("{}")),
            ty: ColumnType::Text,
        };
        assert!(sqlstate(result_type("jsonb_typeof", vec![as_text]).expect_err("text")) == "42883");
        // The adopted literal is parsed as its new type, so malformed input is
        // 22P02 — for the jsonb parameters and for the `text[]` path alike.
        assert!(sqlstate(call("jsonb_typeof", vec![u("notjson")]).expect_err("json")) == "22P02");
        assert!(
            sqlstate(call("jsonb_set", vec![u(r#"{"a":1}"#), u("a"), u("2")]).expect_err("path"))
                == "22P02"
        );
    }

    /// A jsonb SCALAR is stored as a one-element array, because `PostgreSQL`
    /// flags the container `JB_FSCALAR`. So `->`/`->>` with an integer subscript
    /// walk it like one: `'"quoted"'::jsonb ->> 0` is `quoted`, not NULL. An
    /// OBJECT never answers an integer subscript, and the path operators reject
    /// a scalar root outright. Every row was taken from `PostgreSQL` 18.4.
    #[test]
    fn a_scalar_answers_an_integer_subscript_like_a_one_element_array() {
        let quoted = r#""quoted""#;
        let cases: [(&str, i32, Datum, Datum); 12] = [
            (quoted, 0, j(quoted), t("quoted")),
            (quoted, -1, j(quoted), t("quoted")),
            (quoted, 1, Datum::Null, Datum::Null),
            (quoted, -2, Datum::Null, Datum::Null),
            ("5", 0, j("5"), t("5")),
            ("true", 0, j("true"), t("true")),
            // The jsonb `null` is a value: `->` yields it, `->>` renders SQL NULL.
            ("null", 0, j("null"), Datum::Null),
            // An object misses on every integer subscript.
            (r#"{"a":1}"#, 0, Datum::Null, Datum::Null),
            (r#"{"a":1}"#, -1, Datum::Null, Datum::Null),
            // Arrays are unchanged.
            ("[1,2,3]", -1, j("3"), t("3")),
            ("[1,2,3]", 2, j("3"), t("3")),
            ("[1,2,3]", -4, Datum::Null, Datum::Null),
        ];
        for (value, index, get, get_text) in cases {
            let subscript = Datum::Int4(index);
            assert!(
                json_get(&j(value), &subscript).expect("->") == get,
                "{value} -> {index}"
            );
            assert!(
                json_get_text(&j(value), &subscript).expect("->>") == get_text,
                "{value} ->> {index}"
            );
        }
        // A KEY subscript on a scalar still misses.
        assert!(json_get(&j(quoted), &t("k")).expect("->") == Datum::Null);
        assert!(json_get_text(&j(quoted), &t("k")).expect("->>") == Datum::Null);
        // `#>` does not share the behavior: a scalar root answers only the empty
        // path, with itself.
        assert!(json_get_path(&j(quoted), &text_array(&["0"])).expect("#>") == Datum::Null);
        assert!(json_get_path_text(&j(quoted), &text_array(&["0"])).expect("#>>") == Datum::Null);
        assert!(json_get_path(&j(quoted), &text_array(&[])).expect("#>") == j(quoted));
        assert!(json_get_path_text(&j(quoted), &text_array(&[])).expect("#>>") == t("quoted"));
    }

    #[test]
    fn classifier_covers_the_family_only() {
        for name in [
            "jsonb_build_object",
            "jsonb_build_array",
            "jsonb_array_length",
            "jsonb_typeof",
            "jsonb_extract_path",
            "jsonb_extract_path_text",
            "jsonb_set",
            "to_jsonb",
            // The `json_*` spellings resolve to the same implementations,
            // because `json` is stored as `jsonb`.
            "json_build_object",
            "to_json",
            "jsonb_path_query_array",
            "jsonb_path_exists_tz",
        ] {
            assert!(is_json_func(name));
        }
        // The aggregates and the set-returning functions have their own
        // registries, and an unrelated name is not claimed here.
        assert!(!is_json_func("jsonb_agg"));
        assert!(!is_json_func("jsonb_each"));
        assert!(!is_json_func("jsonb_path_query"));
        assert!(!is_json_func("json_nope"));
    }

    #[test]
    fn arrow_operators_separate_jsonb_null_from_sql_null() {
        let obj = j(r#"{"a": null, "b": "x", "c": 1}"#);
        // `->` on a JSON null yields the JSON null, which is not SQL NULL;
        // `->>` on it yields SQL NULL. A missing key is SQL NULL for both.
        assert!(json_get(&obj, &t("a")).expect("->") == Datum::Jsonb(JsonbValue::Null));
        assert!(json_get_text(&obj, &t("a")).expect("->>") == Datum::Null);
        assert!(json_get(&obj, &t("missing")).expect("->") == Datum::Null);
        assert!(json_get_text(&obj, &t("missing")).expect("->>") == Datum::Null);
        // A string loses its quotes through `->>` but keeps them through `->`.
        assert!(json_get(&obj, &t("b")).expect("->") == j(r#""x""#));
        assert!(json_get_text(&obj, &t("b")).expect("->>") == t("x"));
        assert!(json_get_text(&obj, &t("c")).expect("->>") == t("1"));
        // A jsonb `null` value is a value: `'null'::jsonb` is not SQL NULL.
        assert!(
            json_get(&j("[null]"), &Datum::Int4(0)).expect("->") == Datum::Jsonb(JsonbValue::Null)
        );
        // The operators are strict on SQL NULL.
        assert!(json_get(&Datum::Null, &t("a")).expect("->") == Datum::Null);
        assert!(json_get(&obj, &Datum::Null).expect("->") == Datum::Null);
    }

    #[test]
    fn arrow_indexes_count_from_the_end_and_respect_the_container() {
        let arr = j("[10, 20, 30]");
        let cases: [(i32, Datum); 6] = [
            (0, j("10")),
            (2, j("30")),
            (-1, j("30")),
            (-3, j("10")),
            (3, Datum::Null),
            (-4, Datum::Null),
        ];
        for (index, expected) in cases {
            assert!(json_get(&arr, &Datum::Int4(index)).expect("->") == expected);
        }
        // A key on an array and an index on an object both miss.
        assert!(json_get(&arr, &t("0")).expect("->") == Datum::Null);
        assert!(json_get(&j(r#"{"a": 1}"#), &Datum::Int4(0)).expect("->") == Datum::Null);
        // A scalar behaves as a one-element array — see
        // `a_scalar_answers_an_integer_subscript_like_a_one_element_array`.
        assert!(json_get(&j("1"), &Datum::Int4(0)).expect("->") == j("1"));
        assert!(json_get_text(&arr, &Datum::Int4(-1)).expect("->>") == t("30"));
    }

    #[test]
    fn path_operators_walk_objects_and_arrays() {
        let value = j(r#"{"a": {"b": [1, {"c": "deep"}]}, "n": null}"#);
        assert!(
            json_get_path(&value, &text_array(&["a", "b", "1", "c"])).expect("#>")
                == j(r#""deep""#)
        );
        assert!(
            json_get_path_text(&value, &text_array(&["a", "b", "1", "c"])).expect("#>>")
                == t("deep")
        );
        // A negative index works in a path too.
        assert!(json_get_path(&value, &text_array(&["a", "b", "-2"])).expect("#>") == j("1"));
        // A missing step, a non-integer array step and a scalar step all miss.
        assert!(json_get_path(&value, &text_array(&["a", "z"])).expect("#>") == Datum::Null);
        assert!(json_get_path(&value, &text_array(&["a", "b", "x"])).expect("#>") == Datum::Null);
        assert!(json_get_path(&value, &text_array(&["n", "x"])).expect("#>") == Datum::Null);
        // The JSON null / SQL NULL split holds for `#>` vs `#>>`.
        assert!(
            json_get_path(&value, &text_array(&["n"])).expect("#>")
                == Datum::Jsonb(JsonbValue::Null)
        );
        assert!(json_get_path_text(&value, &text_array(&["n"])).expect("#>>") == Datum::Null);
        // An empty path is the value itself.
        assert!(json_get_path(&j("[1]"), &text_array(&[])).expect("#>") == j("[1]"));
        // A NULL path element makes the whole lookup NULL.
        let with_null = Datum::Array(ArrayValue::new(
            ElemType::Text,
            vec![Datum::Text("a".into()), Datum::Null],
        ));
        assert!(json_get_path(&value, &with_null).expect("#>") == Datum::Null);
    }

    #[test]
    fn containment_is_recursive_with_the_raw_scalar_exception() {
        // (left, right, left @> right)
        let cases: [(&str, &str, bool); 14] = [
            (r#"{"a": 1, "b": 2}"#, r#"{"a": 1}"#, true),
            (r#"{"a": 1}"#, r#"{"a": 1, "b": 2}"#, false),
            (r#"{"a": 1}"#, r#"{"a": 2}"#, false),
            // Numbers compare by value, not by scale.
            (r#"{"a": 1.0}"#, r#"{"a": 1.00}"#, true),
            (r#"{"foo": {"bar": "baz"}}"#, r#"{"foo": {}}"#, true),
            (
                r#"{"foo": {"bar": "baz", "x": 1}}"#,
                r#"{"foo": {"bar": "baz"}}"#,
                true,
            ),
            ("[1, 2, 3]", "[1, 3]", true),
            ("[1, 2, 3]", "[]", true),
            ("[1, 2, 3]", "[4]", false),
            // The documented exception: an array contains a bare scalar ...
            (r#"["foo", "bar"]"#, r#""bar""#, true),
            // ... and it is not reciprocal.
            (r#""bar""#, r#"["bar"]"#, false),
            // Object-ness must agree at the top level.
            (r#"{"a": 1}"#, "[1]", false),
            ("[1]", r#"{"a": 1}"#, false),
            // The exception does not reach inside an object member.
            (r#"{"a": [1, 2]}"#, r#"{"a": 1}"#, false),
        ];
        for (left, right, expected) in cases {
            let (l, r) = (j(left), j(right));
            assert!(json_contains(&l, &r).expect("@>") == Datum::Bool(expected));
            assert!(json_contained_by(&r, &l).expect("<@") == Datum::Bool(expected));
        }
        // Nested arrays match by containment, not by identity.
        assert!(json_contains(&j("[[1, 2, 3]]"), &j("[[1, 3]]")).expect("@>") == Datum::Bool(true));
        assert!(json_contains(&j("[[1, 2]]"), &j("[1]")).expect("@>") == Datum::Bool(false));
        // Strict on SQL NULL.
        assert!(json_contains(&Datum::Null, &j("1")).expect("@>") == Datum::Null);
    }

    #[test]
    fn existence_operators_see_keys_and_string_elements() {
        let obj = j(r#"{"a": 1, "b": null}"#);
        let arr = j(r#"["a", 1, null]"#);
        assert!(json_key_exists(&obj, &t("a")).expect("?") == Datum::Bool(true));
        // A key whose value is JSON null still exists.
        assert!(json_key_exists(&obj, &t("b")).expect("?") == Datum::Bool(true));
        assert!(json_key_exists(&obj, &t("z")).expect("?") == Datum::Bool(false));
        // Arrays match string elements; a scalar string matches itself.
        assert!(json_key_exists(&arr, &t("a")).expect("?") == Datum::Bool(true));
        assert!(json_key_exists(&arr, &t("1")).expect("?") == Datum::Bool(false));
        assert!(json_key_exists(&j(r#""a""#), &t("a")).expect("?") == Datum::Bool(true));
        assert!(json_key_exists(&j("1"), &t("1")).expect("?") == Datum::Bool(false));

        assert!(
            json_key_exists_any(&obj, &text_array(&["z", "a"])).expect("?|") == Datum::Bool(true)
        );
        assert!(json_key_exists_any(&obj, &text_array(&["z"])).expect("?|") == Datum::Bool(false));
        assert!(
            json_key_exists_all(&obj, &text_array(&["a", "b"])).expect("?&") == Datum::Bool(true)
        );
        assert!(
            json_key_exists_all(&obj, &text_array(&["a", "z"])).expect("?&") == Datum::Bool(false)
        );
        // An empty key list: nothing to find (?|), nothing missing (?&).
        assert!(json_key_exists_any(&obj, &text_array(&[])).expect("?|") == Datum::Bool(false));
        assert!(json_key_exists_all(&obj, &text_array(&[])).expect("?&") == Datum::Bool(true));
        // NULL elements are skipped, as in PostgreSQL.
        let with_null = Datum::Array(ArrayValue::new(
            ElemType::Text,
            vec![Datum::Null, Datum::Text("a".into())],
        ));
        assert!(json_key_exists_any(&obj, &with_null).expect("?|") == Datum::Bool(true));
        assert!(json_key_exists_all(&obj, &with_null).expect("?&") == Datum::Bool(true));
        assert!(json_key_exists(&obj, &Datum::Null).expect("?") == Datum::Null);
    }

    #[test]
    fn concatenation_merges_objects_right_wins_and_arrays_append() {
        let cases: [(&str, &str, &str); 8] = [
            (
                r#"{"a": 1, "b": 2}"#,
                r#"{"b": 3, "c": 4}"#,
                r#"{"a": 1, "b": 3, "c": 4}"#,
            ),
            ("{}", r#"{"a": 1}"#, r#"{"a": 1}"#),
            ("[1, 2]", "[3]", "[1, 2, 3]"),
            ("[]", "[1]", "[1]"),
            // Every other combination wraps the non-array side.
            ("[1, 2]", "3", "[1, 2, 3]"),
            ("1", "[2, 3]", "[1, 2, 3]"),
            (r#"{"a": 1}"#, "[1]", r#"[{"a": 1}, 1]"#),
            (r#""x""#, r#""y""#, r#"["x", "y"]"#),
        ];
        for (left, right, expected) in cases {
            assert!(json_concat(&j(left), &j(right)).expect("||") == j(expected));
        }
        assert!(json_concat(&Datum::Null, &j("[1]")).expect("||") == Datum::Null);
    }

    #[test]
    fn deletion_by_key_index_and_path() {
        let obj = j(r#"{"a": 1, "b": 2}"#);
        assert!(json_delete(&obj, &t("a")).expect("-") == j(r#"{"b": 2}"#));
        // Deleting an absent key is a no-op.
        assert!(json_delete(&obj, &t("z")).expect("-") == obj);
        // On an array, a text operand deletes matching string elements.
        assert!(json_delete(&j(r#"["a", "b", "a", 1]"#), &t("a")).expect("-") == j(r#"["b", 1]"#));
        // Integer deletion counts from the end when negative; out of range is a no-op.
        let arr = j("[1, 2, 3]");
        assert!(json_delete(&arr, &Datum::Int4(0)).expect("-") == j("[2, 3]"));
        assert!(json_delete(&arr, &Datum::Int4(-1)).expect("-") == j("[1, 2]"));
        assert!(json_delete(&arr, &Datum::Int4(9)).expect("-") == arr);
        // A text[] operand deletes every listed key.
        assert!(
            json_delete(&j(r#"{"a": 1, "b": 2, "c": 3}"#), &text_array(&["a", "c"])).expect("-")
                == j(r#"{"b": 2}"#)
        );
        // Deleting from a scalar, or by index from an object, is 22023.
        assert!(sqlstate(json_delete(&j("1"), &t("a")).expect_err("scalar")) == "22023");
        assert!(sqlstate(json_delete(&obj, &Datum::Int4(0)).expect_err("object")) == "22023");
        assert!(json_delete(&Datum::Null, &t("a")).expect("-") == Datum::Null);
        assert!(json_delete(&obj, &Datum::Null).expect("-") == Datum::Null);
    }

    #[test]
    fn operator_result_types_resolve_only_the_defined_operands() {
        let jsonb = ColumnType::Jsonb;
        let text = ColumnType::Text;
        let int4 = ColumnType::Int4;
        let text_array = ColumnType::Array(ElemType::Text);
        let cases: [(JsonOp, ColumnType, ColumnType, Option<ColumnType>); 13] = [
            (JsonOp::Get, jsonb, text, Some(jsonb)),
            (JsonOp::Get, jsonb, int4, Some(jsonb)),
            (JsonOp::GetText, jsonb, text, Some(text)),
            (JsonOp::GetPath, jsonb, text_array, Some(jsonb)),
            (JsonOp::GetPathText, jsonb, text_array, Some(text)),
            (JsonOp::Contains, jsonb, jsonb, Some(ColumnType::Bool)),
            (JsonOp::ContainedBy, jsonb, jsonb, Some(ColumnType::Bool)),
            (JsonOp::KeyExists, jsonb, text, Some(ColumnType::Bool)),
            (
                JsonOp::KeyExistsAny,
                jsonb,
                text_array,
                Some(ColumnType::Bool),
            ),
            (
                JsonOp::KeyExistsAll,
                jsonb,
                text_array,
                Some(ColumnType::Bool),
            ),
            (JsonOp::Concat, jsonb, jsonb, Some(jsonb)),
            (JsonOp::Delete, jsonb, text_array, Some(jsonb)),
            // Undefined combinations resolve to nothing (the caller reports 42883).
            (JsonOp::Contains, jsonb, text, None),
        ];
        for (op, left, right, expected) in cases {
            assert!(json_operator_result_type(op, left, right) == expected);
        }
        assert!(json_operator_result_type(JsonOp::Get, text, text).is_none());
        assert!(json_operator_result_type(JsonOp::GetPath, jsonb, text).is_none());
        // The dispatcher and the individual helpers agree.
        assert!(
            eval_json_operator(JsonOp::GetText, &j(r#"{"a": 1}"#), &t("a")).expect("->>") == t("1")
        );
    }

    #[test]
    fn build_functions_embed_nulls_as_json_nulls() {
        assert!(
            call(
                "jsonb_build_object",
                vec![
                    Expr::StringLiteral("a".into()),
                    Expr::IntLiteral("1".into()),
                    Expr::StringLiteral("b".into()),
                    Expr::NullLiteral,
                ]
            )
            .expect("build")
                == j(r#"{"a": 1, "b": null}"#)
        );
        // A duplicate key keeps the last value.
        assert!(
            call(
                "jsonb_build_object",
                vec![
                    Expr::StringLiteral("a".into()),
                    Expr::IntLiteral("1".into()),
                    Expr::StringLiteral("a".into()),
                    Expr::IntLiteral("2".into()),
                ]
            )
            .expect("build")
                == j(r#"{"a": 2}"#)
        );
        // An odd argument count is 22023, a NULL key too.
        assert!(
            sqlstate(
                call("jsonb_build_object", vec![Expr::StringLiteral("a".into())]).expect_err("odd")
            ) == "22023"
        );
        assert!(
            sqlstate(
                call(
                    "jsonb_build_object",
                    vec![Expr::NullLiteral, Expr::IntLiteral("1".into())]
                )
                .expect_err("null key")
            ) == "22023"
        );
        assert!(
            call(
                "jsonb_build_array",
                vec![
                    Expr::IntLiteral("1".into()),
                    Expr::StringLiteral("x".into()),
                    Expr::NullLiteral,
                    Expr::BoolLiteral(true),
                ]
            )
            .expect("build")
                == j(r#"[1, "x", null, true]"#)
        );
        assert!(call("jsonb_build_array", vec![]).expect("build") == j("[]"));
    }

    #[test]
    fn typeof_and_array_length_report_the_json_shape() {
        let cases: [(&str, &str); 6] = [
            ("{}", "object"),
            ("[]", "array"),
            ("null", "null"),
            ("true", "boolean"),
            ("1.5", "number"),
            (r#""s""#, "string"),
        ];
        for (value, expected) in cases {
            assert!(
                call("jsonb_typeof", vec![jsonb_expr(value)]).expect("typeof")
                    == Datum::Text(expected.to_string())
            );
        }
        // `'null'::jsonb` is a value, so jsonb_typeof reports it; SQL NULL is NULL.
        assert!(call("jsonb_typeof", vec![Expr::NullLiteral]).expect("typeof") == Datum::Null);
        assert!(
            call("jsonb_array_length", vec![jsonb_expr("[1, 2, 3]")]).expect("len")
                == Datum::Int4(3)
        );
        assert!(call("jsonb_array_length", vec![jsonb_expr("[]")]).expect("len") == Datum::Int4(0));
        assert!(
            sqlstate(
                call("jsonb_array_length", vec![jsonb_expr(r#"{"a": 1}"#)]).expect_err("object")
            ) == "22023"
        );
        assert!(
            sqlstate(call("jsonb_array_length", vec![jsonb_expr("1")]).expect_err("scalar"))
                == "22023"
        );
    }

    #[test]
    fn extract_path_functions_match_their_operators() {
        let value = || jsonb_expr(r#"{"a": {"b": [1, 2]}, "n": null}"#);
        assert!(
            call(
                "jsonb_extract_path",
                vec![
                    value(),
                    Expr::StringLiteral("a".into()),
                    Expr::StringLiteral("b".into())
                ]
            )
            .expect("path")
                == j("[1, 2]")
        );
        assert!(
            call(
                "jsonb_extract_path_text",
                vec![
                    value(),
                    Expr::StringLiteral("a".into()),
                    Expr::StringLiteral("b".into()),
                    Expr::StringLiteral("1".into()),
                ]
            )
            .expect("path")
                == t("2")
        );
        // A JSON null through the _text form is SQL NULL.
        assert!(
            call(
                "jsonb_extract_path_text",
                vec![value(), Expr::StringLiteral("n".into())]
            )
            .expect("path")
                == Datum::Null
        );
        // Strict: a NULL path argument is SQL NULL.
        assert!(
            call("jsonb_extract_path", vec![value(), Expr::NullLiteral]).expect("path")
                == Datum::Null
        );
    }

    #[test]
    fn set_replaces_creates_and_leaves_missing_paths_alone() {
        let target = || jsonb_expr(r#"{"a": {"b": [1, 2]}}"#);
        let set = |path: &[&str], value: &str, create: Option<bool>| {
            let mut args = vec![target(), text_array_expr(path), jsonb_expr(value)];
            if let Some(create) = create {
                args.push(Expr::BoolLiteral(create));
            }
            call("jsonb_set", args).expect("set")
        };
        assert!(set(&["a", "b", "0"], "9", None) == j(r#"{"a": {"b": [9, 2]}}"#));
        // A negative index counts from the end.
        assert!(set(&["a", "b", "-1"], "9", None) == j(r#"{"a": {"b": [1, 9]}}"#));
        // A missing final key is created only when create_if_missing is set.
        assert!(set(&["a", "c"], "9", None) == j(r#"{"a": {"b": [1, 2], "c": 9}}"#));
        assert!(set(&["a", "c"], "9", Some(false)) == j(r#"{"a": {"b": [1, 2]}}"#));
        // An out-of-range index appends (or prepends, when negative).
        assert!(set(&["a", "b", "5"], "9", None) == j(r#"{"a": {"b": [1, 2, 9]}}"#));
        assert!(set(&["a", "b", "-5"], "9", None) == j(r#"{"a": {"b": [9, 1, 2]}}"#));
        // Intermediate levels are never invented.
        assert!(set(&["z", "y"], "9", None) == j(r#"{"a": {"b": [1, 2]}}"#));
        // An empty path returns the target unchanged.
        assert!(set(&[], "9", None) == j(r#"{"a": {"b": [1, 2]}}"#));
        // Strict, and 22023 on a scalar target.
        assert!(
            call(
                "jsonb_set",
                vec![target(), text_array_expr(&["a"]), Expr::NullLiteral]
            )
            .expect("set")
                == Datum::Null
        );
        assert!(
            sqlstate(
                call(
                    "jsonb_set",
                    vec![jsonb_expr("1"), text_array_expr(&["a"]), jsonb_expr("9")]
                )
                .expect_err("scalar")
            ) == "22023"
        );
        // A non-integer array subscript in the path is 22P02.
        assert!(
            sqlstate(
                call(
                    "jsonb_set",
                    vec![
                        jsonb_expr(r#"{"a": [1]}"#),
                        text_array_expr(&["a", "x"]),
                        jsonb_expr("9")
                    ]
                )
                .expect_err("path")
            ) == "22P02"
        );
    }

    #[test]
    fn to_jsonb_renders_every_datum_kind() {
        let ctx = ctx();
        let cases: [(Datum, &str); 8] = [
            (Datum::Bool(true), "true"),
            (Datum::Int4(1), "1"),
            (Datum::Int8(-2), "-2"),
            (Datum::Text("x\"y".into()), r#""x\"y""#),
            (
                Datum::Numeric(numeric::parse("1.50").expect("numeric")),
                "1.50",
            ),
            (Datum::Float8(1.5), "1.5"),
            (j(r#"{"a": 1}"#), r#"{"a": 1}"#),
            (
                Datum::Array(ArrayValue::new(
                    ElemType::Int4,
                    vec![Datum::Int4(1), Datum::Null],
                )),
                "[1, null]",
            ),
        ];
        for (datum, expected) in cases {
            assert!(to_jsonb(&datum, &ctx).expect("to_jsonb").to_text() == expected);
        }
        // Date/time values use the JSON spelling, not the SQL one.
        assert!(
            to_jsonb(
                &Datum::Timestamp(jiff::civil::datetime(2024, 1, 15, 13, 45, 6, 0)),
                &ctx
            )
            .expect("ts")
            .to_text()
                == r#""2024-01-15T13:45:06""#
        );
        assert!(
            to_jsonb(
                &Datum::Timestamptz("2024-01-15T13:45:06Z".parse().expect("ts")),
                &ctx
            )
            .expect("tstz")
            .to_text()
                == r#""2024-01-15T13:45:06+00:00""#
        );
        assert!(
            to_jsonb(&Datum::Date(jiff::civil::date(2024, 1, 15)), &ctx)
                .expect("date")
                .to_text()
                == r#""2024-01-15""#
        );
        // JSON has no non-finite number, so PostgreSQL renders one as a string.
        assert!(
            to_jsonb(&Datum::Float8(f64::NAN), &ctx)
                .expect("nan")
                .to_text()
                == r#""NaN""#
        );
        assert!(
            to_jsonb(&Datum::Float8(f64::INFINITY), &ctx)
                .expect("inf")
                .to_text()
                == r#""Infinity""#
        );
    }

    #[test]
    fn result_types_for_row_description() {
        let cases: [(&str, Vec<Expr>, ColumnType); 6] = [
            (
                "jsonb_build_object",
                vec![
                    Expr::StringLiteral("a".into()),
                    Expr::IntLiteral("1".into()),
                ],
                ColumnType::Jsonb,
            ),
            ("jsonb_build_array", vec![], ColumnType::Jsonb),
            (
                "jsonb_array_length",
                vec![jsonb_expr("[1]")],
                ColumnType::Int4,
            ),
            ("jsonb_typeof", vec![jsonb_expr("[1]")], ColumnType::Text),
            (
                "jsonb_extract_path_text",
                vec![jsonb_expr("[1]"), Expr::StringLiteral("0".into())],
                ColumnType::Text,
            ),
            (
                "to_jsonb",
                vec![Expr::IntLiteral("1".into())],
                ColumnType::Jsonb,
            ),
        ];
        for (name, args, expected) in cases {
            assert!(result_type(name, args).expect(name) == expected);
        }
        // Arity and argument types are 42883 at plan time.
        assert!(
            sqlstate(
                result_type("jsonb_typeof", vec![jsonb_expr("[1]"), jsonb_expr("[1]")])
                    .expect_err("arity")
            ) == "42883"
        );
        assert!(
            sqlstate(
                result_type("jsonb_typeof", vec![Expr::IntLiteral("1".into())]).expect_err("type")
            ) == "42883"
        );
    }

    /// A jsonb subscript *read* is `navigate` over one text path element. A
    /// missing key, an out-of-range index, a NULL subscript and a scalar
    /// container are all SQL NULL. An integer subscript becomes its decimal
    /// text, so `('{"0": 1}'::jsonb)[0]` finds the key `"0"`. Rows measured
    /// against PostgreSQL 18.4.
    #[test]
    fn jsonb_subscript_reads_by_key_or_index() {
        let cases: &[(&str, Datum, Datum)] = &[
            (r#"{"a": 1}"#, t("a"), j("1")),
            (r#"{"a": 1}"#, t("nope"), Datum::Null),
            (r#"{"a": 1}"#, Datum::Int4(0), Datum::Null),
            (r#"{"0": 1}"#, Datum::Int4(0), j("1")),
            (r#"{"a": 1}"#, Datum::Null, Datum::Null),
            (r#"[1, "2", null]"#, Datum::Int4(0), j("1")),
            (r#"[1, "2", null]"#, Datum::Int4(2), j("null")),
            (r#"[1, "2", null]"#, Datum::Int4(3), Datum::Null),
            (r#"[1, "2", null]"#, Datum::Int4(-2), j(r#""2""#)),
            (r#"[1, "2", null]"#, t("1"), j(r#""2""#)),
            (r#"[1, "2", null]"#, t("-1"), j("null")),
            (r#"[1, "2", null]"#, t("a"), Datum::Null),
            ("123", t("a"), Datum::Null),
            ("123", Datum::Int4(0), Datum::Null),
        ];
        for (target, index, want) in cases {
            let got = jsonb_subscript(&j(target), index).expect("subscript");
            assert!(got == *want, "{target}[{index:?}]");
        }
        // A NULL container is SQL NULL whatever the subscript is.
        assert!(jsonb_subscript(&Datum::Null, &t("a")).expect("null base") == Datum::Null);
        // Only text-ish and `integer` subscripts exist; `bigint` is 42804.
        let err = jsonb_subscript(&j("[1]"), &Datum::Int8(1)).expect_err("bigint subscript");
        assert!(err.into_pg().code == "42804");
    }

    /// `UPDATE t SET j[…] = v` is PostgreSQL's `jsonb_set_element`. It creates
    /// missing path steps, where an integer step makes an array and a text step
    /// makes an object. It pads a positive index past the end with JSON nulls.
    /// It refuses a negative index that reaches before the start, and it refuses
    /// a path step through a scalar. Rows measured against PostgreSQL 18.4.
    #[test]
    fn jsonb_subscript_assignment_creates_and_fills_paths() {
        let cases: &[(&str, &[Datum], &str, &str)] = &[
            ("{}", &[t("a")], "1", r#"{"a": 1}"#),
            (
                r#"{"key": "value"}"#,
                &[t("a")],
                "1",
                r#"{"a": 1, "key": "value"}"#,
            ),
            (r#"{"a": 1}"#, &[t("a")], r#""x""#, r#"{"a": "x"}"#),
            (
                "[0]",
                &[Datum::Int4(5)],
                "1",
                "[0, null, null, null, null, 1]",
            ),
            (
                "[]",
                &[Datum::Int4(5)],
                "1",
                "[null, null, null, null, null, 1]",
            ),
            (
                "[0, null, null, null, null, 1]",
                &[Datum::Int4(-4)],
                "1",
                "[0, null, 1, null, null, 1]",
            ),
            (
                "{}",
                &[t("a"), Datum::Int4(0), t("b"), Datum::Int4(0), t("c")],
                "1",
                r#"{"a": [{"b": [{"c": 1}]}]}"#,
            ),
            (
                "{}",
                &[
                    t("a"),
                    Datum::Int4(2),
                    t("b"),
                    Datum::Int4(2),
                    t("c"),
                    Datum::Int4(2),
                ],
                "1",
                r#"{"a": [null, null, {"b": [null, null, {"c": [null, null, 1]}]}]}"#,
            ),
            (
                r#"{"b": 1}"#,
                &[t("a"), Datum::Int4(0)],
                "2",
                r#"{"a": [2], "b": 1}"#,
            ),
            // An object container reads an integer subscript as a key.
            ("{}", &[Datum::Int4(0), t("a")], "1", r#"{"0": {"a": 1}}"#),
            ("[]", &[Datum::Int4(0), t("a")], "1", r#"[{"a": 1}]"#),
            (
                r#"{"a": {}}"#,
                &[t("a"), t("b"), t("c"), Datum::Int4(2)],
                "1",
                r#"{"a": {"b": {"c": [null, null, 1]}}}"#,
            ),
            (
                r#"{"a": []}"#,
                &[t("a"), Datum::Int4(1), t("c"), Datum::Int4(2)],
                "1",
                r#"{"a": [null, {"c": [null, null, 1]}]}"#,
            ),
        ];
        for (target, subscripts, value, want) in cases {
            let got = jsonb_subscript_assign(&j(target), subscripts, &j(value))
                .expect("subscripted assignment");
            assert!(got == j(want), "{target} {subscripts:?}");
        }
        // A NULL container is created from scratch, as an array when the first
        // subscript was written as an integer and an object otherwise.
        assert!(
            jsonb_subscript_assign(&Datum::Null, &[t("a")], &j("1")).expect("from null")
                == j(r#"{"a": 1}"#)
        );
        assert!(
            jsonb_subscript_assign(&Datum::Null, &[Datum::Int4(0)], &j("1")).expect("from null")
                == j("[1]")
        );
        // A SQL NULL value writes the JSON `null` literal.
        assert!(
            jsonb_subscript_assign(&j("{}"), &[t("a")], &Datum::Null).expect("null value")
                == j(r#"{"a": null}"#)
        );
        // A NULL subscript, an out-of-range negative index, and a path step
        // through a scalar are PostgreSQL's errors.
        let cases: &[(&str, &[Datum], &str)] = &[
            ("{}", &[Datum::Null], "22004"),
            ("[0]", &[Datum::Int4(-8)], "22023"),
            (r#"{"a": 1}"#, &[t("a"), t("b")], "22023"),
            ("null", &[Datum::Int4(0)], "22023"),
        ];
        for (target, subscripts, code) in cases {
            let err = jsonb_subscript_assign(&j(target), subscripts, &j("1"))
                .expect_err("refused assignment");
            assert!(err.into_pg().code == *code, "{target} {subscripts:?}");
        }
    }
}
