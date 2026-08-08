//! The `json` and `jsonb` function families and the semantics of every operator
//! either type has.
//!
//! This module follows the existing scalar families `func.rs`, `datetime_fn.rs`
//! and `format_fn.rs`. It holds a `json_func(name)` classifier, an
//! `is_json_func` dispatch predicate, a `json_func_result_type` static resolver
//! for RowDescription, and an `eval_json` value evaluator that takes the
//! caller's child-evaluation closure. So scalar `eval` and the grouped evaluator
//! share the math.
//!
//! `json` and `jsonb` are two types over one syntax, and `PostgreSQL` gives them
//! two disjoint function families rather than one polymorphic family — every
//! name is spelled for exactly one of the two, and calling it with the other's
//! value is 42883. The classifier therefore returns a ([`JsonFunc`],
//! [`Flavour`]) pair, and that flavour is threaded through [`param_types`],
//! [`json_func_result_type`] and [`eval_json`] so `json_typeof` takes a `json`
//! and `jsonb_typeof` takes a `jsonb`, with no overlap in either direction.
//!
//! The two families are *not* two spellings of one implementation:
//!
//!   * a `jsonb` function works on a decomposed [`JsonbValue`], so its answers
//!     carry `jsonb`'s canonical key order, de-duplicated keys and re-rendered
//!     whitespace;
//!   * a `json` function works on the stored text, so `'{"a":{"b":  1}}'::json
//!     -> 'a'` is the byte-identical sub-document `{"b":  1}` and `json_each`
//!     yields duplicate keys in input order. [`crabka_pgtypes::json`] owns the
//!     text-level reader those functions are written against.
//!
//! The constructors differ again, in spacing rather than in structure:
//! `row_to_json`/`array_to_json`/`to_json` write [`Layout::Compact`],
//! `json_build_object`/`json_build_array`/`json_object` write
//! [`Layout::Spaced`], and `jsonb`'s single rendering is a third. [`to_json_text`]
//! is the one place a `Datum` becomes `json` text, so `srf.rs` and `agg.rs` share
//! it.
//!
//! The operator semantics (`->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`,
//! `?&`, `||`, `-`) live here as well, exposed through [`JsonOp`] +
//! [`eval_json_operator`] (and individually), so the eval layer only has to map
//! its `BinaryOp` variants onto [`JsonOp`]. Keeping them beside the functions
//! puts every PostgreSQL corner case — jsonb null vs SQL NULL, the raw-scalar
//! containment exception, right-wins object merge — in one file. Only the four
//! extraction operators accept a `json` left operand; `json` has no equality,
//! containment, existence, concatenation or deletion operator at all.
//!
//! Everything here is a pure, deterministic transform over a single row's
//! already-resolved `Datum`s, so it introduces no lock, visibility, or
//! interleaving rule.

use std::borrow::Cow;

use bigdecimal::BigDecimal;
use crabka_pgparser::ast::{Expr, FuncArgs, FuncCall, SqlJsonExpr};
use crabka_pgtypes::{
    ArrayValue, ColumnType, Datum, ElemType, JsonbValue, TypeError,
    json::{self, Kind, Layout},
    jsonb, numeric,
};

use crate::{clock::EvalCtx, error::ExecError, eval::ArgType, scope::Scope};

/// Which of the two JSON types a call is spelled for.
///
/// `PostgreSQL` has no function that accepts either, so this is a property of
/// the *name*, fixed by [`json_func`] and never inferred from the arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flavour {
    /// `json_*`, `to_json`, `row_to_json`, `array_to_json`.
    Json,
    /// `jsonb_*` and `to_jsonb`.
    Jsonb,
}

impl Flavour {
    /// The type of this family's document parameter and (where the function
    /// returns a document at all) of its result.
    fn document(self) -> ColumnType {
        match self {
            Flavour::Json => ColumnType::Json,
            Flavour::Jsonb => ColumnType::Jsonb,
        }
    }

    /// The same distinction as seen by the record-population walk, which draws
    /// it for a different reason: not which parameter type a call takes, but
    /// whether a populated field keeps the document's original text.
    fn populate(self) -> crate::json_record::Flavour {
        match self {
            Flavour::Json => crate::json_record::Flavour::Json,
            Flavour::Jsonb => crate::json_record::Flavour::Jsonb,
        }
    }
}

/// The `json` and `jsonb` functions, named for whichever family has both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonFunc {
    /// `json_build_object` / `jsonb_build_object(VARIADIC k, v, …)` — an
    /// even-length key/value list.
    BuildObject,
    /// `json_build_array` / `jsonb_build_array(VARIADIC …)`.
    BuildArray,
    /// `json_array_length` / `jsonb_array_length`.
    ArrayLength,
    /// `json_typeof` / `jsonb_typeof`.
    Typeof,
    /// `json_extract_path` / `jsonb_extract_path(doc, VARIADIC path text)` — the
    /// `#>` function form.
    ExtractPath,
    /// `json_extract_path_text` / `jsonb_extract_path_text` — the `#>>` form.
    ExtractPathText,
    /// `jsonb_set(target, path, new_value [, create_if_missing])`. `jsonb` only.
    Set,
    /// `to_json(anyelement)` / `to_jsonb(anyelement)`.
    ToJson,
    /// `row_to_json(record [, pretty])`. `json` only — a composite reaches
    /// `jsonb` through `to_jsonb`.
    RowToJson,
    /// `array_to_json(anyarray [, pretty])`. `json` only.
    ArrayToJson,
    /// `json_strip_nulls` / `jsonb_strip_nulls(doc [, strip_in_arrays])` — drop
    /// every object field whose value is the JSON `null` literal, recursively.
    /// Array nulls are kept unless `strip_in_arrays`.
    StripNulls,
    /// `jsonb_pretty(jsonb)` — the indented rendering, as `text`. `jsonb` only.
    Pretty,
    /// `jsonb_insert(target, path, new_value [, insert_after])`. `jsonb` only.
    Insert,
    /// `jsonb_delete_path(target, path)` — the `#-` operator's function form.
    /// `jsonb` only.
    DeletePath,
    /// `jsonb_set_lax(target, path, new_value [, create_if_missing [, null_value_treatment]])`.
    /// `jsonb` only.
    SetLax,
    /// `json_populate_record(base anyelement, doc)` / `json_to_record(doc)` and
    /// the two `jsonb_` twins, in a *select list*.
    ///
    /// The FROM-position spelling of these — and the whole `*_recordset` half —
    /// is `srf`'s, because a FROM item expands a composite result into columns
    /// and a column-definition list can only be written there. What is left here
    /// is the one place the same call is a scalar: `SELECT
    /// json_populate_record(row(1,2), '…')` yields one composite value.
    PopulateRecord,
    /// `json_to_record(doc)` / `jsonb_to_record(doc)` in a select list, which is
    /// always the 0A000 below: nothing there can give a `record` a row type.
    ToRecord,
    /// `json_object(text[])` / `json_object(text[], text[])` (and the `jsonb_`
    /// spellings) — an object built from a flat or two-column key/value array.
    /// Every value is a JSON string.
    Object,
    /// `jsonb_path_exists(target, path [, vars [, silent]])` — the `@?` function
    /// form. `jsonb` only: `json` has no jsonpath support.
    PathExists,
    /// `jsonb_path_match(target, path [, vars [, silent]])`: the `@@` function form.
    PathMatch,
    /// `jsonb_path_query_array(target, path [, vars [, silent]])`.
    PathQueryArray,
    /// `jsonb_path_query_first(target, path [, vars [, silent]])`.
    PathQueryFirst,
    /// `jsonb_contains` / `jsonb_contained` / `jsonb_exists` / `jsonb_exists_any`
    /// / `jsonb_exists_all` / `jsonb_delete` / `jsonb_concat` — the function
    /// spellings of the operators, which `\df` and the regress corpus both use.
    /// All `jsonb` only.
    Operator(JsonOp),
}

/// Classify a (lowercased — the lexer lowercases unquoted idents) function name,
/// yielding the function and the family it belongs to. `None` means "not a JSON
/// function".
///
/// Every name maps to exactly one [`Flavour`]: `json_typeof` is not `jsonb_typeof`
/// under another name, and the names `PostgreSQL` only defines for `jsonb`
/// (`jsonb_set`, `jsonb_pretty`, the whole `jsonb_path_*` group, the operator
/// spellings) have no `json_` sibling here either, so `json_pretty(…)` falls
/// through to 42883 like any other unknown function.
///
/// `_tz` jsonpath variants share their implementation with the plain ones:
/// crabka's jsonpath datetime items are rendered strings, so no comparison in
/// them depends on the session time zone.
fn json_func(name: &str) -> Option<(JsonFunc, Flavour)> {
    use Flavour::{Json, Jsonb};

    Some(match name {
        "json_build_object" => (JsonFunc::BuildObject, Json),
        "jsonb_build_object" => (JsonFunc::BuildObject, Jsonb),
        "json_build_array" => (JsonFunc::BuildArray, Json),
        "jsonb_build_array" => (JsonFunc::BuildArray, Jsonb),
        "json_array_length" => (JsonFunc::ArrayLength, Json),
        "jsonb_array_length" => (JsonFunc::ArrayLength, Jsonb),
        "json_typeof" => (JsonFunc::Typeof, Json),
        "jsonb_typeof" => (JsonFunc::Typeof, Jsonb),
        "json_extract_path" => (JsonFunc::ExtractPath, Json),
        "jsonb_extract_path" => (JsonFunc::ExtractPath, Jsonb),
        "json_extract_path_text" => (JsonFunc::ExtractPathText, Json),
        "jsonb_extract_path_text" => (JsonFunc::ExtractPathText, Jsonb),
        "json_strip_nulls" => (JsonFunc::StripNulls, Json),
        "jsonb_strip_nulls" => (JsonFunc::StripNulls, Jsonb),
        "json_populate_record" => (JsonFunc::PopulateRecord, Json),
        "jsonb_populate_record" => (JsonFunc::PopulateRecord, Jsonb),
        "json_to_record" => (JsonFunc::ToRecord, Json),
        "jsonb_to_record" => (JsonFunc::ToRecord, Jsonb),
        "json_object" => (JsonFunc::Object, Json),
        "jsonb_object" => (JsonFunc::Object, Jsonb),
        "to_json" => (JsonFunc::ToJson, Json),
        "to_jsonb" => (JsonFunc::ToJson, Jsonb),
        "row_to_json" => (JsonFunc::RowToJson, Json),
        "array_to_json" => (JsonFunc::ArrayToJson, Json),
        "jsonb_set" => (JsonFunc::Set, Jsonb),
        "jsonb_set_lax" => (JsonFunc::SetLax, Jsonb),
        "jsonb_pretty" => (JsonFunc::Pretty, Jsonb),
        "jsonb_insert" => (JsonFunc::Insert, Jsonb),
        "jsonb_delete_path" => (JsonFunc::DeletePath, Jsonb),
        "jsonb_path_exists" | "jsonb_path_exists_tz" => (JsonFunc::PathExists, Jsonb),
        "jsonb_path_match" | "jsonb_path_match_tz" => (JsonFunc::PathMatch, Jsonb),
        "jsonb_path_query_array" | "jsonb_path_query_array_tz" => (JsonFunc::PathQueryArray, Jsonb),
        "jsonb_path_query_first" | "jsonb_path_query_first_tz" => (JsonFunc::PathQueryFirst, Jsonb),
        "jsonb_contains" => (JsonFunc::Operator(JsonOp::Contains), Jsonb),
        "jsonb_contained" => (JsonFunc::Operator(JsonOp::ContainedBy), Jsonb),
        "jsonb_exists" => (JsonFunc::Operator(JsonOp::KeyExists), Jsonb),
        "jsonb_exists_any" => (JsonFunc::Operator(JsonOp::KeyExistsAny), Jsonb),
        "jsonb_exists_all" => (JsonFunc::Operator(JsonOp::KeyExistsAll), Jsonb),
        "jsonb_delete" => (JsonFunc::Operator(JsonOp::Delete), Jsonb),
        "jsonb_concat" => (JsonFunc::Operator(JsonOp::Concat), Jsonb),
        _ => return None,
    })
}

/// Is `name` a `json`/`jsonb` function? (The dispatch point for the eval guard
/// chains.)
pub(crate) fn is_json_func(name: &str) -> bool {
    json_func(name).is_some()
}

/// Is `name` one of the two record-mapping functions a *select list* evaluates
/// as a scalar? (`*_populate_recordset`/`*_to_recordset` are set-returning and
/// belong to `srf`.)
pub(crate) fn is_record_func(name: &str) -> bool {
    matches!(
        json_func(name),
        Some((JsonFunc::PopulateRecord | JsonFunc::ToRecord, _))
    )
}

/// Evaluate a select-list `json_populate_record`/`jsonb_populate_record`, with
/// the scope the declared row type is resolved against.
///
/// A `populate_record` call's result shape has two possible sources and needs
/// both: `NULL::jpop` carries the composite only in its *declared* type, and
/// `ROW(1, 2)` carries it only in its *value*. Neither alone answers both, so
/// the declared type is tried first and the value is the fallback.
pub(crate) fn eval_record_func(
    fc: &FuncCall,
    scope: &Scope,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let (f, flavour) = json_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    if f == JsonFunc::ToRecord {
        // `json_to_record` returns `record` and a select list can say nothing
        // about it; the arity/type checks still have to run first.
        json_func_result_type(fc, scope)?;
        return Err(crate::json_record::indeterminate_row_type(&fc.name));
    }
    let declared = json_func_result_type(fc, scope)?;
    let args = exprs_of(fc)?;
    let mut vals: Vec<Datum> = args.iter().map(&mut eval_child).collect::<Result<_, _>>()?;
    let given = crate::eval::value_arg_types(args, &vals);
    crate::eval::coerce_unknown_args(args, &mut vals, &param_types(f, flavour, &given)?, ctx)?;
    populate_record_value(fc, flavour, Some(declared), &vals, ctx)
}

/// The shared body of both entry points: resolve the shape, populate it, and
/// rebuild the composite.
fn populate_record_value(
    fc: &FuncCall,
    flavour: Flavour,
    declared: Option<ColumnType>,
    vals: &[Datum],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    require_arity(
        fc,
        vals.len() == 2 || (vals.len() == 3 && flavour == Flavour::Json),
    )?;
    let base = match &vals[0] {
        Datum::Record(record) => Some(record),
        _ => None,
    };
    let (shape, named) = match declared.and_then(crate::json_record::RecordShape::of) {
        Some(shape) => (
            shape,
            declared.and_then(|ty| match ty {
                ColumnType::Record(named) => named,
                _ => None,
            }),
        ),
        None => match base {
            Some(base) => (crate::json_record::RecordShape::of_value(base), base.ty),
            None => return Err(crate::json_record::indeterminate_row_type(&fc.name)),
        },
    };
    let values = if vals[1].is_null() {
        crate::json_record::populate_missing(&shape, base, ctx)?
    } else {
        let node = crate::json_record::Node::of(&vals[1], flavour.populate())?;
        crate::json_record::populate(&shape, base, node, ctx)?
    };
    let fields: Vec<String> = shape
        .fields
        .iter()
        .map(|(field, _)| field.clone())
        .collect();
    Ok(Datum::Record(crabka_pgtypes::RecordValue::named(
        named,
        fields.into(),
        values,
    )))
}

// ---- argument-type resolution ----

/// The type an `unknown` literal argument adopts, per position — the ONE place
/// each family's parameter types are written down.
///
/// PostgreSQL leaves a bare `'…'` / `NULL` literal `unknown` and resolves it
/// against the parameter it is passed to, so `jsonb_set('{"a":1}', '{a}', '2')`
/// passes a `jsonb`, a `text[]` and a `jsonb`. `None` in the result means the
/// literal adopts nothing and stays `text`, which is PostgreSQL's own rule for a
/// `"any"` parameter. That is why `jsonb_build_object('a', '{"x":1}')` stores
/// the JSON *string* `"{\"x\":1}"` rather than a nested object.
///
/// `flavour` picks which of `json`/`jsonb` the document parameters take, so
/// `json_typeof('{}')` coerces its literal with `json_in` and `jsonb_typeof('{}')`
/// with `jsonb_in`.
///
/// Both [`json_func_result_type`] (plan time, over statically inferred argument
/// types) and [`eval_json`] (run time, over the evaluated values' types) drive
/// this one rule, so a literal is typed and converted by the same decision.
fn param_types(
    f: JsonFunc,
    flavour: Flavour,
    given: &[ArgType],
) -> Result<Vec<Option<ColumnType>>, ExecError> {
    let n = given.len();
    let doc = Some(flavour.document());
    let jsonb = Some(ColumnType::Jsonb);
    Ok(match f {
        // `VARIADIC "any"`: an `unknown` literal resolves to `text`.
        JsonFunc::BuildObject | JsonFunc::BuildArray => vec![None; n],
        JsonFunc::ArrayLength | JsonFunc::Typeof => vec![doc],
        // `(doc, VARIADIC text[])`.
        JsonFunc::ExtractPath | JsonFunc::ExtractPathText => std::iter::once(doc)
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
        // PG18's `strip_nulls(doc, strip_in_arrays boolean)`.
        JsonFunc::StripNulls => vec![doc, Some(ColumnType::Bool)],
        JsonFunc::Pretty => vec![jsonb],
        JsonFunc::DeletePath => vec![jsonb, ColumnType::array_of(ColumnType::Text)],
        JsonFunc::Object => vec![ColumnType::array_of(ColumnType::Text); n.max(1)],
        // `(anyelement, doc)`: only the document position resolves a literal —
        // the row-type argument is polymorphic, so an `unknown` there is 42804.
        JsonFunc::PopulateRecord => {
            if given.first().is_some_and(|a| a.is_unknown()) {
                return Err(crate::eval::undetermined_polymorphic_type());
            }
            // The third parameter is `json_populate_record`'s vestigial
            // `use_json_as_text boolean DEFAULT false`; `jsonb_populate_record`
            // has no such parameter, so a third argument there is 42883.
            vec![None, doc, Some(ColumnType::Bool)]
        }
        JsonFunc::ToRecord => vec![doc],
        // `(jsonb, jsonpath [, jsonb vars [, boolean silent]])`.
        JsonFunc::PathExists
        | JsonFunc::PathMatch
        | JsonFunc::PathQueryArray
        | JsonFunc::PathQueryFirst => {
            vec![
                jsonb,
                Some(ColumnType::JsonPath),
                jsonb,
                Some(ColumnType::Bool),
            ]
        }
        JsonFunc::Operator(op) => match op {
            JsonOp::KeyExists => vec![jsonb, Some(ColumnType::Text)],
            JsonOp::KeyExistsAny | JsonOp::KeyExistsAll => {
                vec![jsonb, ColumnType::array_of(ColumnType::Text)]
            }
            JsonOp::Delete => vec![jsonb, None],
            _ => vec![jsonb, jsonb],
        },
        // `to_json(anyelement)` / `to_jsonb(anyelement)`: nothing else in the
        // call can resolve the parameter, so an `unknown` literal there is
        // 42804 — not a JSON string.
        JsonFunc::ToJson => {
            if given.first().is_some_and(|a| a.is_unknown()) {
                return Err(crate::eval::undetermined_polymorphic_type());
            }
            vec![None; n]
        }
        JsonFunc::RowToJson | JsonFunc::ArrayToJson => vec![None, Some(ColumnType::Bool)],
    })
}

// ---- result-type inference ----

/// Statically infer a JSON call's result type (for RowDescription). Arity and
/// argument-type mismatches surface as 42883 here, at plan time.
pub(crate) fn json_func_result_type(fc: &FuncCall, scope: &Scope) -> Result<ColumnType, ExecError> {
    let (f, flavour) = json_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = exprs_of(fc)?;
    let n = args.len();
    let given = crate::eval::static_arg_types(args, scope)?;
    let types = crate::eval::effective_arg_types(&given, &param_types(f, flavour, &given)?);
    // The document type this family produces (`json` for `json_*`, `jsonb` for
    // `jsonb_*`) and the one it accepts, which are the same type.
    let doc = flavour.document();
    Ok(match f {
        JsonFunc::BuildObject => {
            // PostgreSQL reports the odd-length list at run time (22023); the
            // arity itself is unconstrained here.
            doc
        }
        JsonFunc::BuildArray => doc,
        JsonFunc::RowToJson => {
            require_arity(fc, n == 1 || n == 2)?;
            if !matches!(types[0], ColumnType::Record(_))
                || types.get(1).is_some_and(|ty| *ty != ColumnType::Bool)
            {
                return Err(undefined_function(&fc.name));
            }
            doc
        }
        JsonFunc::ArrayToJson => {
            require_arity(fc, n == 1 || n == 2)?;
            if !matches!(types[0], ColumnType::Array(_))
                || types.get(1).is_some_and(|ty| *ty != ColumnType::Bool)
            {
                return Err(undefined_function(&fc.name));
            }
            doc
        }
        // A select-list call's row type can only come from its own argument, and
        // for the anonymous `record` only the *value* has one: `ROW(1, 2)` and
        // `NULL::record` are one type here and two different answers — a row of
        // two fields, and the 0A000 below. So the refusal is deferred to
        // [`eval_json`], which has the value.
        JsonFunc::PopulateRecord => {
            require_arity(fc, n == 2 || (n == 3 && flavour == Flavour::Json))?;
            require_document_arg(fc, doc, types[1])?;
            match types[0] {
                ty @ ColumnType::Record(_) => ty,
                _ => return Err(undefined_function(&fc.name)),
            }
        }
        JsonFunc::ToRecord => {
            require_arity(fc, n == 1)?;
            require_document_arg(fc, doc, types[0])?;
            return Err(crate::json_record::indeterminate_row_type(&fc.name));
        }
        JsonFunc::ArrayLength => {
            require_arity(fc, n == 1)?;
            require_document_arg(fc, doc, types[0])?;
            ColumnType::Int4
        }
        JsonFunc::Typeof => {
            require_arity(fc, n == 1)?;
            require_document_arg(fc, doc, types[0])?;
            ColumnType::Text
        }
        JsonFunc::ExtractPath | JsonFunc::ExtractPathText => {
            require_arity(fc, n >= 1)?;
            require_document_arg(fc, doc, types[0])?;
            if types[1..].iter().any(|t| !t.is_string()) {
                return Err(undefined_function(&fc.name));
            }
            if f == JsonFunc::ExtractPath {
                doc
            } else {
                ColumnType::Text
            }
        }
        JsonFunc::Set | JsonFunc::Insert => {
            require_arity(fc, n == 3 || n == 4)?;
            require_document_arg(fc, doc, types[0])?;
            if types[1] != ColumnType::Array(ElemType::Text) {
                return Err(undefined_function(&fc.name));
            }
            require_document_arg(fc, doc, types[2])?;
            doc
        }
        JsonFunc::ToJson => {
            require_arity(fc, n == 1)?;
            doc
        }
        JsonFunc::StripNulls => {
            require_arity(fc, n == 1 || n == 2)?;
            require_document_arg(fc, doc, types[0])?;
            doc
        }
        JsonFunc::Pretty => {
            require_arity(fc, n == 1)?;
            require_document_arg(fc, doc, types[0])?;
            ColumnType::Text
        }
        JsonFunc::DeletePath => {
            require_arity(fc, n == 2)?;
            require_document_arg(fc, doc, types[0])?;
            if types[1] != ColumnType::Array(ElemType::Text) {
                return Err(undefined_function(&fc.name));
            }
            doc
        }
        JsonFunc::SetLax => {
            require_arity(fc, (3..=5).contains(&n))?;
            require_document_arg(fc, doc, types[0])?;
            if types[1] != ColumnType::Array(ElemType::Text) {
                return Err(undefined_function(&fc.name));
            }
            require_document_arg(fc, doc, types[2])?;
            doc
        }
        JsonFunc::Object => {
            require_arity(fc, n == 1 || n == 2)?;
            if types
                .iter()
                .any(|t| *t != ColumnType::Array(ElemType::Text))
            {
                return Err(undefined_function(&fc.name));
            }
            doc
        }
        JsonFunc::PathExists | JsonFunc::PathMatch => {
            require_arity(fc, (2..=4).contains(&n))?;
            require_document_arg(fc, doc, types[0])?;
            if types[1] != ColumnType::JsonPath {
                return Err(undefined_function(&fc.name));
            }
            ColumnType::Bool
        }
        JsonFunc::PathQueryArray | JsonFunc::PathQueryFirst => {
            require_arity(fc, (2..=4).contains(&n))?;
            require_document_arg(fc, doc, types[0])?;
            if types[1] != ColumnType::JsonPath {
                return Err(undefined_function(&fc.name));
            }
            doc
        }
        JsonFunc::Operator(op) => {
            require_arity(fc, n == 2)?;
            json_operator_result_type(op, types[0], types[1])
                .ok_or_else(|| undefined_function(&fc.name))?
        }
    })
}

/// A document argument of this family's own type. An unadorned literal
/// (`jsonb_typeof('{"a":1}')`) has already adopted that type in [`param_types`],
/// so — as in PostgreSQL — a genuinely `text`-typed argument is 42883 and needs
/// an explicit cast, and so is the *other* JSON type: `jsonb_typeof('{}'::json)`
/// and `json_typeof('{}'::jsonb)` are both `function … does not exist`.
fn require_document_arg(fc: &FuncCall, want: ColumnType, t: ColumnType) -> Result<(), ExecError> {
    if t == want {
        Ok(())
    } else {
        Err(undefined_function(&fc.name))
    }
}

// ---- evaluation ----

/// Evaluate a `json` or `jsonb` function call.
///
/// Every function except `*_build_object`/`*_build_array` is STRICT: a NULL
/// argument yields SQL NULL. The two builders are deliberately not strict —
/// a NULL *value* becomes the JSON `null` literal (a NULL *key* is an error).
///
/// The arms a name exists in both families under branch on `flavour`; the ones
/// only `jsonb` has are simply never reached with [`Flavour::Json`], because
/// [`json_func`] does not classify a `json_` spelling onto them.
pub(crate) fn eval_json(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let (f, flavour) = json_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = exprs_of(fc)?;
    let mut vals: Vec<Datum> = args.iter().map(&mut eval_child).collect::<Result<_, _>>()?;
    // Give every `unknown` literal argument the value its parameter's type calls
    // for, by the same rule the plan-time resolver typed it with.
    let given = crate::eval::value_arg_types(args, &vals);
    crate::eval::coerce_unknown_args(args, &mut vals, &param_types(f, flavour, &given)?, ctx)?;
    let n = vals.len();
    let json_flavoured = flavour == Flavour::Json;
    match f {
        JsonFunc::BuildObject if json_flavoured => build_json_object(&vals, ctx),
        JsonFunc::BuildObject => build_object(&vals, ctx),
        JsonFunc::BuildArray if json_flavoured => {
            let mut out = String::from("[");
            for (i, v) in vals.iter().enumerate() {
                if i > 0 {
                    out.push_str(Layout::Spaced.comma());
                }
                out.push_str(&to_json_text(v, Layout::Compact, ctx)?);
            }
            out.push(']');
            Ok(Datum::Json(out))
        }
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
            let count = if json_flavoured {
                let doc = json_operand(&vals[0], &fc.name)?;
                match json::kind(&doc) {
                    Kind::Array => json::array_elements(&doc).unwrap_or_default().len(),
                    Kind::Object => {
                        return Err(invalid_parameter("cannot get array length of a non-array"));
                    }
                    _ => return Err(invalid_parameter("cannot get array length of a scalar")),
                }
            } else {
                let value = jsonb_operand(&vals[0], &fc.name)?;
                match value.as_ref() {
                    JsonbValue::Array(items) => items.len(),
                    JsonbValue::Object(_) => {
                        return Err(invalid_parameter("cannot get array length of a non-array"));
                    }
                    _ => return Err(invalid_parameter("cannot get array length of a scalar")),
                }
            };
            Ok(Datum::Int4(
                i32::try_from(count).map_err(|_| ExecError::Type(TypeError::Overflow))?,
            ))
        }
        JsonFunc::Typeof => {
            require_arity(fc, n == 1)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            let name = if json_flavoured {
                json::kind(&json_operand(&vals[0], &fc.name)?)
                    .name()
                    .to_string()
            } else {
                jsonb_operand(&vals[0], &fc.name)?.type_name().to_string()
            };
            Ok(Datum::Text(name))
        }
        JsonFunc::ExtractPath | JsonFunc::ExtractPathText => {
            require_arity(fc, n >= 1)?;
            if vals.iter().any(Datum::is_null) {
                return Ok(Datum::Null);
            }
            let mut path = Vec::with_capacity(n - 1);
            for v in &vals[1..] {
                path.push(Some(text_arg(v, &fc.name)?.to_string()));
            }
            let want_document = f == JsonFunc::ExtractPath;
            if json_flavoured {
                let doc = json_operand(&vals[0], &fc.name)?;
                return Ok(match (json_navigate(&doc, &path), want_document) {
                    (None, _) => Datum::Null,
                    (Some(sub), true) => Datum::Json(sub.to_string()),
                    (Some(sub), false) => json_as_sql_text(sub),
                });
            }
            let value = jsonb_operand(&vals[0], &fc.name)?;
            let found = navigate(value.as_ref(), &path);
            Ok(match (found, want_document) {
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
        JsonFunc::ToJson if json_flavoured => {
            require_arity(fc, n == 1)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Json(to_json_text(&vals[0], Layout::Compact, ctx)?))
        }
        JsonFunc::ToJson => {
            require_arity(fc, n == 1)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Jsonb(to_jsonb(&vals[0], ctx)?))
        }
        // `row_to_json(record [, pretty])` and `array_to_json(anyarray [,
        // pretty])` are the same rendering over two argument shapes: compact
        // by default, and with every top-level separator broken onto a fresh
        // line when the flag is set (`use_line_feeds`).
        JsonFunc::RowToJson | JsonFunc::ArrayToJson => {
            require_arity(fc, n == 1 || n == 2)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            let shape_matches = match f {
                JsonFunc::RowToJson => matches!(vals[0], Datum::Record(_)),
                _ => matches!(vals[0], Datum::Array(_) | Datum::OidVector(_)),
            };
            if !shape_matches {
                return Err(undefined_function(&fc.name));
            }
            let punct = match vals.get(1) {
                None | Some(Datum::Bool(false)) => Punct::COMPACT,
                Some(Datum::Bool(true)) => Punct::LINE_FEEDS,
                Some(Datum::Null) => return Ok(Datum::Null),
                Some(other) => return Err(type_error(&fc.name, other)),
            };
            let mut out = String::new();
            write_json(&vals[0], punct, ctx, &mut out)?;
            Ok(Datum::Json(out))
        }
        // Reached only from the aggregate evaluator, which has no scope to read
        // a declared row type from; [`eval_record_func`] is the scoped entry
        // point every other caller takes.
        JsonFunc::PopulateRecord => populate_record_value(fc, flavour, None, &vals, ctx),
        // Unreachable in practice: `json_func_result_type` has already refused
        // the call, and every path that evaluates one types it first.
        JsonFunc::ToRecord => Err(crate::json_record::indeterminate_row_type(&fc.name)),
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
            if json_flavoured {
                let doc = json_operand(&vals[0], &fc.name)?;
                return Ok(Datum::Json(json::strip_nulls(&doc, in_arrays)));
            }
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
            eval_json_object(&fc.name, flavour, &vals)
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
    let path = crate::jsonpath::JsonPath::parse(jsonpath_arg(&args[1], name)?)?;
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
    let path = crate::jsonpath::JsonPath::parse(jsonpath_arg(right, op.spelling())?)?;
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

/// `json_object(text[])` / `json_object(text[], text[])` (and the `jsonb_`
/// spellings): an object whose values are all JSON strings (a NULL element
/// becomes the JSON `null` literal).
///
/// The two flavours agree on every error and on which pairs survive; they differ
/// only in the result — `json_object` writes [`Layout::Spaced`] text keeping
/// duplicate keys in input order, `jsonb_object` a de-duplicated [`JsonbValue`].
fn eval_json_object(name: &str, flavour: Flavour, vals: &[Datum]) -> Result<Datum, ExecError> {
    let flat = |d: &Datum| -> Result<Vec<Datum>, ExecError> {
        match d {
            Datum::Array(a) => Ok(a.elems.clone()),
            other => Err(type_error(name, other)),
        }
    };
    let element = |d: &Datum| -> Option<String> {
        match d {
            Datum::Null => None,
            Datum::Text(s) => Some(s.clone()),
            other => Some(
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
            Datum::Null => Err(object_key_null()),
            Datum::Text(s) => Ok(s.clone()),
            other => Err(type_error(name, other)),
        }
    };
    let mut pairs: Vec<(String, Option<String>)> = Vec::new();
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
    if flavour == Flavour::Json {
        let rendered = pairs.iter().map(|(k, v)| {
            (
                k.as_str(),
                v.as_ref()
                    .map_or_else(|| "null".to_string(), |s| json::quote(s)),
            )
        });
        return Ok(Datum::Json(write_object(Layout::Spaced, rendered)));
    }
    Ok(Datum::Jsonb(JsonbValue::object_from_pairs(
        pairs
            .into_iter()
            .map(|(k, v)| (k, v.map_or(JsonbValue::Null, JsonbValue::String)))
            .collect(),
    )))
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
        // The SQL/JSON constructors all produce `json`, not `jsonb`:
        // `JSON_OBJECT('a': 1)` is `{"a" : 1}` (Layout::Spaced) and keeps its
        // duplicate keys, where the `jsonb` rendering would be `{"a": 1}` with
        // one of them gone.
        SqlJsonExpr::Object {
            entries,
            absent_on_null,
            unique_keys,
            returning,
        } => {
            let mut pairs: Vec<(String, String)> = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                let value = eval_child(value)?;
                if *absent_on_null && value.is_null() {
                    continue;
                }
                let key = eval_child(key)?;
                if key.is_null() {
                    return Err(object_key_null());
                }
                let key = json_object_key_text(&key, ctx)?;
                if *unique_keys && pairs.iter().any(|(k, _)| *k == key) {
                    return Err(ExecError::FunctionError {
                        sqlstate: "22030",
                        message: "duplicate JSON object key value".into(),
                    });
                }
                pairs.push((key, to_json_text(&value, Layout::Compact, ctx)?));
            }
            let text = write_object(
                Layout::Spaced,
                pairs.iter().map(|(k, v)| (k.as_str(), v.clone())),
            );
            returning_json(text, *returning, ctx)
        }
        SqlJsonExpr::Array {
            items,
            absent_on_null,
            returning,
        } => {
            let mut text = String::from("[");
            for item in items {
                let value = eval_child(item)?;
                if *absent_on_null && value.is_null() {
                    continue;
                }
                if text.len() > 1 {
                    text.push_str(Layout::Spaced.comma());
                }
                text.push_str(&to_json_text(&value, Layout::Compact, ctx)?);
            }
            text.push(']');
            returning_json(text, *returning, ctx)
        }
        SqlJsonExpr::Scalar(expr) => {
            let value = eval_child(expr)?;
            if value.is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Json(to_json_text(&value, Layout::Compact, ctx)?))
        }
        SqlJsonExpr::Serialize { expr, returning } => {
            let value = eval_child(expr)?;
            if value.is_null() {
                return Ok(Datum::Null);
            }
            // `JSON_SERIALIZE` writes the document's own text out, so a `json`
            // operand keeps its spacing rather than being re-rendered.
            let text = Datum::Text(match &value {
                Datum::Json(stored) => stored.clone(),
                other => json_document(other)?.to_text(),
            });
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
            // `JSON(x)` is `json_in` over `x`'s text, so — like the cast — it
            // keeps every byte, and `WITH UNIQUE KEYS` is the only reason it has
            // to decompose the document at all.
            let (text, duplicate) = match &value {
                Datum::Json(stored) => (stored.clone(), duplicate_keys(stored)),
                Datum::Jsonb(j) => (j.to_text(), false),
                Datum::Text(text) => {
                    json::validate(text)?;
                    (text.clone(), duplicate_keys(text))
                }
                other => return Err(type_error("json", other)),
            };
            if *unique_keys && duplicate {
                return Err(ExecError::FunctionError {
                    sqlstate: "22030",
                    message: "duplicate JSON object key value".into(),
                });
            }
            Ok(Datum::Json(text))
        }
        SqlJsonExpr::Query(q) => eval_json_query(q, ctx, eval_child),
    }
}

/// Does any object in this already-validated `json` text repeat a key? (`WITH
/// UNIQUE KEYS`'s observation, which only `jsonb_in` computes as it goes.)
fn duplicate_keys(text: &str) -> bool {
    jsonb::parse_with_options(text, false).is_ok_and(|(_, duplicate)| duplicate)
}

/// `RETURNING <type>` on a constructor: the document is produced as `json` and
/// converted, so `RETURNING jsonb` parses it (dropping the spacing and the
/// duplicate keys) and `RETURNING text` hands back the text unchanged.
fn returning_json(
    text: String,
    returning: Option<ColumnType>,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let datum = Datum::Json(text);
    match returning {
        None | Some(ColumnType::Json) => Ok(datum),
        Some(ty) => Ok(crabka_pgtypes::cast::cast(&datum, ty, &ctx.time_zone)?),
    }
}

/// `IS [NOT] JSON [<item>] [WITH UNIQUE KEYS]` over an already-evaluated,
/// non-NULL value. Only the string family, `json` and `jsonb` have a JSON
/// reading at all; every other type is 42804, as in `PostgreSQL`.
fn is_json(
    value: &Datum,
    item: crabka_pgparser::ast::JsonItemType,
    unique_keys: bool,
) -> Result<bool, ExecError> {
    use crabka_pgparser::ast::JsonItemType;

    let (document, duplicate) = match value {
        Datum::Jsonb(j) => (j.clone(), false),
        // A `json` value was validated on input, so it is always JSON; only its
        // duplicate keys are still in question.
        Datum::Json(text) | Datum::Text(text) => match jsonb::parse_with_options(text, false) {
            Ok(pair) => pair,
            // Text that does not parse is simply not JSON.
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

/// The plan-time counterpart: `IS JSON` accepts only the string family, `json`
/// and `jsonb`, so `1 IS JSON` is 42804 before a row is ever read.
pub(crate) fn is_json_operand_type(ty: ColumnType) -> Result<(), ExecError> {
    if ty.is_string() || ty == ColumnType::Json || ty == ColumnType::Jsonb {
        Ok(())
    } else {
        Err(ExecError::TypeMismatch(format!(
            "cannot use type {} in IS JSON predicate",
            ty.name()
        )))
    }
}

/// The jsonb document an argument denotes: a `jsonb` value as-is, `json` and
/// `text` parsed. This is the *jsonpath* reading of a document — jsonpath has no
/// `json` flavour, so `JSON_QUERY(json '…', '$.a')` still answers in `jsonb`.
pub(crate) fn json_document(value: &Datum) -> Result<Cow<'_, JsonbValue>, ExecError> {
    match value {
        Datum::Jsonb(j) => Ok(Cow::Borrowed(j)),
        Datum::Json(text) => jsonb::parse(text).map(Cow::Owned).map_err(ExecError::Type),
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
/// Numbers stay numbers (scale preserved, as `jsonb` is numeric-backed), strings
/// and every stringly type become JSON strings, `jsonb` is the identity, and an
/// array becomes a JSON array. Date/time values use PostgreSQL's JSON spelling —
/// ISO 8601 with a `T` separator and an `hh:mm` offset — not their SQL output.
pub(crate) fn to_jsonb(d: &Datum, ctx: &EvalCtx) -> Result<JsonbValue, ExecError> {
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
        // `to_jsonb(json)` is the `json → jsonb` cast: parse the stored text,
        // which is where its whitespace and duplicate keys are finally lost.
        Datum::Json(text) => jsonb::parse(text)?,
        Datum::Array(a) | Datum::OidVector(a) => JsonbValue::Array(
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
        // `xml` is there too — `to_jsonb('<a/>'::xml)` is the document as a
        // JSON string, because `xml` is not one of the types `to_jsonb`
        // special-cases by oid.
        Datum::Text(_)
        | Datum::Xml(_)
        | Datum::JsonPath(_)
        | Datum::Point(_)
        | Datum::Path(_)
        | Datum::Polygon(_)
        | Datum::Lseg(_)
        | Datum::Line(_)
        | Datum::Circle(_)
        | Datum::Box(_)
        | Datum::Date(_)
        | Datum::Time(_)
        | Datum::Timetz(_)
        | Datum::Interval(_)
        | Datum::Enum(_)
        | Datum::Regclass(_)
        | Datum::TsVector(_)
        | Datum::TsQuery(_)
        | Datum::Inet(_)
        | Datum::MacAddr(_)
        | Datum::MacAddr8(_)
        | Datum::BitString(_)
        | Datum::Money(_)
        | Datum::Range(_)
        | Datum::Multirange(_)
        // The system identifier family joins the stringly group even though
        // `oid` is `typcategory` N: `to_jsonb` special-cases the six concrete
        // numeric types by oid, and `oid` is not one of them, so
        // `to_jsonb(4294967295::oid)` is `"4294967295"`, not a JSON number.
        | Datum::Oid(_)
        | Datum::Xid(_)
        | Datum::Xid8(_)
        | Datum::Cid(_)
        | Datum::Tid(_)
        | Datum::PgLsn(_)
        | Datum::Bytea(_) => JsonbValue::String(datum_text(d, ctx)),
    })
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

// ---- the `json` renderers ----

/// The punctuation ONE level of a `json` rendering is written with.
///
/// `PostgreSQL` never propagates a constructor's spacing inward: only the level
/// the function itself writes is spaced, and everything nested inside a value is
/// rendered by `datum_to_json`, which is always compact. So
/// `json_build_object('a', array[1,2])` is `{"a" : [1,2]}` — a spaced object
/// holding a compact array — and every recursive step below uses
/// [`Punct::COMPACT`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Punct {
    /// Between two members.
    comma: &'static str,
    /// Between a key and its value.
    colon: &'static str,
    /// Just inside the braces of a non-empty object.
    pad: &'static str,
}

impl Punct {
    /// `composite_to_json` / `array_to_json` with `use_line_feeds` false, and
    /// every nested value everywhere.
    const COMPACT: Punct = Punct {
        comma: ",",
        colon: ":",
        pad: "",
    };
    /// `row_to_json(…, true)` / `array_to_json(…, true)`: `use_line_feeds` puts
    /// each member after the first on its own line, indented one space. It is
    /// deliberately not a [`Layout`] — no constructor spells it, and it applies
    /// only to the level the flag was passed to.
    const LINE_FEEDS: Punct = Punct {
        comma: ",\n ",
        colon: ":",
        pad: "",
    };

    /// The punctuation a constructor's [`Layout`] calls for.
    fn of(layout: Layout) -> Punct {
        Punct {
            comma: layout.comma(),
            colon: layout.colon(),
            pad: layout.pad(),
        }
    }
}

/// `to_json(anyelement)`: the value's `json` rendering under `layout`.
///
/// `layout` governs only the outermost container this writes (an array or a
/// composite); anything nested inside one is compact, because that is what
/// `PostgreSQL` does. Pass [`Layout::Compact`] for a value being placed inside a
/// document some *other* code is punctuating — which is every caller in
/// `srf.rs` and `agg.rs`, since those write their own brackets.
///
/// A `json` value is inlined verbatim, which is the whole reason `json` and
/// `jsonb` cannot share this: `json_build_object('k', '{"b":1,  "a":2}'::json)`
/// keeps the two spaces, where the `jsonb` route would re-render the document.
pub(crate) fn to_json_text(
    d: &Datum,
    layout: crabka_pgtypes::json::Layout,
    ctx: &EvalCtx,
) -> Result<String, ExecError> {
    let mut out = String::new();
    write_json(d, Punct::of(layout), ctx, &mut out)?;
    Ok(out)
}

/// `datum_to_json`: append `d`'s `json` text, punctuating this level with
/// `punct` and every level below it compactly.
fn write_json(d: &Datum, punct: Punct, ctx: &EvalCtx, out: &mut String) -> Result<(), ExecError> {
    match d {
        Datum::Null => out.push_str("null"),
        Datum::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        // `JSONTYPE_NUMERIC`: PostgreSQL prints the type's OWN output text when
        // that text is a JSON number and quotes it when it is not, so
        // `to_json(1e30::float8)` is `1e+30` — the `float8out` spelling, not the
        // all-digits `numeric` one `to_jsonb` produces — and `'NaN'::float8`,
        // which JSON has no spelling for, becomes the string `"NaN"`.
        Datum::Int2(_)
        | Datum::Int4(_)
        | Datum::Int8(_)
        | Datum::Numeric(_)
        | Datum::Float4(_)
        | Datum::Float8(_) => {
            let text = datum_text(d, ctx);
            if is_json_number(&text) {
                out.push_str(&text);
            } else {
                json::write_string(&text, out);
            }
        }
        // `JSONTYPE_JSON`: the stored text, byte for byte.
        Datum::Json(text) => out.push_str(text),
        // `jsonb` reaches `json` through its output function, so it keeps its
        // own canonical rendering rather than adopting `punct`.
        Datum::Jsonb(j) => out.push_str(&j.to_text()),
        Datum::Array(a) | Datum::OidVector(a) => {
            out.push('[');
            for (i, e) in a.elems.iter().enumerate() {
                if i > 0 {
                    out.push_str(punct.comma);
                }
                write_json(e, Punct::COMPACT, ctx, out)?;
            }
            out.push(']');
        }
        // A composite keeps every field, duplicate names included — the
        // difference from `to_jsonb`, whose object collapses them last-wins.
        Datum::Record(r) => {
            out.push('{');
            for (index, value) in r.values.iter().enumerate() {
                out.push_str(if index == 0 { punct.pad } else { punct.comma });
                let name = r
                    .names
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| format!("f{}", index + 1));
                json::write_string(&name, out);
                out.push_str(punct.colon);
                write_json(value, Punct::COMPACT, ctx, out)?;
            }
            if !r.values.is_empty() {
                out.push_str(punct.pad);
            }
            out.push('}');
        }
        Datum::Timestamp(_) | Datum::Timestamptz(_) => {
            json::write_string(&iso_8601_datetime(&datum_text(d, ctx)), out);
        }
        other => json::write_string(&datum_text(other, ctx), out),
    }
    Ok(())
}

/// `IsValidJsonNumber`: would this output text lex as a JSON number, so that it
/// can go into the document unquoted?
fn is_json_number(text: &str) -> bool {
    json::kind(text) == Kind::Number && json::validate(text).is_ok()
}

/// Assemble `{k1<colon>v1<comma>k2<colon>v2}` from already-rendered value text,
/// keeping duplicate keys — the shape every `json` object constructor writes.
fn write_object<'a>(layout: Layout, pairs: impl IntoIterator<Item = (&'a str, String)>) -> String {
    let mut out = String::from("{");
    let mut empty = true;
    for (key, value) in pairs {
        out.push_str(if empty { layout.pad() } else { layout.comma() });
        empty = false;
        json::write_string(key, &mut out);
        out.push_str(layout.colon());
        out.push_str(&value);
    }
    if !empty {
        out.push_str(layout.pad());
    }
    out.push('}');
    out
}

/// `json_build_object(k1, v1, …)`: [`Layout::Spaced`] text, with duplicate keys
/// kept in the order they were given (`jsonb_build_object` keeps only the last).
fn build_json_object(vals: &[Datum], ctx: &EvalCtx) -> Result<Datum, ExecError> {
    if !vals.len().is_multiple_of(2) {
        return Err(invalid_parameter(
            "argument list must have even number of elements",
        ));
    }
    let mut pairs = Vec::with_capacity(vals.len() / 2);
    for pair in vals.chunks_exact(2) {
        let key = match &pair[0] {
            Datum::Null => return Err(object_key_null()),
            Datum::Json(_) | Datum::Jsonb(_) | Datum::Array(_) | Datum::Record(_) => {
                return Err(invalid_parameter(
                    "key value must be scalar, not array, composite, or json",
                ));
            }
            other => json_object_key_text(other, ctx)?,
        };
        pairs.push((key, to_json_text(&pair[1], Layout::Compact, ctx)?));
    }
    Ok(Datum::Json(write_object(
        Layout::Spaced,
        pairs.iter().map(|(k, v)| (k.as_str(), v.clone())),
    )))
}

/// A scalar's spelling as a `json` object key.
///
/// `PostgreSQL` renders the key through the same conversion as a value and then
/// quotes whatever came out, so a key follows the *JSON* spelling rather than the
/// SQL one — `true` not `t`, `1e+30` not `1000…0`, and `2020-01-02T03:04:05`
/// rather than the space-separated form. Container and NULL keys are rejected by
/// the caller, so every key that reaches here is a scalar.
fn json_object_key_text(d: &Datum, ctx: &EvalCtx) -> Result<String, ExecError> {
    let rendered = to_json_text(d, Layout::Compact, ctx)?;
    Ok(json::unescape(&rendered).unwrap_or(rendered))
}

/// `json_build_object` / `json_object` on a NULL key. `jsonb_build_object`
/// reports a different error (`argument N: key must not be null`), so this is
/// deliberately not shared with [`build_object`].
fn object_key_null() -> ExecError {
    ExecError::FunctionError {
        sqlstate: "22004",
        message: "null value not allowed for object key".into(),
    }
}

// ---- the `json` text reader ----

/// The `json` document an argument denotes: a `json` value's stored text, or a
/// `text` value put through `json_in`.
///
/// The plan-time check has already refused a `jsonb` argument to a `json_*`
/// function, so the remaining `text` case only arises on paths that never ran
/// it — validating rather than failing keeps those working.
fn json_operand<'a>(d: &'a Datum, name: &str) -> Result<Cow<'a, str>, ExecError> {
    match d {
        Datum::Json(text) => Ok(Cow::Borrowed(text.as_str())),
        Datum::Text(s) => {
            json::validate(s)?;
            Ok(Cow::Borrowed(s.as_str()))
        }
        other => Err(type_error(name, other)),
    }
}

/// An object field's ORIGINAL text. `json` keeps duplicate keys, and
/// `PostgreSQL`'s scanner overwrites its match as it goes, so the LAST field
/// with this key wins: `'{"a":1,"a":2}'::json -> 'a'` is `2`.
fn json_field<'a>(doc: &'a str, key: &str) -> Option<&'a str> {
    json::object_fields(doc)?
        .into_iter()
        .rfind(|(k, _)| k == key)
        .map(|(_, value)| value)
}

/// An array element's ORIGINAL text, counting from the end for a negative index.
///
/// Unlike `jsonb`, a `json` SCALAR is not an array of one: `'"s"'::json -> 0` is
/// SQL NULL where `'"s"'::jsonb -> 0` is `"s"`. Nothing here special-cases that
/// — [`json::array_elements`] simply declines a document that is not an array.
fn json_element(doc: &str, index: i64) -> Option<&str> {
    let items = json::array_elements(doc)?;
    items.get(resolve_index(index, items.len())?).copied()
}

/// One `->`/`->>` step over `json` text.
fn json_extract<'a>(doc: &'a str, subscript: &Subscript) -> Option<&'a str> {
    match subscript {
        Subscript::Key(key) => json_field(doc, key),
        Subscript::Index(index) => json_element(doc, *index),
    }
}

/// Follow a `#>` / `json_extract_path` path through `json` text. A NULL path
/// element makes the whole lookup miss, and a scalar cannot be stepped into —
/// so `'"s"'::json #> '{0}'` is SQL NULL, matching the `jsonb` path operators
/// rather than the `jsonb` subscript operators.
fn json_navigate<'a>(doc: &'a str, path: &[Option<String>]) -> Option<&'a str> {
    let mut current = doc;
    for step in path {
        let step = step.as_deref()?;
        current = match json::kind(current) {
            Kind::Object => json_field(current, step)?,
            Kind::Array => json_element(current, step.parse::<i64>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

/// The `->>`/`#>>` rendering of `json` sub-text: the `null` literal becomes SQL
/// NULL, a JSON string is de-escaped, and every other value keeps its ORIGINAL
/// text — spacing included, which is where this parts company with `jsonb`.
fn json_as_sql_text(raw: &str) -> Datum {
    if json::kind(raw) == Kind::Null {
        Datum::Null
    } else {
        Datum::Text(json::as_text(raw))
    }
}

/// The `json` sibling of [`jsonb_srf_rows`]: expand one `json` document for
/// `json_each` / `json_each_text` / `json_object_keys` / `json_array_elements`
/// / `json_array_elements_text`, preserving input order and duplicate keys.
///
/// The wrong-shape errors are `PostgreSQL`'s `json` wording, which is not the
/// `jsonb` wording — `json_each('[]')` is `cannot deconstruct an array as an
/// object` where `jsonb_each('[]')` is `cannot call jsonb_each on a non-object`.
pub(crate) fn json_srf_rows(kind: JsonbSrf, vals: &[Datum]) -> Result<Vec<Vec<Datum>>, ExecError> {
    let name = match kind {
        JsonbSrf::Each => "json_each",
        JsonbSrf::EachText => "json_each_text",
        JsonbSrf::ObjectKeys => "json_object_keys",
        JsonbSrf::ArrayElements => "json_array_elements",
        JsonbSrf::ArrayElementsText => "json_array_elements_text",
    };
    let doc = json_operand(&vals[0], name)?;
    let shape = json::kind(&doc);
    Ok(match kind {
        JsonbSrf::Each | JsonbSrf::EachText => {
            let Some(fields) = json::object_fields(&doc) else {
                return Err(invalid_parameter(match shape {
                    Kind::Array => "cannot deconstruct an array as an object",
                    _ => "cannot deconstruct a scalar",
                }));
            };
            fields
                .into_iter()
                .map(|(key, value)| {
                    let cell = if kind == JsonbSrf::Each {
                        Datum::Json(value.to_string())
                    } else {
                        json_as_sql_text(value)
                    };
                    vec![Datum::Text(key), cell]
                })
                .collect()
        }
        JsonbSrf::ObjectKeys => {
            let Some(fields) = json::object_fields(&doc) else {
                return Err(invalid_parameter(match shape {
                    Kind::Array => "cannot call json_object_keys on an array",
                    _ => "cannot call json_object_keys on a scalar",
                }));
            };
            fields
                .into_iter()
                .map(|(key, _)| vec![Datum::Text(key)])
                .collect()
        }
        JsonbSrf::ArrayElements | JsonbSrf::ArrayElementsText => {
            let Some(items) = json::array_elements(&doc) else {
                return Err(invalid_parameter(match (kind, shape) {
                    (JsonbSrf::ArrayElements, Kind::Object) => {
                        "cannot call json_array_elements on a non-array"
                    }
                    (JsonbSrf::ArrayElements, _) => "cannot call json_array_elements on a scalar",
                    (_, Kind::Object) => "cannot call json_array_elements_text on a non-array",
                    _ => "cannot call json_array_elements_text on a scalar",
                }));
            };
            items
                .into_iter()
                .map(|item| {
                    let cell = if kind == JsonbSrf::ArrayElements {
                        Datum::Json(item.to_string())
                    } else {
                        json_as_sql_text(item)
                    };
                    vec![cell]
                })
                .collect()
        }
    })
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
    let text_array = ColumnType::Array(ElemType::Text);
    let integral = matches!(right, ColumnType::Int4 | ColumnType::Int8);
    // `json` has FOUR operators and no more: PostgreSQL defines no `=`, `<`,
    // `@>`, `<@`, `?`, `?|`, `?&`, `||`, `-`, `#-`, `@?` or `@@` for it, so
    // every other `op` falls through to 42883 rather than borrowing `jsonb`'s.
    if left == ColumnType::Json {
        return match op {
            JsonOp::Get if right.is_string() || integral => Some(ColumnType::Json),
            JsonOp::GetText if right.is_string() || integral => Some(ColumnType::Text),
            JsonOp::GetPath if right == text_array => Some(ColumnType::Json),
            JsonOp::GetPathText if right == text_array => Some(ColumnType::Text),
            _ => None,
        };
    }
    if left != ColumnType::Jsonb {
        return None;
    }
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
        JsonOp::PathExists | JsonOp::PathMatch if right == ColumnType::JsonPath => {
            Some(ColumnType::Bool)
        }
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

/// `json -> …` / `json #> …`: the sub-document's ORIGINAL text, and — for the
/// `_text` operators — that text de-escaped. `Ok(None)` means "not a `json`
/// left operand", so the caller falls through to the `jsonb` implementation.
///
/// The four extraction operators are the only ones `json` has, so this is the
/// whole of `json`'s operator surface.
fn json_text_operator(op: JsonOp, left: &Datum, right: &Datum) -> Result<Option<Datum>, ExecError> {
    let Datum::Json(doc) = left else {
        return Ok(None);
    };
    let found = match op {
        JsonOp::Get | JsonOp::GetText => match subscript_operand(op, left, right)? {
            None => return Ok(Some(Datum::Null)),
            Some(subscript) => json_extract(doc, &subscript),
        },
        _ => match array_operand(op, left, right)? {
            None => return Ok(Some(Datum::Null)),
            Some(path) => json_navigate(doc, &path),
        },
    };
    let as_document = matches!(op, JsonOp::Get | JsonOp::GetPath);
    Ok(Some(match (found, as_document) {
        (None, _) => Datum::Null,
        (Some(sub), true) => Datum::Json(sub.to_string()),
        (Some(sub), false) => json_as_sql_text(sub),
    }))
}

/// `jsonb -> text` / `jsonb -> integer`: an object field or an array element
/// (negative indexes count from the end). A missing field/index is SQL NULL; a
/// JSON `null` value is the JSON null, which is *not* SQL NULL.
///
/// A `json` left operand takes the text route instead, which returns the field's
/// original bytes rather than a re-rendered document.
pub(crate) fn json_get(left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    if let Some(result) = json_text_operator(JsonOp::Get, left, right)? {
        return Ok(result);
    }
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
    if let Some(result) = json_text_operator(JsonOp::GetText, left, right)? {
        return Ok(result);
    }
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
    if let Some(result) = json_text_operator(JsonOp::GetPath, left, right)? {
        return Ok(result);
    }
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
    if let Some(result) = json_text_operator(JsonOp::GetPathText, left, right)? {
        return Ok(result);
    }
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

/// The right-hand subscript of `->`/`->>`, whatever the left operand's type;
/// `Ok(None)` when it is SQL NULL.
fn subscript_operand(
    op: JsonOp,
    left: &Datum,
    right: &Datum,
) -> Result<Option<Subscript>, ExecError> {
    Ok(Some(match right {
        Datum::Null => return Ok(None),
        Datum::Text(s) => Subscript::Key(s.clone()),
        Datum::Int4(n) => Subscript::Index(i64::from(*n)),
        Datum::Int8(n) => Subscript::Index(*n),
        other => return Err(operator_undefined(op, left, other)),
    }))
}

/// The right-hand `text[]` of `#>`/`#>>`/`?|`/`?&`, whatever the left operand's
/// type; `Ok(None)` when it is SQL NULL.
fn array_operand(
    op: JsonOp,
    left: &Datum,
    right: &Datum,
) -> Result<Option<Vec<Option<String>>>, ExecError> {
    Ok(Some(match right {
        Datum::Null => return Ok(None),
        Datum::Array(a) if a.elem == ElemType::Text => array_path(a),
        other => return Err(operator_undefined(op, left, other)),
    }))
}

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
    let Some(subscript) = subscript_operand(op, left, right)? else {
        return Ok(None);
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
    let Some(path) = array_operand(op, left, right)? else {
        return Ok(None);
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

fn jsonpath_arg<'a>(d: &'a Datum, name: &str) -> Result<&'a str, ExecError> {
    match d {
        Datum::JsonPath(s) => Ok(s),
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
        extra_float_digits: ctx.extra_float_digits,
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

    // ---- `json` is not `jsonb`: text preservation, spacing, and the split
    // function families. Every expectation below was read off PostgreSQL 18.4.

    fn jn(text: &str) -> Datum {
        Datum::Json(text.to_string())
    }

    fn json_expr(text: &str) -> Expr {
        Expr::Cast {
            expr: Box::new(Expr::StringLiteral(text.to_string())),
            ty: ColumnType::Json,
        }
    }

    /// The whole point of the type: every `json` accessor hands back the bytes
    /// the document was written with, where the `jsonb` accessor hands back a
    /// re-rendered document.
    #[test]
    fn json_accessors_return_the_original_sub_text() {
        let doc = r#"{"a":{"b":  1},"s":"x\ty","n":null,"arr":[1,  2]}"#;
        // (subscript, `->`, `->>`)
        let cases: [(&str, Datum, Datum); 5] = [
            ("a", jn("{\"b\":  1}"), t("{\"b\":  1}")),
            // `->>` de-escapes a JSON *string* and leaves everything else alone.
            ("s", jn(r#""x\ty""#), t("x\ty")),
            // The JSON `null` literal is a value for `->` and SQL NULL for `->>`.
            ("n", jn("null"), Datum::Null),
            ("arr", jn("[1,  2]"), t("[1,  2]")),
            ("missing", Datum::Null, Datum::Null),
        ];
        for (key, get, get_text) in cases {
            assert!(json_get(&jn(doc), &t(key)).expect("->") == get, "-> {key}");
            assert!(
                json_get_text(&jn(doc), &t(key)).expect("->>") == get_text,
                "->> {key}"
            );
            assert!(
                json_get_path(&jn(doc), &text_array(&[key])).expect("#>") == get,
                "#> {key}"
            );
            assert!(
                json_get_path_text(&jn(doc), &text_array(&[key])).expect("#>>") == get_text,
                "#>> {key}"
            );
            assert!(
                call("json_extract_path", vec![json_expr(doc), u(key)]).expect("extract_path")
                    == get,
                "json_extract_path {key}"
            );
            assert!(
                call("json_extract_path_text", vec![json_expr(doc), u(key)])
                    .expect("extract_path_text")
                    == get_text,
                "json_extract_path_text {key}"
            );
        }
        // The same document through `jsonb` loses the spacing, which is what
        // makes the two families genuinely different implementations.
        assert!(json_get(&j(doc), &t("a")).expect("jsonb ->") == j(r#"{"b":1}"#));
    }

    /// Array indexing over `json`, including the one place it parts company with
    /// `jsonb`: a `json` scalar is NOT a one-element array.
    #[test]
    fn json_indexes_arrays_but_never_scalars() {
        let cases: [(&str, i32, Datum); 6] = [
            ("[1,  2]", 0, jn("1")),
            ("[1,  2]", -1, jn("2")),
            ("[1,  2]", 5, Datum::Null),
            // `'"s"'::jsonb -> 0` is `"s"`; the `json` operator misses.
            (r#""s""#, 0, Datum::Null),
            ("5", 0, Datum::Null),
            (r#"{"a":1}"#, 0, Datum::Null),
        ];
        for (doc, index, want) in cases {
            assert!(
                json_get(&jn(doc), &Datum::Int4(index)).expect("->") == want,
                "{doc} -> {index}"
            );
        }
        // A `json` document keeps duplicate keys, and the LAST one wins.
        assert!(json_get(&jn(r#"{"a":1,"a":2}"#), &t("a")).expect("->") == jn("2"));
        // A path step into a scalar simply misses.
        assert!(json_get_path(&jn(r#""s""#), &text_array(&["0"])).expect("#>") == Datum::Null);
        // The empty path is the document itself, text and all.
        assert!(
            json_get_path(&jn(r#"{"a":  1}"#), &text_array(&[])).expect("#>") == jn(r#"{"a":  1}"#)
        );
    }

    /// `PostgreSQL` uses four different spacings across the JSON constructors and
    /// they are not interchangeable.
    #[test]
    fn each_constructor_writes_its_own_spacing() {
        let row = Expr::Row(vec![Expr::IntLiteral("1".to_string()), u("x")]);
        let array = Expr::Cast {
            expr: Box::new(u("{1,2}")),
            ty: ColumnType::Array(ElemType::Int4),
        };
        let cases: [(&str, Vec<Expr>, Datum); 8] = [
            // Compact: `composite_to_json` / `array_to_json`.
            ("row_to_json", vec![row.clone()], jn(r#"{"f1":1,"f2":"x"}"#)),
            ("to_json", vec![row.clone()], jn(r#"{"f1":1,"f2":"x"}"#)),
            ("array_to_json", vec![array.clone()], jn("[1,2]")),
            ("to_json", vec![array.clone()], jn("[1,2]")),
            // Spaced: the builders.
            (
                "json_build_object",
                vec![
                    u("a"),
                    Expr::IntLiteral("1".to_string()),
                    u("b"),
                    Expr::IntLiteral("2".to_string()),
                ],
                jn(r#"{"a" : 1, "b" : 2}"#),
            ),
            (
                "json_build_array",
                vec![Expr::IntLiteral("1".to_string()), u("x"), Expr::NullLiteral],
                jn(r#"[1, "x", null]"#),
            ),
            (
                "json_object",
                vec![text_array_expr(&["a", "1", "b", "2"])],
                jn(r#"{"a" : "1", "b" : "2"}"#),
            ),
            // ... and `jsonb`'s single rendering, which is none of the above.
            (
                "jsonb_build_object",
                vec![
                    u("a"),
                    Expr::IntLiteral("1".to_string()),
                    u("b"),
                    Expr::IntLiteral("2".to_string()),
                ],
                j(r#"{"a": 1, "b": 2}"#),
            ),
        ];
        for (name, args, want) in cases {
            assert!(call(name, args).expect(name) == want, "{name}");
        }
    }

    /// A `json` value placed inside a constructor is inlined verbatim; a `jsonb`
    /// one arrives through its own output function; and NOTHING propagates the
    /// outer spacing inward.
    #[test]
    fn constructors_inline_json_verbatim_and_nest_compactly() {
        let nested = r#"{"b":1,  "a":2}"#;
        let array = Expr::Cast {
            expr: Box::new(u("{1,2}")),
            ty: ColumnType::Array(ElemType::Int4),
        };
        let cases: [(&str, Vec<Expr>, Datum); 4] = [
            (
                "json_build_object",
                vec![u("k"), json_expr(nested)],
                jn(r#"{"k" : {"b":1,  "a":2}}"#),
            ),
            (
                "json_build_object",
                vec![u("k"), jsonb_expr(nested)],
                jn(r#"{"k" : {"a": 2, "b": 1}}"#),
            ),
            // A spaced object holding a COMPACT array.
            (
                "json_build_object",
                vec![u("k"), array.clone()],
                jn(r#"{"k" : [1,2]}"#),
            ),
            (
                "json_build_array",
                vec![
                    array,
                    Expr::Row(vec![
                        Expr::IntLiteral("1".to_string()),
                        Expr::IntLiteral("2".to_string()),
                    ]),
                ],
                jn(r#"[[1,2], {"f1":1,"f2":2}]"#),
            ),
        ];
        for (name, args, want) in cases {
            assert!(call(name, args).expect(name) == want, "{name}");
        }
        // The same rule stated against the helper `srf.rs` and `agg.rs` share:
        // `layout` punctuates the outermost container only.
        let record = Datum::Record(crabka_pgtypes::RecordValue::anonymous(vec![
            Datum::Int4(1),
            Datum::Array(ArrayValue::new(
                ElemType::Int4,
                vec![Datum::Int4(2), Datum::Int4(3)],
            )),
        ]));
        assert!(
            to_json_text(&record, Layout::Compact, &ctx()).expect("compact")
                == r#"{"f1":1,"f2":[2,3]}"#
        );
        assert!(
            to_json_text(&record, Layout::Spaced, &ctx()).expect("spaced")
                == r#"{"f1" : 1, "f2" : [2,3]}"#
        );
        assert!(
            to_json_text(&record, Layout::Padded, &ctx()).expect("padded")
                == r#"{ "f1" : 1, "f2" : [2,3] }"#
        );
    }

    /// `row_to_json(…, true)` / `array_to_json(…, true)`: `use_line_feeds` breaks
    /// the TOP level onto fresh lines and leaves every nested level compact.
    #[test]
    fn the_pretty_flag_breaks_only_the_top_level() {
        let array = Expr::Cast {
            expr: Box::new(u("{1,2,3}")),
            ty: ColumnType::Array(ElemType::Int4),
        };
        // A composite whose second field is itself a container: the nested one
        // stays on one line however the flag is set.
        let row = Expr::Row(vec![
            Expr::IntLiteral("1".to_string()),
            Expr::Cast {
                expr: Box::new(u("{2,3}")),
                ty: ColumnType::Array(ElemType::Int4),
            },
        ]);
        let cases: [(&str, Vec<Expr>, Datum); 4] = [
            (
                "array_to_json",
                vec![array.clone(), Expr::BoolLiteral(true)],
                jn("[1,\n 2,\n 3]"),
            ),
            (
                "array_to_json",
                vec![array, Expr::BoolLiteral(false)],
                jn("[1,2,3]"),
            ),
            (
                "row_to_json",
                vec![row.clone(), Expr::BoolLiteral(true)],
                jn("{\"f1\":1,\n \"f2\":[2,3]}"),
            ),
            (
                "row_to_json",
                vec![row, Expr::BoolLiteral(false)],
                jn(r#"{"f1":1,"f2":[2,3]}"#),
            ),
        ];
        for (name, args, want) in cases {
            assert!(call(name, args).expect(name) == want, "{name}");
        }
    }

    /// `json_strip_nulls` is the one `json` function that does NOT preserve its
    /// input's spacing — it re-serialises compactly — but it does keep key order
    /// and duplicate keys.
    #[test]
    fn json_strip_nulls_reserialises_compactly_but_keeps_order_and_duplicates() {
        let cases: [(&str, Option<bool>, Datum); 4] = [
            (
                r#"[1, null,  {"a":null,  "b": 2}]"#,
                None,
                jn(r#"[1,null,{"b":2}]"#),
            ),
            (
                r#"{"b":1, "a":null, "b":  3}"#,
                None,
                jn(r#"{"b":1,"b":3}"#),
            ),
            ("[1, null]", Some(true), jn("[1]")),
            ("[1, null]", Some(false), jn("[1,null]")),
        ];
        for (doc, in_arrays, want) in cases {
            let mut args = vec![json_expr(doc)];
            if let Some(flag) = in_arrays {
                args.push(Expr::BoolLiteral(flag));
            }
            assert!(call("json_strip_nulls", args).expect(doc) == want, "{doc}");
        }
    }

    /// `to_json(json)` is the identity; `to_jsonb(json)` is the parse that throws
    /// the text away.
    #[test]
    fn to_json_keeps_the_text_and_to_jsonb_parses_it() {
        let doc = r#"{"b":1,  "a":2}"#;
        assert!(call("to_json", vec![json_expr(doc)]).expect("to_json") == jn(doc));
        assert!(call("to_jsonb", vec![json_expr(doc)]).expect("to_jsonb") == j(doc));
        // `to_json(jsonb)` goes the other way, through `jsonb`'s output function.
        assert!(
            call("to_json", vec![jsonb_expr(doc)]).expect("to_json") == jn(r#"{"a": 2, "b": 1}"#)
        );
    }

    /// `datum_to_json` prints a number's OWN output text when that text is a
    /// valid JSON number and quotes it when it is not — which is why
    /// `to_json(1e30::float8)` and `to_jsonb(1e30::float8)` disagree.
    #[test]
    fn to_json_text_renders_scalars_through_their_output_function() {
        let cases: [(Datum, &str); 9] = [
            (Datum::Int4(1), "1"),
            (Datum::Bool(true), "true"),
            (Datum::Null, "null"),
            (
                Datum::Numeric(numeric::parse("1.50").expect("numeric")),
                "1.50",
            ),
            (Datum::Float8(1e30), "1e+30"),
            (Datum::Float8(0.1), "0.1"),
            // JSON has no non-finite number, so these become JSON strings.
            (Datum::Float8(f64::NAN), r#""NaN""#),
            (Datum::Float8(f64::INFINITY), r#""Infinity""#),
            (t("a\"b"), r#""a\"b""#),
        ];
        for (value, want) in cases {
            assert!(
                to_json_text(&value, Layout::Compact, &ctx()).expect("to_json_text") == want,
                "{value:?}"
            );
        }
    }

    /// The names `PostgreSQL` only defines for `jsonb` have no `json_` sibling,
    /// so they are `function … does not exist` rather than a working alias.
    #[test]
    fn the_jsonb_only_functions_have_no_json_spelling() {
        for name in [
            "json_pretty",
            "json_set",
            "json_set_lax",
            "json_insert",
            "json_delete_path",
            "json_path_exists",
            "json_path_match",
            "json_path_query_array",
            "json_path_query_first",
            "json_contains",
            "json_contained",
            "json_exists_any",
            "json_exists_all",
            "json_delete",
            "json_concat",
        ] {
            assert!(!is_json_func(name), "{name}");
            assert!(json_func(name).is_none(), "{name}");
        }
        // ... and the `json`-only ones have no `jsonb_` sibling either.
        for name in ["row_to_jsonb", "array_to_jsonb"] {
            assert!(!is_json_func(name), "{name}");
        }
    }

    /// Neither family accepts the other's document, and neither accepts a value
    /// of an unrelated type: all of these are 42883 in `PostgreSQL`.
    #[test]
    fn each_family_rejects_the_other_types_document() {
        let cases: [(&str, Vec<Expr>); 10] = [
            ("json_typeof", vec![jsonb_expr("{}")]),
            ("jsonb_typeof", vec![json_expr("{}")]),
            ("json_array_length", vec![jsonb_expr("[]")]),
            ("jsonb_array_length", vec![json_expr("[]")]),
            ("json_strip_nulls", vec![jsonb_expr("{}")]),
            ("jsonb_strip_nulls", vec![json_expr("{}")]),
            ("jsonb_pretty", vec![json_expr("{}")]),
            ("json_extract_path", vec![jsonb_expr("{}"), u("a")]),
            ("jsonb_extract_path", vec![json_expr("{}"), u("a")]),
            // An unrelated argument type is the same 42883.
            ("json_typeof", vec![Expr::IntLiteral("1".to_string())]),
        ];
        for (name, args) in cases {
            // 42883 is what a client sees: the plan-time resolver runs first, so
            // the call never reaches evaluation. The run-time guard behind it
            // reports the family's 42804 `does not accept an argument of type …`
            // instead, so this only asserts that it also refuses.
            let err = result_type(name, args.clone()).expect_err(name);
            assert!(sqlstate(err) == "42883", "{name} plan time");
            assert!(call(name, args).is_err(), "{name} run time");
        }
        // A genuinely `text`-typed argument needs an explicit cast, exactly as
        // it does for `jsonb`.
        let as_text = Expr::Cast {
            expr: Box::new(u("{}")),
            ty: ColumnType::Text,
        };
        assert!(sqlstate(result_type("json_typeof", vec![as_text]).expect_err("text")) == "42883");
        // `row_to_json` / `array_to_json` take a composite and an array
        // respectively, and nothing else.
        for (name, arg) in [
            ("row_to_json", Expr::IntLiteral("1".to_string())),
            ("array_to_json", Expr::IntLiteral("1".to_string())),
            ("row_to_json", json_expr("{}")),
            (
                "array_to_json",
                Expr::Row(vec![Expr::IntLiteral("1".to_string())]),
            ),
        ] {
            assert!(
                sqlstate(result_type(name, vec![arg]).expect_err(name)) == "42883",
                "{name}"
            );
        }
    }

    /// The `json_*` spellings take `json` and answer in `json`, position for
    /// position with their `jsonb_*` counterparts.
    #[test]
    fn the_two_families_infer_their_own_document_type() {
        let cases: [(&str, Vec<Expr>, ColumnType); 14] = [
            ("json_typeof", vec![u("{}")], ColumnType::Text),
            ("jsonb_typeof", vec![u("{}")], ColumnType::Text),
            ("json_array_length", vec![u("[]")], ColumnType::Int4),
            ("json_build_object", vec![], ColumnType::Json),
            ("jsonb_build_object", vec![], ColumnType::Jsonb),
            ("json_build_array", vec![], ColumnType::Json),
            ("jsonb_build_array", vec![], ColumnType::Jsonb),
            ("json_strip_nulls", vec![u("{}")], ColumnType::Json),
            ("jsonb_strip_nulls", vec![u("{}")], ColumnType::Jsonb),
            (
                "json_object",
                vec![text_array_expr(&["a", "1"])],
                ColumnType::Json,
            ),
            (
                "jsonb_object",
                vec![text_array_expr(&["a", "1"])],
                ColumnType::Jsonb,
            ),
            (
                "to_json",
                vec![Expr::IntLiteral("1".to_string())],
                ColumnType::Json,
            ),
            (
                "to_jsonb",
                vec![Expr::IntLiteral("1".to_string())],
                ColumnType::Jsonb,
            ),
            (
                "row_to_json",
                vec![Expr::Row(vec![Expr::IntLiteral("1".to_string())])],
                ColumnType::Json,
            ),
        ];
        for (name, args, want) in cases {
            assert!(result_type(name, args).expect(name) == want, "{name}");
        }
        // `json_extract_path` answers in `json`, its `_text` sibling in `text`.
        assert!(
            result_type("json_extract_path", vec![u("{}"), u("a")]).expect("extract_path")
                == ColumnType::Json
        );
        assert!(
            result_type("json_extract_path_text", vec![u("{}"), u("a")]).expect("text")
                == ColumnType::Text
        );
    }

    /// An unadorned literal adopts `json` for a `json_*` parameter, so
    /// `json_typeof('{}')` works without a cast and reads the text.
    #[test]
    fn an_unknown_literal_adopts_json_for_the_json_family() {
        let cases: [(&str, Vec<Expr>, Datum); 5] = [
            ("json_typeof", vec![u(r#"{"a":1}"#)], t("object")),
            ("json_typeof", vec![u("null")], t("null")),
            ("json_typeof", vec![u(r#""s""#)], t("string")),
            ("json_array_length", vec![u("[1,2,3]")], Datum::Int4(3)),
            // Strict, as in PostgreSQL.
            ("json_typeof", vec![Expr::NullLiteral], Datum::Null),
        ];
        for (name, args, want) in cases {
            assert!(call(name, args).expect(name) == want, "{name}");
        }
        // `json_array_length`'s two wrong-shape errors, which name the shape.
        for (doc, message) in [
            ("{}", "cannot get array length of a non-array"),
            ("1", "cannot get array length of a scalar"),
        ] {
            let err = call("json_array_length", vec![u(doc)]).expect_err(doc);
            let reported = err.into_pg();
            assert!(
                (reported.code.as_str(), reported.message.as_str()) == ("22023", message),
                "{doc}"
            );
        }
    }

    /// `json` has FOUR operators. Every other operator `jsonb` has is 42883 for
    /// a `json` left operand, which is why `json` also has no equality and so no
    /// btree opclass.
    #[test]
    fn json_resolves_only_the_four_extraction_operators() {
        let text_array_type = ColumnType::Array(ElemType::Text);
        let resolves: [(JsonOp, ColumnType, ColumnType); 6] = [
            (JsonOp::Get, ColumnType::Text, ColumnType::Json),
            (JsonOp::Get, ColumnType::Int4, ColumnType::Json),
            (JsonOp::GetText, ColumnType::Text, ColumnType::Text),
            (JsonOp::GetText, ColumnType::Int4, ColumnType::Text),
            (JsonOp::GetPath, text_array_type, ColumnType::Json),
            (JsonOp::GetPathText, text_array_type, ColumnType::Text),
        ];
        for (op, right, want) in resolves {
            assert!(
                json_operator_result_type(op, ColumnType::Json, right) == Some(want),
                "{} json {}",
                op.spelling(),
                right.name()
            );
        }
        for op in [
            JsonOp::Contains,
            JsonOp::ContainedBy,
            JsonOp::KeyExists,
            JsonOp::KeyExistsAny,
            JsonOp::KeyExistsAll,
            JsonOp::Concat,
            JsonOp::Delete,
            JsonOp::PathExists,
            JsonOp::PathMatch,
        ] {
            for right in [
                ColumnType::Json,
                ColumnType::Jsonb,
                ColumnType::Text,
                text_array_type,
                ColumnType::JsonPath,
            ] {
                assert!(
                    json_operator_result_type(op, ColumnType::Json, right).is_none(),
                    "json {} {}",
                    op.spelling(),
                    right.name()
                );
            }
        }
        // The `jsonb` operators are untouched: they still refuse a `json` right
        // operand and still resolve for a `jsonb` one.
        assert!(
            json_operator_result_type(JsonOp::Contains, ColumnType::Jsonb, ColumnType::Jsonb)
                == Some(ColumnType::Bool)
        );
        assert!(
            json_operator_result_type(JsonOp::Contains, ColumnType::Jsonb, ColumnType::Json)
                .is_none()
        );
    }

    /// `json_each` and friends walk the text, so they see input order and every
    /// duplicate key — and they report a wrong-shaped document in PostgreSQL's
    /// `json` wording, which is not the `jsonb` wording.
    #[test]
    fn json_srf_rows_preserves_input_order_and_duplicate_keys() {
        let object = jn(r#"{"b":1,   "a":"x\ty",  "b":{"c":  3}}"#);
        assert!(
            json_srf_rows(JsonbSrf::Each, std::slice::from_ref(&object)).expect("json_each")
                == vec![
                    vec![t("b"), jn("1")],
                    vec![t("a"), jn(r#""x\ty""#)],
                    vec![t("b"), jn("{\"c\":  3}")],
                ]
        );
        assert!(
            json_srf_rows(JsonbSrf::EachText, std::slice::from_ref(&object))
                .expect("json_each_text")
                == vec![
                    vec![t("b"), t("1")],
                    vec![t("a"), t("x\ty")],
                    vec![t("b"), t("{\"c\":  3}")],
                ]
        );
        assert!(
            json_srf_rows(JsonbSrf::ObjectKeys, std::slice::from_ref(&object)).expect("keys")
                == vec![vec![t("b")], vec![t("a")], vec![t("b")]]
        );
        let array = jn(r#"[1,  {"a":  2}, "x\ty", null]"#);
        assert!(
            json_srf_rows(JsonbSrf::ArrayElements, std::slice::from_ref(&array)).expect("elements")
                == vec![
                    vec![jn("1")],
                    vec![jn("{\"a\":  2}")],
                    vec![jn(r#""x\ty""#)],
                    vec![jn("null")],
                ]
        );
        assert!(
            json_srf_rows(JsonbSrf::ArrayElementsText, std::slice::from_ref(&array))
                .expect("elements_text")
                == vec![
                    vec![t("1")],
                    vec![t("{\"a\":  2}")],
                    vec![t("x\ty")],
                    vec![Datum::Null],
                ]
        );
    }

    /// The wrong-shape errors, which `PostgreSQL` words differently for `json`
    /// than for `jsonb`.
    #[test]
    fn the_json_srfs_report_postgres_json_wording() {
        let cases: [(JsonbSrf, &str, &str); 8] = [
            (
                JsonbSrf::Each,
                "[]",
                "cannot deconstruct an array as an object",
            ),
            (JsonbSrf::Each, "1", "cannot deconstruct a scalar"),
            (
                JsonbSrf::EachText,
                "[]",
                "cannot deconstruct an array as an object",
            ),
            (JsonbSrf::EachText, "1", "cannot deconstruct a scalar"),
            (
                JsonbSrf::ObjectKeys,
                "[]",
                "cannot call json_object_keys on an array",
            ),
            (
                JsonbSrf::ObjectKeys,
                "1",
                "cannot call json_object_keys on a scalar",
            ),
            (
                JsonbSrf::ArrayElements,
                "{}",
                "cannot call json_array_elements on a non-array",
            ),
            (
                JsonbSrf::ArrayElementsText,
                "1",
                "cannot call json_array_elements_text on a scalar",
            ),
        ];
        for (kind, doc, message) in cases {
            let err = json_srf_rows(kind, &[jn(doc)]).expect_err(doc);
            let reported = err.into_pg();
            assert!(
                (reported.code.as_str(), reported.message.as_str()) == ("22023", message),
                "{kind:?} {doc}"
            );
        }
        // The `jsonb` SRFs keep their own, different wording.
        let err = jsonb_srf_rows(JsonbSrf::Each, &[j("[]")]).expect_err("jsonb_each");
        assert!(err.into_pg().message == "cannot call jsonb_each on a non-object");
    }

    /// The two builders' NULL-key errors differ between the families, and
    /// `json_build_object` rejects a container key.
    #[test]
    fn the_json_builders_report_postgres_json_errors() {
        let cases: [(&str, Vec<Expr>, &str, &str); 5] = [
            (
                "json_build_object",
                vec![Expr::NullLiteral, Expr::IntLiteral("2".to_string())],
                "22004",
                "null value not allowed for object key",
            ),
            (
                "json_build_object",
                vec![u("a")],
                "22023",
                "argument list must have even number of elements",
            ),
            (
                "json_build_object",
                vec![jsonb_expr("{}"), Expr::IntLiteral("2".to_string())],
                "22023",
                "key value must be scalar, not array, composite, or json",
            ),
            (
                "json_object",
                vec![text_array_expr(&["a"])],
                "2202E",
                "array must have even number of elements",
            ),
            (
                "json_object",
                vec![text_array_expr(&["a", "b"]), text_array_expr(&["1"])],
                "2202E",
                "mismatched array dimensions",
            ),
        ];
        for (name, args, code, message) in cases {
            let reported = call(name, args).expect_err(name).into_pg();
            assert!(
                (reported.code.as_str(), reported.message.as_str()) == (code, message),
                "{name}"
            );
        }
    }

    /// The SQL/JSON constructors produce `json`, honouring `RETURNING`.
    #[test]
    fn the_sql_json_constructors_produce_json() {
        use crabka_pgparser::ast::JsonItemType;

        let nested = r#"{"b":1,  "a":2}"#;
        let entries = vec![(u("a"), Expr::IntLiteral("1".to_string()))];
        let object = |returning| SqlJsonExpr::Object {
            entries: entries.clone(),
            absent_on_null: false,
            unique_keys: false,
            returning,
        };
        let cases: [(SqlJsonExpr, Datum); 6] = [
            (object(None), jn(r#"{"a" : 1}"#)),
            (object(Some(ColumnType::Jsonb)), j(r#"{"a": 1}"#)),
            (object(Some(ColumnType::Text)), t(r#"{"a" : 1}"#)),
            (
                SqlJsonExpr::Array {
                    items: vec![
                        Expr::IntLiteral("1".to_string()),
                        Expr::IntLiteral("2".to_string()),
                    ],
                    absent_on_null: false,
                    returning: None,
                },
                jn("[1, 2]"),
            ),
            (SqlJsonExpr::Scalar(json_expr(nested)), jn(nested)),
            (
                SqlJsonExpr::Parse {
                    expr: u(nested),
                    unique_keys: false,
                },
                jn(nested),
            ),
        ];
        let ctx = ctx();
        for (node, want) in cases {
            let got = eval_sql_json(&node, &ctx, |e| {
                crate::eval::eval(e, &Scope::empty(), &[], &ctx)
            })
            .expect("sql/json");
            assert!(got == want, "{node:?}");
        }
        // `JSON_SERIALIZE` writes the document's own text, spacing included.
        let serialized = eval_sql_json(
            &SqlJsonExpr::Serialize {
                expr: json_expr(nested),
                returning: None,
            },
            &ctx,
            |e| crate::eval::eval(e, &Scope::empty(), &[], &ctx),
        )
        .expect("serialize");
        assert!(serialized == t(nested));
        // `IS JSON` accepts a `json` operand rather than reporting 42804.
        assert!(is_json_operand_type(ColumnType::Json).is_ok());
        assert!(is_json(&jn("{}"), JsonItemType::Object, false).expect("is json"));
        assert!(!is_json(&jn("{}"), JsonItemType::Array, false).expect("is json"));
    }
}
