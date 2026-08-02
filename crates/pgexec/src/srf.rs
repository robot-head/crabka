//! Set-returning functions (SRFs) — the one registry that answers, for a
//! function name and its arguments, the *columns* the call produces and the
//! *rows* it expands to.
//!
//! Every SRF is described once, by [`plan`] (names + types, for RowDescription
//! and the extended protocol's `Describe`) and [`rows`] (values). Both the
//! FROM-item path ([`from_item`] / [`from_item_schema`]) and the
//! select-list path ([`project_rows_ordered`]) drive that single description, so
//! a prepared statement's `RowDescription` cannot drift from what execution
//! actually returns.
//!
//! Naming follows PostgreSQL: a single-column SRF names its column after the
//! function (`generate_series`, `unnest`), a multi-column one names them
//! individually (`jsonb_each` → `key`, `value`), a bare `AS u` on a
//! single-column item renames the column as well as the qualifier, and
//! `AS u(a, b)` renames each column positionally.
//!
//! ## Deliberate divergences from PostgreSQL
//!
//! - A **multi-column** SRF in the select list (`SELECT jsonb_each(j)`) is
//!   `0A000`: PostgreSQL returns one `record`-typed column, and crabka has no
//!   composite type to put in it.
//! - An SRF nested inside **another SRF's arguments**
//!   (`SELECT generate_series(1, generate_series(1, 2))`) is `0A000`;
//!   PostgreSQL lifts the inner call into its own ProjectSet level.
//! - An SRF in an **aggregate query's** select list is `0A000`; PostgreSQL
//!   evaluates SRFs after aggregation.
//! - `SELECT *` over a multi-argument `unnest(a, b)` written without column
//!   aliases is `42702`. PostgreSQL names both output columns `unnest` and
//!   expands `*` positionally, while crabka expands a wildcard into one
//!   *by-name* column reference each, so two identically named columns under one
//!   qualifier are ambiguous. `unnest(a, b) AS t(x, y)` names them apart and
//!   works. (The same by-name expansion makes `SELECT *` over a derived table
//!   with duplicate output names ambiguous, so this is not specific to SRFs.)
//!
//! Multiple SRFs in one select list *are* supported and follow PostgreSQL 10+
//! semantics — the calls run in lockstep and the shorter ones pad with NULL
//! until the longest is exhausted (the pre-10 "least common multiple" rule is
//! gone from PostgreSQL itself).

use std::borrow::Cow;

use crabka_pgparser::ast::{ArraySubscript, Expr, FuncArgs, SelectItem, SelectStmt, TableFuncCall};
use crabka_pgtypes::{ColumnType, Datum, ElemType, TypeError, numeric::NumericValue};
use crabka_pgwire::engine::FieldDescription;

use crate::{
    clock::EvalCtx,
    error::ExecError,
    eval::ArgType,
    join::Relation,
    scope::{ColumnBinding, Scope},
};

/// The set-returning functions crabka implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Srf {
    /// `unnest(anyarray [, anyarray …])` — one column per argument, shorter
    /// arrays padded with NULL (PostgreSQL expands the multi-argument form as
    /// `ROWS FROM`).
    Unnest,
    /// `generate_series(start, stop [, step])` over int4/int8/numeric, and
    /// `(timestamp|timestamptz, same, interval)`.
    GenerateSeries,
    /// `generate_subscripts(anyarray, dim [, reverse])`.
    GenerateSubscripts,
    /// `string_to_table(text, delimiter [, null_string])`.
    StringToTable,
    /// `regexp_split_to_table(text, pattern [, flags])`.
    RegexpSplitToTable,
    /// `jsonb_each(jsonb)` → `(key text, value jsonb)`.
    JsonbEach,
    /// `jsonb_each_text(jsonb)` → `(key text, value text)`.
    JsonbEachText,
    /// `jsonb_object_keys(jsonb)` → `text`.
    JsonbObjectKeys,
    /// `jsonb_array_elements(jsonb)` → `value jsonb`.
    JsonbArrayElements,
    /// `jsonb_array_elements_text(jsonb)` → `value text`.
    JsonbArrayElementsText,
    /// `jsonb_path_query(target, path [, vars [, silent]])` → one row per item
    /// the jsonpath produces.
    JsonbPathQuery,
    EventDdlCommands,
    EventDroppedObjects,
}

/// Classify a function name. Unquoted identifiers reach here lowercased, but a
/// quoted `"UNNEST"` does not, and PostgreSQL matches those case-sensitively —
/// the folding here is deliberate leniency, matching the pre-existing `unnest`
/// handling this registry replaces.
fn classify(name: &str) -> Option<Srf> {
    let lowered = if name.bytes().any(|b| b.is_ascii_uppercase()) {
        Cow::Owned(name.to_ascii_lowercase())
    } else {
        Cow::Borrowed(name)
    };
    Some(match lowered.as_ref() {
        "unnest" => Srf::Unnest,
        "generate_series" => Srf::GenerateSeries,
        "generate_subscripts" => Srf::GenerateSubscripts,
        "string_to_table" => Srf::StringToTable,
        "regexp_split_to_table" => Srf::RegexpSplitToTable,
        // The `json_*` spellings share their implementation: crabka stores
        // `json` as `jsonb` (see the compatibility matrix row).
        "jsonb_each" | "json_each" => Srf::JsonbEach,
        "jsonb_each_text" | "json_each_text" => Srf::JsonbEachText,
        "jsonb_object_keys" | "json_object_keys" => Srf::JsonbObjectKeys,
        "jsonb_array_elements" | "json_array_elements" => Srf::JsonbArrayElements,
        "jsonb_array_elements_text" | "json_array_elements_text" => Srf::JsonbArrayElementsText,
        "jsonb_path_query" | "jsonb_path_query_tz" => Srf::JsonbPathQuery,
        "pg_event_trigger_ddl_commands" => Srf::EventDdlCommands,
        "pg_event_trigger_dropped_objects" => Srf::EventDroppedObjects,
        _ => return None,
    })
}

/// Is `name` a set-returning function? (The dispatch point for the FROM-item and
/// select-list guards.)
pub(crate) fn is_srf(name: &str) -> bool {
    classify(name).is_some()
}

/// One resolved SRF call: which function, and the columns it produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SrfPlan {
    kind: Srf,
    name: String,
    columns: Vec<ColumnBinding>,
}

/// Resolve `name(args)` to the columns it produces. Every arity/type rule a call
/// can fail is checked here, at plan time, so `Describe` reports the same error
/// `Execute` would.
pub(crate) fn plan(name: &str, args: &[Expr], scope: &Scope) -> Result<SrfPlan, ExecError> {
    // Resolve the arguments' types first, so a name no entry claims still reports
    // the argument types PostgreSQL's 42883 names.
    let given = crate::eval::static_arg_types(args, scope)?;
    let kind = classify(name).ok_or_else(|| undefined_function(name, &given))?;
    let columns = match kind {
        Srf::Unnest => unnest_columns(name, &given)?,
        Srf::GenerateSeries => vec![column("generate_series", series_types(name, &given)?)],
        Srf::GenerateSubscripts => {
            require_arity(name, &given, (2, 3))?;
            require_array(name, &given, 0)?;
            vec![column("generate_subscripts", ColumnType::Int4)]
        }
        Srf::StringToTable => {
            require_arity(name, &given, (2, 3))?;
            vec![column("string_to_table", ColumnType::Text)]
        }
        Srf::RegexpSplitToTable => {
            require_arity(name, &given, (2, 3))?;
            vec![column("regexp_split_to_table", ColumnType::Text)]
        }
        Srf::JsonbEach => {
            require_arity(name, &given, (1, 1))?;
            vec![
                column("key", ColumnType::Text),
                column("value", ColumnType::Jsonb),
            ]
        }
        Srf::JsonbEachText => {
            require_arity(name, &given, (1, 1))?;
            vec![
                column("key", ColumnType::Text),
                column("value", ColumnType::Text),
            ]
        }
        Srf::JsonbObjectKeys => {
            require_arity(name, &given, (1, 1))?;
            // A single-column SRF names its column after the function, so the
            // `json_object_keys` alias must not report `jsonb_object_keys`.
            vec![column(&name.to_ascii_lowercase(), ColumnType::Text)]
        }
        Srf::JsonbArrayElements => {
            require_arity(name, &given, (1, 1))?;
            vec![column("value", ColumnType::Jsonb)]
        }
        Srf::JsonbArrayElementsText => {
            require_arity(name, &given, (1, 1))?;
            vec![column("value", ColumnType::Text)]
        }
        Srf::JsonbPathQuery => {
            require_arity(name, &given, (2, 4))?;
            vec![column(&name.to_ascii_lowercase(), ColumnType::Jsonb)]
        }
        Srf::EventDdlCommands => {
            require_arity(name, &given, (0, 0))?;
            vec![
                column("classid", ColumnType::Int4),
                column("objid", ColumnType::Int4),
                column("objsubid", ColumnType::Int4),
                column("command_tag", ColumnType::Text),
                column("object_type", ColumnType::Text),
                column("schema_name", ColumnType::Text),
                column("object_identity", ColumnType::Text),
                column("in_extension", ColumnType::Bool),
                column("command", ColumnType::Text),
            ]
        }
        Srf::EventDroppedObjects => {
            require_arity(name, &given, (0, 0))?;
            vec![
                column("classid", ColumnType::Int4),
                column("objid", ColumnType::Int4),
                column("objsubid", ColumnType::Int4),
                column("original", ColumnType::Bool),
                column("normal", ColumnType::Bool),
                column("is_temporary", ColumnType::Bool),
                column("object_type", ColumnType::Text),
                column("schema_name", ColumnType::Text),
                column("object_name", ColumnType::Text),
                column("object_identity", ColumnType::Text),
                column("address_names", ColumnType::Array(ElemType::Text)),
                column("address_args", ColumnType::Array(ElemType::Text)),
            ]
        }
    };
    Ok(SrfPlan {
        kind,
        name: name.to_string(),
        columns,
    })
}

/// Expand a planned call over already-evaluated arguments.
pub(crate) fn rows(
    plan: &SrfPlan,
    args: &[Expr],
    vals: &mut [Datum],
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let params = param_types(plan);
    crate::eval::coerce_unknown_args(args, vals, &params, ctx)?;
    // A STRICT SRF returns no rows at all — not one NULL row — when any of the
    // arguments PostgreSQL declared strict is NULL. `unnest` is not strict (a
    // NULL array behaves as an empty one) and `string_to_table` is strict only
    // in its subject string: a NULL delimiter splits into characters and a NULL
    // `null_string` simply names no piece.
    let strict_upto = match plan.kind {
        Srf::Unnest => 0,
        Srf::StringToTable => 1,
        _ => vals.len(),
    };
    if vals[..strict_upto.min(vals.len())]
        .iter()
        .any(Datum::is_null)
    {
        return Ok(Vec::new());
    }
    let produced = match plan.kind {
        Srf::Unnest => unnest_rows(vals),
        Srf::GenerateSeries => series_rows(plan, vals, ctx)?,
        Srf::GenerateSubscripts => subscript_rows(&plan.name, vals)?,
        Srf::StringToTable => string_to_table_rows(&plan.name, vals)?,
        Srf::RegexpSplitToTable => regexp_split_rows(&plan.name, vals)?,
        Srf::JsonbEach
        | Srf::JsonbEachText
        | Srf::JsonbObjectKeys
        | Srf::JsonbArrayElements
        | Srf::JsonbArrayElementsText => crate::json_fn::jsonb_srf_rows(json_srf(plan.kind), vals)?,
        Srf::JsonbPathQuery => crate::json_fn::jsonb_path_query_rows(&plan.name, vals)?,
        Srf::EventDdlCommands => event_ddl_command_rows(ctx)?,
        Srf::EventDroppedObjects => event_dropped_object_rows(ctx)?,
    };
    ensure_expansion_fits(&produced)?;
    Ok(produced)
}

/// The type an `unknown` literal argument adopts, per position — the ONE place
/// each SRF's parameter types are written down, driven by both the plan-time
/// resolver and the run-time coercion.
fn param_types(plan: &SrfPlan) -> Vec<Option<ColumnType>> {
    let text = Some(ColumnType::Text);
    match plan.kind {
        // `anyarray`: an `unknown` literal resolves nothing, and `plan` has
        // already rejected the call.
        Srf::Unnest => Vec::new(),
        Srf::GenerateSeries => {
            let value = plan.columns[0].ty;
            let step = if value == ColumnType::Timestamp || value == ColumnType::Timestamptz {
                ColumnType::Interval
            } else {
                value
            };
            vec![Some(value), Some(value), Some(step)]
        }
        Srf::GenerateSubscripts => vec![None, Some(ColumnType::Int4), Some(ColumnType::Bool)],
        Srf::StringToTable | Srf::RegexpSplitToTable => vec![text, text, text],
        Srf::JsonbEach
        | Srf::JsonbEachText
        | Srf::JsonbObjectKeys
        | Srf::JsonbArrayElements
        | Srf::JsonbArrayElementsText => vec![Some(ColumnType::Jsonb)],
        // `(jsonb, jsonpath [, jsonb vars [, boolean silent]])`; crabka spells
        // `jsonpath` `text`.
        Srf::JsonbPathQuery => vec![
            Some(ColumnType::Jsonb),
            text,
            Some(ColumnType::Jsonb),
            Some(ColumnType::Bool),
        ],
        Srf::EventDdlCommands | Srf::EventDroppedObjects => Vec::new(),
    }
}

fn json_srf(kind: Srf) -> crate::json_fn::JsonbSrf {
    use crate::json_fn::JsonbSrf;
    match kind {
        Srf::JsonbEach => JsonbSrf::Each,
        Srf::JsonbEachText => JsonbSrf::EachText,
        Srf::JsonbObjectKeys => JsonbSrf::ObjectKeys,
        Srf::JsonbArrayElements => JsonbSrf::ArrayElements,
        Srf::JsonbArrayElementsText => JsonbSrf::ArrayElementsText,
        _ => unreachable!("only the jsonb SRFs reach the jsonb dispatch"),
    }
}

fn event_context<'a>(
    ctx: &'a EvalCtx,
    expected: crabka_pgcatalog::trigger::EventTriggerEvent,
    function: &str,
) -> Result<&'a crate::clock::EventTriggerContext, ExecError> {
    ctx.event_trigger
        .as_deref()
        .filter(|context| context.event == expected)
        .ok_or_else(|| ExecError::FunctionError {
            sqlstate: "39P03",
            message: format!("{function}() can only be called in the appropriate event trigger"),
        })
}

fn event_ddl_command_rows(ctx: &EvalCtx) -> Result<Vec<Vec<Datum>>, ExecError> {
    let context = event_context(
        ctx,
        crabka_pgcatalog::trigger::EventTriggerEvent::DdlCommandEnd,
        "pg_event_trigger_ddl_commands",
    )?;
    Ok(context
        .commands
        .iter()
        .map(|object| {
            vec![
                Datum::Int4(object.class_id),
                Datum::Int4(object.object_id),
                Datum::Int4(object.object_sub_id),
                Datum::Text(context.tag.clone()),
                Datum::Text(object.object_type.clone()),
                object
                    .schema_name
                    .as_ref()
                    .map_or(Datum::Null, |name| Datum::Text(name.clone())),
                Datum::Text(object.identity.clone()),
                Datum::Bool(false),
                Datum::Null,
            ]
        })
        .collect())
}

fn event_dropped_object_rows(ctx: &EvalCtx) -> Result<Vec<Vec<Datum>>, ExecError> {
    let context = event_context(
        ctx,
        crabka_pgcatalog::trigger::EventTriggerEvent::SqlDrop,
        "pg_event_trigger_dropped_objects",
    )?;
    Ok(context
        .dropped
        .iter()
        .map(|object| {
            let names = object
                .schema_name
                .iter()
                .chain(object.object_name.iter())
                .cloned()
                .map(Datum::Text)
                .collect();
            vec![
                Datum::Int4(object.class_id),
                Datum::Int4(object.object_id),
                Datum::Int4(object.object_sub_id),
                Datum::Bool(true),
                Datum::Bool(false),
                Datum::Bool(false),
                Datum::Text(object.object_type.clone()),
                object
                    .schema_name
                    .as_ref()
                    .map_or(Datum::Null, |name| Datum::Text(name.clone())),
                object
                    .object_name
                    .as_ref()
                    .map_or(Datum::Null, |name| Datum::Text(name.clone())),
                Datum::Text(object.identity.clone()),
                Datum::Array(crabka_pgtypes::ArrayValue::new(ElemType::Text, names)),
                Datum::Array(crabka_pgtypes::ArrayValue::new(ElemType::Text, Vec::new())),
            ]
        })
        .collect())
}

// ---- FROM position ----

/// A FROM-position function item (`FROM generate_series(1, 3) AS g(n)`,
/// `FROM ROWS FROM (f(…), g(…)) WITH ORDINALITY`) as a relation.
///
/// The arguments evaluate in the empty scope. A lateral item's outer references
/// have already been substituted for constants by the caller, so nothing here
/// needs an outer row.
pub(crate) fn from_item(
    functions: &[TableFuncCall],
    with_ordinality: bool,
    alias: Option<&str>,
    column_aliases: &Option<Vec<String>>,
    ctx: &EvalCtx,
) -> Result<Relation, ExecError> {
    let plans = plan_all(functions)?;
    let mut produced = Vec::new();
    for (call, plan) in functions.iter().zip(&plans) {
        let mut vals = call
            .args
            .iter()
            .map(|arg| crate::eval::eval(arg, &Scope::empty(), &[], ctx))
            .collect::<Result<Vec<_>, _>>()?;
        produced.push(rows(plan, &call.args, &mut vals, ctx)?);
    }
    let rows = zip_in_lockstep(produced, &plans);
    qualify(&plans, rows, with_ordinality, alias, column_aliases)
}

/// The same item's schema, with no rows — the `Describe` path, which must agree
/// with [`from_item`] on every column name and type.
pub(crate) fn from_item_schema(
    functions: &[TableFuncCall],
    with_ordinality: bool,
    alias: Option<&str>,
    column_aliases: &Option<Vec<String>>,
) -> Result<Relation, ExecError> {
    let plans = plan_all(functions)?;
    qualify(&plans, Vec::new(), with_ordinality, alias, column_aliases)
}

/// Plan every call in the item, rejecting a column-definition list the way
/// `PostgreSQL` does: crabka has no composite types, so no function it knows
/// returns `record` and the list is never allowed.
fn plan_all(functions: &[TableFuncCall]) -> Result<Vec<SrfPlan>, ExecError> {
    functions
        .iter()
        .map(|call| {
            if call.column_defs.is_some() {
                return Err(ExecError::Syntax(
                    "a column definition list is only allowed for functions returning \"record\""
                        .into(),
                ));
            }
            plan(&call.name, &call.args, &Scope::empty())
        })
        .collect()
}

/// Combine several calls' rows side by side. `ROWS FROM` runs its functions in
/// lockstep and pads the shorter ones with NULL until the longest is exhausted —
/// the same rule the multi-argument `unnest(a, b)` form follows.
fn zip_in_lockstep(produced: Vec<Vec<Vec<Datum>>>, plans: &[SrfPlan]) -> Vec<Vec<Datum>> {
    if let [single] = produced.as_slice() {
        return single.clone();
    }
    let height = produced.iter().map(Vec::len).max().unwrap_or(0);
    (0..height)
        .map(|index| {
            produced
                .iter()
                .zip(plans)
                .flat_map(|(call_rows, plan)| match call_rows.get(index) {
                    Some(row) => row.clone(),
                    None => vec![Datum::Null; plan.columns.len()],
                })
                .collect()
        })
        .collect()
}

/// The name a function FROM item is qualified by: its alias, or the first
/// function's own name.
fn qualifier_for(plans: &[SrfPlan], alias: Option<&str>) -> String {
    alias.map_or_else(
        || {
            plans
                .first()
                .map_or_else(String::new, |plan| plan.name.to_ascii_lowercase())
        },
        str::to_string,
    )
}

/// The `WITH ORDINALITY` column: `PostgreSQL` names it `ordinality` and types it
/// `bigint`.
fn ordinality_column() -> ColumnBinding {
    column("ordinality", ColumnType::Int8)
}

/// Apply `WITH ORDINALITY`, then the FROM item's alias and column aliases.
///
/// Absent an explicit alias the item is qualified by the first function's name
/// (`generate_series.generate_series`). A bare `AS g` renames the column of an
/// item whose *functions* produce exactly one column — a function in FROM
/// returning one scalar takes its column name from the table alias in
/// PostgreSQL, so `SELECT g FROM generate_series(1, 3) AS g` resolves — and the
/// ordinality column keeps its own name either way. A column-alias list renames
/// a prefix positionally; naming more columns than the item has is
/// `PostgreSQL`'s 42P10.
fn qualify(
    plans: &[SrfPlan],
    rows: Vec<Vec<Datum>>,
    with_ordinality: bool,
    alias: Option<&str>,
    column_aliases: &Option<Vec<String>>,
) -> Result<Relation, ExecError> {
    let columns: Vec<ColumnBinding> = plans
        .iter()
        .flat_map(|p| p.columns.iter().cloned())
        .collect();
    qualify_columns(
        qualifier_for(plans, alias),
        columns,
        rows,
        with_ordinality,
        alias,
        column_aliases,
    )
}

pub(crate) fn user_function_relation(
    function_name: &str,
    columns: Vec<(String, ColumnType)>,
    rows: Vec<Vec<Datum>>,
    with_ordinality: bool,
    alias: Option<&str>,
    column_aliases: &Option<Vec<String>>,
) -> Result<Relation, ExecError> {
    let columns = columns
        .into_iter()
        .map(|(name, ty)| column(&name, ty))
        .collect();
    qualify_columns(
        alias.unwrap_or(function_name).to_string(),
        columns,
        rows,
        with_ordinality,
        alias,
        column_aliases,
    )
}

fn qualify_columns(
    qualifier: String,
    mut columns: Vec<ColumnBinding>,
    mut rows: Vec<Vec<Datum>>,
    with_ordinality: bool,
    alias: Option<&str>,
    column_aliases: &Option<Vec<String>>,
) -> Result<Relation, ExecError> {
    let function_columns = columns.len();
    if with_ordinality {
        columns.push(ordinality_column());
        for (index, row) in rows.iter_mut().enumerate() {
            let ordinal = i64::try_from(index).unwrap_or(i64::MAX).saturating_add(1);
            row.push(Datum::Int8(ordinal));
        }
    }
    if let Some(names) = column_aliases {
        if names.len() > columns.len() {
            return Err(ExecError::DerivedColumnAliasCount {
                table: qualifier.clone(),
                expected: columns.len(),
                got: names.len(),
            });
        }
        for (column, name) in columns.iter_mut().zip(names) {
            column.name.clone_from(name);
        }
    } else if let Some(alias) = alias
        && function_columns == 1
    {
        columns[0].name = alias.to_string();
    }
    for column in &mut columns {
        column.qualifier = Some(qualifier.clone());
    }
    Ok(Relation {
        scope: Scope { columns },
        rows,
    })
}

// ---- select list (PostgreSQL's ProjectSet) ----

/// The qualifier the select-list rewrite binds each SRF call's value under. The
/// lexer lowercases unquoted identifiers and never produces `*`, so no user
/// column reference can collide with it.
const SRF_QUALIFIER: &str = "*srf*";

/// Does this select list contain a set-returning function call?
pub(crate) fn projection_contains_srf(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_contains_srf(expr),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    })
}

/// Does any output expression contain a set-returning function call?
pub(crate) fn exprs_contain_srf(exprs: &[Expr]) -> bool {
    exprs.iter().any(expr_contains_srf)
}

/// Does any `ORDER BY` item call a set-returning function? Such a call expands
/// the output the same way a select-list one does, so the whole sort/dedup/limit
/// shape has to run over the expansion.
pub(crate) fn order_by_contains_srf(order_by: &[crabka_pgparser::ast::OrderItem]) -> bool {
    order_by.iter().any(|item| expr_contains_srf(&item.expr))
}

fn expr_contains_srf(expr: &Expr) -> bool {
    if let Expr::Func(fc) = expr
        && is_srf(&fc.name)
    {
        return true;
    }
    children(expr).into_iter().any(expr_contains_srf)
}

/// 0A000 for an SRF in an aggregate query's select list — PostgreSQL evaluates
/// SRFs after aggregation, which crabka's aggregate path does not model.
pub(crate) fn reject_in_aggregate(exprs: &[Expr]) -> Result<(), ExecError> {
    if exprs_contain_srf(exprs) {
        return Err(ExecError::Unsupported(
            "set-returning functions are not supported with aggregation or GROUP BY".into(),
        ));
    }
    Ok(())
}

/// An expression's immediate sub-expressions, for the SRF walks. A set-returning
/// call is found by walking these, so a variant that hides a sub-expression here
/// would hide an SRF from expansion — hence the exhaustive match.
fn children(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::FieldSelect { base, .. } | Expr::FieldSelectAll(base) => vec![base],
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::InSubquery { expr, .. } => vec![expr],
        Expr::Binary { left, right, .. } => vec![left, right],
        Expr::Func(fc) => match &fc.args {
            FuncArgs::Exprs(args) => args.iter().collect(),
            FuncArgs::Star => Vec::new(),
        },
        Expr::InList { expr, list, .. } => std::iter::once(&**expr).chain(list).collect(),
        Expr::Between {
            expr, low, high, ..
        } => vec![expr, low, high],
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => std::iter::once(&**expr)
            .chain(std::iter::once(&**pattern))
            .chain(escape.iter().map(std::convert::AsRef::as_ref))
            .collect(),
        Expr::Case {
            operand,
            whens,
            else_result,
        } => operand
            .iter()
            .map(std::convert::AsRef::as_ref)
            .chain(whens.iter().flat_map(|(w, r)| [w, r]))
            .chain(else_result.iter().map(std::convert::AsRef::as_ref))
            .collect(),
        Expr::Quantified { expr, .. } => vec![expr],
        Expr::QuantifiedArray { expr, array, .. } => vec![expr, array],
        Expr::ArrayLiteral(items) | Expr::Row(items) => items.iter().collect(),
        Expr::Subscript { base, index } => vec![base, index],
        Expr::ArrayRef { base, subscripts } => std::iter::once(base.as_ref())
            .chain(subscripts.iter().flat_map(ArraySubscript::bounds))
            .collect(),
        Expr::ArraySubquery(_) => Vec::new(),
        Expr::SqlJson(json) => json.children(),
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Default
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::Const { .. } => Vec::new(),
    }
}

/// A select list rewritten for set expansion: each SRF call replaced by a
/// synthetic column reference, plus the calls themselves and the scope those
/// references resolve against.
struct ProjectSet {
    exprs: Vec<Expr>,
    calls: Vec<SrfCall>,
    scope: Scope,
}

struct SrfCall {
    plan: SrfPlan,
    args: Vec<Expr>,
}

/// Rewrite `out_exprs` so every SRF call becomes a reference to a synthetic
/// column, and extend `scope` with one binding per call. Both the type resolver
/// ([`projection_type`]) and the row expander drive this one rewrite, so a
/// projected SRF's `RowDescription` type is the type its rows carry.
fn rewrite(out_exprs: &[Expr], scope: &Scope) -> Result<ProjectSet, ExecError> {
    let mut calls: Vec<SrfCall> = Vec::new();
    let mut exprs = Vec::with_capacity(out_exprs.len());
    for expr in out_exprs {
        let mut rewritten = expr.clone();
        rewrite_expr(&mut rewritten, scope, &mut calls)?;
        exprs.push(rewritten);
    }
    let mut extended = scope.clone();
    for (index, call) in calls.iter().enumerate() {
        extended.columns.push(ColumnBinding {
            qualifier: Some(SRF_QUALIFIER.to_string()),
            name: index.to_string(),
            ty: call.plan.columns[0].ty,
        });
    }
    Ok(ProjectSet {
        exprs,
        calls,
        scope: extended,
    })
}

fn rewrite_expr(expr: &mut Expr, scope: &Scope, calls: &mut Vec<SrfCall>) -> Result<(), ExecError> {
    if let Expr::Func(fc) = expr
        && is_srf(&fc.name)
    {
        let FuncArgs::Exprs(args) = &fc.args else {
            return Err(undefined_function(&fc.name, &[]));
        };
        if exprs_contain_srf(args) {
            return Err(ExecError::Unsupported(
                "a set-returning function may not be nested inside another set-returning \
                 function's arguments"
                    .into(),
            ));
        }
        let plan = plan(&fc.name, args, scope)?;
        if plan.columns.len() != 1 {
            return Err(ExecError::Unsupported(format!(
                "set-returning function {} with multiple output columns is only supported in FROM",
                plan.name
            )));
        }
        let index = calls.len();
        let args = args.clone();
        calls.push(SrfCall { plan, args });
        *expr = Expr::Column {
            table: Some(SRF_QUALIFIER.to_string()),
            name: index.to_string(),
        };
        return Ok(());
    }
    for child in children_mut(expr) {
        rewrite_expr(child, scope, calls)?;
    }
    Ok(())
}

fn children_mut(expr: &mut Expr) -> Vec<&mut Expr> {
    match expr {
        Expr::FieldSelect { base, .. } | Expr::FieldSelectAll(base) => vec![base],
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::InSubquery { expr, .. } => vec![expr],
        Expr::Binary { left, right, .. } => vec![left, right],
        Expr::Func(fc) => match &mut fc.args {
            FuncArgs::Exprs(args) => args.iter_mut().collect(),
            FuncArgs::Star => Vec::new(),
        },
        Expr::InList { expr, list, .. } => std::iter::once(&mut **expr).chain(list).collect(),
        Expr::Between {
            expr, low, high, ..
        } => vec![expr, low, high],
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => std::iter::once(&mut **expr)
            .chain(std::iter::once(&mut **pattern))
            .chain(escape.iter_mut().map(std::convert::AsMut::as_mut))
            .collect(),
        Expr::Case {
            operand,
            whens,
            else_result,
        } => operand
            .iter_mut()
            .map(std::convert::AsMut::as_mut)
            .chain(whens.iter_mut().flat_map(|(w, r)| [w, r]))
            .chain(else_result.iter_mut().map(std::convert::AsMut::as_mut))
            .collect(),
        Expr::Quantified { expr, .. } => vec![expr],
        Expr::QuantifiedArray { expr, array, .. } => vec![expr, array],
        Expr::ArrayLiteral(items) | Expr::Row(items) => items.iter_mut().collect(),
        Expr::Subscript { base, index } => vec![base, index],
        Expr::ArrayRef { base, subscripts } => std::iter::once(base.as_mut())
            .chain(subscripts.iter_mut().flat_map(ArraySubscript::bounds_mut))
            .collect(),
        Expr::ArraySubquery(_) => Vec::new(),
        Expr::SqlJson(json) => json.children_mut(),
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Default
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::Const { .. } => Vec::new(),
    }
}

/// Statically infer a projected expression's type, resolving any SRF call it
/// contains to that call's single output column type. Expressions without an SRF
/// take the ordinary path unchanged.
pub(crate) fn projection_type(expr: &Expr, scope: &Scope) -> Result<ColumnType, ExecError> {
    if !expr_contains_srf(expr) {
        return crate::eval::infer_type(expr, scope);
    }
    let set = rewrite(std::slice::from_ref(expr), scope)?;
    crate::eval::infer_type(&set.exprs[0], &set.scope)
}

/// PostgreSQL's ProjectSet + Sort + Unique + Limit for a select list containing
/// set-returning functions.
///
/// Row expansion happens *below* DISTINCT, ORDER BY and LIMIT, exactly as
/// PostgreSQL plans it: `SELECT generate_series(1, 3) ORDER BY 1 DESC LIMIT 2`
/// sorts the three expanded rows and then takes two of them. An ORDER BY key
/// that is not a select-list output is evaluated once per *source* row and
/// replicated across that row's expansion, which is what PostgreSQL's resjunk
/// target does.
pub(crate) fn project_rows_ordered(
    s: &SelectStmt,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    kept: Vec<Vec<Datum>>,
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    if s.distinct.on_exprs().is_some() {
        // PostgreSQL runs `DISTINCT ON` over the expanded rows; that needs the ON
        // keys evaluated per *output* row, which this ProjectSet fold does not
        // model.
        return Err(ExecError::Unsupported(
            "SELECT DISTINCT ON with a set-returning function in the select list is not supported"
                .into(),
        ));
    }
    let order_keys = crate::exec::resolve_select_order_keys(
        &s.order_by,
        scope,
        fields,
        out_exprs,
        s.distinct.dedups(),
    )?;
    // An ORDER BY expression may call an SRF of its own. PostgreSQL adds such an
    // expression to the target list as a junk column, so it expands in lockstep
    // with the select list's calls and multiplies the output rows exactly as a
    // select-list call would; the junk columns are then dropped. Expanding them
    // here — rather than evaluating them once per SOURCE row — is what makes
    // `SELECT a FROM t ORDER BY a, generate_series(1, 2)` return two rows per `a`.
    let mut set_exprs = out_exprs.to_vec();
    let junk_key_columns: Vec<Option<usize>> = order_keys
        .iter()
        .map(|key| match key {
            crate::exec::SelectOrderKey::SourceExpr(expr)
                if exprs_contain_srf(std::slice::from_ref(expr)) =>
            {
                let column = set_exprs.len();
                set_exprs.push(expr.clone());
                Some(column)
            }
            _ => None,
        })
        .collect();
    let set = rewrite(&set_exprs, scope)?;

    let mut projected: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::new();
    let mut budget = MemoryBudget::default();
    for row in &kept {
        let source_keys = order_keys
            .iter()
            .zip(&junk_key_columns)
            .map(|(key, junk)| match key {
                crate::exec::SelectOrderKey::Output(_) => Ok(Datum::Null),
                crate::exec::SelectOrderKey::SourceExpr(_) if junk.is_some() => Ok(Datum::Null),
                crate::exec::SelectOrderKey::SourceExpr(expr) => {
                    crate::eval::eval(expr, scope, row, ctx)
                }
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        for expanded in expand_row(&set, scope, row, ctx)? {
            let keys: Vec<Datum> = order_keys
                .iter()
                .zip(&source_keys)
                .zip(&junk_key_columns)
                .map(|((key, source), junk)| match (key, junk) {
                    (_, Some(column)) => expanded[*column].clone(),
                    (crate::exec::SelectOrderKey::Output(i), None) => expanded[*i].clone(),
                    (crate::exec::SelectOrderKey::SourceExpr(_), None) => source.clone(),
                })
                .collect();
            let out = expanded[..out_exprs.len()].to_vec();
            budget.charge(&keys)?;
            budget.charge(&out)?;
            projected.push((keys, out));
        }
    }

    if s.distinct.dedups() {
        let mut seen: std::collections::HashSet<Vec<Datum>> = std::collections::HashSet::new();
        projected.retain(|(_, out)| seen.insert(out.clone()));
    }
    if !s.order_by.is_empty() {
        projected.sort_by(|a, b| crate::exec::order_cmp(&a.0, &b.0, &s.order_by));
    }
    let window = crate::exec::RowWindow {
        offset: crate::exec::eval_row_count(
            s.offset.as_ref(),
            crate::exec::RowCountClause::Offset,
            ctx,
        )?,
        limit: crate::exec::eval_row_count(
            s.limit.as_ref(),
            crate::exec::RowCountClause::Limit,
            ctx,
        )?,
        with_ties: s.with_ties,
    };
    Ok(crate::exec::apply_row_window(
        projected,
        window,
        &s.order_by,
    ))
}

/// Expand one source row into the output rows its select-list SRFs produce.
/// PostgreSQL 10+ runs the calls in lockstep: the row count is the longest
/// call's, and the shorter ones read as NULL past their end.
fn expand_row(
    set: &ProjectSet,
    scope: &Scope,
    row: &[Datum],
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut values: Vec<Vec<Datum>> = Vec::with_capacity(set.calls.len());
    for call in &set.calls {
        let mut vals = call
            .args
            .iter()
            .map(|arg| crate::eval::eval(arg, scope, row, ctx))
            .collect::<Result<Vec<_>, _>>()?;
        let produced = rows(&call.plan, &call.args, &mut vals, ctx)?;
        values.push(
            produced
                .into_iter()
                .filter_map(|r| r.into_iter().next())
                .collect(),
        );
    }
    let count = values.iter().map(Vec::len).max().unwrap_or(0);
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let mut extended = row.to_vec();
        for column in &values {
            extended.push(column.get(i).cloned().unwrap_or(Datum::Null));
        }
        let mut cells = Vec::with_capacity(set.exprs.len());
        for expr in &set.exprs {
            cells.push(crate::eval::eval(expr, &set.scope, &extended, ctx)?);
        }
        out.push(cells);
    }
    Ok(out)
}

// ---- unnest ----

fn unnest_columns(name: &str, given: &[ArgType]) -> Result<Vec<ColumnBinding>, ExecError> {
    if given.is_empty() {
        return Err(undefined_function(name, given));
    }
    given
        .iter()
        .map(|arg| {
            let ty = arg.known().ok_or_else(|| undefined_function(name, given))?;
            let elem = ty
                .array_element()
                .ok_or_else(|| undefined_function(name, given))?;
            Ok(column("unnest", elem.column_type()))
        })
        .collect()
}

/// `unnest(a, b, …)`: PostgreSQL expands the multi-argument form as
/// `ROWS FROM (unnest(a), unnest(b), …)` — one column per argument, as many rows
/// as the longest array, shorter arrays padded with NULL. A NULL array behaves
/// exactly as an empty one.
fn unnest_rows(vals: &[Datum]) -> Vec<Vec<Datum>> {
    let columns: Vec<&[Datum]> = vals
        .iter()
        .map(|v| match v {
            Datum::Array(array) => array.elems.as_slice(),
            _ => [].as_slice(),
        })
        .collect();
    let count = columns.iter().map(|c| c.len()).max().unwrap_or(0);
    (0..count)
        .map(|i| {
            columns
                .iter()
                .map(|c| c.get(i).cloned().unwrap_or(Datum::Null))
                .collect()
        })
        .collect()
}

// ---- generate_series ----

/// The value type `generate_series` resolves to — the type of its one output
/// column, and (except for the temporal candidates, whose step is an `interval`)
/// of its step.
///
/// PostgreSQL's candidate set is `(int4, int4, int4)`, `(int8, …)`,
/// `(numeric, …)`, `(timestamp, timestamp, interval)` and
/// `(timestamptz, timestamptz, interval)`. `double precision` matches none of
/// them (42883), and a call whose bounds are all `unknown` literals matches
/// several equally well (42725).
fn series_types(name: &str, given: &[ArgType]) -> Result<ColumnType, ExecError> {
    require_arity(name, given, (2, 3))?;
    let bounds: Vec<ColumnType> = given[..2].iter().filter_map(|arg| arg.known()).collect();
    if bounds.is_empty() {
        return Err(ExecError::Type(TypeError::Domain {
            sqlstate: "42725",
            message: "function generate_series(unknown, unknown) is not unique",
        }));
    }
    // A temporal series: `date` prefers the `timestamptz` candidate, exactly as
    // PostgreSQL's preferred-type rule picks it over the `timestamp` one.
    if bounds.iter().any(|t| {
        matches!(
            t,
            ColumnType::Timestamp | ColumnType::Timestamptz | ColumnType::Date
        )
    }) {
        if given.len() != 3 {
            return Err(undefined_function(name, given));
        }
        let value = if bounds
            .iter()
            .all(|t| *t == ColumnType::Timestamp || *t == ColumnType::Date)
            && bounds.contains(&ColumnType::Timestamp)
        {
            ColumnType::Timestamp
        } else {
            ColumnType::Timestamptz
        };
        let step = given[2].known();
        if step.is_some_and(|t| t != ColumnType::Interval) {
            return Err(undefined_function(name, given));
        }
        return Ok(value);
    }
    // A numeric series takes its type from every argument that carries one, by
    // the same numeric-tower unification the arithmetic operators use.
    let step = given.get(2).and_then(|arg| arg.known());
    let mut value = bounds[0];
    for known in bounds[1..].iter().copied().chain(step) {
        value = crate::eval::unify_types(value, known)?;
    }
    Ok(match value {
        ColumnType::Int4 => ColumnType::Int4,
        ColumnType::Int8 => ColumnType::Int8,
        ColumnType::Numeric(_) => ColumnType::Numeric(None),
        _ => return Err(undefined_function(name, given)),
    })
}

/// `generate_series(start, stop [, step])`: PostgreSQL walks `current += step`
/// from `start` while the bound holds, so a `1 month` step over a month-end date
/// drifts exactly as its iterative implementation does. A zero step is 22023; a
/// step whose sign points away from `stop` yields no rows.
fn series_rows(
    plan: &SrfPlan,
    vals: &[Datum],
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let value_ty = plan.columns[0].ty;
    let start = crabka_pgtypes::cast::cast(&vals[0], value_ty, &ctx.time_zone)?;
    let bound = crabka_pgtypes::cast::cast(&vals[1], value_ty, &ctx.time_zone)?;
    let step = match vals.get(2) {
        Some(step) => step.clone(),
        None => default_step(value_ty),
    };
    let ascending = match step_sign(&step)? {
        std::cmp::Ordering::Equal => {
            return Err(ExecError::Type(TypeError::Domain {
                sqlstate: "22023",
                message: "step size cannot equal zero",
            }));
        }
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
    };
    let mut out = Vec::new();
    let mut budget = MemoryBudget::default();
    let mut current = start;
    loop {
        let ordering = crabka_pgtypes::ops::compare(&current, &bound)?;
        let past_end = match ordering {
            Some(std::cmp::Ordering::Greater) => ascending,
            Some(std::cmp::Ordering::Less) => !ascending,
            Some(std::cmp::Ordering::Equal) => false,
            None => true,
        };
        if past_end {
            break;
        }
        budget.charge(std::slice::from_ref(&current))?;
        out.push(vec![current.clone()]);
        current = series_advance(&current, &step, ctx)?;
    }
    Ok(out)
}

fn default_step(value_ty: ColumnType) -> Datum {
    match value_ty {
        ColumnType::Int8 => Datum::Int8(1),
        ColumnType::Numeric(_) => Datum::Numeric(NumericValue::from(1i64)),
        _ => Datum::Int4(1),
    }
}

/// The step's sign. `interval` compares by PostgreSQL's canonical 30-day-month /
/// 24-hour-day estimate, which is what `interval_cmp` uses.
fn step_sign(step: &Datum) -> Result<std::cmp::Ordering, ExecError> {
    Ok(match step {
        Datum::Int4(n) => n.cmp(&0),
        Datum::Int8(n) => n.cmp(&0),
        Datum::Numeric(n) => n.cmp(&NumericValue::from(0i64)),
        Datum::Interval(iv) => iv.canonical_micros().cmp(&0),
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "generate_series step is of type {} but must be numeric or interval",
                other.column_type().map_or("unknown", ColumnType::name)
            )));
        }
    })
}

/// `current + step`. `timestamptz + interval` is zone-aware, so it does not live
/// in the type layer's `ops::add`.
fn series_advance(current: &Datum, step: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    if let (Datum::Timestamptz(ts), Datum::Interval(iv)) = (current, step) {
        return Ok(Datum::Timestamptz(
            crabka_pgtypes::datetime::timestamptz_plus_interval(*ts, *iv, &ctx.time_zone)?,
        ));
    }
    Ok(crabka_pgtypes::ops::add(current, step)?)
}

// ---- generate_subscripts ----

/// `generate_subscripts(array, dim [, reverse])`: crabka arrays are
/// one-dimensional and 1-based, so any `dim` other than 1 — and any empty or
/// NULL array — yields no rows, exactly as PostgreSQL does for a dimension the
/// array does not have.
fn subscript_rows(name: &str, vals: &[Datum]) -> Result<Vec<Vec<Datum>>, ExecError> {
    let Datum::Array(array) = &vals[0] else {
        return Err(ExecError::TypeMismatch(format!(
            "{name} argument is of type {} but must be an array",
            vals[0].column_type().map_or("unknown", ColumnType::name)
        )));
    };
    let dim = match &vals[1] {
        Datum::Int4(n) => i64::from(*n),
        Datum::Int8(n) => *n,
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "{name} dimension is of type {} but must be an integer",
                other.column_type().map_or("unknown", ColumnType::name)
            )));
        }
    };
    let reverse = matches!(vals.get(2), Some(Datum::Bool(true)));
    if dim != 1 || array.elems.is_empty() {
        return Ok(Vec::new());
    }
    let len = i32::try_from(array.elems.len()).map_err(|_| ExecError::Type(TypeError::Overflow))?;
    let subscripts: Vec<i32> = if reverse {
        (1..=len).rev().collect()
    } else {
        (1..=len).collect()
    };
    Ok(subscripts
        .into_iter()
        .map(|i| vec![Datum::Int4(i)])
        .collect())
}

// ---- string_to_table / regexp_split_to_table ----

/// `string_to_table(text, delimiter [, null_string])` — the row-wise twin of
/// `string_to_array`, and split by exactly the same rules: a NULL delimiter
/// splits into single characters, an empty one yields the whole string as one
/// row, an empty input yields no rows at all, and a piece equal to
/// `null_string` becomes SQL NULL.
fn string_to_table_rows(name: &str, vals: &[Datum]) -> Result<Vec<Vec<Datum>>, ExecError> {
    let input = text_arg(name, &vals[0])?;
    let sep = match &vals[1] {
        Datum::Null => None,
        other => Some(text_arg(name, other)?),
    };
    let null_text = match vals.get(2) {
        None | Some(Datum::Null) => None,
        Some(other) => Some(text_arg(name, other)?),
    };
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
    Ok(parts
        .into_iter()
        .map(|part| {
            let cell = if null_text == Some(part.as_str()) {
                Datum::Null
            } else {
                Datum::Text(part)
            };
            vec![cell]
        })
        .collect())
}

/// `regexp_split_to_table(text, pattern [, flags])`.
///
/// Zero-length matches follow PostgreSQL's documented rule: one at the start of
/// the string, at its end, or immediately after a previous match is ignored, so
/// `regexp_split_to_table('abc', 'x*')` is `a, b, c` rather than a run of empty
/// strings. Unlike `string_to_table`, an empty input yields ONE empty row.
fn regexp_split_rows(name: &str, vals: &[Datum]) -> Result<Vec<Vec<Datum>>, ExecError> {
    let input = text_arg(name, &vals[0])?;
    let pattern = text_arg(name, &vals[1])?;
    let flags = match vals.get(2) {
        None => "",
        Some(other) => text_arg(name, other)?,
    };
    let re = compile_regex(pattern, flags)?;
    let mut pieces = Vec::new();
    let mut piece_start = 0usize;
    let mut search = 0usize;
    let mut previous_end: Option<usize> = None;
    while search <= input.len() {
        let Some(m) = re.find_at(input, search) else {
            break;
        };
        let (start, end) = (m.start(), m.end());
        if start == end {
            let ignored = start == 0 || start == input.len() || previous_end == Some(start);
            if !ignored {
                pieces.push(input[piece_start..start].to_string());
                piece_start = end;
                previous_end = Some(end);
            }
            search = next_boundary(input, start);
            if search <= start {
                break;
            }
        } else {
            pieces.push(input[piece_start..start].to_string());
            piece_start = end;
            previous_end = Some(end);
            search = end;
        }
    }
    pieces.push(input[piece_start..].to_string());
    Ok(pieces
        .into_iter()
        .map(|piece| vec![Datum::Text(piece)])
        .collect())
}

/// The next UTF-8 character boundary after `at` (or one past the end, so the
/// scan always terminates).
fn next_boundary(input: &str, at: usize) -> usize {
    input[at..]
        .chars()
        .next()
        .map_or(at + 1, |c| at + c.len_utf8())
}

/// Compile a PostgreSQL regular expression with its flag string.
///
/// PostgreSQL's default is "non-newline-sensitive": `.` matches a newline and
/// `^`/`$` anchor only at the ends of the string. `n`/`m` make both
/// newline-sensitive, `s` restores the default, `i`/`c` set case folding, `x`
/// enables expanded syntax and `q` makes the pattern a literal. `g` is rejected
/// the way PostgreSQL rejects it for this function.
fn compile_regex(pattern: &str, flags: &str) -> Result<regex::Regex, ExecError> {
    let mut case_insensitive = false;
    let mut newline_sensitive = false;
    let mut expanded = false;
    let mut literal = false;
    for flag in flags.chars() {
        match flag {
            'i' => case_insensitive = true,
            'c' => case_insensitive = false,
            'n' | 'm' | 'p' | 'w' => newline_sensitive = true,
            's' | 'e' | 'b' | 't' => newline_sensitive = false,
            'x' => expanded = true,
            'q' => literal = true,
            'g' => {
                return Err(ExecError::FunctionError {
                    sqlstate: "22023",
                    message: "regexp_split_to_table() does not support the \"global\" option"
                        .to_string(),
                });
            }
            other => {
                return Err(ExecError::FunctionError {
                    sqlstate: "22023",
                    message: format!("invalid regular expression option: \"{other}\""),
                });
            }
        }
    }
    let source = if literal {
        regex::escape(pattern)
    } else {
        pattern.to_string()
    };
    regex::RegexBuilder::new(&source)
        .case_insensitive(case_insensitive)
        .multi_line(newline_sensitive)
        .dot_matches_new_line(!newline_sensitive)
        .ignore_whitespace(expanded)
        .build()
        .map_err(|error| ExecError::FunctionError {
            sqlstate: "2201B",
            message: format!("invalid regular expression: {error}"),
        })
}

// ---- shared helpers ----

fn column(name: &str, ty: ColumnType) -> ColumnBinding {
    ColumnBinding {
        qualifier: None,
        name: name.to_string(),
        ty,
    }
}

fn require_arity(name: &str, given: &[ArgType], range: (usize, usize)) -> Result<(), ExecError> {
    if given.len() < range.0 || given.len() > range.1 {
        return Err(undefined_function(name, given));
    }
    Ok(())
}

fn require_array(name: &str, given: &[ArgType], at: usize) -> Result<ElemType, ExecError> {
    given
        .get(at)
        .and_then(|arg| arg.known())
        .and_then(ColumnType::array_element)
        .ok_or_else(|| undefined_function(name, given))
}

fn text_arg<'a>(name: &str, value: &'a Datum) -> Result<&'a str, ExecError> {
    match value {
        Datum::Text(s) => Ok(s),
        other => Err(ExecError::TypeMismatch(format!(
            "{name} does not accept an argument of type {}",
            other.column_type().map_or("unknown", ColumnType::name)
        ))),
    }
}

/// PostgreSQL's 42883, spelling out the argument types it could not match.
fn undefined_function(name: &str, given: &[ArgType]) -> ExecError {
    let types: Vec<&str> = given
        .iter()
        .map(|arg| arg.known().map_or("unknown", ColumnType::name))
        .collect();
    ExecError::UndefinedFunction(format!(
        "function {name}({}) does not exist",
        types.join(", ")
    ))
}

/// A running total of the bytes an expansion has materialized.
///
/// An SRF can name far more rows than fit in memory (`generate_series(1, 1e9)`),
/// and crabka materializes, so the same whole-result budget every other blocking
/// operator honors caps the expansion instead of exhausting the process. The
/// total is carried rather than recomputed so charging stays O(1) per row.
#[derive(Debug, Default)]
struct MemoryBudget {
    bytes: usize,
}

impl MemoryBudget {
    fn charge(&mut self, row: &[Datum]) -> Result<(), ExecError> {
        self.bytes = self
            .bytes
            .saturating_add(crate::scanner::datum_row_bytes(row));
        if crate::scanner::exceeds_query_memory(self.bytes, crate::scanner::BLOCKING_QUERY_MEMORY) {
            return Err(crate::scanner::memory_budget_exceeded());
        }
        Ok(())
    }
}

fn ensure_expansion_fits(rows: &[Vec<Datum>]) -> Result<(), ExecError> {
    let mut budget = MemoryBudget::default();
    for row in rows {
        budget.charge(row)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgtypes::{ArrayValue, JsonbValue, jsonb};
    use crabka_pgwire::engine::{Engine, QueryResult, Session};

    use super::*;
    use crate::SqlEngine;

    fn ctx() -> EvalCtx {
        EvalCtx::test_default()
    }

    fn constant(value: Datum, ty: ColumnType) -> Expr {
        Expr::Const { value, ty }
    }

    fn int4(n: i32) -> Expr {
        constant(Datum::Int4(n), ColumnType::Int4)
    }

    fn text(s: &str) -> Expr {
        constant(Datum::Text(s.to_string()), ColumnType::Text)
    }

    fn array(elem: ElemType, elems: Vec<Datum>) -> Expr {
        constant(
            Datum::Array(ArrayValue::new(elem, elems)),
            ColumnType::Array(elem),
        )
    }

    fn jsonb_arg(source: &str) -> Expr {
        constant(
            Datum::Jsonb(jsonb::parse(source).expect("valid jsonb")),
            ColumnType::Jsonb,
        )
    }

    /// Plan then expand a call, the way both callers do.
    fn call(name: &str, args: &[Expr]) -> Result<Vec<Vec<Datum>>, ExecError> {
        let plan = plan(name, args, &Scope::empty())?;
        let mut vals = args
            .iter()
            .map(|a| crate::eval::eval(a, &Scope::empty(), &[], &ctx()))
            .collect::<Result<Vec<_>, _>>()?;
        rows(&plan, args, &mut vals, &ctx())
    }

    fn single_column(name: &str, args: &[Expr]) -> Result<Vec<Datum>, ExecError> {
        Ok(call(name, args)?
            .into_iter()
            .map(|mut row| row.remove(0))
            .collect())
    }

    fn ints(values: &[i32]) -> Vec<Datum> {
        values.iter().copied().map(Datum::Int4).collect()
    }

    fn texts(values: &[&str]) -> Vec<Datum> {
        values
            .iter()
            .map(|s| Datum::Text((*s).to_string()))
            .collect()
    }

    #[test]
    fn the_registry_claims_exactly_the_implemented_set_returning_functions() {
        for name in [
            "unnest",
            "generate_series",
            "generate_subscripts",
            "string_to_table",
            "regexp_split_to_table",
            "jsonb_each",
            "jsonb_each_text",
            "jsonb_object_keys",
            "jsonb_array_elements",
            "jsonb_array_elements_text",
        ] {
            assert!(is_srf(name), "{name} should be a set-returning function");
            assert!(is_srf(&name.to_ascii_uppercase()), "{name} uppercased");
        }
        for name in ["jsonb_typeof", "generate_seriess", "abs", ""] {
            assert!(
                !is_srf(name),
                "{name} should not be a set-returning function"
            );
        }
    }

    /// One planning case: the function, its arguments, and the `(name, type)`
    /// of each column the call is expected to produce.
    type ColumnCase = (&'static str, Vec<Expr>, Vec<(&'static str, ColumnType)>);

    #[test]
    fn each_srf_reports_postgres_default_column_names_and_types() {
        let cases: Vec<ColumnCase> = vec![
            (
                "generate_series",
                vec![int4(1), int4(3)],
                vec![("generate_series", ColumnType::Int4)],
            ),
            (
                "generate_series",
                vec![constant(Datum::Int8(1), ColumnType::Int8), int4(3), int4(1)],
                vec![("generate_series", ColumnType::Int8)],
            ),
            (
                "generate_subscripts",
                vec![array(ElemType::Int4, ints(&[1, 2])), int4(1)],
                vec![("generate_subscripts", ColumnType::Int4)],
            ),
            (
                "string_to_table",
                vec![text("a,b"), text(",")],
                vec![("string_to_table", ColumnType::Text)],
            ),
            (
                "regexp_split_to_table",
                vec![text("a b"), text(r"\s+")],
                vec![("regexp_split_to_table", ColumnType::Text)],
            ),
            (
                "unnest",
                vec![
                    array(ElemType::Int4, ints(&[1])),
                    array(ElemType::Text, texts(&["a"])),
                ],
                vec![("unnest", ColumnType::Int4), ("unnest", ColumnType::Text)],
            ),
            (
                "jsonb_each",
                vec![jsonb_arg(r#"{"a": 1}"#)],
                vec![("key", ColumnType::Text), ("value", ColumnType::Jsonb)],
            ),
            (
                "jsonb_each_text",
                vec![jsonb_arg(r#"{"a": 1}"#)],
                vec![("key", ColumnType::Text), ("value", ColumnType::Text)],
            ),
            (
                "jsonb_object_keys",
                vec![jsonb_arg(r#"{"a": 1}"#)],
                vec![("jsonb_object_keys", ColumnType::Text)],
            ),
            (
                "jsonb_array_elements",
                vec![jsonb_arg("[1]")],
                vec![("value", ColumnType::Jsonb)],
            ),
            (
                "jsonb_array_elements_text",
                vec![jsonb_arg("[1]")],
                vec![("value", ColumnType::Text)],
            ),
        ];

        for (name, args, expected) in cases {
            let expected: Vec<ColumnBinding> = expected
                .into_iter()
                .map(|(name, ty)| column(name, ty))
                .collect();
            let planned = plan(name, &args, &Scope::empty()).expect("plan");
            assert!(planned.columns == expected, "planning {name}");
            // The `Describe` path must agree with the executing one, column for
            // column, or a prepared statement's RowDescription would lie.
            let item = [TableFuncCall {
                name: name.into(),
                args: args.clone(),
                column_defs: None,
            }];
            let schema = from_item_schema(&item, false, None, &None).expect("schema");
            let executed = from_item(&item, false, None, &None, &ctx()).expect("rows");
            assert!(schema.scope == executed.scope, "describing {name}");
        }
    }

    #[test]
    fn generate_series_resolves_its_candidate_set_like_postgres() {
        let ts = |s: &str| {
            constant(
                Datum::Timestamp(crabka_pgtypes::datetime::parse_timestamp(s).expect("timestamp")),
                ColumnType::Timestamp,
            )
        };
        let interval = constant(
            Datum::Interval(crabka_pgtypes::datetime::Interval {
                months: 0,
                days: 1,
                micros: 0,
            }),
            ColumnType::Interval,
        );
        let numeric = constant(
            Datum::Numeric(NumericValue::from(1i64)),
            ColumnType::Numeric(None),
        );
        let float = constant(Datum::Float8(1.0), ColumnType::Float8);

        let cases: Vec<(Vec<Expr>, Result<ColumnType, &str>)> = vec![
            (vec![int4(1), int4(3)], Ok(ColumnType::Int4)),
            (
                vec![constant(Datum::Int8(1), ColumnType::Int8), int4(3)],
                Ok(ColumnType::Int8),
            ),
            (
                vec![int4(1), numeric.clone()],
                Ok(ColumnType::Numeric(None)),
            ),
            (
                vec![
                    ts("2024-01-01 00:00:00"),
                    ts("2024-01-03 00:00:00"),
                    interval,
                ],
                Ok(ColumnType::Timestamp),
            ),
            (vec![float.clone(), float], Err("42883")),
            (vec![int4(1)], Err("42883")),
            (vec![int4(1), int4(2), int4(3), int4(4)], Err("42883")),
            (
                vec![
                    Expr::StringLiteral("1".into()),
                    Expr::StringLiteral("3".into()),
                ],
                Err("42725"),
            ),
        ];

        for (args, expected) in cases {
            let planned = plan("generate_series", &args, &Scope::empty());
            match expected {
                Ok(ty) => assert!(planned.expect("plan").columns[0].ty == ty),
                Err(sqlstate) => {
                    let error = planned.expect_err("resolution failure").into_pg();
                    assert!(error.code == sqlstate, "{args:?} gave {error:?}");
                }
            }
        }
    }

    #[test]
    fn generate_series_walks_start_to_stop_by_step() {
        let cases: Vec<(Vec<Expr>, Vec<Datum>)> = vec![
            (vec![int4(1), int4(3)], ints(&[1, 2, 3])),
            (vec![int4(3), int4(1)], Vec::new()),
            (vec![int4(3), int4(1), int4(-1)], ints(&[3, 2, 1])),
            (vec![int4(1), int4(10), int4(3)], ints(&[1, 4, 7, 10])),
            (vec![int4(1), int4(1)], ints(&[1])),
            (vec![int4(1), int4(0)], Vec::new()),
            (vec![int4(1), int4(3), int4(-1)], Vec::new()),
            (
                vec![constant(Datum::Null, ColumnType::Int4), int4(3)],
                Vec::new(),
            ),
        ];

        for (args, expected) in cases {
            assert!(
                single_column("generate_series", &args).expect("rows") == expected,
                "generate_series{args:?}"
            );
        }
    }

    #[test]
    fn a_zero_generate_series_step_is_22023() {
        let error = single_column("generate_series", &[int4(1), int4(3), int4(0)])
            .expect_err("zero step")
            .into_pg();
        assert!(error.code == "22023");
        assert!(error.message == "step size cannot equal zero");
    }

    #[test]
    fn multi_argument_unnest_pads_shorter_arrays_with_null() {
        let rows = call(
            "unnest",
            &[
                array(ElemType::Int4, ints(&[1, 2, 3])),
                array(ElemType::Text, texts(&["a", "b"])),
            ],
        )
        .expect("rows");
        assert!(
            rows == vec![
                vec![Datum::Int4(1), Datum::Text("a".into())],
                vec![Datum::Int4(2), Datum::Text("b".into())],
                vec![Datum::Int4(3), Datum::Null],
            ]
        );
        // A NULL array behaves exactly as an empty one.
        let rows = call(
            "unnest",
            &[
                constant(Datum::Null, ColumnType::Array(ElemType::Int4)),
                array(ElemType::Text, texts(&["a"])),
            ],
        )
        .expect("rows");
        assert!(rows == vec![vec![Datum::Null, Datum::Text("a".into())]]);
    }

    #[test]
    fn generate_subscripts_counts_a_one_dimensional_array() {
        let a = array(ElemType::Text, texts(&["x", "y", "z"]));
        let cases: Vec<(Vec<Expr>, Vec<Datum>)> = vec![
            (vec![a.clone(), int4(1)], ints(&[1, 2, 3])),
            (
                vec![
                    a.clone(),
                    int4(1),
                    constant(Datum::Bool(true), ColumnType::Bool),
                ],
                ints(&[3, 2, 1]),
            ),
            (vec![a, int4(2)], Vec::new()),
            (vec![array(ElemType::Int4, Vec::new()), int4(1)], Vec::new()),
            (
                vec![
                    constant(Datum::Null, ColumnType::Array(ElemType::Int4)),
                    int4(1),
                ],
                Vec::new(),
            ),
        ];
        for (args, expected) in cases {
            assert!(
                single_column("generate_subscripts", &args).expect("rows") == expected,
                "generate_subscripts{args:?}"
            );
        }
    }

    #[test]
    fn text_splitting_follows_each_functions_own_rules() {
        let cases: Vec<(&str, Vec<Expr>, Vec<Datum>)> = vec![
            (
                "string_to_table",
                vec![text("a,b,c"), text(",")],
                texts(&["a", "b", "c"]),
            ),
            (
                "string_to_table",
                vec![text("a,b,,c"), text(","), text("")],
                vec![
                    Datum::Text("a".into()),
                    Datum::Text("b".into()),
                    Datum::Null,
                    Datum::Text("c".into()),
                ],
            ),
            (
                "string_to_table",
                vec![text("abc"), constant(Datum::Null, ColumnType::Text)],
                texts(&["a", "b", "c"]),
            ),
            (
                "string_to_table",
                vec![text("abc"), text("")],
                texts(&["abc"]),
            ),
            // An empty input yields NO rows here...
            ("string_to_table", vec![text(""), text(",")], Vec::new()),
            (
                "regexp_split_to_table",
                vec![text("hello   world"), text(r"\s+")],
                texts(&["hello", "world"]),
            ),
            // ...but exactly one empty row here.
            (
                "regexp_split_to_table",
                vec![text(""), text(",")],
                texts(&[""]),
            ),
            (
                "regexp_split_to_table",
                vec![text("a,,b"), text(",")],
                texts(&["a", "", "b"]),
            ),
            (
                "regexp_split_to_table",
                vec![text(",a,"), text(",")],
                texts(&["", "a", ""]),
            ),
            // Zero-length matches at the start, at the end, and immediately after
            // a previous match are ignored, so these split into characters.
            (
                "regexp_split_to_table",
                vec![text("abc"), text("")],
                texts(&["a", "b", "c"]),
            ),
            (
                "regexp_split_to_table",
                vec![text("abc"), text("x*")],
                texts(&["a", "b", "c"]),
            ),
            (
                "regexp_split_to_table",
                vec![text("abcb"), text("b*")],
                texts(&["a", "c", ""]),
            ),
            (
                "regexp_split_to_table",
                vec![text("ABCabc"), text("[b]"), text("i")],
                texts(&["A", "Ca", "c"]),
            ),
        ];

        for (name, args, expected) in cases {
            assert!(
                single_column(name, &args).expect("rows") == expected,
                "{name}{args:?}"
            );
        }
    }

    #[test]
    fn a_rejected_regexp_flag_or_pattern_carries_postgres_sqlstate() {
        let cases: Vec<(&str, &str, &str)> = vec![
            ("[0-9]", "z", "22023"),
            ("[0-9]", "g", "22023"),
            ("[", "", "2201B"),
        ];
        for (pattern, flags, sqlstate) in cases {
            let error = single_column(
                "regexp_split_to_table",
                &[text("a1b"), text(pattern), text(flags)],
            )
            .expect_err("rejected")
            .into_pg();
            assert!(error.code == sqlstate, "{pattern}/{flags} gave {error:?}");
        }
    }

    #[test]
    fn jsonb_set_returning_functions_expand_containers_and_reject_others() {
        let object = jsonb_arg(r#"{"b": 1, "a": "x", "c": null}"#);
        let rows = call("jsonb_each", std::slice::from_ref(&object)).expect("rows");
        assert!(
            rows == vec![
                vec![
                    Datum::Text("a".into()),
                    Datum::Jsonb(JsonbValue::String("x".into()))
                ],
                vec![
                    Datum::Text("b".into()),
                    Datum::Jsonb(JsonbValue::Number(bigdecimal::BigDecimal::from(1)))
                ],
                vec![Datum::Text("c".into()), Datum::Jsonb(JsonbValue::Null)],
            ]
        );
        let rows = call("jsonb_each_text", std::slice::from_ref(&object)).expect("rows");
        assert!(
            rows == vec![
                vec![Datum::Text("a".into()), Datum::Text("x".into())],
                vec![Datum::Text("b".into()), Datum::Text("1".into())],
                // The JSON `null` literal becomes SQL NULL, as `->>` does.
                vec![Datum::Text("c".into()), Datum::Null],
            ]
        );
        assert!(
            single_column("jsonb_object_keys", std::slice::from_ref(&object)).expect("rows")
                == texts(&["a", "b", "c"])
        );

        let arr = jsonb_arg(r#"[1, "a", null]"#);
        assert!(
            single_column("jsonb_array_elements_text", std::slice::from_ref(&arr)).expect("rows")
                == vec![
                    Datum::Text("1".into()),
                    Datum::Text("a".into()),
                    Datum::Null
                ]
        );

        // Each wrong-shaped container carries PostgreSQL's own 22023 message.
        let cases: Vec<(&str, &str, &str)> = vec![
            (
                "jsonb_each",
                "[1]",
                "cannot call jsonb_each on a non-object",
            ),
            (
                "jsonb_each_text",
                "1",
                "cannot call jsonb_each_text on a non-object",
            ),
            (
                "jsonb_object_keys",
                "[1]",
                "cannot call jsonb_object_keys on an array",
            ),
            (
                "jsonb_object_keys",
                "1",
                "cannot call jsonb_object_keys on a scalar",
            ),
            (
                "jsonb_array_elements",
                r#"{"a": 1}"#,
                "cannot extract elements from an object",
            ),
            (
                "jsonb_array_elements",
                "1",
                "cannot extract elements from a scalar",
            ),
        ];
        for (name, source, message) in cases {
            let error = call(name, &[jsonb_arg(source)])
                .expect_err("wrong container")
                .into_pg();
            assert!(
                (error.code.as_str(), error.message.as_str()) == ("22023", message),
                "{name}({source}) gave {error:?}"
            );
        }
        // A NULL argument produces no rows at all.
        assert!(
            call("jsonb_each", &[constant(Datum::Null, ColumnType::Jsonb)]).expect("rows")
                == Vec::<Vec<Datum>>::new()
        );
    }

    // ---- end-to-end, through the engine ----

    async fn query(session: &mut crate::SqlSession, sql: &str) -> QueryResult {
        session
            .simple_query(sql)
            .await
            .expect("query ok")
            .pop()
            .expect("one result")
    }

    fn shape(result: &QueryResult) -> (Vec<String>, Vec<u32>, Vec<Vec<Option<String>>>) {
        match result {
            QueryResult::Rows { fields, rows, .. } => (
                fields.iter().map(|f| f.name.clone()).collect(),
                fields.iter().map(|f| f.type_oid).collect(),
                rows.iter()
                    .map(|row| {
                        row.iter()
                            .map(|cell| {
                                cell.as_ref()
                                    .map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
                            })
                            .collect()
                    })
                    .collect(),
            ),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    fn column_of(result: &QueryResult) -> Vec<Option<String>> {
        shape(result)
            .2
            .into_iter()
            .map(|mut row| row.remove(0))
            .collect()
    }

    #[tokio::test]
    async fn a_from_item_takes_its_names_from_its_alias() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();

        // No alias: the item is qualified by the function name and the single
        // column keeps the function's name.
        let r = query(&mut s, "SELECT * FROM generate_series(1, 3)").await;
        assert!(shape(&r).0 == vec!["generate_series"]);
        assert!(shape(&r).1 == vec![crabka_pgtypes::oids::INT4]);

        // A bare alias renames the single column too.
        let r = query(&mut s, "SELECT g FROM generate_series(1, 2) AS g").await;
        assert!(shape(&r).0 == vec!["g"]);
        assert!(column_of(&r) == vec![Some("1".into()), Some("2".into())]);

        // A column alias list renames positionally.
        let r = query(&mut s, "SELECT * FROM generate_series(1, 2) AS g(n)").await;
        assert!(shape(&r).0 == vec!["n"]);

        // A multi-column item keeps its own names under a bare alias.
        let r = query(
            &mut s,
            "SELECT je.key, je.value FROM jsonb_each('{\"a\": 1}'::jsonb) je",
        )
        .await;
        assert!(shape(&r).0 == vec!["key", "value"]);
        let r = query(&mut s, "SELECT * FROM jsonb_each('{\"a\": 1}'::jsonb) je").await;
        assert!(shape(&r).0 == vec!["key", "value"]);
        let r = query(
            &mut s,
            "SELECT * FROM jsonb_each('{\"a\": 1}'::jsonb) AS je(k, v)",
        )
        .await;
        assert!(shape(&r).0 == vec!["k", "v"]);
    }

    /// An `ORDER BY` expression may call an SRF of its own. `PostgreSQL` adds it
    /// to the target list as a junk column, so it expands in lockstep with the
    /// select list's calls and multiplies the output rows the same way; the junk
    /// columns are then dropped.
    #[tokio::test]
    async fn an_order_by_srf_expands_the_output_and_then_disappears() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        query(&mut s, "CREATE TABLE t (a int4)").await;
        query(&mut s, "INSERT INTO t VALUES (2), (1)").await;

        // Two rows in, two series values each, four rows out — and the sort key
        // is the EXPANDED value, not one reading per source row.
        let r = query(&mut s, "SELECT a FROM t ORDER BY a, generate_series(1, 2)").await;
        assert!(shape(&r).0 == vec!["a"]);
        assert!(
            column_of(&r)
                == vec![
                    Some("1".into()),
                    Some("1".into()),
                    Some("2".into()),
                    Some("2".into()),
                ]
        );

        // The series may lead the sort, and it may be the only expanding key.
        let r = query(&mut s, "SELECT a FROM t ORDER BY generate_series(1, 2), a").await;
        assert!(
            column_of(&r)
                == vec![
                    Some("1".into()),
                    Some("2".into()),
                    Some("1".into()),
                    Some("2".into()),
                ]
        );

        // LIMIT applies to the expanded rows, and any SRF does it.
        let r = query(
            &mut s,
            "SELECT a FROM t ORDER BY a, unnest(ARRAY[1, 2]) LIMIT 3",
        )
        .await;
        assert!(column_of(&r) == vec![Some("1".into()), Some("1".into()), Some("2".into())]);

        // A source row that produces nothing is dropped, exactly as in the
        // select-list case.
        let r = query(&mut s, "SELECT a FROM t ORDER BY a, generate_series(1, 0)").await;
        assert!(column_of(&r).is_empty());
    }

    #[tokio::test]
    async fn a_select_list_srf_expands_below_order_by_and_limit() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();

        let r = query(&mut s, "SELECT generate_series(1, 3)").await;
        assert!(shape(&r).0 == vec!["generate_series"]);
        assert!(shape(&r).1 == vec![crabka_pgtypes::oids::INT4]);
        assert!(column_of(&r) == vec![Some("1".into()), Some("2".into()), Some("3".into())]);

        // The expansion happens below ORDER BY and LIMIT, so both see all rows.
        let r = query(
            &mut s,
            "SELECT generate_series(1, 3) ORDER BY 1 DESC LIMIT 2",
        )
        .await;
        assert!(column_of(&r) == vec![Some("3".into()), Some("2".into())]);

        // An SRF inside a larger expression expands the same way.
        let r = query(&mut s, "SELECT generate_series(1, 3) * 2").await;
        assert!(shape(&r).0 == vec!["?column?"]);
        assert!(column_of(&r) == vec![Some("2".into()), Some("4".into()), Some("6".into())]);

        // DISTINCT dedups the expanded output.
        let r = query(
            &mut s,
            "SELECT DISTINCT generate_series(1, 4) % 2 ORDER BY 1",
        )
        .await;
        assert!(column_of(&r) == vec![Some("0".into()), Some("1".into())]);

        // Two SRFs run in lockstep, the shorter padding with NULL (PostgreSQL 10+).
        let r = query(
            &mut s,
            "SELECT generate_series(1, 2), generate_series(1, 4)",
        )
        .await;
        assert!(
            shape(&r).2
                == vec![
                    vec![Some("1".into()), Some("1".into())],
                    vec![Some("2".into()), Some("2".into())],
                    vec![None, Some("3".into())],
                    vec![None, Some("4".into())],
                ]
        );
    }

    #[tokio::test]
    async fn a_select_list_srf_expands_once_per_source_row() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        query(&mut s, "CREATE TABLE srft (a int4 PRIMARY KEY)").await;
        query(&mut s, "INSERT INTO srft (a) VALUES (1), (3)").await;

        let r = query(
            &mut s,
            "SELECT a, generate_series(1, a) FROM srft ORDER BY 1, 2",
        )
        .await;
        assert!(
            shape(&r).2
                == vec![
                    vec![Some("1".into()), Some("1".into())],
                    vec![Some("3".into()), Some("1".into())],
                    vec![Some("3".into()), Some("2".into())],
                    vec![Some("3".into()), Some("3".into())],
                ]
        );
    }

    #[tokio::test]
    async fn describe_reports_what_execution_returns() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        let cases: Vec<(&str, Vec<&str>, Vec<u32>)> = vec![
            (
                "SELECT * FROM generate_series(1, 3)",
                vec!["generate_series"],
                vec![crabka_pgtypes::oids::INT4],
            ),
            (
                "SELECT generate_series(1, 3)",
                vec!["generate_series"],
                vec![crabka_pgtypes::oids::INT4],
            ),
            (
                "SELECT * FROM jsonb_each('{\"a\": 1}'::jsonb)",
                vec!["key", "value"],
                vec![crabka_pgtypes::oids::TEXT, crabka_pgtypes::oids::JSONB],
            ),
            (
                "SELECT * FROM jsonb_each_text('{\"a\": 1}'::jsonb)",
                vec!["key", "value"],
                vec![crabka_pgtypes::oids::TEXT, crabka_pgtypes::oids::TEXT],
            ),
            (
                "SELECT * FROM string_to_table('a,b', ',')",
                vec!["string_to_table"],
                vec![crabka_pgtypes::oids::TEXT],
            ),
            (
                "SELECT * FROM unnest(ARRAY[1], ARRAY['a']) AS t(x, y)",
                vec!["x", "y"],
                vec![crabka_pgtypes::oids::INT4, crabka_pgtypes::oids::TEXT],
            ),
        ];
        for (sql, names, oids) in cases {
            let described = s.test_describe(sql).await.expect("describe");
            assert!(
                described
                    .iter()
                    .map(|f| f.name.as_str())
                    .collect::<Vec<_>>()
                    == names,
                "describing {sql}"
            );
            assert!(
                described.iter().map(|f| f.type_oid).collect::<Vec<_>>() == oids,
                "describing {sql}"
            );
            let executed = query(&mut s, sql).await;
            assert!(shape(&executed).0 == names, "executing {sql}");
            assert!(shape(&executed).1 == oids, "executing {sql}");
        }
    }

    #[tokio::test]
    async fn deliberate_divergences_are_refused_with_0a000() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        for sql in [
            // A multi-column SRF in the select list would need a record type.
            "SELECT jsonb_each('{\"a\": 1}'::jsonb)",
            // A nested SRF would need its own ProjectSet level.
            "SELECT generate_series(1, generate_series(1, 2))",
            // SRFs are evaluated after aggregation in PostgreSQL.
            "SELECT generate_series(1, 3), count(*)",
        ] {
            let error = s.simple_query(sql).await.expect_err("refused");
            assert!(error.code == "0A000", "{sql} gave {error:?}");
        }
    }

    #[tokio::test]
    async fn a_wildcard_over_duplicate_srf_column_names_expands_positionally() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        // PostgreSQL names both of `unnest(a, b)`'s columns `unnest` and expands
        // `*` into positional Vars, so the wildcard works and keeps both names.
        let both = query(&mut s, "SELECT * FROM unnest(ARRAY[1], ARRAY['a'])").await;
        assert!(shape(&both).0 == vec!["unnest", "unnest"]);
        // A bare reference to the repeated name is still 42702, as in PostgreSQL.
        let error = s
            .simple_query("SELECT unnest FROM unnest(ARRAY[1], ARRAY['a'])")
            .await
            .expect_err("ambiguous");
        assert!(error.code == "42702", "{error:?}");
        let named = query(
            &mut s,
            "SELECT * FROM unnest(ARRAY[1], ARRAY['a']) AS t(x, y)",
        )
        .await;
        assert!(shape(&named).0 == vec!["x", "y"]);
    }
}
