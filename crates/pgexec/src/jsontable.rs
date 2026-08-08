//! SQL/JSON `JSON_TABLE` — a FROM item that turns one JSON document into rows.
//!
//! The row pattern is a jsonpath over the context item; every item it matches
//! becomes one *scan row*, and each `COLUMNS` entry is an independent
//! `JSON_VALUE` / `JSON_QUERY` / `JSON_EXISTS` evaluated against that item.
//!
//! `NESTED PATH … COLUMNS (…)` nests a second scan inside each of its parent's
//! rows. `PostgreSQL`'s default plan — the only one `PostgreSQL` 18 has, the
//! explicit `PLAN` clause having been dropped before the feature shipped — joins
//! them two ways at once: sibling nested sets are **UNION**ed, so each sibling
//! contributes its own rows with the other siblings' columns NULL, and the union
//! is **OUTER**-joined to the parent, so a parent whose nested sets all produced
//! nothing still emits one row. Both rules fall out of [`scan`] below.
//!
//! Column order is a pre-order walk: a scan level's own columns come first, in
//! declaration order, and only then the nested sets below it — which is why
//! `COLUMNS (NESTED …, xx int)` reports `xx` *before* the nested columns.

use std::borrow::Cow;

use crabka_pgparser::ast::{
    Expr, JsonBehavior, JsonTable, JsonTableColumn, JsonTableExistsColumn, JsonTableNestedColumns,
    JsonTableValueColumn, JsonWrapper,
};
use crabka_pgtypes::{ColumnType, Datum, JsonbValue};

use crate::{
    clock::EvalCtx,
    error::{ExecError, SqlJsonError},
    join::Relation,
    scope::{ColumnBinding, Scope},
};

/// The relation a `JSON_TABLE` FROM item produces.
///
/// The context item and the `PASSING` values are evaluated in the empty scope:
/// a lateral reference has already been substituted for a constant by the
/// caller, exactly as for a function item's arguments.
pub(crate) fn from_item(table: &JsonTable, ctx: &EvalCtx) -> Result<Relation, ExecError> {
    validate(table)?;
    let columns = schema(table);
    let context = crate::eval::eval(&table.context, &Scope::empty(), &[], ctx)?;
    let mut vars = Vec::with_capacity(table.passing.len());
    for (name, expr) in &table.passing {
        let value = crate::eval::eval(expr, &Scope::empty(), &[], ctx)?;
        vars.push((name.clone(), crate::json_fn::to_jsonb(&value, ctx)?));
    }
    let vars = JsonbValue::object_from_pairs(vars);
    let rows = if context.is_null() {
        // A NULL document is an empty table, never a row of NULLs.
        Vec::new()
    } else {
        let document = crate::json_fn::json_document(&context)?;
        let node = Node {
            path: &table.path,
            columns: &table.columns,
        };
        let plan = Plan {
            vars: &vars,
            error_on_error: table.error_on_error(),
            width: columns.len(),
            ctx,
        };
        plan.scan(&node, document.as_ref(), 0)?
    };
    relation(table, columns, rows)
}

/// The same item's schema with no rows — the `Describe` path, which must agree
/// with [`from_item`] on every column name and type.
pub(crate) fn from_item_schema(table: &JsonTable) -> Result<Relation, ExecError> {
    validate(table)?;
    relation(table, schema(table), Vec::new())
}

/// The parse-analysis checks over the behavior clauses.
///
/// These live here rather than in the grammar because each carries
/// `PostgreSQL`'s `DETAIL` line naming the behaviors the clause *does* admit,
/// and because which set a value column admits depends on whether its type made
/// it a formatted column — a question about types, not about syntax.
fn validate(table: &JsonTable) -> Result<(), ExecError> {
    if let Some(behavior) = &table.on_error
        && !matches!(
            behavior,
            JsonBehavior::Error | JsonBehavior::EmptyArray | JsonBehavior::EmptyObject
        )
    {
        return Err(behavior_error(
            "invalid ON ERROR behavior".to_string(),
            "Only EMPTY [ ARRAY ] or ERROR is allowed in the top-level ON ERROR clause."
                .to_string(),
        ));
    }
    validate_columns(&table.columns)
}

/// Each scan level is checked before the nested sets below it, which is the
/// order `PostgreSQL` reports these in.
fn validate_columns(columns: &[JsonTableColumn]) -> Result<(), ExecError> {
    for column in columns {
        match column {
            JsonTableColumn::Value(value) => validate_value_column(value)?,
            JsonTableColumn::Exists(exists) => validate_exists_column(exists)?,
            JsonTableColumn::Ordinality { .. } | JsonTableColumn::Nested(_) => {}
        }
    }
    for column in columns {
        if let JsonTableColumn::Nested(nested) = column {
            validate_columns(&nested.columns)?;
        }
    }
    Ok(())
}

fn validate_value_column(column: &JsonTableValueColumn) -> Result<(), ExecError> {
    let formatted = column.is_formatted();
    let (allowed, kind) = if formatted {
        (
            "ERROR, NULL, EMPTY ARRAY, EMPTY OBJECT, or DEFAULT expression",
            "formatted columns",
        )
    } else {
        ("ERROR, NULL, or DEFAULT expression", "scalar columns")
    };
    for (behavior, clause) in [
        (&column.on_empty, "ON EMPTY"),
        (&column.on_error, "ON ERROR"),
    ] {
        let Some(behavior) = behavior else { continue };
        let admitted = matches!(
            behavior,
            JsonBehavior::Error | JsonBehavior::Null | JsonBehavior::Default(_)
        ) || (formatted
            && matches!(
                behavior,
                JsonBehavior::EmptyArray | JsonBehavior::EmptyObject
            ));
        if !admitted {
            return Err(behavior_error(
                format!("invalid {clause} behavior for column \"{}\"", column.name),
                format!("Only {allowed} is allowed in {clause} for {kind}."),
            ));
        }
    }
    Ok(())
}

fn validate_exists_column(column: &JsonTableExistsColumn) -> Result<(), ExecError> {
    if let Some(behavior) = &column.on_error
        && !matches!(
            behavior,
            JsonBehavior::Error | JsonBehavior::True | JsonBehavior::False | JsonBehavior::Unknown
        )
    {
        return Err(behavior_error(
            format!("invalid ON ERROR behavior for column \"{}\"", column.name),
            "Only ERROR, TRUE, FALSE, or UNKNOWN is allowed in ON ERROR for EXISTS columns."
                .to_string(),
        ));
    }
    Ok(())
}

/// 42601 — a behavior word the clause does not admit, with the detail line that
/// names the ones it does.
fn behavior_error(message: String, detail: String) -> ExecError {
    ExecError::SqlJson(Box::new(SqlJsonError {
        sqlstate: "42601",
        message,
        detail: Some(detail),
        hint: None,
    }))
}

/// Does this item read anything from the FROM items to its left? `JSON_TABLE` is
/// implicitly `LATERAL` in `PostgreSQL` exactly as a function item is, so the
/// keyword is not what decides.
pub(crate) fn references_scope(table: &JsonTable, outer: &Scope) -> bool {
    table.lateral
        || table
            .exprs()
            .into_iter()
            .any(|expr| crate::exec::expr_references_scope(expr, outer))
}

/// Apply the item's alias and column-alias list to its columns.
fn relation(
    table: &JsonTable,
    mut columns: Vec<ColumnBinding>,
    rows: Vec<Vec<Datum>>,
) -> Result<Relation, ExecError> {
    // Unaliased, PostgreSQL calls the item `json_table` — the name its
    // `Table Function Scan` plan node reports.
    let qualifier = table.alias.clone().unwrap_or_else(|| "json_table".into());
    if let Some(names) = &table.column_aliases {
        if names.len() > columns.len() {
            // PostgreSQL words a table function's alias-arity error without the
            // `table "…"` framing an ordinary relation's carries.
            return Err(ExecError::SqlJson(Box::new(crate::error::SqlJsonError {
                sqlstate: "42P10",
                message: format!(
                    "JSON_TABLE function has {} columns available but {} columns specified",
                    columns.len(),
                    names.len()
                ),
                detail: None,
                hint: None,
            })));
        }
        for (column, name) in columns.iter_mut().zip(names) {
            column.name.clone_from(name);
        }
    }
    for column in &mut columns {
        column.qualifier = Some(qualifier.clone());
    }
    Ok(Relation {
        scope: Scope { columns },
        rows,
    })
}

/// The output columns, in `PostgreSQL`'s pre-order: each scan level's own
/// columns, then the nested sets below it.
fn schema(table: &JsonTable) -> Vec<ColumnBinding> {
    let mut out = Vec::new();
    schema_of(&table.columns, &mut out);
    out
}

fn schema_of(columns: &[JsonTableColumn], out: &mut Vec<ColumnBinding>) {
    for column in columns {
        let (name, ty) = match column {
            JsonTableColumn::Ordinality { name } => (name, ColumnType::Int4),
            JsonTableColumn::Value(value) => (&value.name, value.ty),
            JsonTableColumn::Exists(exists) => (&exists.name, exists.ty),
            JsonTableColumn::Nested(_) => continue,
        };
        out.push(ColumnBinding {
            qualifier: None,
            name: name.clone(),
            ty,
        });
    }
    for column in columns {
        if let JsonTableColumn::Nested(nested) = column {
            schema_of(&nested.columns, out);
        }
    }
}

/// One scan level: a row pattern and the columns evaluated against each item it
/// matches.
struct Node<'a> {
    path: &'a str,
    columns: &'a [JsonTableColumn],
}

/// How many output columns a `COLUMNS (…)` list occupies, counting the nested
/// sets below it. This is what places each scan level's slice of the flat output
/// row, and it walks the tree the same way [`schema_of`] does.
fn width_of(columns: &[JsonTableColumn]) -> usize {
    columns
        .iter()
        .map(|column| match column {
            JsonTableColumn::Nested(nested) => width_of(&nested.columns),
            _ => 1,
        })
        .sum()
}

/// Everything a scan needs that does not change between levels.
struct Plan<'a> {
    vars: &'a JsonbValue,
    /// `ERROR ON ERROR` on the `JSON_TABLE(…)` itself. It governs the *row
    /// patterns* — a column's own `ON ERROR` is never overridden by it.
    error_on_error: bool,
    width: usize,
    ctx: &'a EvalCtx,
}

impl Plan<'_> {
    /// The rows one scan level produces over `item`, each already widened to the
    /// full output row with every other level's columns left NULL.
    fn scan(
        &self,
        node: &Node<'_>,
        item: &JsonbValue,
        base: usize,
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        let own: Vec<&JsonTableColumn> = node
            .columns
            .iter()
            .filter(|c| !matches!(c, JsonTableColumn::Nested(_)))
            .collect();
        // Each child's slice starts after this level's own columns and after
        // every earlier sibling's whole subtree.
        let mut next = base + own.len();
        let mut children: Vec<(&JsonTableNestedColumns, usize)> = Vec::new();
        for column in node.columns {
            if let JsonTableColumn::Nested(nested) = column {
                children.push((nested, next));
                next += width_of(&nested.columns);
            }
        }

        let items = self.match_row_pattern(node.path, item)?;
        let mut rows = Vec::new();
        for (ordinal, matched) in items.iter().enumerate() {
            let mut own_values = Vec::with_capacity(own.len());
            for column in &own {
                own_values.push(self.column_value(column, matched, ordinal)?);
            }
            let mut nested_rows = Vec::new();
            for (child, start) in &children {
                let child_node = Node {
                    path: &child.path,
                    columns: &child.columns,
                };
                nested_rows.extend(self.scan(&child_node, matched, *start)?);
            }
            if nested_rows.is_empty() {
                // OUTER: a parent row survives its nested sets producing nothing.
                nested_rows.push(vec![Datum::Null; self.width]);
            }
            for mut row in nested_rows {
                for (index, value) in own_values.iter().enumerate() {
                    row[base + index] = value.clone();
                }
                rows.push(row);
            }
        }
        Ok(rows)
    }

    /// Run a scan level's row pattern. Unless the `JSON_TABLE` was written
    /// `ERROR ON ERROR`, a failing pattern contributes no items rather than
    /// failing the query.
    fn match_row_pattern(
        &self,
        path: &str,
        item: &JsonbValue,
    ) -> Result<Vec<JsonbValue>, ExecError> {
        let matched = crate::jsonpath::JsonPath::parse(path)
            .and_then(|compiled| compiled.query(item, Some(self.vars), false));
        match matched {
            Ok(items) => Ok(items),
            Err(error) if self.error_on_error => Err(error),
            Err(_) => Ok(Vec::new()),
        }
    }

    /// One column's value for one scan row.
    fn column_value(
        &self,
        column: &JsonTableColumn,
        item: &JsonbValue,
        ordinal: usize,
    ) -> Result<Datum, ExecError> {
        match column {
            JsonTableColumn::Ordinality { .. } => Ok(Datum::Int4(
                i32::try_from(ordinal.saturating_add(1)).unwrap_or(i32::MAX),
            )),
            JsonTableColumn::Value(value) => self.value_column(value, item),
            JsonTableColumn::Exists(exists) => self.exists_column(exists, item),
            // Filtered out before this is reached.
            JsonTableColumn::Nested(_) => Ok(Datum::Null),
        }
    }

    /// A `JSON_VALUE`/`JSON_QUERY` column.
    fn value_column(
        &self,
        column: &JsonTableValueColumn,
        item: &JsonbValue,
    ) -> Result<Datum, ExecError> {
        let path = column
            .path
            .as_deref()
            .map_or_else(|| Cow::Owned(default_path(&column.name)), Cow::Borrowed);
        let formatted = column.is_formatted();
        let computed = (|| -> Result<Option<Datum>, ExecError> {
            let compiled = crate::jsonpath::JsonPath::parse(&path)?;
            let items = compiled.query(item, Some(self.vars), false)?;
            if formatted {
                self.query_result(column, items)
            } else {
                self.value_result(column, items)
            }
        })();
        match computed {
            Ok(Some(value)) => Ok(value),
            Ok(None) => match &column.on_empty {
                None | Some(JsonBehavior::Null) => Ok(Datum::Null),
                Some(JsonBehavior::Error) => Err(no_item_for_column(&column.name)),
                Some(behavior) => {
                    self.behavior_value(behavior, column.ty, "ON EMPTY", &column.on_error)
                }
            },
            Err(error) => match &column.on_error {
                Some(JsonBehavior::Error) => Err(error),
                None | Some(JsonBehavior::Null) => Ok(Datum::Null),
                Some(behavior) => self.behavior_value(behavior, column.ty, "ON ERROR", &None),
            },
        }
    }

    /// `JSON_QUERY` semantics: wrap, optionally unquote, then convert.
    fn query_result(
        &self,
        column: &JsonTableValueColumn,
        items: Vec<JsonbValue>,
    ) -> Result<Option<Datum>, ExecError> {
        let wrapped = match (column.wrapper.unwrap_or(JsonWrapper::Without), items.len()) {
            (JsonWrapper::Unconditional, _) | (JsonWrapper::Conditional, 0) => {
                Some(JsonbValue::Array(items))
            }
            (JsonWrapper::Conditional, 1) | (JsonWrapper::Without, 1) => items.into_iter().next(),
            (JsonWrapper::Conditional, _) => Some(JsonbValue::Array(items)),
            (JsonWrapper::Without, 0) => None,
            (JsonWrapper::Without, _) => return Err(more_than_one_item(&column.name)),
        };
        let Some(value) = wrapped else {
            return Ok(None);
        };
        self.populate(&value, column.ty, column.omit_quotes.unwrap_or(false))
            .map(Some)
    }

    /// `JSON_VALUE` semantics: exactly one scalar item, unquoted.
    fn value_result(
        &self,
        column: &JsonTableValueColumn,
        items: Vec<JsonbValue>,
    ) -> Result<Option<Datum>, ExecError> {
        match items.as_slice() {
            [] => Ok(None),
            [JsonbValue::Null] => Ok(Some(Datum::Null)),
            [JsonbValue::Object(_) | JsonbValue::Array(_)] => Err(not_single_scalar(&column.name)),
            [single] => self.scalar_item(single, column.ty).map(Some),
            _ => Err(not_single_scalar(&column.name)),
        }
    }

    /// `JSON_VALUE`'s conversion of one scalar item.
    ///
    /// `PostgreSQL` renders the item with the *SQL* type's output function and
    /// then calls the target's input function, which is why a JSON `false` read
    /// into `text` is `f` and not `false`. Two targets skip that: `json`/`jsonb`
    /// take the item's JSON rendering, and a domain that carries constraints
    /// goes through the same populate path a formatted column uses — where a
    /// boolean spells itself `false`.
    fn scalar_item(&self, item: &JsonbValue, ty: ColumnType) -> Result<Datum, ExecError> {
        // `json` and `jsonb` alike take the item's JSON rendering. The item
        // has already been through a jsonpath evaluation, which is a `jsonb`
        // operation, so a `json` column here reports the canonical spelling —
        // exactly as PostgreSQL does, whose JSON_QUERY returns `jsonb` and then
        // casts.
        if ty == ColumnType::Jsonb || ty == ColumnType::Json {
            return self.convert(&item.to_text(), ty);
        }
        if let ColumnType::Domain(domain) = ty
            && crate::usertype::domain_has_checks(domain.oid)
        {
            // A constrained domain takes the populate path, where a JSON string
            // loses its quotes and a boolean spells itself out.
            let text = match item {
                JsonbValue::String(s) => s.clone(),
                JsonbValue::Bool(b) => if *b { "true" } else { "false" }.to_string(),
                other => other.to_text(),
            };
            return self.convert(&text, ty);
        }
        let text = match item {
            JsonbValue::String(s) => s.clone(),
            JsonbValue::Bool(b) => if *b { "t" } else { "f" }.to_string(),
            other => other.to_text(),
        };
        self.convert(&text, ty)
    }

    /// `PostgreSQL`'s `json_populate_type` for a `JSON_QUERY` result.
    ///
    /// The result arrives as a whole JSON document, so what the target's input
    /// function sees is its JSON rendering — a string keeps its quotes unless
    /// `OMIT QUOTES` asked for them to go. An array target is populated
    /// element-wise and *requires* a JSON array, which is why `int[] PATH '$'`
    /// over a JSON object is an error rather than the empty array `'{}'::int[]`
    /// would be.
    fn populate(
        &self,
        value: &JsonbValue,
        ty: ColumnType,
        omit_quotes: bool,
    ) -> Result<Datum, ExecError> {
        if let ColumnType::Array(elem) = ty {
            let JsonbValue::Array(items) = value else {
                return Err(ExecError::Type(crabka_pgtypes::TypeError::Coded {
                    sqlstate: "22P02",
                    message: format!("expected JSON array, got \"{}\"", value.to_text()),
                }));
            };
            let mut elems = Vec::with_capacity(items.len());
            for item in items {
                elems.push(self.populate(item, elem.column_type(), false)?);
            }
            return Ok(Datum::Array(crabka_pgtypes::ArrayValue::new(elem, elems)));
        }
        let text = match (value, omit_quotes) {
            (JsonbValue::String(s), true) => s.clone(),
            (other, _) => other.to_text(),
        };
        self.convert(&text, ty)
    }

    /// An `EXISTS` column: the boolean the path's emptiness gives, converted to
    /// the column's type the way `PostgreSQL` converts it — through `integer`
    /// when that is the base type, and through the boolean's own text otherwise,
    /// which is why `int2 EXISTS` fails where `int4 EXISTS` succeeds.
    fn exists_column(
        &self,
        column: &JsonTableExistsColumn,
        item: &JsonbValue,
    ) -> Result<Datum, ExecError> {
        let path = column
            .path
            .as_deref()
            .map_or_else(|| Cow::Owned(default_path(&column.name)), Cow::Borrowed);
        let computed = (|| -> Result<Datum, ExecError> {
            let compiled = crate::jsonpath::JsonPath::parse(&path)?;
            let items = compiled.query(item, Some(self.vars), false)?;
            self.convert_bool(!items.is_empty(), column.ty)
        })();
        match computed {
            Ok(value) => Ok(value),
            Err(error) => match &column.on_error {
                Some(JsonBehavior::Error) => Err(error),
                Some(JsonBehavior::Unknown) => Ok(Datum::Null),
                Some(JsonBehavior::True) => self
                    .convert_bool(true, column.ty)
                    .map_err(|e| coercion_failure("ON ERROR", "TRUE", &e)),
                // FALSE is also the default when no clause was written.
                _ => self
                    .convert_bool(false, column.ty)
                    .map_err(|e| coercion_failure("ON ERROR", "FALSE", &e)),
            },
        }
    }

    /// What an `ON EMPTY` / `ON ERROR` clause produces, with the coercion
    /// failure `PostgreSQL` reports when the behavior's own value does not fit
    /// the column type.
    ///
    /// `fallback` is the column's `ON ERROR` clause when an `ON EMPTY` default
    /// is what failed to *evaluate*: `PostgreSQL` treats that as an error and
    /// hands it to `ON ERROR`.
    fn behavior_value(
        &self,
        behavior: &JsonBehavior,
        ty: ColumnType,
        clause: &'static str,
        fallback: &Option<JsonBehavior>,
    ) -> Result<Datum, ExecError> {
        let produced = match behavior {
            JsonBehavior::Error => return Err(no_item_for_column("")),
            JsonBehavior::Null | JsonBehavior::Unknown => return Ok(Datum::Null),
            JsonBehavior::True => self.convert_bool(true, ty),
            JsonBehavior::False => self.convert_bool(false, ty),
            JsonBehavior::EmptyArray => self.convert("[]", ty),
            JsonBehavior::EmptyObject => self.convert("{}", ty),
            JsonBehavior::Default(expr) => self.default_value(expr, ty),
        };
        match produced {
            Ok(value) => Ok(value),
            Err(error) => match fallback {
                Some(JsonBehavior::Error) => Err(error),
                _ => Err(coercion_failure(clause, behavior_name(behavior), &error)),
            },
        }
    }

    /// A `DEFAULT expr` behavior: evaluate, then coerce to the column type.
    fn default_value(&self, expr: &Expr, ty: ColumnType) -> Result<Datum, ExecError> {
        let value = crate::eval::eval(expr, &Scope::empty(), &[], self.ctx)?;
        if value.is_null() {
            return Ok(Datum::Null);
        }
        let cast = crabka_pgtypes::cast::cast_in(&value, ty, self.ctx.output_style())?;
        crate::usertype::check_domain(ty, &cast, self.ctx)?;
        Ok(cast)
    }

    /// Hand text to the column type's input function.
    ///
    /// Assignment rules, not explicit-cast rules: `PostgreSQL` calls the type's
    /// *input* function here, and `bpcharin`/`varcharin` reject an over-long
    /// value rather than truncating it — which is what makes `char(4)` NULL for
    /// `"aaaaaaa"` instead of `aaaa`.
    fn convert(&self, text: &str, ty: ColumnType) -> Result<Datum, ExecError> {
        let value = crabka_pgtypes::cast::cast_assign_in(
            &Datum::Text(text.to_string()),
            ty,
            self.ctx.output_style(),
        )?;
        crate::usertype::check_domain(ty, &value, self.ctx)?;
        Ok(value)
    }

    /// Convert an `EXISTS` boolean. `PostgreSQL` has a `boolean → integer` cast
    /// and no `boolean → smallint`/`bigint`/`real` one, so everything but
    /// `integer` (and a domain over it) goes through the boolean's text — and
    /// `'false'` is not valid input for those types, nor short enough for
    /// `character(3)`.
    fn convert_bool(&self, value: bool, ty: ColumnType) -> Result<Datum, ExecError> {
        let base = match ty {
            ColumnType::Domain(domain) => *domain.base,
            other => other,
        };
        let converted = match base {
            ColumnType::Bool => Datum::Bool(value),
            ColumnType::Int4 => Datum::Int4(i32::from(value)),
            _ => {
                let text = if value { "true" } else { "false" };
                crabka_pgtypes::cast::cast_assign_in(
                    &Datum::Text(text.to_string()),
                    ty,
                    self.ctx.output_style(),
                )?
            }
        };
        crate::usertype::check_domain(ty, &converted, self.ctx)?;
        Ok(converted)
    }
}

/// The implicit column path: `$."name"`, with the name JSON-escaped.
fn default_path(name: &str) -> String {
    format!("$.{}", JsonbValue::String(name.to_string()).to_text())
}

fn behavior_name(behavior: &JsonBehavior) -> &'static str {
    match behavior {
        JsonBehavior::Error => "ERROR",
        JsonBehavior::Null => "NULL",
        JsonBehavior::True => "TRUE",
        JsonBehavior::False => "FALSE",
        JsonBehavior::Unknown => "UNKNOWN",
        JsonBehavior::EmptyArray => "EMPTY ARRAY",
        JsonBehavior::EmptyObject => "EMPTY OBJECT",
        JsonBehavior::Default(_) => "DEFAULT",
    }
}

/// 42804 — the behavior clause fired, and its own value does not fit the column.
fn coercion_failure(clause: &str, behavior: &str, source: &ExecError) -> ExecError {
    ExecError::SqlJson(Box::new(SqlJsonError {
        sqlstate: "42804",
        message: format!("could not coerce {clause} expression ({behavior}) to the RETURNING type"),
        detail: Some(source.clone().into_pg().message),
        hint: None,
    }))
}

/// 22035 — `ERROR ON EMPTY` on a column whose path matched nothing.
fn no_item_for_column(column: &str) -> ExecError {
    ExecError::SqlJson(Box::new(SqlJsonError {
        sqlstate: "22035",
        message: if column.is_empty() {
            "no SQL/JSON item found for specified path".into()
        } else {
            format!("no SQL/JSON item found for specified path of column \"{column}\"")
        },
        detail: None,
        hint: None,
    }))
}

/// 22034 — more than one item with no wrapper requested.
fn more_than_one_item(column: &str) -> ExecError {
    ExecError::SqlJson(Box::new(SqlJsonError {
        sqlstate: "22034",
        message: format!(
            "JSON path expression for column \"{column}\" must return single item when no wrapper is requested"
        ),
        detail: None,
        hint: Some("Use the WITH WRAPPER clause to wrap SQL/JSON items into an array.".into()),
    }))
}

/// 2203F — a scalar column whose path produced a container or several items.
fn not_single_scalar(column: &str) -> ExecError {
    ExecError::SqlJson(Box::new(SqlJsonError {
        sqlstate: "2203F",
        message: format!(
            "JSON path expression for column \"{column}\" must return single scalar item"
        ),
        detail: None,
        hint: None,
    }))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgwire::engine::{Engine, QueryResult, Session};

    use crate::SqlEngine;

    /// One query's column names and its rows, each cell as the text the wire
    /// carries (`None` for NULL).
    async fn shape(sql: &str) -> (Vec<String>, Vec<Vec<Option<String>>>) {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run(&mut session, sql).await.expect("query ok")
    }

    async fn run(
        session: &mut crate::SqlSession,
        sql: &str,
    ) -> Result<(Vec<String>, Vec<Vec<Option<String>>>), crabka_pgwire::error::PgError> {
        let result = session.simple_query(sql).await?.pop().expect("one result");
        match result {
            QueryResult::Rows { fields, rows, .. } => Ok((
                fields.iter().map(|f| f.name.clone()).collect(),
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
            )),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// The rows of a one-column query.
    async fn column(sql: &str) -> Vec<Option<String>> {
        shape(sql)
            .await
            .1
            .into_iter()
            .map(|mut row| row.remove(0))
            .collect()
    }

    fn cells(values: &[&str]) -> Vec<Option<String>> {
        values
            .iter()
            .map(|v| (!v.is_empty()).then(|| (*v).to_string()))
            .collect()
    }

    #[tokio::test]
    async fn the_row_pattern_produces_one_row_per_item_it_matches() {
        assert!(
            column("SELECT * FROM JSON_TABLE(jsonb '[1,2,3]', '$[*]' COLUMNS (v int PATH '$'))")
                .await
                == cells(&["1", "2", "3"])
        );
        // A NULL document is an empty table, not a row of NULLs.
        assert!(
            column("SELECT * FROM JSON_TABLE(NULL::jsonb, '$' COLUMNS (v int))")
                .await
                .is_empty()
        );
        // A row-pattern error is swallowed to zero rows unless ERROR ON ERROR.
        assert!(
            column("SELECT * FROM JSON_TABLE(jsonb '1', 'strict $.a' COLUMNS (v int PATH '$'))")
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_column_without_a_path_reads_the_member_named_after_it() {
        let (names, rows) =
            shape("SELECT * FROM JSON_TABLE(jsonb '{\"aaa\": 7}', '$' COLUMNS (aaa int, bbb int))")
                .await;
        assert!(names == vec!["aaa", "bbb"]);
        assert!(rows == vec![cells(&["7", ""])]);
    }

    #[tokio::test]
    async fn for_ordinality_counts_the_rows_of_its_own_scan_level() {
        let (names, rows) = shape(
            "SELECT * FROM JSON_TABLE(jsonb '[\"a\",\"b\"]', '$[*]' \
             COLUMNS (n FOR ORDINALITY, v text PATH '$'))",
        )
        .await;
        assert!(names == vec!["n", "v"]);
        assert!(rows == vec![cells(&["1", "a"]), cells(&["2", "b"])]);
    }

    /// A value column runs as `JSON_VALUE` — the item is rendered with its SQL
    /// type's output function, so a JSON boolean reads as `f` — unless a
    /// wrapper, a quotes clause, `FORMAT JSON` or a composite-ish return type
    /// promotes it to `JSON_QUERY`, where the JSON rendering is what converts.
    #[tokio::test]
    async fn a_columns_kind_decides_how_its_item_is_rendered() {
        let cases: &[(&str, &str, &str)] = &[
            ("text PATH '$'", "false", "f"),
            ("text PATH '$'", "\"str\"", "str"),
            ("text FORMAT JSON PATH '$'", "false", "false"),
            ("text FORMAT JSON PATH '$'", "\"str\"", "\"str\""),
            ("text FORMAT JSON PATH '$' OMIT QUOTES", "\"str\"", "str"),
            ("jsonb PATH '$' WITH WRAPPER", "1", "[1]"),
            // char(4) is an input conversion, so an over-long value is an error
            // the default NULL ON ERROR swallows — not a truncation.
            ("char(4) PATH '$'", "\"aaaaaaa\"", ""),
            ("char(4) PATH '$'", "\"aa\"", "aa  "),
        ];
        for (spec, document, expected) in cases {
            let sql =
                format!("SELECT * FROM JSON_TABLE(jsonb '{document}', '$' COLUMNS (v {spec}))");
            assert!(column(&sql).await == cells(&[expected]), "{sql}");
        }
    }

    /// An array column is populated element-wise and needs a JSON array, so a
    /// JSON object is an error rather than the empty array `'{}'::int[]` is.
    #[tokio::test]
    async fn an_array_column_requires_a_json_array() {
        assert!(
            column("SELECT * FROM JSON_TABLE(jsonb '[1,2]', '$' COLUMNS (v int[] PATH '$'))").await
                == cells(&["{1,2}"])
        );
        assert!(
            column("SELECT * FROM JSON_TABLE(jsonb '{}', '$' COLUMNS (v int[] PATH '$'))").await
                == cells(&[""])
        );
    }

    /// `EXISTS` converts through `integer` when that is the base type and
    /// through the boolean's own text otherwise, which is why `int4` works where
    /// `int2` cannot read `'false'`.
    #[tokio::test]
    async fn an_exists_column_converts_the_way_postgresql_does() {
        let cases: &[(&str, &str)] = &[
            ("bool EXISTS PATH '$.a'", "f"),
            ("int4 EXISTS PATH '$.a'", "0"),
            ("int4 EXISTS PATH '$'", "1"),
            ("text EXISTS PATH '$.a'", "false"),
            ("char(5) EXISTS PATH '$.a'", "false"),
            ("int EXISTS PATH 'strict $.a' UNKNOWN ON ERROR", ""),
        ];
        for (spec, expected) in cases {
            let sql = format!("SELECT * FROM JSON_TABLE(jsonb '\"a\"', '$' COLUMNS (v {spec}))");
            assert!(column(&sql).await == cells(&[expected]), "{sql}");
        }
    }

    /// Sibling `NESTED` sets are UNIONed and the union is outer-joined to its
    /// parent: a parent row survives even when every nested set is empty, and
    /// each sibling's rows leave the others' columns NULL.
    #[tokio::test]
    async fn sibling_nested_paths_union_and_outer_join_to_their_parent() {
        let (names, rows) = shape(
            "SELECT * FROM JSON_TABLE(jsonb '[{\"b\": [], \"c\": []}, {\"b\": [1], \"c\": [2,3]}]', \
             '$[*]' COLUMNS ( \
                n FOR ORDINALITY, \
                NESTED PATH '$.b[*]' COLUMNS (b int PATH '$'), \
                NESTED PATH '$.c[*]' COLUMNS (c int PATH '$')))",
        )
        .await;
        // Parent columns come first, then each nested set in declaration order.
        assert!(names == vec!["n", "b", "c"]);
        assert!(
            rows == vec![
                cells(&["1", "", ""]),
                cells(&["2", "1", ""]),
                cells(&["2", "", "2"]),
                cells(&["2", "", "3"]),
            ]
        );
    }

    #[tokio::test]
    async fn a_nested_path_nests_further_and_outer_joins_at_every_level() {
        let rows = shape(
            "SELECT * FROM JSON_TABLE(jsonb '[[1],[2]]', '$[*]' COLUMNS ( \
                NESTED PATH '$[*] ? (@ > 1)' COLUMNS (v int PATH '$')))",
        )
        .await
        .1;
        // The first element matches nothing, and still contributes one row.
        assert!(rows == vec![cells(&[""]), cells(&["2"])]);
    }

    #[tokio::test]
    async fn passing_variables_reach_the_row_pattern_and_every_column_path() {
        assert!(
            column(
                "SELECT * FROM JSON_TABLE(jsonb '[1,2,3]', '$[*] ? (@ < $x)' PASSING 3 AS x, 2 AS y \
                 COLUMNS (v text FORMAT JSON PATH '$ ? (@ < $y)'))"
            )
            .await
                == cells(&["1", ""])
        );
    }

    #[tokio::test]
    async fn on_empty_and_on_error_pick_the_value_and_the_diagnostics() {
        let cases: &[(&str, &str, &str)] = &[
            (
                "int PATH 'lax $.a' DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR",
                "\"a\"",
                "1",
            ),
            (
                "int PATH 'strict $.a' DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR",
                "\"a\"",
                "2",
            ),
            (
                "int PATH '$' DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR",
                "\"a\"",
                "2",
            ),
        ];
        for (spec, document, expected) in cases {
            let sql =
                format!("SELECT * FROM JSON_TABLE(jsonb '{document}', '$' COLUMNS (v {spec}))");
            assert!(column(&sql).await == cells(&[expected]), "{sql}");
        }
    }

    /// The negative cases, each with the SQLSTATE and message `PostgreSQL`
    /// reports for it.
    #[tokio::test]
    async fn the_refusals_carry_postgresqls_sqlstates_and_messages() {
        let cases: &[(&str, &str, &str)] = &[
            (
                "SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (v int) NULL ON ERROR)",
                "42601",
                "invalid ON ERROR behavior",
            ),
            (
                "SELECT * FROM JSON_TABLE(jsonb '1', '$' AS a COLUMNS (a int))",
                "42712",
                "duplicate JSON_TABLE column or path name: a",
            ),
            (
                "SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a int, a int))",
                "42712",
                "duplicate JSON_TABLE column or path name: a",
            ),
            (
                "SELECT * FROM JSON_TABLE(jsonb '1', '$' \
                 COLUMNS (NESTED PATH '$' AS b COLUMNS (c int), b int))",
                "42712",
                "duplicate JSON_TABLE column or path name: b",
            ),
            (
                "SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a FOR ORDINALITY, b FOR ORDINALITY))",
                "42601",
                "only one FOR ORDINALITY column is allowed",
            ),
            (
                "SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS ())",
                "42601",
                "syntax error at or near \")\"",
            ),
            (
                "SELECT * FROM JSON_TABLE(jsonb '1', '$' || '.a' COLUMNS (a int))",
                "0A000",
                "only string constants are supported in JSON_TABLE path specification",
            ),
            (
                "SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a int)) AS f(v1, v2)",
                "42P10",
                "JSON_TABLE function has 1 columns available but 2 columns specified",
            ),
            (
                "SELECT * FROM JSON_TABLE(jsonb '\"w\"', '$' \
                 COLUMNS (a text PATH '$' WITH WRAPPER OMIT QUOTES))",
                "42601",
                "SQL/JSON QUOTES behavior must not be specified when WITH WRAPPER is used",
            ),
            (
                "SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a int PATH '$.a' ERROR ON EMPTY))",
                "22035",
                "no SQL/JSON item found for specified path of column \"a\"",
            ),
            (
                "SELECT * FROM JSON_TABLE(jsonb '{\"a\": [1, 2]}', '$' \
                 COLUMNS (b jsonb PATH '$.a[*]' ERROR ON ERROR))",
                "22034",
                "JSON path expression for column \"b\" must return single item when no wrapper is requested",
            ),
            (
                "SELECT * FROM JSON_TABLE(jsonb '\"a\"', '$' COLUMNS (a int2 EXISTS PATH '$.a'))",
                "42804",
                "could not coerce ON ERROR expression (FALSE) to the RETURNING type",
            ),
            // JSON_TABLE is a FROM item; PostgreSQL's grammar has no expression
            // production for it.
            (
                "SELECT JSON_TABLE('[]', '$')",
                "42601",
                "syntax error at or near \"(\"",
            ),
        ];
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        for (sql, sqlstate, message) in cases {
            let error = run(&mut session, sql).await.expect_err(sql);
            assert!(error.code == *sqlstate, "{sql}: {error:?}");
            assert!(error.message == *message, "{sql}: {error:?}");
        }
    }

    /// The behavior words each column kind admits, with the detail line naming
    /// the admitted set.
    #[tokio::test]
    async fn a_behavior_the_column_kind_cannot_use_names_the_ones_it_can() {
        let cases: &[(&str, &str, &str)] = &[
            (
                "a int TRUE ON EMPTY",
                "invalid ON EMPTY behavior for column \"a\"",
                "Only ERROR, NULL, or DEFAULT expression is allowed in ON EMPTY for scalar columns.",
            ),
            (
                "a int OMIT QUOTES TRUE ON ERROR",
                "invalid ON ERROR behavior for column \"a\"",
                "Only ERROR, NULL, EMPTY ARRAY, EMPTY OBJECT, or DEFAULT expression is allowed in ON ERROR for formatted columns.",
            ),
            (
                "a int EXISTS EMPTY OBJECT ON ERROR",
                "invalid ON ERROR behavior for column \"a\"",
                "Only ERROR, TRUE, FALSE, or UNKNOWN is allowed in ON ERROR for EXISTS columns.",
            ),
        ];
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        for (spec, message, detail) in cases {
            let sql = format!("SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS ({spec}))");
            let error = run(&mut session, &sql).await.expect_err(&sql);
            assert!(error.code == "42601", "{sql}: {error:?}");
            assert!(error.message == *message, "{sql}: {error:?}");
            assert!(
                error.diagnostics.as_ref().and_then(|d| d.detail.as_deref()) == Some(*detail),
                "{sql}: {error:?}"
            );
        }
    }

    /// The item is implicitly `LATERAL`: its context expression may read a FROM
    /// item to its left with no keyword, and is re-evaluated per outer row.
    #[tokio::test]
    async fn the_context_item_may_read_an_earlier_from_item() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        for setup in [
            "CREATE TABLE docs (js jsonb)",
            "INSERT INTO docs VALUES ('[1,2]'), ('[3]')",
        ] {
            session.simple_query(setup).await.expect(setup);
        }
        let rows = run(
            &mut session,
            "SELECT d.js, jt.v FROM docs d, JSON_TABLE(d.js, '$[*]' COLUMNS (v int PATH '$')) jt \
             ORDER BY jt.v",
        )
        .await
        .expect("query")
        .1;
        assert!(
            rows == vec![
                cells(&["[1, 2]", "1"]),
                cells(&["[1, 2]", "2"]),
                cells(&["[3]", "3"]),
            ]
        );
    }
}
