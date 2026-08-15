//! The `json`/`jsonb` record-mapping family: `json_populate_record`,
//! `json_populate_recordset`, `json_to_record`, `json_to_recordset` and the four
//! `jsonb_` twins.
//!
//! All eight map a JSON *object* onto a composite type by field name, and all
//! eight share the walk in this module. What differs is where the target's shape
//! comes from — a `populate_*` call takes it from its first argument's type, a
//! `to_*` call from the FROM item's column-definition list — and which of the two
//! document representations the family reads.
//!
//! That second difference is not cosmetic. `json` keeps the document's input
//! text, so a sub-document landing in a `text` column arrives with its original
//! spacing (`{"a" :  1}`) and a number keeps the notation it was written in
//! (`1e3`); `jsonb` has already decomposed the document, so the same field
//! arrives canonically rendered (`{"a": 1}`, `1000`). [`Node`] is the seam: one
//! walk, two readings.

use std::{borrow::Cow, fmt::Write as _};

use crabka_pgtypes::{
    ArrayDim, ArrayValue, ColumnType, Datum, ElemType, RecordValue,
    json::{self, Kind},
    jsonb::{self, JsonbValue},
    usertype,
};
use crabka_pgwire::error::PgError;

use crate::{clock::EvalCtx, error::ExecError};

/// Which of the two JSON types a call reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Flavour {
    /// `json_*` — the document's original text, so spacing, key order, duplicate
    /// keys and number notation all survive into the populated fields.
    Json,
    /// `jsonb_*` — the decomposed document, canonically rendered.
    Jsonb,
}

impl Flavour {
    /// The document parameter's type.
    pub(crate) fn document(self) -> ColumnType {
        match self {
            Flavour::Json => ColumnType::Json,
            Flavour::Jsonb => ColumnType::Jsonb,
        }
    }
}

/// One JSON value, in whichever representation its family carries.
///
/// The two arms are not interchangeable: `Json` borrows a slice of the original
/// document text and `Jsonb` borrows a decomposed value, and every accessor below
/// preserves that difference rather than normalizing it away.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Node<'a> {
    Json(&'a str),
    Jsonb(&'a JsonbValue),
}

impl<'a> Node<'a> {
    /// The document a call's second argument denotes, typed by the call's family.
    ///
    /// A `json` call reads `Datum::Json`, a `jsonb` call reads `Datum::Jsonb`;
    /// the coercion of an `unknown` literal to one or the other has already run
    /// by the time a value gets here.
    ///
    /// The whole family populates fields from *decoded* strings — object keys to
    /// match on and `text` values to store — so `get_json_object_as_hash`,
    /// `populate_array_json` and `populate_recordset_worker` all build their
    /// lexer with `need_escapes` set. That check belongs here rather than at the
    /// point a string is decoded, because upstream parses the document up front
    /// and so rejects a bad escape in a field the call never reads.
    pub(crate) fn of(value: &'a Datum, flavour: Flavour) -> Result<Self, ExecError> {
        match (flavour, value) {
            (Flavour::Json, Datum::Json(text)) => {
                json::validate_escapes(text)?;
                Ok(Node::Json(text))
            }
            (Flavour::Jsonb, Datum::Jsonb(value)) => Ok(Node::Jsonb(value)),
            (_, other) => Err(ExecError::TypeMismatch(format!(
                "{} does not accept an argument of type {}",
                flavour.document().name(),
                other
                    .column_type()
                    .map_or("unknown", crabka_pgtypes::ColumnType::name)
            ))),
        }
    }

    fn kind(self) -> Kind {
        match self {
            Node::Json(text) => json::kind(text),
            Node::Jsonb(value) => match value {
                JsonbValue::Null => Kind::Null,
                JsonbValue::Bool(_) => Kind::Bool,
                JsonbValue::Number(_) => Kind::Number,
                JsonbValue::String(_) => Kind::String,
                JsonbValue::Array(_) => Kind::Array,
                JsonbValue::Object(_) => Kind::Object,
            },
        }
    }

    /// Is this the JSON `null` literal? (Which populates a field with SQL NULL,
    /// whatever the field's type.)
    fn is_json_null(self) -> bool {
        self.kind() == Kind::Null
    }

    /// The object's members in document order, or `None` when this is not an
    /// object. Duplicate keys are *kept* here — [`field`](Self::field) resolves
    /// them the way both families do, by taking the last.
    fn object_fields(self) -> Option<Vec<(String, Node<'a>)>> {
        match self {
            Node::Json(text) => Some(
                json::object_fields(text)?
                    .into_iter()
                    .map(|(key, value)| (key, Node::Json(value)))
                    .collect(),
            ),
            Node::Jsonb(JsonbValue::Object(pairs)) => Some(
                pairs
                    .iter()
                    .map(|(key, value)| (key.clone(), Node::Jsonb(value)))
                    .collect(),
            ),
            Node::Jsonb(_) => None,
        }
    }

    fn array_elements(self) -> Option<Vec<Node<'a>>> {
        match self {
            Node::Json(text) => Some(
                json::array_elements(text)?
                    .into_iter()
                    .map(Node::Json)
                    .collect(),
            ),
            Node::Jsonb(JsonbValue::Array(items)) => Some(items.iter().map(Node::Jsonb).collect()),
            Node::Jsonb(_) => None,
        }
    }

    /// The text a non-JSON, non-composite column is populated from — the `->>`
    /// rendering: a JSON string unquoted, everything else as this family renders
    /// it.
    fn as_sql_text(self) -> String {
        match self {
            Node::Json(text) => json::as_text(text),
            Node::Jsonb(JsonbValue::String(s)) => s.clone(),
            Node::Jsonb(other) => other.to_text(),
        }
    }

    /// This value as a `json` document: the original text for the `json` family,
    /// the canonical rendering for `jsonb`.
    fn as_json_text(self) -> String {
        match self {
            Node::Json(text) => text.trim().to_string(),
            Node::Jsonb(value) => json::from_jsonb(value),
        }
    }

    /// This value as a `jsonb` document.
    fn as_jsonb(self) -> Result<Cow<'a, JsonbValue>, ExecError> {
        match self {
            Node::Json(text) => jsonb::parse(text).map(Cow::Owned).map_err(ExecError::Type),
            Node::Jsonb(value) => Ok(Cow::Borrowed(value)),
        }
    }
}

/// The composite a call populates: its fields, in declaration order.
///
/// A `populate_*` call reads these off the named composite its first argument is
/// typed as; a `to_*` call reads them off the FROM item's column-definition list.
/// Field *names* are matched against the document's keys exactly — PostgreSQL
/// looks the attribute name up in a hash of the object, so `{"A": 1}` does not
/// populate a column called `a`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordShape {
    pub(crate) fields: Vec<(String, ColumnType)>,
}

impl RecordShape {
    /// The shape of a named composite type, or `None` when `ty` is not one (the
    /// anonymous `record`, or anything else).
    pub(crate) fn of(ty: ColumnType) -> Option<Self> {
        let ColumnType::Record(Some(named)) = ty else {
            return None;
        };
        let registered = usertype::lookup_oid(named.oid)?;
        let fields = registered
            .fields()?
            .iter()
            .map(|field| (field.name.clone(), field.ty))
            .collect();
        Some(RecordShape { fields })
    }

    /// The shape a record *value* carries.
    ///
    /// A value knows its field names always, and each field's type whenever that
    /// field is non-NULL. `PostgreSQL` reads both off the tuple descriptor, which
    /// types a NULL field too; this type layer's anonymous `record` carries no
    /// descriptor, so a NULL field falls back to `text` — the type its own text
    /// rendering would take. That only matters for a field the document *also*
    /// omits, whose value is inherited unchanged either way.
    pub(crate) fn of_value(value: &RecordValue) -> Self {
        let fields = value
            .names
            .iter()
            .cloned()
            .zip(&value.values)
            .map(|(name, datum)| (name, datum.column_type().unwrap_or(ColumnType::Text)))
            .collect();
        RecordShape { fields }
    }
}

/// Populate one composite from one JSON object.
///
/// `base` supplies the value of every field the document has no key for, which
/// is what makes `json_populate_record(row('x',3,…)::jpop, '{"a":"y"}')` keep the
/// row's `b` and `c`. A key that *is* present with a JSON `null` overrides the
/// base with SQL NULL rather than falling back to it.
pub(crate) fn populate(
    shape: &RecordShape,
    base: Option<&RecordValue>,
    doc: Node<'_>,
    ctx: &EvalCtx,
) -> Result<Vec<Datum>, ExecError> {
    let Some(members) = doc.object_fields() else {
        return Err(populate_composite_shape(doc.kind()));
    };
    shape
        .fields
        .iter()
        .enumerate()
        .map(|(index, (name, ty))| {
            // Duplicate keys resolve to the last, as both families' own lookups do.
            match members.iter().rev().find(|(key, _)| key == name) {
                Some((_, value)) => populate_value(*ty, *value, &Context::key(name), ctx),
                None => {
                    let inherited = base
                        .and_then(|record| record.values.get(index))
                        .cloned()
                        .unwrap_or(Datum::Null);
                    crate::usertype::check_domain(*ty, &inherited, ctx)?;
                    Ok(inherited)
                }
            }
        })
        .collect()
}

/// The row a `populate_record` call yields when its document is SQL NULL: the
/// base row, or all NULLs.
///
/// This is not the same as producing no row. `PostgreSQL` declares
/// `json_populate_record` non-strict precisely so that a NULL document still
/// answers with the base record — it is `json_populate_recordset` that answers
/// with the empty set.
pub(crate) fn populate_missing(
    shape: &RecordShape,
    base: Option<&RecordValue>,
    ctx: &EvalCtx,
) -> Result<Vec<Datum>, ExecError> {
    shape
        .fields
        .iter()
        .enumerate()
        .map(|(index, (_, ty))| {
            let inherited = base
                .and_then(|record| record.values.get(index))
                .cloned()
                .unwrap_or(Datum::Null);
            crate::usertype::check_domain(*ty, &inherited, ctx)?;
            Ok(inherited)
        })
        .collect()
}

/// Populate a *set* of composites from one JSON array of objects.
pub(crate) fn populate_set(
    shape: &RecordShape,
    base: Option<&RecordValue>,
    doc: Node<'_>,
    name: &str,
    flavour: Flavour,
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let Some(items) = doc.array_elements() else {
        return Err(recordset_not_array(name, flavour, doc.kind()));
    };
    items
        .into_iter()
        .map(|item| {
            if item.object_fields().is_none() {
                return Err(invalid_parameter(format!(
                    "argument of {name} must be an array of objects"
                )));
            }
            populate(shape, base, item, ctx)
        })
        .collect()
}

/// Where in the document the value being populated came from, for the array
/// walker's `HINT`.
struct Context<'a> {
    key: &'a str,
    /// The subscript path inside the value, outermost first; empty at the value
    /// itself.
    indexes: Vec<usize>,
}

impl<'a> Context<'a> {
    fn key(key: &'a str) -> Self {
        Context {
            key,
            indexes: Vec::new(),
        }
    }

    /// PostgreSQL's `populate_array_report_expected_array` hint: the value of the
    /// key at the top, a subscripted element below it.
    fn hint(&self) -> String {
        if self.indexes.is_empty() {
            return format!("See the value of key \"{}\".", self.key);
        }
        let mut path = String::new();
        for index in &self.indexes {
            let _ = write!(path, "[{index}]");
        }
        format!("See the array element {path} of key \"{}\".", self.key)
    }
}

/// Populate one field.
fn populate_value(
    target: ColumnType,
    node: Node<'_>,
    context: &Context<'_>,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    // A domain contributes its constraints, not its shape: the value is built for
    // the base type and then checked.
    if let ColumnType::Domain(domain) = target {
        let value = populate_value(*domain.base, node, context, ctx)?;
        crate::usertype::check_domain(target, &value, ctx)?;
        return Ok(value);
    }
    if node.is_json_null() {
        return Ok(Datum::Null);
    }
    match target {
        ColumnType::Json => Ok(Datum::Json(node.as_json_text())),
        ColumnType::Jsonb => Ok(Datum::Jsonb(node.as_jsonb()?.into_owned())),
        ColumnType::Array(elem) => populate_array(elem, node, context, ctx),
        ColumnType::Record(_) => populate_composite(target, node, ctx),
        _ => {
            let text = Datum::Text(node.as_sql_text());
            crabka_pgtypes::cast::cast_assign_in(&text, target, ctx.output_style())
                .map_err(ExecError::from)
        }
    }
}

/// Populate a nested composite field.
///
/// A JSON object recurses; a JSON *string* is handed to `record_in` instead,
/// which is how `'{"rec": "(abc,42,01.02.2003)"}'` populates a `jpop`. Anything
/// else is the same 22023 the top level raises.
fn populate_composite(
    target: ColumnType,
    node: Node<'_>,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    if node.kind() == Kind::String {
        let text = Datum::Text(node.as_sql_text());
        return crabka_pgtypes::cast::cast_assign_in(&text, target, ctx.output_style())
            .map_err(ExecError::from);
    }
    let ColumnType::Record(named) = target else {
        unreachable!("only a record type reaches populate_composite");
    };
    let Some(shape) = RecordShape::of(target) else {
        return Err(indeterminate_row_type("populate_composite"));
    };
    let values = populate(&shape, None, node, ctx)?;
    let fields: Vec<String> = shape
        .fields
        .iter()
        .map(|(field, _)| field.clone())
        .collect();
    Ok(Datum::Record(RecordValue::named(
        named,
        fields.into(),
        values,
    )))
}

/// Populate an array field from a JSON array (or from a SQL array literal held
/// in a JSON string).
///
/// `PostgreSQL` has no nested array *type* — `int[][]` is `_int4` — so the
/// dimension count comes from the document: the walker descends the first
/// element of each level until it reaches a non-array, and every sibling must
/// then agree on both array-ness and length.
fn populate_array(
    elem: ElemType,
    node: Node<'_>,
    context: &Context<'_>,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    if node.kind() == Kind::String {
        let text = Datum::Text(node.as_sql_text());
        return crabka_pgtypes::cast::cast_assign_in(
            &text,
            ColumnType::Array(elem),
            ctx.output_style(),
        )
        .map_err(ExecError::from);
    }
    let Some(items) = node.array_elements() else {
        return Err(expected_json_array(context));
    };
    let mut walk = ArrayWalk {
        elem,
        depth: array_depth(&items),
        dims: vec![ArrayDim::from_len(items.len())],
        flat: Vec::new(),
        key: context.key,
        path: Vec::new(),
        ctx,
    };
    walk.level(&items, 1)?;
    Ok(Datum::Array(ArrayValue::with_dims(
        elem, walk.flat, walk.dims,
    )))
}

/// How many array levels the document nests, following the first element down.
/// `PostgreSQL` fixes the dimension count this way and then holds every sibling
/// to it, which is why `[[1], 2]` is an error rather than a ragged array.
fn array_depth(items: &[Node<'_>]) -> usize {
    let mut depth = 1;
    let mut level = items.first().copied();
    while let Some(node) = level {
        let Some(inner) = node.array_elements() else {
            break;
        };
        depth += 1;
        level = inner.first().copied();
    }
    depth
}

/// The state of one array field's walk: the dimensions discovered so far, the
/// leaf values in row-major order, and the subscript path the `HINT` names.
struct ArrayWalk<'a, 'ctx> {
    elem: ElemType,
    depth: usize,
    dims: Vec<ArrayDim>,
    flat: Vec<Datum>,
    key: &'a str,
    path: Vec<usize>,
    ctx: &'ctx EvalCtx,
}

impl ArrayWalk<'_, '_> {
    /// Walk one level, appending its leaves.
    fn level(&mut self, items: &[Node<'_>], level: usize) -> Result<(), ExecError> {
        for (index, item) in items.iter().enumerate() {
            self.path.push(index);
            let step = self.element(*item, level);
            self.path.pop();
            step?;
        }
        Ok(())
    }

    fn element(&mut self, item: Node<'_>, level: usize) -> Result<(), ExecError> {
        if level == self.depth {
            let context = self.context();
            let value = populate_value(self.elem.column_type(), item, &context, self.ctx)?;
            self.flat.push(value);
            return Ok(());
        }
        let Some(inner) = item.array_elements() else {
            return Err(expected_json_array(&self.context()));
        };
        match self.dims.get(level) {
            None => self.dims.push(ArrayDim::from_len(inner.len())),
            Some(seen) if usize::try_from(seen.len).unwrap_or(usize::MAX) == inner.len() => {}
            Some(_) => return Err(mismatched_array_dimensions()),
        }
        self.level(&inner, level + 1)
    }

    fn context(&self) -> Context<'_> {
        Context {
            key: self.key,
            indexes: self.path.clone(),
        }
    }
}

// ---- errors ----

/// A `PostgreSQL` `invalid_parameter_value` (22023) — the SQLSTATE this whole
/// family's shape refusals use.
fn invalid_parameter(message: String) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "22023",
        message,
    }
}

/// The refusal both families raise when a value that must be an object is not.
///
/// `PostgreSQL` reports the *internal* routine's name here rather than the SQL
/// function's, so `json_to_record('[1,2]')` and `jsonb_populate_record(…, '3')`
/// both say `populate_composite`. That is not an oversight to correct: the same
/// wording appears for the nested-composite case, where no SQL function name
/// would be right.
fn populate_composite_shape(kind: Kind) -> ExecError {
    invalid_parameter(match kind {
        Kind::Array => "cannot call populate_composite on an array".into(),
        _ => "cannot call populate_composite on a scalar".into(),
    })
}

/// The two families word a non-array argument to `*_populate_recordset` /
/// `*_to_recordset` differently, because they find out differently: `json`'s
/// parser trips at the first structural token, so it can say *what* it found,
/// while `jsonb` has the whole decomposed value and only checks that the root is
/// an array.
fn recordset_not_array(name: &str, flavour: Flavour, kind: Kind) -> ExecError {
    invalid_parameter(match (flavour, kind) {
        (Flavour::Jsonb, _) => format!("cannot call {name} on a non-array"),
        (Flavour::Json, Kind::Object) => format!("cannot call {name} on an object"),
        (Flavour::Json, _) => format!("cannot call {name} on a scalar"),
    })
}

fn expected_json_array(context: &Context<'_>) -> ExecError {
    ExecError::Remote(PgError::error("22P02", "expected JSON array").with_hint(context.hint()))
}

fn mismatched_array_dimensions() -> ExecError {
    ExecError::Remote(
        PgError::error("22P02", "malformed JSON array")
            .with_detail("Multidimensional arrays must have sub-arrays with matching dimensions."),
    )
}

/// PostgreSQL's 0A000 for a record-returning call whose row type nothing pins
/// down — a select-list `json_to_record(…)`, or `json_populate_record(null::record, …)`.
pub(crate) fn indeterminate_row_type(name: &str) -> ExecError {
    ExecError::Remote(
        PgError::error(
            "0A000",
            format!("could not determine row type for result of {name}"),
        )
        .with_hint(
            "Provide a non-null record argument, or call the function in the FROM clause \
             using a column definition list.",
        ),
    )
}
