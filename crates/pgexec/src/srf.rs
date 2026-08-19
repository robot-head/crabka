//! Set-returning functions (SRFs): the one registry that answers, for a
//! function name and its arguments, the *columns* the call produces and the
//! *rows* it expands to.
//!
//! Two functions describe every SRF once. [`plan`] gives the names and types,
//! for RowDescription and the extended protocol's `Describe`, and [`rows`] gives
//! the values. Both the FROM-item path, [`from_item`] and [`from_item_schema`],
//! and the select-list path, [`project_rows_ordered`], drive that single
//! description. So a prepared statement's `RowDescription` cannot drift from
//! what execution returns.
//!
//! Naming follows PostgreSQL. A single-column SRF names its column after the
//! function, as `generate_series` and `unnest` do. A multi-column one names them
//! individually, so `jsonb_each` gives `key` and `value`. A bare `AS u` on a
//! single-column item renames the column as well as the qualifier, and
//! `AS u(a, b)` renames each column positionally.
//!
//! ## Deliberate divergences from PostgreSQL
//!
//! - A **multi-column** SRF in the select list, such as `SELECT jsonb_each(j)`,
//!   is `0A000`. PostgreSQL returns one `record`-typed column, and crabka has no
//!   composite type to put in it.
//! - An SRF nested inside **another SRF's arguments**, such as
//!   `SELECT generate_series(1, generate_series(1, 2))`, is `0A000`. PostgreSQL
//!   lifts the inner call into its own ProjectSet level.
//! - An SRF in an **aggregate query's** select list is `0A000`. PostgreSQL
//!   evaluates SRFs after aggregation.
//! - `SELECT *` over a multi-argument `unnest(a, b)` written without column
//!   aliases is `42702`. PostgreSQL names both output columns `unnest` and
//!   expands `*` positionally, while crabka expands a wildcard into one
//!   *by-name* column reference each, so two identically named columns under one
//!   qualifier are ambiguous. `unnest(a, b) AS t(x, y)` names them apart and
//!   works. The same by-name expansion makes `SELECT *` over a derived table
//!   with duplicate output names ambiguous, so this is not specific to SRFs.
//!
//! Multiple SRFs in one select list *are* supported and follow PostgreSQL 10+
//! semantics. The calls run in lockstep, and the shorter ones pad with NULL
//! until the longest is exhausted. The pre-10 "least common multiple" rule is
//! gone from PostgreSQL itself.

use std::borrow::Cow;

use crabka_pgparser::ast::{
    ArraySubscript, Expr, FuncArgs, FuncCall, SelectItem, SelectStmt, TableFuncCall,
    TableFuncColumnDef,
};
use crabka_pgtypes::{
    ArrayValue, ColumnType, Datum, ElemType, RecordValue, TypeError, numeric::NumericValue,
    usertype::UserTypeRef,
};
use crabka_pgwire::engine::FieldDescription;

use crate::{
    clock::EvalCtx,
    error::ExecError,
    eval::ArgType,
    join::Relation,
    json_fn::JsonbSrf,
    json_record::RecordShape,
    regexp_fn::{compile_pattern, group_datums},
    scope::{ColumnBinding, Exposure, Scope},
};

/// The columns and rows a single FROM-position function call produces.
pub(crate) type FunctionCallRows = (Vec<(String, ColumnType)>, Vec<Vec<Datum>>);

/// The set-returning functions crabka implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Srf {
    /// `unnest(anyarray [, anyarray …])`: one column per argument, with shorter
    /// arrays padded with NULL. PostgreSQL expands the multi-argument form as
    /// `ROWS FROM`.
    Unnest,
    /// `generate_series(start, stop [, step])` over int4/int8/numeric, and
    /// `(timestamp|timestamptz, same, interval)`, plus the four-argument
    /// `timestamptz` form whose final argument names the stepping zone.
    GenerateSeries,
    /// `generate_subscripts(anyarray, dim [, reverse])`.
    GenerateSubscripts,
    /// `string_to_table(text, delimiter [, null_string])`.
    StringToTable,
    /// `regexp_split_to_table(text, pattern [, flags])`.
    RegexpSplitToTable,
    /// `regexp_matches(text, pattern [, flags])` → one `text[]` per match.
    ///
    /// The set-returning sibling of `regexp_match`: without the `g` flag it
    /// produces the first match's capture groups and stops, and with `g` it
    /// produces one row per non-overlapping match. A pattern that matches
    /// nothing produces no rows at all rather than a NULL one.
    RegexpMatches,
    /// `json_each(json)` → `(key text, value json)`, `jsonb_each(jsonb)` →
    /// `(key text, value jsonb)`.
    Each(JsonFamily),
    /// `json_each_text`/`jsonb_each_text` → `(key text, value text)`.
    EachText(JsonFamily),
    /// `json_object_keys`/`jsonb_object_keys` → `text`.
    ObjectKeys(JsonFamily),
    /// `json_array_elements(json)` → `value json`,
    /// `jsonb_array_elements(jsonb)` → `value jsonb`.
    ArrayElements(JsonFamily),
    /// `json_array_elements_text`/`jsonb_array_elements_text` → `value text`.
    ArrayElementsText(JsonFamily),
    /// `jsonb_path_query(target, path [, vars [, silent]])` → one row per item
    /// the jsonpath produces. There is no `json_path_query`: `PostgreSQL`
    /// declares the jsonpath functions over `jsonb` alone.
    JsonbPathQuery,
    /// `json_populate_record` and its seven relatives — see [`RecordCall`].
    Record(RecordCall),
    PgInputErrorInfo,
    /// `pg_snapshot_xip(pg_snapshot)` → `xid8`, and `txid_snapshot_xip`, which
    /// is the same expansion reported as `bigint`. One row per running
    /// transaction the snapshot lists, ascending, and no row at all for a
    /// snapshot with an empty window.
    SnapshotXip(SnapshotFamily),
    /// `pg_partition_ancestors(regclass)` → `relid regclass` — the relation
    /// itself, then every parent up to the root of its partition tree.
    PgPartitionAncestors,
    EventDdlCommands,
    EventDroppedObjects,
}

/// Which of the two JSON document types one of the five expansion shapes reads.
///
/// `PostgreSQL` declares them as two families of five, over `json` and over
/// `jsonb`, and the families are not interchangeable: there is no implicit cast
/// between the types, so `json_each('{}'::jsonb)` is a 42883 rather than a
/// coercion. The document type is also the *output* type of the two shapes that
/// hand back a sub-document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonFamily {
    /// `json` — the original input text, so whitespace, object key order and
    /// duplicate keys all survive into the expansion.
    Json,
    /// `jsonb` — decomposed and canonically ordered, with duplicate keys already
    /// resolved to the last one.
    Jsonb,
}

/// Which of the two declared spellings a snapshot expansion belongs to.
///
/// `pg_snapshot_xip` reads a `pg_snapshot` and reports `xid8`;
/// `txid_snapshot_xip` reads a `txid_snapshot` and reports `bigint`. The two
/// run the same C function upstream, and they expand the same value here, so
/// the family decides only which types the signature names. The scalar half of
/// the same surface is [`crate::snapshot_fn`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotFamily {
    Modern,
    Legacy,
}

impl SnapshotFamily {
    /// The type the family's argument carries.
    fn snapshot_type(self) -> ColumnType {
        match self {
            SnapshotFamily::Modern => ColumnType::PgSnapshot,
            SnapshotFamily::Legacy => ColumnType::TxidSnapshot,
        }
    }

    /// The type the family reports one running transaction id as.
    fn xid_type(self) -> ColumnType {
        match self {
            SnapshotFamily::Modern => ColumnType::Xid8,
            SnapshotFamily::Legacy => ColumnType::Int8,
        }
    }

    /// One running transaction id as a value of that type.
    fn xid_datum(self, xid: u64) -> Datum {
        match self {
            SnapshotFamily::Modern => Datum::Xid8(xid),
            // `bigint` reinterprets the bits, as the whole `txid_*` family does.
            SnapshotFamily::Legacy => Datum::Int8(xid.cast_signed()),
        }
    }
}

impl JsonFamily {
    /// The type the family's argument carries — and, for `each` and
    /// `array_elements`, the type of the sub-document column they produce.
    fn column_type(self) -> ColumnType {
        match self {
            JsonFamily::Json => ColumnType::Json,
            JsonFamily::Jsonb => ColumnType::Jsonb,
        }
    }

    /// The same distinction as seen by the shared population walk.
    fn flavour(self) -> crate::json_record::Flavour {
        match self {
            JsonFamily::Json => crate::json_record::Flavour::Json,
            JsonFamily::Jsonb => crate::json_record::Flavour::Jsonb,
        }
    }
}

/// One of the eight record-mapping functions, as three independent choices.
///
/// `PostgreSQL` declares them as two families of four, and the four differ only
/// in where the target row type comes from and how many rows the document
/// yields — so they are one implementation with three flags rather than eight
/// entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordCall {
    family: JsonFamily,
    /// `*_populate_record`/`*_populate_recordset`, whose first argument is both
    /// the row type and the source of every field the document omits. The
    /// `*_to_record`/`*_to_recordset` half takes neither: its row type comes
    /// from the FROM item's column-definition list and its omitted fields are
    /// NULL.
    populate: bool,
    /// `*_recordset`/`*_to_recordset`: the document is an array of objects and
    /// each element is a row.
    set: bool,
}

impl RecordCall {
    /// The SQL name, which several of this family's run-time errors quote.
    fn name(self) -> &'static str {
        match (self.family, self.populate, self.set) {
            (JsonFamily::Json, true, false) => "json_populate_record",
            (JsonFamily::Json, true, true) => "json_populate_recordset",
            (JsonFamily::Json, false, false) => "json_to_record",
            (JsonFamily::Json, false, true) => "json_to_recordset",
            (JsonFamily::Jsonb, true, false) => "jsonb_populate_record",
            (JsonFamily::Jsonb, true, true) => "jsonb_populate_recordset",
            (JsonFamily::Jsonb, false, false) => "jsonb_to_record",
            (JsonFamily::Jsonb, false, true) => "jsonb_to_recordset",
        }
    }

    /// Which argument holds the document.
    fn document_at(self) -> usize {
        usize::from(self.populate)
    }

    /// The arities `PostgreSQL` declares, inclusive.
    ///
    /// The `json_populate_*` half carries a third parameter the `jsonb_` half
    /// does not: `use_json_as_text boolean DEFAULT false`, kept since 9.4 for
    /// callers written against the old signature. It has had no effect for a
    /// decade — a sub-document reaches a `text` column as its own text either
    /// way — but it is part of the signature, so a three-argument call resolves
    /// and a boolean is coerced.
    fn arity(self) -> (usize, usize) {
        match (self.populate, self.family) {
            (true, JsonFamily::Json) => (2, 3),
            (true, JsonFamily::Jsonb) => (2, 2),
            (false, _) => (1, 1),
        }
    }

    /// The `*_recordset` sibling of this call.
    const fn into_set(self) -> Self {
        RecordCall { set: true, ..self }
    }
}

const RECORD_JSON_POPULATE: RecordCall = RecordCall {
    family: JsonFamily::Json,
    populate: true,
    set: false,
};
const RECORD_JSONB_POPULATE: RecordCall = RecordCall {
    family: JsonFamily::Jsonb,
    populate: true,
    set: false,
};
const RECORD_JSON_TO: RecordCall = RecordCall {
    family: JsonFamily::Json,
    populate: false,
    set: false,
};
const RECORD_JSONB_TO: RecordCall = RecordCall {
    family: JsonFamily::Jsonb,
    populate: false,
    set: false,
};

/// What a call *is*, as opposed to how it was written: the name with its
/// `pg_catalog` qualifier and its letter case folded away.
///
/// `PostgreSQL` resolves `pg_catalog.generate_series(1, 3)` to the same function
/// the bare spelling names, and calls the output column — and the FROM item's
/// qualifier — `generate_series` either way; the schema survives only in the
/// `42883` a name that resolves to nothing raises. psql's `\d` writes every
/// call this way, so the qualified spelling is the common one, not the exotic
/// one.
///
/// Unquoted identifiers reach here lowercased, but a quoted `"UNNEST"` does not,
/// and PostgreSQL matches those case-sensitively — the folding here is
/// deliberate leniency, matching the pre-existing `unnest` handling this
/// registry replaces.
fn bare_name(name: &str) -> Cow<'_, str> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    if name.bytes().any(|b| b.is_ascii_uppercase()) {
        Cow::Owned(name.to_ascii_lowercase())
    } else {
        Cow::Borrowed(name)
    }
}

/// Classify a function name.
fn classify(name: &str) -> Option<Srf> {
    Some(match bare_name(name).as_ref() {
        "unnest" => Srf::Unnest,
        "generate_series" => Srf::GenerateSeries,
        "generate_subscripts" => Srf::GenerateSubscripts,
        "string_to_table" => Srf::StringToTable,
        "regexp_split_to_table" => Srf::RegexpSplitToTable,
        "regexp_matches" => Srf::RegexpMatches,
        // Ten functions, five shapes over two document types. The `json_*` half
        // reads the original text, so it keeps input order and duplicate keys
        // where the `jsonb_*` half has already discarded both.
        "json_each" => Srf::Each(JsonFamily::Json),
        "jsonb_each" => Srf::Each(JsonFamily::Jsonb),
        "json_each_text" => Srf::EachText(JsonFamily::Json),
        "jsonb_each_text" => Srf::EachText(JsonFamily::Jsonb),
        "json_object_keys" => Srf::ObjectKeys(JsonFamily::Json),
        "jsonb_object_keys" => Srf::ObjectKeys(JsonFamily::Jsonb),
        "json_array_elements" => Srf::ArrayElements(JsonFamily::Json),
        "jsonb_array_elements" => Srf::ArrayElements(JsonFamily::Jsonb),
        "json_array_elements_text" => Srf::ArrayElementsText(JsonFamily::Json),
        "jsonb_array_elements_text" => Srf::ArrayElementsText(JsonFamily::Jsonb),
        "jsonb_path_query" | "jsonb_path_query_tz" => Srf::JsonbPathQuery,
        // Eight names, one implementation: see `RecordCall`.
        "json_populate_record" => Srf::Record(RECORD_JSON_POPULATE),
        "jsonb_populate_record" => Srf::Record(RECORD_JSONB_POPULATE),
        "json_populate_recordset" => Srf::Record(RECORD_JSON_POPULATE.into_set()),
        "jsonb_populate_recordset" => Srf::Record(RECORD_JSONB_POPULATE.into_set()),
        "json_to_record" => Srf::Record(RECORD_JSON_TO),
        "jsonb_to_record" => Srf::Record(RECORD_JSONB_TO),
        "json_to_recordset" => Srf::Record(RECORD_JSON_TO.into_set()),
        "jsonb_to_recordset" => Srf::Record(RECORD_JSONB_TO.into_set()),
        "pg_input_error_info" => Srf::PgInputErrorInfo,
        "pg_snapshot_xip" => Srf::SnapshotXip(SnapshotFamily::Modern),
        "txid_snapshot_xip" => Srf::SnapshotXip(SnapshotFamily::Legacy),
        "pg_partition_ancestors" => Srf::PgPartitionAncestors,
        "pg_event_trigger_ddl_commands" => Srf::EventDdlCommands,
        "pg_event_trigger_dropped_objects" => Srf::EventDroppedObjects,
        _ => return None,
    })
}

/// Is `name` one of the functions this registry expands in a FROM item?
///
/// Not every one of them is *set*-returning: `json_populate_record` returns one
/// composite, and `json_to_record` one `record`. They live here because a FROM
/// item expands a composite result into columns exactly as it expands a set into
/// rows, and because the two halves of each pair share everything but their row
/// count. [`is_set_returning`] is the narrower predicate the select-list rewrite
/// needs.
pub(crate) fn is_srf(name: &str) -> bool {
    classify(name).is_some()
}

/// Does `name` return a *set*? Only these multiply a select list's rows, so only
/// these take the ProjectSet path — and only these are refused inside an
/// aggregate.
pub(crate) fn is_set_returning(name: &str) -> bool {
    classify(name).is_some_and(Srf::returns_set)
}

impl Srf {
    /// Does a call produce zero or more rows, rather than exactly one?
    fn returns_set(self) -> bool {
        match self {
            Srf::Record(call) => call.set,
            _ => true,
        }
    }

    /// Does `PostgreSQL` declare this function's output columns as OUT
    /// parameters? That changes which 42601 a column-definition list earns:
    /// `json_each` is "redundant for a function with OUT parameters" where
    /// `generate_series` is "only allowed for functions returning \"record\"".
    fn has_out_parameters(self) -> bool {
        matches!(
            self,
            Srf::Each(_)
                | Srf::EachText(_)
                | Srf::PgInputErrorInfo
                | Srf::EventDdlCommands
                | Srf::EventDroppedObjects
        )
    }
}

/// One resolved SRF call: which function, and the columns it produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SrfPlan {
    kind: Srf,
    /// The call's [`bare_name`] — which alias was written, folded to lowercase
    /// and stripped of a `pg_catalog` qualifier. Output names and run-time
    /// diagnostics come from this; a *plan-time* `42883` quotes the call as
    /// written instead, because there is no function to have a real name.
    name: String,
    columns: Vec<ColumnBinding>,
    /// For the record family, the composite the call populates and the type a
    /// *select-list* occurrence of the call yields.
    ///
    /// `columns` always holds the FROM-position shape — a composite result
    /// expands into one column per attribute there — but the same call in a
    /// select list is a single value of the composite type, so the two shapes
    /// are kept side by side rather than one being derived from the other.
    record: Option<RecordResult>,
}

/// The composite a record-family call produces.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordResult {
    shape: RecordShapeSource,
    /// The composite type itself, for the select-list column. `None` for the
    /// anonymous `record` a column-definition list gave a shape but not a name.
    named: Option<UserTypeRef>,
    /// Was the shape supplied by a column-definition list? A run-time record
    /// argument must then agree with it, which is `PostgreSQL`'s
    /// "function return row and query-specified return row do not match".
    from_column_defs: bool,
}

/// What tells a record-family call the shape of the rows it produces.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RecordShapeSource {
    /// Known at plan time: the named composite the first argument is declared
    /// as, or the FROM item's column-definition list.
    Fixed(RecordShape),
    /// Known only at run time, from the first argument's *value*.
    ///
    /// `SELECT json_populate_recordset(ROW(1, 2), '…')` has no FROM item to hang
    /// a column-definition list on and no named composite to read, yet
    /// PostgreSQL answers it — because a `ROW(…)` carries a row type the type
    /// layer here cannot see but the value can. `NULL::record` reaches the same
    /// plan and is the 0A000, so the two are only told apart by the value.
    Argument,
}

/// Where a call was written. Which of the two it is decides what may supply a
/// `record` result's row type, and so which refusal an unresolvable one earns.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CallSite<'a> {
    /// A FROM item, which may carry a column-definition list.
    FromItem(Option<&'a [TableFuncColumnDef]>),
    /// A select-list call, where only a run-time record argument can supply one.
    Projection,
}

impl<'a> CallSite<'a> {
    fn column_defs(self) -> Option<&'a [TableFuncColumnDef]> {
        match self {
            CallSite::FromItem(defs) => defs,
            CallSite::Projection => None,
        }
    }
}

/// Resolve `name(args)` to the columns it produces. Every arity/type rule a call
/// can fail is checked here, at plan time, so `Describe` reports the same error
/// `Execute` would.
pub(crate) fn plan(
    name: &str,
    args: &[Expr],
    site: CallSite<'_>,
    scope: &Scope,
) -> Result<SrfPlan, ExecError> {
    // Resolve the arguments' types first, so a name no entry claims still reports
    // the argument types PostgreSQL's 42883 names.
    let given = crate::eval::static_arg_types(args, scope)?;
    let kind = classify(name).ok_or_else(|| undefined_function(name, &given))?;
    // Diagnostics quote the call as written, so `name` stays whole for
    // `undefined_function` above; everything the *result* is named after uses
    // the folded spelling.
    let bare = bare_name(name);
    if let Srf::Record(call) = kind {
        return plan_record(call, &bare, &given, site);
    }
    if site.column_defs().is_some() {
        return Err(ExecError::Syntax(
            if kind.has_out_parameters() {
                "a column definition list is redundant for a function with OUT parameters"
            } else {
                "a column definition list is only allowed for functions returning \"record\""
            }
            .into(),
        ));
    }
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
        Srf::RegexpMatches => {
            require_arity(name, &given, (2, 3))?;
            vec![column("regexp_matches", ColumnType::Array(ElemType::Text))]
        }
        Srf::Each(family) => {
            require_json_document(name, &given, family)?;
            vec![
                column("key", ColumnType::Text),
                column("value", family.column_type()),
            ]
        }
        Srf::EachText(family) => {
            require_json_document(name, &given, family)?;
            vec![
                column("key", ColumnType::Text),
                column("value", ColumnType::Text),
            ]
        }
        Srf::ObjectKeys(family) => {
            require_json_document(name, &given, family)?;
            // A single-column SRF names its column after the function, so
            // `json_object_keys` must not report `jsonb_object_keys`.
            vec![column(&bare, ColumnType::Text)]
        }
        Srf::ArrayElements(family) => {
            require_json_document(name, &given, family)?;
            vec![column("value", family.column_type())]
        }
        Srf::ArrayElementsText(family) => {
            require_json_document(name, &given, family)?;
            vec![column("value", ColumnType::Text)]
        }
        Srf::JsonbPathQuery => {
            require_arity(name, &given, (2, 4))?;
            vec![column(&bare, ColumnType::Jsonb)]
        }
        // Handled above: the record family resolves its own shape, and the
        // column-definition-list rules differ for it.
        Srf::Record(_) => unreachable!("plan_record answered the record family"),
        Srf::SnapshotXip(family) => {
            require_arity(name, &given, (1, 1))?;
            // A single-column SRF names its column after the function, so
            // `txid_snapshot_xip` must not report `pg_snapshot_xip`.
            vec![column(&bare, family.xid_type())]
        }
        Srf::PgInputErrorInfo => {
            require_arity(name, &given, (2, 2))?;
            vec![
                column("message", ColumnType::Text),
                column("detail", ColumnType::Text),
                column("hint", ColumnType::Text),
                column("sql_error_code", ColumnType::Text),
            ]
        }
        Srf::PgPartitionAncestors => {
            require_arity(name, &given, (1, 1))?;
            vec![column("relid", ColumnType::Regclass)]
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
        name: bare.into_owned(),
        columns,
        record: None,
    })
}

/// Resolve one record-family call: where its row type comes from, and what a
/// column-definition list is allowed to say about it.
///
/// `PostgreSQL` splits this three ways and words each refusal differently, so
/// the three cases are spelled out rather than folded into one check:
///
/// * `json_populate_record(null::jpop, …)` returns the *named* composite `jpop`,
///   and a column-definition list on it is "redundant for a function returning a
///   named composite type";
/// * `json_to_record(…)`, and `json_populate_record(null::record, …)`, return
///   `record`, whose shape only a column-definition list can supply — without
///   one a FROM item is "a column definition list is required", and a select-list
///   call is the 0A000 "could not determine row type";
/// * an argument carrying no type at all (`json_populate_record('x', …)`) leaves
///   the `anyelement` parameter unresolved, which is 42804.
fn plan_record(
    call: RecordCall,
    bare: &str,
    given: &[ArgType],
    site: CallSite<'_>,
) -> Result<SrfPlan, ExecError> {
    let name = call.name();
    require_arity(name, given, call.arity())?;
    let document = given[call.document_at()];
    if let Some(ty) = document.known()
        && ty != call.family.column_type()
    {
        return Err(undefined_function(name, given));
    }

    let declared = if call.populate {
        match given[0] {
            ArgType::Known(ty @ ColumnType::Record(_)) => ty,
            // `anyelement` resolved to a non-composite: no such function.
            ArgType::Known(_) => return Err(undefined_function(name, given)),
            // A bare literal or NULL resolves the polymorphic parameter to
            // nothing at all, which PostgreSQL reports before it looks at the
            // document.
            ArgType::Unknown | ArgType::Opaque => {
                return Err(ExecError::TypeMismatch(
                    "could not determine polymorphic type because input has type unknown".into(),
                ));
            }
        }
    } else {
        ColumnType::Record(None)
    };

    let record = match (RecordShape::of(declared), site.column_defs()) {
        (Some(_), Some(_)) => {
            return Err(ExecError::Syntax(
                "a column definition list is redundant for a function returning a named \
                 composite type"
                    .into(),
            ));
        }
        (Some(shape), None) => {
            let ColumnType::Record(named) = declared else {
                unreachable!("RecordShape::of accepted a non-record type");
            };
            RecordResult {
                shape: RecordShapeSource::Fixed(shape),
                named,
                from_column_defs: false,
            }
        }
        (None, Some(defs)) => {
            // The list becomes a tuple descriptor, and PostgreSQL builds that
            // through the same attribute-name check a `CREATE TABLE` goes
            // through — so a repeated name is 42701, not two columns.
            if let Some(name) = first_duplicate(defs) {
                return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                    "42701",
                    format!("column name \"{name}\" specified more than once"),
                )));
            }
            RecordResult {
                shape: RecordShapeSource::Fixed(RecordShape {
                    fields: defs.iter().map(|d| (d.name.clone(), d.ty)).collect(),
                }),
                named: None,
                from_column_defs: true,
            }
        }
        (None, None) => match site {
            CallSite::FromItem(_) => {
                return Err(ExecError::Syntax(
                    "a column definition list is required for functions returning \"record\""
                        .into(),
                ));
            }
            CallSite::Projection => RecordResult {
                shape: RecordShapeSource::Argument,
                named: None,
                from_column_defs: false,
            },
        },
    };

    let columns = match &record.shape {
        RecordShapeSource::Fixed(shape) => shape
            .fields
            .iter()
            .map(|(name, ty)| column(name, *ty))
            .collect(),
        // Never a FROM item's shape — only the one column a select list sees.
        RecordShapeSource::Argument => vec![column(bare, ColumnType::Record(None))],
    };
    Ok(SrfPlan {
        kind: Srf::Record(call),
        name: bare.to_string(),
        columns,
        record: Some(record),
    })
}

/// Expand a planned call with the enclosing statement's blocking-memory limit.
pub(crate) fn rows_with_memory(
    plan: &SrfPlan,
    args: &[Expr],
    vals: &mut [Datum],
    ctx: &EvalCtx,
    statement_memory: &crate::scanner::StatementMemory,
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
        // Not strict in *either* argument, and deliberately so: the row type
        // usually arrives as `NULL::jpop`, and a NULL document yields one
        // all-NULL row from `populate_record` rather than no rows. `record_rows`
        // decides what a NULL document means for each half.
        Srf::Record(_) => 0,
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
        Srf::GenerateSeries => series_rows(plan, vals, ctx, statement_memory)?,
        Srf::GenerateSubscripts => subscript_rows(&plan.name, vals)?,
        Srf::StringToTable => string_to_table_rows(&plan.name, vals)?,
        Srf::RegexpSplitToTable => regexp_split_rows(&plan.name, vals)?,
        Srf::RegexpMatches => regexp_matches_rows(&plan.name, vals)?,
        Srf::Each(family) => expand_json(family, JsonbSrf::Each, vals)?,
        Srf::EachText(family) => expand_json(family, JsonbSrf::EachText, vals)?,
        Srf::ObjectKeys(family) => expand_json(family, JsonbSrf::ObjectKeys, vals)?,
        Srf::ArrayElements(family) => expand_json(family, JsonbSrf::ArrayElements, vals)?,
        Srf::ArrayElementsText(family) => expand_json(family, JsonbSrf::ArrayElementsText, vals)?,
        Srf::JsonbPathQuery => crate::json_fn::jsonb_path_query_rows(&plan.name, vals)?,
        Srf::Record(call) => record_rows(call, plan, vals, ctx)?,
        Srf::PgInputErrorInfo => input_error_info_rows(vals, ctx)?,
        Srf::SnapshotXip(family) => snapshot_xip_rows(family, &plan.name, &vals[0], ctx)?,
        Srf::PgPartitionAncestors => partition_ancestor_rows(&vals[0], ctx)?,
        Srf::EventDdlCommands => event_ddl_command_rows(ctx)?,
        Srf::EventDroppedObjects => event_dropped_object_rows(ctx)?,
    };
    ensure_expansion_fits(&produced, statement_memory)?;
    Ok(produced)
}

/// The type an `unknown` literal argument adopts, per position. This is the ONE
/// place each SRF's parameter types are written down. Both the plan-time
/// resolver and the run-time coercion drive it.
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
            vec![Some(value), Some(value), Some(step), Some(ColumnType::Text)]
        }
        Srf::GenerateSubscripts => vec![None, Some(ColumnType::Int4), Some(ColumnType::Bool)],
        Srf::StringToTable | Srf::RegexpSplitToTable | Srf::RegexpMatches => {
            vec![text, text, text]
        }
        Srf::Each(family)
        | Srf::EachText(family)
        | Srf::ObjectKeys(family)
        | Srf::ArrayElements(family)
        | Srf::ArrayElementsText(family) => vec![Some(family.column_type())],
        // `(jsonb, jsonpath [, jsonb vars [, boolean silent]])`.
        Srf::JsonbPathQuery => vec![
            Some(ColumnType::Jsonb),
            Some(ColumnType::JsonPath),
            Some(ColumnType::Jsonb),
            Some(ColumnType::Bool),
        ],
        // The row-type argument is `anyelement` — an `unknown` literal there is
        // already a 42804 — so only the document and the vestigial
        // `use_json_as_text` flag resolve a literal.
        Srf::Record(call) => {
            let mut params = vec![None; call.document_at()];
            params.push(Some(call.family.column_type()));
            params.push(Some(ColumnType::Bool));
            params
        }
        Srf::PgInputErrorInfo => vec![text, text],
        Srf::SnapshotXip(family) => vec![Some(family.snapshot_type())],
        // `regclass`, but resolving a *name* to a relation needs the catalog and
        // the search path, which the pure cast this drives has neither of. The
        // literal is left `unknown` so the row builder can run the catalog-aware
        // cast itself.
        Srf::PgPartitionAncestors => vec![None],
        Srf::EventDdlCommands | Srf::EventDroppedObjects => Vec::new(),
    }
}

/// Run one record-family call over its evaluated arguments.
fn record_rows(
    call: RecordCall,
    plan: &SrfPlan,
    vals: &[Datum],
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let result = plan
        .record
        .as_ref()
        .expect("plan_record attached a shape to every record-family plan");
    let base = match vals.first() {
        Some(Datum::Record(record)) if call.populate => Some(record),
        _ => None,
    };
    let shape = match &result.shape {
        RecordShapeSource::Fixed(shape) => Cow::Borrowed(shape),
        // The row type is the argument's, so there is no answer without one.
        RecordShapeSource::Argument => match base {
            Some(base) => Cow::Owned(RecordShape::of_value(base)),
            None => return Err(crate::json_record::indeterminate_row_type(&plan.name)),
        },
    };
    // A column-definition list does not *replace* a run-time record argument's
    // row type, it has to agree with it — PostgreSQL compares the two tuple
    // descriptors and refuses a mismatch rather than coercing.
    if let Some(base) = base
        && result.from_column_defs
    {
        check_row_type_matches(base, &shape)?;
    }
    let document = &vals[call.document_at()];
    if document.is_null() {
        // A NULL document leaves `populate_record` with the base row (all NULL
        // when there is none) and `populate_recordset` with no rows at all.
        return Ok(reshape(
            result,
            &shape,
            if call.set {
                Vec::new()
            } else {
                vec![crate::json_record::populate_missing(&shape, base, ctx)?]
            },
        ));
    }
    let node = crate::json_record::Node::of(document, call.family.flavour())?;
    let produced = if call.set {
        crate::json_record::populate_set(
            &shape,
            base,
            node,
            call.name(),
            call.family.flavour(),
            ctx,
        )?
    } else {
        vec![crate::json_record::populate(&shape, base, node, ctx)?]
    };
    Ok(reshape(result, &shape, produced))
}

/// Fold each row into a single composite value when the plan's one column *is*
/// the composite — the deferred select-list shape, whose columns nothing outside
/// the value knows.
fn reshape(
    result: &RecordResult,
    shape: &RecordShape,
    produced: Vec<Vec<Datum>>,
) -> Vec<Vec<Datum>> {
    if matches!(result.shape, RecordShapeSource::Fixed(_)) {
        return produced;
    }
    let names: std::sync::Arc<[String]> =
        shape.fields.iter().map(|(name, _)| name.clone()).collect();
    produced
        .into_iter()
        .map(|row| vec![Datum::Record(RecordValue::named(None, names.clone(), row))])
        .collect()
}

/// PostgreSQL's `tupledesc_match`: a record argument's own row type and the
/// column-definition list must agree on width and on every column's type.
fn check_row_type_matches(base: &RecordValue, shape: &RecordShape) -> Result<(), ExecError> {
    let mismatch = |detail: String| {
        ExecError::Remote(
            crabka_pgwire::error::PgError::error(
                "42804",
                "function return row and query-specified return row do not match",
            )
            .with_detail(detail),
        )
    };
    if base.values.len() != shape.fields.len() {
        return Err(mismatch(format!(
            "Returned row contains {} attribute{}, but query expects {}.",
            base.values.len(),
            if base.values.len() == 1 { "" } else { "s" },
            shape.fields.len()
        )));
    }
    for (index, (value, (_, wanted))) in base.values.iter().zip(&shape.fields).enumerate() {
        let Some(actual) = value.column_type() else {
            continue;
        };
        if actual != *wanted {
            return Err(mismatch(format!(
                "Returned type {} at ordinal position {}, but query expects {}.",
                actual.name(),
                index + 1,
                wanted.name()
            )));
        }
    }
    Ok(())
}

fn input_error_info_rows(vals: &[Datum], ctx: &EvalCtx) -> Result<Vec<Vec<Datum>>, ExecError> {
    let input = match &vals[0] {
        Datum::Text(value) => value.as_str(),
        other => return Err(type_error("pg_input_error_info", other)),
    };
    let type_name = match &vals[1] {
        Datum::Text(value) => value.as_str(),
        other => return Err(type_error("pg_input_error_info", other)),
    };
    let Some(error) = crate::func::input_error(input, type_name, ctx)? else {
        return Ok(vec![vec![Datum::Null; 4]]);
    };
    let detail = error
        .diagnostics
        .as_ref()
        .and_then(|fields| fields.detail.as_ref())
        .map_or(Datum::Null, |detail| Datum::Text(detail.clone()));
    Ok(vec![vec![
        Datum::Text(error.message),
        detail,
        Datum::Null,
        Datum::Text(error.code),
    ]])
}

/// `pg_snapshot_xip` / `txid_snapshot_xip`: one row per running transaction the
/// snapshot lists, in the ascending order the value already holds them in.
fn snapshot_xip_rows(
    family: SnapshotFamily,
    name: &str,
    value: &Datum,
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    // An `unknown` literal reaches here as text, because `param_types` names
    // the parameter and the coercion runs `pg_snapshot_in` — which reports the
    // same 22P02 a written cast would.
    let snapshot = match value {
        Datum::PgSnapshot(snapshot) => snapshot.as_ref().clone(),
        other => {
            match crabka_pgtypes::cast::cast_in(other, ColumnType::PgSnapshot, ctx.output_style())?
            {
                Datum::PgSnapshot(snapshot) => *snapshot,
                _ => return Err(undefined_function(name, &[])),
            }
        }
    };
    Ok(snapshot
        .xip()
        .iter()
        .map(|xid| vec![family.xid_datum(*xid)])
        .collect())
}

/// `pg_partition_ancestors(regclass)`: the relation itself, then its parent, its
/// grandparent, and so on to the root of the partition tree.
///
/// The gate at the top is `PostgreSQL`'s `check_rel_can_be_partition`: a
/// relation that is neither a partition nor a partitioned parent belongs to no
/// partition tree, so it produces **no** rows rather than naming itself. An
/// unresolvable oid produces none either, while a *name* no relation has is the
/// `42P01` the `regclass` cast itself raises — the same place `PostgreSQL`
/// raises it.
fn partition_ancestor_rows(value: &Datum, ctx: &EvalCtx) -> Result<Vec<Vec<Datum>>, ExecError> {
    // No catalog — a planning context or a unit test — means no partition tree
    // to walk.
    let Some(catalog) = ctx.catalog() else {
        return Ok(Vec::new());
    };
    let Some(Datum::Regclass(start)) =
        crate::catalog_fn::regclass_cast(catalog, ctx.resolution(), value)?
    else {
        return Ok(Vec::new());
    };
    // One catalog scan serves both directions: the oid the argument names has to
    // become a relation name to walk from, and every parent the walk reaches has
    // to become an oid to report.
    let mut oids = std::collections::HashMap::new();
    for table in crabka_pgcatalog::list_tables(catalog)? {
        oids.insert(
            table.name,
            crate::catalog_rel::table_relation_oid(table.id)?,
        );
    }
    let Some(mut current) = oids
        .iter()
        .find(|(_, oid)| **oid == start.oid)
        .map(|(name, _)| name.clone())
    else {
        return Ok(Vec::new());
    };
    if crate::partition::parent_of(catalog, &current)?.is_none()
        && !crate::partition::is_partitioned(catalog, &current)?
    {
        return Ok(Vec::new());
    }
    let mut produced = vec![vec![Datum::Regclass(start)]];
    // A partition tree is acyclic and every relation in it is a table, so the
    // table count bounds the walk; stopping there rather than looping keeps
    // corrupt partition metadata from hanging the query.
    while produced.len() <= oids.len()
        && let Some((parent, _)) = crate::partition::parent_of(catalog, &current)?
        && let Some(&oid) = oids.get(&parent)
    {
        produced.push(vec![Datum::Regclass(crate::exec::regclass_by_oid(
            catalog,
            ctx.resolution(),
            oid,
        )?)]);
        current = parent;
    }
    Ok(produced)
}

fn type_error(name: &str, value: &Datum) -> ExecError {
    ExecError::UndefinedFunction(format!(
        "function {name}({}) does not exist",
        value.column_type().unwrap_or(ColumnType::Text).name()
    ))
}

/// The single document argument all ten expansion functions take.
///
/// `PostgreSQL` declares the `json` and `jsonb` families separately and has no
/// implicit cast between the two types, so each family accepts only its own:
/// `json_each('{}'::jsonb)` and `jsonb_each('{}'::json)` are both 42883, as is
/// any other type. A bare literal is still `unknown` here and adopts the
/// family's type in [`param_types`].
fn require_json_document(
    name: &str,
    given: &[ArgType],
    family: JsonFamily,
) -> Result<(), ExecError> {
    require_arity(name, given, (1, 1))?;
    if given[0]
        .known()
        .is_some_and(|actual| actual != family.column_type())
    {
        return Err(undefined_function(name, given));
    }
    Ok(())
}

/// Expand one JSON document. The two families share all five shapes and differ
/// only in what they expand — `jsonb`'s decomposed value, or `json`'s original
/// text — so the family picks the row builder and the shape rides along.
fn expand_json(
    family: JsonFamily,
    shape: JsonbSrf,
    vals: &[Datum],
) -> Result<Vec<Vec<Datum>>, ExecError> {
    match family {
        JsonFamily::Json => crate::json_fn::json_srf_rows(shape, vals),
        JsonFamily::Jsonb => crate::json_fn::jsonb_srf_rows(shape, vals),
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
                Datum::Bool(object.is_temporary),
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

/// A FROM-position function item as a relation. Two examples are
/// `FROM generate_series(1, 3) AS g(n)` and
/// `FROM ROWS FROM (f(…), g(…)) WITH ORDINALITY`.
///
/// The arguments evaluate in the empty scope. The caller has already
/// substituted constants for a lateral item's outer references, so nothing here
/// needs an outer row.
/// Build a FROM-position SRF relation with the statement's blocking-memory
/// limit.
pub(crate) fn from_item_with_memory(
    functions: &[TableFuncCall],
    with_ordinality: bool,
    rows_from: bool,
    alias: Option<&str>,
    column_aliases: &Option<Vec<String>>,
    ctx: &EvalCtx,
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<Relation, ExecError> {
    if with_ordinality {
        reject_ordinality_with_column_defs(functions, rows_from)?;
    }
    if let Some(relation) = scalar_builtin_relation(
        functions,
        with_ordinality,
        alias,
        column_aliases,
        ctx,
    )? {
        return Ok(relation);
    }
    let plans = plan_all(functions)?;
    let mut produced = Vec::new();
    for (call, plan) in functions.iter().zip(&plans) {
        let mut vals = call
            .args
            .iter()
            .map(|arg| crate::eval::eval(arg, &Scope::empty(), &[], ctx))
            .collect::<Result<Vec<_>, _>>()?;
        produced.push(rows_with_memory(
            plan,
            &call.args,
            &mut vals,
            ctx,
            statement_memory,
        )?);
    }
    let rows = if produced.len() == 1 {
        // `rows_with_memory` has already charged this allocation.  A sole
        // function needs no lockstep zipping, so move its rows into the
        // relation instead of cloning and charging the same retained rows
        // again.
        produced.pop().expect("one produced function")
    } else {
        let rows = zip_in_lockstep(produced, &plans);
        ensure_expansion_fits(&rows, statement_memory)?;
        rows
    };
    qualify(&plans, rows, with_ordinality, alias, column_aliases)
}

/// Expand one built-in FunctionScan call without applying an item alias.
pub(crate) fn function_call_rows_with_memory(
    call: &TableFuncCall,
    ctx: &EvalCtx,
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<FunctionCallRows, ExecError> {
    let item = [call.clone()];
    let relation = from_item_with_memory(
        &item,
        false,
        false,
        None,
        &None,
        ctx,
        statement_memory,
    )?;
    Ok((
        relation
            .scope
            .columns
            .into_iter()
            .map(|column| (column.name, column.ty))
            .collect(),
        relation.rows,
    ))
}

/// The same item's schema, with no rows. This is the `Describe` path, and it
/// must agree with the runtime FROM-position path on every column name and type.
pub(crate) fn from_item_schema(
    functions: &[TableFuncCall],
    with_ordinality: bool,
    rows_from: bool,
    alias: Option<&str>,
    column_aliases: &Option<Vec<String>>,
) -> Result<Relation, ExecError> {
    if with_ordinality {
        reject_ordinality_with_column_defs(functions, rows_from)?;
    }
    if let Some(relation) =
        scalar_builtin_schema(functions, with_ordinality, alias, column_aliases)?
    {
        return Ok(relation);
    }
    let plans = plan_all(functions)?;
    qualify(&plans, Vec::new(), with_ordinality, alias, column_aliases)
}

/// Describe one built-in FunctionScan call without applying an item alias.
pub(crate) fn function_call_schema(
    call: &TableFuncCall,
) -> Result<Vec<(String, ColumnType)>, ExecError> {
    let item = [call.clone()];
    let relation = from_item_schema(&item, false, false, None, &None)?;
    Ok(relation
        .scope
        .columns
        .into_iter()
        .map(|column| (column.name, column.ty))
        .collect())
}

/// Build a `ROWS FROM` relation from calls that have already supplied columns
/// and rows. Each call is zipped in lockstep and shorter calls are null-padded.
pub(crate) fn rows_from_function_relation(
    function_name: &str,
    calls: Vec<FunctionCallRows>,
    with_ordinality: bool,
    alias: Option<&str>,
    column_aliases: &Option<Vec<String>>,
) -> Result<Relation, ExecError> {
    let height = calls.iter().map(|(_, rows)| rows.len()).max().unwrap_or(0);
    let rows = (0..height)
        .map(|index| {
            calls
                .iter()
                .flat_map(|(columns, rows)| {
                    rows.get(index)
                        .cloned()
                        .unwrap_or_else(|| vec![Datum::Null; columns.len()])
                })
                .collect()
        })
        .collect();
    let columns = calls
        .into_iter()
        .flat_map(|(columns, _)| columns)
        .collect();
    user_function_relation(
        function_name,
        columns,
        rows,
        with_ordinality,
        alias,
        column_aliases,
        None,
    )
}

/// A scalar built-in in `FROM` supplies its one result as a one-row relation.
///
/// PostgreSQL accepts `FROM abs(-3) AS t(value)`, even though `abs` is not an
/// SRF. Keep the value and its type on the same `eval`/`infer_type` path as a
/// scalar select-list expression; this only supplies the FunctionScan envelope.
fn scalar_builtin_relation(
    functions: &[TableFuncCall],
    with_ordinality: bool,
    alias: Option<&str>,
    column_aliases: &Option<Vec<String>>,
    ctx: &EvalCtx,
) -> Result<Option<Relation>, ExecError> {
    let [call] = functions else {
        return Ok(None);
    };
    let Some(call_expr) = scalar_builtin_call(call)? else {
        return Ok(None);
    };
    let scope = Scope::empty();
    let ty = crate::eval::infer_type(&Expr::Func(call_expr.clone()), &scope)?;
    let value = crate::eval::eval(&Expr::Func(call_expr), &scope, &[], ctx)?;
    Ok(Some(qualify_columns(
        call.name.clone(),
        vec![column(&call.name, ty)],
        vec![vec![value]],
        with_ordinality,
        alias,
        column_aliases,
        true,
    )?))
}

/// Describe the same scalar FunctionScan without evaluating its arguments.
fn scalar_builtin_schema(
    functions: &[TableFuncCall],
    with_ordinality: bool,
    alias: Option<&str>,
    column_aliases: &Option<Vec<String>>,
) -> Result<Option<Relation>, ExecError> {
    let [call] = functions else {
        return Ok(None);
    };
    let Some(call_expr) = scalar_builtin_call(call)? else {
        return Ok(None);
    };
    let scope = Scope::empty();
    let ty = crate::eval::infer_type(&Expr::Func(call_expr), &scope)?;
    Ok(Some(qualify_columns(
        call.name.clone(),
        vec![column(&call.name, ty)],
        Vec::new(),
        with_ordinality,
        alias,
        column_aliases,
        true,
    )?))
}

fn scalar_builtin_call(call: &TableFuncCall) -> Result<Option<FuncCall>, ExecError> {
    let call_expr = FuncCall {
        name: call.name.clone(),
        distinct: false,
        args: FuncArgs::Exprs(call.args.clone()),
        order_by: Vec::new(),
        within_group: false,
        filter: None,
        sql_syntax: false,
    };
    if !is_scalar_builtin(&call_expr) {
        return Ok(None);
    }
    if call.column_defs.is_some() {
        return Err(ExecError::Syntax(
            "a column definition list is only allowed for functions returning \"record\"".into(),
        ));
    }
    Ok(Some(call_expr))
}

/// The scalar-function families handled by [`crate::eval::eval`].
///
/// Set-returning built-ins stay on the SRF path, and user-defined functions
/// are handled by the routine table-function builder before reaching here.
fn is_scalar_builtin(call: &FuncCall) -> bool {
    crate::catalog_fn::is_catalog_func(&call.name)
        || crate::reg_fn::is_reg_func(&call.name)
        || crate::tid_fn::is_tid_func(&call.name)
        || crate::datetime_fn::is_datetime_constructor(call)
        || crate::func::is_scalar(&call.name)
        || crate::datetime_fn::is_datetime_func(&call.name)
        || crate::format_fn::is_format_func(&call.name)
        || crate::json_fn::is_json_func(&call.name)
        || crate::array_fn::is_array_func(&call.name)
}

/// The first column name a definition list repeats.
fn first_duplicate(defs: &[TableFuncColumnDef]) -> Option<&str> {
    let mut seen = std::collections::HashSet::new();
    defs.iter()
        .find(|def| !seen.insert(def.name.as_str()))
        .map(|def| def.name.as_str())
}

/// `WITH ORDINALITY` and a column-definition list cannot be written on the same
/// FROM item.
///
/// `PostgreSQL`'s grammar allows both, and then refuses the combination with a
/// hint pointing at the one spelling that does accept both — `ROWS FROM(f(…) AS
/// (…)) WITH ORDINALITY`, where the list belongs to the call rather than to the
/// item.
///
/// `rows_from` is what tells the two apart, and it has to be passed in: both
/// spellings parse to a single call carrying `column_defs`, so the calls alone
/// cannot distinguish the legal form from the illegal one. Without it this
/// refused the very shape its own hint recommends.
fn reject_ordinality_with_column_defs(
    functions: &[TableFuncCall],
    rows_from: bool,
) -> Result<(), ExecError> {
    if !rows_from && matches!(functions, [call] if call.column_defs.is_some()) {
        return Err(ExecError::Remote(
            crabka_pgwire::error::PgError::error(
                "42601",
                "WITH ORDINALITY cannot be used with a column definition list",
            )
            .with_hint("Put the column definition list inside ROWS FROM()."),
        ));
    }
    Ok(())
}

/// Plan every call in the item. Whether a column-definition list is allowed,
/// required or refused is the individual function's business — [`plan`] decides
/// it, because for the record family the answer depends on the arguments.
fn plan_all(functions: &[TableFuncCall]) -> Result<Vec<SrfPlan>, ExecError> {
    functions
        .iter()
        .map(|call| {
            plan(
                &call.name,
                &call.args,
                CallSite::FromItem(call.column_defs.as_deref()),
                &Scope::empty(),
            )
        })
        .collect()
}

/// Combine several calls' rows side by side. `ROWS FROM` runs its functions in
/// lockstep and pads the shorter ones with NULL until the longest is exhausted.
/// The multi-argument `unnest(a, b)` form follows the same rule.
fn zip_in_lockstep(produced: Vec<Vec<Vec<Datum>>>, plans: &[SrfPlan]) -> Vec<Vec<Datum>> {
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

/// The name that qualifies a function FROM item: its alias, or the first
/// function's own name.
fn qualifier_for(plans: &[SrfPlan], alias: Option<&str>) -> String {
    alias.map_or_else(
        || {
            plans
                .first()
                .map_or_else(String::new, |plan| plan.name.clone())
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
/// Without an explicit alias, the first function's name qualifies the item, as
/// in `generate_series.generate_series`. A bare `AS g` renames the column of an
/// item whose *functions* produce exactly one column. In PostgreSQL a function
/// in FROM that returns one scalar takes its column name from the table alias,
/// so `SELECT g FROM generate_series(1, 3) AS g` resolves. The ordinality column
/// keeps its own name either way. A column-alias list renames a prefix
/// positionally. A list that names more columns than the item has is
/// `PostgreSQL`'s 42P10.
///
/// That renaming is a property of a *scalar* result, not of column count. A
/// one-attribute composite is one column too, but its column is the attribute
/// and `AS q` names only the item — so `json_to_record(…) AS x(a int)` yields
/// `a`, not `x`.
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
        plans.iter().all(|plan| plan.record.is_none()),
    )
}

pub(crate) fn user_function_relation(
    function_name: &str,
    columns: Vec<(String, ColumnType)>,
    rows: Vec<Vec<Datum>>,
    with_ordinality: bool,
    alias: Option<&str>,
    column_aliases: &Option<Vec<String>>,
    column_defs: Option<&[TableFuncColumnDef]>,
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
        column_defs.is_none(),
    )
}

fn qualify_columns(
    qualifier: String,
    mut columns: Vec<ColumnBinding>,
    mut rows: Vec<Vec<Datum>>,
    with_ordinality: bool,
    alias: Option<&str>,
    column_aliases: &Option<Vec<String>>,
    // `alias_names_column`: may a bare alias rename the item's single column?
    // Only when that column is the function's scalar result.
    alias_names_column: bool,
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
        && alias_names_column
        && function_columns == 1
    {
        columns[0].name = alias.to_string();
    }
    for column in &mut columns {
        column.qualifier = Some(qualifier.clone());
    }
    Ok(Relation {
        scope: Scope {
            columns,
            ..Default::default()
        },
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
        && (is_set_returning(&fc.name) || crate::routine::is_plpgsql_set_runtime(fc))
    {
        return true;
    }
    children(expr).into_iter().any(expr_contains_srf)
}

/// 0A000 for an SRF in an aggregate query's select list. PostgreSQL evaluates
/// SRFs after aggregation, and crabka's aggregate path does not model that.
pub(crate) fn reject_in_aggregate(exprs: &[Expr]) -> Result<(), ExecError> {
    if exprs_contain_srf(exprs) {
        return Err(ExecError::Unsupported(
            "set-returning functions are not supported with aggregation or GROUP BY".into(),
        ));
    }
    Ok(())
}

/// An expression's immediate sub-expressions, for the SRF walks. The walks find
/// a set-returning call through these, so a variant that hides a sub-expression
/// here would hide an SRF from expansion. That is why the match is exhaustive.
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
            FuncArgs::Named { positional, named } => positional
                .iter()
                .chain(named.iter().map(|(_, arg)| arg))
                .collect(),
            FuncArgs::Variadic { positional, array } => positional
                .iter()
                .chain(std::iter::once(array.as_ref()))
                .collect(),
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
        | Expr::BitStringLiteral(_)
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

/// A select list rewritten for set expansion. Each SRF call becomes a synthetic
/// column reference. This value also carries the calls themselves and the scope
/// those references resolve against.
struct ProjectSet {
    exprs: Vec<Expr>,
    calls: Vec<SrfCall>,
    scope: Scope,
}

enum SrfCall {
    Builtin { plan: SrfPlan, args: Vec<Expr> },
    PlPgSql { call: FuncCall, ty: ColumnType },
}

impl SrfCall {
    fn projected_type(&self) -> ColumnType {
        match self {
            Self::Builtin { plan, .. } => projected_type(plan),
            Self::PlPgSql { ty, .. } => *ty,
        }
    }
}

/// Rewrite `out_exprs` so every SRF call becomes a reference to a synthetic
/// column, and extend `scope` with one binding per call. Both the type resolver
/// [`projection_type`] and the row expander drive this one rewrite, so a
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
            exposure: Exposure::Output,
            qualifier: Some(SRF_QUALIFIER.to_string()),
            name: index.to_string(),
            ty: call.projected_type(),
        });
    }
    Ok(ProjectSet {
        exprs,
        calls,
        scope: extended,
    })
}

fn rewrite_expr(expr: &mut Expr, scope: &Scope, calls: &mut Vec<SrfCall>) -> Result<(), ExecError> {
    if let Expr::Func(fc) = expr {
        let plpgsql_type = crate::routine::is_plpgsql_set_runtime(fc)
            .then(|| crate::routine::plpgsql_set_result_type(fc, scope))
            .flatten()
            .transpose()?;
        if !is_set_returning(&fc.name) && plpgsql_type.is_none() {
            for child in children_mut(expr) {
                rewrite_expr(child, scope, calls)?;
            }
            return Ok(());
        }
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
        // A select-list call has no FROM item to hang a column-definition list
        // on, so a record-returning one has no row type at all.
        let index = calls.len();
        if let Some(ty) = plpgsql_type {
            calls.push(SrfCall::PlPgSql {
                call: fc.clone(),
                ty,
            });
        } else {
            let plan = plan(&fc.name, args, CallSite::Projection, scope)?;
            if plan.record.is_none() && plan.columns.len() != 1 {
                return Err(ExecError::Unsupported(format!(
                    "set-returning function {} with multiple output columns is only supported in FROM",
                    plan.name
                )));
            }
            calls.push(SrfCall::Builtin {
                plan,
                args: args.clone(),
            });
        }
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
            FuncArgs::Named { positional, named } => positional
                .iter_mut()
                .chain(named.iter_mut().map(|(_, arg)| arg))
                .collect(),
            FuncArgs::Variadic { positional, array } => positional
                .iter_mut()
                .chain(std::iter::once(array.as_mut()))
                .collect(),
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
        | Expr::BitStringLiteral(_)
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

/// Statically infer a projected expression's type, and resolve any SRF call it
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
/// PostgreSQL plans it. `SELECT generate_series(1, 3) ORDER BY 1 DESC LIMIT 2`
/// sorts the three expanded rows and then takes two of them. An ORDER BY key
/// that is not a select-list output evaluates once per *source* row, and
/// replicates across that row's expansion. That is what PostgreSQL's resjunk
/// target does.
/// Run ProjectSet with the enclosing statement's blocking-memory limit.
pub(crate) fn project_rows_ordered_with_memory(
    s: &SelectStmt,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    kept: Vec<Vec<Datum>>,
    ctx: &EvalCtx,
    statement_memory: &crate::scanner::StatementMemory,
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
        for expanded in expand_row(&set, scope, row, ctx, statement_memory)? {
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
            statement_memory.charge_row(&keys)?;
            statement_memory.charge_row(&out)?;
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

/// The type a select-list occurrence of a planned call yields.
///
/// [`SrfPlan::columns`] is the FROM-position shape, which for the record family
/// is one column per attribute; a select list gets the composite whole.
fn projected_type(plan: &SrfPlan) -> ColumnType {
    match &plan.record {
        Some(record) => ColumnType::Record(record.named),
        None => plan.columns[0].ty,
    }
}

/// Reduce a call's FROM-position rows to the one column a select list sees.
///
/// A record-family call is *reassembled* here rather than truncated: its FROM
/// shape is the composite's attributes spread across columns, and the select
/// list wants them back inside one value.
fn collapse_projection(plan: &SrfPlan, produced: Vec<Vec<Datum>>) -> Vec<Datum> {
    let take_first = |produced: Vec<Vec<Datum>>| {
        produced
            .into_iter()
            .filter_map(|row| row.into_iter().next())
            .collect()
    };
    let Some(record) = &plan.record else {
        return take_first(produced);
    };
    let RecordShapeSource::Fixed(shape) = &record.shape else {
        // `rows` already folded these; the one column is the composite.
        return take_first(produced);
    };
    let names: std::sync::Arc<[String]> =
        shape.fields.iter().map(|(name, _)| name.clone()).collect();
    produced
        .into_iter()
        .map(|row| Datum::Record(RecordValue::named(record.named, names.clone(), row)))
        .collect()
}

/// Expand one source row into the output rows its select-list SRFs produce.
/// PostgreSQL 10+ runs the calls in lockstep. The row count is the longest
/// call's, and the shorter ones read as NULL past their end.
fn expand_row(
    set: &ProjectSet,
    scope: &Scope,
    row: &[Datum],
    ctx: &EvalCtx,
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut values: Vec<Vec<Datum>> = Vec::with_capacity(set.calls.len());
    for call in &set.calls {
        match call {
            SrfCall::Builtin { plan, args } => {
                let mut vals = args
                    .iter()
                    .map(|arg| crate::eval::eval(arg, scope, row, ctx))
                    .collect::<Result<Vec<_>, _>>()?;
                let produced = rows_with_memory(plan, args, &mut vals, ctx, statement_memory)?;
                values.push(collapse_projection(plan, produced));
            }
            SrfCall::PlPgSql { call, .. } => {
                let produced = crate::routine::eval_plpgsql_set_function(call, scope, row, ctx)
                    .ok_or_else(|| {
                    ExecError::ObjectNotInPrerequisiteState(
                        "PL/pgSQL set function runtime disappeared".into(),
                    )
                })??;
                statement_memory.charge_row(&produced)?;
                values.push(produced);
            }
        }
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
        statement_memory.charge_row(&cells)?;
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
            if let ColumnType::Multirange(multirange) = ty
                && given.len() == 1
            {
                return Ok(column("unnest", ColumnType::Range(multirange.range)));
            }
            let elem = ty
                .array_element()
                .ok_or_else(|| undefined_function(name, given))?;
            Ok(column("unnest", elem.column_type()))
        })
        .collect()
}

/// `unnest(a, b, …)`: PostgreSQL expands the multi-argument form as
/// `ROWS FROM (unnest(a), unnest(b), …)`: one column per argument, as many rows
/// as the longest array, shorter arrays padded with NULL. A NULL array behaves
/// exactly as an empty one.
fn unnest_rows(vals: &[Datum]) -> Vec<Vec<Datum>> {
    if let [Datum::Multirange(multirange)] = vals {
        return multirange
            .ranges
            .iter()
            .cloned()
            .map(|range| vec![Datum::Range(range)])
            .collect();
    }
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

/// The value type `generate_series` resolves to. This is the type of its one
/// output column, and also of its step. The temporal candidates are the
/// exception, because their step is an `interval`.
///
/// PostgreSQL's candidate set is `(int4, int4, int4)`, `(int8, …)`,
/// `(numeric, …)`, `(timestamp, timestamp, interval)` and
/// `(timestamptz, timestamptz, interval)`. `double precision` matches none of
/// them, which is 42883. A call whose bounds are all `unknown` literals matches
/// several equally well, which is 42725.
fn series_types(name: &str, given: &[ArgType]) -> Result<ColumnType, ExecError> {
    require_arity(name, given, (2, 4))?;
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
        if !(3..=4).contains(&given.len()) {
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
        if given.len() == 4
            && (value != ColumnType::Timestamptz
                || given[3].known().is_some_and(|t| t != ColumnType::Text))
        {
            return Err(undefined_function(name, given));
        }
        return Ok(value);
    }
    if given.len() > 3 {
        return Err(undefined_function(name, given));
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
/// drifts exactly as its iterative implementation does. A zero step is 22023. A
/// step whose sign points away from `stop` yields no rows.
fn series_rows(
    plan: &SrfPlan,
    vals: &[Datum],
    ctx: &EvalCtx,
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let value_ty = plan.columns[0].ty;
    let start = crabka_pgtypes::cast::cast(&vals[0], value_ty, &ctx.time_zone)?;
    let bound = crabka_pgtypes::cast::cast(&vals[1], value_ty, &ctx.time_zone)?;
    let step = match vals.get(2) {
        Some(step) => step.clone(),
        None => default_step(value_ty),
    };
    let series_time_zone = match vals.get(3) {
        Some(Datum::Text(name)) => crabka_pgtypes::datetime::resolve_time_zone(name)
            .ok_or_else(|| ExecError::UnknownTimeZone(name.clone()))?,
        Some(other) => {
            return Err(ExecError::TypeMismatch(format!(
                "generate_series fourth argument is of type {} but must be text",
                other.column_type().map_or("unknown", ColumnType::name)
            )));
        }
        None => ctx.time_zone.clone(),
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
    let mut budget = crate::scanner::MemoryBudget::new(statement_memory.limit());
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
        budget.charge_row(std::slice::from_ref(&current))?;
        out.push(vec![current.clone()]);
        current = series_advance(&current, &step, &series_time_zone)?;
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
fn series_advance(
    current: &Datum,
    step: &Datum,
    time_zone: &jiff::tz::TimeZone,
) -> Result<Datum, ExecError> {
    if let (Datum::Timestamptz(ts), Datum::Interval(iv)) = (current, step) {
        return Ok(Datum::Timestamptz(
            crabka_pgtypes::datetime::timestamptz_plus_interval(*ts, *iv, time_zone)?,
        ));
    }
    Ok(crabka_pgtypes::ops::add(current, step)?)
}

// ---- generate_subscripts ----

/// `generate_subscripts(array, dim [, reverse])`: crabka arrays are
/// one-dimensional and 1-based. So any `dim` other than 1 yields no rows, and so
/// does any empty or NULL array. PostgreSQL does the same for a dimension the
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

/// `string_to_table(text, delimiter [, null_string])`: the row-wise twin of
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
/// Zero-length matches follow PostgreSQL's documented rule. This function
/// ignores one at the start of the string, at its end, or directly after a
/// previous match. So `regexp_split_to_table('abc', 'x*')` is `a, b, c` rather
/// than a run of empty strings. An empty input yields ONE empty row, unlike
/// `string_to_table`.
fn regexp_split_rows(name: &str, vals: &[Datum]) -> Result<Vec<Vec<Datum>>, ExecError> {
    let input = text_arg(name, &vals[0])?;
    let pattern = text_arg(name, &vals[1])?;
    let flags = match vals.get(2) {
        None => "",
        Some(other) => text_arg(name, other)?,
    };
    let re = compile_pattern("regexp_split_to_table()", false, pattern, flags)?;
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

/// The next UTF-8 character boundary after `at`, or one past the end, so the
/// scan always terminates.
fn next_boundary(input: &str, at: usize) -> usize {
    input[at..]
        .chars()
        .next()
        .map_or(at + 1, |c| at + c.len_utf8())
}

/// `regexp_matches(string, pattern [, flags])`: the capture groups of each
/// match, one `text[]` row per match.
///
/// Without `g` only the first match produces a row. With `g` the scan walks
/// forward from the end of each match — and one character past it when the
/// match was empty, which is what makes `regexp_matches(…, '^', 'mg')` yield a
/// row per line instead of looping. A pattern with no capture groups reports
/// the whole match as the array's one element.
fn regexp_matches_rows(name: &str, vals: &[Datum]) -> Result<Vec<Vec<Datum>>, ExecError> {
    let input = text_arg(name, &vals[0])?;
    let pattern = text_arg(name, &vals[1])?;
    let flags = match vals.get(2) {
        None => "",
        Some(other) => text_arg(name, other)?,
    };
    let global = flags.contains('g');
    let re = compile_pattern("regexp_matches()", true, pattern, flags)?;
    let mut rows = Vec::new();
    let mut search = 0usize;
    while search <= input.len() {
        let Some(caps) = re.captures_at(input, search) else {
            break;
        };
        let whole = caps.get(0).expect("group 0 always participates");
        rows.push(vec![Datum::Array(ArrayValue::new(
            ElemType::Text,
            group_datums(&re, &caps),
        ))]);
        if !global {
            break;
        }
        search = if whole.is_empty() {
            next_boundary(input, whole.end())
        } else {
            whole.end()
        };
    }
    Ok(rows)
}

// ---- shared helpers ----

fn column(name: &str, ty: ColumnType) -> ColumnBinding {
    ColumnBinding {
        exposure: Exposure::Output,
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

/// PostgreSQL's 42883, which spells out the argument types it could not match.
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

fn ensure_expansion_fits(
    rows: &[Vec<Datum>],
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<(), ExecError> {
    for row in rows {
        statement_memory.charge_row(row)?;
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
    /// One snapshot with three running ids, declared as whichever of the two
    /// SQL types the caller is testing.
    fn snapshot_arg(ty: ColumnType) -> Expr {
        constant(
            Datum::PgSnapshot(Box::new(
                "12:20:13,15,18".parse().expect("valid pg_snapshot"),
            )),
            ty,
        )
    }

    /// A `json` argument. Unlike [`jsonb_arg`] the document is *not* rebuilt —
    /// `json_in` validates and keeps every byte — so the spacing, key order and
    /// duplicate keys written here are what the expansion has to hand back.
    fn json_arg(source: &str) -> Expr {
        crabka_pgtypes::json::validate(source).expect("valid json");
        constant(Datum::Json(source.to_string()), ColumnType::Json)
    }

    fn jsons(values: &[&str]) -> Vec<Datum> {
        values
            .iter()
            .map(|s| Datum::Json((*s).to_string()))
            .collect()
    }

    /// Plan then expand a call, the way both callers do.
    fn call(name: &str, args: &[Expr]) -> Result<Vec<Vec<Datum>>, ExecError> {
        let plan = plan(name, args, CallSite::FromItem(None), &Scope::empty())?;
        let mut vals = args
            .iter()
            .map(|a| crate::eval::eval(a, &Scope::empty(), &[], &ctx()))
            .collect::<Result<Vec<_>, _>>()?;
        let statement_memory =
            crate::scanner::StatementMemory::new(crate::scanner::BLOCKING_QUERY_MEMORY);
        rows_with_memory(&plan, args, &mut vals, &ctx(), &statement_memory)
    }

    #[test]
    fn srf_uses_the_callers_memory_limit() {
        let args = [int4(1), int4(2)];
        let plan = plan(
            "generate_series",
            &args,
            CallSite::FromItem(None),
            &Scope::empty(),
        )
        .expect("plan");
        let mut values = vec![Datum::Int4(1), Datum::Int4(2)];
        let statement_memory = crate::scanner::StatementMemory::new(crabka_units::bytes(1));
        let error = rows_with_memory(&plan, &args, &mut values, &ctx(), &statement_memory)
            .expect_err("series materialization must respect the supplied limit")
            .into_pg();

        assert!(error.code == "53200");
    }

    #[test]
    fn expansion_rows_charge_statement_memory() {
        let statement_memory = crate::scanner::StatementMemory::new(crabka_units::bytes(1));
        let error = ensure_expansion_fits(&[vec![Datum::Int4(1)]], &statement_memory)
            .expect_err("expansion rows must consume the statement budget")
            .into_pg();

        assert!(error.code == "53200");
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
            "regexp_matches",
            "jsonb_each",
            "jsonb_each_text",
            "jsonb_object_keys",
            "jsonb_array_elements",
            "jsonb_array_elements_text",
            "json_each",
            "json_each_text",
            "json_object_keys",
            "json_array_elements",
            "json_array_elements_text",
            "jsonb_path_query",
            "pg_input_error_info",
            "pg_snapshot_xip",
            "txid_snapshot_xip",
        ] {
            assert!(is_srf(name), "{name} should be a set-returning function");
            assert!(is_srf(&name.to_ascii_uppercase()), "{name} uppercased");
        }
        // `PostgreSQL` declares the jsonpath functions over `jsonb` alone, so
        // the `json_` spelling five of these have has no counterpart here.
        for name in [
            "jsonb_typeof",
            "json_path_query",
            "json_path_query_tz",
            "generate_seriess",
            "abs",
            "",
        ] {
            assert!(
                !is_srf(name),
                "{name} should not be a set-returning function"
            );
        }
    }

    #[test]
    fn input_error_info_returns_postgres_error_fields() {
        assert_eq!(
            call("pg_input_error_info", &[text("(1,4"), text("int4range")]).expect("error info"),
            vec![vec![
                Datum::Text("malformed range literal: \"(1,4\"".into()),
                Datum::Text("Unexpected end of input.".into()),
                Datum::Null,
                Datum::Text("22P02".into()),
            ]]
        );
        assert_eq!(
            call("pg_input_error_info", &[text("(1,4)"), text("int4range")]).expect("valid input"),
            vec![vec![Datum::Null; 4]]
        );
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
            // The `json` family is the same five shapes over the other document
            // type, and the two that hand back a sub-document hand back `json`.
            (
                "json_each",
                vec![json_arg(r#"{"a": 1}"#)],
                vec![("key", ColumnType::Text), ("value", ColumnType::Json)],
            ),
            (
                "json_each_text",
                vec![json_arg(r#"{"a": 1}"#)],
                vec![("key", ColumnType::Text), ("value", ColumnType::Text)],
            ),
            (
                "json_object_keys",
                vec![json_arg(r#"{"a": 1}"#)],
                vec![("json_object_keys", ColumnType::Text)],
            ),
            (
                "json_array_elements",
                vec![json_arg("[1]")],
                vec![("value", ColumnType::Json)],
            ),
            (
                "json_array_elements_text",
                vec![json_arg("[1]")],
                vec![("value", ColumnType::Text)],
            ),
            // The snapshot pair is the same expansion over two declared types,
            // and each names its column after itself rather than after the
            // other.
            (
                "pg_snapshot_xip",
                vec![snapshot_arg(ColumnType::PgSnapshot)],
                vec![("pg_snapshot_xip", ColumnType::Xid8)],
            ),
            (
                "txid_snapshot_xip",
                vec![snapshot_arg(ColumnType::TxidSnapshot)],
                vec![("txid_snapshot_xip", ColumnType::Int8)],
            ),
        ];

        for (name, args, expected) in cases {
            let expected: Vec<ColumnBinding> = expected
                .into_iter()
                .map(|(name, ty)| column(name, ty))
                .collect();
            let planned =
                plan(name, &args, CallSite::FromItem(None), &Scope::empty()).expect("plan");
            assert!(planned.columns == expected, "planning {name}");
            // The `Describe` path must agree with the executing one, column for
            // column, or a prepared statement's RowDescription would lie.
            let item = [TableFuncCall {
                name: name.into(),
                args: args.clone(),
                column_defs: None,
            }];
            let schema = from_item_schema(&item, false, false, None, &None).expect("schema");
            let statement_memory =
                crate::scanner::StatementMemory::new(crate::scanner::BLOCKING_QUERY_MEMORY);
            let executed =
                from_item_with_memory(&item, false, false, None, &None, &ctx(), &statement_memory)
                    .expect("rows");
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
        let date = |s: &str| {
            constant(
                Datum::Date(crabka_pgtypes::datetime::parse_date(s).expect("date")),
                ColumnType::Date,
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
                    interval.clone(),
                ],
                Ok(ColumnType::Timestamp),
            ),
            (
                vec![
                    constant(
                        Datum::Timestamptz(
                            crabka_pgtypes::datetime::parse_timestamptz(
                                "2024-03-10 05:00:00+00",
                                &jiff::tz::TimeZone::UTC,
                            )
                            .expect("timestamptz"),
                        ),
                        ColumnType::Timestamptz,
                    ),
                    constant(
                        Datum::Timestamptz(
                            crabka_pgtypes::datetime::parse_timestamptz(
                                "2024-03-12 04:00:00+00",
                                &jiff::tz::TimeZone::UTC,
                            )
                            .expect("timestamptz"),
                        ),
                        ColumnType::Timestamptz,
                    ),
                    interval.clone(),
                    text("America/New_York"),
                ],
                Ok(ColumnType::Timestamptz),
            ),
            (
                vec![
                    date("2024-03-10"),
                    date("2024-03-12"),
                    interval.clone(),
                    text("America/New_York"),
                ],
                Ok(ColumnType::Timestamptz),
            ),
            (
                vec![
                    ts("2024-03-10 00:00:00"),
                    ts("2024-03-12 00:00:00"),
                    interval.clone(),
                    text("America/New_York"),
                ],
                Err("42883"),
            ),
            (
                vec![
                    ts("2024-03-10 00:00:00"),
                    date("2024-03-12"),
                    interval.clone(),
                    text("America/New_York"),
                ],
                Err("42883"),
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
            let planned = plan(
                "generate_series",
                &args,
                CallSite::FromItem(None),
                &Scope::empty(),
            );
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
    fn generate_series_uses_its_fourth_argument_for_dst_steps() {
        let zoned = |s| {
            constant(
                Datum::Timestamptz(
                    crabka_pgtypes::datetime::parse_timestamptz(s, &jiff::tz::TimeZone::UTC)
                        .expect("timestamptz"),
                ),
                ColumnType::Timestamptz,
            )
        };
        let day = constant(
            Datum::Interval(crabka_pgtypes::datetime::Interval {
                months: 0,
                days: 1,
                micros: 0,
            }),
            ColumnType::Interval,
        );
        assert!(
            single_column(
                "generate_series",
                &[
                    zoned("2024-03-10 05:00:00+00"),
                    zoned("2024-03-12 04:00:00+00"),
                    day,
                    text("America/New_York"),
                ],
            )
            .expect("series")
                == [
                    "2024-03-10 05:00:00+00",
                    "2024-03-11 04:00:00+00",
                    "2024-03-12 04:00:00+00",
                ]
                .into_iter()
                .map(|s| Datum::Timestamptz(
                    crabka_pgtypes::datetime::parse_timestamptz(s, &jiff::tz::TimeZone::UTC)
                        .expect("expected timestamptz"),
                ))
                .collect::<Vec<_>>()
        );
        assert!(
            single_column(
                "generate_series",
                &[
                    zoned("2024-03-10 05:00:00+00"),
                    zoned("2024-03-11 05:00:00+00"),
                    constant(
                        Datum::Interval(crabka_pgtypes::datetime::Interval {
                            months: 0,
                            days: 1,
                            micros: 0,
                        }),
                        ColumnType::Interval,
                    ),
                    text("UTC"),
                ],
            )
            .expect("UTC series")
                == ["2024-03-10 05:00:00+00", "2024-03-11 05:00:00+00"]
                    .into_iter()
                    .map(|s| Datum::Timestamptz(
                        crabka_pgtypes::datetime::parse_timestamptz(s, &jiff::tz::TimeZone::UTC)
                            .expect("expected timestamptz"),
                    ))
                    .collect::<Vec<_>>()
        );
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

    /// One expected row of `regexp_matches`: a capture group per element, where
    /// `None` is a group that did not participate.
    type Groups<'a> = &'a [Option<&'a str>];

    /// `regexp_matches` reports one `text[]` per match, and the `g` flag is
    /// what decides whether it stops after the first one.
    #[test]
    fn regexp_matches_reports_the_capture_groups_of_each_match() {
        let arrays = |rows: &[Groups<'_>]| -> Vec<Datum> {
            rows.iter()
                .map(|groups| {
                    Datum::Array(ArrayValue::new(
                        ElemType::Text,
                        groups
                            .iter()
                            .map(|g| g.map_or(Datum::Null, |s| Datum::Text(s.to_string())))
                            .collect(),
                    ))
                })
                .collect()
        };
        let cases: Vec<(Vec<Expr>, Vec<Groups<'_>>)> = vec![
            // Two groups, one match.
            (
                vec![text("foobarbequebaz"), text("(bar)(beque)")],
                vec![&[Some("bar"), Some("beque")]],
            ),
            // No groups at all: the array holds the whole match.
            (
                vec![text("foobarbequebaz"), text("barbeque")],
                vec![&[Some("barbeque")]],
            ),
            // A group that did not participate is a NULL element, not an empty
            // string — which an *empty* match is.
            (
                vec![text("foobarbequebaz"), text("(bar)(.+)?(beque)")],
                vec![&[Some("bar"), None, Some("beque")]],
            ),
            (
                vec![text("foobarbequebaz"), text("(bar)(.*)(beque)")],
                vec![&[Some("bar"), Some(""), Some("beque")]],
            ),
            // No match is no rows, not one NULL row.
            (
                vec![text("foobarbequebaz"), text("(bar)(.+)(beque)")],
                vec![],
            ),
            // Without `g` the scan stops after the first match; with it every
            // non-overlapping match reports.
            (
                vec![text("foobarbequebazilbarfbonk"), text("(b[^b]+)(b[^b]+)")],
                vec![&[Some("bar"), Some("beque")]],
            ),
            (
                vec![
                    text("foobarbequebazilbarfbonk"),
                    text("(b[^b]+)(b[^b]+)"),
                    text("g"),
                ],
                vec![
                    &[Some("bar"), Some("beque")],
                    &[Some("bazil"), Some("barf")],
                ],
            ),
            (
                vec![text("foObARbEqUEbAz"), text("(bar)(beque)"), text("i")],
                vec![&[Some("bAR"), Some("bEqUE")]],
            ),
            // An empty match advances one character, so a line anchor under `m`
            // reports once per line instead of looping.
            (
                vec![text("foo\nbar\nbaz"), text("^"), text("mg")],
                vec![&[Some("")], &[Some("")], &[Some("")]],
            ),
            (
                vec![text("1\n2\n"), text("^.?"), text("mg")],
                vec![&[Some("1")], &[Some("2")], &[Some("")]],
            ),
        ];
        for (args, expected) in cases {
            assert!(
                single_column("regexp_matches", &args).expect("rows") == arrays(&expected),
                "{args:?}"
            );
        }
    }

    /// `regexp_matches` is the one function in the family that reads `g`, so it
    /// must not inherit `regexp_split_to_table`'s rejection of it.
    #[test]
    fn regexp_matches_accepts_the_global_flag_that_the_split_srf_rejects() {
        let args = [text("aa"), text("a"), text("g")];
        assert!(single_column("regexp_matches", &args).expect("rows").len() == 2);
        let error = single_column("regexp_split_to_table", &args)
            .expect_err("rejected")
            .into_pg();
        assert!(error.code == "22023", "{error:?}");
        assert!(
            error.message == "regexp_split_to_table() does not support the \"global\" option",
            "{error:?}"
        );
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

    /// One row of a single-column expansion per value.
    fn one_column(values: Vec<Datum>) -> Vec<Vec<Datum>> {
        values.into_iter().map(|value| vec![value]).collect()
    }

    /// The `json` family expands the ORIGINAL document text, so the three things
    /// `jsonb` has already thrown away by the time it is stored all survive:
    /// input order, duplicate keys, and each sub-document's own spacing.
    #[test]
    fn the_json_set_returning_functions_preserve_input_order_duplicates_and_spacing() {
        let object = json_arg(r#"{"b":1,   "a":"x",  "b":null, "o": { "b" : 1 }}"#);
        let array = json_arg(r#"[1,  { "b" : 1 } , "x\"y", null]"#);

        let cases: Vec<(&str, &Expr, Vec<Vec<Datum>>)> = vec![
            (
                "json_each",
                &object,
                vec![
                    vec![Datum::Text("b".into()), Datum::Json("1".into())],
                    vec![Datum::Text("a".into()), Datum::Json("\"x\"".into())],
                    // The duplicate `b` is kept, where it was written — the one
                    // row `jsonb_each` cannot produce.
                    vec![Datum::Text("b".into()), Datum::Json("null".into())],
                    vec![Datum::Text("o".into()), Datum::Json("{ \"b\" : 1 }".into())],
                ],
            ),
            (
                "json_each_text",
                &object,
                vec![
                    vec![Datum::Text("b".into()), Datum::Text("1".into())],
                    // `->>`'s rule: a JSON string is de-escaped, the JSON `null`
                    // literal becomes SQL NULL, anything else is its own text.
                    vec![Datum::Text("a".into()), Datum::Text("x".into())],
                    vec![Datum::Text("b".into()), Datum::Null],
                    vec![Datum::Text("o".into()), Datum::Text("{ \"b\" : 1 }".into())],
                ],
            ),
            (
                "json_object_keys",
                &object,
                one_column(texts(&["b", "a", "b", "o"])),
            ),
            (
                "json_array_elements",
                &array,
                one_column(jsons(&["1", "{ \"b\" : 1 }", r#""x\"y""#, "null"])),
            ),
            (
                "json_array_elements_text",
                &array,
                one_column(vec![
                    Datum::Text("1".into()),
                    Datum::Text("{ \"b\" : 1 }".into()),
                    Datum::Text("x\"y".into()),
                    Datum::Null,
                ]),
            ),
        ];
        for (name, argument, expected) in cases {
            assert!(
                call(name, std::slice::from_ref(argument)).expect("rows") == expected,
                "{name}"
            );
        }

        // An empty container expands to nothing, and — STRICT — so does NULL.
        for (name, empty) in [
            ("json_each", "{}"),
            ("json_each_text", "{}"),
            ("json_object_keys", "{}"),
            ("json_array_elements", "[]"),
            ("json_array_elements_text", "[]"),
        ] {
            assert!(
                call(name, &[json_arg(empty)]).expect("rows") == Vec::<Vec<Datum>>::new(),
                "{name}({empty})"
            );
            assert!(
                call(name, &[constant(Datum::Null, ColumnType::Json)]).expect("rows")
                    == Vec::<Vec<Datum>>::new(),
                "{name}(NULL)"
            );
        }
    }

    /// The `json` family raises its own messages for a wrongly shaped document,
    /// and they are NOT the `jsonb` ones: `json_each` on an array is "cannot
    /// deconstruct an array as an object" where `jsonb_each` says "cannot call
    /// jsonb_each on a non-object".
    #[test]
    fn a_wrongly_shaped_json_document_carries_the_json_familys_own_message() {
        let cases: Vec<(&str, &str, &str)> = vec![
            (
                "json_each",
                "[1]",
                "cannot deconstruct an array as an object",
            ),
            ("json_each", "1", "cannot deconstruct a scalar"),
            (
                "json_each_text",
                "[1]",
                "cannot deconstruct an array as an object",
            ),
            ("json_each_text", "1", "cannot deconstruct a scalar"),
            (
                "json_object_keys",
                "[1]",
                "cannot call json_object_keys on an array",
            ),
            (
                "json_object_keys",
                "1",
                "cannot call json_object_keys on a scalar",
            ),
            (
                "json_array_elements",
                r#"{"a": 1}"#,
                "cannot call json_array_elements on a non-array",
            ),
            (
                "json_array_elements",
                "1",
                "cannot call json_array_elements on a scalar",
            ),
            (
                "json_array_elements_text",
                r#"{"a": 1}"#,
                "cannot call json_array_elements_text on a non-array",
            ),
            (
                "json_array_elements_text",
                "1",
                "cannot call json_array_elements_text on a scalar",
            ),
        ];
        for (name, source, message) in cases {
            let error = call(name, &[json_arg(source)])
                .expect_err("wrong container")
                .into_pg();
            assert!(
                (error.code.as_str(), error.message.as_str()) == ("22023", message),
                "{name}({source}) gave {error:?}"
            );
        }
    }

    /// `PostgreSQL` declares the two families over `json` and `jsonb` with no
    /// implicit cast between them, so neither accepts the other's document — nor
    /// any other type. Each is the plain 42883 an unresolvable call raises.
    #[test]
    fn neither_json_family_accepts_the_others_document_type() {
        let cases: Vec<(&str, Expr, &str)> = vec![
            ("json_each", jsonb_arg("{}"), "json_each(jsonb)"),
            ("json_each_text", jsonb_arg("{}"), "json_each_text(jsonb)"),
            (
                "json_object_keys",
                jsonb_arg("{}"),
                "json_object_keys(jsonb)",
            ),
            (
                "json_array_elements",
                jsonb_arg("[]"),
                "json_array_elements(jsonb)",
            ),
            (
                "json_array_elements_text",
                jsonb_arg("[]"),
                "json_array_elements_text(jsonb)",
            ),
            ("jsonb_each", json_arg("{}"), "jsonb_each(json)"),
            ("jsonb_each_text", json_arg("{}"), "jsonb_each_text(json)"),
            (
                "jsonb_object_keys",
                json_arg("{}"),
                "jsonb_object_keys(json)",
            ),
            (
                "jsonb_array_elements",
                json_arg("[]"),
                "jsonb_array_elements(json)",
            ),
            (
                "jsonb_array_elements_text",
                json_arg("[]"),
                "jsonb_array_elements_text(json)",
            ),
            ("json_each", int4(1), "json_each(integer)"),
            ("json_each", text("x"), "json_each(text)"),
            ("jsonb_each", int4(1), "jsonb_each(integer)"),
            ("jsonb_each", text("x"), "jsonb_each(text)"),
        ];
        for (name, argument, signature) in cases {
            let error = call(name, std::slice::from_ref(&argument))
                .expect_err("wrong argument type")
                .into_pg();
            let expected = format!("function {signature} does not exist");
            assert!(
                (error.code.as_str(), error.message.as_str()) == ("42883", expected.as_str()),
                "{name} gave {error:?}"
            );
        }

        // A bare literal is `unknown` and adopts whichever type the family it was
        // passed to declares, so the same spelling reaches both.
        let literal = [Expr::StringLiteral(r#"{"a":1}"#.into())];
        assert!(
            call("json_each", &literal).expect("rows")
                == vec![vec![Datum::Text("a".into()), Datum::Json("1".into())]]
        );
        assert!(
            call("jsonb_each", &literal).expect("rows")
                == vec![vec![
                    Datum::Text("a".into()),
                    Datum::Jsonb(JsonbValue::Number(bigdecimal::BigDecimal::from(1)))
                ]]
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
    /// select list's calls and multiplies the output rows the same way.
    /// `PostgreSQL` then drops the junk columns.
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
    async fn a_plpgsql_set_function_expands_in_the_select_list() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        query(
            &mut s,
            "CREATE FUNCTION select_list_set() RETURNS SETOF int LANGUAGE plpgsql AS \
             $$ BEGIN RETURN NEXT 1; RETURN NEXT 2; END $$",
        )
        .await;
        let result = query(&mut s, "SELECT select_list_set()").await;
        assert!(shape(&result).0 == vec!["select_list_set"]);
        assert!(column_of(&result) == vec![Some("1".into()), Some("2".into())]);
    }

    #[tokio::test]
    async fn scalar_udf_can_share_a_select_list_with_a_builtin_srf() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        query(
            &mut session,
            "CREATE FUNCTION scalar_plus_one(n int) RETURNS int LANGUAGE sql AS 'SELECT n + 1'",
        )
        .await;

        let result = query(
            &mut session,
            "SELECT scalar_plus_one(1), generate_series(1, 2)",
        )
        .await;

        assert!(
            shape(&result).2
                == vec![
                    vec![Some("2".into()), Some("1".into())],
                    vec![Some("2".into()), Some("2".into())],
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
            // The `json` family's sub-document columns are `json` (114), not
            // `jsonb` (3802) — `json` is a type of its own, not an alias.
            (
                "SELECT * FROM json_each('{\"a\": 1}'::json)",
                vec!["key", "value"],
                vec![crabka_pgtypes::oids::TEXT, crabka_pgtypes::oids::JSON],
            ),
            (
                "SELECT * FROM json_each_text('{\"a\": 1}'::json)",
                vec!["key", "value"],
                vec![crabka_pgtypes::oids::TEXT, crabka_pgtypes::oids::TEXT],
            ),
            (
                "SELECT * FROM json_array_elements('[1]'::json)",
                vec!["value"],
                vec![crabka_pgtypes::oids::JSON],
            ),
            (
                "SELECT * FROM json_array_elements_text('[1]'::json)",
                vec!["value"],
                vec![crabka_pgtypes::oids::TEXT],
            ),
            (
                "SELECT * FROM json_object_keys('{\"a\": 1}'::json)",
                vec!["json_object_keys"],
                vec![crabka_pgtypes::oids::TEXT],
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

    /// The two families expand the SAME document differently, all the way out to
    /// the wire: `json` keeps what was written, `jsonb` reports what it stored.
    #[tokio::test]
    async fn the_two_json_families_disagree_about_the_same_document_end_to_end() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        let document = r#"{"b":1,   "a":2,  "b":3}"#;

        let r = query(
            &mut s,
            &format!("SELECT * FROM json_each('{document}'::json)"),
        )
        .await;
        assert!(shape(&r).0 == vec!["key", "value"]);
        assert!(shape(&r).1 == vec![crabka_pgtypes::oids::TEXT, crabka_pgtypes::oids::JSON]);
        assert!(
            shape(&r).2
                == vec![
                    vec![Some("b".into()), Some("1".into())],
                    vec![Some("a".into()), Some("2".into())],
                    vec![Some("b".into()), Some("3".into())],
                ]
        );

        // The same three fields through `jsonb`: sorted, and the duplicate `b`
        // already resolved to the last one written.
        let r = query(
            &mut s,
            &format!("SELECT * FROM jsonb_each('{document}'::jsonb)"),
        )
        .await;
        assert!(shape(&r).1 == vec![crabka_pgtypes::oids::TEXT, crabka_pgtypes::oids::JSONB]);
        assert!(
            shape(&r).2
                == vec![
                    vec![Some("a".into()), Some("2".into())],
                    vec![Some("b".into()), Some("3".into())],
                ]
        );

        // A single-column call names its column after the function it was
        // written as, and reports the duplicate key the same way.
        let r = query(
            &mut s,
            &format!("SELECT * FROM json_object_keys('{document}'::json)"),
        )
        .await;
        assert!(shape(&r).0 == vec!["json_object_keys"]);
        assert!(column_of(&r) == vec![Some("b".into()), Some("a".into()), Some("b".into())]);

        // Element text survives verbatim, spacing and escapes included.
        let r = query(
            &mut s,
            "SELECT * FROM json_array_elements('[1,  { \"b\" : 1 } ]'::json)",
        )
        .await;
        assert!(shape(&r).1 == vec![crabka_pgtypes::oids::JSON]);
        assert!(column_of(&r) == vec![Some("1".into()), Some("{ \"b\" : 1 }".into())]);
    }

    /// `PostgreSQL` declares the jsonpath functions over `jsonb` alone, so the
    /// `json_` spelling the other five have is a name that resolves to nothing.
    #[tokio::test]
    async fn json_path_query_is_not_a_function() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        for sql in [
            "SELECT * FROM json_path_query('{\"a\": 1}'::json, '$.a')",
            "SELECT * FROM json_path_query_tz('{\"a\": 1}'::json, '$.a')",
        ] {
            let error = s.simple_query(sql).await.expect_err("refused");
            assert!(error.code == "42883", "{sql} gave {error:?}");
        }
    }

    /// Build `proot ⊃ pmid ⊃ pleaf`, an ordinary table beside it, and an index
    /// on each — the tree every `pg_partition_ancestors` test below walks.
    async fn partition_tree(s: &mut crate::SqlSession) {
        query(
            s,
            "CREATE TABLE proot (a int, b int) PARTITION BY RANGE (a)",
        )
        .await;
        query(
            s,
            "CREATE TABLE pmid PARTITION OF proot FOR VALUES FROM (0) TO (100) \
             PARTITION BY RANGE (b)",
        )
        .await;
        query(
            s,
            "CREATE TABLE pleaf PARTITION OF pmid FOR VALUES FROM (0) TO (50)",
        )
        .await;
        query(s, "CREATE TABLE plain_t (a int)").await;
        query(s, "CREATE INDEX plain_idx ON plain_t (a)").await;
    }

    #[tokio::test]
    async fn pg_partition_ancestors_names_the_relation_then_every_parent_to_the_root() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        partition_tree(&mut s).await;

        // The relation comes *first*, then each parent in turn — psql orders by
        // the ordinality of this walk to find a trigger's topmost definer, so
        // the direction is load-bearing, not incidental.
        for (argument, expected) in [
            ("'pleaf'", vec!["pleaf", "pmid", "proot"]),
            ("'pmid'", vec!["pmid", "proot"]),
            // A partitioned parent is in a partition tree, so it names itself.
            ("'proot'", vec!["proot"]),
            // A relation in no partition tree at all produces NO rows — not
            // even itself. This is PostgreSQL's `check_rel_can_be_partition`.
            ("'plain_t'", vec![]),
            ("'plain_idx'", vec![]),
            // STRICT: a NULL argument yields nothing.
            ("NULL::regclass", vec![]),
            // An oid no relation carries resolves to nothing, which is not an
            // error — unlike a *name* no relation carries.
            ("999999::regclass", vec![]),
        ] {
            let sql = format!("SELECT relid FROM pg_partition_ancestors({argument})");
            let r = query(&mut s, &sql).await;
            assert!(shape(&r).0 == vec!["relid"], "{sql}");
            assert!(shape(&r).1 == vec![crabka_pgtypes::oids::REGCLASS], "{sql}");
            let names: Vec<String> = column_of(&r).into_iter().flatten().collect();
            assert!(names == expected, "{sql} gave {names:?}");
        }
    }

    #[tokio::test]
    async fn pg_partition_ancestors_reaches_the_same_walk_from_every_call_position() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        partition_tree(&mut s).await;
        let expected = vec![
            Some("pleaf".to_string()),
            Some("pmid".to_string()),
            Some("proot".to_string()),
        ];

        // The select list names the single column after the function, the FROM
        // item after the column, and `pg_catalog.` qualifies neither.
        for (sql, column_name) in [
            (
                "SELECT pg_partition_ancestors('pleaf')",
                "pg_partition_ancestors",
            ),
            (
                "SELECT pg_catalog.pg_partition_ancestors('pleaf')",
                "pg_partition_ancestors",
            ),
            ("SELECT relid FROM pg_partition_ancestors('pleaf')", "relid"),
            (
                "SELECT relid FROM pg_catalog.pg_partition_ancestors('pleaf')",
                "relid",
            ),
            (
                "SELECT * FROM pg_partition_ancestors('pleaf'::regclass)",
                "relid",
            ),
        ] {
            let r = query(&mut s, sql).await;
            assert!(shape(&r).0 == vec![column_name], "{sql}");
            assert!(column_of(&r) == expected, "{sql}");
        }

        // psql's `\d` walks a *correlated* call with ordinality and orders by
        // the depth it produces, so the ancestors have to arrive in walk order
        // through the lateral path too.
        let r = query(
            &mut s,
            "SELECT a.relid, a.depth \
               FROM pg_catalog.pg_class t, \
                    pg_catalog.pg_partition_ancestors(t.oid) WITH ORDINALITY AS a(relid, depth) \
              WHERE t.relname = 'pleaf' ORDER BY a.depth",
        )
        .await;
        assert!(shape(&r).0 == vec!["relid", "depth"]);
        assert!(
            shape(&r).2
                == vec![
                    vec![Some("pleaf".into()), Some("1".into())],
                    vec![Some("pmid".into()), Some("2".into())],
                    vec![Some("proot".into()), Some("3".into())],
                ]
        );
    }

    #[tokio::test]
    async fn pg_partition_ancestors_rejects_a_wrong_arity_and_an_unresolvable_name() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        partition_tree(&mut s).await;

        for (sql, code) in [
            ("SELECT * FROM pg_partition_ancestors()", "42883"),
            (
                "SELECT * FROM pg_partition_ancestors('pleaf', 'pmid')",
                "42883",
            ),
            // A name is resolved by the `regclass` cast, which raises 42P01 —
            // where PostgreSQL raises it too.
            (
                "SELECT * FROM pg_partition_ancestors('no_such_rel')",
                "42P01",
            ),
        ] {
            let error = s.simple_query(sql).await.expect_err("refused");
            assert!(error.code == code, "{sql} gave {error:?}");
        }
    }

    /// A `pg_catalog.` qualifier resolves to the same function everywhere and
    /// never reaches the output names, but it *does* survive into the `42883`
    /// for a name that resolves to nothing — PostgreSQL quotes the call as it
    /// was written there, because there is no function to have a real name.
    #[tokio::test]
    async fn a_pg_catalog_qualifier_resolves_an_srf_without_naming_its_columns() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();

        let r = query(&mut s, "SELECT * FROM pg_catalog.generate_series(1, 2)").await;
        assert!(shape(&r).0 == vec!["generate_series"]);
        assert!(column_of(&r) == vec![Some("1".into()), Some("2".into())]);

        // The item is qualified by the bare name, so the qualified reference
        // resolves.
        let r = query(
            &mut s,
            "SELECT generate_series.* FROM pg_catalog.generate_series(1, 2)",
        )
        .await;
        assert!(column_of(&r) == vec![Some("1".into()), Some("2".into())]);

        let r = query(
            &mut s,
            "SELECT * FROM pg_catalog.json_object_keys('{\"a\": 1}'::json)",
        )
        .await;
        assert!(shape(&r).0 == vec!["json_object_keys"]);

        let error = s
            .simple_query("SELECT * FROM pg_catalog.nosuch_srf(1)")
            .await
            .expect_err("refused");
        assert!(error.code == "42883", "{error:?}");
        assert!(error.message.contains("pg_catalog.nosuch_srf"), "{error:?}");
    }
}
