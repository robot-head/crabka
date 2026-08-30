//! P2: SQL routines. Definition, resolution, calling and catalog projection.
//!
//! A `LANGUAGE sql` routine is a query with holes. Gres builds no second
//! executor for routine bodies. Instead it *inlines* a call: it re-parses the
//! body from the catalog and replaces the body's parameter references with the
//! call's argument expressions. The resulting expression or query then runs
//! through the ordinary evaluation path and sees the same snapshot the caller
//! does. That is exactly how `PostgreSQL` treats a simple SQL function, and it
//! means a routine never carries a stale plan.
//!
//! Gres parses PL/pgSQL bodies at definition time and re-parses them for
//! execution, just like SQL bodies. Dynamic C/internal routines remain
//! catalog-only.

use std::{
    cell::{Cell, RefCell},
    fmt::Write as _,
    sync::Arc,
};

use crabka_pgcatalog::routine::{
    BodyForm, ParamMode, Routine, RoutineKind, RoutineParam, RoutineResult, RoutineType,
    drop_routine_ops, get_routine, list_routines, put_routine_ops, routines_named,
    signature_identity,
};
use crabka_pgkv::{Kv, WriteOp};
use crabka_pgparser::ast::{
    AlterRoutineAction, ArraySubscript, Assignment, AssignmentValue, CreateRoutineStmt, Expr,
    FuncArgs, FuncCall, InsertOverride, MergeAction, MergeMatchKind, MergeSource, PlPgSqlBlock,
    PlPgSqlStatement, QueryExpr, RelationRef, Returning, RoutineArg, RoutineArgMode, RoutineBody,
    RoutineObject, RoutineOption, RoutineParallel, RoutineReturn, RoutineSignature,
    RoutineVolatility, SelectItem, SelectStmt, Statement, TableFuncCall, TableFuncColumnDef,
    TargetIndirection,
};
use crabka_pgtypes::{ArrayValue, ColumnType, Datum};
use crabka_pgwire::engine::QueryResult;

use crate::{error::ExecError, eval::ArgType};

pub(crate) struct ScalarFunctionRequest {
    pub routine: Option<Routine>,
    pub values: Vec<Datum>,
    pub kind: FunctionRequestKind,
    pub command_row_claims: Option<crate::exec::CommandRowClaims>,
    pub reply: std::sync::mpsc::Sender<FunctionRequestReply>,
}

pub(crate) enum FunctionRequestKind {
    Scalar,
    Table(Vec<(String, ColumnType)>),
    Trigger(Box<crate::trigger::TriggerInvocation>),
    Statistics(crate::stats_fn::StatisticsRequest),
    TableXml(crate::xmlmap::TableXmlRequest),
    QueryXml(crate::xmlmap::QueryXmlRequest),
    CursorXml(crate::xmlmap::CursorXmlRequest),
    TableXmlSchema(crate::xmlmap::TableXmlSchemaRequest),
    QueryXmlSchema(crate::xmlmap::QueryXmlSchemaRequest),
    CursorXmlSchema(crate::xmlmap::CursorXmlSchemaRequest),
    SchemaXml(crate::xmlmap::SchemaXmlRequest),
}

pub(crate) enum FunctionRequestResult {
    Scalar(Datum),
    Table(Vec<Vec<Datum>>),
}

pub(crate) type FunctionRequestReply =
    Result<(FunctionRequestResult, Vec<crate::session::GucMutation>), ExecError>;

pub(crate) fn request_statistics(
    request: crate::stats_fn::StatisticsRequest,
) -> Result<Datum, ExecError> {
    let requests = scalar_runtime_request_sender().ok_or_else(|| {
        ExecError::Unsupported("statistics import functions require a SQL session".into())
    })?;
    let (reply, response) = std::sync::mpsc::channel();
    requests
        .try_send(ScalarFunctionRequest {
            routine: None,
            values: Vec::new(),
            kind: FunctionRequestKind::Statistics(request),
            command_row_claims: scalar_runtime_command_row_claims(),
            reply,
        })
        .map_err(|_| {
            ExecError::ObjectNotInPrerequisiteState(
                "statistics import function executor stopped".into(),
            )
        })?;
    let (result, mutations) = response.recv().map_err(|_| {
        ExecError::ObjectNotInPrerequisiteState(
            "statistics import function executor stopped".into(),
        )
    })??;
    crate::session::apply_guc_runtime_mutations(mutations)?;
    match result {
        FunctionRequestResult::Scalar(value) => Ok(value),
        FunctionRequestResult::Table(_) => Err(ExecError::ObjectNotInPrerequisiteState(
            "statistics import function executor returned rows".into(),
        )),
    }
}

pub(crate) fn request_table_xml(
    request: crate::xmlmap::TableXmlRequest,
) -> Result<Datum, ExecError> {
    let requests = scalar_runtime_request_sender()
        .ok_or_else(|| ExecError::Unsupported("table_to_xml requires a SQL session".into()))?;
    let (reply, response) = std::sync::mpsc::channel();
    requests
        .try_send(ScalarFunctionRequest {
            routine: None,
            values: Vec::new(),
            kind: FunctionRequestKind::TableXml(request),
            command_row_claims: scalar_runtime_command_row_claims(),
            reply,
        })
        .map_err(|_| {
            ExecError::ObjectNotInPrerequisiteState("table_to_xml executor stopped".into())
        })?;
    let (result, mutations) = response.recv().map_err(|_| {
        ExecError::ObjectNotInPrerequisiteState("table_to_xml executor stopped".into())
    })??;
    crate::session::apply_guc_runtime_mutations(mutations)?;
    match result {
        FunctionRequestResult::Scalar(value) => Ok(value),
        FunctionRequestResult::Table(_) => Err(ExecError::ObjectNotInPrerequisiteState(
            "table_to_xml executor returned rows".into(),
        )),
    }
}

pub(crate) fn request_query_xml(
    request: crate::xmlmap::QueryXmlRequest,
) -> Result<Datum, ExecError> {
    let requests = scalar_runtime_request_sender()
        .ok_or_else(|| ExecError::Unsupported("query_to_xml requires a SQL session".into()))?;
    let (reply, response) = std::sync::mpsc::channel();
    requests
        .try_send(ScalarFunctionRequest {
            routine: None,
            values: Vec::new(),
            kind: FunctionRequestKind::QueryXml(request),
            command_row_claims: scalar_runtime_command_row_claims(),
            reply,
        })
        .map_err(|_| {
            ExecError::ObjectNotInPrerequisiteState("query_to_xml executor stopped".into())
        })?;
    let (result, mutations) = response.recv().map_err(|_| {
        ExecError::ObjectNotInPrerequisiteState("query_to_xml executor stopped".into())
    })??;
    crate::session::apply_guc_runtime_mutations(mutations)?;
    match result {
        FunctionRequestResult::Scalar(value) => Ok(value),
        FunctionRequestResult::Table(_) => Err(ExecError::ObjectNotInPrerequisiteState(
            "query_to_xml executor returned rows".into(),
        )),
    }
}

pub(crate) fn request_cursor_xml(
    request: crate::xmlmap::CursorXmlRequest,
) -> Result<Datum, ExecError> {
    let requests = scalar_runtime_request_sender()
        .ok_or_else(|| ExecError::Unsupported("cursor_to_xml requires a SQL session".into()))?;
    let (reply, response) = std::sync::mpsc::channel();
    requests
        .try_send(ScalarFunctionRequest {
            routine: None,
            values: Vec::new(),
            kind: FunctionRequestKind::CursorXml(request),
            command_row_claims: scalar_runtime_command_row_claims(),
            reply,
        })
        .map_err(|_| {
            ExecError::ObjectNotInPrerequisiteState("cursor_to_xml executor stopped".into())
        })?;
    let (result, mutations) = response.recv().map_err(|_| {
        ExecError::ObjectNotInPrerequisiteState("cursor_to_xml executor stopped".into())
    })??;
    crate::session::apply_guc_runtime_mutations(mutations)?;
    match result {
        FunctionRequestResult::Scalar(value) => Ok(value),
        FunctionRequestResult::Table(_) => Err(ExecError::ObjectNotInPrerequisiteState(
            "cursor_to_xml executor returned rows".into(),
        )),
    }
}

pub(crate) fn request_table_xmlschema(
    request: crate::xmlmap::TableXmlSchemaRequest,
) -> Result<Datum, ExecError> {
    let requests = scalar_runtime_request_sender().ok_or_else(|| {
        ExecError::Unsupported("table XML schema functions require a SQL session".into())
    })?;
    let (reply, response) = std::sync::mpsc::channel();
    requests
        .try_send(ScalarFunctionRequest {
            routine: None,
            values: Vec::new(),
            kind: FunctionRequestKind::TableXmlSchema(request),
            command_row_claims: scalar_runtime_command_row_claims(),
            reply,
        })
        .map_err(|_| {
            ExecError::ObjectNotInPrerequisiteState("table XML schema executor stopped".into())
        })?;
    let (result, mutations) = response.recv().map_err(|_| {
        ExecError::ObjectNotInPrerequisiteState("table XML schema executor stopped".into())
    })??;
    crate::session::apply_guc_runtime_mutations(mutations)?;
    match result {
        FunctionRequestResult::Scalar(value) => Ok(value),
        FunctionRequestResult::Table(_) => Err(ExecError::ObjectNotInPrerequisiteState(
            "table XML schema executor returned rows".into(),
        )),
    }
}

pub(crate) fn request_query_xmlschema(
    request: crate::xmlmap::QueryXmlSchemaRequest,
) -> Result<Datum, ExecError> {
    request_xml_schema(
        FunctionRequestKind::QueryXmlSchema(request),
        "query XML schema",
    )
}

pub(crate) fn request_cursor_xmlschema(
    request: crate::xmlmap::CursorXmlSchemaRequest,
) -> Result<Datum, ExecError> {
    request_xml_schema(
        FunctionRequestKind::CursorXmlSchema(request),
        "cursor XML schema",
    )
}

pub(crate) fn request_schema_xml(
    request: crate::xmlmap::SchemaXmlRequest,
) -> Result<Datum, ExecError> {
    request_xml_schema(FunctionRequestKind::SchemaXml(request), "schema XML")
}

fn request_xml_schema(kind: FunctionRequestKind, operation: &str) -> Result<Datum, ExecError> {
    let requests = scalar_runtime_request_sender().ok_or_else(|| {
        ExecError::Unsupported(format!("{operation} functions require a SQL session"))
    })?;
    let (reply, response) = std::sync::mpsc::channel();
    requests
        .try_send(ScalarFunctionRequest {
            routine: None,
            values: Vec::new(),
            kind,
            command_row_claims: scalar_runtime_command_row_claims(),
            reply,
        })
        .map_err(|_| {
            ExecError::ObjectNotInPrerequisiteState(format!("{operation} executor stopped"))
        })?;
    let (result, mutations) = response.recv().map_err(|_| {
        ExecError::ObjectNotInPrerequisiteState(format!("{operation} executor stopped"))
    })??;
    crate::session::apply_guc_runtime_mutations(mutations)?;
    match result {
        FunctionRequestResult::Scalar(value) => Ok(value),
        FunctionRequestResult::Table(_) => Err(ExecError::ObjectNotInPrerequisiteState(format!(
            "{operation} executor returned rows"
        ))),
    }
}

type FunctionColumns = Vec<(String, ColumnType)>;
type PlPgSqlTableSchema = (Routine, FunctionColumns);
type PlPgSqlTableRows = (FunctionColumns, Vec<Vec<Datum>>);

#[derive(Clone)]
struct ScalarRuntime {
    catalog: Arc<dyn Kv>,
    requests: Option<tokio::sync::mpsc::Sender<ScalarFunctionRequest>>,
    command_row_claims: Option<crate::exec::CommandRowClaims>,
}

thread_local! {
    /// Catalog available to the synchronous scalar evaluator for the duration
    /// of one statement. This mirrors the existing GUC/advisory-lock runtimes:
    /// it is installed only around synchronous execution and never crosses an
    /// await point.
    static SCALAR_RUNTIME: RefCell<Option<ScalarRuntime>> = const { RefCell::new(None) };
    static PLPGSQL_CALL_DEPTH: Cell<usize> = const { Cell::new(0) };
}

const MAX_PLPGSQL_CALL_DEPTH: usize = 64;

pub(crate) fn with_scalar_runtime<T>(
    catalog: &Arc<dyn Kv>,
    requests: Option<tokio::sync::mpsc::Sender<ScalarFunctionRequest>>,
    f: impl FnOnce() -> T,
) -> T {
    with_scalar_runtime_with_command_row_claims(catalog, requests, None, f)
}

pub(crate) fn with_scalar_runtime_with_command_row_claims<T>(
    catalog: &Arc<dyn Kv>,
    requests: Option<tokio::sync::mpsc::Sender<ScalarFunctionRequest>>,
    command_row_claims: Option<crate::exec::CommandRowClaims>,
    f: impl FnOnce() -> T,
) -> T {
    SCALAR_RUNTIME.with(|cell| {
        let previous = cell.replace(Some(ScalarRuntime {
            catalog: Arc::clone(catalog),
            requests,
            command_row_claims,
        }));
        let result = f();
        cell.replace(previous);
        result
    })
}

/// The catalog the statement runtime exposes, when one is installed.
///
/// The aggregate resolver needs a catalog from inside expression walkers that
/// take no `kv` — the same problem `is_plpgsql_scalar_runtime` solves — and this
/// is the one seam that answers it.
pub(crate) fn scalar_runtime_catalog() -> Option<Arc<dyn Kv>> {
    SCALAR_RUNTIME.with(|runtime| {
        runtime
            .borrow()
            .as_ref()
            .map(|runtime| Arc::clone(&runtime.catalog))
    })
}

pub(crate) fn scalar_runtime_request_sender()
-> Option<tokio::sync::mpsc::Sender<ScalarFunctionRequest>> {
    SCALAR_RUNTIME.with(|runtime| {
        runtime
            .borrow()
            .as_ref()
            .and_then(|runtime| runtime.requests.clone())
    })
}

pub(crate) fn scalar_runtime_command_row_claims() -> Option<crate::exec::CommandRowClaims> {
    SCALAR_RUNTIME.with(|runtime| {
        runtime
            .borrow()
            .as_ref()
            .and_then(|runtime| runtime.command_row_claims.clone())
    })
}

struct PlPgSqlCallGuard;

impl Drop for PlPgSqlCallGuard {
    fn drop(&mut self) {
        PLPGSQL_CALL_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

fn enter_plpgsql_call() -> Result<PlPgSqlCallGuard, ExecError> {
    PLPGSQL_CALL_DEPTH.with(|depth| {
        if depth.get() >= MAX_PLPGSQL_CALL_DEPTH {
            return Err(ExecError::StackDepthExceeded);
        }
        depth.set(depth.get() + 1);
        Ok(PlPgSqlCallGuard)
    })
}

/// The languages `pg_language` lists. For `CREATE FUNCTION`, a routine in any
/// other language does not exist (`42704`).
const LANGUAGES: [&str; 4] = ["internal", "c", "sql", "plpgsql"];

/// `pg_proc.prolang` for each accepted language, matched to `pg_language.oid`.
fn language_oid(language: &str) -> i32 {
    match language {
        "internal" => 12,
        "c" => 13,
        "plpgsql" => 13_647,
        _ => 14,
    }
}

/// PostgreSQL 18 built-in type and pseudo-type names.
///
/// A routine signature may name a type Gres does not implement, and the
/// regression corpus is full of `smallint`, `anyelement` and `cstring`. A
/// refusal of the *definition* would diverge from `PostgreSQL`, which accepts
/// it. Gres records such a type by name, and a call that would have to produce
/// a value of it is `0A000`. A name that is neither on this list, a built-in
/// Gres resolves, nor a relation, is `42704`, exactly as `PostgreSQL` reports
/// it.
const KNOWN_TYPE_NAMES: &[&str] = &[
    "aclitem",
    "any",
    "anyarray",
    "anycompatible",
    "anycompatiblearray",
    "anycompatiblemultirange",
    "anycompatiblenonarray",
    "anycompatiblerange",
    "anyelement",
    "anyenum",
    "anymultirange",
    "anynonarray",
    "anyrange",
    "bigint",
    "bit",
    "bit varying",
    "bool",
    "boolean",
    "box",
    "bpchar",
    "bytea",
    "char",
    "character",
    "character varying",
    "cid",
    "cidr",
    "circle",
    "cstring",
    "date",
    "datemultirange",
    "daterange",
    "decimal",
    "double precision",
    "event_trigger",
    "fdw_handler",
    "float",
    "float4",
    "float8",
    "gtsvector",
    "index_am_handler",
    "inet",
    "int",
    "int2",
    "int2vector",
    "int4",
    "int4multirange",
    "int4range",
    "int8",
    "int8multirange",
    "int8range",
    "integer",
    "internal",
    "interval",
    "json",
    "jsonb",
    "jsonpath",
    "language_handler",
    "line",
    "lseg",
    "macaddr",
    "macaddr8",
    "money",
    "name",
    "numeric",
    "nummultirange",
    "numrange",
    "oid",
    "oidvector",
    "path",
    "pg_brin_bloom_summary",
    "pg_brin_minmax_multi_summary",
    "pg_ddl_command",
    "pg_dependencies",
    "pg_lsn",
    "pg_mcv_list",
    "pg_ndistinct",
    "pg_node_tree",
    "pg_snapshot",
    "point",
    "polygon",
    "real",
    "record",
    "refcursor",
    "regclass",
    "regcollation",
    "regconfig",
    "regdictionary",
    "regnamespace",
    "regoper",
    "regoperator",
    "regproc",
    "regprocedure",
    "regrole",
    "regtype",
    "smallint",
    "table_am_handler",
    "text",
    "tid",
    "time",
    "time with time zone",
    "time without time zone",
    "timestamp",
    "timestamp with time zone",
    "timestamp without time zone",
    "timestamptz",
    "timetz",
    "trigger",
    "tsm_handler",
    "tsmultirange",
    "tsquery",
    "tsrange",
    "tstzmultirange",
    "tstzrange",
    "tsvector",
    "txid_snapshot",
    "unknown",
    "uuid",
    "varbit",
    "varchar",
    "void",
    "xid",
    "xid8",
    "xml",
];

/// 42704 for a type a routine signature names that does not exist.
///
/// `PostgreSQL` quotes the name in the `RETURNS` position and leaves it bare in
/// a parameter position. Gres reproduces both spellings verbatim.
fn undefined_type(name: &str, quoted: bool) -> ExecError {
    let spelled = if quoted {
        format!("type \"{name}\" does not exist")
    } else {
        format!("type {name} does not exist")
    };
    ExecError::FunctionError {
        sqlstate: "42704",
        message: spelled,
    }
}

/// 42P13: a routine definition `PostgreSQL` rejects as invalid.
fn invalid_definition(message: impl Into<String>) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42P13",
        message: message.into(),
    }
}

/// 42809: the named routine exists but is not of the kind the statement asked
/// for.
pub(crate) fn wrong_routine_kind(message: impl Into<String>) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42809",
        message: message.into(),
    }
}

/// 42723: a routine with this name and input signature already exists.
fn duplicate_routine(name: &str) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42723",
        message: format!("function \"{name}\" already exists with same argument types"),
    }
}

/// 42883: no routine matches the identity a statement or call named.
pub(crate) fn undefined_routine(message: impl Into<String>) -> ExecError {
    ExecError::UndefinedFunction(message.into())
}

const STATIC_REGRESS_ENTRYPOINTS: &[&str] = &[
    "binary_coercible",
    "get_environ",
    "int44in",
    "int44out",
    "interpt_pp",
    "is_catalog_text_unique_index_oid",
    "make_tuple_indirect",
    "overpaid",
    "pt_in_widget",
    "regress_setenv",
    "reverse_name",
    "test_atomic_ops",
    "test_bytea_to_text",
    "test_canonicalize_path",
    "test_enc_conversion",
    "test_enc_setup",
    "test_fdw_handler",
    "test_mblen_func",
    "test_opclass_options_func",
    "test_pglz_compress",
    "test_pglz_decompress",
    "test_relpath",
    "test_support_func",
    "test_text_to_bytea",
    "test_text_to_wchars",
    "test_valid_server_encoding",
    "test_wchars_to_text",
    "trigger_return_old",
    "wait_pid",
    "widget_in",
    "widget_out",
];
/// The PostgreSQL regression fixture's `postgres_fdw` validator.
///
/// It is not part of the core `pg_proc` fixture, but FDW catalog rows retain
/// its OID and `regproc` must print its name.
pub(crate) const POSTGRESQL_FDW_VALIDATOR_OID: i32 = 215_410;

const MAX_PGLZ_OUTPUT_BYTES: usize = 64 * 1024 * 1024;

fn is_static_regress_object(object_file: &str) -> bool {
    object_file == "regress"
        || std::env::var_os("CRABKA_PG_REGRESS_LIBRARY")
            .is_some_and(|configured| configured == std::ffi::OsStr::new(object_file))
}

fn static_regress_symbol(symbol: &str) -> bool {
    symbol == "Pg_magic_func"
        || STATIC_REGRESS_ENTRYPOINTS.contains(&symbol)
        || symbol
            .strip_prefix("pg_finfo_")
            .is_some_and(|entrypoint| STATIC_REGRESS_ENTRYPOINTS.contains(&entrypoint))
}

fn inaccessible_c_object(object_file: &str) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "58P01",
        message: format!("could not access file \"{object_file}\": No such file or directory"),
    }
}

fn unsupported_c_object(object_file: &str) -> ExecError {
    ExecError::Unsupported(format!(
        "loading C object file \"{object_file}\" is supported only for PostgreSQL modules"
    ))
}

fn unavailable_c_object(object_file: &str) -> ExecError {
    let path = std::path::Path::new(object_file);
    let library_name = path
        .extension()
        .is_some_and(|extension| extension == "so" || extension == "dylib" || extension == "dll");
    if !path.is_absolute() || library_name {
        inaccessible_c_object(object_file)
    } else {
        unsupported_c_object(object_file)
    }
}

/// Resolve a `LOAD` target to one of the modules compiled into Gres.
///
/// The pinned regression module and PL/pgSQL runtime are static compatibility
/// modules. Unknown relative names behave like missing dynamic libraries.
/// Absolute paths are rejected without reading server-side files.
pub(crate) fn validate_load_target(object_file: &str) -> Result<(), ExecError> {
    if is_static_regress_object(object_file) || object_file == "plpgsql" {
        Ok(())
    } else {
        Err(unavailable_c_object(object_file))
    }
}

fn validate_c_routine(routine: &Routine) -> Result<(), ExecError> {
    let object_file = routine
        .object_file
        .as_deref()
        .ok_or_else(|| invalid_definition("C language function must specify an object file"))?;
    if !is_static_regress_object(object_file) {
        return Err(unavailable_c_object(object_file));
    }
    if !static_regress_symbol(&routine.body) {
        return Err(undefined_routine(format!(
            "could not find function \"{}\" in file \"{object_file}\"",
            routine.body
        )));
    }
    Ok(())
}

fn validate_internal_routine(routine: &Routine) -> Result<(), ExecError> {
    let found = builtin_pg_proc_rows()?.iter().any(|row| {
        row[4] == Datum::Int4(12)
            && matches!(&row[25], Datum::Text(source) if source == &routine.body)
    });
    if found {
        Ok(())
    } else {
        Err(undefined_routine(format!(
            "there is no built-in function named \"{}\"",
            routine.body
        )))
    }
}

/// 42725 — a routine name that more than one routine carries.
fn ambiguous_routine(name: &str) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42725",
        message: format!("function name \"{name}\" is not unique"),
    }
}

/// Resolve a type written in a routine signature against the catalog.
/// [`resolve_type`] for callers outside this module — the aggregate DDL, which
/// resolves the same signature vocabulary.
pub(crate) fn resolve_routine_type(
    kv: &dyn Kv,
    ty: &crabka_pgparser::ast::RoutineType,
    quoted: bool,
) -> Result<RoutineType, ExecError> {
    resolve_type(
        kv,
        crate::relname::ResolutionScope::default_scope(),
        ty,
        quoted,
    )
}

fn resolve_type(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    ty: &crabka_pgparser::ast::RoutineType,
    quoted: bool,
) -> Result<RoutineType, ExecError> {
    if let Some(column) = ty.resolved {
        return Ok(RoutineType::builtin(column));
    }
    let lowered = ty.name.to_ascii_lowercase();
    if KNOWN_TYPE_NAMES.contains(&lowered.as_str()) {
        return Ok(RoutineType::named(lowered));
    }
    // A relation name is that relation's composite type. It follows the same
    // session search path as the routine statement, including `pg_temp`.
    let base = lowered.strip_suffix("[]").unwrap_or(&lowered);
    let relation =
        crate::relname::parse_written_relation(resolution, base).and_then(|written| {
            crate::relname::resolve_relation(
                kv,
                resolution,
                &written.reference,
                crate::relname::SchemaDisposition::Reference,
            )
        })?;
    if crabka_pgcatalog::get_table(kv, &relation).is_ok()
        || crabka_pgcatalog::get_view(kv, &relation).is_ok()
        || crabka_pgcatalog::get_sequence(kv, &relation).is_ok()
    {
        if !lowered.ends_with("[]") {
            crate::catalog_rel::sync_relation_rowtypes(kv)?;
            if let Some(rowtype) = crate::catalog_rel::relation_rowtype(kv, &relation)? {
                return Ok(RoutineType {
                    column: Some(ColumnType::Record(Some(rowtype))),
                    name: lowered,
                });
            }
        }
        return Ok(RoutineType::named(lowered));
    }
    if !lowered.ends_with("[]") {
        if let Some(column) = crabka_pgtypes::usertype::lookup(&lowered)
            .and_then(|definition| definition.column_type())
        {
            return Ok(RoutineType {
                column: Some(column),
                name: lowered,
            });
        }
    }
    // A shell type resolves nowhere else — it has no `ColumnType`, so the
    // parser could not have resolved it — but a routine signature is precisely
    // what a shell exists to be named in. `CREATE TYPE xfloat4;` then
    // `xfloat4in(cstring) RETURNS xfloat4` is the two-phase definition every
    // base type needs, and refusing here would close it before it opened.
    if shell_type_named(kv, &lowered) {
        return Ok(RoutineType::named(lowered));
    }
    Err(undefined_type(&ty.name, quoted))
}

/// The `NOTICE`s `CREATE FUNCTION` emits when its signature names a shell type.
///
/// PostgreSQL reports the two ends differently, and the difference is not
/// cosmetic: an argument type carries a parse location, so the client draws a
/// `LINE`/caret under it, while the return type is resolved by
/// `compute_return_type`, which has none.
///
/// Computed from the statement before it runs, like every other precomputed
/// notice, because a shell that the statement itself completes would no longer
/// be one afterwards. A catalog read that fails reports no shell rather than
/// failing the statement: the notice is advisory, and the DDL itself will
/// raise whatever the same failed read causes there.
pub(crate) fn shell_type_notices(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    stmt: &crabka_pgparser::ast::Statement,
) -> Vec<crabka_pgwire::error::PgError> {
    use crabka_pgparser::ast::{RoutineReturn, Statement};
    let Statement::CreateRoutine(create) = stmt else {
        return Vec::new();
    };
    let mut notices = Vec::new();
    if let RoutineReturn::Type { ty, .. } = &create.returns {
        if return_shell_to_create(kv, resolution, create).is_ok_and(|shell| shell.is_some()) {
            notices.push(
                crabka_pgwire::error::PgError::notice(format!(
                    "type \"{}\" is not yet defined",
                    ty.name
                ))
                .with_detail("Creating a shell type definition."),
            );
        } else if shell_type_named(kv, &ty.name.to_ascii_lowercase()) {
            notices.push(crabka_pgwire::error::PgError::notice(format!(
                "return type {} is only a shell",
                ty.name
            )));
        }
    }
    for arg in &create.args {
        if !shell_type_named(kv, &arg.ty.name.to_ascii_lowercase()) {
            continue;
        }
        let notice = crabka_pgwire::error::PgError::notice(format!(
            "argument type {} is only a shell",
            arg.ty.name
        ));
        notices.push(match arg.ty.location {
            Some(location) => notice.with_position(location),
            None => notice,
        });
    }
    notices
}

/// Whether `name` is a registered shell type, matched the way a routine
/// signature resolves: unqualified names come from `public`.
pub(crate) fn shell_type_named(kv: &dyn Kv, name: &str) -> bool {
    let (schema, bare) = match name.split_once('.') {
        Some((schema, bare)) => (schema, bare),
        None => (crabka_pgtypes::usertype::USER_TYPE_DEFAULT_SCHEMA, name),
    };
    crabka_pgcatalog::list_user_types(kv).is_ok_and(|types| {
        types
            .iter()
            .any(|ty| ty.is_shell() && ty.schema == schema && ty.name == bare)
    })
}

/// Resolve the shell an unknown `RETURNS type` makes. PostgreSQL creates this
/// one shell before type resolution, so the function can be the type's input
/// routine. Parameter types never take this path.
fn return_shell_to_create(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    stmt: &CreateRoutineStmt,
) -> Result<Option<(crabka_pgcatalog::RelationName, String)>, ExecError> {
    let RoutineReturn::Type { ty, .. } = &stmt.returns else {
        return Ok(None);
    };
    if ty.resolved.is_some() || ty.name.ends_with("[]") {
        return Ok(None);
    }
    match resolve_type(kv, resolution, ty, true) {
        Ok(_) => return Ok(None),
        Err(ExecError::FunctionError {
            sqlstate: "42704", ..
        }) => {}
        Err(error) => return Err(error),
    }
    let written = crate::relname::parse_written_relation(resolution, &ty.name)?;
    let name = crate::relname::resolve_relation(
        kv,
        resolution,
        &written.reference,
        crate::relname::SchemaDisposition::Creation,
    )?;
    Ok(Some((name, ty.name.to_ascii_lowercase())))
}

fn resolve_return_type(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    ty: &crabka_pgparser::ast::RoutineType,
    return_shell: Option<&str>,
) -> Result<RoutineType, ExecError> {
    match resolve_type(kv, resolution, ty, true) {
        Ok(ty) => Ok(ty),
        Err(_) if return_shell == Some(ty.name.to_ascii_lowercase().as_str()) => {
            Ok(RoutineType::named(ty.name.to_ascii_lowercase()))
        }
        Err(error) => Err(error),
    }
}

/// The routine kind a `CREATE`/`DROP`/`ALTER` spelling selects.
fn object_kind(object: RoutineObject) -> Option<RoutineKind> {
    match object {
        RoutineObject::Function => Some(RoutineKind::Function),
        RoutineObject::Procedure => Some(RoutineKind::Procedure),
        RoutineObject::Routine => None,
    }
}

/// Fold the parsed option list into the routine record's fields.
struct Options {
    language: Option<String>,
    body: Option<RoutineBody>,
    volatility: Option<char>,
    parallel: Option<char>,
    window: bool,
    strict: Option<bool>,
    security_definer: Option<bool>,
    leakproof: Option<bool>,
    cost: Option<f64>,
    rows: Option<f64>,
    config: Vec<String>,
    config_source: Vec<String>,
}

impl Options {
    fn collect(options: &[RoutineOption]) -> Result<Self, ExecError> {
        let mut out = Self {
            language: None,
            body: None,
            volatility: None,
            parallel: None,
            window: false,
            strict: None,
            security_definer: None,
            leakproof: None,
            cost: None,
            rows: None,
            config: Vec::new(),
            config_source: Vec::new(),
        };
        for option in options {
            match option {
                RoutineOption::Language(language) => {
                    out.language = Some(language.to_ascii_lowercase());
                }
                RoutineOption::Body(body) => {
                    if out.body.is_some() {
                        return Err(invalid_definition("duplicate function body specified"));
                    }
                    out.body = Some(body.clone());
                }
                RoutineOption::Volatility(volatility) => {
                    out.volatility = Some(match volatility {
                        RoutineVolatility::Immutable => 'i',
                        RoutineVolatility::Stable => 's',
                        RoutineVolatility::Volatile => 'v',
                    });
                }
                RoutineOption::Parallel(parallel) => {
                    out.parallel = Some(match parallel {
                        RoutineParallel::Safe => 's',
                        RoutineParallel::Restricted => 'r',
                        RoutineParallel::Unsafe => 'u',
                    });
                }
                RoutineOption::Strict(strict) => out.strict = Some(*strict),
                RoutineOption::SecurityDefiner(definer) => out.security_definer = Some(*definer),
                RoutineOption::Leakproof(leakproof) => out.leakproof = Some(*leakproof),
                RoutineOption::Cost(cost) => {
                    if *cost <= 0.0 {
                        return Err(invalid_definition("COST must be positive"));
                    }
                    out.cost = Some(*cost);
                }
                RoutineOption::Rows(rows) => {
                    if *rows <= 0.0 {
                        return Err(invalid_definition("ROWS must be positive"));
                    }
                    out.rows = Some(*rows);
                }
                RoutineOption::Set {
                    name,
                    value,
                    source,
                } => {
                    out.config.push(match value {
                        Some(value) => format!("{name}={value}"),
                        None => name.clone(),
                    });
                    out.config_source.push(source.clone());
                }
                RoutineOption::Window => out.window = true,
                RoutineOption::Support(_) | RoutineOption::Transform(_) => {}
            }
        }
        Ok(out)
    }
}

/// The source, external object, and form a routine record stores.
fn body_parts(
    body: &RoutineBody,
    language: &str,
    routine_name: &str,
) -> Result<(String, Option<String>, BodyForm), ExecError> {
    match body {
        RoutineBody::Source(object_file) if language == "c" => Ok((
            routine_name.to_string(),
            Some(object_file.clone()),
            BodyForm::Source,
        )),
        RoutineBody::External {
            object_file,
            link_symbol,
        } if language == "c" => Ok((
            link_symbol.clone(),
            Some(object_file.clone()),
            BodyForm::Source,
        )),
        RoutineBody::External { .. } => Err(invalid_definition(format!(
            "only one AS item needed for language \"{language}\""
        ))),
        RoutineBody::Source(text) => Ok((text.clone(), None, BodyForm::Source)),
        RoutineBody::Atomic { text, .. } => Ok((text.clone(), None, BodyForm::Atomic)),
        RoutineBody::Return { text, .. } => Ok((text.clone(), None, BodyForm::Return)),
    }
}

/// Build the catalog record a `CREATE … FUNCTION`/`PROCEDURE` defines.
fn build_routine(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    stmt: &CreateRoutineStmt,
    owner: &str,
    return_shell: Option<&str>,
) -> Result<Routine, ExecError> {
    let kind = object_kind(stmt.object).expect("CREATE ROUTINE is not PostgreSQL syntax");
    let options = Options::collect(&stmt.options)?;
    let routine_body = options
        .body
        .ok_or_else(|| invalid_definition("no function body specified"))?;
    let language = options.language.unwrap_or_else(|| match routine_body {
        RoutineBody::Atomic { .. } | RoutineBody::Return { .. } => "sql".into(),
        _ => String::new(),
    });
    if language.is_empty() {
        return Err(invalid_definition("no language specified"));
    }
    if !LANGUAGES.contains(&language.as_str()) {
        return Err(ExecError::FunctionError {
            sqlstate: "42704",
            message: format!("language \"{language}\" does not exist"),
        });
    }
    let mut params = Vec::with_capacity(stmt.args.len());
    let mut seen_default = false;
    for arg in &stmt.args {
        if arg.mode.is_input() {
            if arg.default.is_some() {
                seen_default = true;
            } else if seen_default {
                return Err(invalid_definition(
                    "input parameters after one with a default value must also have defaults",
                ));
            }
        }
        params.push(RoutineParam {
            name: arg.name.clone(),
            mode: catalog_mode(arg.mode),
            ty: resolve_type(kv, resolution, &arg.ty, false)?,
            default: arg.default.clone(),
        });
    }
    let result = match &stmt.returns {
        RoutineReturn::Unspecified => RoutineResult::Unspecified,
        RoutineReturn::Type { ty, setof } => RoutineResult::Type {
            ty: resolve_return_type(kv, resolution, ty, return_shell)?,
            setof: *setof,
        },
        RoutineReturn::Table(columns) => RoutineResult::Table(
            columns
                .iter()
                .map(|column| {
                    Ok((
                        column.name.clone(),
                        resolve_type(kv, resolution, &column.ty, true)?,
                    ))
                })
                .collect::<Result<Vec<_>, ExecError>>()?,
        ),
    };
    validate_polymorphic_result(&params, &result)?;
    if kind == RoutineKind::Function {
        validate_output_result(&params, &result)?;
    }
    if kind == RoutineKind::Procedure && !matches!(result, RoutineResult::Unspecified) {
        return Err(invalid_definition("procedures cannot have a return value"));
    }
    let (mut body, object_file, body_form) = body_parts(&routine_body, &language, &stmt.name)?;
    if let RoutineBody::Atomic { statements, .. } = &routine_body
        && let Some(deparsed) = deparse_atomic_merge(kv, resolution, statements)
    {
        body = deparsed;
    }
    if body_form == BodyForm::Return
        && (params
            .iter()
            .any(|param| param.mode.is_input() && is_polymorphic_type(&param.ty.name))
            || matches!(
                &result,
                RoutineResult::Type { ty, .. } if is_polymorphic_type(&ty.name)
            )
            || matches!(
                &result,
                RoutineResult::Table(columns)
                    if columns.iter().any(|(_, ty)| is_polymorphic_type(&ty.name))
            ))
    {
        return Err(invalid_definition(
            "SQL function with unquoted function body cannot have polymorphic arguments",
        ));
    }
    let volatility = options.volatility.unwrap_or('v');
    Ok(Routine {
        oid: 0,
        name: stmt.name.clone(),
        kind,
        params,
        result,
        language,
        body,
        object_file,
        body_form,
        volatility,
        parallel: options.parallel.unwrap_or('u'),
        window: options.window,
        strict: options.strict.unwrap_or(false),
        security_definer: options.security_definer.unwrap_or(false),
        leakproof: options.leakproof.unwrap_or(false),
        cost: options.cost.unwrap_or(100.0),
        rows: options
            .rows
            .unwrap_or(if parsed_returns_set(&stmt.returns) {
                1000.0
            } else {
                0.0
            }),
        config: options.config,
        config_source: options.config_source,
        owner: owner.to_string(),
        aggregate: None,
    })
}

/// PostgreSQL stores an analysed query tree for `BEGIN ATOMIC`, then prints its
/// canonical spelling through `pg_get_functiondef`. Keep the raw text for every
/// other body and only normalize the single-statement MERGE form we can fully
/// reconstruct from this AST.
fn deparse_atomic_merge(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    statements: &[Statement],
) -> Option<String> {
    let [
        Statement::Merge {
            table,
            with: None,
            alias,
            source:
                MergeSource::Table {
                    name: source,
                    alias: source_alias,
                },
            on,
            clauses,
            returning,
        },
    ] = statements
    else {
        return None;
    };
    let target_name = crate::relname::resolve_relation(
        kv,
        resolution,
        table,
        crate::relname::SchemaDisposition::Reference,
    )
    .ok()?;
    let source_name = crate::relname::resolve_relation(
        kv,
        resolution,
        source,
        crate::relname::SchemaDisposition::Reference,
    )
    .ok()?;
    let target_columns = crabka_pgcatalog::get_table(kv, &target_name)
        .ok()?
        .columns
        .into_iter()
        .map(|column| column.name)
        .collect::<Vec<_>>();
    let source_columns = crabka_pgcatalog::get_table(kv, &source_name)
        .ok()?
        .columns
        .into_iter()
        .map(|column| column.name)
        .collect::<Vec<_>>();
    let utc = jiff::tz::TimeZone::UTC;
    let style = crabka_pgtypes::encoding::OutputStyle::with_zone(&utc);
    let target_alias = alias.as_deref().unwrap_or(&table.name);
    let source_alias = source_alias.as_deref().unwrap_or(&source.name);
    let mut out = format!(
        "MERGE INTO {} {}\n    USING {} {}\n    ON {}",
        merge_relation_text(table),
        crate::catalog_fn::quote_identifier(target_alias),
        merge_relation_text(source),
        crate::catalog_fn::quote_identifier(source_alias),
        merge_expression_text(on, style),
    );
    for clause in clauses {
        let kind = match clause.kind {
            MergeMatchKind::Matched => "MATCHED",
            MergeMatchKind::NotMatchedByTarget => {
                if returning.is_some() {
                    "NOT MATCHED"
                } else {
                    "NOT MATCHED BY TARGET"
                }
            }
            MergeMatchKind::NotMatchedBySource => "NOT MATCHED BY SOURCE",
        };
        let _ = write!(out, "\n    WHEN {kind}");
        if let Some(condition) = &clause.condition {
            let _ = write!(
                out,
                "\n     AND {}",
                merge_expression_text(condition, style)
            );
        }
        out.push_str("\n     THEN ");
        merge_action_text(&mut out, &clause.action, &target_columns, style);
    }
    if let Some(returning) = returning {
        merge_returning_text(
            &mut out,
            returning,
            source_alias,
            &source_columns,
            target_alias,
            &target_columns,
            style,
        );
    }
    Some(out)
}

fn merge_relation_text(relation: &RelationRef) -> String {
    relation.schema.as_ref().map_or_else(
        || crate::catalog_fn::quote_identifier(&relation.name),
        |schema| {
            format!(
                "{}.{}",
                crate::catalog_fn::quote_identifier(schema),
                crate::catalog_fn::quote_identifier(&relation.name)
            )
        },
    )
}

fn merge_expression_text(
    expression: &Expr,
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> String {
    crate::viewdef::expression_text_with_qualifiers(expression, style)
        .replace("merge_action()", "MERGE_ACTION()")
}

fn merge_action_text(
    out: &mut String,
    action: &MergeAction,
    target_columns: &[String],
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) {
    match action {
        MergeAction::Update(assignments) => {
            let assignments = assignments
                .iter()
                .map(|assignment| merge_assignment_text(assignment, style))
                .collect::<Vec<_>>()
                .join(", ");
            let _ = write!(out, "UPDATE SET {assignments}");
        }
        MergeAction::Delete => out.push_str("DELETE"),
        MergeAction::DoNothing => out.push_str("DO NOTHING"),
        MergeAction::Insert {
            columns,
            indirections,
            overriding,
            values,
        } => {
            out.push_str("INSERT");
            let columns = if let Some(columns) = columns {
                columns.as_slice()
            } else if values.is_some() {
                target_columns
            } else {
                &[]
            };
            let indirections = indirections.as_deref().unwrap_or_default();
            let columns = columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    let chain = indirections.get(index).map_or(&[][..], Vec::as_slice);
                    merge_target_text(column, chain, style)
                })
                .collect::<Vec<_>>()
                .join(", ");
            if !columns.is_empty() {
                let _ = write!(out, " ({columns})");
            }
            if let Some(overriding) = overriding {
                out.push_str(match overriding {
                    InsertOverride::User => " OVERRIDING USER VALUE",
                    InsertOverride::System => " OVERRIDING SYSTEM VALUE",
                });
            }
            match values {
                Some(values) => {
                    let values = values
                        .iter()
                        .map(|value| merge_expression_text(value, style))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let _ = write!(out, "\n      VALUES ({values})");
                }
                None => out.push_str(" DEFAULT VALUES"),
            }
        }
    }
}

fn merge_assignment_text(
    assignment: &Assignment,
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> String {
    let targets = assignment
        .targets
        .iter()
        .enumerate()
        .map(|(index, target)| {
            merge_target_text(
                target,
                if index == 0 {
                    &assignment.indirections
                } else {
                    &[]
                },
                style,
            )
        })
        .collect::<Vec<_>>();
    let targets = if targets.len() == 1 {
        targets.into_iter().next().expect("one assignment target")
    } else {
        format!("({})", targets.join(", "))
    };
    let value = match &assignment.value {
        AssignmentValue::Expr(value) => merge_expression_text(value, style),
        AssignmentValue::Row(values) => format!(
            "({})",
            values
                .iter()
                .map(|value| merge_expression_text(value, style))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        AssignmentValue::Subquery(query) => {
            let mut value = "(".to_string();
            crate::viewdef::write_rule_query_with_qualifiers(&mut value, query, &[], false, style);
            value.push(')');
            value
        }
    };
    format!("{targets} = {value}")
}

fn merge_target_text(
    target: &str,
    indirections: &[TargetIndirection],
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> String {
    let mut out = crate::catalog_fn::quote_identifier(target);
    for indirection in indirections {
        match indirection {
            TargetIndirection::Field(field) => {
                let _ = write!(out, ".{}", crate::catalog_fn::quote_identifier(field));
            }
            TargetIndirection::Subscript(ArraySubscript::Index(index)) => {
                let _ = write!(out, "[{}]", merge_expression_text(index, style));
            }
            TargetIndirection::Subscript(ArraySubscript::Slice { lower, upper }) => {
                let lower = lower
                    .as_ref()
                    .map_or_else(String::new, |value| merge_expression_text(value, style));
                let upper = upper
                    .as_ref()
                    .map_or_else(String::new, |value| merge_expression_text(value, style));
                let _ = write!(out, "[{lower}:{upper}]");
            }
        }
    }
    out
}

fn merge_returning_text(
    out: &mut String,
    returning: &Returning,
    source_alias: &str,
    source_columns: &[String],
    target_alias: &str,
    target_columns: &[String],
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) {
    let old_alias = returning.old_alias.as_deref().unwrap_or("old");
    let new_alias = returning.new_alias.as_deref().unwrap_or("new");
    let mut items = Vec::new();
    for item in &returning.items {
        match item {
            SelectItem::Expr { expr, alias } => {
                let mut item = merge_expression_text(expr, style);
                if let Some(alias) = alias {
                    let _ = write!(item, " AS {}", crate::catalog_fn::quote_identifier(alias));
                }
                items.push(item);
            }
            SelectItem::Wildcard => {
                items.extend(merge_columns(source_alias, source_columns));
                items.extend(merge_columns(target_alias, target_columns));
            }
            SelectItem::QualifiedWildcard(alias) if alias == old_alias => {
                items.extend(merge_columns(old_alias, target_columns));
            }
            SelectItem::QualifiedWildcard(alias) if alias == new_alias => {
                items.extend(merge_columns(new_alias, target_columns));
            }
            SelectItem::QualifiedWildcard(alias) if alias == source_alias => {
                items.extend(merge_columns(source_alias, source_columns));
            }
            SelectItem::QualifiedWildcard(alias) if alias == target_alias => {
                items.extend(merge_columns(target_alias, target_columns));
            }
            SelectItem::QualifiedWildcard(alias) => {
                items.push(format!("{}.*", crate::catalog_fn::quote_identifier(alias)))
            }
        }
    }
    let with = match (&returning.old_alias, &returning.new_alias) {
        (None, None) => String::new(),
        (old, new) => format!(
            " WITH (OLD AS {}, NEW AS {})",
            crate::catalog_fn::quote_identifier(old.as_deref().unwrap_or("old")),
            crate::catalog_fn::quote_identifier(new.as_deref().unwrap_or("new")),
        ),
    };
    let _ = write!(out, "\n   RETURNING{with} ");
    let mut items = items.into_iter();
    if let Some(item) = items.next() {
        out.push_str(&item);
        for item in items {
            let _ = write!(out, ",\n     {item}");
        }
    }
}

fn merge_columns(alias: &str, columns: &[String]) -> Vec<String> {
    columns
        .iter()
        .map(|column| {
            format!(
                "{}.{}",
                crate::catalog_fn::quote_identifier(alias),
                crate::catalog_fn::quote_identifier(column)
            )
        })
        .collect()
}

fn validate_polymorphic_result(
    params: &[RoutineParam],
    result: &RoutineResult,
) -> Result<(), ExecError> {
    let mut outputs = params
        .iter()
        .filter(|param| param.mode.is_output())
        .map(|param| &param.ty)
        .collect::<Vec<_>>();
    match result {
        RoutineResult::Type { ty, .. } => outputs.push(ty),
        RoutineResult::Table(columns) => outputs.extend(columns.iter().map(|(_, ty)| ty)),
        RoutineResult::Unspecified => {}
    }
    let validations: &[(&str, &[&str], &str)] = &[
        (
            "anyelement",
            &[
                "anyelement",
                "anyarray",
                "anynonarray",
                "anyenum",
                "anyrange",
                "anymultirange",
            ],
            "A result of type anyelement requires at least one input of type anyelement, anyarray, anynonarray, anyenum, anyrange, or anymultirange.",
        ),
        (
            "anycompatible",
            &[
                "anycompatible",
                "anycompatiblearray",
                "anycompatiblenonarray",
                "anycompatiblerange",
                "anycompatiblemultirange",
            ],
            "A result of type anycompatible requires at least one input of type anycompatible, anycompatiblearray, anycompatiblenonarray, anycompatiblerange, or anycompatiblemultirange.",
        ),
        (
            "anymultirange",
            &["anyrange", "anymultirange"],
            "A result of type anymultirange requires at least one input of type anyrange or anymultirange.",
        ),
        (
            "anyrange",
            &["anyrange", "anymultirange"],
            "A result of type anyrange requires at least one input of type anyrange or anymultirange.",
        ),
        (
            "anycompatiblemultirange",
            &["anycompatiblerange", "anycompatiblemultirange"],
            "A result of type anycompatiblemultirange requires at least one input of type anycompatiblerange or anycompatiblemultirange.",
        ),
        (
            "anycompatiblerange",
            &["anycompatiblerange", "anycompatiblemultirange"],
            "A result of type anycompatiblerange requires at least one input of type anycompatiblerange or anycompatiblemultirange.",
        ),
    ];
    for &(result_name, input_names, detail) in validations {
        if outputs.iter().any(|ty| ty.name == result_name)
            && !params
                .iter()
                .any(|param| param.mode.is_input() && input_names.contains(&param.ty.name.as_str()))
        {
            return Err(ExecError::Remote(
                crabka_pgwire::error::PgError::error("42P13", "cannot determine result data type")
                    .with_detail(detail),
            ));
        }
    }
    Ok(())
}

fn validate_output_result(
    params: &[RoutineParam],
    result: &RoutineResult,
) -> Result<(), ExecError> {
    let outputs = params
        .iter()
        .filter(|param| param.mode.is_output())
        .collect::<Vec<_>>();
    let Some(first) = outputs.first() else {
        return Ok(());
    };
    let expected = if outputs.len() == 1 {
        &first.ty.name
    } else {
        "record"
    };
    if matches!(result, RoutineResult::Unspecified)
        || (outputs.len() == 1
            && matches!(result, RoutineResult::Type { ty, setof: false } if ty == &first.ty))
        || (outputs.len() > 1
            && matches!(result, RoutineResult::Type { ty, .. } if is_record_type(ty)))
    {
        return Ok(());
    }
    Err(invalid_definition(format!(
        "function result type must be {expected} because of OUT parameters"
    )))
}

fn parsed_returns_set(returns: &RoutineReturn) -> bool {
    match returns {
        RoutineReturn::Type { setof, .. } => *setof,
        RoutineReturn::Table(_) => true,
        RoutineReturn::Unspecified => false,
    }
}

fn catalog_mode(mode: RoutineArgMode) -> ParamMode {
    match mode {
        RoutineArgMode::In => ParamMode::In,
        RoutineArgMode::Out => ParamMode::Out,
        RoutineArgMode::InOut => ParamMode::InOut,
        RoutineArgMode::Variadic => ParamMode::Variadic,
    }
}

/// `CREATE [OR REPLACE] { FUNCTION | PROCEDURE }`.
pub(crate) fn create(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    stmt: &CreateRoutineStmt,
    owner: &str,
    check_function_bodies: bool,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let return_shell = return_shell_to_create(kv, resolution, stmt)?;
    let routine = build_routine(
        kv,
        resolution,
        stmt,
        owner,
        return_shell
            .as_ref()
            .map(|(_, type_name)| type_name.as_str()),
    )?;
    let identity = routine.identity();
    if let Some(existing) = get_routine(kv, &identity)? {
        if !stmt.or_replace {
            return Err(duplicate_routine(&routine.name));
        }
        check_replaceable(&existing, &routine)?;
    }
    match routine.language.as_str() {
        "c" => validate_c_routine(&routine)?,
        "internal" => validate_internal_routine(&routine)?,
        _ => {}
    }
    require_leakproof_superuser(kv, owner, routine.leakproof)?;
    // A quoted SQL body is checked only when `check_function_bodies` is on;
    // standard SQL bodies always undergo definition-time analysis.
    if routine.language == "sql" {
        check_sql_body(&routine, check_function_bodies)?;
    } else if routine.language == "plpgsql"
        && let Err(error) = parse_plpgsql_body(&routine)
        && !matches!(
            &error,
            ExecError::FunctionError {
                sqlstate: "0A000",
                ..
            }
        )
    {
        return Err(error);
    }
    let mut ops = return_shell.map_or_else(
        || Ok(Vec::new()),
        |(name, _)| crate::usertype::create_routine_return_shell(kv, &name),
    )?;
    ops.extend(put_routine_ops(kv, &routine)?);
    Ok((
        QueryResult::Command {
            tag: format!("CREATE {}", stmt.object.tag_word()),
        },
        ops,
    ))
}

/// The `CREATE OR REPLACE` compatibility rules `PostgreSQL` enforces.
fn check_replaceable(existing: &Routine, replacement: &Routine) -> Result<(), ExecError> {
    let hint = format!(
        "Use DROP {} {} first.",
        if existing.kind == RoutineKind::Procedure {
            "PROCEDURE"
        } else {
            "FUNCTION"
        },
        existing.identity()
    );
    if existing.kind != replacement.kind || existing.window != replacement.window {
        return Err(ExecError::Remote(
            crabka_pgwire::error::PgError::error("42809", "cannot change routine kind")
                .with_detail(format!(
                    "\"{}\" is a {}.",
                    existing.name,
                    existing.kind.word()
                )),
        ));
    }
    if effective_result(existing) != effective_result(replacement) {
        return Err(ExecError::Remote(
            crabka_pgwire::error::PgError::error(
                "42P13",
                "cannot change return type of existing function",
            )
            .with_hint(hint),
        ));
    }
    for (before, after) in existing
        .input_params()
        .zip(replacement.input_params())
        .filter(|(before, after)| before.name != after.name)
    {
        let _ = after;
        if let Some(name) = &before.name {
            return Err(ExecError::Remote(
                crabka_pgwire::error::PgError::error(
                    "42P13",
                    format!("cannot change name of input parameter \"{name}\""),
                )
                .with_hint(hint),
            ));
        }
    }
    Ok(())
}

fn effective_result(routine: &Routine) -> RoutineResult {
    let result = if !matches!(routine.result, RoutineResult::Unspecified) {
        routine.result.clone()
    } else {
        let outputs = routine.output_params().collect::<Vec<_>>();
        match outputs.as_slice() {
            [] => RoutineResult::Unspecified,
            [output] => RoutineResult::Type {
                ty: output.ty.clone(),
                setof: false,
            },
            _ => RoutineResult::Type {
                ty: RoutineType::builtin(ColumnType::Record(None)),
                setof: false,
            },
        }
    };
    match result {
        RoutineResult::Type { ty, setof } if is_record_type(&ty) => RoutineResult::Type {
            ty: RoutineType::builtin(ColumnType::Record(None)),
            setof,
        },
        result => result,
    }
}

/// `DROP { FUNCTION | PROCEDURE | ROUTINE }`.
pub(crate) fn drop_routines(
    kv: &dyn Kv,
    object: RoutineObject,
    if_exists: bool,
    routines: &[RoutineSignature],
    cascade: bool,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let mut ops = Vec::new();
    for signature in routines {
        match resolve_signature(kv, object, signature) {
            Ok(routine) => {
                let ordinary = crabka_pgcatalog::trigger::list_triggers(kv)?
                    .into_iter()
                    .filter(|trigger| trigger.function_oid == routine.oid)
                    .collect::<Vec<_>>();
                let event = crabka_pgcatalog::trigger::list_event_triggers(kv)?
                    .into_iter()
                    .filter(|trigger| trigger.function_oid == routine.oid)
                    .collect::<Vec<_>>();
                let casts = crabka_pgcatalog::list_user_casts(kv)?
                    .into_iter()
                    .filter(|cast| {
                        cast.method == 'f' && cast.function.parse::<u32>() == Ok(routine.oid)
                    })
                    .collect::<Vec<_>>();
                let fdws = crabka_pgcatalog::list_fdws(kv)?
                    .into_iter()
                    .filter(|fdw| {
                        [fdw.handler.as_deref(), fdw.validator.as_deref()]
                            .into_iter()
                            .flatten()
                            .any(|name| {
                                name.strip_prefix("public.").unwrap_or(name) == routine.name
                            })
                    })
                    .collect::<Vec<_>>();
                if !cascade && (!ordinary.is_empty() || !event.is_empty() || !casts.is_empty()) {
                    return Err(ExecError::DependentObjectsStillExist(format!(
                        "cannot drop function {} because other objects depend on it",
                        routine.identity()
                    )));
                }
                if !cascade && !fdws.is_empty() {
                    let detail = fdws
                        .iter()
                        .map(|fdw| {
                            format!(
                                "foreign-data wrapper {} depends on function {}",
                                fdw.name,
                                routine.identity()
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    return Err(ExecError::Remote(
                        crabka_pgwire::error::PgError::error(
                            "2BP01",
                            format!(
                                "cannot drop function {} because other objects depend on it",
                                routine.identity()
                            ),
                        )
                        .with_detail(detail)
                        .with_hint("Use DROP ... CASCADE to drop the dependent objects too."),
                    ));
                }
                if cascade {
                    for trigger in ordinary {
                        ops.extend(crabka_pgcatalog::trigger::drop_trigger_ops(
                            trigger.table_id,
                            &trigger.name,
                        ));
                    }
                    for trigger in event {
                        ops.extend(crabka_pgcatalog::trigger::drop_event_trigger_ops(
                            &trigger.name,
                        ));
                    }
                    for cast in casts {
                        ops.extend(crabka_pgcatalog::drop_user_cast_ops(
                            cast.source,
                            cast.target,
                        ));
                    }
                    for fdw in fdws {
                        ops.extend(crabka_pgcatalog::drop_fdw_with_dependents_ops(
                            kv, &fdw.name, true,
                        )?);
                    }
                }
                ops.extend(drop_routine_ops(&routine.identity()));
            }
            Err(error) if if_exists && is_undefined(&error) => {}
            Err(error) => return Err(error),
        }
    }
    Ok((
        QueryResult::Command {
            tag: format!("DROP {}", object.tag_word()),
        },
        ops,
    ))
}

fn is_undefined(error: &ExecError) -> bool {
    matches!(error, ExecError::UndefinedFunction(_))
}

/// `ALTER { FUNCTION | PROCEDURE | ROUTINE }`.
pub(crate) fn alter(
    kv: &dyn Kv,
    object: RoutineObject,
    signature: &RoutineSignature,
    action: &AlterRoutineAction,
    owner: &str,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let mut routine = resolve_signature(kv, object, signature)?;
    let tag = QueryResult::Command {
        tag: format!("ALTER {}", object.tag_word()),
    };
    let mut ops = Vec::new();
    match action {
        AlterRoutineAction::RenameTo(new_name) => {
            let renamed = signature_identity(new_name, &routine.input_type_names());
            if get_routine(kv, &renamed)?.is_some() {
                return Err(duplicate_routine(new_name));
            }
            ops.extend(drop_routine_ops(&routine.identity()));
            routine.name = new_name.clone();
        }
        AlterRoutineAction::OwnerTo(owner) => routine.owner = owner.clone(),
        AlterRoutineAction::SetSchema(schema) => {
            if schema != "public" {
                return Err(ExecError::Unsupported(format!(
                    "ALTER {} … SET SCHEMA {schema} is not supported: user routines live in \
                     the public schema",
                    object.tag_word()
                )));
            }
        }
        AlterRoutineAction::DependsOnExtension { .. } => {}
        AlterRoutineAction::Options(options) => {
            let collected = Options::collect(options)?;
            if let Some(volatility) = collected.volatility {
                routine.volatility = volatility;
            }
            if let Some(parallel) = collected.parallel {
                routine.parallel = parallel;
            }
            if let Some(strict) = collected.strict {
                routine.strict = strict;
            }
            if let Some(definer) = collected.security_definer {
                routine.security_definer = definer;
            }
            if let Some(leakproof) = collected.leakproof {
                require_leakproof_superuser(kv, owner, leakproof)?;
                routine.leakproof = leakproof;
            }
            if let Some(cost) = collected.cost {
                routine.cost = cost;
            }
            if let Some(rows) = collected.rows {
                routine.rows = rows;
            }
            for (entry, source) in collected.config.into_iter().zip(collected.config_source) {
                let name = entry.split('=').next().unwrap_or(&entry).to_string();
                let mut config = Vec::new();
                let mut config_source = Vec::new();
                for (existing, existing_source) in routine.config.iter().zip(&routine.config_source)
                {
                    if existing.split('=').next().unwrap_or(existing) != name && name != "all" {
                        config.push(existing.clone());
                        config_source.push(existing_source.clone());
                    }
                }
                routine.config = config;
                routine.config_source = config_source;
                if entry.contains('=') {
                    routine.config.push(entry);
                    routine.config_source.push(source);
                }
            }
        }
    }
    ops.extend(put_routine_ops(kv, &routine)?);
    Ok((tag, ops))
}

/// `LEAKPROOF` lets the optimizer move a call across security barriers, so only
/// a superuser may assert it.
fn require_leakproof_superuser(kv: &dyn Kv, owner: &str, leakproof: bool) -> Result<(), ExecError> {
    if leakproof && !crate::rls::role_is_superuser(kv, owner)? {
        return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
            "42501",
            "only superuser can define a leakproof function",
        )));
    }
    Ok(())
}

/// Resolve the routine a `DROP`/`ALTER` names.
fn resolve_signature(
    kv: &dyn Kv,
    object: RoutineObject,
    signature: &RoutineSignature,
) -> Result<Routine, ExecError> {
    let wanted = object_kind(object);
    let found = match &signature.args {
        Some(args) => {
            let types = signature_type_names(kv, args)?;
            let identity = signature_identity(&signature.name, &types);
            get_routine(kv, &identity)?.ok_or_else(|| {
                undefined_routine(format!(
                    "function {}({}) does not exist",
                    signature.name,
                    types.join(", ")
                ))
            })?
        }
        None => {
            let mut candidates = routines_named(kv, &signature.name)?;
            if candidates.is_empty() {
                return Err(undefined_routine(format!(
                    "could not find a function named \"{}\"",
                    signature.name
                )));
            }
            if candidates.len() > 1 {
                return Err(ambiguous_routine(&signature.name));
            }
            candidates.remove(0)
        }
    };
    if let Some(kind) = wanted
        && found.kind != kind
    {
        return Err(wrong_routine_kind(format!(
            "{} is not a {}",
            spelled_signature(&found),
            kind.word()
        )));
    }
    Ok(found)
}

/// `name(t1, t2)` as `PostgreSQL` spells a routine in a `42809`.
fn spelled_signature(routine: &Routine) -> String {
    format!(
        "{}({})",
        routine.name,
        routine.input_type_names().join(", ")
    )
}

fn signature_type_names(kv: &dyn Kv, args: &[RoutineArg]) -> Result<Vec<String>, ExecError> {
    args.iter()
        .filter(|arg| arg.mode.is_input())
        .map(|arg| {
            Ok(resolve_type(
                kv,
                crate::relname::ResolutionScope::default_scope(),
                &arg.ty,
                false,
            )?
            .name)
        })
        .collect()
}

// ------------------------------------------------------------------ calling

/// Does a user-defined aggregate of this name and arity own the call?
///
/// A name may carry both an aggregate and an ordinary function — nothing stops
/// `CREATE AGGREGATE acc(int4)` next to `CREATE FUNCTION acc(text)` — and the
/// scalar paths must stand aside when the aggregate is the one being called,
/// or the query silently returns one row per input instead of aggregating.
fn shadowing_user_aggregate(kv: &dyn Kv, name: &str, arity: usize) -> bool {
    routines_named(kv, name).is_ok_and(|found| {
        found
            .iter()
            .any(|routine| routine.is_aggregate() && routine.input_params().count() == arity)
    })
}

/// Is `name` a routine this catalog defines that an ordinary call could reach?
///
/// A user-defined aggregate does not count. Its `pg_proc` row names the
/// internal language and a dummy body, so letting one through here would send
/// every aggregate call into the scalar inliner instead of the aggregate
/// evaluator.
pub(crate) fn is_user_routine(kv: &dyn Kv, name: &str) -> bool {
    routines_named(kv, name).is_ok_and(|found| found.iter().any(|routine| !routine.is_aggregate()))
}

/// Shell-type routines are created before their type has a `ColumnType`.
/// Once the shell is completed, make their stored named signature concrete at
/// every call boundary.
fn hydrate_user_type_signature(mut routine: Routine) -> Routine {
    let hydrate = |ty: &mut RoutineType| {
        if ty.column.is_none() {
            ty.column = crabka_pgtypes::usertype::lookup(&ty.name)
                .and_then(|definition| definition.column_type());
        }
    };
    for param in &mut routine.params {
        hydrate(&mut param.ty);
    }
    if let RoutineResult::Type { ty, .. } = &mut routine.result {
        hydrate(ty);
    }
    routine
}

/// Resolve a call of `name` with `given` argument types.
///
/// Returns `Ok(None)` when the name carries no routine at all, which lets the
/// caller keep whichever error its own family would have produced.
pub(crate) fn resolve_call(
    kv: &dyn Kv,
    name: &str,
    given: &[ArgType],
) -> Result<Option<Routine>, ExecError> {
    // An aggregate is never the answer to a scalar call; `agg` resolves those.
    let candidates: Vec<Routine> = routines_named(kv, name)?
        .into_iter()
        .map(hydrate_user_type_signature)
        .filter(|routine| !routine.is_aggregate())
        .collect();
    resolve_candidates(name, candidates, given)
}

/// Apply ordinary overload resolution to a caller-selected candidate set.
fn resolve_candidates(
    name: &str,
    candidates: Vec<Routine>,
    given: &[ArgType],
) -> Result<Option<Routine>, ExecError> {
    if candidates.is_empty() {
        return Ok(None);
    }
    let arity_matched: Vec<&Routine> = candidates
        .iter()
        .filter(|routine| {
            let params: Vec<&RoutineParam> = routine.input_params().collect();
            let Some(variadic_index) = variadic_input_index(&params) else {
                let total = params.len();
                let required = total - routine.default_count();
                return (required..=total).contains(&given.len());
            };
            let required = params[..variadic_index]
                .iter()
                .filter(|param| param.default.is_none())
                .count();
            given.len() >= required
        })
        .collect();
    let mut exact = Vec::new();
    let mut coercible = Vec::new();
    for routine in arity_matched {
        let params: Vec<&RoutineParam> = routine.input_params().collect();
        let variadic_index = variadic_input_index(&params);
        let expand_variadic = variadic_index
            .is_some_and(|index| variadic_arguments_are_expanded(&params, given, index));
        let mut is_exact = true;
        let mut is_coercible = true;
        for (index, arg) in given.iter().enumerate() {
            let param = variadic_index
                .filter(|variadic_index| expand_variadic && index >= *variadic_index)
                .map(|variadic_index| params[variadic_index])
                .or_else(|| params.get(index).copied())
                .expect("arity matching supplies a parameter");
            let target = if variadic_index
                .is_some_and(|variadic_index| expand_variadic && index >= variadic_index)
            {
                variadic_element_type(param)
            } else {
                param.ty.column
            };
            let Some(target) = target else {
                if is_polymorphic_type(&param.ty.name) {
                    is_exact = false;
                    is_coercible &= polymorphic_argument_matches(&param.ty.name, *arg);
                } else {
                    // A type Gres does not model can only match an untyped literal.
                    is_exact = false;
                    is_coercible = matches!(arg, ArgType::Unknown | ArgType::Opaque);
                }
                continue;
            };
            match arg {
                ArgType::Known(source) if *source == target => {}
                ArgType::Known(source) => {
                    is_exact = false;
                    if !implicitly_coercible(*source, target) {
                        is_coercible = false;
                    }
                }
                ArgType::Unknown | ArgType::Opaque => is_exact = false,
            }
        }
        is_coercible &= polymorphic_arguments_are_consistent(&params, given);
        is_exact &= polymorphic_arguments_are_exact(&params, given);
        if is_exact {
            exact.push(routine.clone());
        } else if is_coercible {
            coercible.push(routine.clone());
        }
    }
    if exact.len() == 1 {
        return resolved_candidate(exact.remove(0), given);
    }
    if exact.is_empty() && coercible.len() > 1 {
        // PostgreSQL's last resolution step: an `unknown` literal prefers the
        // string category's preferred type, so a candidate taking `text` at
        // every unknown position wins outright.
        let preferred: Vec<Routine> = coercible
            .iter()
            .filter(|routine| prefers_text_at_unknowns(routine, given))
            .cloned()
            .collect();
        if preferred.len() == 1 {
            return resolved_candidate(preferred.into_iter().next().expect("one candidate"), given);
        }
    }
    if exact.len() > 1 || (exact.is_empty() && coercible.len() > 1) {
        return Err(ExecError::FunctionError {
            sqlstate: "42725",
            message: format!(
                "function {name}({}) is not unique",
                given
                    .iter()
                    .copied()
                    .map(spelled_arg_type)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        });
    }
    if coercible.len() == 1 {
        return resolved_candidate(coercible.remove(0), given);
    }
    // The name exists but no overload takes these arguments; 42883 with the
    // argument types spelled out, like `PostgreSQL`.
    Err(undefined_routine(format!(
        "function {name}({}) does not exist",
        given
            .iter()
            .copied()
            .map(spelled_arg_type)
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

fn resolved_candidate(routine: Routine, given: &[ArgType]) -> Result<Option<Routine>, ExecError> {
    let polymorphic = routine
        .input_params()
        .zip(given)
        .filter(|(param, _)| is_polymorphic_type(&param.ty.name));
    let mut has_unknown = false;
    let mut has_concrete_type = false;
    for (param, arg) in polymorphic {
        has_unknown |= arg.is_unknown();
        has_concrete_type |= polymorphic_base_type(&param.ty.name, *arg).is_some();
    }
    if has_unknown && !has_concrete_type {
        return Err(crate::eval::undetermined_polymorphic_type());
    }
    Ok(Some(routine))
}

fn variadic_input_index(params: &[&RoutineParam]) -> Option<usize> {
    params
        .iter()
        .position(|param| param.mode == ParamMode::Variadic)
}

fn variadic_element_type(param: &RoutineParam) -> Option<ColumnType> {
    match param.ty.column {
        Some(ColumnType::Array(element)) => Some(element.column_type()),
        _ => None,
    }
}

fn variadic_arguments_are_expanded(
    params: &[&RoutineParam],
    args: &[ArgType],
    index: usize,
) -> bool {
    let Some(param) = params.get(index) else {
        return false;
    };
    args.len() != params.len()
        || !matches!((args.get(index), param.ty.column),
            (Some(ArgType::Known(arg)), Some(target)) if *arg == target)
}

fn is_polymorphic_type(name: &str) -> bool {
    matches!(
        name,
        "anyarray"
            | "anyelement"
            | "anyenum"
            | "anynonarray"
            | "anyrange"
            | "anymultirange"
            | "anycompatible"
            | "anycompatiblearray"
            | "anycompatiblemultirange"
            | "anycompatiblenonarray"
            | "anycompatiblerange"
    )
}

fn polymorphic_argument_matches(name: &str, arg: ArgType) -> bool {
    match arg {
        ArgType::Unknown | ArgType::Opaque => true,
        ArgType::Known(ty) => match name {
            "anyarray" | "anycompatiblearray" => matches!(ty, ColumnType::Array(_)),
            "anyrange" | "anycompatiblerange" => matches!(ty, ColumnType::Range(_)),
            "anymultirange" | "anycompatiblemultirange" => {
                matches!(ty, ColumnType::Multirange(_))
            }
            "anynonarray" | "anycompatiblenonarray" => !matches!(ty, ColumnType::Array(_)),
            "anyenum" => matches!(ty, ColumnType::Enum(_)),
            "anyelement" | "anycompatible" => true,
            _ => false,
        },
    }
}

fn polymorphic_base_type(name: &str, arg: ArgType) -> Option<ColumnType> {
    let ArgType::Known(ty) = arg else {
        return None;
    };
    match name {
        "anyarray" | "anycompatiblearray" => match ty {
            ColumnType::Array(elem) => Some(elem.column_type()),
            _ => None,
        },
        "anyrange" | "anycompatiblerange" => match ty {
            ColumnType::Range(range) => Some(*range.subtype),
            _ => None,
        },
        "anymultirange" | "anycompatiblemultirange" => match ty {
            ColumnType::Multirange(multirange) => Some(*multirange.range.subtype),
            _ => None,
        },
        "anyelement" | "anyenum" | "anynonarray" | "anycompatible" | "anycompatiblenonarray" => {
            Some(ty)
        }
        _ => None,
    }
}

fn polymorphic_range_type(name: &str, arg: ArgType) -> Option<crabka_pgtypes::usertype::RangeRef> {
    let ArgType::Known(ty) = arg else {
        return None;
    };
    match (name, ty) {
        ("anyrange" | "anycompatiblerange", ColumnType::Range(range)) => Some(range),
        ("anymultirange" | "anycompatiblemultirange", ColumnType::Multirange(multirange)) => {
            Some(multirange.range)
        }
        _ => None,
    }
}

fn polymorphic_arguments_are_consistent(params: &[&RoutineParam], given: &[ArgType]) -> bool {
    let mut exact = None;
    let mut exact_range = None;
    let mut compatible = None;
    let mut compatible_range = None;
    let mut compatible_range_identity = None;
    let mut compatible_inputs = Vec::new();
    for (param, arg) in params.iter().zip(given) {
        let Some(base) = polymorphic_base_type(&param.ty.name, *arg) else {
            continue;
        };
        if param.ty.name.starts_with("anycompatible") {
            compatible_inputs.push(base);
            if let Some(range) = polymorphic_range_type(&param.ty.name, *arg) {
                match compatible_range_identity {
                    None => compatible_range_identity = Some(range),
                    Some(current) if current == range => {}
                    Some(_) => return false,
                }
                match compatible_range {
                    None => compatible_range = Some(base),
                    Some(current) if current == base => {}
                    Some(_) => return false,
                }
            }
            compatible = match compatible {
                None => Some(base),
                Some(current) if implicitly_coercible(base, current) => Some(current),
                Some(current) if implicitly_coercible(current, base) => Some(base),
                Some(_) => return false,
            };
        } else if param.ty.name.starts_with("any") {
            if let Some(range) = polymorphic_range_type(&param.ty.name, *arg) {
                match exact_range {
                    None => exact_range = Some(range),
                    Some(current) if current == range => {}
                    Some(_) => return false,
                }
            }
            match exact {
                None => exact = Some(base),
                Some(current) if current == base => {}
                Some(_) => return false,
            }
        }
    }
    match compatible_range {
        None => true,
        Some(target) => compatible_inputs
            .into_iter()
            .all(|source| implicitly_coercible(source, target)),
    }
}

fn polymorphic_arguments_are_exact(params: &[&RoutineParam], given: &[ArgType]) -> bool {
    let mut traditional = None;
    let mut compatible = None;
    for (param, arg) in params.iter().zip(given) {
        let Some(base) = polymorphic_base_type(&param.ty.name, *arg) else {
            continue;
        };
        let slot = if param.ty.name.starts_with("anycompatible") {
            &mut compatible
        } else if param.ty.name.starts_with("any") {
            &mut traditional
        } else {
            continue;
        };
        match slot {
            None => *slot = Some(base),
            Some(current) if *current == base => {}
            Some(_) => return false,
        }
    }
    true
}

/// A resolved routine plus the call arguments after unknown-literal coercion
/// and trailing parameter defaults have been applied.
#[derive(Debug, Clone)]
pub(crate) struct BoundRoutineCall {
    pub routine: Routine,
    pub args: Vec<Expr>,
}

/// Resolve an overload and bind its arguments, but choose no execution
/// strategy. The returned routine keeps its `strict` flag and kind, so the
/// caller can apply the correct function/procedure semantics.
pub(crate) fn bind_call(
    kv: &dyn Kv,
    name: &str,
    args: &[Expr],
    given: &[ArgType],
) -> Result<Option<BoundRoutineCall>, ExecError> {
    let Some(routine) = resolve_call(kv, name, given)? else {
        return Ok(None);
    };
    let args = bound_args(&routine, args)?;
    Ok(Some(BoundRoutineCall { routine, args }))
}

/// Bind labeled call arguments while the routine catalog is available.
///
/// Parser-side code cannot know user routine parameter names.  This converts a
/// labeled call into the positional form the existing execution paths use.
pub(crate) fn normalize_named_call(
    kv: &dyn Kv,
    call: &FuncCall,
) -> Result<Option<FuncCall>, ExecError> {
    let FuncArgs::Named { positional, named } = &call.args else {
        return Ok(None);
    };
    let candidates: Vec<Routine> = routines_named(kv, &call.name)?
        .into_iter()
        .map(hydrate_user_type_signature)
        .filter(|routine| !routine.is_aggregate())
        .collect();
    if candidates.is_empty() {
        return normalize_builtin_named_call(call);
    }
    let mut groups: Vec<(Vec<ArgType>, Vec<(Routine, Vec<Expr>)>)> = Vec::new();
    for routine in candidates {
        let Some(args) = bind_named_args(&routine, positional, named)? else {
            continue;
        };
        let given = best_effort_arg_types(&args);
        if let Some((_, routines)) = groups.iter_mut().find(|(types, _)| *types == given) {
            routines.push((routine, args));
        } else {
            groups.push((given, vec![(routine, args)]));
        }
    }
    let mut matches = Vec::new();
    for (given, routines) in groups {
        let candidates = routines
            .iter()
            .map(|(routine, _)| routine.clone())
            .collect();
        let Some(selected) = resolve_candidates(&call.name, candidates, &given)? else {
            continue;
        };
        if let Some((_, args)) = routines
            .into_iter()
            .find(|(routine, _)| routine.identity() == selected.identity())
        {
            matches.push(args);
        }
    }
    if matches.len() != 1 {
        return Err(undefined_routine(format!(
            "function {} does not exist",
            call.name
        )));
    }
    let args = matches.pop().expect("one matched routine");
    Ok(Some(FuncCall {
        sql_syntax: call.sql_syntax,
        name: call.name.clone(),
        distinct: call.distinct,
        args: FuncArgs::Exprs(args),
        order_by: call.order_by.clone(),
        within_group: call.within_group,
        filter: call.filter.clone(),
    }))
}

/// Change explicit `VARIADIC array` syntax to the array argument a user routine
/// receives. Built-ins keep the original syntax because their evaluators expand
/// it themselves.
pub(crate) fn normalize_variadic_call(
    kv: &dyn Kv,
    call: &FuncCall,
) -> Result<Option<FuncCall>, ExecError> {
    let FuncArgs::Variadic { positional, array } = &call.args else {
        return Ok(None);
    };
    if !is_user_routine(kv, &call.name) {
        return Ok(None);
    }
    let mut args = positional.clone();
    args.push((**array).clone());
    let given = best_effort_arg_types(&args);
    match resolve_call(kv, &call.name, &given) {
        Ok(Some(_)) => {
            let mut normalized = call.clone();
            normalized.args = FuncArgs::Exprs(args);
            Ok(Some(normalized))
        }
        Ok(None) | Err(ExecError::UndefinedFunction(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Resolve the delayed argument forms of a FROM-position function call.
///
/// `TableFuncCall` keeps labels until this point because only the catalog knows
/// a user routine's parameter names. The downstream FunctionScan paths have
/// always consumed a positional vector, so normalize once at their shared
/// boundary rather than teaching every SRF implementation about labels.
pub(crate) fn normalize_table_function_call(
    kv: &dyn Kv,
    call: &TableFuncCall,
) -> Result<TableFuncCall, ExecError> {
    if crate::func::is_scalar(&call.name) {
        return Ok(call.clone());
    }
    let args = if let Some(array) = &call.variadic {
        let mut args = call.args.clone();
        args.push((**array).clone());
        args
    } else if call.named_args.is_empty() {
        call.args.clone()
    } else {
        let named_call = FuncCall {
            sql_syntax: false,
            name: call.name.clone(),
            distinct: false,
            args: FuncArgs::Named {
                positional: call.args.clone(),
                named: call.named_args.clone(),
            },
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        };
        let normalized = normalize_named_call(kv, &named_call)?
            .ok_or_else(|| undefined_routine(format!("function {} does not exist", call.name)))?;
        let FuncArgs::Exprs(args) = normalized.args else {
            unreachable!("named-call normalization produces positional arguments")
        };
        args
    };
    Ok(TableFuncCall {
        name: call.name.clone(),
        args,
        named_args: Vec::new(),
        variadic: None,
        column_defs: call.column_defs.clone(),
    })
}

/// The input signature of a built-in routine whose catalog row has argument
/// labels. This is decoded from the initialized PostgreSQL `pg_proc` fixture:
/// initdb adds several labels/defaults which are absent from `pg_proc.dat`.
#[derive(Clone)]
struct BuiltinNamedSignature {
    name: String,
    input_types: Vec<Option<ColumnType>>,
    input_names: Vec<String>,
    defaults: Vec<Expr>,
}

static BUILTIN_NAMED_SIGNATURES: std::sync::OnceLock<Option<Vec<BuiltinNamedSignature>>> =
    std::sync::OnceLock::new();

fn normalize_builtin_named_call(call: &FuncCall) -> Result<Option<FuncCall>, ExecError> {
    let FuncArgs::Named { positional, named } = &call.args else {
        return Ok(None);
    };
    let signatures = BUILTIN_NAMED_SIGNATURES
        .get_or_init(|| decode_builtin_named_signatures().ok())
        .as_ref()
        .ok_or_else(|| ExecError::Unsupported("built-in pg_proc fixture is corrupt".into()))?;
    let mut exact = Vec::new();
    let mut coercible = Vec::new();
    for signature in signatures
        .iter()
        .filter(|signature| signature.name.eq_ignore_ascii_case(&call.name))
    {
        let Some(args) = bind_builtin_named_args(signature, positional, named, &call.name)? else {
            continue;
        };
        match builtin_named_match(signature, &args) {
            Some(true) => exact.push(args),
            Some(false) => coercible.push(args),
            None => {}
        }
    }
    let matches = if exact.len() == 1 {
        exact
    } else if exact.is_empty() && coercible.len() == 1 {
        coercible
    } else if exact.is_empty() && coercible.is_empty() {
        return Ok(None);
    } else {
        return Err(undefined_routine(format!(
            "function {} does not exist",
            call.name
        )));
    };
    Ok(Some(FuncCall {
        sql_syntax: call.sql_syntax,
        name: call.name.clone(),
        distinct: call.distinct,
        args: FuncArgs::Exprs(matches.into_iter().next().expect("one matching signature")),
        order_by: call.order_by.clone(),
        within_group: call.within_group,
        filter: call.filter.clone(),
    }))
}

fn bind_builtin_named_args(
    signature: &BuiltinNamedSignature,
    positional: &[Expr],
    named: &[(String, Expr)],
    routine_name: &str,
) -> Result<Option<Vec<Expr>>, ExecError> {
    if positional.len() > signature.input_names.len() {
        return Ok(None);
    }
    let mut slots = positional.iter().cloned().map(Some).collect::<Vec<_>>();
    slots.resize(signature.input_names.len(), None);
    for (label, value) in named {
        let Some(index) = signature
            .input_names
            .iter()
            .position(|name| name.eq_ignore_ascii_case(label))
        else {
            return Ok(None);
        };
        if slots[index].is_some() {
            return Err(ExecError::Syntax(format!(
                "argument \"{label}\" of {routine_name} specified more than once"
            )));
        }
        slots[index] = Some(value.clone());
    }
    let first_default = signature
        .input_names
        .len()
        .checked_sub(signature.defaults.len())
        .expect("default count cannot exceed argument count");
    let mut args = Vec::with_capacity(signature.input_names.len());
    for (index, argument) in slots.into_iter().enumerate() {
        match argument {
            Some(argument) => args.push(argument),
            None if index >= first_default => {
                args.push(signature.defaults[index - first_default].clone());
            }
            None => return Ok(None),
        }
    }
    Ok(Some(args))
}

fn builtin_named_match(signature: &BuiltinNamedSignature, args: &[Expr]) -> Option<bool> {
    let given = best_effort_arg_types(args);
    let mut exact = true;
    for (arg, target) in given.iter().zip(&signature.input_types) {
        let Some(target) = target else {
            if !matches!(arg, ArgType::Unknown | ArgType::Opaque) {
                return None;
            }
            exact = false;
            continue;
        };
        match arg {
            ArgType::Known(source) if source == target => {}
            ArgType::Known(source) if implicitly_coercible(*source, *target) => exact = false,
            ArgType::Known(_) => return None,
            ArgType::Unknown | ArgType::Opaque => exact = false,
        }
    }
    Some(exact)
}

/// Resolve a procedure call whose argument list includes `OUT` placeholders.
/// Unlike function calls, procedure arguments stay aligned with the full
/// declaration. Output-only expressions take no part in overload resolution,
/// and Gres never coerces or evaluates them.
pub(crate) fn bind_procedure_call(
    kv: &dyn Kv,
    name: &str,
    positional: &[Expr],
    named: &[(String, Expr)],
    variadic: Option<&Expr>,
) -> Result<Option<BoundRoutineCall>, ExecError> {
    let candidates = routines_named(kv, name)?
        .into_iter()
        .map(hydrate_user_type_signature)
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(None);
    }
    let mut exact = Vec::new();
    let mut coercible = Vec::new();
    for routine in candidates {
        let Some(args) = procedure_arguments(&routine, positional, named, variadic)? else {
            continue;
        };
        let input_args = routine
            .params
            .iter()
            .zip(&args)
            .filter_map(|(param, arg)| param.mode.is_input().then_some(arg.clone()))
            .collect::<Vec<_>>();
        let given = input_args
            .iter()
            .map(|arg| {
                crate::eval::static_arg_types(
                    std::slice::from_ref(arg),
                    &crate::scope::Scope::empty(),
                )
                .map(|mut types| types.remove(0))
                .or_else(|error| match error {
                    ExecError::UndefinedFunction(_) => Ok(ArgType::Opaque),
                    error => Err(error),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let input_params = routine
            .params
            .iter()
            .filter(|param| param.mode.is_input())
            .collect::<Vec<_>>();
        let mut is_exact = true;
        let mut is_coercible = true;
        for (arg, param) in given.iter().zip(&input_params) {
            let Some(target) = param.ty.column else {
                is_exact = false;
                is_coercible = matches!(arg, ArgType::Unknown | ArgType::Opaque);
                continue;
            };
            match arg {
                ArgType::Known(source) if *source == target => {}
                ArgType::Known(source) => {
                    is_exact = false;
                    if !implicitly_coercible(*source, target) {
                        is_coercible = false;
                    }
                }
                ArgType::Unknown | ArgType::Opaque => is_exact = false,
            }
        }
        if is_exact {
            exact.push((routine, given, args));
        } else if is_coercible {
            coercible.push((routine, given, args));
        }
    }
    let selected = if exact.len() == 1 {
        exact.pop()
    } else if exact.is_empty() && coercible.len() == 1 {
        coercible.pop()
    } else if exact.is_empty() {
        let preferred = coercible
            .iter()
            .filter(|(routine, given, _)| prefers_text_at_unknowns(routine, given))
            .cloned()
            .collect::<Vec<_>>();
        (preferred.len() == 1).then(|| preferred.into_iter().next().expect("one candidate"))
    } else {
        None
    };
    let Some((routine, _, args)) = selected else {
        if coercible.len() > 1 {
            return Err(ExecError::FunctionError {
                sqlstate: "42725",
                message: format!("procedure {name} is not unique"),
            });
        }
        return Err(undefined_routine(format!(
            "procedure {name} does not exist"
        )));
    };
    let mut bound = Vec::with_capacity(routine.params.len());
    for (arg, param) in args.iter().zip(&routine.params) {
        if param.mode.is_input()
            && let Some(ty) = param.ty.column
            && crate::func::is_unknown_arg(arg)
        {
            bound.push(Expr::Cast {
                expr: Box::new(arg.clone()),
                ty,
            });
            continue;
        }
        bound.push(arg.clone());
    }
    Ok(Some(BoundRoutineCall {
        routine,
        args: bound,
    }))
}

/// Put a procedure call's forms into declaration order before overload
/// resolution. Output placeholders remain in the vector, unlike function-call
/// binding, because `CALL` matches them to the procedure's full signature.
fn procedure_arguments(
    routine: &Routine,
    positional: &[Expr],
    named: &[(String, Expr)],
    variadic: Option<&Expr>,
) -> Result<Option<Vec<Expr>>, ExecError> {
    if !named.is_empty() && variadic.is_some() {
        return Ok(None);
    }
    let params = &routine.params;
    let mut slots = if named.is_empty() {
        let mut args = positional.to_vec();
        let variadic_index = params
            .iter()
            .position(|param| param.mode == ParamMode::Variadic);
        if let Some(array) = variadic {
            let Some(index) = variadic_index else {
                return Ok(None);
            };
            if args.len() != index {
                return Ok(None);
            }
            args.push(array.clone());
        } else if let Some(index) = variadic_index
            && args.len() > index
        {
            let tail = args.split_off(index);
            args.push(Expr::ArrayLiteral(tail));
        }
        if args.len() > params.len() {
            return Ok(None);
        }
        args.into_iter().map(Some).collect::<Vec<_>>()
    } else {
        if positional.len() > params.len() {
            return Ok(None);
        }
        let mut slots = positional.iter().cloned().map(Some).collect::<Vec<_>>();
        slots.resize(params.len(), None);
        for (label, value) in named {
            let Some(index) = params
                .iter()
                .position(|param| param.name.as_deref() == Some(label))
            else {
                return Ok(None);
            };
            if slots[index].is_some() {
                return Err(ExecError::Syntax(format!(
                    "argument \"{label}\" of procedure {} specified more than once",
                    routine.name
                )));
            }
            slots[index] = Some(value.clone());
        }
        slots
    };
    slots.resize(params.len(), None);
    slots
        .into_iter()
        .zip(params)
        .map(|(arg, param)| match arg {
            Some(arg) => Ok(arg),
            None if param.mode.is_input() => param
                .default
                .as_deref()
                .map(crabka_pgparser::parser::parse_expression)
                .transpose()
                .map_err(|error| ExecError::Syntax(error.message))?
                .ok_or_else(|| {
                    undefined_routine(format!("procedure {} does not exist", routine.name))
                }),
            None => Err(undefined_routine(format!(
                "procedure {} does not exist",
                routine.name
            ))),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// Is `source` implicitly coercible to `target` for the purpose of resolving a
/// routine call?
///
/// `PostgreSQL` resolves function calls with *implicit* casts only. That is why
/// `f(bigint)` does not match `f(text)` even though the explicit cast exists.
/// This is the implicit graph restricted to the types Gres models.
pub(crate) fn implicitly_coercible(source: ColumnType, target: ColumnType) -> bool {
    use ColumnType::{
        Char, Float8, Int2, Int4, Int8, Numeric, Oid, Regclass, Regnamespace, Regprocedure,
        Regtype, Text, Timestamp, Timestamptz, Varchar,
    };
    if source == target {
        return true;
    }
    matches!(
        (source, target),
        (Int4, Int8 | Numeric(_) | Float8)
            | (Int8, Numeric(_) | Float8)
            | (Numeric(_), Float8)
            | (Timestamp, Timestamptz)
            | (Text | Varchar(_) | Char(_), Text | Varchar(_) | Char(_))
            | (Numeric(_), Numeric(_))
            // `pg_cast` marks every integer width implicitly coercible to
            // `oid`, which is what lets `binary_coercible(23, 23)` — the
            // `regress.so` helper `type_sanity` calls with bare integer
            // literals — resolve against its `(oid, oid)` signature.
            | (Int2 | Int4 | Int8, Oid)
            | (
                Oid,
                Regclass | Regtype | Regprocedure | Regnamespace
            )
            | (
                Regclass | Regtype | Regprocedure | Regnamespace,
                Oid
            )
    )
}

/// Does every parameter facing an untyped literal have the string category's
/// preferred type?
fn prefers_text_at_unknowns(routine: &Routine, given: &[ArgType]) -> bool {
    let params: Vec<&RoutineParam> = routine.input_params().collect();
    let mut saw_unknown = false;
    for (arg, param) in given.iter().zip(params.iter()) {
        if matches!(arg, ArgType::Unknown) {
            saw_unknown = true;
            if param.ty.column != Some(ColumnType::Text) {
                return false;
            }
        }
    }
    saw_unknown
}

fn spelled_arg_type(arg: ArgType) -> &'static str {
    match arg {
        ArgType::Known(ty) => ty.name(),
        ArgType::Unknown | ArgType::Opaque => "unknown",
    }
}

/// How deep one statement may inline SQL routines into each other.
///
/// A routine whose body calls another routine inlines transitively. A routine
/// that calls itself, directly or mutually, would not terminate. So Gres caps
/// the depth and reports it with PostgreSQL's own `54001`.
const MAX_INLINE_DEPTH: usize = 25;

thread_local! {
    static INLINE_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// One level of routine inlining, released when the guard drops.
pub(crate) struct InlineGuard;

impl Drop for InlineGuard {
    fn drop(&mut self) {
        INLINE_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

/// Enter one level of routine inlining.
///
/// # Errors
///
/// `54001` once [`MAX_INLINE_DEPTH`] levels are already open.
pub(crate) fn enter_inline() -> Result<InlineGuard, ExecError> {
    INLINE_DEPTH.with(|depth| {
        let next = depth.get() + 1;
        if next > MAX_INLINE_DEPTH {
            return Err(ExecError::FunctionError {
                sqlstate: "54001",
                message: "stack depth limit exceeded".into(),
            });
        }
        depth.set(next);
        Ok(InlineGuard)
    })
}

/// The label `PostgreSQL` gives an unaliased call of `expr`, which is the
/// function's own name. So an inlined routine keeps its output column name.
pub(crate) fn call_label(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Func(call) => Some(call.name.clone()),
        _ => None,
    }
}

/// Is `name` a built-in function family this engine already provides?
///
/// Gres tries a user routine first, because `PostgreSQL`'s default
/// `search_path` puts `public` ahead of `pg_catalog`, so a user function of the
/// same signature wins. When resolution *fails*, the built-in families keep
/// their own error and do not inherit the routine catalog's.
fn known_builtin(name: &str) -> bool {
    crate::catalog_fn::is_catalog_func(name)
        || crate::func::is_scalar(name)
        || crate::datetime_fn::is_datetime_func(name)
        || crate::format_fn::is_format_func(name)
        || crate::json_fn::is_json_func(name)
        || crate::array_fn::is_array_func(name)
        || crate::srf::is_srf(name)
        // An aggregate belongs here too. Nothing stops `CREATE FUNCTION
        // sum(text)`, and once one exists every `sum` in the database is a user
        // routine as far as the inliner is concerned; without this arm
        // `sum(int4)` stops resolving to the built-in aggregate and reports
        // 42883 where PostgreSQL still sums.
        || crate::agg::is_builtin_aggregate_name(name)
        || crate::window::is_window_only_function(name)
}

fn is_regression_c_entrypoint(routine: &Routine, symbol: &str) -> bool {
    routine.language == "c"
        && routine
            .object_file
            .as_deref()
            .map(std::path::Path::new)
            .and_then(std::path::Path::file_stem)
            .is_some_and(|stem| stem == "regress")
        && routine.body == symbol
}

/// `pg_type.oid` of `oid` itself, as `pg_proc.proargtypes` records it.
const OID_TYPE_OID: i32 = 26;

fn is_regression_binary_coercible(routine: &Routine) -> bool {
    routine.name.eq_ignore_ascii_case("binary_coercible")
        && is_regression_c_entrypoint(routine, "binary_coercible")
        // `regress.so` declares it `binary_coercible(oid, oid)`. This read 23
        // (int4) while `oid` was an alias for `int4`; now that `oid` is its own
        // type the parameters carry 26, and the old constant would silently
        // stop recognising the helper.
        && routine
            .input_params()
            .map(|param| type_oid(&param.ty))
            .eq([OID_TYPE_OID, OID_TYPE_OID])
        && matches!(
            &routine.result,
            RoutineResult::Type { ty, setof: false }
                if type_oid(ty) == 16
        )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegressionCAdapter {
    PglzCompress,
    PglzDecompress,
    InterptPp,
    PointInWidget,
    CatalogTextUniqueIndexOid,
}

fn has_exact_regression_c_signature(
    routine: &Routine,
    name: &str,
    params: &[ColumnType],
    result: ColumnType,
) -> bool {
    routine.kind == RoutineKind::Function
        && routine.name.eq_ignore_ascii_case(name)
        && is_regression_c_entrypoint(routine, name)
        && routine.strict
        && routine.params.len() == params.len()
        && routine.params.iter().zip(params).all(|(param, ty)| {
            param.mode == ParamMode::In && param.default.is_none() && param.ty.column == Some(*ty)
        })
        && matches!(
            &routine.result,
            RoutineResult::Type { ty, setof: false }
                if ty.column == Some(result)
        )
}

fn regression_c_adapter(routine: &Routine) -> Option<RegressionCAdapter> {
    if has_exact_regression_c_signature(
        routine,
        "test_pglz_compress",
        &[ColumnType::Bytea],
        ColumnType::Bytea,
    ) {
        Some(RegressionCAdapter::PglzCompress)
    } else if has_exact_regression_c_signature(
        routine,
        "test_pglz_decompress",
        &[ColumnType::Bytea, ColumnType::Int4, ColumnType::Bool],
        ColumnType::Bytea,
    ) {
        Some(RegressionCAdapter::PglzDecompress)
    } else if has_exact_regression_c_signature(
        routine,
        "interpt_pp",
        &[ColumnType::Path, ColumnType::Path],
        ColumnType::Point,
    ) {
        Some(RegressionCAdapter::InterptPp)
    } else if is_regression_point_in_widget(routine) {
        Some(RegressionCAdapter::PointInWidget)
    } else if routine.kind == RoutineKind::Function
        && routine
            .name
            .eq_ignore_ascii_case("is_catalog_text_unique_index_oid")
        && is_regression_c_entrypoint(routine, "is_catalog_text_unique_index_oid")
        && routine.strict
        && routine.params.len() == 1
        && routine.params[0].mode == ParamMode::In
        && routine.params[0].default.is_none()
        && routine.params[0].ty.column == Some(ColumnType::Oid)
        && matches!(&routine.result, RoutineResult::Type { ty, setof: false } if ty.column == Some(ColumnType::Bool))
    {
        Some(RegressionCAdapter::CatalogTextUniqueIndexOid)
    } else {
        None
    }
}

fn is_regression_point_in_widget(routine: &Routine) -> bool {
    routine.kind == RoutineKind::Function
        && routine.name.eq_ignore_ascii_case("pt_in_widget")
        && is_regression_c_entrypoint(routine, "pt_in_widget")
        && routine.strict
        && routine.params.len() == 2
        && routine.params[0].mode == ParamMode::In
        && routine.params[0].default.is_none()
        && routine.params[0].ty.column == Some(ColumnType::Point)
        && routine.params[1].mode == ParamMode::In
        && routine.params[1].default.is_none()
        && routine.params[1].ty.name.eq_ignore_ascii_case("widget")
        && matches!(&routine.result, RoutineResult::Type { ty, setof: false } if ty.column == Some(ColumnType::Bool))
}

fn pglz_internal_error(message: &'static str) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "XX000",
        message: message.into(),
    }
}

fn pglz_output_limit_error() -> ExecError {
    ExecError::FunctionError {
        sqlstate: "54000",
        message: format!(
            "requested PGLZ output exceeds the {} MiB safety limit",
            MAX_PGLZ_OUTPUT_BYTES / (1024 * 1024)
        ),
    }
}

fn interpt_pp(
    left: &crabka_pgtypes::Path,
    right: &crabka_pgtypes::Path,
) -> Option<crabka_pgtypes::Point> {
    left.points.windows(2).find_map(|left| {
        right.points.windows(2).find_map(|right| {
            left[0]
                .lseg_with(left[1])
                .intersection_point(right[0].lseg_with(right[1]))
        })
    })
}

fn eval_regression_c_adapter(
    adapter: RegressionCAdapter,
    values: &[Datum],
) -> Result<Datum, ExecError> {
    match (adapter, values) {
        (RegressionCAdapter::PglzCompress, [Datum::Bytea(source)]) => {
            Ok(pglz::compress(source, &pglz::Strategy::ALWAYS).map_or(Datum::Null, Datum::Bytea))
        }
        (
            RegressionCAdapter::PglzDecompress,
            [
                Datum::Bytea(source),
                Datum::Int4(rawsize),
                Datum::Bool(check_complete),
            ],
        ) => {
            let rawsize = usize::try_from(*rawsize)
                .map_err(|_| pglz_internal_error("rawsize must not be negative"))?;
            if rawsize > MAX_PGLZ_OUTPUT_BYTES {
                return Err(pglz_output_limit_error());
            }
            let mut output = Vec::new();
            output
                .try_reserve_exact(rawsize)
                .map_err(|_| pglz_output_limit_error())?;
            output.resize(rawsize, 0);
            let written = pglz::decompress_into(source, &mut output, *check_complete)
                .ok_or_else(|| pglz_internal_error("pglz_decompress failed"))?;
            output.truncate(written);
            Ok(Datum::Bytea(output))
        }
        (RegressionCAdapter::InterptPp, [Datum::Path(left), Datum::Path(right)]) => {
            Ok(interpt_pp(left, right).map_or(Datum::Null, Datum::Point))
        }
        (RegressionCAdapter::PointInWidget, [Datum::Point(point), Datum::Text(widget)]) => {
            crate::usertype::regression_widget_contains(*point, widget).map(Datum::Bool)
        }
        (RegressionCAdapter::CatalogTextUniqueIndexOid, [Datum::Oid(oid)]) => {
            Ok(Datum::Bool(matches!(*oid, 3593 | 3597 | 6002 | 6246)))
        }
        (RegressionCAdapter::CatalogTextUniqueIndexOid, [Datum::Int4(oid)]) => {
            Ok(Datum::Bool(matches!(*oid, 3593 | 3597 | 6002 | 6246)))
        }
        _ => Err(ExecError::TypeMismatch(
            "regression C adapter received values outside its pinned signature".into(),
        )),
    }
}

fn falls_back_to_regression_binary_coercible(kv: &dyn Kv, name: &str, given: &[ArgType]) -> bool {
    if !name.eq_ignore_ascii_case("binary_coercible")
        || !routines_named(kv, name)
            .is_ok_and(|routines| routines.iter().any(is_regression_binary_coercible))
    {
        return false;
    }
    match resolve_call(kv, name, given) {
        Ok(Some(routine)) => is_regression_binary_coercible(&routine),
        Ok(None) | Err(ExecError::UndefinedFunction(_)) => true,
        Err(_) => false,
    }
}

/// The argument types a call carries, as far as they can be known without the
/// caller's scope.
///
/// A column reference resolves to [`ArgType::Opaque`], which matches any
/// candidate. So literal arguments disambiguate overloads, and a name that
/// carries exactly one overload always resolves.
fn best_effort_arg_types(args: &[Expr]) -> Vec<ArgType> {
    args.iter()
        .map(|arg| {
            if crate::func::is_unknown_arg(arg) {
                ArgType::Unknown
            } else {
                crate::eval::infer_type(arg, &crate::scope::Scope::empty())
                    .map_or(ArgType::Opaque, ArgType::Known)
            }
        })
        .collect()
}

/// Inline a call of a user-defined SQL function, if `call` names one.
///
/// This is the single seam through which a routine call becomes ordinary SQL.
/// The expression walkers that rewrite a statement before execution call it, so
/// both the value path and the describe path see the same rewritten tree.
///
/// # Errors
///
/// Propagates catalog read errors, and the routine model's own refusals.
pub(crate) fn inline_scalar(kv: &dyn Kv, call: &FuncCall) -> Result<Option<Expr>, ExecError> {
    let FuncArgs::Exprs(args) = &call.args else {
        return Ok(None);
    };
    if !is_user_routine(kv, &call.name) {
        return Ok(None);
    }
    // A user aggregate of this name and arity owns the call: `agg` evaluates it.
    // Without this the inliner would bind a same-named ordinary function --
    // `CREATE AGGREGATE acc(int4)` next to `CREATE FUNCTION acc(text)` -- and
    // the query would silently return one row per input instead of aggregating.
    if routines_named(kv, &call.name)?
        .iter()
        .any(|routine| routine.is_aggregate() && routine.input_params().count() == args.len())
    {
        return Ok(None);
    }
    let given = best_effort_arg_types(args);
    // Procedural and SQL-language routines execute through the scalar runtime
    // rather than inlining. Keeping the call node intact evaluates its
    // arguments once for each input row before the body receives their values.
    match resolve_call(kv, &call.name, &given) {
        Ok(Some(routine))
            if routine.kind == RoutineKind::Function
                && (matches!(routine.language.as_str(), "plpgsql" | "sql")
                    || regression_c_adapter(&routine).is_some()) =>
        {
            return Ok(None);
        }
        Err(_)
            if routines_named(kv, &call.name)?
                .iter()
                .all(|routine| matches!(routine.language.as_str(), "plpgsql" | "sql")) =>
        {
            return Ok(None);
        }
        Err(_) if known_builtin(&call.name) => return Ok(None),
        Ok(_) => {}
        Err(error) => return Err(error),
    }
    match inline_scalar_call(kv, call, &given) {
        Ok(inlined) => Ok(inlined),
        Err(_) if known_builtin(&call.name) => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) fn plpgsql_declared_call_type(
    kv: &dyn Kv,
    call: &FuncCall,
) -> Result<Option<ColumnType>, ExecError> {
    let FuncArgs::Exprs(args) = &call.args else {
        return Ok(None);
    };
    if !is_user_routine(kv, &call.name) {
        return Ok(None);
    }
    let given = best_effort_arg_types(args);
    let routine = match resolve_call(kv, &call.name, &given) {
        Ok(Some(routine))
            if matches!(routine.language.as_str(), "plpgsql" | "sql")
                || regression_c_adapter(&routine).is_some() =>
        {
            routine
        }
        Ok(_) => return Ok(None),
        Err(_) if known_builtin(&call.name) => return Ok(None),
        Err(error) => return Err(error),
    };
    if routine.language == "plpgsql" {
        validate_plpgsql_scalar(&routine)?;
    }
    called_scalar_result_type_with_catalog(kv, &routine, &given)?
        .ok_or_else(|| {
            ExecError::Unsupported(format!(
                "function {} has no scalar result type",
                routine.identity()
            ))
        })
        .map(Some)
}

/// Resolve the declared type of a PL/pgSQL scalar call while a statement
/// runtime is installed. `None` lets the ordinary built-in/aggregate resolver
/// keep its existing error and overload fallback behavior.
pub(crate) fn plpgsql_scalar_result_type(
    call: &FuncCall,
    scope: &crate::scope::Scope,
) -> Option<Result<ColumnType, ExecError>> {
    let FuncArgs::Exprs(args) = &call.args else {
        return None;
    };
    SCALAR_RUNTIME.with(|runtime| {
        let runtime = runtime.borrow();
        let runtime = runtime.as_ref()?;
        if !is_user_routine(runtime.catalog.as_ref(), &call.name) {
            return None;
        }
        if shadowing_user_aggregate(runtime.catalog.as_ref(), &call.name, args.len()) {
            return None;
        }
        let given = crate::eval::static_arg_types(args, scope);
        if given.as_ref().is_ok_and(|given| {
            falls_back_to_regression_binary_coercible(runtime.catalog.as_ref(), &call.name, given)
        }) {
            return None;
        }
        let result = given.and_then(|given| {
            let Some(routine) = resolve_call(runtime.catalog.as_ref(), &call.name, &given)? else {
                return Err(undefined_routine(format!(
                    "function {} does not exist",
                    call.name
                )));
            };
            let adapter = regression_c_adapter(&routine);
            if !matches!(routine.language.as_str(), "plpgsql" | "sql") && adapter.is_none() {
                return Err(undefined_routine(format!(
                    "function {} does not exist",
                    call.name
                )));
            }
            if matches!(routine.language.as_str(), "plpgsql" | "sql") {
                validate_plpgsql_scalar(&routine)?;
            }
            called_scalar_result_type_with_catalog(runtime.catalog.as_ref(), &routine, &given)?
                .ok_or_else(|| {
                    ExecError::Unsupported(format!(
                        "function {} has no scalar result type",
                        routine.identity()
                    ))
                })
        });
        Some(result)
    })
}

/// Resolve the one column a set-returning PL/pgSQL call produces in a select
/// list. The table-function path owns multi-column results; a select list has
/// one expression slot for this call.
pub(crate) fn plpgsql_set_result_type(
    call: &FuncCall,
    scope: &crate::scope::Scope,
) -> Option<Result<ColumnType, ExecError>> {
    let FuncArgs::Exprs(args) = &call.args else {
        return None;
    };
    SCALAR_RUNTIME.with(|runtime| {
        let runtime = runtime.borrow();
        let runtime = runtime.as_ref()?;
        if !is_user_routine(runtime.catalog.as_ref(), &call.name) {
            return None;
        }
        let result = (|| {
            let given = crate::eval::static_arg_types(args, scope)?;
            let routine =
                resolve_call(runtime.catalog.as_ref(), &call.name, &given)?.ok_or_else(|| {
                    undefined_routine(format!("function {} does not exist", call.name))
                })?;
            if !matches!(routine.language.as_str(), "plpgsql" | "sql")
                || !declared_returns_set(&routine)
            {
                return Err(undefined_routine(format!(
                    "function {} does not exist",
                    call.name
                )));
            }
            if routine.kind != RoutineKind::Function {
                return Err(wrong_routine_kind(format!(
                    "{} is a procedure\nHINT:  To call a procedure, use CALL.",
                    spelled_signature(&routine)
                )));
            }
            let columns = set_result_columns(&routine, &given)?;
            Ok(if columns.len() == 1 {
                columns[0].1
            } else {
                ColumnType::Record(None)
            })
        })();
        Some(result)
    })
}

/// Whether the current statement runtime resolves `call` to a set-returning
/// PL/pgSQL function. This cheap predicate selects ProjectSet; the typed
/// resolver above returns the user-facing error.
pub(crate) fn is_plpgsql_set_runtime(call: &FuncCall) -> bool {
    let FuncArgs::Exprs(args) = &call.args else {
        return false;
    };
    SCALAR_RUNTIME.with(|runtime| {
        let runtime = runtime.borrow();
        let Some(runtime) = runtime.as_ref() else {
            return false;
        };
        let given = best_effort_arg_types(args);
        resolve_call(runtime.catalog.as_ref(), &call.name, &given).is_ok_and(|routine| {
            routine.is_some_and(|routine| {
                matches!(routine.language.as_str(), "plpgsql" | "sql")
                    && declared_returns_set(&routine)
            })
        })
    })
}

pub(crate) fn is_plpgsql_scalar_runtime(call: &FuncCall, scope: &crate::scope::Scope) -> bool {
    let FuncArgs::Exprs(args) = &call.args else {
        return false;
    };
    SCALAR_RUNTIME.with(|runtime| {
        let runtime = runtime.borrow();
        let Some(runtime) = runtime.as_ref() else {
            return false;
        };
        let Ok(given) = crate::eval::static_arg_types(args, scope) else {
            return false;
        };
        resolve_call(runtime.catalog.as_ref(), &call.name, &given).is_ok_and(|routine| {
            routine.is_some_and(|routine| {
                matches!(routine.language.as_str(), "plpgsql" | "sql")
                    || regression_c_adapter(&routine).is_some()
            })
        })
    })
}

/// Evaluate a PL/pgSQL scalar call against the current input row. This method
/// evaluates the arguments exactly once before it binds the parameters. The
/// procedural body then uses ordinary scalar evaluation, so nested calls and
/// lazy SQL conditionals keep the caller's semantics.
pub(crate) fn eval_plpgsql_scalar(
    call: &FuncCall,
    scope: &crate::scope::Scope,
    row: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> Option<Result<Datum, ExecError>> {
    eval_plpgsql_scalar_with(call, ctx, |arg| crate::eval::eval(arg, scope, row, ctx))
}

pub(crate) fn eval_plpgsql_scalar_with(
    call: &FuncCall,
    ctx: &crate::clock::EvalCtx,
    mut eval_arg: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Option<Result<Datum, ExecError>> {
    let FuncArgs::Exprs(args) = &call.args else {
        return None;
    };
    SCALAR_RUNTIME.with(|runtime| {
        let runtime = runtime.borrow();
        let runtime = runtime.as_ref()?.clone();
        if !is_user_routine(runtime.catalog.as_ref(), &call.name) {
            return None;
        }
        if shadowing_user_aggregate(runtime.catalog.as_ref(), &call.name, args.len()) {
            return None;
        }
        let result = (|| {
            if call.distinct || call.filter.is_some() {
                return Err(ExecError::Syntax(format!(
                    "FILTER or DISTINCT is not allowed for function {}",
                    call.name
                )));
            }
            let mut values = args
                .iter()
                .map(&mut eval_arg)
                .collect::<Result<Vec<_>, _>>()?;
            let given = crate::eval::value_arg_types(args, &values);
            if falls_back_to_regression_binary_coercible(
                runtime.catalog.as_ref(),
                &call.name,
                &given,
            ) {
                let mut values = values.into_iter();
                return crate::func::eval_scalar(call, None, ctx, |_| {
                    values.next().ok_or_else(|| {
                        ExecError::Unsupported("binary_coercible argument is missing".into())
                    })
                });
            }
            let Some(BoundRoutineCall {
                routine,
                args: bound_args,
            }) = bind_call(runtime.catalog.as_ref(), &call.name, args, &given)?
            else {
                return Err(undefined_routine(format!(
                    "function {} does not exist",
                    call.name
                )));
            };
            let adapter = regression_c_adapter(&routine);
            if !matches!(routine.language.as_str(), "plpgsql" | "sql") && adapter.is_none() {
                return Err(undefined_routine(format!(
                    "function {} does not exist",
                    call.name
                )));
            }
            if matches!(routine.language.as_str(), "plpgsql" | "sql") {
                validate_plpgsql_scalar(&routine)?;
            }
            for default in bound_args.iter().skip(values.len()) {
                values.push(eval_arg(default)?);
            }
            pack_variadic_values(&routine, args, &mut values, ctx)?;
            let params = routine
                .input_params()
                .map(|param| param.ty.column)
                .collect::<Vec<_>>();
            crate::eval::coerce_unknown_args(&bound_args, &mut values, &params, ctx)?;
            if routine.strict && values.iter().any(Datum::is_null) {
                return Ok(Datum::Null);
            }
            if let Some(adapter) = adapter {
                return eval_regression_c_adapter(adapter, &values);
            }
            let _guard = enter_plpgsql_call()?;
            let value = if routine.language == "sql"
                || crate::plpgsql::scalar_function_requires_session(
                    runtime.catalog.as_ref(),
                    &routine,
                )? {
                let requests = runtime.requests.ok_or_else(|| {
                    ExecError::Unsupported(
                        "SQL-bearing PL/pgSQL function requires a session executor".into(),
                    )
                })?;
                let (reply, response) = std::sync::mpsc::channel();
                requests
                    .try_send(ScalarFunctionRequest {
                        routine: Some(routine.clone()),
                        values: values.clone(),
                        kind: FunctionRequestKind::Scalar,
                        command_row_claims: scalar_runtime_command_row_claims(),
                        reply,
                    })
                    .map_err(|_| {
                        ExecError::ObjectNotInPrerequisiteState(
                            "PL/pgSQL function executor stopped".into(),
                        )
                    })?;
                let (result, mutations) = response.recv().map_err(|_| {
                    ExecError::ObjectNotInPrerequisiteState(
                        "PL/pgSQL function executor stopped".into(),
                    )
                })??;
                crate::session::apply_guc_runtime_mutations(mutations)?;
                match result {
                    FunctionRequestResult::Scalar(value) => value,
                    FunctionRequestResult::Table(_) => {
                        return Err(ExecError::ObjectNotInPrerequisiteState(
                            "PL/pgSQL function executor returned rows for a scalar call".into(),
                        ));
                    }
                }
            } else {
                crate::plpgsql::eval_scalar_function(&routine, &values, ctx)?
            };
            match called_scalar_result_type(&routine, &given) {
                Some(ty) => crate::plpgsql::cast_value(&value, ty, ctx),
                None => Ok(value),
            }
        })();
        Some(result)
    })
}

/// Expand one set-returning PL/pgSQL call in a select list.
pub(crate) fn eval_plpgsql_set_function(
    call: &FuncCall,
    scope: &crate::scope::Scope,
    row: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> Option<Result<Vec<Datum>, ExecError>> {
    let FuncArgs::Exprs(args) = &call.args else {
        return None;
    };
    SCALAR_RUNTIME.with(|runtime| {
        let runtime = runtime.borrow();
        let runtime = runtime.as_ref()?.clone();
        if !is_user_routine(runtime.catalog.as_ref(), &call.name) {
            return None;
        }
        let result = (|| {
            if call.distinct || call.filter.is_some() {
                return Err(ExecError::Syntax(format!(
                    "FILTER or DISTINCT is not allowed for function {}",
                    call.name
                )));
            }
            let mut values = args
                .iter()
                .map(|arg| crate::eval::eval(arg, scope, row, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            let given = crate::eval::value_arg_types(args, &values);
            let BoundRoutineCall {
                routine,
                args: bound_args,
            } = bind_call(runtime.catalog.as_ref(), &call.name, args, &given)?.ok_or_else(
                || undefined_routine(format!("function {} does not exist", call.name)),
            )?;
            if !matches!(routine.language.as_str(), "plpgsql" | "sql")
                || !declared_returns_set(&routine)
            {
                return Err(undefined_routine(format!(
                    "function {} does not exist",
                    call.name
                )));
            }
            if routine.kind != RoutineKind::Function {
                return Err(wrong_routine_kind(format!(
                    "{} is a procedure\nHINT:  To call a procedure, use CALL.",
                    spelled_signature(&routine)
                )));
            }
            let columns = set_result_columns(&routine, &given)?;
            let names = Arc::from(
                columns
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect::<Vec<_>>(),
            );
            for default in bound_args.iter().skip(values.len()) {
                values.push(crate::eval::eval(default, scope, row, ctx)?);
            }
            pack_variadic_values(&routine, args, &mut values, ctx)?;
            let params = routine
                .input_params()
                .map(|param| param.ty.column)
                .collect::<Vec<_>>();
            crate::eval::coerce_unknown_args(&bound_args, &mut values, &params, ctx)?;
            if routine.strict && values.iter().any(Datum::is_null) {
                return Ok(Vec::new());
            }
            let _guard = enter_plpgsql_call()?;
            let requests = runtime.requests.ok_or_else(|| {
                ExecError::Unsupported("PL/pgSQL table function requires a session executor".into())
            })?;
            let (reply, response) = std::sync::mpsc::channel();
            requests
                .try_send(ScalarFunctionRequest {
                    routine: Some(routine),
                    values,
                    kind: FunctionRequestKind::Table(columns.clone()),
                    command_row_claims: scalar_runtime_command_row_claims(),
                    reply,
                })
                .map_err(|_| {
                    ExecError::ObjectNotInPrerequisiteState(
                        "PL/pgSQL function executor stopped".into(),
                    )
                })?;
            let (result, mutations) = response.recv().map_err(|_| {
                ExecError::ObjectNotInPrerequisiteState("PL/pgSQL function executor stopped".into())
            })??;
            crate::session::apply_guc_runtime_mutations(mutations)?;
            match result {
                FunctionRequestResult::Scalar(value) => Ok(vec![value]),
                FunctionRequestResult::Table(rows) => rows
                    .into_iter()
                    .map(|row| {
                        if columns.is_empty() || columns.len() == 1 {
                            return match row.as_slice() {
                                [value] => Ok(value.clone()),
                                _ => Err(ExecError::ObjectNotInPrerequisiteState(
                                    "PL/pgSQL function executor returned the wrong table width"
                                        .into(),
                                )),
                            };
                        }
                        if row.len() != columns.len() {
                            return Err(ExecError::ObjectNotInPrerequisiteState(
                                "PL/pgSQL function executor returned the wrong table width".into(),
                            ));
                        }
                        Ok(Datum::Record(crabka_pgtypes::RecordValue::named(
                            None,
                            Arc::clone(&names),
                            row,
                        )))
                    })
                    .collect(),
            }
        })();
        Some(result)
    })
}

fn set_result_columns(
    routine: &Routine,
    given: &[ArgType],
) -> Result<Vec<(String, ColumnType)>, ExecError> {
    if let Some(column) = single_set_result_column(routine, given) {
        return Ok(vec![column]);
    }
    if let RoutineResult::Table(columns) = &routine.result {
        return columns
            .iter()
            .map(|(name, ty)| {
                ty.column
                    .or_else(|| resolved_polymorphic_type(routine, given, &ty.name))
                    .map(|ty| (name.clone(), ty))
                    .ok_or_else(|| {
                        ExecError::Unsupported(format!(
                            "set-returning function {} has unsupported result type {}",
                            routine.identity(),
                            ty.name
                        ))
                    })
            })
            .collect();
    }
    let columns = routine
        .output_params()
        .enumerate()
        .map(|(index, param)| {
            param
                .ty
                .column
                .or_else(|| resolved_polymorphic_type(routine, given, &param.ty.name))
                .map(|ty| {
                    (
                        param
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("column{}", index + 1)),
                        ty,
                    )
                })
                .ok_or_else(|| {
                    ExecError::Unsupported(format!(
                        "set-returning function {} has unsupported result type {}",
                        routine.identity(),
                        param.ty.name
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !columns.is_empty()
        || matches!(&routine.result, RoutineResult::Type { ty, setof: true } if is_record_type(ty))
    {
        return Ok(columns);
    }
    Err(ExecError::Unsupported(format!(
        "set-returning function {} has no select-list result",
        routine.identity()
    )))
}

fn validate_plpgsql_scalar(routine: &Routine) -> Result<(), ExecError> {
    if routine.kind != RoutineKind::Function {
        return Err(wrong_routine_kind(format!(
            "{} is a procedure\nHINT:  To call a procedure, use CALL.",
            spelled_signature(routine)
        )));
    }
    if declared_returns_set(routine) {
        return Err(ExecError::Unsupported(format!(
            "set-returning function {} is only supported in FROM position",
            routine.identity()
        )));
    }
    if declared_output_parameter_count(routine) > 1 && routine.language != "sql" {
        return Err(ExecError::Unsupported(format!(
            "{} returns a record; only FROM position is supported for a routine with several OUT parameters",
            routine.identity()
        )));
    }
    Ok(())
}

/// The parsed body of a `LANGUAGE sql` routine, as a statement list.
pub(crate) fn parse_body(routine: &Routine) -> Result<Vec<Statement>, ExecError> {
    let source = match routine.body_form {
        BodyForm::Return => format!("SELECT {}", routine.body),
        BodyForm::Source => routine.body.clone(),
        BodyForm::Atomic => atomic_execution_source(&routine.body),
    };
    crabka_pgparser::parse(&source).map_err(|error| ExecError::Syntax(error.message))
}

/// Parse-analyze the `RETURN expression` form of a SQL routine at definition
/// time.  Its parameters are query variables, so substitute them with typed
/// synthetic columns before reusing the ordinary expression analyzer.
fn check_sql_body(routine: &Routine, check_function_bodies: bool) -> Result<(), ExecError> {
    if routine.body_form == BodyForm::Source && !check_function_bodies {
        return Ok(());
    }
    let statements = parse_body(routine)?;
    match routine.body_form {
        BodyForm::Return => check_sql_return_body(routine, &statements),
        BodyForm::Source => check_sql_source_body(routine, &statements),
        BodyForm::Atomic => Ok(()),
    }
}

fn check_sql_return_body(routine: &Routine, statements: &[Statement]) -> Result<(), ExecError> {
    let [Statement::Query(query)] = statements else {
        return Ok(());
    };
    let Some(body) = scalar_body(query) else {
        return Ok(());
    };
    let Some((scope, args)) = routine_argument_scope(routine) else {
        return Ok(());
    };
    let binding = Binding {
        routine,
        args,
        uses: RefCell::new(vec![0; routine.input_params().count()]),
    };
    let body = substitute(&binding, body, true)?;
    if matches!(body, Expr::ScalarSubquery(_)) {
        // Subquery scope is resolved only by the query executor.  Asking the
        // scalar expression type checker to resolve it at CREATE time produces
        // an internal error before the subquery has a relation scope.
        return Ok(());
    }
    crate::eval::check_predicate_resolves(&body, &scope)
}

fn check_sql_source_body(routine: &Routine, statements: &[Statement]) -> Result<(), ExecError> {
    let Some(expected) = declared_scalar_result_type(routine) else {
        return Ok(());
    };
    // A record return is described by the complete projection, not one scalar
    // expression. Leave it to the existing call-site record binder.
    if matches!(expected, ColumnType::Record(_)) {
        return Ok(());
    }
    let Some(last) = statements.last() else {
        return Err(sql_return_mismatch(
            routine,
            "Function's final statement must be SELECT or INSERT/UPDATE/DELETE/MERGE RETURNING.",
        ));
    };
    let Statement::Query(query) = last else {
        // The executor already recognizes RETURNING for data-modifying bodies;
        // validating its projected type here would duplicate that full path.
        return Ok(());
    };
    let crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Select(select)) =
        &query.body
    else {
        return Ok(());
    };
    if !select.from.is_empty() {
        return Ok(());
    }
    let [SelectItem::Expr { expr, .. }] = select.projection.as_slice() else {
        return Err(sql_return_mismatch(
            routine,
            "Final statement must return exactly one column.",
        ));
    };
    let Some((scope, args)) = routine_argument_scope(routine) else {
        return Ok(());
    };
    let binding = Binding {
        routine,
        args,
        uses: RefCell::new(vec![0; routine.input_params().count()]),
    };
    let expr = substitute(&binding, expr, true).map_err(source_parameter_error)?;
    // PostgreSQL resolves a directly recursive call while the new routine is
    // visible to its body checker. Gres stores it after this lightweight check.
    if is_direct_self_call(routine, &expr) {
        return Ok(());
    }
    crate::eval::check_predicate_resolves(&expr, &scope)?;
    let actual = crate::eval::infer_type(&expr, &scope)?;
    if actual != expected
        && !implicitly_coercible(actual, expected)
        && !matches!(expr, Expr::NullLiteral)
    {
        return Err(sql_return_mismatch(
            routine,
            format!("Actual return type is {}.", actual.name()),
        ));
    }
    Ok(())
}

fn is_direct_self_call(routine: &Routine, expr: &Expr) -> bool {
    matches!(expr, Expr::Func(FuncCall { name, args: FuncArgs::Exprs(args), .. })
        if name == &routine.name && args.len() == routine.input_params().count())
}

fn routine_argument_scope(routine: &Routine) -> Option<(crate::scope::Scope, Vec<Expr>)> {
    let params: Vec<&RoutineParam> = routine.input_params().collect();
    let types = params
        .iter()
        .map(|param| param.ty.column)
        .collect::<Option<Vec<_>>>()?;
    let names: Vec<String> = types
        .iter()
        .enumerate()
        .map(|(index, _)| format!("__routine_arg_{index}"))
        .collect();
    let scope = crate::scope::Scope {
        columns: names
            .iter()
            .zip(types)
            .map(|(name, ty)| crate::scope::ColumnBinding {
                qualifier: None,
                name: name.clone(),
                ty,
                exposure: crate::scope::Exposure::Output,
            })
            .collect(),
        ..Default::default()
    };
    let args = names
        .into_iter()
        .map(|name| Expr::Column { table: None, name })
        .collect();
    Some((scope, args))
}

fn source_parameter_error(error: ExecError) -> ExecError {
    match error {
        ExecError::Syntax(message) if message.starts_with("there is no parameter $") => {
            ExecError::Remote(crabka_pgwire::error::PgError::error("42P02", message))
        }
        error => error,
    }
}

fn sql_return_mismatch(routine: &Routine, detail: impl Into<String>) -> ExecError {
    let expected = declared_scalar_result_type(routine)
        .expect("only scalar functions request SQL return validation");
    ExecError::Remote(
        crabka_pgwire::error::PgError::error(
            "42P13",
            format!(
                "return type mismatch in function declared to return {}",
                expected.name()
            ),
        )
        .with_detail(detail)
        .with_context(format!("SQL function \"{}\"", routine.name)),
    )
}

/// The run-time error for an empty quoted SQL body created while
/// `check_function_bodies` was disabled.
pub(crate) fn sql_empty_body_error(routine: &Routine, values: &[Datum]) -> ExecError {
    let given = routine
        .input_params()
        .zip(values)
        .map(|(param, value)| {
            value
                .column_type()
                .or(param.ty.column)
                .map(crate::eval::ArgType::Known)
                .unwrap_or(crate::eval::ArgType::Opaque)
        })
        .collect::<Vec<_>>();
    let expected = resolved_scalar_result_type(routine, &given)
        .map(|ty| ty.name().to_string())
        .or_else(|| match &routine.result {
            RoutineResult::Type { ty, .. } => Some(ty.name.clone()),
            RoutineResult::Table(_) | RoutineResult::Unspecified => None,
        })
        .unwrap_or_else(|| "record".into());
    ExecError::Remote(
        crabka_pgwire::error::PgError::error(
            "42P13",
            format!("return type mismatch in function declared to return {expected}"),
        )
        .with_detail(
            "Function's final statement must be SELECT or INSERT/UPDATE/DELETE/MERGE RETURNING.",
        )
        .with_context(format!("SQL function \"{}\" during startup", routine.name)),
    )
}

/// The executable spelling of an atomic routine body.
///
/// `RETURN expr` is valid only in `BEGIN ATOMIC` and has the same result as
/// the final `SELECT expr`; the catalog retains the original body for display.
fn atomic_execution_source(body: &str) -> String {
    if let Some(expr) = atomic_return_expression(body) {
        format!("SELECT {expr}")
    } else {
        atomic_body_source(body).to_string()
    }
}

fn atomic_body_source(body: &str) -> &str {
    body.trim_start_matches(';').trim_start()
}

fn atomic_return_expression(body: &str) -> Option<&str> {
    let body = atomic_body_source(body);
    if body.len() >= 6
        && body[..6].eq_ignore_ascii_case("return")
        && body[6..].chars().next().is_some_and(char::is_whitespace)
    {
        Some(body[6..].trim())
    } else {
        None
    }
}

/// Parse the stored source of a PL/pgSQL routine.
pub(crate) fn parse_plpgsql_body(routine: &Routine) -> Result<PlPgSqlBlock, ExecError> {
    if routine.body_form != BodyForm::Source {
        return Err(invalid_definition(
            "PL/pgSQL function body must be a string literal",
        ));
    }
    let block = crabka_pgparser::parse_plpgsql(&routine.body).map_err(|error| {
        ExecError::FunctionError {
            sqlstate: error.sqlstate(),
            message: error.message,
        }
    })?;
    if routine.kind == RoutineKind::Procedure && plpgsql_has_return_value(&block) {
        return Err(ExecError::FunctionError {
            sqlstate: "42804",
            message: "RETURN cannot have a parameter in a procedure\nHINT:  Use RETURN without a parameter in a procedure."
                .into(),
        });
    }
    Ok(block)
}

fn plpgsql_has_return_value(block: &PlPgSqlBlock) -> bool {
    fn statements_have_return_value(statements: &[PlPgSqlStatement]) -> bool {
        statements.iter().any(|statement| match statement {
            PlPgSqlStatement::Return(Some(_)) => true,
            PlPgSqlStatement::Block(block) => plpgsql_has_return_value(block),
            PlPgSqlStatement::If {
                branches,
                else_body,
            } => {
                branches
                    .iter()
                    .any(|(_, body)| statements_have_return_value(body))
                    || statements_have_return_value(else_body)
            }
            PlPgSqlStatement::Case {
                arms, else_body, ..
            } => {
                arms.iter()
                    .any(|(_, body)| statements_have_return_value(body))
                    || else_body
                        .as_deref()
                        .is_some_and(statements_have_return_value)
            }
            PlPgSqlStatement::Loop { body, .. } => statements_have_return_value(body),
            _ => false,
        })
    }

    statements_have_return_value(&block.statements)
        || block
            .exceptions
            .iter()
            .any(|handler| statements_have_return_value(&handler.statements))
}

/// A `LANGUAGE sql` routine's final query, the one whose result is the
/// routine's result.
///
/// `PostgreSQL` runs EVERY statement in the body and returns the last one's
/// result. Gres reaches a SQL routine only when it inlines the final query into
/// the calling query, and that cannot run the statements before it. So Gres
/// refuses a body with more than one statement instead of silently losing
/// them. A run of only the last statement would make
/// `INSERT INTO audit VALUES ($1); SELECT $1` return the right answer and drop
/// the write.
fn final_query(routine: &Routine) -> Result<Option<QueryExpr>, ExecError> {
    let statements = parse_body(routine)?;
    if statements.len() > 1 {
        return Err(uncallable(
            routine,
            "a SQL body with several statements needs all of them to run, and Gres reaches a \
             routine only by inlining its final query",
        ));
    }
    match statements.last() {
        Some(Statement::Query(query)) => Ok(Some(query.clone())),
        _ => Ok(None),
    }
}

/// Whether duplicating `arg` during inlining could change what the call does.
///
/// Inlining substitutes the argument EXPRESSION at each parameter reference, so
/// a parameter used twice evaluates its argument twice. `f(nextval('s'))` over
/// a body of `SELECT $1 + $1` would consume two sequence values. A literal or a
/// plain column reference is free to duplicate. Anything that could call a
/// function is not. `PostgreSQL`'s own inliner makes the same check first.
fn unsafe_to_duplicate(arg: &Expr) -> bool {
    match arg {
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BitStringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Param(_)
        | Expr::Column { .. } => false,
        Expr::ArrayLiteral(items) | Expr::Row(items) => items.iter().any(unsafe_to_duplicate),
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => unsafe_to_duplicate(expr),
        Expr::Binary { left, right, .. } => unsafe_to_duplicate(left) || unsafe_to_duplicate(right),
        Expr::Func(call)
            if call.name.eq_ignore_ascii_case("multirange")
                || matches!(
                    ColumnType::from_sql_name(&call.name),
                    Some(ColumnType::Range(_) | ColumnType::Multirange(_))
                ) =>
        {
            match &call.args {
                FuncArgs::Exprs(args) => args.iter().any(unsafe_to_duplicate),
                FuncArgs::Named { positional, named } => positional
                    .iter()
                    .chain(named.iter().map(|(_, arg)| arg))
                    .any(unsafe_to_duplicate),
                FuncArgs::Variadic { positional, array } => positional
                    .iter()
                    .chain(std::iter::once(array.as_ref()))
                    .any(unsafe_to_duplicate),
                FuncArgs::Star => true,
            }
        }
        // Everything else can reach a function call, so treat it as volatile.
        _ => true,
    }
}

/// Refuse a routine Gres can define but not run, naming why.
fn uncallable(routine: &Routine, reason: &str) -> ExecError {
    ExecError::Unsupported(format!(
        "cannot execute {} {}: {reason}",
        routine.kind.word(),
        routine.identity()
    ))
}

/// The reason a routine cannot be called, if there is one.
fn callable(routine: &Routine) -> Result<(), ExecError> {
    if routine.language != "sql" {
        return Err(uncallable(
            routine,
            &format!(
                "Gres has no {} interpreter; the routine is defined and reported by pg_proc, \
                 but calling it is not supported",
                routine.language
            ),
        ));
    }
    Ok(())
}

/// Bind a routine's parameters to the call's argument expressions.
///
/// A body refers to a parameter positionally (`$1`) or by its declared name;
/// both resolve to the same argument expression.
struct Binding<'a> {
    routine: &'a Routine,
    args: Vec<Expr>,
    /// How many times each argument has been substituted into the body so far.
    /// `substitute` counts these on its own traversal, not on a second walk, and
    /// [`Binding::reject_repeated_volatile_args`] checks them once it finishes.
    uses: std::cell::RefCell<Vec<usize>>,
}

impl Binding<'_> {
    /// The expression bound to `$n`, one-based.
    fn positional(&self, index: u32) -> Option<&Expr> {
        let position = usize::try_from(index).ok()?.checked_sub(1)?;
        let bound = self.args.get(position)?;
        self.note_use(position);
        Some(bound)
    }

    fn note_use(&self, position: usize) {
        if let Some(count) = self.uses.borrow_mut().get_mut(position) {
            *count += 1;
        }
    }

    /// Refuse a call whose inlining would evaluate a volatile argument more than
    /// once. Inlining substitutes the argument expression at every parameter
    /// reference, so `f(nextval('s'))` over a body of `SELECT $1 + $1` would
    /// consume two sequence values and return their sum. PostgreSQL's inliner
    /// makes the same check. Here there is no non-inlined path to fall back to,
    /// so Gres refuses the call instead of answering it wrongly.
    fn reject_repeated_volatile_args(&self) -> Result<(), ExecError> {
        for (position, count) in self.uses.borrow().iter().enumerate() {
            if *count > 1 && self.args.get(position).is_some_and(unsafe_to_duplicate) {
                return Err(uncallable(
                    self.routine,
                    "an argument that may not be constant is used more than once in the body, \
                     and inlining would evaluate it once per use",
                ));
            }
        }
        Ok(())
    }

    /// The expression bound to a parameter name.
    fn named(&self, name: &str) -> Option<&Expr> {
        let position = self
            .routine
            .input_params()
            .position(|param| param.name.as_deref() == Some(name))?;
        let bound = self.args.get(position)?;
        self.note_use(position);
        Some(bound)
    }
}

/// Fill in the defaults of the input parameters the call omitted, and coerce
/// the untyped literals `PostgreSQL` would have resolved to the parameter type.
fn bound_args(routine: &Routine, args: &[Expr]) -> Result<Vec<Expr>, ExecError> {
    let params: Vec<&RoutineParam> = routine.input_params().collect();
    let variadic_index = variadic_input_index(&params);
    let fixed = variadic_index.unwrap_or(params.len());
    let mut out = Vec::with_capacity(params.len());
    for (index, param) in params.iter().take(fixed).enumerate() {
        let arg = match args.get(index) {
            Some(arg) => arg.clone(),
            None => param
                .default
                .as_deref()
                .map(crabka_pgparser::parser::parse_expression)
                .transpose()
                .map_err(|error| ExecError::Syntax(error.message))?
                .ok_or_else(|| {
                    undefined_routine(format!("function {} does not exist", routine.name))
                })?,
        };
        out.push(coerce_unknown_argument(arg, param.ty.column));
    }
    if let Some(index) = variadic_index {
        let param = params[index];
        if variadic_expr_arguments_are_expanded(&params, args, index) {
            let element = variadic_element_type(param).ok_or_else(|| {
                ExecError::Unsupported(format!(
                    "variadic parameter of {} must be an array",
                    routine.identity()
                ))
            })?;
            out.push(Expr::ArrayLiteral(
                args[index..]
                    .iter()
                    .cloned()
                    .map(|arg| coerce_unknown_argument(arg, Some(element)))
                    .collect(),
            ));
        } else {
            let arg = args.get(index).cloned().ok_or_else(|| {
                ExecError::Unsupported(format!(
                    "variadic parameter of {} must be an array",
                    routine.identity()
                ))
            })?;
            out.push(coerce_unknown_argument(arg, param.ty.column));
        }
    }
    Ok(out)
}

fn coerce_unknown_argument(arg: Expr, target: Option<ColumnType>) -> Expr {
    match target {
        Some(ty) if crate::func::is_unknown_arg(&arg) => Expr::Cast {
            expr: Box::new(arg),
            ty,
        },
        _ => arg,
    }
}

fn variadic_expr_arguments_are_expanded(
    params: &[&RoutineParam],
    args: &[Expr],
    index: usize,
) -> bool {
    let Some(param) = params.get(index) else {
        return false;
    };
    args.len() != params.len()
        || !matches!(
            (args.get(index), param.ty.column),
            (Some(arg), Some(target))
                if crate::eval::infer_type(arg, &crate::scope::Scope::empty()) == Ok(target)
        )
}

fn pack_variadic_values(
    routine: &Routine,
    args: &[Expr],
    values: &mut Vec<Datum>,
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    let params: Vec<&RoutineParam> = routine.input_params().collect();
    let Some(index) = variadic_input_index(&params) else {
        return Ok(());
    };
    if !variadic_expr_arguments_are_expanded(&params, args, index) {
        return Ok(());
    }
    let element = match params[index].ty.column {
        Some(ColumnType::Array(element)) => element,
        _ => {
            return Err(ExecError::Unsupported(format!(
                "variadic parameter of {} must be an array",
                routine.identity()
            )));
        }
    };
    let mut elements = values.split_off(index);
    let argument_types = vec![Some(element.column_type()); elements.len()];
    crate::eval::coerce_unknown_args(&args[index..], &mut elements, &argument_types, ctx)?;
    values.push(Datum::Array(ArrayValue::new(element, elements)));
    Ok(())
}

/// Arrange positional and labeled arguments in the input parameter order.
/// `None` means this overload cannot accept the supplied labels or arity.
fn bind_named_args(
    routine: &Routine,
    positional: &[Expr],
    named: &[(String, Expr)],
) -> Result<Option<Vec<Expr>>, ExecError> {
    let params: Vec<&RoutineParam> = routine.input_params().collect();
    if positional.len() > params.len() {
        return Ok(None);
    }
    let mut slots = positional.iter().cloned().map(Some).collect::<Vec<_>>();
    slots.resize(params.len(), None);
    for (label, value) in named {
        let Some(index) = params
            .iter()
            .position(|param| param.name.as_deref() == Some(label))
        else {
            return Ok(None);
        };
        if slots[index].is_some() {
            return Err(ExecError::Syntax(format!(
                "argument \"{label}\" of {} specified more than once",
                routine.name
            )));
        }
        slots[index] = Some(value.clone());
    }
    slots
        .into_iter()
        .zip(params)
        .map(|(arg, param)| match arg {
            Some(arg) => Ok(arg),
            None => param
                .default
                .as_deref()
                .map(crabka_pgparser::parser::parse_expression)
                .transpose()
                .map_err(|error| ExecError::Syntax(error.message))?
                .ok_or_else(|| {
                    undefined_routine(format!("function {} does not exist", routine.name))
                }),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// Substitute a routine's parameters into one of its body expressions.
fn substitute(
    binding: &Binding,
    expr: &Expr,
    composite_parameter_fields: bool,
) -> Result<Expr, ExecError> {
    use crabka_pgparser::ast::ArraySubscript;

    let sub = |e: &Expr| substitute(binding, e, composite_parameter_fields);
    let boxed = |e: &Expr| -> Result<Box<Expr>, ExecError> {
        Ok(Box::new(substitute(
            binding,
            e,
            composite_parameter_fields,
        )?))
    };
    let list = |es: &[Expr]| -> Result<Vec<Expr>, ExecError> { es.iter().map(sub).collect() };
    Ok(match expr {
        Expr::Param(index) => binding
            .positional(*index)
            .cloned()
            .ok_or_else(|| ExecError::Syntax(format!("there is no parameter ${index}")))?,
        Expr::Column { table: None, name } => match binding.named(name) {
            Some(bound) => bound.clone(),
            None => expr.clone(),
        },
        Expr::Column {
            table: Some(table),
            name,
        } if composite_parameter_fields => match binding.named(table) {
            Some(bound) => Expr::FieldSelect {
                base: Box::new(bound.clone()),
                field: name.clone(),
            },
            None => expr.clone(),
        },
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BitStringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Default
        | Expr::Const { .. } => expr.clone(),
        Expr::FieldSelect { base, field } => Expr::FieldSelect {
            base: boxed(base)?,
            field: field.clone(),
        },
        Expr::FieldSelectAll(base) => Expr::FieldSelectAll(boxed(base)?),
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: boxed(expr)?,
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: boxed(left)?,
            right: boxed(right)?,
        },
        Expr::Func(call) => Expr::Func(FuncCall {
            sql_syntax: call.sql_syntax,
            name: call.name.clone(),
            distinct: call.distinct,
            args: match &call.args {
                FuncArgs::Star => FuncArgs::Star,
                FuncArgs::Exprs(args) => FuncArgs::Exprs(list(args)?),
                FuncArgs::Named { positional, named } => FuncArgs::Named {
                    positional: list(positional)?,
                    named: named
                        .iter()
                        .map(|(label, arg)| Ok((label.clone(), sub(arg)?)))
                        .collect::<Result<_, ExecError>>()?,
                },
                FuncArgs::Variadic { positional, array } => FuncArgs::Variadic {
                    positional: list(positional)?,
                    array: boxed(array)?,
                },
            },
            // A routine body may aggregate, so its sort keys and its FILTER are
            // substituted into like every other sub-expression. Dropping either
            // would silently change the body's answer rather than fail.
            order_by: call
                .order_by
                .iter()
                .map(|item| {
                    Ok(crabka_pgparser::ast::OrderItem {
                        expr: sub(&item.expr)?,
                        asc: item.asc,
                        nulls_first: item.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>, ExecError>>()?,
            within_group: call.within_group,
            filter: call.filter.as_deref().map(boxed).transpose()?,
        }),
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: boxed(expr)?,
            negated: *negated,
        },
        Expr::InList {
            expr,
            list: items,
            negated,
        } => Expr::InList {
            expr: boxed(expr)?,
            list: list(items)?,
            negated: *negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: boxed(expr)?,
            low: boxed(low)?,
            high: boxed(high)?,
            negated: *negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            kind,
            escape,
        } => Expr::Like {
            expr: boxed(expr)?,
            pattern: boxed(pattern)?,
            negated: *negated,
            kind: *kind,
            escape: escape.as_deref().map(boxed).transpose()?,
        },
        Expr::Case {
            operand,
            whens,
            else_result,
        } => Expr::Case {
            operand: operand.as_deref().map(boxed).transpose()?,
            whens: whens
                .iter()
                .map(|(when, then)| Ok((sub(when)?, sub(then)?)))
                .collect::<Result<Vec<_>, ExecError>>()?,
            else_result: else_result.as_deref().map(boxed).transpose()?,
        },
        Expr::Cast { expr, ty } => Expr::Cast {
            expr: boxed(expr)?,
            ty: *ty,
        },
        Expr::Collate { expr, collation } => Expr::Collate {
            expr: boxed(expr)?,
            collation: collation.clone(),
        },
        Expr::QuantifiedArray {
            expr,
            op,
            all,
            array,
        } => Expr::QuantifiedArray {
            expr: boxed(expr)?,
            op: *op,
            all: *all,
            array: boxed(array)?,
        },
        Expr::ArrayLiteral(items) => Expr::ArrayLiteral(list(items)?),
        Expr::Row(items) => Expr::Row(list(items)?),
        Expr::Subscript { base, index } => Expr::Subscript {
            base: boxed(base)?,
            index: boxed(index)?,
        },
        Expr::ArrayRef { base, subscripts } => Expr::ArrayRef {
            base: boxed(base)?,
            subscripts: subscripts
                .iter()
                .map(|subscript| {
                    Ok(match subscript {
                        ArraySubscript::Index(index) => ArraySubscript::Index(sub(index)?),
                        ArraySubscript::Slice { lower, upper } => ArraySubscript::Slice {
                            lower: lower.as_ref().map(&sub).transpose()?,
                            upper: upper.as_ref().map(&sub).transpose()?,
                        },
                    })
                })
                .collect::<Result<Vec<_>, ExecError>>()?,
        },
        // A nested query inside a routine body would need the parameters
        // substituted through the whole query tree; without parameters to
        // substitute the body is already closed and passes through unchanged.
        Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery { .. }
        | Expr::Quantified { .. }
        | Expr::ArraySubquery(_)
        | Expr::SqlJson(_) => {
            if binding.routine.params.is_empty() {
                expr.clone()
            } else {
                return Err(uncallable(
                    binding.routine,
                    "a parameter cannot be substituted into a subquery or SQL/JSON construct \
                     inside a SQL function body",
                ));
            }
        }
    })
}

/// A SQL routine body that is a single `SELECT <expr>` over no relation. This
/// is the shape that inlines into the caller's own expression.
fn scalar_body(query: &QueryExpr) -> Option<&Expr> {
    let crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Select(select)) =
        &query.body
    else {
        return None;
    };
    let SelectStmt {
        projection,
        from,
        filter,
        group_by,
        having,
        ..
    } = select.as_ref();
    if !from.is_empty()
        || filter.is_some()
        || having.is_some()
        || !group_by.is_empty()
        || !query.order_by.is_empty()
        || query.limit.is_some()
        || query.offset.is_some()
        || projection.len() != 1
    {
        return None;
    }
    match &projection[0] {
        SelectItem::Expr { expr, .. } => Some(expr),
        _ => None,
    }
}

/// Inline a scalar call of a user routine into the caller's expression tree.
///
/// `Ok(None)` means the name carries no routine, so the caller keeps its own
/// error. Anything the routine model cannot express is an error, never a
/// silently wrong value.
pub(crate) fn inline_scalar_call(
    kv: &dyn Kv,
    call: &FuncCall,
    given: &[ArgType],
) -> Result<Option<Expr>, ExecError> {
    let FuncArgs::Exprs(args) = &call.args else {
        return Ok(None);
    };
    let Some(BoundRoutineCall { routine, args }) = bind_call(kv, &call.name, args, given)? else {
        return Ok(None);
    };
    if routine.kind == RoutineKind::Procedure {
        return Err(wrong_routine_kind(format!(
            "{} is a procedure\nHINT:  To call a procedure, use CALL.",
            spelled_signature(&routine)
        )));
    }
    callable(&routine)?;
    if declared_returns_set(&routine) {
        return Err(ExecError::Unsupported(format!(
            "set-returning function {} is only supported in FROM position",
            routine.identity()
        )));
    }
    if declared_output_parameter_count(&routine) > 1 {
        if routine.language == "sql" {
            // The session executor packs SQL OUT columns into one record.
            return Ok(None);
        }
        return Err(ExecError::Unsupported(format!(
            "{} returns a record; only FROM position is supported for a routine with several \
             OUT parameters",
            routine.identity()
        )));
    }
    let binding = Binding {
        routine: &routine,
        uses: std::cell::RefCell::new(vec![0; args.len()]),
        args,
    };
    let Some(query) = final_query(&routine)? else {
        return Err(uncallable(
            &routine,
            "a SQL function's final statement must be a query",
        ));
    };
    let inlined = match scalar_body(&query) {
        // The common shape: a single expression over no relation inlines into
        // the caller's own tree, so it evaluates once per row like PostgreSQL's
        // own inlined SQL function.
        Some(body) => substitute(&binding, body, true)?,
        // A body that reads a relation becomes a scalar subquery, which the
        // subquery pass runs under the caller's snapshot. That pass resolves
        // only *uncorrelated* subqueries, so an argument that varies per row is
        // refused there rather than answered wrongly.
        None => Expr::ScalarSubquery(Box::new(substitute_in_query(&binding, &query)?)),
    };
    binding.reject_repeated_volatile_args()?;
    let inlined = match resolved_scalar_result_type(&routine, given) {
        Some(ty) => Expr::Cast {
            expr: Box::new(inlined),
            ty,
        },
        None => inlined,
    };
    // `RETURNS void` discards the body's value but still evaluates it, so an
    // error inside the body still surfaces.
    let inlined = if declared_returns_void(&routine) {
        Expr::Case {
            operand: None,
            whens: vec![(
                Expr::IsNull {
                    expr: Box::new(inlined),
                    negated: false,
                },
                Expr::NullLiteral,
            )],
            else_result: Some(Box::new(Expr::NullLiteral)),
        }
    } else {
        inlined
    };
    // A STRICT routine yields NULL without evaluating its body when any
    // argument is NULL; spell that as the equivalent CASE.
    Ok(Some(if routine.strict {
        strict_guard(&binding.args, inlined)
    } else {
        inlined
    }))
}

/// `CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE <body> END`.
fn strict_guard(args: &[Expr], body: Expr) -> Expr {
    let Some(guard) = args
        .iter()
        .map(|arg| Expr::IsNull {
            expr: Box::new(arg.clone()),
            negated: false,
        })
        .reduce(|left, right| Expr::Binary {
            op: crabka_pgparser::ast::BinaryOp::Or,
            left: Box::new(left),
            right: Box::new(right),
        })
    else {
        return body;
    };
    Expr::Case {
        operand: None,
        whens: vec![(guard, Expr::NullLiteral)],
        else_result: Some(Box::new(body)),
    }
}

/// Does the routine return `void`?
pub(crate) fn declared_returns_void(routine: &Routine) -> bool {
    matches!(&routine.result, RoutineResult::Type { ty, setof: false } if ty.is_void())
}

/// Whether a table function declares the pseudo-type `SETOF void`.
pub(crate) fn declared_returns_setof_void(routine: &Routine) -> bool {
    matches!(&routine.result, RoutineResult::Type { ty, setof: true } if ty.is_void())
}

/// The column type a `RETURNS void` call answers with.
///
/// Crabka models no `void` column type, so the built-in void functions --
/// `setseed` and `pg_notify` -- already answer an empty `text`. A PL/pgSQL
/// `RETURNS void` function answers the same way, which is what PostgreSQL
/// prints for its own void: a blank, *non-null* value, so `\pset null` leaves
/// it blank rather than showing the null marker.
pub(crate) const VOID_RESULT_TYPE: ColumnType = ColumnType::Text;

/// The value a `RETURNS void` PL/pgSQL call answers with.
pub(crate) fn void_result_value() -> Datum {
    Datum::Text(String::new())
}

/// The result type a *call site* sees, including `RETURNS void`.
///
/// [`resolved_scalar_result_type`] answers what the routine's declaration
/// models, and `void` is modelled by nothing; this answers what the column the
/// caller reads is made of.
fn called_scalar_result_type(routine: &Routine, given: &[ArgType]) -> Option<ColumnType> {
    if declared_returns_void(routine) {
        return Some(VOID_RESULT_TYPE);
    }
    if declared_output_parameter_count(routine) > 1 {
        return Some(ColumnType::Record(None));
    }
    resolved_scalar_result_type(routine, given)
}

fn called_scalar_result_type_with_catalog(
    kv: &dyn Kv,
    routine: &Routine,
    given: &[ArgType],
) -> Result<Option<ColumnType>, ExecError> {
    Ok(called_scalar_result_type(routine, given)
        .or(declared_relation_rowtype(kv, routine)?
            .map(|rowtype| ColumnType::Record(Some(rowtype)))))
}

pub(crate) fn declared_relation_rowtype(
    kv: &dyn Kv,
    routine: &Routine,
) -> Result<Option<crabka_pgtypes::usertype::UserTypeRef>, ExecError> {
    let RoutineResult::Type { ty, .. } = &routine.result else {
        return Ok(None);
    };
    if let Some(ColumnType::Record(Some(rowtype))) = ty.column {
        return Ok(Some(rowtype));
    }
    if ty.column.is_some() {
        return Ok(None);
    }
    let resolution = crate::relname::ResolutionScope::default_scope();
    let Ok(written) = crate::relname::parse_written_relation(resolution, &ty.name) else {
        return Ok(None);
    };
    let Ok(relation) = crate::relname::resolve_relation(
        kv,
        resolution,
        &written.reference,
        crate::relname::SchemaDisposition::Reference,
    ) else {
        return Ok(None);
    };
    crate::catalog_rel::relation_rowtype(kv, &relation)
}

/// The scalar type a routine's result carries, when Gres models it.
pub(crate) fn declared_scalar_result_type(routine: &Routine) -> Option<ColumnType> {
    match &routine.result {
        RoutineResult::Type { ty, setof: false } => ty.column,
        RoutineResult::Unspecified if routine.output_params().count() == 1 => routine
            .output_params()
            .next()
            .and_then(|param| param.ty.column),
        _ => None,
    }
}

fn resolved_scalar_result_type(routine: &Routine, given: &[ArgType]) -> Option<ColumnType> {
    declared_scalar_result_type(routine).or_else(|| {
        let name = match &routine.result {
            RoutineResult::Type { ty, setof: false } => &ty.name,
            RoutineResult::Unspecified if routine.output_params().count() == 1 => {
                &routine.output_params().next()?.ty.name
            }
            _ => return None,
        };
        resolved_polymorphic_type(routine, given, name)
    })
}

/// The one select-list column a set-returning routine can provide.
fn single_set_result_column(routine: &Routine, given: &[ArgType]) -> Option<(String, ColumnType)> {
    match &routine.result {
        RoutineResult::Type { ty, setof: true } => {
            resolved_polymorphic_type(routine, given, &ty.name)
                .or(ty.column)
                .and_then(|ty| {
                    // Bare `record` gets its anonymous fields from the SQL body;
                    // a named composite remains one select-list value.
                    (!matches!(ty, ColumnType::Record(None))).then_some((routine.name.clone(), ty))
                })
        }
        RoutineResult::Table(columns) if columns.len() == 1 => columns
            .first()
            .and_then(|(name, ty)| ty.column.map(|ty| (name.clone(), ty))),
        RoutineResult::Unspecified if routine.output_params().count() == 1 => {
            routine.output_params().next().and_then(|param| {
                param.ty.column.map(|ty| {
                    (
                        param.name.clone().unwrap_or_else(|| routine.name.clone()),
                        ty,
                    )
                })
            })
        }
        _ => None,
    }
}

fn resolved_polymorphic_type(
    routine: &Routine,
    given: &[ArgType],
    result_name: &str,
) -> Option<ColumnType> {
    let inputs = routine.input_params().zip(given);
    let mut base = None;
    let mut range = None;
    let mut array = None;
    let mut multirange = None;
    for (param, arg) in inputs {
        let Some(candidate) = polymorphic_base_type(&param.ty.name, *arg) else {
            continue;
        };
        base = match base {
            None => Some(candidate),
            Some(current) if result_name.starts_with("anycompatible") => {
                if implicitly_coercible(current, candidate) {
                    Some(candidate)
                } else {
                    Some(current)
                }
            }
            current => current,
        };
        let ArgType::Known(ty) = arg else { continue };
        match ty {
            ColumnType::Array(_) => array = Some(*ty),
            ColumnType::Range(found) => range = Some(*found),
            ColumnType::Multirange(found) => {
                range = Some(found.range);
                multirange = Some(*found);
            }
            _ => {}
        }
    }
    match result_name {
        "anyelement" | "anyenum" | "anynonarray" | "anycompatible" | "anycompatiblenonarray" => {
            base
        }
        "anyarray" | "anycompatiblearray" => array.or_else(|| ColumnType::array_of(base?)),
        "anyrange" | "anycompatiblerange" => range.map(ColumnType::Range),
        "anymultirange" | "anycompatiblemultirange" => multirange
            .map(ColumnType::Multirange)
            .or_else(|| ColumnType::multirange_for_range(range?)),
        _ => None,
    }
}

/// Whether the declared result produces more than one row.
pub(crate) fn declared_returns_set(routine: &Routine) -> bool {
    routine.returns_set()
}

/// Number of explicit `OUT`/`INOUT` parameters in declaration order.
pub(crate) fn declared_output_parameter_count(routine: &Routine) -> usize {
    routine.output_params().count()
}

/// The query a set-returning call of a user routine expands to, with the
/// call's arguments substituted for the routine's parameters.
///
/// `Ok(None)` means the name carries no routine, so the built-in
/// set-returning-function registry keeps the call.
pub(crate) fn expand_table_function(
    kv: &dyn Kv,
    call: &crabka_pgparser::ast::TableFuncCall,
    given: &[ArgType],
) -> Result<Option<(QueryExpr, Routine)>, ExecError> {
    let Some(BoundRoutineCall { routine, args }) = bind_call(kv, &call.name, &call.args, given)?
    else {
        return Ok(None);
    };
    if routine.kind == RoutineKind::Procedure {
        return Err(wrong_routine_kind(format!(
            "{} is a procedure\nHINT:  To call a procedure, use CALL.",
            spelled_signature(&routine)
        )));
    }
    callable(&routine)?;
    let binding = Binding {
        routine: &routine,
        uses: std::cell::RefCell::new(vec![0; args.len()]),
        args,
    };
    let Some(query) = final_query(&routine)? else {
        return Err(uncallable(
            &routine,
            "a SQL function's final statement must be a query",
        ));
    };
    let substituted = substitute_in_query(&binding, &query)?;
    binding.reject_repeated_volatile_args()?;
    Ok(Some((substituted, routine)))
}

/// Substitute a routine's parameters through a whole body query.
///
/// This rewrites only the shapes a SQL function body actually uses. It refuses
/// a body whose parameters would have to reach into a nested query, instead of
/// silently leaving them unbound.
fn substitute_in_query(binding: &Binding, query: &QueryExpr) -> Result<QueryExpr, ExecError> {
    use crabka_pgparser::ast::{QueryBody, SetExpr};

    let mut out = query.clone();
    let SetExpr::Query(QueryBody::Select(select)) = &mut out.body else {
        if binding.routine.params.is_empty() {
            return Ok(out);
        }
        return Err(uncallable(
            binding.routine,
            "a parameter cannot be substituted into a set-operation body",
        ));
    };
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            *expr = substitute(binding, expr, false)?;
        }
    }
    if let Some(filter) = &mut select.filter {
        *filter = substitute(binding, filter, false)?;
    }
    if let Some(having) = &mut select.having {
        *having = substitute(binding, having, false)?;
    }
    for group in &mut select.group_by {
        *group = substitute(binding, group, false)?;
    }
    for item in &mut out.order_by {
        item.expr = substitute(binding, &item.expr, false)?;
    }
    substitute_in_from(binding, select)?;
    Ok(out)
}

/// Substitute into the argument expressions of FROM-clause function calls,
/// which is where a body like `SELECT … FROM generate_series(1, $1)` keeps its
/// parameters.
fn substitute_in_from(binding: &Binding, select: &mut SelectStmt) -> Result<(), ExecError> {
    use crabka_pgparser::ast::TableExpr;

    for item in &mut select.from {
        if let TableExpr::Function { functions, .. } = item {
            for call in functions.iter_mut() {
                for arg in call.arguments_mut() {
                    *arg = substitute(binding, arg, false)?;
                }
            }
        }
    }
    Ok(())
}

/// Does this FROM-clause function call name a user routine instead of a
/// built-in set-returning function?
///
/// `ROWS FROM (…)` with more than one call, and any call the built-in registry
/// already owns, stay with `srf`.
pub(crate) fn expands_as_table(
    kv: &dyn Kv,
    functions: &[crabka_pgparser::ast::TableFuncCall],
) -> bool {
    matches!(functions, [call] if !crate::srf::is_srf(&call.name) && is_user_routine(kv, &call.name))
}

/// The derived-table query, alias and column names a FROM-clause call of a user
/// routine expands to.
///
/// # Errors
///
/// Propagates catalog read errors and the routine model's own refusals.
pub(crate) fn table_function_expansion(
    kv: &dyn Kv,
    call: &crabka_pgparser::ast::TableFuncCall,
) -> Result<(QueryExpr, Routine, Vec<String>), ExecError> {
    if call.column_defs.is_some() {
        plpgsql_table_function_schema(kv, call)?;
    }
    let given = best_effort_arg_types(&call.args);
    let (query, routine) = expand_table_function(kv, call, &given)?
        .ok_or_else(|| undefined_routine(format!("function {} does not exist", call.name)))?;
    let names = table_function_columns(&routine).unwrap_or_else(|| vec![routine.name.clone()]);
    Ok((query, routine, names))
}

pub(crate) fn validate_inlined_record_column_defs(
    routine: &Routine,
    columns: &[crate::scope::ColumnBinding],
    defs: &[TableFuncColumnDef],
) -> Result<(), ExecError> {
    for (index, (column, def)) in columns.iter().zip(defs).enumerate() {
        if crabka_pgtypes::cast::assignment_cast_allowed(column.ty, def.ty) {
            continue;
        }
        return Err(ExecError::Remote(
            crabka_pgwire::error::PgError::error(
                "42P13",
                "return type mismatch in function declared to return record",
            )
            .with_detail(format!(
                "Final statement returns {} instead of {} at column {}.",
                column.ty.name(),
                def.ty.name(),
                index + 1
            ))
            .with_context(format!("SQL function \"{}\" during inlining", routine.name)),
        ));
    }
    Ok(())
}

pub(crate) fn table_function_result_rows(
    routine: &Routine,
    mut rows: Vec<Vec<Datum>>,
) -> Vec<Vec<Datum>> {
    if !routine.returns_set() {
        rows.truncate(1);
    }
    rows
}

/// The output column names a set-returning routine's rows carry.
pub(crate) fn table_function_columns(routine: &Routine) -> Option<Vec<String>> {
    match &routine.result {
        RoutineResult::Table(columns) => Some(columns.iter().map(|(n, _)| n.clone()).collect()),
        RoutineResult::Unspecified => {
            let names: Vec<String> = routine
                .output_params()
                .enumerate()
                .map(|(index, param)| {
                    param
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("column{}", index + 1))
                })
                .collect();
            (!names.is_empty()).then_some(names)
        }
        RoutineResult::Type { .. } => Some(vec![routine.name.clone()]),
    }
}

fn is_record_type(ty: &RoutineType) -> bool {
    ty.is_record() || matches!(ty.column, Some(ColumnType::Record(_)))
}

pub(crate) fn plpgsql_table_function_schema(
    kv: &dyn Kv,
    call: &crabka_pgparser::ast::TableFuncCall,
) -> Result<Option<PlPgSqlTableSchema>, ExecError> {
    let given = best_effort_arg_types(&call.args);
    let Some(routine) = resolve_call(kv, &call.name, &given)? else {
        return Ok(None);
    };
    if !matches!(routine.language.as_str(), "plpgsql" | "sql") {
        return Ok(None);
    }
    if routine.kind != RoutineKind::Function {
        return Err(wrong_routine_kind(format!(
            "{} is a procedure\nHINT:  To call a procedure, use CALL.",
            spelled_signature(&routine)
        )));
    }
    let output_params = || {
        routine
            .output_params()
            .enumerate()
            .map(|(index, param)| {
                param
                    .ty
                    .column
                    .or_else(|| resolved_polymorphic_type(&routine, &given, &param.ty.name))
                    .map(|ty| {
                        (
                            param
                                .name
                                .clone()
                                .unwrap_or_else(|| format!("column{}", index + 1)),
                            ty,
                        )
                    })
                    .ok_or_else(|| {
                        ExecError::Unsupported(format!(
                            "function {} returns unsupported type {}",
                            routine.identity(),
                            param.ty.name
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()
    };
    let rowtype = declared_relation_rowtype(kv, &routine)?;
    if call.column_defs.is_some() {
        if rowtype.is_some() {
            return Err(ExecError::Syntax(
                "a column definition list is redundant for a function returning a named \
                 composite type"
                    .into(),
            ));
        }
        match &routine.result {
            RoutineResult::Type { ty, .. }
                if is_record_type(ty) && routine.output_params().next().is_none() => {}
            RoutineResult::Table(_) | RoutineResult::Unspecified => {
                return Err(ExecError::Syntax(
                    "a column definition list is redundant for a function with OUT parameters"
                        .into(),
                ));
            }
            RoutineResult::Type { .. } if routine.output_params().next().is_some() => {
                return Err(ExecError::Syntax(
                    "a column definition list is redundant for a function with OUT parameters"
                        .into(),
                ));
            }
            RoutineResult::Type { .. } => {
                return Err(ExecError::Syntax(
                    "a column definition list is only allowed for functions returning \"record\""
                        .into(),
                ));
            }
        }
    }
    if let Some(rowtype) = rowtype {
        let columns = crabka_pgtypes::usertype::lookup_oid(rowtype.oid)
            .and_then(|registered| {
                registered.fields().map(|fields| {
                    fields
                        .iter()
                        .map(|field| (field.name.clone(), field.ty))
                        .collect()
                })
            })
            .or(crate::catalog_rel::relation_rowtype_columns(
                kv,
                rowtype.oid,
            )?)
            .ok_or_else(|| {
                ExecError::Unsupported(format!(
                    "function {} returns unsupported type {}",
                    routine.identity(),
                    rowtype.name
                ))
            })?;
        return Ok(Some((routine, columns)));
    }
    let columns = match &routine.result {
        RoutineResult::Table(columns) => columns
            .iter()
            .map(|(name, ty)| {
                ty.column.map(|ty| (name.clone(), ty)).ok_or_else(|| {
                    ExecError::Unsupported(format!(
                        "function {} returns unsupported type {}",
                        routine.identity(),
                        ty.name
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        RoutineResult::Unspecified => output_params()?,
        RoutineResult::Type { ty, .. }
            if is_record_type(ty) && routine.output_params().next().is_some() =>
        {
            output_params()?
        }
        RoutineResult::Type { ty, .. } if is_record_type(ty) => call
            .column_defs
            .as_ref()
            .ok_or_else(|| {
                ExecError::Syntax(
                    "a column definition list is required for functions returning \"record\""
                        .into(),
                )
            })?
            .iter()
            .map(|column| (column.name.clone(), column.ty))
            .collect(),
        RoutineResult::Type { ty, .. } if ty.is_void() => {
            vec![(routine.name.clone(), VOID_RESULT_TYPE)]
        }
        RoutineResult::Type { ty, .. } => vec![(
            routine.name.clone(),
            ty.column.ok_or_else(|| {
                ExecError::Unsupported(format!(
                    "function {} returns unsupported type {}",
                    routine.identity(),
                    ty.name
                ))
            })?,
        )],
    };
    if columns.is_empty() {
        return Err(ExecError::Unsupported(format!(
            "function {} has no table result columns",
            routine.identity()
        )));
    }
    Ok(Some((routine, columns)))
}

pub(crate) fn eval_plpgsql_table_function(
    call: &crabka_pgparser::ast::TableFuncCall,
    ctx: &crate::clock::EvalCtx,
    allow_inlining: bool,
) -> Result<Option<PlPgSqlTableRows>, ExecError> {
    SCALAR_RUNTIME.with(|runtime| {
        let runtime = runtime.borrow();
        let Some(runtime) = runtime.as_ref().cloned() else {
            return Ok(None);
        };
        let Some((_routine, columns)) =
            plpgsql_table_function_schema(runtime.catalog.as_ref(), call)?
        else {
            return Ok(None);
        };
        let mut values = call
            .args
            .iter()
            .map(|arg| crate::eval::eval(arg, &crate::scope::Scope::empty(), &[], ctx))
            .collect::<Result<Vec<_>, _>>()?;
        let given = crate::eval::value_arg_types(&call.args, &values);
        let BoundRoutineCall {
            routine,
            args: bound_args,
        } = bind_call(runtime.catalog.as_ref(), &call.name, &call.args, &given)?
            .ok_or_else(|| undefined_routine(format!("function {} does not exist", call.name)))?;
        if allow_inlining
            && routine.language == "sql"
            && !routine.strict
            && matches!(
                &routine.result,
                RoutineResult::Type { ty, .. }
                    if is_record_type(ty) && routine.output_params().next().is_none()
            )
            && call.column_defs.is_some()
            && expand_table_function(runtime.catalog.as_ref(), call, &given)
                .is_ok_and(|query| query.is_some())
        {
            return Ok(None);
        }
        for default in bound_args.iter().skip(values.len()) {
            values.push(crate::eval::eval(
                default,
                &crate::scope::Scope::empty(),
                &[],
                ctx,
            )?);
        }
        pack_variadic_values(&routine, &call.args, &mut values, ctx)?;
        let params = routine
            .input_params()
            .map(|param| param.ty.column)
            .collect::<Vec<_>>();
        crate::eval::coerce_unknown_args(&bound_args, &mut values, &params, ctx)?;
        if routine.strict && values.iter().any(Datum::is_null) {
            let rows = if routine.returns_set() {
                Vec::new()
            } else {
                vec![vec![Datum::Null; columns.len()]]
            };
            return Ok(Some((columns, rows)));
        }
        let _guard = enter_plpgsql_call()?;
        let requests = runtime.requests.ok_or_else(|| {
            ExecError::Unsupported("PL/pgSQL table function requires a session executor".into())
        })?;
        let (reply, response) = std::sync::mpsc::channel();
        requests
            .try_send(ScalarFunctionRequest {
                routine: Some(routine),
                values,
                kind: FunctionRequestKind::Table(columns.clone()),
                command_row_claims: scalar_runtime_command_row_claims(),
                reply,
            })
            .map_err(|_| {
                ExecError::ObjectNotInPrerequisiteState("PL/pgSQL function executor stopped".into())
            })?;
        let (result, mutations) = response.recv().map_err(|_| {
            ExecError::ObjectNotInPrerequisiteState("PL/pgSQL function executor stopped".into())
        })??;
        crate::session::apply_guc_runtime_mutations(mutations)?;
        match result {
            FunctionRequestResult::Table(rows) => Ok(Some((columns, rows))),
            FunctionRequestResult::Scalar(value) => Ok(Some((columns, vec![vec![value]]))),
        }
    })
}

/// `DO`, refused for every language, with the reason `PostgreSQL` gives when
/// it has one.
pub(crate) fn do_block(language: &str) -> ExecError {
    if language == "sql" {
        // PostgreSQL's own refusal: the SQL language has no inline handler.
        return ExecError::Unsupported(
            "language \"sql\" does not support inline code execution".into(),
        );
    }
    if LANGUAGES.contains(&language) {
        return ExecError::Unsupported(format!(
            "DO blocks in language \"{language}\" are not supported: Gres has no {language} \
             interpreter"
        ));
    }
    ExecError::FunctionError {
        sqlstate: "42704",
        message: format!("language \"{language}\" does not exist"),
    }
}

// ---------------------------------------------------------------- rendering

/// `pg_get_function_arguments`: every parameter, with modes and defaults.
#[must_use]
pub(crate) fn render_arguments(routine: &Routine) -> String {
    let procedure = routine.kind == RoutineKind::Procedure;
    routine
        .params
        .iter()
        .map(|param| {
            let mut out = if procedure && param.mode == ParamMode::In {
                // PostgreSQL always spells a procedure's IN parameters.
                "IN ".to_string()
            } else {
                String::from(param.mode.spelled_prefix())
            };
            if let Some(name) = &param.name {
                out.push_str(name);
                out.push(' ');
            }
            out.push_str(&param.ty.name);
            if let Some(default) = &param.default {
                out.push_str(" DEFAULT ");
                out.push_str(default);
            }
            out
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// `pg_get_function_identity_arguments`: the input parameters only, with no
/// names dropped but no defaults, as `ALTER`/`DROP` accept them.
#[must_use]
pub(crate) fn render_identity_arguments(routine: &Routine) -> String {
    let procedure = routine.kind == RoutineKind::Procedure;
    routine
        .input_params()
        .map(|param| {
            let prefix = if procedure && param.mode == ParamMode::In {
                "IN "
            } else {
                param.mode.spelled_prefix()
            };
            match &param.name {
                Some(name) => format!("{prefix}{name} {}", param.ty.name),
                None => format!("{prefix}{}", param.ty.name),
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// `pg_get_function_result`: `NULL` for a procedure, as `PostgreSQL` reports.
#[must_use]
pub(crate) fn render_result(routine: &Routine) -> Option<String> {
    if routine.kind == RoutineKind::Procedure {
        return None;
    }
    Some(match &routine.result {
        RoutineResult::Type { ty, setof } => {
            if *setof {
                format!("SETOF {}", ty.name)
            } else {
                ty.name.clone()
            }
        }
        RoutineResult::Table(columns) => format!(
            "TABLE({})",
            columns
                .iter()
                .map(|(name, ty)| format!("{name} {}", ty.name))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        RoutineResult::Unspecified => {
            let outputs: Vec<&RoutineParam> = routine.output_params().collect();
            match outputs.as_slice() {
                [] => "void".to_string(),
                [only] => only.ty.name.clone(),
                _ => "record".to_string(),
            }
        }
    })
}

/// `pg_get_functiondef`: the `CREATE OR REPLACE` text `PostgreSQL` renders.
#[must_use]
pub(crate) fn render_functiondef(routine: &Routine) -> String {
    let mut out = format!(
        "CREATE OR REPLACE {} public.{}({})\n",
        if routine.kind == RoutineKind::Procedure {
            "PROCEDURE"
        } else {
            "FUNCTION"
        },
        routine.name,
        render_arguments(routine)
    );
    if let Some(result) = render_result(routine) {
        let _ = writeln!(out, " RETURNS {result}");
    }
    let _ = writeln!(out, " LANGUAGE {}", routine.language);
    let mut qualifiers = Vec::new();
    match routine.volatility {
        'i' => qualifiers.push("IMMUTABLE".to_string()),
        's' => qualifiers.push("STABLE".to_string()),
        _ => {}
    }
    if routine.leakproof {
        qualifiers.push("LEAKPROOF".to_string());
    }
    match routine.parallel {
        's' => qualifiers.push("PARALLEL SAFE".to_string()),
        'r' => qualifiers.push("PARALLEL RESTRICTED".to_string()),
        _ => {}
    }
    if routine.strict {
        qualifiers.push("STRICT".to_string());
    }
    if routine.security_definer {
        qualifiers.push("SECURITY DEFINER".to_string());
    }
    if (routine.cost - 100.0).abs() > f64::EPSILON {
        qualifiers.push(format!("COST {}", render_number(routine.cost)));
    }
    if routine.rows > 0.0
        && (routine.rows - if routine.returns_set() { 1000.0 } else { 0.0 }).abs() > f64::EPSILON
    {
        qualifiers.push(format!("ROWS {}", render_number(routine.rows)));
    }
    if !qualifiers.is_empty() {
        let _ = writeln!(out, " {}", qualifiers.join(" "));
    }
    for (index, entry) in routine.config.iter().enumerate() {
        let source = routine.config_source.get(index).unwrap_or(entry);
        let _ = writeln!(out, " SET {}", render_config_setting(entry, source));
    }
    if let Some(object_file) = &routine.object_file {
        let _ = writeln!(
            out,
            "AS {}, {}",
            routine_literal(object_file),
            routine_literal(&routine.body)
        );
    } else {
        match routine.body_form {
            BodyForm::Source => {
                // PostgreSQL tags the quote with the routine's kind.
                let tag = if routine.kind == RoutineKind::Procedure {
                    "procedure"
                } else {
                    "function"
                };
                let _ = writeln!(out, "AS ${tag}${}${tag}$", routine.body);
            }
            BodyForm::Atomic => {
                let body = atomic_return_expression(&routine.body).map_or_else(
                    || atomic_body_source(&routine.body).to_string(),
                    |expr| format!("RETURN {expr}"),
                );
                let _ = writeln!(out, "BEGIN ATOMIC\n {body};\nEND");
            }
            BodyForm::Return => {
                let _ = writeln!(out, "RETURN {}", routine.body);
            }
        }
    }
    out
}

fn routine_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn render_config_setting(entry: &str, source: &str) -> String {
    let Some((name, value)) = entry.split_once('=') else {
        return source.to_string();
    };
    let name_end = source.find([' ', '\t', '=']).unwrap_or(source.len());
    let rest = source[name_end..].trim_start();
    let raw_value = rest
        .strip_prefix('=')
        .map(str::trim_start)
        .or_else(|| {
            rest.split_once(char::is_whitespace)
                .map(|(_, value)| value.trim())
        })
        .unwrap_or_default();
    let value = if raw_value.starts_with('\'') {
        raw_value.to_string()
    } else if raw_value.starts_with('"') {
        render_double_quoted_config_values(raw_value)
    } else {
        routine_literal(value)
    };
    let name = if name == "datestyle" {
        "\"DateStyle\""
    } else {
        name
    };
    format!("{name} TO {value}")
}

fn render_double_quoted_config_values(value: &str) -> String {
    let mut out = String::new();
    let mut chars = value.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '\'' {
            out.push(character);
            while let Some(character) = chars.next() {
                out.push(character);
                if character == '\'' {
                    if chars.next_if_eq(&'\'').is_some() {
                        out.push('\'');
                    } else {
                        break;
                    }
                }
            }
            continue;
        }
        if character != '"' {
            out.push(character);
            continue;
        }
        let mut quoted = String::new();
        while let Some(character) = chars.next() {
            if character != '"' {
                quoted.push(character);
            } else if chars.next_if_eq(&'"').is_some() {
                quoted.push('"');
            } else {
                break;
            }
        }
        out.push_str(&routine_literal(&quoted));
    }
    out
}

/// A `COST`/`ROWS` value the way `PostgreSQL` prints it: whole numbers with no
/// fractional part.
fn render_number(value: f64) -> String {
    if (value.fract()).abs() < f64::EPSILON {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

// ----------------------------------------------------------------- pg_proc

/// Look a routine up by its `pg_proc.oid`.
///
/// # Errors
///
/// Propagates catalog read errors.
pub(crate) fn routine_by_oid(kv: &dyn Kv, oid: i32) -> Result<Option<Routine>, ExecError> {
    let oid = u32::try_from(oid).unwrap_or(0);
    Ok(list_routines(kv)?.into_iter().find(|r| r.oid == oid))
}

/// The SQL source of one declared function-argument default, if it exists.
pub(crate) fn function_arg_default(
    kv: &dyn Kv,
    oid: i32,
    index: i64,
) -> Result<Option<String>, ExecError> {
    let Ok(index) = usize::try_from(index) else {
        return Ok(None);
    };
    Ok(routine_by_oid(kv, oid)?.and_then(|routine| routine.params.get(index)?.default.clone()))
}

/// Look a routine up the way `pg_get_functiondef('f(int)'::regprocedure)` does:
/// by identity text, or by oid.
///
/// # Errors
///
/// Propagates catalog read errors.
pub(crate) fn routine_by_reference(
    kv: &dyn Kv,
    value: &Datum,
) -> Result<Option<Routine>, ExecError> {
    match value {
        Datum::Int4(oid) => routine_by_oid(kv, *oid),
        Datum::Int8(oid) => routine_by_oid(kv, i32::try_from(*oid).unwrap_or(0)),
        Datum::Regclass(value) => routine_by_oid(kv, value.oid),
        Datum::Text(text) => {
            let trimmed = text.trim();
            let bare = trimmed.strip_prefix("public.").unwrap_or(trimmed);
            if let Some(found) = get_routine(kv, bare)? {
                return Ok(Some(found));
            }
            let mut named = routines_named(kv, bare)?;
            if named.len() == 1 {
                return Ok(Some(named.remove(0)));
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

/// The `pg_proc` rows for every user routine, in catalog order.
///
/// # Errors
///
/// Propagates catalog read errors.
pub(crate) fn pg_proc_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = builtin_pg_proc_rows()?;
    rows.extend(user_pg_proc_rows(kv)?);
    Ok(rows)
}

/// The `pg_proc` rows for routines this database defines, without the built-in
/// fixture.
///
/// Split out so a caller that only needs the user half -- `reg*` output, which
/// indexes the built-ins once -- does not pay for cloning several thousand
/// static rows on every call.
pub(crate) fn user_pg_proc_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    use crabka_pgtypes::{ArrayValue, ElemType};

    let mut rows = Vec::new();
    for routine in list_routines(kv)? {
        let inputs: Vec<&RoutineParam> = routine.input_params().collect();
        let all_default_modes = routine
            .params
            .iter()
            .all(|param| param.mode == ParamMode::In)
            && matches!(
                routine.result,
                RoutineResult::Type { .. } | RoutineResult::Unspecified
            );
        let arg_type_oids: Vec<Datum> = inputs
            .iter()
            .map(|param| Ok(Datum::Int4(catalog_type_oid(kv, &param.ty)?)))
            .collect::<Result<_, ExecError>>()?;
        let all_types = catalog_all_types(kv, &routine)?;
        let all_modes: Vec<Datum> = catalog_all_modes(&routine);
        let mut arg_names: Vec<Datum> = routine
            .params
            .iter()
            .map(|param| Datum::Text(param.name.clone().unwrap_or_default()))
            .collect();
        let mut has_names = routine.params.iter().any(|param| param.name.is_some());
        if let RoutineResult::Table(columns) = &routine.result {
            arg_names.extend(columns.iter().map(|(name, _)| Datum::Text(name.clone())));
            has_names = true;
        }
        rows.push(vec![
            Datum::Int4(i32::try_from(routine.oid).unwrap_or(0)),
            Datum::Text(routine.name.clone()),
            Datum::Int4(crate::exec::PUBLIC_NAMESPACE_OID),
            Datum::Int4(crate::catalog_fn::BOOTSTRAP_ROLE_OID),
            Datum::Int4(language_oid(&routine.language)),
            Datum::Float8(routine.cost),
            Datum::Float8(routine.rows),
            Datum::Int4(0),
            Datum::Int4(0),
            Datum::Text(routine.kind.catalog_code().to_string()),
            Datum::Bool(routine.security_definer),
            Datum::Bool(routine.leakproof),
            Datum::Bool(routine.strict),
            Datum::Bool(routine.returns_set()),
            Datum::Text(routine.volatility.to_string()),
            Datum::Text(routine.parallel.to_string()),
            Datum::Int2(i16::try_from(inputs.len()).unwrap_or(0)),
            Datum::Int2(i16::try_from(routine.default_count()).unwrap_or(0)),
            Datum::Int4(catalog_return_type_oid(kv, &routine)?),
            Datum::OidVector(ArrayValue::with_dims(
                ElemType::Int4,
                arg_type_oids,
                vec![crabka_pgtypes::ArrayDim::new(
                    0,
                    i32::try_from(inputs.len()).unwrap_or(i32::MAX),
                )],
            )),
            if all_default_modes {
                Datum::Null
            } else {
                Datum::Array(ArrayValue::new(ElemType::Int4, all_types))
            },
            if all_default_modes {
                Datum::Null
            } else {
                Datum::Array(ArrayValue::new(ElemType::Text, all_modes))
            },
            if has_names {
                Datum::Array(ArrayValue::new(ElemType::Text, arg_names))
            } else {
                Datum::Null
            },
            Datum::Null,
            Datum::Null,
            match routine.body_form {
                BodyForm::Source => Datum::Text(routine.body.clone()),
                BodyForm::Atomic | BodyForm::Return => Datum::Text(String::new()),
            },
            routine.object_file.clone().map_or(Datum::Null, Datum::Text),
            match routine.body_form {
                BodyForm::Source => Datum::Null,
                BodyForm::Atomic | BodyForm::Return => Datum::Text(routine.body.clone()),
            },
            if routine.config.is_empty() {
                Datum::Null
            } else {
                Datum::Array(ArrayValue::new(
                    ElemType::Text,
                    routine
                        .config
                        .iter()
                        .map(|entry| Datum::Text(entry.clone()))
                        .collect(),
                ))
            },
            Datum::Null,
        ]);
    }
    Ok(rows)
}

/// The decoded built-in `pg_proc` fixture, decompressed and parsed once.
///
/// The fixture is immutable static data, but decoding it means zstd over the
/// whole table and then a parse per line. `reg*` output resolves one oid to a
/// name per rendered row, so a scan that projects a `reg*` value used to pay
/// that decode once per row -- around 19 ms each, which turned a 3,400-row
/// `pg_proc` scan into a minute and a full certification into a timeout.
static BUILTIN_PG_PROC_ROWS: std::sync::OnceLock<Option<Vec<Vec<Datum>>>> =
    std::sync::OnceLock::new();

pub(crate) fn builtin_pg_proc_rows() -> Result<Vec<Vec<Datum>>, ExecError> {
    BUILTIN_PG_PROC_ROWS
        .get_or_init(|| decode_builtin_pg_proc_rows().ok())
        .clone()
        .ok_or_else(|| ExecError::Unsupported("built-in pg_proc fixture is corrupt".into()))
}

fn decode_builtin_pg_proc_rows() -> Result<Vec<Vec<Datum>>, ExecError> {
    let corrupt = || ExecError::Unsupported("built-in pg_proc fixture is corrupt".into());
    let data = crate::builtin_procs::BUILTIN_PROCS
        .iter()
        .map(|data| zstd::decode_all(*data).map_err(|_| corrupt()))
        .collect::<Result<Vec<_>, _>>()?;
    let support_oids = data
        .iter()
        .flat_map(|data| data.split(|byte| *byte == b'\n'))
        .filter(|line| !line.is_empty())
        .map(|line| {
            let line = std::str::from_utf8(line).map_err(|_| corrupt())?;
            let fields = line.split('\t').collect::<Vec<_>>();
            let oid = fields
                .first()
                .ok_or_else(corrupt)?
                .parse::<i32>()
                .map_err(|_| corrupt())?;
            let name = fields.get(1).ok_or_else(corrupt)?;
            Ok(((*name).to_string(), oid))
        })
        .collect::<Result<std::collections::BTreeMap<_, _>, ExecError>>()?;
    data.iter()
        .flat_map(|data| data.split(|byte| *byte == b'\n'))
        .filter(|line| !line.is_empty())
        .map(|line| {
            let line = std::str::from_utf8(line).map_err(|_| corrupt())?;
            let fields = line.split('\t').collect::<Vec<_>>();
            let [
                oid,
                name,
                language,
                cost,
                result_rows,
                variadic,
                support,
                kind,
                flags,
                volatility,
                parallel,
                argument_count,
                default_count,
                result_type,
                argument_types,
                source,
                sql_body,
                argument_modes,
                all_argument_types,
                argument_names,
                argument_defaults,
            ] = fields.as_slice()
            else {
                return Err(corrupt());
            };
            let int = |value: &str| value.parse::<i32>().map_err(|_| corrupt());
            let short = |value: &str| value.parse::<i16>().map_err(|_| corrupt());
            let character =
                |value: &str| value.parse::<u8>().map(char::from).map_err(|_| corrupt());
            let flags = flags.as_bytes();
            if flags.len() != 4 {
                return Err(corrupt());
            }
            let array_items = |value: &str| -> Result<Option<Vec<String>>, ExecError> {
                if value == "-" {
                    return Ok(None);
                }
                let values = value
                    .strip_prefix('{')
                    .and_then(|value| value.strip_suffix('}'))
                    .ok_or_else(corrupt)?;
                let items = values
                    .split(',')
                    .map(|item| item.trim().to_string())
                    .collect::<Vec<_>>();
                if items.is_empty() || items.iter().any(|item| item.is_empty()) {
                    return Err(corrupt());
                }
                Ok(Some(items))
            };
            let all_argument_types = array_items(all_argument_types)?;
            let argument_modes = array_items(argument_modes)?;
            let argument_names = array_items(argument_names)?;
            Ok(vec![
                Datum::Int4(int(oid)?),
                Datum::Text((*name).to_string()),
                Datum::Int4(crate::exec::PG_CATALOG_NAMESPACE_OID),
                Datum::Int4(crate::catalog_fn::BOOTSTRAP_ROLE_OID),
                Datum::Int4(int(language)?),
                Datum::Float8(f64::from(int(cost)?)),
                Datum::Float8(f64::from(int(result_rows)?)),
                Datum::Int4(int(variadic)?),
                Datum::Int4(if *support == "-" || *support == "0" {
                    0
                } else {
                    *support_oids.get(*support).ok_or_else(corrupt)?
                }),
                Datum::Text(character(kind)?.to_string()),
                Datum::Bool(flags[0] == b'1'),
                Datum::Bool(flags[1] == b'1'),
                Datum::Bool(flags[2] == b'1'),
                Datum::Bool(flags[3] == b'1'),
                Datum::Text(character(volatility)?.to_string()),
                Datum::Text(character(parallel)?.to_string()),
                Datum::Int2(short(argument_count)?),
                Datum::Int2(short(default_count)?),
                Datum::Int4(int(result_type)?),
                Datum::OidVector(crabka_pgtypes::ArrayValue::with_dims(
                    crabka_pgtypes::ElemType::Int4,
                    argument_types
                        .split_whitespace()
                        .map(|value| int(value).map(Datum::Int4))
                        .collect::<Result<Vec<_>, _>>()?,
                    vec![crabka_pgtypes::ArrayDim::new(
                        0,
                        i32::from(short(argument_count)?),
                    )],
                )),
                match all_argument_types {
                    Some(types) => Datum::Array(crabka_pgtypes::ArrayValue::new(
                        crabka_pgtypes::ElemType::Int4,
                        types
                            .into_iter()
                            .map(|value| int(&value).map(Datum::Int4))
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    None => Datum::Null,
                },
                match argument_modes {
                    Some(modes) => {
                        let modes = modes
                            .into_iter()
                            .map(|mode| match mode.as_str() {
                                "i" | "o" | "b" | "v" | "t" => Ok(Datum::Text(mode)),
                                _ => Err(corrupt()),
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        Datum::Array(crabka_pgtypes::ArrayValue::new(
                            crabka_pgtypes::ElemType::Text,
                            modes,
                        ))
                    }
                    None => Datum::Null,
                },
                match argument_names {
                    Some(names) => Datum::Array(crabka_pgtypes::ArrayValue::new(
                        crabka_pgtypes::ElemType::Text,
                        names.into_iter().map(Datum::Text).collect(),
                    )),
                    None => Datum::Null,
                },
                if *argument_defaults == "-" {
                    Datum::Null
                } else {
                    Datum::Text((*argument_defaults).to_string())
                },
                Datum::Null,
                Datum::Text((*source).to_string()),
                if int(language)? == 13 {
                    Datum::Text((*name).to_string())
                } else {
                    Datum::Null
                },
                if *sql_body == "-" {
                    Datum::Null
                } else {
                    Datum::Text((*sql_body).to_string())
                },
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ])
        })
        .collect()
}

fn decode_builtin_named_signatures() -> Result<Vec<BuiltinNamedSignature>, ExecError> {
    let corrupt = || ExecError::Unsupported("built-in pg_proc fixture is corrupt".into());
    let signatures = crate::builtin_procs::BUILTIN_PROCS
        .iter()
        .map(|data| zstd::decode_all(*data).map_err(|_| corrupt()))
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .flat_map(|data| data.split(|byte| *byte == b'\n'))
        .filter(|line| !line.is_empty())
        .map(|line| -> Result<Option<BuiltinNamedSignature>, ExecError> {
            let line = std::str::from_utf8(line).map_err(|_| corrupt())?;
            let fields = line.split('\t').collect::<Vec<_>>();
            let [
                _,
                name,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                argument_count,
                default_count,
                _,
                argument_types,
                _,
                argument_modes,
                _,
                argument_names,
                defaults,
            ] = fields.as_slice()
            else {
                return Err(corrupt());
            };
            if *argument_names == "-" {
                return Ok(None);
            }
            let argument_count = argument_count.parse::<usize>().map_err(|_| corrupt())?;
            let default_count = default_count.parse::<usize>().map_err(|_| corrupt())?;
            let type_oids = argument_types
                .split_whitespace()
                .map(|oid| oid.parse::<u32>().map_err(|_| corrupt()))
                .collect::<Result<Vec<_>, _>>()?;
            if type_oids.len() != argument_count || default_count > argument_count {
                return Err(corrupt());
            }
            let Some(names) = array_items(argument_names, &corrupt)? else {
                return Ok(None);
            };
            let input_names = match array_items(argument_modes, &corrupt)? {
                None => names,
                Some(modes) => names
                    .into_iter()
                    .zip(modes)
                    .filter_map(|(name, mode)| {
                        matches!(mode.as_str(), "i" | "b" | "v").then_some(name)
                    })
                    .collect(),
            };
            if input_names.len() != argument_count {
                return Ok(None);
            }
            let defaults = default_exprs(defaults, default_count, &corrupt)?;
            Ok(Some(BuiltinNamedSignature {
                name: (*name).to_string(),
                input_types: type_oids
                    .into_iter()
                    .map(|oid| crate::exec::column_type_from_oid(oid).ok())
                    .collect(),
                input_names,
                defaults,
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(signatures.into_iter().flatten().collect())
}

fn array_items(
    value: &str,
    corrupt: impl Fn() -> ExecError,
) -> Result<Option<Vec<String>>, ExecError> {
    if value == "-" {
        return Ok(None);
    }
    let values = value
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
        .ok_or_else(|| corrupt())?;
    let items = values
        .split(',')
        .map(|item| item.trim().to_string())
        .collect::<Vec<_>>();
    if items.is_empty() || items.iter().any(|item| item.is_empty()) {
        return Err(corrupt());
    }
    Ok(Some(items))
}

fn default_exprs(
    defaults: &str,
    count: usize,
    corrupt: impl Fn() -> ExecError,
) -> Result<Vec<Expr>, ExecError> {
    if count == 0 {
        return (defaults == "-").then(Vec::new).ok_or_else(corrupt);
    }
    let Expr::ArrayLiteral(expressions) =
        crabka_pgparser::parser::parse_expression(&format!("ARRAY[{defaults}]"))
            .map_err(|_| corrupt())?
    else {
        return Err(corrupt());
    };
    if expressions.len() != count {
        return Err(corrupt());
    }
    Ok(expressions)
}

/// `pg_proc.proallargtypes` — every parameter's type, in declaration order.
fn catalog_all_types(kv: &dyn Kv, routine: &Routine) -> Result<Vec<Datum>, ExecError> {
    let mut out: Vec<Datum> = routine
        .params
        .iter()
        .map(|param| Ok(Datum::Int4(catalog_type_oid(kv, &param.ty)?)))
        .collect::<Result<_, ExecError>>()?;
    if let RoutineResult::Table(columns) = &routine.result {
        out.extend(
            columns
                .iter()
                .map(|(_, ty)| Ok(Datum::Int4(catalog_type_oid(kv, ty)?)))
                .collect::<Result<Vec<_>, ExecError>>()?,
        );
    }
    Ok(out)
}

/// `pg_proc.proargmodes`: `t` for a `RETURNS TABLE` column, as `PostgreSQL`
/// records it.
fn catalog_all_modes(routine: &Routine) -> Vec<Datum> {
    let mut out: Vec<Datum> = routine
        .params
        .iter()
        .map(|param| Datum::Text(param.mode.catalog_code().to_string()))
        .collect();
    if let RoutineResult::Table(columns) = &routine.result {
        out.extend(columns.iter().map(|_| Datum::Text("t".to_string())));
    }
    out
}

/// `pg_type.oid` for every name [`KNOWN_TYPE_NAMES`] accepts, so `pg_proc`
/// reports the type a routine signature names even when Gres cannot produce a
/// value of it. Taken from PostgreSQL 18.4's own `pg_type`.
pub(crate) const TYPE_OIDS: &[(&str, i32)] = &[
    ("aclitem", 1033),
    ("any", 2276),
    ("anyarray", 2277),
    ("anycompatible", 5077),
    ("anycompatiblearray", 5078),
    ("anycompatiblemultirange", 4538),
    ("anycompatiblenonarray", 5079),
    ("anycompatiblerange", 5080),
    ("anyelement", 2283),
    ("anyenum", 3500),
    ("anymultirange", 4537),
    ("anynonarray", 2776),
    ("anyrange", 3831),
    ("bigint", 20),
    ("bit varying", 1562),
    ("bit", 1560),
    ("bool", 16),
    ("boolean", 16),
    ("box", 603),
    ("bpchar", 1042),
    ("bytea", 17),
    ("char", 18),
    ("character varying", 1043),
    ("character", 1042),
    ("cid", 29),
    ("cidr", 650),
    ("circle", 718),
    ("cstring", 2275),
    ("date", 1082),
    ("datemultirange", 4535),
    ("daterange", 3912),
    ("decimal", 1700),
    ("double precision", 701),
    ("event_trigger", 3838),
    ("fdw_handler", 3115),
    ("float", 701),
    ("float4", 700),
    ("float8", 701),
    ("gtsvector", 3642),
    ("index_am_handler", 325),
    ("inet", 869),
    ("int", 23),
    ("int2", 21),
    ("int2vector", 22),
    ("int4", 23),
    ("int4multirange", 4451),
    ("int4range", 3904),
    ("int8", 20),
    ("int8multirange", 4536),
    ("int8range", 3926),
    ("integer", 23),
    ("internal", 2281),
    ("interval", 1186),
    ("json", 114),
    ("jsonb", 3802),
    ("jsonpath", 4072),
    ("language_handler", 2280),
    ("line", 628),
    ("lseg", 601),
    ("macaddr", 829),
    ("macaddr8", 774),
    ("money", 790),
    ("name", 19),
    ("numeric", 1700),
    ("nummultirange", 4532),
    ("numrange", 3906),
    ("oid", 26),
    ("oidvector", 30),
    ("path", 602),
    ("pg_brin_bloom_summary", 4600),
    ("pg_brin_minmax_multi_summary", 4601),
    ("pg_ddl_command", 32),
    ("pg_dependencies", 3402),
    ("pg_lsn", 3220),
    ("pg_mcv_list", 5017),
    ("pg_ndistinct", 3361),
    ("pg_node_tree", 194),
    ("pg_snapshot", 5038),
    ("point", 600),
    ("polygon", 604),
    ("real", 700),
    ("record", 2249),
    ("refcursor", 1790),
    ("regclass", 2205),
    ("regcollation", 4191),
    ("regconfig", 3734),
    ("regdictionary", 3769),
    ("regnamespace", 4089),
    ("regoper", 2203),
    ("regoperator", 2204),
    ("regproc", 24),
    ("regprocedure", 2202),
    ("regrole", 4096),
    ("regtype", 2206),
    ("smallint", 21),
    ("table_am_handler", 269),
    ("text", 25),
    ("tid", 27),
    ("time with time zone", 1266),
    ("time without time zone", 1083),
    ("time", 1083),
    ("timestamp with time zone", 1184),
    ("timestamp without time zone", 1114),
    ("timestamp", 1114),
    ("timestamptz", 1184),
    ("timetz", 1266),
    ("trigger", 2279),
    ("tsm_handler", 3310),
    ("tsmultirange", 4533),
    ("tsquery", 3615),
    ("tsrange", 3908),
    ("tstzmultirange", 4534),
    ("tstzrange", 3910),
    ("tsvector", 3614),
    ("txid_snapshot", 2970),
    ("unknown", 705),
    ("uuid", 2950),
    ("varbit", 1562),
    ("varchar", 1043),
    ("void", 2278),
    ("xid", 28),
    ("xid8", 5069),
    ("xml", 142),
];

/// The `pg_type.oid` of a signature type name, or `0` for a relation's
/// composite type, whose oid Gres does not model.
fn named_type_oid(name: &str) -> i32 {
    TYPE_OIDS
        .iter()
        .find(|(candidate, _)| *candidate == name)
        .map_or(0, |(_, oid)| *oid)
}

/// A routine's `pg_proc.proargtypes` — the input parameters' type oids, in
/// declaration order. The same list [`user_pg_proc_rows`] publishes, so a
/// caller comparing signatures against `pg_proc` reads the same values.
pub(crate) fn routine_arg_type_oids(kv: &dyn Kv, routine: &Routine) -> Result<Vec<i32>, ExecError> {
    routine
        .input_params()
        .map(|param| catalog_type_oid(kv, &param.ty))
        .collect()
}

/// A signature type OID from this database's catalog.  Relation types get the
/// OID of their catalog-owned composite (or its array companion), while the
/// static table remains the source for PostgreSQL's built-ins.
fn catalog_type_oid(kv: &dyn Kv, ty: &RoutineType) -> Result<i32, ExecError> {
    if ty.column.is_some() {
        return Ok(type_oid(ty));
    }
    let array = ty.name.ends_with("[]");
    let base = ty.name.strip_suffix("[]").unwrap_or(&ty.name);
    if let oid @ 1.. = named_type_oid(base) {
        return Ok(oid);
    }
    let resolution = crate::relname::ResolutionScope::default_scope();
    let Ok(written) = crate::relname::parse_written_relation(resolution, base) else {
        return Ok(0);
    };
    let Ok(relation) = crate::relname::resolve_relation(
        kv,
        resolution,
        &written.reference,
        crate::relname::SchemaDisposition::Reference,
    ) else {
        return Ok(0);
    };
    Ok(crate::catalog_rel::relation_rowtype_oids(kv)?
        .get(&relation)
        .map_or(
            0,
            |(rowtype, array_type)| if array { *array_type } else { *rowtype },
        ))
}

/// A signature type's `pg_type.oid`; `0` for a relation's composite type.
pub(crate) fn type_oid(ty: &RoutineType) -> i32 {
    ty.column.map_or_else(
        || named_type_oid(&ty.name),
        |column| i32::try_from(column.oid()).unwrap_or(0),
    )
}

/// Resolve a `pg_proc` result OID to the routine-signature type it represents.
pub(crate) fn routine_type_from_oid(oid: u32) -> Result<RoutineType, ExecError> {
    if let Ok(column) = crate::exec::column_type_from_oid(oid) {
        return Ok(RoutineType::builtin(column));
    }
    TYPE_OIDS
        .iter()
        .find(|(_, candidate)| *candidate == i32::try_from(oid).unwrap_or(0))
        .map(|(name, _)| RoutineType::named((*name).to_string()))
        .ok_or_else(|| ExecError::Unsupported(format!("unknown routine result type oid {oid}")))
}

/// `pg_proc.prorettype`.
fn catalog_return_type_oid(kv: &dyn Kv, routine: &Routine) -> Result<i32, ExecError> {
    match &routine.result {
        RoutineResult::Type { ty, .. } => catalog_type_oid(kv, ty),
        RoutineResult::Table(_) => Ok(2249),
        RoutineResult::Unspecified => {
            let outputs: Vec<&RoutineParam> = routine.output_params().collect();
            match outputs.as_slice() {
                [] => Ok(2278),
                [only] => catalog_type_oid(kv, &only.ty),
                _ => Ok(2249),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::MemKv;
    use crabka_pgparser::ast::{QueryBody, SetExpr, Statement};
    use crabka_pgwire::engine::{Engine, Session};

    use super::*;

    #[test]
    fn atomic_return_rewrite_is_safe_and_selective() {
        assert!(atomic_execution_source(";; RETURN false") == "SELECT false");
        assert!(atomic_execution_source("SELECT false") == "SELECT false");
        assert!(atomic_execution_source("returning false") == "returning false");
        assert!(atomic_execution_source("ret") == "ret");
    }

    #[test]
    fn atomic_return_keeps_its_catalog_spelling() {
        let routine = defined(
            &MemKv::default(),
            "CREATE FUNCTION atomic_catalog() RETURNS boolean LANGUAGE sql \
             BEGIN ATOMIC ;;RETURN false;; END",
        );
        assert!(render_functiondef(&routine).contains("BEGIN ATOMIC\n RETURN false;\nEND"));
    }

    #[test]
    fn routine_references_resolve_integer_and_text_forms_but_reject_ambiguous_names() {
        let kv = MemKv::default();
        let routine = defined(
            &kv,
            "CREATE FUNCTION reference_target() RETURNS int LANGUAGE sql AS 'SELECT 1'",
        );
        for reference in [
            Datum::Int4(i32::try_from(routine.oid).expect("catalog oid fits i32")),
            Datum::Int8(i64::from(routine.oid)),
            Datum::Regclass(crabka_pgtypes::RegclassValue::unresolved(
                i32::try_from(routine.oid).expect("catalog oid fits i32"),
            )),
            Datum::Text("reference_target".into()),
        ] {
            assert!(
                routine_by_reference(&kv, &reference)
                    .expect("lookup")
                    .is_some_and(|found| found.oid == routine.oid)
            );
        }

        defined(
            &kv,
            "CREATE FUNCTION ambiguous_reference(int) RETURNS int LANGUAGE sql AS 'SELECT $1'",
        );
        defined(
            &kv,
            "CREATE FUNCTION ambiguous_reference(text) RETURNS text LANGUAGE sql AS 'SELECT $1'",
        );
        assert!(
            routine_by_reference(&kv, &Datum::Text("ambiguous_reference".into()))
                .expect("lookup")
                .is_none()
        );
    }

    #[test]
    fn duplicate_function_bodies_are_rejected() {
        let kv = MemKv::default();
        assert!(matches!(
            define(
                &kv,
                "CREATE FUNCTION duplicate_body() RETURNS int LANGUAGE sql \
                 AS $$ SELECT 1 $$ RETURN 2",
            ),
            Err(ExecError::FunctionError { sqlstate: "42P13", message })
                if message == "duplicate function body specified"
        ));
    }

    #[test]
    fn rows_must_be_positive() {
        let kv = MemKv::default();
        assert!(matches!(
            define(
                &kv,
                "CREATE FUNCTION nonpositive_rows() RETURNS SETOF int LANGUAGE sql \
                 ROWS 0 AS $$ SELECT 1 $$",
            ),
            Err(ExecError::FunctionError { sqlstate: "42P13", message })
                if message == "ROWS must be positive"
        ));
    }

    #[test]
    fn unquoted_sql_bodies_reject_polymorphic_signatures() {
        let kv = MemKv::default();
        for sql in [
            "CREATE FUNCTION polymorphic_return(x anyarray) RETURNS anyelement \
             LANGUAGE sql RETURN x[1]",
            "CREATE FUNCTION polymorphic_input(x anyarray) RETURNS int \
             LANGUAGE sql RETURN 1",
        ] {
            assert!(matches!(
                define(&kv, sql),
                Err(ExecError::FunctionError { sqlstate: "42P13", message })
                    if message == "SQL function with unquoted function body cannot have polymorphic arguments"
            ));
        }
        defined(
            &kv,
            "CREATE FUNCTION plain_return(x int) RETURNS int LANGUAGE sql RETURN x",
        );
    }

    #[test]
    fn builtin_c_routines_have_a_nonempty_probin() {
        let rows = builtin_pg_proc_rows().expect("built-in pg_proc rows");
        assert!(rows.iter().all(|row| {
            if row[4] == Datum::Int4(13) {
                matches!(&row[26], Datum::Text(value) if !value.is_empty() && value != "-")
            } else {
                row[26] == Datum::Null
            }
        }));
    }

    #[test]
    fn builtin_support_functions_are_catalog_oids() {
        let rows = builtin_pg_proc_rows().expect("built-in pg_proc rows");
        let starts_with = rows
            .iter()
            .find(|row| row[0] == Datum::Int4(3696))
            .expect("starts_with row");
        assert!(starts_with[8] == Datum::Int4(6242));
        assert!(rows.iter().all(|row| matches!(row[8], Datum::Int4(_))));
    }

    #[test]
    fn builtin_routines_preserve_catalog_sources() {
        let rows = builtin_pg_proc_rows().expect("built-in pg_proc rows");
        for (oid, source) in [
            (77, "chartoi4"),
            (313, "i2toi4"),
            (668, "bpchar"),
            (1242, "boolin"),
        ] {
            let row = rows
                .iter()
                .find(|row| row[0] == Datum::Int4(oid))
                .expect("catalog routine");
            assert!(row[25] == Datum::Text(source.to_string()));
        }
    }

    #[test]
    fn builtin_sql_routines_keep_their_sql_body() {
        let rows = builtin_pg_proc_rows().expect("built-in pg_proc rows");
        let lpad = rows
            .iter()
            .find(|row| row[0] == Datum::Int4(879))
            .expect("lpad row");
        assert!(lpad[25] == Datum::Text(String::new()));
        assert!(matches!(&lpad[27], Datum::Text(body) if !body.is_empty()));
    }

    #[test]
    fn builtin_routines_preserve_catalog_argument_modes() {
        let rows = builtin_pg_proc_rows().expect("built-in pg_proc rows");
        let concat_ws = rows
            .iter()
            .find(|row| row[0] == Datum::Int4(3059))
            .expect("concat_ws row");
        assert!(
            concat_ws[21]
                == Datum::Array(crabka_pgtypes::ArrayValue::new(
                    crabka_pgtypes::ElemType::Text,
                    vec![Datum::Text("i".into()), Datum::Text("v".into())],
                ))
        );
        let boolin = rows
            .iter()
            .find(|row| row[0] == Datum::Int4(1242))
            .expect("boolin row");
        assert!(boolin[21] == Datum::Null);
        let aclexplode = rows
            .iter()
            .find(|row| row[0] == Datum::Int4(1689))
            .expect("aclexplode row");
        assert!(
            aclexplode[20]
                == Datum::Array(crabka_pgtypes::ArrayValue::new(
                    crabka_pgtypes::ElemType::Int4,
                    vec![1034, 26, 26, 25, 16]
                        .into_iter()
                        .map(Datum::Int4)
                        .collect(),
                ))
        );
        assert!(
            aclexplode[22]
                == Datum::Array(crabka_pgtypes::ArrayValue::new(
                    crabka_pgtypes::ElemType::Text,
                    [
                        "acl",
                        "grantor",
                        "grantee",
                        "privilege_type",
                        "is_grantable"
                    ]
                    .into_iter()
                    .map(|name| Datum::Text(name.into()))
                    .collect(),
                ))
        );
    }

    #[test]
    fn returns_table_catalog_names_include_output_columns() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION tab(n int) RETURNS TABLE(a int, b text) AS 'SELECT 1' LANGUAGE sql",
        )
        .expect("define table function");
        let rows = pg_proc_rows(&kv).expect("pg_proc rows");
        let row = rows
            .iter()
            .find(|row| row[1] == Datum::Text("tab".into()))
            .expect("table function row");
        assert!(
            row[22]
                == Datum::Array(crabka_pgtypes::ArrayValue::new(
                    crabka_pgtypes::ElemType::Text,
                    vec![
                        Datum::Text("n".into()),
                        Datum::Text("a".into()),
                        Datum::Text("b".into()),
                    ],
                ))
        );
    }

    /// Run `sql` as a definition against `kv`, returning the completion tag.
    fn define(kv: &MemKv, sql: &str) -> Result<String, ExecError> {
        let statements = crabka_pgparser::parse(sql).expect("definition parses");
        let [Statement::CreateRoutine(stmt)] = statements.as_slice() else {
            panic!("{sql} is not a routine definition");
        };
        let (result, ops) = create(
            kv,
            &crate::relname::ResolutionScope::default_scope(),
            stmt,
            "crab",
            true,
        )?;
        kv.write_batch(&ops).expect("write");
        match result {
            QueryResult::Command { tag } => Ok(tag),
            other => panic!("expected a command tag, got {other:?}"),
        }
    }

    fn defined(kv: &MemKv, sql: &str) -> Routine {
        define(kv, sql).expect("definition succeeds");
        list_routines(kv)
            .expect("catalog")
            .pop()
            .expect("one routine")
    }

    fn seeded() -> MemKv {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION add2(a int, b int) RETURNS int AS 'SELECT $1 + $2' LANGUAGE sql",
        )
        .expect("add2");
        kv
    }

    #[tokio::test]
    async fn routine_composite_results_follow_the_session_search_path() {
        let mut session = crate::SqlEngine::new().connect();
        session
            .simple_query("CREATE TEMP TABLE foo (id int)")
            .await
            .expect("temporary relation");
        let result = session
            .simple_query(
                "CREATE FUNCTION foo_rows() RETURNS SETOF foo \
                 AS $$ SELECT * FROM foo $$ LANGUAGE sql",
            )
            .await
            .expect("temporary composite result");
        assert!(
            matches!(result.as_slice(), [QueryResult::Command { tag }] if tag == "CREATE FUNCTION")
        );
    }

    #[tokio::test]
    async fn session_binds_named_default_and_variadic_function_arguments() {
        let mut session = crate::SqlEngine::new().connect();
        session
            .simple_query(
                "CREATE FUNCTION named_default(a int, b int DEFAULT 2) RETURNS int \
                 LANGUAGE sql AS 'SELECT a + b'; \
                 CREATE FUNCTION variadic_len(VARIADIC values int[]) RETURNS int \
                 LANGUAGE sql AS 'SELECT array_length(values, 1)'; \
                 CREATE FUNCTION table_default(a int, b int DEFAULT 2) \
                 RETURNS TABLE (first int, second int) LANGUAGE sql AS 'SELECT a, b'",
            )
            .await
            .expect("function definitions");

        let result = session
            .simple_query(
                "SELECT named_default(b => 4, a => 3), named_default(4), \
                 variadic_len(1, 2, 3), variadic_len(VARIADIC ARRAY[5, 6]), \
                 make_interval(days => 2)",
            )
            .await
            .expect("named, default, and variadic calls");
        let [QueryResult::Rows { rows, .. }] = result.as_slice() else {
            panic!("expected one result row")
        };
        let [row] = rows.as_slice() else {
            panic!("expected one row")
        };
        let values: Vec<String> = row
            .iter()
            .map(|cell| {
                String::from_utf8(cell.as_ref().expect("non-null result").text.to_vec())
                    .expect("utf8 result")
            })
            .collect();
        assert!(values == ["7", "6", "3", "2", "2 days"]);

        let result = session
            .simple_query("SELECT * FROM table_default(b => 9, a => 8)")
            .await
            .expect("named table function call");
        let [QueryResult::Rows { rows, .. }] = result.as_slice() else {
            panic!("expected table function rows")
        };
        assert!(
            rows.iter()
                .map(|row| {
                    row.iter()
                        .map(|cell| {
                            String::from_utf8(cell.as_ref().expect("non-null result").text.to_vec())
                                .expect("utf8 result")
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
                == vec![vec!["8".to_string(), "9".to_string()]]
        );

        session
            .simple_query(
                "CREATE TABLE procedure_args (a int, b text); \
                 CREATE PROCEDURE procedure_default(a int, b text, c int DEFAULT 100) \
                 LANGUAGE sql AS $$ \
                   INSERT INTO procedure_args VALUES (a, b); \
                   INSERT INTO procedure_args VALUES (c, b); \
                 $$; \
                 CREATE PROCEDURE procedure_variadic(VARIADIC values int[]) LANGUAGE sql AS \
                   'INSERT INTO procedure_args VALUES (array_length(values, 1), ''variadic'')'",
            )
            .await
            .expect("procedure definitions");
        session
            .simple_query(
                "CALL procedure_default(b => 'named', a => 10); \
                 CALL procedure_variadic(1, 2, 3); \
                 CALL procedure_variadic(VARIADIC ARRAY[4, 5])",
            )
            .await
            .expect("named and variadic procedure calls");
        let result = session
            .simple_query("SELECT a, b FROM procedure_args ORDER BY a, b")
            .await
            .expect("procedure effects");
        let [QueryResult::Rows { rows, .. }] = result.as_slice() else {
            panic!("expected procedure rows")
        };
        assert!(
            rows.iter()
                .map(|row| {
                    row.iter()
                        .map(|cell| {
                            String::from_utf8(cell.as_ref().expect("non-null result").text.to_vec())
                                .expect("utf8 result")
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
                == vec![
                    vec!["2".to_string(), "variadic".to_string()],
                    vec!["3".to_string(), "variadic".to_string()],
                    vec!["10".to_string(), "named".to_string()],
                    vec!["100".to_string(), "named".to_string()],
                ]
        );
    }

    #[test]
    fn a_plpgsql_set_function_expands_as_one_select_list_column() {
        let kv = std::sync::Arc::new(MemKv::default());
        define(
            kv.as_ref(),
            "CREATE FUNCTION set_result() RETURNS SETOF int LANGUAGE plpgsql AS \
             $$ BEGIN RETURN NEXT 1; RETURN NEXT 2; END $$",
        )
        .expect("definition");
        let catalog: std::sync::Arc<dyn Kv> = kv;
        let call = FuncCall {
            sql_syntax: false,
            name: "set_result".into(),
            distinct: false,
            args: FuncArgs::Exprs(Vec::new()),
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        };
        let (request_tx, mut request_rx) = tokio::sync::mpsc::channel::<ScalarFunctionRequest>(1);
        let worker = std::thread::spawn(move || {
            let request = request_rx.blocking_recv().expect("table request");
            assert!(matches!(
                request.kind,
                FunctionRequestKind::Table(columns)
                    if columns == vec![("set_result".into(), ColumnType::Int4)]
            ));
            request
                .reply
                .send(Ok((
                    FunctionRequestResult::Table(vec![vec![Datum::Int4(1)], vec![Datum::Int4(2)]]),
                    Vec::new(),
                )))
                .expect("reply");
        });
        let rows = with_scalar_runtime(&catalog, Some(request_tx), || {
            assert!(
                plpgsql_set_result_type(&call, &crate::scope::Scope::empty())
                    .expect("user routine")
                    .expect("type")
                    == ColumnType::Int4
            );
            assert!(is_plpgsql_set_runtime(&call));
            eval_plpgsql_set_function(
                &call,
                &crate::scope::Scope::empty(),
                &[],
                &crate::clock::EvalCtx::test_default(),
            )
            .expect("user routine")
            .expect("rows")
        });
        assert!(rows == vec![Datum::Int4(1), Datum::Int4(2)]);
        assert!(worker.join().is_ok());
    }

    #[test]
    fn set_functions_use_the_scalar_runtime_and_reject_distinct() {
        let kv = std::sync::Arc::new(MemKv::default());
        define(
            kv.as_ref(),
            "CREATE FUNCTION sql_set() RETURNS SETOF int LANGUAGE sql AS 'SELECT 1'",
        )
        .expect("definition");
        define(
            kv.as_ref(),
            "CREATE FUNCTION plpgsql_set() RETURNS SETOF int LANGUAGE plpgsql AS \
             $$ BEGIN RETURN NEXT 1; END $$",
        )
        .expect("definition");
        let catalog: std::sync::Arc<dyn crabka_pgkv::Kv> = kv;
        let call = |name: &str, distinct| FuncCall {
            sql_syntax: false,
            name: name.into(),
            distinct,
            args: FuncArgs::Exprs(Vec::new()),
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        };

        with_scalar_runtime(&catalog, None, || {
            let sql_call = call("sql_set", false);
            assert!(
                plpgsql_set_result_type(&sql_call, &crate::scope::Scope::empty())
                    .expect("user routine")
                    .expect("SQL set function type")
                    == ColumnType::Int4
            );
            let error = eval_plpgsql_set_function(
                &sql_call,
                &crate::scope::Scope::empty(),
                &[],
                &crate::clock::EvalCtx::test_default(),
            )
            .expect("user routine")
            .expect_err("SQL set function needs a session executor");
            assert!(
                error.into_pg().message == "PL/pgSQL table function requires a session executor"
            );

            let distinct_call = call("plpgsql_set", true);
            let error = eval_plpgsql_set_function(
                &distinct_call,
                &crate::scope::Scope::empty(),
                &[],
                &crate::clock::EvalCtx::test_default(),
            )
            .expect("user routine")
            .expect_err("DISTINCT is rejected before dispatch");
            assert!(
                error.into_pg().message
                    == "FILTER or DISTINCT is not allowed for function plpgsql_set"
            );
        });
    }

    #[test]
    fn strict_set_functions_only_short_circuit_for_null_arguments() {
        let kv = std::sync::Arc::new(MemKv::default());
        define(
            kv.as_ref(),
            "CREATE FUNCTION strict_set(a int) RETURNS SETOF int LANGUAGE plpgsql STRICT AS \
             $$ BEGIN RETURN NEXT a; END $$",
        )
        .expect("definition");
        let catalog: std::sync::Arc<dyn crabka_pgkv::Kv> = kv;
        let call = FuncCall {
            sql_syntax: false,
            name: "strict_set".into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![Expr::Const {
                value: Datum::Int4(1),
                ty: ColumnType::Int4,
            }]),
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        };
        let error = with_scalar_runtime(&catalog, None, || {
            eval_plpgsql_set_function(
                &call,
                &crate::scope::Scope::empty(),
                &[],
                &crate::clock::EvalCtx::test_default(),
            )
            .expect("user routine")
            .expect_err("a non-NULL strict call reaches the session executor")
        });
        assert!(error.into_pg().message == "PL/pgSQL table function requires a session executor");
    }

    #[test]
    fn select_list_set_results_require_exactly_one_declared_column() {
        let one_table = defined(
            &MemKv::default(),
            "CREATE FUNCTION one_table() RETURNS TABLE(a int) LANGUAGE plpgsql AS \
             $$ BEGIN RETURN NEXT 1; END $$",
        );
        let two_table = defined(
            &MemKv::default(),
            "CREATE FUNCTION two_table() RETURNS TABLE(a int, b text) LANGUAGE plpgsql AS \
             $$ BEGIN RETURN QUERY SELECT 1, 'x'; END $$",
        );
        let one_output = defined(
            &MemKv::default(),
            "CREATE FUNCTION one_output(OUT a int) LANGUAGE plpgsql AS $$ BEGIN a := 1; END $$",
        );
        let two_output = defined(
            &MemKv::default(),
            "CREATE FUNCTION two_output(OUT a int, OUT b text) LANGUAGE plpgsql AS \
             $$ BEGIN a := 1; b := 'x'; END $$",
        );

        assert!(single_set_result_column(&one_table, &[]) == Some(("a".into(), ColumnType::Int4)));
        assert!(single_set_result_column(&two_table, &[]).is_none());
        assert!(single_set_result_column(&one_output, &[]) == Some(("a".into(), ColumnType::Int4)));
        assert!(single_set_result_column(&two_output, &[]).is_none());
    }

    fn sqlstate(error: &ExecError) -> String {
        error.clone().into_pg().code
    }

    #[test]
    fn a_definition_reports_the_kind_it_used() {
        let kv = MemKv::default();
        assert!(
            define(
                &kv,
                "CREATE FUNCTION f() RETURNS int AS 'SELECT 1' LANGUAGE sql"
            )
            .expect("function")
                == "CREATE FUNCTION"
        );
        assert!(
            define(&kv, "CREATE PROCEDURE p() LANGUAGE sql AS 'SELECT 1'").expect("procedure")
                == "CREATE PROCEDURE"
        );
    }

    #[test]
    fn a_definition_is_stored_under_its_postgresql_identity() {
        let kv = seeded();
        let routine = get_routine(&kv, "add2(integer,integer)")
            .expect("read")
            .expect("stored");
        assert!(routine.name == "add2");
        assert!(routine.language == "sql");
        assert!(routine.body == "SELECT $1 + $2");
        assert!(routine.volatility == 'v');
        assert!(routine.parallel == 'u');
        assert!(!routine.strict);
        assert!((routine.cost - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn sql_standard_return_body_implies_sql_language() {
        let routine = defined(
            &MemKv::default(),
            "CREATE FUNCTION f(x int) RETURNS int IMMUTABLE RETURN x + 1",
        );
        assert!(routine.language == "sql");
        assert!(routine.body_form == BodyForm::Return);
    }

    #[test]
    fn definition_errors_carry_postgresqls_sqlstates() {
        let cases: Vec<(&str, &str, &str)> = vec![
            (
                "CREATE FUNCTION add2(a int, b int) RETURNS int AS 'SELECT 0' LANGUAGE sql",
                "42723",
                "already exists with same argument types",
            ),
            (
                "CREATE OR REPLACE FUNCTION add2(a int, b int) RETURNS bigint AS 'SELECT 0' \
                 LANGUAGE sql",
                "42P13",
                "cannot change return type of existing function",
            ),
            (
                "CREATE OR REPLACE FUNCTION add2(x int, y int) RETURNS int AS 'SELECT 0' \
                 LANGUAGE sql",
                "42P13",
                "cannot change name of input parameter \"a\"",
            ),
            (
                "CREATE FUNCTION g() RETURNS int AS 'SELECT 1'",
                "42P13",
                "no language specified",
            ),
            (
                "CREATE FUNCTION g() RETURNS int LANGUAGE sql",
                "42P13",
                "no function body specified",
            ),
            (
                "CREATE FUNCTION g() RETURNS int AS 'SELECT 1' LANGUAGE nosuchlang",
                "42704",
                "language \"nosuchlang\" does not exist",
            ),
            (
                "CREATE FUNCTION g(x nosuchtype) RETURNS int AS 'SELECT 1' LANGUAGE sql",
                "42704",
                "type nosuchtype does not exist",
            ),
        ];
        for (sql, state, fragment) in cases {
            let kv = seeded();
            let error = define(&kv, sql).expect_err(sql);
            assert!(sqlstate(&error) == state, "{sql}");
            assert!(error.clone().into_pg().message.contains(fragment), "{sql}");
        }
    }

    #[test]
    fn output_parameters_define_the_replacement_result_type() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION plain_result(IN a int) RETURNS int AS 'SELECT $1' LANGUAGE sql",
        )
        .expect("initial plain definition");
        let error = define(
            &kv,
            "CREATE OR REPLACE FUNCTION plain_result(IN a int) RETURNS bigint \
             AS 'SELECT $1' LANGUAGE sql",
        )
        .expect_err("changed plain result");
        assert!(sqlstate(&error) == "42P13");
        assert!(error.into_pg().message == "cannot change return type of existing function");
        define(
            &kv,
            "CREATE PROCEDURE no_result() LANGUAGE sql AS 'SELECT 1'",
        )
        .expect("initial procedure definition");
        define(
            &kv,
            "CREATE OR REPLACE PROCEDURE no_result() LANGUAGE sql AS 'SELECT 2'",
        )
        .expect("replacement procedure definition");

        define(
            &kv,
            "CREATE FUNCTION out_int(IN a int, OUT b int) AS 'SELECT $1' LANGUAGE sql",
        )
        .expect("initial scalar OUT definition");
        define(
            &kv,
            "CREATE OR REPLACE FUNCTION out_int(IN a int, OUT b int) RETURNS int \
             AS 'SELECT $1' LANGUAGE sql",
        )
        .expect("matching explicit result");
        let error = define(
            &kv,
            "CREATE OR REPLACE FUNCTION out_int(IN a int, OUT b int) RETURNS float \
             AS 'SELECT $1' LANGUAGE sql",
        )
        .expect_err("mismatched scalar result");
        assert!(sqlstate(&error) == "42P13");
        assert!(
            error.into_pg().message
                == "function result type must be integer because of OUT parameters"
        );
        let error = define(
            &kv,
            "CREATE OR REPLACE FUNCTION out_int(IN a int, OUT b int) RETURNS record \
             AS 'SELECT $1' LANGUAGE sql",
        )
        .expect_err("record result for one OUT parameter");
        assert!(sqlstate(&error) == "42P13");
        assert!(
            error.into_pg().message
                == "function result type must be integer because of OUT parameters"
        );

        define(
            &kv,
            "CREATE FUNCTION out_record(IN a int, OUT b int, OUT c text) \
             AS 'SELECT $1, $1::text' LANGUAGE sql",
        )
        .expect("initial record OUT definition");
        let mut named_record = get_routine(&kv, "out_record(integer)")
            .expect("catalog")
            .expect("routine");
        named_record.result = RoutineResult::Type {
            ty: RoutineType::named("record".into()),
            setof: false,
        };
        assert!(
            effective_result(&named_record)
                == RoutineResult::Type {
                    ty: RoutineType::builtin(ColumnType::Record(None)),
                    setof: false,
                }
        );
        let error = define(
            &kv,
            "CREATE OR REPLACE FUNCTION out_record(IN a int, OUT b int, OUT c text) RETURNS int \
             AS 'SELECT $1, $1::text' LANGUAGE sql",
        )
        .expect_err("non-record result");
        assert!(sqlstate(&error) == "42P13");
        assert!(
            error.into_pg().message
                == "function result type must be record because of OUT parameters"
        );
        define(
            &kv,
            "CREATE OR REPLACE FUNCTION out_record(IN a int, OUT b int, OUT c text) RETURNS record \
             AS 'SELECT $1, $1::text' LANGUAGE sql",
        )
        .expect("matching explicit record result");
        define(
            &kv,
            "CREATE FUNCTION out_set(IN a int, OUT b int, OUT c text) RETURNS SETOF record \
             AS 'SELECT $1, $1::text' LANGUAGE sql",
        )
        .expect("setof record OUT result");
    }

    #[test]
    fn polymorphic_literals_need_a_concrete_peer_but_opaque_inputs_defer() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION poly_second(anyelement, anyelement) RETURNS anyelement \
             AS 'SELECT $2' LANGUAGE sql",
        )
        .expect("definition");
        let error = resolve_call(&kv, "poly_second", &[ArgType::Unknown, ArgType::Unknown])
            .expect_err("two unknown arguments");
        assert!(sqlstate(&error) == "42804");
        assert!(
            resolve_call(
                &kv,
                "poly_second",
                &[ArgType::Unknown, ArgType::Known(ColumnType::Int4)],
            )
            .expect("mixed arguments")
            .is_some()
        );
        assert!(
            resolve_call(&kv, "poly_second", &[ArgType::Opaque, ArgType::Opaque])
                .expect("opaque arguments defer")
                .is_some()
        );
    }

    #[test]
    fn external_definition_errors_leave_no_catalog_residue() {
        let kv = MemKv::default();
        let directory = tempfile::tempdir().expect("temporary directory");
        let missing = directory.path().join("missing.so");
        let cases = [
            (
                format!(
                    "CREATE FUNCTION test1(int) RETURNS int AS '{}' LANGUAGE C",
                    missing.display()
                ),
                "58P01",
                format!(
                    "could not access file \"{}\": No such file or directory",
                    missing.display()
                ),
            ),
            (
                "CREATE FUNCTION test1(int) RETURNS int AS 'regress', 'nosuchsymbol' LANGUAGE C"
                    .to_string(),
                "42883",
                "could not find function \"nosuchsymbol\" in file \"regress\"".to_string(),
            ),
            (
                "CREATE FUNCTION test1(int) RETURNS int AS 'nosuch' LANGUAGE internal".to_string(),
                "42883",
                "there is no built-in function named \"nosuch\"".to_string(),
            ),
        ];

        for (sql, state, message) in cases {
            let error = define(&kv, &sql).expect_err(&sql);
            let error = error.into_pg();
            assert!(error.code == state, "{sql}");
            assert!(error.message == message, "{sql}");
            assert!(
                list_routines(&kv)
                    .expect("catalog remains readable")
                    .is_empty()
            );
        }
    }

    #[test]
    fn external_sources_project_link_symbol_and_object_file_separately() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION public.binary_coercible(oid, oid) RETURNS bool \
             AS 'regress' LANGUAGE C",
        )
        .expect("one-string C definition");
        define(
            &kv,
            "CREATE FUNCTION c_alias(oid, oid) RETURNS bool \
             AS 'regress', 'pg_finfo_binary_coercible' LANGUAGE C",
        )
        .expect("two-string C definition");
        define(
            &kv,
            "CREATE FUNCTION nth_value(anyelement, int4) RETURNS anyelement \
             AS 'window_nth_value' LANGUAGE internal",
        )
        .expect("pinned internal definition");

        let routines = list_routines(&kv).expect("catalog");
        let binary = routines
            .iter()
            .find(|routine| routine.name == "binary_coercible")
            .expect("binary_coercible");
        assert!(binary.name == "binary_coercible");
        assert!(binary.body == "binary_coercible");
        assert!(binary.object_file == Some("regress".into()));

        let alias = routines
            .iter()
            .find(|routine| routine.name == "c_alias")
            .expect("alias");
        assert!(alias.body == "pg_finfo_binary_coercible");
        assert!(alias.object_file == Some("regress".into()));
        assert!(render_functiondef(alias).contains("AS 'regress', 'pg_finfo_binary_coercible'"));

        let internal = routines
            .iter()
            .find(|routine| routine.name == "nth_value")
            .expect("internal");
        assert!(internal.body == "window_nth_value");
        assert!(internal.object_file.is_none());

        let rows = pg_proc_rows(&kv).expect("pg_proc");
        for (name, source, binary) in [
            ("binary_coercible", "binary_coercible", "regress"),
            ("c_alias", "pg_finfo_binary_coercible", "regress"),
        ] {
            let row = rows
                .iter()
                .find(|row| row[1] == Datum::Text(name.into()))
                .expect("projected C routine");
            assert!(row[25] == Datum::Text(source.into()));
            assert!(row[26] == Datum::Text(binary.into()));
        }
    }

    #[test]
    fn or_replace_keeps_the_oid_and_swaps_the_body() {
        let kv = seeded();
        let before = get_routine(&kv, "add2(integer,integer)")
            .expect("read")
            .expect("stored");
        define(
            &kv,
            "CREATE OR REPLACE FUNCTION add2(a int, b int) RETURNS int AS 'SELECT $1 - $2' \
             LANGUAGE sql",
        )
        .expect("replacement");
        let after = get_routine(&kv, "add2(integer,integer)")
            .expect("read")
            .expect("stored");
        assert!(after.oid == before.oid);
        assert!(after.body == "SELECT $1 - $2");
    }

    #[test]
    fn resolution_prefers_an_exact_match_then_an_implicit_coercion() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION ov(x int) RETURNS text AS $$ SELECT 'int' $$ LANGUAGE sql",
        )
        .expect("int overload");
        define(
            &kv,
            "CREATE FUNCTION ov(x text) RETURNS text AS $$ SELECT 'text' $$ LANGUAGE sql",
        )
        .expect("text overload");

        let by_int = resolve_call(&kv, "ov", &[ArgType::Known(ColumnType::Int4)])
            .expect("resolves")
            .expect("a candidate");
        assert!(by_int.input_type_names() == vec!["integer".to_string()]);

        // An untyped literal takes the string category's preferred type.
        let by_unknown = resolve_call(&kv, "ov", &[ArgType::Unknown])
            .expect("resolves")
            .expect("a candidate");
        assert!(by_unknown.input_type_names() == vec!["text".to_string()]);

        // `bigint` has no implicit cast to `text`, so only the `int` overload
        // is a candidate — PostgreSQL rejects the call outright.
        let error = resolve_call(&kv, "ov", &[ArgType::Known(ColumnType::Int8)])
            .expect_err("bigint matches nothing");
        assert!(sqlstate(&error) == "42883");
        assert!(
            error.clone().into_pg().message == "function ov(bigint) does not exist",
            "{error:?}"
        );
    }

    #[test]
    fn two_equally_good_candidates_are_ambiguous() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION amb(x int, y text) RETURNS int AS 'SELECT 1' LANGUAGE sql",
        )
        .expect("first");
        define(
            &kv,
            "CREATE FUNCTION amb(x text, y int) RETURNS int AS 'SELECT 2' LANGUAGE sql",
        )
        .expect("second");
        let error =
            resolve_call(&kv, "amb", &[ArgType::Unknown, ArgType::Unknown]).expect_err("ambiguous");
        assert!(sqlstate(&error) == "42725");
        assert!(error.into_pg().message == "function amb(unknown, unknown) is not unique");
    }

    #[test]
    fn polymorphic_range_signatures_link_arrays_to_range_subtypes() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION exact_poly(a anyarray, r anyrange) RETURNS anyelement AS 'SELECT 1' LANGUAGE sql",
        )
        .expect("traditional polymorphic function");
        define(
            &kv,
            "CREATE FUNCTION compatible_poly(a anycompatiblearray, r anycompatiblerange) RETURNS anycompatible AS 'SELECT 1' LANGUAGE sql",
        )
        .expect("compatible polymorphic function");
        define(
            &kv,
            "CREATE FUNCTION multirange_poly(a anyarray, r anymultirange) RETURNS anyelement AS 'SELECT 1' LANGUAGE sql",
        )
        .expect("multirange polymorphic function");
        define(
            &kv,
            "CREATE FUNCTION range_overload(r anyrange) RETURNS anyelement AS 'SELECT 1' LANGUAGE sql",
        )
        .expect("range overload");
        define(
            &kv,
            "CREATE FUNCTION range_overload(r anymultirange) RETURNS anyelement AS 'SELECT 1' LANGUAGE sql",
        )
        .expect("multirange overload");
        let int4range =
            ColumnType::builtin_range(crabka_pgtypes::oids::INT4RANGE).expect("int4range");
        let numrange = ColumnType::builtin_range(crabka_pgtypes::oids::NUMRANGE).expect("numrange");
        let ints = ArgType::Known(ColumnType::Array(crabka_pgtypes::ElemType::Int4));

        assert!(
            resolve_call(&kv, "exact_poly", &[ints, ArgType::Known(int4range)])
                .expect("matching subtype")
                .is_some()
        );
        assert!(resolve_call(&kv, "exact_poly", &[ints, ArgType::Known(numrange)]).is_err());
        let int4multirange = ColumnType::builtin_multirange(crabka_pgtypes::oids::INT4MULTIRANGE)
            .expect("int4multirange");
        let nummultirange = ColumnType::builtin_multirange(crabka_pgtypes::oids::NUMMULTIRANGE)
            .expect("nummultirange");
        assert!(
            resolve_call(
                &kv,
                "multirange_poly",
                &[ints, ArgType::Known(int4multirange)]
            )
            .expect("matching multirange subtype")
            .is_some()
        );
        assert!(
            resolve_call(
                &kv,
                "multirange_poly",
                &[ints, ArgType::Known(nummultirange)]
            )
            .is_err()
        );
        let selected = resolve_call(&kv, "range_overload", &[ArgType::Known(int4multirange)])
            .expect("unambiguous multirange overload")
            .expect("multirange overload");
        assert!(selected.input_type_names() == vec!["anymultirange".to_string()]);
        assert!(
            resolve_call(&kv, "compatible_poly", &[ints, ArgType::Known(numrange)])
                .expect("integer promotes to numeric")
                .is_some()
        );
        let numerics = ArgType::Known(ColumnType::Array(crabka_pgtypes::ElemType::Numeric));
        assert!(
            resolve_call(
                &kv,
                "compatible_poly",
                &[numerics, ArgType::Known(int4range)]
            )
            .is_err()
        );

        let error = define(
            &kv,
            "CREATE FUNCTION invalid_poly(a anyelement) RETURNS anyrange AS 'SELECT 1' LANGUAGE sql",
        )
        .expect_err("range result lacks range input");
        let rendered = error.into_pg();
        assert!(rendered.code == "42P13");
        assert!(rendered.message == "cannot determine result data type");
        assert!(
            rendered
                .diagnostics
                .as_deref()
                .and_then(|fields| fields.detail.as_deref())
                == Some(
                    "A result of type anyrange requires at least one input of type anyrange or anymultirange."
                )
        );
        let error = define(
            &kv,
            "CREATE FUNCTION invalid_multi(a anyelement) RETURNS anymultirange AS 'SELECT 1' LANGUAGE sql",
        )
        .expect_err("multirange result lacks range input");
        assert!(sqlstate(&error) == "42P13");

        let error = define(
            &kv,
            "CREATE FUNCTION invalid_element(a int, OUT value anyelement, OUT values anyarray) \
             AS 'SELECT a, ARRAY[a]' LANGUAGE sql",
        )
        .expect_err("element result lacks polymorphic input");
        let rendered = error.into_pg();
        assert!(rendered.code == "42P13");
        assert!(
            rendered
                .diagnostics
                .as_deref()
                .and_then(|fields| fields.detail.as_deref())
                == Some(
                    "A result of type anyelement requires at least one input of type anyelement, anyarray, anynonarray, anyenum, anyrange, or anymultirange."
                )
        );
        let error = define(
            &kv,
            "CREATE FUNCTION invalid_compatible(a anyarray, OUT value anycompatible, \
             OUT values anycompatiblearray) AS 'SELECT a[1], a' LANGUAGE sql",
        )
        .expect_err("compatible result lacks compatible input");
        assert!(sqlstate(&error) == "42P13");
    }

    #[test]
    fn an_unnamed_routine_resolves_to_nothing_rather_than_erroring() {
        let kv = seeded();
        assert!(
            resolve_call(&kv, "not_a_routine", &[])
                .expect("no error")
                .is_none()
        );
    }

    #[test]
    fn builtin_named_call_selects_one_coercible_signature() {
        let call = FuncCall {
            sql_syntax: false,
            name: "pg_terminate_backend".into(),
            distinct: false,
            args: FuncArgs::Named {
                positional: vec![Expr::IntLiteral("1".into())],
                named: vec![("timeout".into(), Expr::IntLiteral("1".into()))],
            },
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        };

        let call = normalize_builtin_named_call(&call)
            .expect("normalizes")
            .expect("built-in signature");
        assert!(matches!(call.args, FuncArgs::Exprs(args) if args.len() == 2));
    }

    #[test]
    fn defaults_widen_the_arity_a_call_may_use() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION d(a int, b int DEFAULT 2) RETURNS int AS 'SELECT $1 + $2' \
             LANGUAGE sql STRICT",
        )
        .expect("definition");
        let routine = get_routine(&kv, "d(integer,integer)")
            .expect("catalog")
            .expect("routine");
        assert_eq!(
            function_arg_default(&kv, routine.oid as i32, 0).expect("default"),
            None
        );
        assert_eq!(
            function_arg_default(&kv, routine.oid as i32, 1).expect("default"),
            Some("2".into())
        );
        assert_eq!(
            function_arg_default(&kv, routine.oid as i32, 2).expect("default"),
            None
        );
        for arity in [1_usize, 2] {
            let given = vec![ArgType::Known(ColumnType::Int4); arity];
            assert!(
                resolve_call(&kv, "d", &given).expect("resolves").is_some(),
                "arity {arity}"
            );
        }
        let bound = bind_call(
            &kv,
            "d",
            &[Expr::IntLiteral("1".into())],
            &[ArgType::Known(ColumnType::Int4)],
        )
        .expect("binds")
        .expect("routine");
        assert!(bound.routine.strict);
        assert!(bound.args == vec![Expr::IntLiteral("1".into()), Expr::IntLiteral("2".into())]);
        let error = resolve_call(&kv, "d", &[]).expect_err("no zero-argument overload");
        assert!(sqlstate(&error) == "42883");
    }

    #[test]
    fn variadic_routine_resolution_expands_element_arguments() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION variadic_len(VARIADIC values int[]) RETURNS int \
             LANGUAGE sql AS 'SELECT array_length(values, 1)'",
        )
        .expect("definition");
        let args = vec![
            Expr::IntLiteral("1".into()),
            Expr::IntLiteral("2".into()),
            Expr::IntLiteral("3".into()),
        ];
        let given = vec![ArgType::Known(ColumnType::Int4); args.len()];
        let bound = bind_call(&kv, "variadic_len", &args, &given)
            .expect("resolution")
            .expect("variadic routine");
        assert!(matches!(bound.args.as_slice(), [Expr::ArrayLiteral(values)] if values.len() == 3));
    }

    #[test]
    fn procedure_named_arguments_keep_output_placeholders_aligned() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE PROCEDURE named_output(OUT a int, IN b int, IN c int) \
             LANGUAGE sql AS 'SELECT b - c'",
        )
        .expect("definition");
        let bound = bind_procedure_call(
            &kv,
            "named_output",
            &[],
            &[
                ("c".into(), Expr::IntLiteral("2".into())),
                ("a".into(), Expr::NullLiteral),
                ("b".into(), Expr::IntLiteral("8".into())),
            ],
            None,
        )
        .expect("binding")
        .expect("procedure");
        assert!(matches!(
            bound.args.as_slice(),
            [Expr::NullLiteral, Expr::IntLiteral(b), Expr::IntLiteral(c)] if b == "8" && c == "2"
        ));
    }

    #[test]
    fn named_and_procedure_overloads_choose_the_postgresql_candidate() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION named_dispatch(a int) RETURNS int LANGUAGE sql AS 'SELECT 1'",
        )
        .expect("integer function");
        define(
            &kv,
            "CREATE FUNCTION named_dispatch(a text) RETURNS int LANGUAGE sql AS 'SELECT 2'",
        )
        .expect("text function");
        let call = FuncCall {
            sql_syntax: false,
            name: "named_dispatch".into(),
            distinct: false,
            args: FuncArgs::Named {
                positional: Vec::new(),
                named: vec![("a".into(), Expr::IntLiteral("1".into()))],
            },
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        };
        assert!(matches!(
            normalize_named_call(&kv, &call).expect("normalization"),
            Some(FuncCall { args: FuncArgs::Exprs(args), .. })
                if matches!(args.as_slice(), [Expr::IntLiteral(value)] if value == "1")
        ));

        define(
            &kv,
            "CREATE PROCEDURE procedure_dispatch(a int) LANGUAGE sql AS 'SELECT 1'",
        )
        .expect("integer procedure");
        define(
            &kv,
            "CREATE PROCEDURE procedure_dispatch(a text) LANGUAGE sql AS 'SELECT 1'",
        )
        .expect("text procedure");
        let exact = bind_procedure_call(
            &kv,
            "procedure_dispatch",
            &[Expr::IntLiteral("1".into())],
            &[],
            None,
        )
        .expect("binding")
        .expect("exact procedure");
        assert!(exact.routine.params[0].ty.column == Some(ColumnType::Int4));
        let unknown = bind_procedure_call(
            &kv,
            "procedure_dispatch",
            &[Expr::StringLiteral("one".into())],
            &[],
            None,
        )
        .expect("binding")
        .expect("text-preferred procedure");
        assert!(unknown.routine.params[0].ty.column == Some(ColumnType::Text));

        define(
            &kv,
            "CREATE PROCEDURE procedure_ambiguous(a int) LANGUAGE sql AS 'SELECT 1'",
        )
        .expect("integer procedure");
        define(
            &kv,
            "CREATE PROCEDURE procedure_ambiguous(a bigint) LANGUAGE sql AS 'SELECT 1'",
        )
        .expect("bigint procedure");
        let error = bind_procedure_call(
            &kv,
            "procedure_ambiguous",
            &[Expr::StringLiteral("one".into())],
            &[],
            None,
        )
        .expect_err("unknown input is ambiguous without a text overload");
        assert!(sqlstate(&error) == "42725");

        define(
            &kv,
            "CREATE PROCEDURE procedure_exact(a int) LANGUAGE sql AS 'SELECT 1'",
        )
        .expect("integer procedure");
        define(
            &kv,
            "CREATE PROCEDURE procedure_exact(a bigint) LANGUAGE sql AS 'SELECT 1'",
        )
        .expect("bigint procedure");
        let exact = bind_procedure_call(
            &kv,
            "procedure_exact",
            &[Expr::IntLiteral("1".into())],
            &[],
            None,
        )
        .expect("binding")
        .expect("exact procedure outranks coercion");
        assert!(exact.routine.params[0].ty.column == Some(ColumnType::Int4));

        define(
            &kv,
            "CREATE PROCEDURE procedure_coercible(a bigint) LANGUAGE sql AS 'SELECT 1'",
        )
        .expect("bigint procedure");
        let coercible = bind_procedure_call(
            &kv,
            "procedure_coercible",
            &[Expr::IntLiteral("1".into())],
            &[],
            None,
        )
        .expect("binding")
        .expect("implicit integer coercion");
        assert!(coercible.routine.params[0].ty.column == Some(ColumnType::Int8));

        define(
            &kv,
            "CREATE PROCEDURE procedure_arity(a int, b int) LANGUAGE sql AS 'SELECT 1'",
        )
        .expect("arity procedure");
        let error = bind_procedure_call(
            &kv,
            "procedure_arity",
            &[
                Expr::IntLiteral("1".into()),
                Expr::IntLiteral("2".into()),
                Expr::IntLiteral("3".into()),
            ],
            &[],
            None,
        )
        .expect_err("too many positional arguments");
        assert!(sqlstate(&error) == "42883");
        let error = bind_procedure_call(
            &kv,
            "procedure_arity",
            &[Expr::IntLiteral("1".into()), Expr::IntLiteral("2".into())],
            &[("a".into(), Expr::IntLiteral("3".into()))],
            None,
        )
        .expect_err("a named argument cannot follow a full positional list");
        assert!(sqlstate(&error) == "42601");
    }

    #[test]
    fn a_single_variadic_procedure_argument_is_collected_into_its_array() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE PROCEDURE one_variadic(VARIADIC values int[]) LANGUAGE sql AS 'SELECT 1'",
        )
        .expect("procedure");
        let bound = bind_procedure_call(
            &kv,
            "one_variadic",
            &[Expr::IntLiteral("1".into())],
            &[],
            None,
        )
        .expect("binding")
        .expect("procedure");
        assert!(matches!(
            bound.args.as_slice(),
            [Expr::ArrayLiteral(values)] if values == &vec![Expr::IntLiteral("1".into())]
        ));
        let error = bind_procedure_call(&kv, "one_variadic", &[], &[], None)
            .expect_err("a variadic procedure still needs an argument");
        assert!(sqlstate(&error) == "42883");
    }

    #[test]
    fn a_lifecycle_statement_enforces_the_routine_kind() {
        let kv = MemKv::default();
        define(&kv, "CREATE PROCEDURE p(x int) LANGUAGE sql AS 'SELECT 1'").expect("procedure");
        let signature = RoutineSignature {
            name: "p".into(),
            args: Some(vec![RoutineArg {
                name: None,
                mode: RoutineArgMode::In,
                ty: crabka_pgparser::ast::RoutineType::builtin(ColumnType::Int4, "integer".into()),
                default: None,
            }]),
        };
        let error = resolve_signature(&kv, RoutineObject::Function, &signature)
            .expect_err("a procedure is not a function");
        assert!(sqlstate(&error) == "42809");
        assert!(error.into_pg().message == "p(integer) is not a function");
        // The kind-agnostic spelling matches either kind.
        assert!(resolve_signature(&kv, RoutineObject::Routine, &signature).is_ok());
    }

    #[test]
    fn a_bare_name_resolves_only_while_it_is_unique() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION u(x int) RETURNS int AS 'SELECT 1' LANGUAGE sql",
        )
        .expect("first");
        let bare = RoutineSignature {
            name: "u".into(),
            args: None,
        };
        assert!(resolve_signature(&kv, RoutineObject::Function, &bare).is_ok());
        define(
            &kv,
            "CREATE FUNCTION u(x text) RETURNS int AS 'SELECT 1' LANGUAGE sql",
        )
        .expect("second");
        let error =
            resolve_signature(&kv, RoutineObject::Function, &bare).expect_err("now ambiguous");
        assert!(sqlstate(&error) == "42725");
        assert!(error.into_pg().message == "function name \"u\" is not unique");

        let missing = RoutineSignature {
            name: "gone".into(),
            args: None,
        };
        let error =
            resolve_signature(&kv, RoutineObject::Function, &missing).expect_err("no such name");
        assert!(sqlstate(&error) == "42883");
        assert!(error.into_pg().message == "could not find a function named \"gone\"");
    }

    #[test]
    fn rendering_matches_postgresqls_own_spelling() {
        let kv = MemKv::default();
        let cases: Vec<(&str, &str, &str, Option<&str>)> = vec![
            (
                "CREATE FUNCTION r1(a int, b int DEFAULT 10) RETURNS int AS 'SELECT 1' LANGUAGE sql",
                "a integer, b integer DEFAULT 10",
                "a integer, b integer",
                Some("integer"),
            ),
            (
                "CREATE FUNCTION r2(n int) RETURNS SETOF int AS 'SELECT 1' LANGUAGE sql",
                "n integer",
                "n integer",
                Some("SETOF integer"),
            ),
            (
                "CREATE FUNCTION r3(n int) RETURNS TABLE(a int, b text) AS 'SELECT 1' LANGUAGE sql",
                "n integer",
                "n integer",
                Some("TABLE(a integer, b text)"),
            ),
            (
                "CREATE FUNCTION r4(IN a int, OUT b int, OUT c int) AS 'SELECT 1' LANGUAGE sql",
                "a integer, OUT b integer, OUT c integer",
                "a integer",
                Some("record"),
            ),
            (
                "CREATE FUNCTION r5() RETURNS void AS 'SELECT 1' LANGUAGE sql",
                "",
                "",
                Some("void"),
            ),
            (
                "CREATE PROCEDURE r6(x int) LANGUAGE sql AS 'SELECT 1'",
                "IN x integer",
                "IN x integer",
                None,
            ),
        ];
        for (sql, arguments, identity, result) in cases {
            let routine = defined(&kv, sql);
            assert!(render_arguments(&routine) == arguments, "{sql}");
            assert!(render_identity_arguments(&routine) == identity, "{sql}");
            assert!(render_result(&routine).as_deref() == result, "{sql}");
        }
    }

    #[test]
    fn functiondef_reproduces_the_create_statement() {
        let kv = MemKv::default();
        let routine = defined(
            &kv,
            "CREATE FUNCTION fd(a int, b int) RETURNS int AS 'SELECT $1 + $2' LANGUAGE sql",
        );
        assert!(
            render_functiondef(&routine)
                == "CREATE OR REPLACE FUNCTION public.fd(a integer, b integer)\n RETURNS integer\n \
                    LANGUAGE sql\nAS $function$SELECT $1 + $2$function$\n"
        );
        let configured_kv = MemKv::default();
        let configured = defined(
            &configured_kv,
            "CREATE FUNCTION configured() RETURNS int AS 'SELECT 1' LANGUAGE sql SET search_path TO PG_CATALOG SET extra_float_digits TO 2 SET work_mem TO '4MB' SET datestyle TO iso, mdy SET local_preload_libraries TO \"Mixed/Case\", 'c:/''a\"/path', '', 'long' IMMUTABLE STRICT",
        );
        assert!(
            render_functiondef(&configured).contains(
                " IMMUTABLE STRICT\n SET search_path TO 'pg_catalog'\n SET extra_float_digits TO '2'\n SET work_mem TO '4MB'\n SET \"DateStyle\" TO 'iso, mdy'\n SET local_preload_libraries TO 'Mixed/Case', 'c:/''a\"/path', '', 'long'\n"
            ),
            "{}",
            render_functiondef(&configured)
        );
        let qualified = defined(
            &kv,
            "CREATE FUNCTION fq() RETURNS int AS 'SELECT 1' LANGUAGE sql IMMUTABLE STRICT \
             PARALLEL SAFE SECURITY DEFINER COST 5",
        );
        assert!(
            render_functiondef(&qualified)
                == "CREATE OR REPLACE FUNCTION public.fq()\n RETURNS integer\n LANGUAGE sql\n \
                    IMMUTABLE PARALLEL SAFE STRICT SECURITY DEFINER COST 5\nAS \
                    $function$SELECT 1$function$\n"
        );
    }

    #[test]
    fn functiondef_omits_the_implicit_set_returning_rows() {
        let default_kv = MemKv::default();
        let default_rows = defined(
            &default_kv,
            "CREATE FUNCTION default_rows() RETURNS TABLE(value int) LANGUAGE sql AS 'SELECT 1'",
        );
        let explicit_kv = MemKv::default();
        let explicit_rows = defined(
            &explicit_kv,
            "CREATE FUNCTION explicit_rows() RETURNS TABLE(value int) LANGUAGE sql ROWS 42 AS 'SELECT 1'",
        );
        let atomic_kv = MemKv::default();
        let atomic = defined(
            &atomic_kv,
            "CREATE FUNCTION atomic_rows() RETURNS int LANGUAGE sql BEGIN ATOMIC RETURN 1; END",
        );
        let restricted_kv = MemKv::default();
        let restricted = defined(
            &restricted_kv,
            "CREATE FUNCTION restricted_rows() RETURNS int LANGUAGE sql STABLE PARALLEL RESTRICTED AS 'SELECT 1'",
        );
        let safe_kv = MemKv::default();
        let safe = defined(
            &safe_kv,
            "CREATE FUNCTION safe_rows() RETURNS int LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT SECURITY DEFINER COST 42 AS 'SELECT 1'",
        );

        assert!(
            render_functiondef(&default_rows)
                == "CREATE OR REPLACE FUNCTION public.default_rows()\n RETURNS TABLE(value integer)\n LANGUAGE sql\nAS $function$SELECT 1$function$\n"
        );
        assert!(
            render_functiondef(&explicit_rows)
                == "CREATE OR REPLACE FUNCTION public.explicit_rows()\n RETURNS TABLE(value integer)\n LANGUAGE sql\n ROWS 42\nAS $function$SELECT 1$function$\n"
        );
        assert!(
            render_functiondef(&atomic)
                == "CREATE OR REPLACE FUNCTION public.atomic_rows()\n RETURNS integer\n LANGUAGE sql\nBEGIN ATOMIC\n RETURN 1;\nEND\n"
        );
        assert!(
            render_functiondef(&restricted)
                == "CREATE OR REPLACE FUNCTION public.restricted_rows()\n RETURNS integer\n LANGUAGE sql\n STABLE PARALLEL RESTRICTED\nAS $function$SELECT 1$function$\n"
        );
        assert!(
            render_functiondef(&safe)
                == "CREATE OR REPLACE FUNCTION public.safe_rows()\n RETURNS integer\n LANGUAGE sql\n IMMUTABLE PARALLEL SAFE STRICT SECURITY DEFINER COST 42\nAS $function$SELECT 1$function$\n"
        );
    }

    #[tokio::test]
    async fn atomic_merge_function_definition_is_canonical() {
        let engine = crate::SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE merge_source (a int, b text); CREATE TABLE merge_target (id int, data text)")
            .await
            .expect("tables");
        let statements = crabka_pgparser::parse(
            "CREATE FUNCTION merge_definition() RETURNS TABLE(action text, a int, b text, id int, data text, old_id int, old_data text, new_id int, new_data text) LANGUAGE sql BEGIN ATOMIC MERGE INTO merge_target t USING merge_source s ON s.a = t.id WHEN NOT MATCHED AND s.b IS NULL THEN INSERT DEFAULT VALUES WHEN NOT MATCHED THEN INSERT OVERRIDING USER VALUE VALUES (s.a, s.b) WHEN MATCHED THEN UPDATE SET data = s.b RETURNING WITH (OLD AS o, NEW AS n) merge_action() AS action, *, o.*, n.*; END",
        )
        .expect("function parses");
        let [Statement::CreateRoutine(statement)] = statements.as_slice() else {
            panic!("expected CREATE FUNCTION");
        };
        let (_, ops) = create(
            engine.catalog_kv(),
            &crate::relname::ResolutionScope::default_scope(),
            statement,
            "crab",
            true,
        )
        .expect("function definition");
        engine
            .catalog_kv()
            .write_batch(&ops)
            .expect("write function");
        let routine = list_routines(engine.catalog_kv())
            .expect("catalog")
            .pop()
            .expect("function");
        let rendered = render_functiondef(&routine);
        assert!(rendered.contains("\n    WHEN NOT MATCHED\n     AND (s.b IS NULL)"));
        assert!(rendered.contains("THEN INSERT (id, data) OVERRIDING USER VALUE"));
        assert!(rendered.contains("THEN INSERT DEFAULT VALUES"));
        assert!(!rendered.contains("INSERT (id, data) DEFAULT VALUES"));
        assert!(rendered.contains("RETURNING WITH (OLD AS o, NEW AS n) MERGE_ACTION() AS action"));
        assert!(rendered.contains("s.a,\n     s.b,\n     t.id,\n     t.data,\n     o.id,\n     o.data,\n     n.id,\n     n.data"));
        let statements = crabka_pgparser::parse(
            "CREATE FUNCTION merge_without_return() RETURNS void LANGUAGE sql BEGIN ATOMIC MERGE INTO merge_target t USING merge_source s ON s.a = t.id WHEN NOT MATCHED THEN INSERT (data, id) VALUES (s.b, s.a); END",
        )
        .expect("second function parses");
        let [Statement::CreateRoutine(statement)] = statements.as_slice() else {
            panic!("expected CREATE FUNCTION");
        };
        let (_, ops) = create(
            engine.catalog_kv(),
            &crate::relname::ResolutionScope::default_scope(),
            statement,
            "crab",
            true,
        )
        .expect("second function definition");
        engine
            .catalog_kv()
            .write_batch(&ops)
            .expect("write second function");
        let no_return = list_routines(engine.catalog_kv())
            .expect("catalog")
            .into_iter()
            .find(|routine| routine.name == "merge_without_return")
            .expect("second function");
        assert!(render_functiondef(&no_return).contains("\n    WHEN NOT MATCHED BY TARGET\n"));
    }

    #[test]
    fn a_sql_body_inlines_into_the_callers_expression() {
        let kv = seeded();
        let call = FuncCall {
            sql_syntax: false,
            name: "add2".into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![
                Expr::IntLiteral("1".into()),
                Expr::IntLiteral("2".into()),
            ]),
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        };
        let inlined = inline_scalar_call(
            &kv,
            &call,
            &[
                ArgType::Known(ColumnType::Int4),
                ArgType::Known(ColumnType::Int4),
            ],
        )
        .expect("inlines")
        .expect("a routine");
        // `SELECT $1 + $2` with the call's arguments substituted, cast to the
        // declared return type.
        assert!(
            inlined
                == Expr::Cast {
                    expr: Box::new(Expr::Binary {
                        op: crabka_pgparser::ast::BinaryOp::Add,
                        left: Box::new(Expr::IntLiteral("1".into())),
                        right: Box::new(Expr::IntLiteral("2".into())),
                    }),
                    ty: ColumnType::Int4,
                }
        );
    }

    #[test]
    fn a_body_may_name_its_parameters_instead_of_numbering_them() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION named(a int) RETURNS int AS 'SELECT a + 1' LANGUAGE sql",
        )
        .expect("definition");
        let call = FuncCall {
            sql_syntax: false,
            name: "named".into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![Expr::IntLiteral("4".into())]),
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        };
        let inlined = inline_scalar_call(&kv, &call, &[ArgType::Known(ColumnType::Int4)])
            .expect("inlines")
            .expect("a routine");
        assert!(
            inlined
                == Expr::Cast {
                    expr: Box::new(Expr::Binary {
                        op: crabka_pgparser::ast::BinaryOp::Add,
                        left: Box::new(Expr::IntLiteral("4".into())),
                        right: Box::new(Expr::IntLiteral("1".into())),
                    }),
                    ty: ColumnType::Int4,
                }
        );
    }

    #[test]
    fn a_name_that_is_not_a_routine_inlines_to_nothing() {
        let kv = seeded();
        let call = FuncCall {
            sql_syntax: false,
            name: "upper".into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![Expr::StringLiteral("x".into())]),
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        };
        assert!(inline_scalar(&kv, &call).expect("no error").is_none());
    }

    #[test]
    fn a_c_helper_named_like_a_builtin_uses_the_builtin() {
        let kv = Arc::new(MemKv::default());
        define(
            &kv,
            "CREATE FUNCTION binary_coercible(oid, oid) RETURNS bool \
             AS 'regress', 'binary_coercible' LANGUAGE C",
        )
        .expect("C helper definition");
        let catalog: Arc<dyn Kv> = kv;
        let call = Expr::Func(FuncCall {
            sql_syntax: false,
            name: "binary_coercible".into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![
                Expr::IntLiteral("23".into()),
                Expr::IntLiteral("23".into()),
            ]),
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        });
        let scope = crate::scope::Scope::empty();
        let ctx = crate::clock::EvalCtx::test_default();
        with_scalar_runtime(&catalog, None, || {
            assert!(crate::eval::infer_type(&call, &scope).expect("type") == ColumnType::Bool);
            assert!(
                crate::eval::eval(&call, &scope, &[], &ctx).expect("value") == Datum::Bool(true)
            );
        });
    }

    #[test]
    fn unrelated_c_name_collision_does_not_use_the_builtin() {
        let kv = Arc::new(MemKv::default());
        define(
            &kv,
            "CREATE FUNCTION md5(text) RETURNS text \
             AS 'regress', 'binary_coercible' LANGUAGE C",
        )
        .expect("C helper definition");
        let catalog: Arc<dyn Kv> = kv;
        let call = Expr::Func(FuncCall {
            sql_syntax: false,
            name: "md5".into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![Expr::StringLiteral("value".into())]),
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        });

        with_scalar_runtime(&catalog, None, || {
            assert!(crate::eval::infer_type(&call, &crate::scope::Scope::empty()).is_err());
        });
    }

    #[test]
    fn pglz_regression_c_adapters_are_exactly_metadata_gated() {
        let compress = defined(
            &MemKv::default(),
            "CREATE FUNCTION test_pglz_compress(bytea) RETURNS bytea \
             AS 'regress' LANGUAGE C STRICT",
        );
        assert!(regression_c_adapter(&compress) == Some(RegressionCAdapter::PglzCompress));

        for sql in [
            "CREATE FUNCTION test_pglz_compress(bytea) RETURNS bytea AS 'regress' LANGUAGE C",
            "CREATE FUNCTION test_pglz_compress(text) RETURNS bytea AS 'regress' LANGUAGE C STRICT",
            "CREATE FUNCTION test_pglz_compress(bytea) RETURNS text AS 'regress' LANGUAGE C STRICT",
            "CREATE FUNCTION pglz_alias(bytea) RETURNS bytea \
             AS 'regress', 'test_pglz_compress' LANGUAGE C STRICT",
        ] {
            let routine = defined(&MemKv::default(), sql);
            assert!(regression_c_adapter(&routine).is_none(), "{sql}");
        }
    }

    #[test]
    fn interpt_pp_regression_c_adapter_is_exact_and_uses_first_intersection() {
        let exact = defined(
            &MemKv::default(),
            "CREATE FUNCTION interpt_pp(path, path) RETURNS point AS 'regress' LANGUAGE C STRICT",
        );
        assert!(regression_c_adapter(&exact) == Some(RegressionCAdapter::InterptPp));
        assert!(
            eval_regression_c_adapter(
                RegressionCAdapter::InterptPp,
                &[
                    Datum::Path(crabka_pgtypes::Path::parse("[(0,0),(2,0),(2,2)]").expect("path")),
                    Datum::Path(crabka_pgtypes::Path::parse("[(1,-1),(1,1),(3,1)]").expect("path")),
                ],
            )
            .expect("adapter input")
                == Datum::Point(crabka_pgtypes::Point { x: 1.0, y: 0.0 })
        );
        assert!(
            eval_regression_c_adapter(
                RegressionCAdapter::InterptPp,
                &[
                    Datum::Path(crabka_pgtypes::Path::parse("[(0,0),(1,0)]").expect("path")),
                    Datum::Path(crabka_pgtypes::Path::parse("[(0,1),(1,1)]").expect("path")),
                ],
            )
            .expect("adapter input")
                == Datum::Null
        );
    }

    #[test]
    fn point_in_widget_regression_c_adapter_uses_the_widget_radius() {
        let point = crabka_pgtypes::Point { x: 1.0, y: 2.0 };
        assert!(
            eval_regression_c_adapter(
                RegressionCAdapter::PointInWidget,
                &[Datum::Point(point), Datum::Text("(0,0,3)".into())],
            )
            .expect("adapter input")
                == Datum::Bool(true)
        );
        assert!(
            eval_regression_c_adapter(
                RegressionCAdapter::PointInWidget,
                &[Datum::Point(point), Datum::Text("(0,0,1)".into())],
            )
            .expect("adapter input")
                == Datum::Bool(false)
        );
        assert!(
            eval_regression_c_adapter(
                RegressionCAdapter::PointInWidget,
                &[
                    Datum::Point(crabka_pgtypes::Point { x: 2.0, y: 2.0 }),
                    Datum::Text("(1,1,1.5)".into()),
                ],
            )
            .expect("adapter input")
                == Datum::Bool(true)
        );
        assert!(
            eval_regression_c_adapter(
                RegressionCAdapter::PointInWidget,
                &[
                    Datum::Point(crabka_pgtypes::Point { x: 3.0, y: 0.0 }),
                    Datum::Text("(0,0,3)".into()),
                ],
            )
            .expect("adapter input")
                == Datum::Bool(false)
        );
    }

    #[test]
    fn point_in_widget_regression_c_adapter_is_exactly_metadata_gated() {
        let kv = MemKv::default();
        let ops = crate::usertype::create_routine_return_shell(
            &kv,
            &crabka_pgcatalog::RelationName::public("widget"),
        )
        .expect("widget shell");
        kv.write_batch(&ops).expect("store widget shell");
        let exact = defined(
            &kv,
            "CREATE FUNCTION pt_in_widget(point, widget) RETURNS bool \
             AS 'regress' LANGUAGE C STRICT",
        );
        assert!(regression_c_adapter(&exact) == Some(RegressionCAdapter::PointInWidget));

        let mut cases = Vec::new();
        let mut changed = exact.clone();
        changed.kind = RoutineKind::Procedure;
        cases.push(changed);
        let mut changed = exact.clone();
        changed.name.push_str("_other");
        cases.push(changed);
        let mut changed = exact.clone();
        changed.language = "sql".into();
        cases.push(changed);
        let mut changed = exact.clone();
        changed.object_file = Some("other".into());
        cases.push(changed);
        let mut changed = exact.clone();
        changed.body = "other".into();
        cases.push(changed);
        let mut changed = exact.clone();
        changed.strict = false;
        cases.push(changed);
        let mut changed = exact.clone();
        changed.params.push(changed.params[0].clone());
        cases.push(changed);
        let mut changed = exact.clone();
        changed.params[0].mode = ParamMode::Out;
        cases.push(changed);
        let mut changed = exact.clone();
        changed.params[0].default = Some("0".into());
        cases.push(changed);
        let mut changed = exact.clone();
        changed.params[0].ty = RoutineType::builtin(ColumnType::Int4);
        cases.push(changed);
        let mut changed = exact.clone();
        changed.params[1].mode = ParamMode::Out;
        cases.push(changed);
        let mut changed = exact.clone();
        changed.params[1].default = Some("0".into());
        cases.push(changed);
        let mut changed = exact.clone();
        changed.params[1].ty = RoutineType::named("other".into());
        cases.push(changed);
        let mut changed = exact.clone();
        changed.result = RoutineResult::Type {
            ty: RoutineType::builtin(ColumnType::Int4),
            setof: false,
        };
        cases.push(changed);

        for routine in cases {
            assert!(regression_c_adapter(&routine).is_none(), "{routine:?}");
        }
    }

    #[test]
    fn catalog_text_unique_index_adapter_is_exactly_metadata_gated() {
        let exact = defined(
            &MemKv::default(),
            "CREATE FUNCTION is_catalog_text_unique_index_oid(oid) RETURNS bool \
             AS 'regress', 'is_catalog_text_unique_index_oid' LANGUAGE C STRICT",
        );
        assert!(
            regression_c_adapter(&exact) == Some(RegressionCAdapter::CatalogTextUniqueIndexOid)
        );

        let mut cases = Vec::new();
        let mut changed = exact.clone();
        changed.kind = RoutineKind::Procedure;
        cases.push(changed);
        let mut changed = exact.clone();
        changed.name.push_str("_other");
        cases.push(changed);
        let mut changed = exact.clone();
        changed.language = "sql".into();
        cases.push(changed);
        let mut changed = exact.clone();
        changed.object_file = Some("other".into());
        cases.push(changed);
        let mut changed = exact.clone();
        changed.body = "other".into();
        cases.push(changed);
        let mut changed = exact.clone();
        changed.strict = false;
        cases.push(changed);
        let mut changed = exact.clone();
        changed.params.push(changed.params[0].clone());
        cases.push(changed);
        let mut changed = exact.clone();
        changed.params[0].mode = ParamMode::Out;
        cases.push(changed);
        let mut changed = exact.clone();
        changed.params[0].default = Some("0".into());
        cases.push(changed);
        let mut changed = exact.clone();
        changed.params[0].ty = RoutineType::builtin(ColumnType::Int4);
        cases.push(changed);
        let mut changed = exact.clone();
        changed.result = RoutineResult::Type {
            ty: RoutineType::builtin(ColumnType::Int4),
            setof: false,
        };
        cases.push(changed);

        for routine in cases {
            assert!(regression_c_adapter(&routine).is_none(), "{routine:?}");
        }
    }

    #[test]
    fn catalog_text_unique_index_adapter_accepts_catalog_oid_storage() {
        for (input, expected) in [
            (Datum::Oid(3593), true),
            (Datum::Int4(6246), true),
            (Datum::Oid(3592), false),
        ] {
            assert!(
                eval_regression_c_adapter(RegressionCAdapter::CatalogTextUniqueIndexOid, &[input])
                    .expect("adapter input")
                    == Datum::Bool(expected)
            );
        }
    }

    fn pglz_regression_catalog() -> Arc<dyn Kv> {
        let kv = Arc::new(MemKv::default());
        define(
            &kv,
            "CREATE FUNCTION test_pglz_compress(bytea) RETURNS bytea \
             AS 'regress' LANGUAGE C STRICT",
        )
        .expect("compression helper");
        define(
            &kv,
            "CREATE FUNCTION test_pglz_decompress(bytea, int4, bool) RETURNS bytea \
             AS 'regress' LANGUAGE C STRICT",
        )
        .expect("decompression helper");
        kv
    }

    fn call_regression_c_adapter(
        catalog: &Arc<dyn Kv>,
        name: &str,
        types: &[ColumnType],
        values: Vec<Datum>,
    ) -> Result<Datum, ExecError> {
        let call = FuncCall {
            sql_syntax: false,
            name: name.into(),
            distinct: false,
            args: FuncArgs::Exprs(
                types
                    .iter()
                    .map(|ty| Expr::Cast {
                        expr: Box::new(Expr::NullLiteral),
                        ty: *ty,
                    })
                    .collect(),
            ),
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        };
        assert!(inline_scalar(catalog.as_ref(), &call)?.is_none());
        assert!(plpgsql_declared_call_type(catalog.as_ref(), &call)? == Some(ColumnType::Bytea));
        let ctx = crate::clock::EvalCtx::test_default();
        with_scalar_runtime(catalog, None, || {
            assert!(is_plpgsql_scalar_runtime(
                &call,
                &crate::scope::Scope::empty()
            ));
            let mut values = values.into_iter();
            eval_plpgsql_scalar_with(&call, &ctx, |_| {
                values.next().ok_or_else(|| {
                    ExecError::Unsupported("test adapter argument is missing".into())
                })
            })
            .expect("the pinned C adapter owns the call")
        })
    }

    fn assert_pglz_error(result: Result<Datum, ExecError>, state: &str, message: &str) {
        let error = result.expect_err(message).into_pg();
        assert!(error.code == state);
        assert!(error.message == message);
    }

    #[test]
    fn pglz_regression_c_adapters_match_upstream_roundtrips_and_failures() {
        let catalog = pglz_regression_catalog();
        let input = b"abcd".repeat(100);
        let compressed = call_regression_c_adapter(
            &catalog,
            "test_pglz_compress",
            &[ColumnType::Bytea],
            vec![Datum::Bytea(input.clone())],
        )
        .expect("compresses");
        let Datum::Bytea(compressed) = compressed else {
            panic!("compressor returned {compressed:?}");
        };

        for check_complete in [false, true] {
            assert!(
                call_regression_c_adapter(
                    &catalog,
                    "test_pglz_decompress",
                    &[ColumnType::Bytea, ColumnType::Int4, ColumnType::Bool],
                    vec![
                        Datum::Bytea(compressed.clone()),
                        Datum::Int4(400),
                        Datum::Bool(check_complete),
                    ],
                )
                .expect("roundtrip")
                    == Datum::Bytea(input.clone())
            );
        }

        for rawsize in [500, 100] {
            assert_pglz_error(
                call_regression_c_adapter(
                    &catalog,
                    "test_pglz_decompress",
                    &[ColumnType::Bytea, ColumnType::Int4, ColumnType::Bool],
                    vec![
                        Datum::Bytea(compressed.clone()),
                        Datum::Int4(rawsize),
                        Datum::Bool(true),
                    ],
                ),
                "XX000",
                "pglz_decompress failed",
            );
        }

        assert!(
            call_regression_c_adapter(
                &catalog,
                "test_pglz_decompress",
                &[ColumnType::Bytea, ColumnType::Int4, ColumnType::Bool],
                vec![
                    Datum::Bytea(vec![0x01]),
                    Datum::Int4(1024),
                    Datum::Bool(false),
                ],
            )
            .expect("permissive control-only stream")
                == Datum::Bytea(Vec::new())
        );

        let nested = crabka_pgparser::parser::parse_expression(
            r"length(test_pglz_decompress('\x01'::bytea, 1024, false))",
        )
        .expect("upstream nested expression parses");
        let ctx = crate::clock::EvalCtx::test_default();
        with_scalar_runtime(&catalog, None, || {
            assert!(
                crate::eval::infer_type(&nested, &crate::scope::Scope::empty()).expect("type")
                    == ColumnType::Int4
            );
            assert!(
                crate::eval::eval(&nested, &crate::scope::Scope::empty(), &[], &ctx,)
                    .expect("nested PGLZ call")
                    == Datum::Int4(0)
            );
        });

        for (source, check_complete) in [
            (vec![0x01], true),
            (vec![0x01, 0xff], false),
            (vec![0x01, 0xff], true),
            (vec![0x01, 0x0f, 0x01], false),
            (vec![0x01, 0x0f, 0x01], true),
        ] {
            assert_pglz_error(
                call_regression_c_adapter(
                    &catalog,
                    "test_pglz_decompress",
                    &[ColumnType::Bytea, ColumnType::Int4, ColumnType::Bool],
                    vec![
                        Datum::Bytea(source),
                        Datum::Int4(1024),
                        Datum::Bool(check_complete),
                    ],
                ),
                "XX000",
                "pglz_decompress failed",
            );
        }
    }

    #[test]
    fn pglz_regression_c_decompress_rejects_negative_and_unbounded_outputs() {
        assert_pglz_error(
            eval_regression_c_adapter(
                RegressionCAdapter::PglzDecompress,
                &[
                    Datum::Bytea(Vec::new()),
                    Datum::Int4(-1),
                    Datum::Bool(false),
                ],
            ),
            "XX000",
            "rawsize must not be negative",
        );

        let error = eval_regression_c_adapter(
            RegressionCAdapter::PglzDecompress,
            &[
                Datum::Bytea(Vec::new()),
                Datum::Int4(i32::try_from(MAX_PGLZ_OUTPUT_BYTES + 1).expect("test bound fits")),
                Datum::Bool(false),
            ],
        )
        .expect_err("oversized output is rejected")
        .into_pg();
        assert!(error.code == "54000");
        assert!(error.message.contains("64 MiB safety limit"));
    }

    #[test]
    fn binary_coercible_overload_keeps_user_precedence() {
        let kv = Arc::new(MemKv::default());
        define(
            &kv,
            "CREATE FUNCTION binary_coercible(oid, oid) RETURNS bool \
             AS 'regress', 'binary_coercible' LANGUAGE C",
        )
        .expect("C helper definition");
        define(
            &kv,
            "CREATE FUNCTION binary_coercible(text, text) RETURNS bool \
             LANGUAGE plpgsql AS $$ BEGIN RETURN false; END $$",
        )
        .expect("PL/pgSQL overload definition");
        let catalog: Arc<dyn Kv> = kv;
        let text = |value: &str| Expr::Cast {
            expr: Box::new(Expr::StringLiteral(value.into())),
            ty: ColumnType::Text,
        };
        let call = Expr::Func(FuncCall {
            sql_syntax: false,
            name: "binary_coercible".into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![text("a"), text("b")]),
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        });
        let scope = crate::scope::Scope::empty();
        let ctx = crate::clock::EvalCtx::test_default();

        with_scalar_runtime(&catalog, None, || {
            assert!(crate::eval::infer_type(&call, &scope).expect("type") == ColumnType::Bool);
            assert!(
                crate::eval::eval(&call, &scope, &[], &ctx).expect("value") == Datum::Bool(false)
            );
        });
    }

    #[test]
    fn plpgsql_bodies_are_validated_when_defined() {
        let kv = MemKv::default();
        let routine = defined(
            &kv,
            "CREATE FUNCTION plp(a int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN a; END $$",
        );
        let body = parse_plpgsql_body(&routine).expect("stored body reparses");
        assert!(body.statements.len() == 1);

        let error = define(
            &kv,
            "CREATE FUNCTION broken() RETURNS int LANGUAGE plpgsql AS \
             $$ BEGIN IF THEN RETURN 1; END IF; END $$",
        )
        .expect_err("invalid body is rejected");
        assert!(sqlstate(&error) == "42601");
        assert!(routines_named(&kv, "broken").expect("catalog").is_empty());

        defined(
            &kv,
            "CREATE FUNCTION deferred_sql() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN PERFORM string_agg(v, ',' ORDER BY v) FROM t; RETURN NULL; END $$",
        );
    }

    #[test]
    fn plpgsql_procedure_rejects_return_values_when_defined() {
        let kv = MemKv::default();
        let error = define(
            &kv,
            "CREATE PROCEDURE bad_return() LANGUAGE plpgsql AS \
             $$ BEGIN IF true THEN RETURN 1; END IF; END $$",
        )
        .expect_err("procedure RETURN value is rejected");
        assert!(sqlstate(&error) == "42804");
        assert!(
            error
                .into_pg()
                .message
                .contains("RETURN cannot have a parameter in a procedure")
        );
        assert!(
            routines_named(&kv, "bad_return")
                .expect("catalog")
                .is_empty()
        );

        define(
            &kv,
            "CREATE PROCEDURE plain_return() LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$",
        )
        .expect("a valueless RETURN is valid");
    }

    #[test]
    fn plpgsql_requires_a_source_string_body() {
        let kv = MemKv::default();
        let error = define(
            &kv,
            "CREATE FUNCTION f() RETURNS int LANGUAGE plpgsql RETURN 1",
        )
        .expect_err("SQL-standard body is not PL/pgSQL");
        assert!(sqlstate(&error) == "42P13");
        assert!(error.into_pg().message == "PL/pgSQL function body must be a string literal");
    }

    #[test]
    fn declared_result_helpers_cover_scalar_void_set_and_outputs() {
        let scalar = defined(
            &MemKv::default(),
            "CREATE FUNCTION scalar_result() RETURNS int LANGUAGE plpgsql AS \
             $$ BEGIN RETURN 1; END $$",
        );
        assert!(declared_scalar_result_type(&scalar) == Some(ColumnType::Int4));
        assert!(!declared_returns_void(&scalar));
        assert!(!declared_returns_set(&scalar));
        assert!(declared_output_parameter_count(&scalar) == 0);

        let void = defined(
            &MemKv::default(),
            "CREATE FUNCTION void_result() RETURNS void LANGUAGE plpgsql AS \
             $$ BEGIN RETURN; END $$",
        );
        assert!(declared_returns_void(&void));
        assert!(declared_scalar_result_type(&void).is_none());

        let set = defined(
            &MemKv::default(),
            "CREATE FUNCTION set_result() RETURNS SETOF int LANGUAGE plpgsql AS \
             $$ BEGIN RETURN NEXT 1; END $$",
        );
        assert!(declared_returns_set(&set));
        assert!(declared_scalar_result_type(&set).is_none());

        let outputs = defined(
            &MemKv::default(),
            "CREATE FUNCTION output_result(IN a int, OUT b int, OUT c text) LANGUAGE plpgsql AS \
             $$ BEGIN b := a; c := 'x'; END $$",
        );
        assert!(declared_output_parameter_count(&outputs) == 2);
        assert!(declared_scalar_result_type(&outputs).is_none());
    }

    #[test]
    fn record_return_recognition_covers_named_and_resolved_forms() {
        assert!(is_record_type(&RoutineType::named("record".into())));
        assert!(is_record_type(&RoutineType::builtin(ColumnType::Record(
            None
        ))));
    }

    #[test]
    fn calling_a_procedure_as_a_function_is_42809() {
        let kv = MemKv::default();
        define(&kv, "CREATE PROCEDURE p(x int) LANGUAGE sql AS 'SELECT 1'").expect("procedure");
        let call = FuncCall {
            sql_syntax: false,
            name: "p".into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![Expr::IntLiteral("1".into())]),
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        };
        let error = inline_scalar(&kv, &call).expect_err("a procedure is not selectable");
        assert!(sqlstate(&error) == "42809");
        assert!(
            error
                .into_pg()
                .message
                .contains("p(integer) is a procedure")
        );
    }

    #[test]
    fn a_do_block_is_refused_with_postgresqls_own_reason_for_sql() {
        let error = do_block("sql");
        assert!(sqlstate(&error) == "0A000");
        assert!(
            error.into_pg().message == "language \"sql\" does not support inline code execution"
        );
        let error = do_block("plpgsql");
        assert!(sqlstate(&error) == "0A000");
        let error = do_block("nosuchlang");
        assert!(sqlstate(&error) == "42704");
    }

    #[test]
    fn pg_proc_reports_one_row_per_stored_routine() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION pp(a int, b int DEFAULT 1) RETURNS SETOF int AS 'SELECT 1' \
             LANGUAGE sql IMMUTABLE STRICT",
        )
        .expect("definition");
        let rows = pg_proc_rows(&kv).expect("rows");
        let row = rows
            .iter()
            .find(|row| row[1] == Datum::Text("pp".into()))
            .expect("stored routine row");
        assert!(row[1] == Datum::Text("pp".into()));
        assert!(row[9] == Datum::Text("f".into()));
        assert!(row[12] == Datum::Bool(true));
        assert!(row[13] == Datum::Bool(true));
        assert!(row[14] == Datum::Text("i".into()));
        assert!(row[16] == Datum::Int2(2));
        assert!(row[17] == Datum::Int2(1));
        assert!(
            row[19]
                == Datum::OidVector(crabka_pgtypes::ArrayValue::with_dims(
                    crabka_pgtypes::ElemType::Int4,
                    vec![Datum::Int4(23), Datum::Int4(23)],
                    vec![crabka_pgtypes::ArrayDim::new(0, 2)],
                ))
        );
        assert!(row[25] == Datum::Text("SELECT 1".into()));
    }

    #[test]
    fn only_postgresqls_implicit_casts_resolve_a_call() {
        let cases = [
            (ColumnType::Int4, ColumnType::Int8, true),
            (ColumnType::Int4, ColumnType::Float8, true),
            (ColumnType::Int8, ColumnType::Int4, false),
            (ColumnType::Int4, ColumnType::Text, false),
            (ColumnType::Text, ColumnType::Int4, false),
            (ColumnType::Text, ColumnType::Varchar(None), true),
            (ColumnType::Timestamp, ColumnType::Timestamptz, true),
            (ColumnType::Timestamptz, ColumnType::Timestamp, false),
        ];
        for (from, to, want) in cases {
            assert!(
                implicitly_coercible(from, to) == want,
                "{} -> {}",
                from.name(),
                to.name()
            );
        }
    }

    #[test]
    fn substitutes_named_from_function_arguments() {
        let routine = defined(
            &MemKv::default(),
            "CREATE FUNCTION args(value int) RETURNS int LANGUAGE sql AS 'SELECT value'",
        );
        let binding = Binding {
            routine: &routine,
            args: vec![Expr::IntLiteral("7".into())],
            uses: std::cell::RefCell::new(vec![0]),
        };
        let statements =
            crabka_pgparser::parse("SELECT * FROM f(named => $1)").expect("query parses");
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
            panic!("expected select");
        };
        let mut select = select.clone();

        substitute_in_from(&binding, &mut select).expect("substitution");

        let [crabka_pgparser::ast::TableExpr::Function { functions, .. }] = select.from.as_slice()
        else {
            panic!("expected function item");
        };
        assert!(
            functions[0]
                .arguments()
                .all(|arg| arg == &Expr::IntLiteral("7".into()))
        );
    }

    #[test]
    fn table_function_schema_resolves_polymorphic_out_columns() {
        let kv = MemKv::default();
        defined(
            &kv,
            "CREATE FUNCTION out_poly(anyelement, OUT value anyelement, OUT values anyarray) \
             LANGUAGE sql AS 'SELECT $1, ARRAY[$1, $1]'",
        );
        let statements =
            crabka_pgparser::parse("SELECT * FROM out_poly('x'::text)").expect("query parses");
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
            panic!("expected select");
        };
        let [crabka_pgparser::ast::TableExpr::Function { functions, .. }] = select.from.as_slice()
        else {
            panic!("expected function item");
        };

        let (_, columns) = plpgsql_table_function_schema(&kv, &functions[0])
            .expect("schema resolves")
            .expect("user routine");
        assert!(
            columns
                == vec![
                    ("value".into(), ColumnType::Text),
                    (
                        "values".into(),
                        ColumnType::array_of(ColumnType::Text).expect("text array")
                    ),
                ]
        );
    }
}
