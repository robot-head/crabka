//! P2: SQL routines — definition, resolution, calling and catalog projection.
//!
//! A `LANGUAGE sql` routine is a query with holes. Rather than build a second
//! executor for routine bodies, a call is *inlined*: the body is re-parsed from
//! the catalog and its parameter references are replaced by the call's argument
//! expressions, so the resulting expression or query runs through the ordinary
//! evaluation path and sees the same snapshot the caller does. That is exactly
//! how `PostgreSQL` treats a simple SQL function, and it means a routine never
//! carries a stale plan.
//!
//! PL/pgSQL bodies are parsed when they are defined and re-parsed for execution,
//! just like SQL bodies. Dynamic C/internal routines remain catalog-only.

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
    AlterRoutineAction, CreateRoutineStmt, Expr, FuncArgs, FuncCall, PlPgSqlBlock,
    PlPgSqlStatement, QueryExpr, RoutineArg, RoutineArgMode, RoutineBody, RoutineObject,
    RoutineOption, RoutineParallel, RoutineReturn, RoutineSignature, RoutineVolatility, SelectItem,
    SelectStmt, Statement,
};
use crabka_pgtypes::{ColumnType, Datum};
use crabka_pgwire::engine::QueryResult;

use crate::{error::ExecError, eval::ArgType};

pub(crate) struct ScalarFunctionRequest {
    pub routine: Routine,
    pub values: Vec<Datum>,
    pub kind: FunctionRequestKind,
    pub reply: std::sync::mpsc::Sender<Result<FunctionRequestResult, ExecError>>,
}

pub(crate) enum FunctionRequestKind {
    Scalar,
    Table(Vec<(String, ColumnType)>),
    Trigger(Box<crate::trigger::TriggerInvocation>),
}

pub(crate) enum FunctionRequestResult {
    Scalar(Datum),
    Table(Vec<Vec<Datum>>),
}

type FunctionColumns = Vec<(String, ColumnType)>;
type PlPgSqlTableSchema = (Routine, FunctionColumns);
type PlPgSqlTableRows = (FunctionColumns, Vec<Vec<Datum>>);

#[derive(Clone)]
struct ScalarRuntime {
    catalog: Arc<dyn Kv>,
    requests: Option<tokio::sync::mpsc::Sender<ScalarFunctionRequest>>,
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
    SCALAR_RUNTIME.with(|cell| {
        let previous = cell.replace(Some(ScalarRuntime {
            catalog: Arc::clone(catalog),
            requests,
        }));
        let result = f();
        cell.replace(previous);
        result
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

/// The languages `pg_language` lists. A routine in any other language does not
/// exist as far as `CREATE FUNCTION` is concerned (`42704`).
const LANGUAGES: [&str; 4] = ["internal", "c", "sql", "plpgsql"];

/// `pg_proc.prolang` for each accepted language, matching `pg_language.oid`.
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
/// A routine signature may name a type Gres does not implement — the regression
/// corpus is full of `smallint`, `anyelement` and `cstring` — and refusing the
/// *definition* would diverge from `PostgreSQL`, which accepts it. Such a type
/// is recorded by name; a call that would have to produce a value of it is
/// `0A000`. A name that is neither on this list, a built-in Gres resolves, nor
/// a relation, is `42704`, exactly as `PostgreSQL` reports it.
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
/// a parameter position; both spellings are reproduced verbatim.
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

/// 42P13 — a routine definition `PostgreSQL` rejects as invalid.
fn invalid_definition(message: impl Into<String>) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42P13",
        message: message.into(),
    }
}

/// 42809 — the named routine exists but is not of the kind the statement asked
/// for.
pub(crate) fn wrong_routine_kind(message: impl Into<String>) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42809",
        message: message.into(),
    }
}

/// 42723 — a routine with this name and input signature already exists.
fn duplicate_routine(name: &str) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42723",
        message: format!("function \"{name}\" already exists with same argument types"),
    }
}

/// 42883 — no routine matches the identity a statement or call named.
pub(crate) fn undefined_routine(message: impl Into<String>) -> ExecError {
    ExecError::UndefinedFunction(message.into())
}

/// 42725 — a routine name that more than one routine carries.
fn ambiguous_routine(name: &str) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42725",
        message: format!("function name \"{name}\" is not unique"),
    }
}

/// Resolve a type written in a routine signature against the catalog.
fn resolve_type(
    kv: &dyn Kv,
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
    // A relation name is that relation's composite type. A routine signature
    // carries no resolution scope of its own, so the composite type it can name
    // is the one `public` holds — the schema an unqualified name resolves to
    // under the default search path.
    let base =
        crabka_pgcatalog::RelationName::public(lowered.strip_suffix("[]").unwrap_or(&lowered));
    if crabka_pgcatalog::get_table(kv, &base).is_ok()
        || crabka_pgcatalog::get_view(kv, &base).is_ok()
    {
        return Ok(RoutineType::named(lowered));
    }
    Err(undefined_type(&ty.name, quoted))
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
    strict: Option<bool>,
    security_definer: Option<bool>,
    leakproof: Option<bool>,
    cost: Option<f64>,
    rows: Option<f64>,
    config: Vec<String>,
}

impl Options {
    fn collect(options: &[RoutineOption]) -> Result<Self, ExecError> {
        let mut out = Self {
            language: None,
            body: None,
            volatility: None,
            parallel: None,
            strict: None,
            security_definer: None,
            leakproof: None,
            cost: None,
            rows: None,
            config: Vec::new(),
        };
        for option in options {
            match option {
                RoutineOption::Language(language) => {
                    out.language = Some(language.to_ascii_lowercase());
                }
                RoutineOption::Body(body) => out.body = Some(body.clone()),
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
                RoutineOption::Set { name, value } => {
                    out.config.push(match value {
                        Some(value) => format!("{name}={value}"),
                        None => name.clone(),
                    });
                }
                RoutineOption::Support(_) | RoutineOption::Transform(_) | RoutineOption::Window => {
                }
            }
        }
        Ok(out)
    }
}

/// The body text and form a routine record stores.
fn body_text(body: &RoutineBody) -> (String, BodyForm) {
    match body {
        RoutineBody::Source(text) => (text.clone(), BodyForm::Source),
        RoutineBody::Atomic { text, .. } => (text.clone(), BodyForm::Atomic),
        RoutineBody::Return { text, .. } => (text.clone(), BodyForm::Return),
    }
}

/// Build the catalog record a `CREATE … FUNCTION`/`PROCEDURE` defines.
fn build_routine(kv: &dyn Kv, stmt: &CreateRoutineStmt, owner: &str) -> Result<Routine, ExecError> {
    let kind = object_kind(stmt.object).expect("CREATE ROUTINE is not PostgreSQL syntax");
    let options = Options::collect(&stmt.options)?;
    let body = options
        .body
        .ok_or_else(|| invalid_definition("no function body specified"))?;
    let language = options.language.unwrap_or_else(|| match body {
        RoutineBody::Return { .. } => "sql".into(),
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
            ty: resolve_type(kv, &arg.ty, false)?,
            default: arg.default.clone(),
        });
    }
    let result = match &stmt.returns {
        RoutineReturn::Unspecified => RoutineResult::Unspecified,
        RoutineReturn::Type { ty, setof } => RoutineResult::Type {
            ty: resolve_type(kv, ty, true)?,
            setof: *setof,
        },
        RoutineReturn::Table(columns) => RoutineResult::Table(
            columns
                .iter()
                .map(|column| Ok((column.name.clone(), resolve_type(kv, &column.ty, true)?)))
                .collect::<Result<Vec<_>, ExecError>>()?,
        ),
    };
    validate_polymorphic_range_result(&params, &result)?;
    if kind == RoutineKind::Procedure && !matches!(result, RoutineResult::Unspecified) {
        return Err(invalid_definition("procedures cannot have a return value"));
    }
    let (body, body_form) = body_text(&body);
    let volatility = options.volatility.unwrap_or('v');
    Ok(Routine {
        oid: 0,
        name: stmt.name.clone(),
        kind,
        params,
        result,
        language,
        body,
        body_form,
        volatility,
        parallel: options.parallel.unwrap_or('u'),
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
        owner: owner.to_string(),
    })
}

fn validate_polymorphic_range_result(
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
    for (result_name, input_names, detail) in [
        (
            "anyrange",
            ["anyrange", "anymultirange"],
            "A result of type anyrange requires at least one input of type anyrange or anymultirange.",
        ),
        (
            "anycompatiblerange",
            ["anycompatiblerange", "anycompatiblemultirange"],
            "A result of type anycompatiblerange requires at least one input of type anycompatiblerange or anycompatiblemultirange.",
        ),
    ] {
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
    stmt: &CreateRoutineStmt,
    owner: &str,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let routine = build_routine(kv, stmt, owner)?;
    let identity = routine.identity();
    if let Some(existing) = get_routine(kv, &identity)? {
        if !stmt.or_replace {
            return Err(duplicate_routine(&routine.name));
        }
        check_replaceable(&existing, &routine)?;
    }
    // A SQL body is checked at definition time, exactly as `PostgreSQL` does
    // with the default `check_function_bodies`.
    if routine.language == "sql" {
        parse_body(&routine)?;
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
    let ops = put_routine_ops(kv, &routine)?;
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
    if existing.kind != replacement.kind {
        return Err(wrong_routine_kind(format!(
            "cannot change routine kind\n{hint}"
        )));
    }
    if existing.result != replacement.result {
        return Err(invalid_definition(
            "cannot change return type of existing function",
        ));
    }
    for (before, after) in existing
        .input_params()
        .zip(replacement.input_params())
        .filter(|(before, after)| before.name != after.name)
    {
        let _ = after;
        if let Some(name) = &before.name {
            return Err(invalid_definition(format!(
                "cannot change name of input parameter \"{name}\""
            )));
        }
    }
    Ok(())
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
                if !cascade && (!ordinary.is_empty() || !event.is_empty()) {
                    return Err(ExecError::DependentObjectsStillExist(format!(
                        "cannot drop function {} because other objects depend on it",
                        routine.identity()
                    )));
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
                routine.leakproof = leakproof;
            }
            if let Some(cost) = collected.cost {
                routine.cost = cost;
            }
            if let Some(rows) = collected.rows {
                routine.rows = rows;
            }
            for entry in collected.config {
                let name = entry.split('=').next().unwrap_or(&entry).to_string();
                routine.config.retain(|existing| {
                    existing.split('=').next().unwrap_or(existing) != name && name != "all"
                });
                if entry.contains('=') {
                    routine.config.push(entry);
                }
            }
        }
    }
    ops.extend(put_routine_ops(kv, &routine)?);
    Ok((tag, ops))
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
        .map(|arg| Ok(resolve_type(kv, &arg.ty, false)?.name))
        .collect()
}

// ------------------------------------------------------------------ calling

/// Is `name` a routine this catalog defines?
pub(crate) fn is_user_routine(kv: &dyn Kv, name: &str) -> bool {
    routines_named(kv, name).is_ok_and(|found| !found.is_empty())
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
    let candidates = routines_named(kv, name)?;
    if candidates.is_empty() {
        return Ok(None);
    }
    let arity_matched: Vec<&Routine> = candidates
        .iter()
        .filter(|routine| {
            let total = routine.input_params().count();
            let required = total - routine.default_count();
            (required..=total).contains(&given.len())
        })
        .collect();
    let mut exact = Vec::new();
    let mut coercible = Vec::new();
    for routine in arity_matched {
        let params: Vec<&RoutineParam> = routine.input_params().collect();
        let mut is_exact = true;
        let mut is_coercible = true;
        for (arg, param) in given.iter().zip(params.iter()) {
            let Some(target) = param.ty.column else {
                if is_polymorphic_type(&param.ty.name) {
                    is_exact &= !matches!(arg, ArgType::Unknown | ArgType::Opaque);
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
        return Ok(Some(exact.remove(0)));
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
            return Ok(Some(preferred.into_iter().next().expect("one candidate")));
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
        return Ok(Some(coercible.remove(0)));
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

fn is_polymorphic_type(name: &str) -> bool {
    matches!(
        name,
        "anyarray"
            | "anyelement"
            | "anynonarray"
            | "anyrange"
            | "anycompatible"
            | "anycompatiblearray"
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
            "anynonarray" | "anycompatiblenonarray" => !matches!(ty, ColumnType::Array(_)),
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
        "anyelement" | "anynonarray" | "anycompatible" | "anycompatiblenonarray" => Some(ty),
        _ => None,
    }
}

fn polymorphic_arguments_are_consistent(params: &[&RoutineParam], given: &[ArgType]) -> bool {
    let mut exact = None;
    let mut compatible = None;
    let mut compatible_range = None;
    let mut compatible_inputs = Vec::new();
    for (param, arg) in params.iter().zip(given) {
        let Some(base) = polymorphic_base_type(&param.ty.name, *arg) else {
            continue;
        };
        if param.ty.name.starts_with("anycompatible") {
            compatible_inputs.push(base);
            if param.ty.name == "anycompatiblerange" {
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

/// Resolve an overload and bind its arguments without choosing an execution
/// strategy. The returned routine retains its `strict` flag and kind so the
/// caller can apply the appropriate function/procedure semantics.
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

/// Resolve a procedure call whose argument list includes `OUT` placeholders.
/// Unlike function calls, procedure arguments remain aligned with the full
/// declaration; output-only expressions do not participate in overload
/// resolution and are never coerced or evaluated.
pub(crate) fn bind_procedure_call(
    kv: &dyn Kv,
    name: &str,
    args: &[Expr],
) -> Result<Option<BoundRoutineCall>, ExecError> {
    let candidates = routines_named(kv, name)?;
    if candidates.is_empty() {
        return Ok(None);
    }
    let mut exact = Vec::new();
    let mut coercible = Vec::new();
    for routine in candidates {
        if args.len() > routine.params.len()
            || routine.params[args.len()..]
                .iter()
                .any(|param| param.default.is_none())
        {
            continue;
        }
        let input_args = routine
            .params
            .iter()
            .zip(args)
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
            .take(args.len())
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
            exact.push((routine, given));
        } else if is_coercible {
            coercible.push((routine, given));
        }
    }
    let selected = if exact.len() == 1 {
        exact.pop().map(|(routine, _)| routine)
    } else if exact.is_empty() && coercible.len() == 1 {
        coercible.pop().map(|(routine, _)| routine)
    } else if exact.is_empty() {
        let preferred = coercible
            .iter()
            .filter(|(routine, given)| prefers_text_at_unknowns(routine, given))
            .map(|(routine, _)| routine.clone())
            .collect::<Vec<_>>();
        (preferred.len() == 1).then(|| preferred[0].clone())
    } else {
        None
    };
    let Some(routine) = selected else {
        if exact.len() > 1 || coercible.len() > 1 {
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
    for param in routine.params.iter().skip(args.len()) {
        let default = param.default.as_ref().ok_or_else(|| {
            undefined_routine(format!("procedure {} does not exist", routine.name))
        })?;
        bound.push(
            crabka_pgparser::parser::parse_expression(default)
                .map_err(|error| ExecError::Syntax(error.message))?,
        );
    }
    Ok(Some(BoundRoutineCall {
        routine,
        args: bound,
    }))
}

/// Is `source` implicitly coercible to `target` for the purpose of resolving a
/// routine call?
///
/// `PostgreSQL` resolves function calls with *implicit* casts only — which is
/// why `f(bigint)` does not match `f(text)` even though the explicit cast
/// exists. This is the implicit graph restricted to the types Gres models.
fn implicitly_coercible(source: ColumnType, target: ColumnType) -> bool {
    use ColumnType::{Char, Float8, Int4, Int8, Numeric, Text, Timestamp, Timestamptz, Varchar};
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
/// A routine whose body calls another routine inlines transitively; a routine
/// that (directly or mutually) calls itself would not terminate, so the depth
/// is capped and reported with PostgreSQL's own `54001`.
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

/// The label `PostgreSQL` gives an unaliased call of `expr` — the function's
/// own name — so inlining a routine does not rename its output column.
pub(crate) fn call_label(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Func(call) => Some(call.name.clone()),
        _ => None,
    }
}

/// Is `name` a built-in function family this engine already provides?
///
/// A user routine is tried first — `PostgreSQL`'s default `search_path` puts
/// `public` ahead of `pg_catalog`, so a user function of the same signature
/// wins — but when resolution *fails* the built-in families keep their own
/// error rather than inheriting the routine catalog's.
fn known_builtin(name: &str) -> bool {
    crate::catalog_fn::is_catalog_func(name)
        || crate::func::is_scalar(name)
        || crate::datetime_fn::is_datetime_func(name)
        || crate::format_fn::is_format_func(name)
        || crate::json_fn::is_json_func(name)
        || crate::array_fn::is_array_func(name)
        || crate::srf::is_srf(name)
        || crate::window::is_window_only_function(name)
}

/// The argument types a call carries, as far as they can be known without the
/// caller's scope.
///
/// A column reference resolves to [`ArgType::Opaque`], which matches any
/// candidate: overloads are therefore disambiguated by literal arguments, and a
/// name carrying exactly one overload always resolves.
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
/// This is the single seam through which a routine call becomes ordinary SQL;
/// the expression walkers that rewrite a statement before execution call it, so
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
    let given = best_effort_arg_types(args);
    // PL/pgSQL is executed by the scalar runtime rather than inlined. Keeping
    // the call node intact is what lets its arguments vary with each input row.
    match resolve_call(kv, &call.name, &given) {
        Ok(Some(routine)) if routine.language == "plpgsql" => return Ok(None),
        Err(_)
            if routines_named(kv, &call.name)?
                .iter()
                .all(|routine| routine.language == "plpgsql") =>
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
        Ok(Some(routine)) if routine.language == "plpgsql" => routine,
        Ok(_) => return Ok(None),
        Err(_) if known_builtin(&call.name) => return Ok(None),
        Err(error) => return Err(error),
    };
    validate_plpgsql_scalar(&routine)?;
    declared_scalar_result_type(&routine)
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
        let given = crate::eval::static_arg_types(args, scope);
        let result = given.and_then(|given| {
            let Some(routine) = resolve_call(runtime.catalog.as_ref(), &call.name, &given)? else {
                return Err(undefined_routine(format!(
                    "function {} does not exist",
                    call.name
                )));
            };
            if routine.language != "plpgsql" {
                return Err(undefined_routine(format!(
                    "function {} does not exist",
                    call.name
                )));
            }
            validate_plpgsql_scalar(&routine)?;
            declared_scalar_result_type(&routine).ok_or_else(|| {
                ExecError::Unsupported(format!(
                    "function {} has no scalar result type",
                    routine.identity()
                ))
            })
        });
        Some(result)
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
        resolve_call(runtime.catalog.as_ref(), &call.name, &given)
            .is_ok_and(|routine| routine.is_some_and(|routine| routine.language == "plpgsql"))
    })
}

/// Evaluate a PL/pgSQL scalar call against the current input row. Arguments
/// are evaluated exactly once before parameter binding; the procedural body
/// then uses ordinary scalar evaluation, so nested calls and lazy SQL
/// conditionals keep the caller's semantics.
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
            if routine.language != "plpgsql" {
                return Err(undefined_routine(format!(
                    "function {} does not exist",
                    call.name
                )));
            }
            validate_plpgsql_scalar(&routine)?;
            for default in &bound_args[values.len()..] {
                values.push(eval_arg(default)?);
            }
            let params = routine
                .input_params()
                .map(|param| param.ty.column)
                .collect::<Vec<_>>();
            crate::eval::coerce_unknown_args(&bound_args, &mut values, &params, ctx)?;
            if routine.strict && values.iter().any(Datum::is_null) {
                return Ok(Datum::Null);
            }
            let _guard = enter_plpgsql_call()?;
            let value = if crate::plpgsql::scalar_function_requires_session(
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
                        routine: routine.clone(),
                        values: values.clone(),
                        kind: FunctionRequestKind::Scalar,
                        reply,
                    })
                    .map_err(|_| {
                        ExecError::ObjectNotInPrerequisiteState(
                            "PL/pgSQL function executor stopped".into(),
                        )
                    })?;
                match response.recv().map_err(|_| {
                    ExecError::ObjectNotInPrerequisiteState(
                        "PL/pgSQL function executor stopped".into(),
                    )
                })?? {
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
            match declared_scalar_result_type(&routine) {
                Some(ty) => {
                    crabka_pgtypes::cast::cast(&value, ty, &ctx.time_zone).map_err(ExecError::from)
                }
                None if declared_returns_void(&routine) => Ok(Datum::Null),
                None => Ok(value),
            }
        })();
        Some(result)
    })
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
    if declared_output_parameter_count(routine) > 1 {
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
        BodyForm::Source | BodyForm::Atomic => routine.body.clone(),
    };
    crabka_pgparser::parse(&source).map_err(|error| ExecError::Syntax(error.message))
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

/// A `LANGUAGE sql` routine's final query — the one whose result is the
/// routine's result.
///
/// `PostgreSQL` runs EVERY statement in the body and returns the last one's
/// result. Gres reaches a SQL routine only by inlining its final query into the
/// calling query, which cannot run the statements before it — so a body with
/// more than one statement is refused rather than silently losing them. Running
/// only the last statement would make `INSERT INTO audit VALUES ($1); SELECT $1`
/// return the right answer while dropping the write.
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
/// a parameter used twice evaluates its argument twice — `f(nextval('s'))` over
/// a body of `SELECT $1 + $1` would consume two sequence values. A literal or a
/// plain column reference is free to duplicate; anything that could call a
/// function is not. `PostgreSQL`'s own inliner makes the same check before
/// inlining.
fn unsafe_to_duplicate(arg: &Expr) -> bool {
    match arg {
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Param(_)
        | Expr::Column { .. } => false,
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => unsafe_to_duplicate(expr),
        Expr::Binary { left, right, .. } => unsafe_to_duplicate(left) || unsafe_to_duplicate(right),
        Expr::Func(call)
            if matches!(
                call.name.to_ascii_lowercase().as_str(),
                "int4range" | "numrange" | "tsrange" | "tstzrange" | "daterange" | "int8range"
            ) =>
        {
            match &call.args {
                FuncArgs::Exprs(args) => args.iter().any(unsafe_to_duplicate),
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
    /// Counted on `substitute`'s own traversal rather than a second walk, and
    /// checked by [`Binding::reject_repeated_volatile_args`] once it finishes.
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
    /// makes the same check; here there is no non-inlined path to fall back to,
    /// so the call is refused rather than answered wrongly.
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
    let mut out: Vec<Expr> = args
        .iter()
        .zip(params.iter())
        .map(|(arg, param)| match param.ty.column {
            Some(ty) if crate::func::is_unknown_arg(arg) => Expr::Cast {
                expr: Box::new(arg.clone()),
                ty,
            },
            _ => arg.clone(),
        })
        .chain(args.iter().skip(params.len()).cloned())
        .collect();
    for param in params.iter().skip(args.len()) {
        let Some(default) = &param.default else {
            return Err(undefined_routine(format!(
                "function {} does not exist",
                routine.name
            )));
        };
        out.push(
            crabka_pgparser::parser::parse_expression(default)
                .map_err(|error| ExecError::Syntax(error.message))?,
        );
    }
    Ok(out)
}

/// Substitute a routine's parameters into one of its body expressions.
fn substitute(binding: &Binding, expr: &Expr) -> Result<Expr, ExecError> {
    use crabka_pgparser::ast::ArraySubscript;

    let sub = |e: &Expr| substitute(binding, e);
    let boxed =
        |e: &Expr| -> Result<Box<Expr>, ExecError> { Ok(Box::new(substitute(binding, e)?)) };
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
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
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
            name: call.name.clone(),
            distinct: call.distinct,
            args: match &call.args {
                FuncArgs::Star => FuncArgs::Star,
                FuncArgs::Exprs(args) => FuncArgs::Exprs(list(args)?),
            },
            filter: None,
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

/// A SQL routine body that is a single `SELECT <expr>` over no relation — the
/// shape that inlines into the caller's own expression.
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
        Some(body) => substitute(&binding, body)?,
        // A body that reads a relation becomes a scalar subquery, which the
        // subquery pass runs under the caller's snapshot. That pass resolves
        // only *uncorrelated* subqueries, so an argument that varies per row is
        // refused there rather than answered wrongly.
        None => Expr::ScalarSubquery(Box::new(substitute_in_query(&binding, &query)?)),
    };
    binding.reject_repeated_volatile_args()?;
    let inlined = match declared_scalar_result_type(&routine) {
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
/// Only the shapes a SQL function body actually uses are rewritten; a body
/// whose parameters would have to reach into a nested query is refused rather
/// than silently left unbound.
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
            *expr = substitute(binding, expr)?;
        }
    }
    if let Some(filter) = &mut select.filter {
        *filter = substitute(binding, filter)?;
    }
    if let Some(having) = &mut select.having {
        *having = substitute(binding, having)?;
    }
    for group in &mut select.group_by {
        *group = substitute(binding, group)?;
    }
    for item in &mut out.order_by {
        item.expr = substitute(binding, &item.expr)?;
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
                for arg in &mut call.args {
                    *arg = substitute(binding, arg)?;
                }
            }
        }
    }
    Ok(())
}

/// Does this FROM-clause function call name a user routine rather than a
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
) -> Result<(QueryExpr, Vec<String>), ExecError> {
    if call.column_defs.is_some() {
        return Err(ExecError::Unsupported(
            "a column definition list on a user-defined function is not supported".into(),
        ));
    }
    let given = best_effort_arg_types(&call.args);
    let (query, routine) = expand_table_function(kv, call, &given)?
        .ok_or_else(|| undefined_routine(format!("function {} does not exist", call.name)))?;
    let names = table_function_columns(&routine).unwrap_or_else(|| vec![routine.name.clone()]);
    Ok((query, names))
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
    if routine.language != "plpgsql" {
        return Ok(None);
    }
    if routine.kind != RoutineKind::Function {
        return Err(wrong_routine_kind(format!(
            "{} is a procedure\nHINT:  To call a procedure, use CALL.",
            spelled_signature(&routine)
        )));
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
        RoutineResult::Unspecified => routine
            .output_params()
            .enumerate()
            .map(|(index, param)| {
                param
                    .ty
                    .column
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
            .collect::<Result<Vec<_>, _>>()?,
        RoutineResult::Type { ty, .. } if is_record_type(ty) => call
            .column_defs
            .as_ref()
            .ok_or_else(|| {
                ExecError::Syntax(format!(
                    "a column definition list is required for functions returning record: {}",
                    routine.identity()
                ))
            })?
            .iter()
            .map(|column| (column.name.clone(), column.ty))
            .collect(),
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
        for default in &bound_args[values.len()..] {
            values.push(crate::eval::eval(
                default,
                &crate::scope::Scope::empty(),
                &[],
                ctx,
            )?);
        }
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
        let requests = runtime.requests.ok_or_else(|| {
            ExecError::Unsupported("PL/pgSQL table function requires a session executor".into())
        })?;
        let (reply, response) = std::sync::mpsc::channel();
        requests
            .try_send(ScalarFunctionRequest {
                routine,
                values,
                kind: FunctionRequestKind::Table(columns.clone()),
                reply,
            })
            .map_err(|_| {
                ExecError::ObjectNotInPrerequisiteState("PL/pgSQL function executor stopped".into())
            })?;
        match response.recv().map_err(|_| {
            ExecError::ObjectNotInPrerequisiteState("PL/pgSQL function executor stopped".into())
        })?? {
            FunctionRequestResult::Table(rows) => Ok(Some((columns, rows))),
            FunctionRequestResult::Scalar(value) => Ok(Some((columns, vec![vec![value]]))),
        }
    })
}

/// The statements a `CALL` of a SQL procedure runs, with the call's arguments
/// substituted for the procedure's parameters.
pub(crate) fn expand_procedure_call(
    kv: &dyn Kv,
    name: &str,
    args: &[Expr],
) -> Result<Vec<Statement>, ExecError> {
    let given = crate::eval::static_arg_types(args, &crate::scope::Scope::empty())?;
    let BoundRoutineCall { routine, args } =
        bind_call(kv, name, args, &given)?.ok_or_else(|| {
            undefined_routine(format!(
                "procedure {name}({}) does not exist",
                given
                    .iter()
                    .copied()
                    .map(spelled_arg_type)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
    if routine.kind != RoutineKind::Procedure {
        return Err(wrong_routine_kind(format!(
            "{} is not a procedure\nHINT:  To call a function, use SELECT.",
            spelled_signature(&routine)
        )));
    }
    callable(&routine)?;
    let binding = Binding {
        routine: &routine,
        uses: std::cell::RefCell::new(vec![0; args.len()]),
        args,
    };
    parse_body(&routine)?
        .iter()
        .map(|statement| match statement {
            Statement::Query(query) => Ok(Statement::Query(substitute_in_query(&binding, query)?)),
            other if routine.params.is_empty() => Ok(other.clone()),
            _ => Err(uncallable(
                &routine,
                "only query statements in a procedure body can take the call's arguments",
            )),
        })
        .collect()
}

/// `DO` — refused for every language, with the reason `PostgreSQL` gives when
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

/// `pg_get_function_arguments` — every parameter, with modes and defaults.
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

/// `pg_get_function_identity_arguments` — the input parameters only, with no
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

/// `pg_get_function_result` — `NULL` for a procedure, as `PostgreSQL` reports.
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

/// `pg_get_functiondef` — the `CREATE OR REPLACE` text `PostgreSQL` renders.
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
    if routine.rows > 0.0 {
        qualifiers.push(format!("ROWS {}", render_number(routine.rows)));
    }
    if !qualifiers.is_empty() {
        let _ = writeln!(out, " {}", qualifiers.join(" "));
    }
    for entry in &routine.config {
        let _ = writeln!(out, " SET {entry}");
    }
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
            let _ = writeln!(out, "BEGIN ATOMIC\n {};\nEND", routine.body);
        }
        BodyForm::Return => {
            let _ = writeln!(out, "RETURN {}", routine.body);
        }
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
        let arg_type_oids: Vec<String> = inputs
            .iter()
            .map(|param| type_oid(&param.ty).to_string())
            .collect();
        let all_types: Vec<Datum> = catalog_all_types(&routine);
        let all_modes: Vec<Datum> = catalog_all_modes(&routine);
        let arg_names: Vec<Datum> = routine
            .params
            .iter()
            .map(|param| Datum::Text(param.name.clone().unwrap_or_default()))
            .collect();
        let has_names = routine.params.iter().any(|param| param.name.is_some());
        rows.push(vec![
            Datum::Int4(i32::try_from(routine.oid).unwrap_or(0)),
            Datum::Text(routine.name.clone()),
            Datum::Int4(crate::exec::PUBLIC_NAMESPACE_OID),
            Datum::Int4(crate::catalog_fn::BOOTSTRAP_ROLE_OID),
            Datum::Int4(language_oid(&routine.language)),
            Datum::Float8(routine.cost),
            Datum::Float8(routine.rows),
            Datum::Int4(0),
            Datum::Null,
            Datum::Text(routine.kind.catalog_code().to_string()),
            Datum::Bool(routine.security_definer),
            Datum::Bool(routine.leakproof),
            Datum::Bool(routine.strict),
            Datum::Bool(routine.returns_set()),
            Datum::Text(routine.volatility.to_string()),
            Datum::Text(routine.parallel.to_string()),
            Datum::Int2(i16::try_from(inputs.len()).unwrap_or(0)),
            Datum::Int2(i16::try_from(routine.default_count()).unwrap_or(0)),
            Datum::Int4(return_type_oid(&routine)),
            Datum::Text(arg_type_oids.join(" ")),
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
            Datum::Null,
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

/// `pg_proc.proallargtypes` — every parameter's type, in declaration order.
fn catalog_all_types(routine: &Routine) -> Vec<Datum> {
    let mut out: Vec<Datum> = routine
        .params
        .iter()
        .map(|param| Datum::Int4(type_oid(&param.ty)))
        .collect();
    if let RoutineResult::Table(columns) = &routine.result {
        out.extend(columns.iter().map(|(_, ty)| Datum::Int4(type_oid(ty))));
    }
    out
}

/// `pg_proc.proargmodes` — `t` for a `RETURNS TABLE` column, as `PostgreSQL`
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
const TYPE_OIDS: &[(&str, i32)] = &[
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

/// A signature type's `pg_type.oid`; `0` for a relation's composite type.
fn type_oid(ty: &RoutineType) -> i32 {
    ty.column.map_or_else(
        || named_type_oid(&ty.name),
        |column| i32::try_from(column.oid()).unwrap_or(0),
    )
}

/// `pg_proc.prorettype`.
fn return_type_oid(routine: &Routine) -> i32 {
    match &routine.result {
        RoutineResult::Type { ty, .. } => type_oid(ty),
        RoutineResult::Table(_) => 2249,
        RoutineResult::Unspecified => {
            let outputs: Vec<&RoutineParam> = routine.output_params().collect();
            match outputs.as_slice() {
                [] => 2278,
                [only] => type_oid(&only.ty),
                _ => 2249,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::MemKv;
    use crabka_pgparser::ast::Statement;

    use super::*;

    /// Run `sql` as a definition against `kv`, returning the completion tag.
    fn define(kv: &MemKv, sql: &str) -> Result<String, ExecError> {
        let statements = crabka_pgparser::parse(sql).expect("definition parses");
        let [Statement::CreateRoutine(stmt)] = statements.as_slice() else {
            panic!("{sql} is not a routine definition");
        };
        let (result, ops) = create(kv, stmt, "crab")?;
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
            (
                "CREATE FUNCTION g() RETURNS nosuchtype AS 'SELECT 1' LANGUAGE sql",
                "42704",
                "type \"nosuchtype\" does not exist",
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
    fn defaults_widen_the_arity_a_call_may_use() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE FUNCTION d(a int, b int DEFAULT 2) RETURNS int AS 'SELECT $1 + $2' \
             LANGUAGE sql STRICT",
        )
        .expect("definition");
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
    fn a_sql_body_inlines_into_the_callers_expression() {
        let kv = seeded();
        let call = FuncCall {
            name: "add2".into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![
                Expr::IntLiteral("1".into()),
                Expr::IntLiteral("2".into()),
            ]),
            filter: None,
        };
        let inlined = inline_scalar(&kv, &call)
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
            name: "named".into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![Expr::IntLiteral("4".into())]),
            filter: None,
        };
        let inlined = inline_scalar(&kv, &call)
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
            name: "upper".into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![Expr::StringLiteral("x".into())]),
            filter: None,
        };
        assert!(inline_scalar(&kv, &call).expect("no error").is_none());
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
            name: "p".into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![Expr::IntLiteral("1".into())]),
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
    fn calling_a_function_with_call_is_42809() {
        let kv = seeded();
        let error = expand_procedure_call(
            &kv,
            "add2",
            &[Expr::IntLiteral("1".into()), Expr::IntLiteral("2".into())],
        )
        .expect_err("a function is not callable");
        assert!(sqlstate(&error) == "42809");
        assert!(
            error
                .into_pg()
                .message
                .contains("add2(integer, integer) is not a procedure")
        );
    }

    #[test]
    fn a_procedure_body_expands_with_the_calls_arguments() {
        let kv = MemKv::default();
        define(
            &kv,
            "CREATE PROCEDURE p(x int) LANGUAGE sql AS $$ SELECT x + 1 $$",
        )
        .expect("procedure");
        let expanded =
            expand_procedure_call(&kv, "p", &[Expr::IntLiteral("1".into())]).expect("expands");
        assert!(expanded.len() == 1);
        assert!(expanded == crabka_pgparser::parse("SELECT 1 + 1").expect("the substituted body"));
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
        assert!(rows.len() == 1);
        let row = &rows[0];
        assert!(row[1] == Datum::Text("pp".into()));
        assert!(row[9] == Datum::Text("f".into()));
        assert!(row[12] == Datum::Bool(true));
        assert!(row[13] == Datum::Bool(true));
        assert!(row[14] == Datum::Text("i".into()));
        assert!(row[16] == Datum::Int2(2));
        assert!(row[17] == Datum::Int2(1));
        assert!(row[19] == Datum::Text("23 23".into()));
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
}
