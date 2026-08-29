//! The object-identifier types — `regproc`, `regprocedure`, `regoper`,
//! `regoperator`, `regclass`, `regtype`, `regconfig`, `regdictionary`,
//! `regnamespace`, `regrole` and `regcollation` — and the `to_reg*` family.
//!
//! Every one of them is an `oid` underneath. What separates them is a pair of
//! I/O functions: the input function resolves a written *name* against a
//! catalog, and the output function renders an oid back as the name that oid
//! reads as. That round trip is the whole contract, and it is why the rendering
//! is schema-qualified exactly when the bare name would not find the object
//! again.
//!
//! The distinction between `'x'::regfoo` and `to_regfoo('x')` is the point of
//! half of `regproc.sql`: the cast raises whatever its input function raises,
//! and `to_regfoo` swallows it and answers NULL. PostgreSQL implements that by
//! running the same input function under an `ErrorSaveContext`, so the set of
//! errors `to_regfoo` absorbs is exactly the set the input function reports
//! *softly* — a missing object (42883/42704/42P01/3F000), an ambiguous name
//! (42725), a malformed one (42602/22P02). A hard error still escapes:
//! `to_regtype('way.too.many.names')` is 42601 in PostgreSQL too. [`soft`]
//! draws that line.
//!
//! Three details are neither guessable nor symmetric, and all three were taken
//! from `postgres:18.4`:
//!
//! * `regoper` and `regoperator` render `InvalidOid` as `0`, while the other
//!   nine render it as `-`. They also have no `-` input shortcut, so
//!   `'-'::regoper` is an ambiguous *operator name* (42725) rather than zero.
//! * `regproc` refuses an overloaded name (42725 `more than one function
//!   named "abs"`) where `regprocedure`, given the argument types, resolves it.
//!   `regoper` refuses an overloaded operator the same way, but with the name
//!   unquoted — `more than one operator named -`.
//! * `regrole` and `regnamespace` accept exactly one name part; two is 42602
//!   `invalid name syntax`, not a missing object.

use crabka_pgkv::Kv;
use crabka_pgparser::ast::{Expr, FuncCall};
use crabka_pgtypes::{ColumnType, Datum, RegclassValue};

use crate::{clock::EvalCtx, error::ExecError, func::require_arity, scope::Scope};

/// Which of the eleven object-identifier types a value or cast names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum RegKind {
    Class,
    Type,
    Proc,
    Procedure,
    Oper,
    Operator,
    Config,
    Dictionary,
    Namespace,
    Role,
    Collation,
}

impl RegKind {
    /// The kind a cast target names, or `None` for a type outside the family.
    pub(crate) fn of(ty: ColumnType) -> Option<Self> {
        Some(match ty {
            ColumnType::Regclass => Self::Class,
            ColumnType::Regtype => Self::Type,
            ColumnType::Regproc => Self::Proc,
            ColumnType::Regprocedure => Self::Procedure,
            ColumnType::Regoper => Self::Oper,
            ColumnType::Regoperator => Self::Operator,
            ColumnType::Regconfig => Self::Config,
            ColumnType::Regdictionary => Self::Dictionary,
            ColumnType::Regnamespace => Self::Namespace,
            ColumnType::Regrole => Self::Role,
            ColumnType::Regcollation => Self::Collation,
            _ => return None,
        })
    }

    /// The cast target this kind names — the result type of both `regfoo(x)`
    /// and `to_regfoo(x)`.
    pub(crate) const fn column_type(self) -> ColumnType {
        match self {
            Self::Class => ColumnType::Regclass,
            Self::Type => ColumnType::Regtype,
            Self::Proc => ColumnType::Regproc,
            Self::Procedure => ColumnType::Regprocedure,
            Self::Oper => ColumnType::Regoper,
            Self::Operator => ColumnType::Regoperator,
            Self::Config => ColumnType::Regconfig,
            Self::Dictionary => ColumnType::Regdictionary,
            Self::Namespace => ColumnType::Regnamespace,
            Self::Role => ColumnType::Regrole,
            Self::Collation => ColumnType::Regcollation,
        }
    }

    /// What `reg*out` prints for `InvalidOid`. Two members of the family print
    /// `0` rather than `-`, because their input functions reserve `-` for an
    /// operator name.
    const fn invalid_oid_text(self) -> &'static str {
        match self {
            Self::Oper | Self::Operator => "0",
            _ => "-",
        }
    }

    /// Whether the input function's leading `-` means `InvalidOid`.
    /// `regoperin`/`regoperatorin` use `parseNumericOid`, which does not
    /// special-case it; the other nine use `parseDashOrOid`, which does.
    const fn accepts_dash(self) -> bool {
        !matches!(self, Self::Oper | Self::Operator)
    }
}

/// Was this error one PostgreSQL reports *softly*, so `to_reg*` answers NULL
/// rather than propagating it?
///
/// PostgreSQL draws the line at `ereturn` vs `ereport` inside the input
/// function: everything the resolution itself can conclude is soft, and a
/// failure of the surrounding machinery — the type-name grammar `regtypein`
/// borrows from the parser, a name with four dotted parts, a cross-database
/// reference — is not. Reproducing that as a SQLSTATE test rather than a flag
/// on the error keeps the two paths sharing one resolver.
pub(crate) fn soft(error: &ExecError) -> bool {
    matches!(
        error.clone().into_pg().code.as_str(),
        // undefined_function / undefined_table / undefined_object /
        // undefined_schema / ambiguous_function / invalid_name /
        // invalid_text_representation.
        "42883" | "42P01" | "42704" | "3F000" | "42725" | "42602" | "22P02"
    )
}

/// The catalog-aware half of a `… :: reg*` cast, for every member of the
/// family. `None` for an operand the catalog adds nothing to (NULL, an
/// out-of-range `int8`, a type with no conversion), which then takes the pure
/// cast in [`crabka_pgtypes::cast`] and its error reporting.
///
/// # Errors
///
/// Whatever the named type's input function raises for an unresolvable name.
pub(crate) fn reg_cast(
    kind: RegKind,
    value: &Datum,
    ctx: &EvalCtx,
) -> Result<Option<Datum>, ExecError> {
    // Without a catalog — a planning context or a unit test — there is nothing
    // to resolve a name against, so the pure cast's bare-oid rendering stands.
    let Some(kv) = ctx.catalog() else {
        return Ok(None);
    };
    // `regclass` is the one member whose name resolution needs the session's
    // search path, and it already has a resolver that applies it. Delegating
    // rather than reimplementing is what keeps `regclass('t')`, `to_regclass`
    // and `'t'::regclass` from drifting apart.
    if kind == RegKind::Class {
        return crate::catalog_fn::regclass_cast(kv, ctx.resolution(), value);
    }
    let oid = match value {
        Datum::Text(text) => match parse_oid_literal(kind, text.trim()) {
            Some(oid) => oid,
            None => resolve(kind, text.trim(), kv, ctx.resolution())?,
        },
        Datum::Int4(oid) => *oid,
        Datum::Int8(oid) => match i32::try_from(*oid) {
            Ok(oid) => oid,
            Err(_) => return Ok(None),
        },
        Datum::Oid(oid) => *oid as i32,
        Datum::Regclass(value) => value.oid,
        _ => return Ok(None),
    };
    Ok(Some(Datum::Regclass(RegclassValue {
        oid,
        name: render(kind, oid, kv)?.into(),
    })))
}

/// The value a stored oid reads back as: the same oid, rendered the way the
/// column's own `reg*out` renders it.
///
/// The row encoding keeps a `reg*` as its bare oid — all PostgreSQL keeps on
/// disk too — so the name has to be re-derived on the way out. Doing it here
/// rather than storing it is what makes a stored value follow a `RENAME` and
/// fall back to the bare oid once the object it names is dropped.
///
/// # Errors
///
/// Propagates the catalog read the rendering needs.
pub(crate) fn stored_value(
    kind: RegKind,
    oid: i32,
    kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
) -> Result<RegclassValue, ExecError> {
    if kind == RegKind::Class {
        return crate::exec::regclass_by_oid(kv, scope, oid);
    }
    Ok(RegclassValue {
        oid,
        name: render(kind, oid, kv)?.into(),
    })
}

/// `parseDashOrOid` / `parseNumericOid`: an all-digit string is an oid, and for
/// nine of the eleven a bare `-` is `InvalidOid`. Anything else is a name.
fn parse_oid_literal(kind: RegKind, text: &str) -> Option<i32> {
    if kind.accepts_dash() && text == "-" {
        return Some(0);
    }
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // `oidin` is unsigned 32-bit, so oids past 2^31 arrive as negative i32s and
    // render back through the same unsigned reading.
    text.parse::<u32>().ok().map(|oid| oid as i32)
}

/// Resolve a written name to an oid, raising exactly what the named type's
/// input function raises when it cannot. Only the seven types this module owns
/// reach here; the other four are delegated in [`reg_cast`].
fn resolve(
    kind: RegKind,
    written: &str,
    kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
) -> Result<i32, ExecError> {
    match kind {
        RegKind::Type => parse_type_string(kv, scope, written).map(|(oid, _)| oid),
        RegKind::Proc => resolve_proc(kv, written),
        RegKind::Procedure => resolve_procedure(kv, scope, written),
        RegKind::Oper => resolve_oper(written),
        RegKind::Operator => resolve_operator(kv, scope, written),
        RegKind::Config => resolve_text_search(kv, written, TextSearch::Config),
        RegKind::Dictionary => resolve_text_search(kv, written, TextSearch::Dictionary),
        RegKind::Namespace => resolve_namespace(kv, written),
        RegKind::Role => resolve_role(kv, written),
        RegKind::Collation => resolve_collation(written),
        RegKind::Class => unreachable!("delegated in reg_cast"),
    }
}

/// Render an oid the way the named type's output function does: `InvalidOid`
/// as its per-type marker, an oid no catalog row matches as the bare number,
/// and anything else as the name that oid reads back as.
fn render(kind: RegKind, oid: i32, kv: &dyn Kv) -> Result<String, ExecError> {
    if oid == 0 {
        return Ok(kind.invalid_oid_text().to_string());
    }
    let name = match kind {
        // `regtypeout` is `format_type`, which already falls back to the bare
        // oid, so it never yields `None`.
        RegKind::Type => Some(crate::exec::regtype_name(oid)),
        RegKind::Proc => proc_name(kv, oid)?,
        RegKind::Procedure => procedure_name(kv, oid)?,
        RegKind::Oper => oper_name(oid),
        RegKind::Operator => operator_name(oid),
        RegKind::Config => text_search_name(kv, oid, TextSearch::Config)?,
        RegKind::Dictionary => text_search_name(kv, oid, TextSearch::Dictionary)?,
        RegKind::Namespace => namespace_name(kv, oid)?.map(quote),
        RegKind::Role => role_name(kv, oid)?.map(quote),
        RegKind::Collation => collation_name(oid),
        RegKind::Class => unreachable!("delegated in reg_cast"),
    }
    .unwrap_or_else(|| unsigned(oid));
    Ok(name)
}

/// The unsigned decimal an oid renders as when no catalog row matches it —
/// `oidout`, which every `reg*out` falls back to.
fn unsigned(oid: i32) -> String {
    (oid as u32).to_string()
}

/// `quote_identifier`: a name is bare when it reads back as itself, and
/// double-quoted (with embedded quotes doubled) otherwise.
fn quote(name: String) -> String {
    crate::catalog_fn::quote_identifier(&name)
}

/// The one written name part `regrole` and `regnamespace` accept. Both refuse a
/// dotted name outright — the object they name has no schema to be in.
fn single_part(written: &str) -> Result<String, ExecError> {
    let parts = crate::relname::split_identifier_string(written).ok_or_else(invalid_name_syntax)?;
    match parts.as_slice() {
        [only] => Ok(only.clone()),
        _ => Err(invalid_name_syntax()),
    }
}

fn invalid_name_syntax() -> ExecError {
    crate::relname::invalid_name_syntax()
}

/// The parts of a name that *may* be schema-qualified, plus the rendering
/// PostgreSQL echoes back in the error message.
///
/// That rendering is `NameListToString`, which joins the *parsed* parts with
/// dots and re-quotes nothing — so `'ng_catalog."POSIX"'` is echoed as
/// `ng_catalog.POSIX`, with the quotes the input carried already consumed.
fn qualified(written: &str) -> Result<(Option<String>, String, String), ExecError> {
    let parts = crate::relname::split_identifier_string(written).ok_or_else(invalid_name_syntax)?;
    let dotted = parts.join(".");
    match parts.as_slice() {
        [name] => Ok((None, name.clone(), dotted)),
        [schema, name] => Ok((Some(schema.clone()), name.clone(), dotted)),
        _ => Err(invalid_name_syntax()),
    }
}

fn undefined_function(message: String) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42883",
        message,
    }
}

fn ambiguous(message: String) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42725",
        message,
    }
}

// ---------------------------------------------------------------- regproc ---

/// `regprocin`: a bare or schema-qualified function name that must match
/// exactly one `pg_proc` row. More than one is 42725 — the difference from
/// `regprocedure`, which is handed the argument types and so never has to
/// choose.
fn resolve_proc(kv: &dyn Kv, written: &str) -> Result<i32, ExecError> {
    let (schema, name, _) = qualified(written)?;
    let matches = proc_oids(kv, schema.as_deref(), &name)?;
    match matches.as_slice() {
        [] => Err(undefined_function(format!(
            "function \"{written}\" does not exist"
        ))),
        [oid] => Ok(*oid),
        _ => Err(ambiguous(format!(
            "more than one function named \"{written}\""
        ))),
    }
}

/// The `pg_proc` oids of every overload of `name`, in `schema` when one was
/// written. crabka's namespaces are `pg_catalog` and `public` plus the user's
/// schemas, and every one of them is on the search path a bare name is resolved
/// against, so an unqualified name matches on the name alone.
fn proc_oids(kv: &dyn Kv, schema: Option<&str>, name: &str) -> Result<Vec<i32>, ExecError> {
    let wanted = schema
        .map(|schema| namespace_oid(kv, schema))
        .transpose()?
        .flatten();
    if schema.is_some() && wanted.is_none() {
        return Ok(Vec::new());
    }
    // Indexed by name over the built-ins. `oid IN ('f'::regproc, ...)` puts a
    // constant cast in a per-row filter, so this runs once per catalog row per
    // list element; walking `pg_proc_rows` here cost 72 s on a three-function
    // database and was what made a full certification time out.
    let mut oids: Vec<i32> = builtin_proc_index()
        .and_then(|(_, _, _, by_name)| by_name.get(name))
        .map(|found| {
            found
                .iter()
                .filter(|(_, namespace)| wanted.is_none_or(|oid| *namespace == Some(oid)))
                .map(|(oid, _)| *oid)
                .collect()
        })
        .unwrap_or_default();
    oids.extend(
        crate::routine::user_pg_proc_rows(kv)?
            .iter()
            .filter(|row| row.get(1) == Some(&Datum::Text(name.to_string())))
            .filter(|row| wanted.is_none_or(|oid| row.get(2) == Some(&Datum::Int4(oid))))
            .filter_map(|row| match row.first() {
                Some(Datum::Int4(oid)) => Some(*oid),
                _ => None,
            }),
    );
    Ok(oids)
}

/// `regprocout`: the bare function name when that name would find this row and
/// only this row, and the schema-qualified name when it would not — which is
/// what makes an overloaded `abs` print as `pg_catalog.abs`.
/// oid -> (name, namespace) and name -> how many procs carry it, over the
/// built-in fixture alone.
///
/// `regprocout` resolves one oid per rendered row. Walking `pg_proc_rows` for
/// each of them made any scan projecting a `reg*` value quadratic in the
/// catalog: 3,400 rows times a 3,400-row search, twice over (once for the oid,
/// once to decide whether the name is unique). The built-in half never changes,
/// so it is indexed once; only the handful of user routines are walked per
/// call.
type ProcIndex = (
    std::collections::HashMap<i32, (String, Option<i32>)>,
    std::collections::HashMap<String, usize>,
    std::collections::HashMap<i32, Vec<i32>>,
    std::collections::HashMap<String, Vec<(i32, Option<i32>)>>,
);
static BUILTIN_PROC_INDEX: std::sync::OnceLock<Option<ProcIndex>> = std::sync::OnceLock::new();

fn builtin_proc_index() -> Option<&'static ProcIndex> {
    BUILTIN_PROC_INDEX
        .get_or_init(|| {
            let rows = crate::routine::builtin_pg_proc_rows().ok()?;
            let mut by_oid = std::collections::HashMap::with_capacity(rows.len());
            let mut counts: std::collections::HashMap<String, usize> =
                std::collections::HashMap::with_capacity(rows.len());
            let mut args: std::collections::HashMap<i32, Vec<i32>> =
                std::collections::HashMap::with_capacity(rows.len());
            let mut by_name: std::collections::HashMap<String, Vec<(i32, Option<i32>)>> =
                std::collections::HashMap::with_capacity(rows.len());
            for row in &rows {
                let (Some(Datum::Int4(oid)), Some(Datum::Text(name))) = (row.first(), row.get(1))
                else {
                    continue;
                };
                let namespace = match row.get(2) {
                    Some(Datum::Int4(namespace)) => Some(*namespace),
                    _ => None,
                };
                by_oid.insert(*oid, (name.clone(), namespace));
                *counts.entry(name.clone()).or_default() += 1;
                by_name
                    .entry(name.clone())
                    .or_default()
                    .push((*oid, namespace));
                if let Some(Datum::OidVector(vector)) = row.get(19) {
                    args.insert(
                        *oid,
                        vector
                            .elems
                            .iter()
                            .filter_map(|arg| match arg {
                                Datum::Int4(oid) => Some(*oid),
                                _ => None,
                            })
                            .collect(),
                    );
                }
            }
            Some((by_oid, counts, args, by_name))
        })
        .as_ref()
}

/// The name and input type oids of the built-in routine `oid` names.
///
/// The pair `pg_function_is_visible` shadows on: two routines hide one another
/// only when both the name and the whole argument list match, which is why an
/// overload on different arguments is visible alongside an earlier namesake.
pub(crate) fn builtin_proc_signature(oid: i32) -> Option<(&'static str, &'static [i32])> {
    let (by_oid, _, args, _) = builtin_proc_index()?;
    let name = by_oid.get(&oid).map(|(name, _)| name.as_str())?;
    Some((name, args.get(&oid).map_or(&[][..], Vec::as_slice)))
}

/// Does the built-in fixture declare a routine of exactly this signature?
pub(crate) fn builtin_proc_declared(name: &str, args: &[i32]) -> bool {
    let Some((_, _, arg_index, by_name)) = builtin_proc_index() else {
        return false;
    };
    by_name.get(name).is_some_and(|candidates| {
        candidates.iter().any(|(oid, _)| {
            arg_index
                .get(oid)
                .map_or(args.is_empty(), |declared| declared == args)
        })
    })
}

/// `regprocout`'s text for a routine oid: bare when the name picks exactly one
/// routine out, and `schema.name` when it does not.
///
/// The qualifier keeps the backend id a temporary namespace's name carries.
/// `format_procedure` asks `get_namespace_name`, not the
/// `get_namespace_name_or_temp` a deparsed reference asks, so `regproc` and
/// `regprocedure` spell what a deparsed reference would spell `pg_temp` as
/// `pg_temp_<n>` — measured on `postgres:18.4`, where a `pg_temp.probe_trig`
/// casts to `pg_temp_46.probe_trig`, and where the same session's
/// `pg_event_trigger_ddl_commands()` reports that routine as
/// `schema = pg_temp` beside `identity = pg_temp_46.probe_trig()` in one row.
/// [`crate::catalog_fn::relation_name_by_oid`] keeps the number for the same
/// reason.
fn proc_name(kv: &dyn Kv, oid: i32) -> Result<Option<String>, ExecError> {
    if oid == crate::routine::POSTGRESQL_FDW_VALIDATOR_OID {
        return Ok(Some("postgresql_fdw_validator".into()));
    }
    // The user routines are few; the built-ins are indexed.
    let user = crate::routine::user_pg_proc_rows(kv)?;
    let index = builtin_proc_index();
    let found = user
        .iter()
        .find(|row| row.first() == Some(&Datum::Int4(oid)))
        .and_then(|row| match (row.get(1), row.get(2)) {
            (Some(Datum::Text(name)), Some(Datum::Int4(namespace))) => {
                Some((name.clone(), Some(*namespace)))
            }
            (Some(Datum::Text(name)), _) => Some((name.clone(), None)),
            _ => None,
        })
        .or_else(|| index.and_then(|(by_oid, _, _, _)| by_oid.get(&oid).cloned()));
    let Some((name, namespace)) = found else {
        return Ok(None);
    };
    let user_count = user
        .iter()
        .filter(|other| other.get(1) == Some(&Datum::Text(name.clone())))
        .count();
    let builtin_count = index.map_or(0, |(_, counts, _, _)| {
        counts.get(&name).copied().unwrap_or(0)
    });
    let unique = user_count + builtin_count == 1;
    if unique {
        return Ok(Some(quote(name.clone())));
    }
    let schema = match namespace {
        Some(namespace) => namespace_name(kv, namespace)?,
        None => None,
    };
    Ok(Some(match schema {
        Some(schema) => format!("{}.{}", quote(schema), quote(name.clone())),
        None => quote(name.clone()),
    }))
}

// ----------------------------------------------------------- regprocedure ---

/// `regprocedurein`: a function name plus the argument types that pick one
/// overload out. Given those, there is never more than one match, which is why
/// this has no ambiguity case where [`resolve_proc`] does.
fn resolve_procedure(
    kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    written: &str,
) -> Result<i32, ExecError> {
    let (name, args) = split_name_and_arg_types(kv, scope, written, false)?;
    let (schema, name, _) = qualified(&name)?;
    let wanted: Vec<Datum> = args.into_iter().map(Datum::Int4).collect();
    let namespace = schema
        .as_deref()
        .map(|schema| namespace_oid(kv, schema))
        .transpose()?
        .flatten();
    if schema.is_some() && namespace.is_none() {
        return Err(undefined_function(format!(
            "function \"{written}\" does not exist"
        )));
    }
    crate::routine::pg_proc_rows(kv)?
        .iter()
        .find(|row| {
            row.get(1) == Some(&Datum::Text(name.clone()))
                && namespace.is_none_or(|oid| row.get(2) == Some(&Datum::Int4(oid)))
                && matches!(row.get(19), Some(Datum::OidVector(args)) if args.elems == wanted)
        })
        .and_then(|row| match row.first() {
            Some(Datum::Int4(oid)) => Some(*oid),
            _ => None,
        })
        .ok_or_else(|| undefined_function(format!("function \"{written}\" does not exist")))
}

/// `regprocedureout`: `name(argtype,argtype)`, with the argument types spelled
/// the way `format_type` spells them and no space after the comma.
fn procedure_name(kv: &dyn Kv, oid: i32) -> Result<Option<String>, ExecError> {
    // Indexed for the same reason `proc_name` is: one lookup per rendered row.
    let user = crate::routine::user_pg_proc_rows(kv)?;
    let found = user
        .iter()
        .find(|row| row.first() == Some(&Datum::Int4(oid)))
        .and_then(|row| match (row.get(1), row.get(19)) {
            (Some(Datum::Text(name)), Some(Datum::OidVector(args))) => Some((
                name.clone(),
                args.elems
                    .iter()
                    .filter_map(|arg| match arg {
                        Datum::Int4(oid) => Some(*oid),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            _ => None,
        })
        .or_else(|| {
            let (by_oid, _, args, _) = builtin_proc_index()?;
            let (name, _) = by_oid.get(&oid)?;
            Some((name.clone(), args.get(&oid).cloned().unwrap_or_default()))
        });
    let Some((name, args)) = found else {
        return Ok(None);
    };
    let rendered = args
        .iter()
        .map(|arg| crate::exec::regtype_name(*arg))
        .collect::<Vec<_>>()
        .join(",");
    Ok(Some(format!("{name}({rendered})")))
}

// ---------------------------------------------------------------- regoper ---

/// `regoperin`: an operator name that must match exactly one `pg_operator`
/// row. The ambiguity message is the one place in the family PostgreSQL does
/// *not* quote the name, because an operator name is never an identifier.
fn resolve_oper(written: &str) -> Result<i32, ExecError> {
    let (schema, name, _) = qualified(written)?;
    let matches = oper_oids(schema.as_deref(), &name);
    match matches.as_slice() {
        [] => Err(undefined_function(format!(
            "operator does not exist: {written}"
        ))),
        [oid] => Ok(*oid),
        _ => Err(ambiguous(format!("more than one operator named {written}"))),
    }
}

/// `regoperatorin`: an operator name plus its two operand types, `NONE`
/// standing in for the missing side of a unary operator.
fn resolve_operator(
    kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    written: &str,
) -> Result<i32, ExecError> {
    let (name, args) = split_name_and_arg_types(kv, scope, written, true)?;
    let (schema, name, _) = qualified(&name)?;
    match args.len() {
        1 => {
            return Err(ExecError::FunctionError {
                sqlstate: "42P02",
                message: "missing argument".into(),
            });
        }
        2 => {}
        _ => {
            return Err(ExecError::FunctionError {
                sqlstate: "54023",
                message: "too many arguments".into(),
            });
        }
    }
    crate::builtin_operators::BUILTIN_OPERATORS
        .iter()
        .find(|(_, candidate, _, _, _, left, right, ..)| {
            *candidate == name
                && *left == args[0]
                && *right == args[1]
                && schema.as_deref().is_none_or(|s| s == "pg_catalog")
        })
        .map(|(oid, ..)| *oid)
        .ok_or_else(|| undefined_function(format!("operator does not exist: {written}")))
}

/// The oids of every operator spelled `name`. crabka declares every built-in
/// operator in `pg_catalog`, so a qualifier only ever selects or excludes that
/// one schema.
fn oper_oids(schema: Option<&str>, name: &str) -> Vec<i32> {
    if schema.is_some_and(|schema| schema != "pg_catalog") {
        return Vec::new();
    }
    crate::builtin_operators::BUILTIN_OPERATORS
        .iter()
        .filter(|(_, candidate, ..)| *candidate == name)
        .map(|(oid, ..)| *oid)
        .collect()
}

/// `regoperout`: the bare operator name when it is unambiguous, and
/// `pg_catalog.<name>` when it is not. The name itself is never quoted.
fn oper_name(oid: i32) -> Option<String> {
    let (_, name, ..) = crate::builtin_operators::BUILTIN_OPERATORS
        .iter()
        .find(|(candidate, ..)| *candidate == oid)?;
    if oper_oids(None, name).len() == 1 {
        Some((*name).to_string())
    } else {
        Some(format!("pg_catalog.{name}"))
    }
}

/// `regoperatorout`: `name(left,right)`, with `NONE` for the absent operand of
/// a unary operator and the operand types spelled as `format_type` spells them.
/// crabka has no user-defined operators, so the name is never qualified.
fn operator_name(oid: i32) -> Option<String> {
    let (_, name, _, _, _, left, right, ..) = crate::builtin_operators::BUILTIN_OPERATORS
        .iter()
        .find(|(candidate, ..)| *candidate == oid)?;
    let operand = |oid: i32| {
        if oid == 0 {
            "NONE".to_string()
        } else {
            crate::exec::regtype_name(oid)
        }
    };
    Some(format!("{name}({},{})", operand(*left), operand(*right)))
}

/// `parseNameAndArgTypes`: split `name(type, type)` into the written name and
/// the oids of the parenthesised type list. `NONE` is `InvalidOid` when
/// `allow_none` — the operator spelling — and an ordinary type name otherwise.
fn split_name_and_arg_types(
    kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    written: &str,
    allow_none: bool,
) -> Result<(String, Vec<i32>), ExecError> {
    let mut in_quote = false;
    let open = written
        .char_indices()
        .find(|&(_, c)| {
            if c == '"' {
                in_quote = !in_quote;
            }
            c == '(' && !in_quote
        })
        .map(|(index, _)| index)
        .ok_or_else(|| invalid_text("expected a left parenthesis"))?;
    let name = written[..open].to_string();
    let rest = written[open + 1..].trim_end();
    let inner = rest
        .strip_suffix(')')
        .ok_or_else(|| invalid_text("expected a right parenthesis"))?;
    let mut args = Vec::new();
    if !inner.trim().is_empty() {
        for arg in split_top_level_commas(inner) {
            let arg = arg.trim();
            if arg.is_empty() {
                return Err(invalid_text("expected a type name"));
            }
            if allow_none && arg.eq_ignore_ascii_case("none") {
                args.push(0);
                continue;
            }
            // An argument is a full type name, modifier and all:
            // `parseNameAndArgTypes` runs each piece through the same
            // `typeStringToTypeName` that `regtypein` runs, and then discards
            // the typmod — an overload is picked by type, never by length.
            args.push(parse_type_string(kv, scope, arg)?.0);
        }
    }
    Ok((name, args))
}

/// The comma-separated pieces of an argument list, ignoring commas inside
/// quotes, parentheses or brackets — a `numeric(10,2)` argument is one piece.
fn split_top_level_commas(inner: &str) -> Vec<&str> {
    let mut pieces = Vec::new();
    let mut start = 0;
    let mut in_quote = false;
    let mut depth = 0i32;
    for (index, c) in inner.char_indices() {
        match c {
            '"' => in_quote = !in_quote,
            ',' if !in_quote && depth == 0 => {
                pieces.push(&inner[start..index]);
                start = index + 1;
            }
            '(' | '[' if !in_quote => depth += 1,
            ')' | ']' if !in_quote => depth -= 1,
            _ => {}
        }
    }
    pieces.push(&inner[start..]);
    pieces
}

fn invalid_text(message: &'static str) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "22P02",
        message: message.into(),
    }
}

// ---------------------------------------------------------------- regtype ---

/// `parseTypeString`: the oid **and** the typmod a written type name resolves
/// to.
///
/// PostgreSQL runs the whole type grammar over the string, which is why
/// `'varchar(32)'::regtype`, `'timestamp(4)'::regtype` and
/// `'double precision'::regtype` all work while the plain name resolver behind
/// [`crate::exec::resolve_type_name`] sees only a `SplitIdentifierString`
/// name. Three of the grammar's productions are reproduced here, and nothing
/// else is:
///
/// * a parenthesised modifier list, lifted out of the middle of the name so
///   that `timestamp(4) with time zone` still reads as one type word;
/// * the compound spellings the grammar reserves — `double precision`,
///   `character varying`, `bit varying` and the four `time`/`timestamp`
///   `with`/`without time zone` forms — which have a space in them and so
///   never reach the name resolver;
/// * the two implicit modifiers, where the keyword `BIT` means `bit(1)` and
///   the keyword `CHARACTER` means `character(1)`. Neither the quoted spelling
///   `"bit"` nor `pg_type`'s own name `bpchar` reaches those productions, so
///   the same two types carry nothing when they are named that way.
///
/// Known divergence: crabka resolves the bare word `char` to `"char"`, the
/// one-byte type, where PostgreSQL's grammar resolves it to `character(1)`. The
/// implicit modifier is therefore keyed on the resolved oid as well as the
/// keyword, so that `to_regtype('char')` and `to_regtypemod('char')` stay
/// consistent with each other rather than describing two different types.
///
/// # Errors
///
/// Everything [`crate::exec::resolve_type_name`] raises, plus the hard 42601
/// the grammar raises for a string that is not a type name at all and the hard
/// 22023 a `typmodin` raises for a modifier the type refuses. Those two are the
/// reason `to_regtype('incorrect type name syntax')` is an ERROR rather than
/// NULL: they are `ereport`, not `ereturn`, so [`soft`] does not cover them,
/// exactly as PostgreSQL leaves them. `regproc.sql` files both under "Some
/// cases that should be soft errors, but are not yet".
fn parse_type_string(
    kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    written: &str,
) -> Result<(i32, i32), ExecError> {
    let spelling = TypeSpelling::read(written);
    let oid = spelling.resolve(kv, scope, &spelling.name)?;
    // `LookupTypeName` reads the modifier against the *element* type and only
    // then switches to the array, which is what makes `numeric(10,2)[]` carry
    // `numerictypmodin`'s answer rather than refusing a modifier `_numeric`
    // has no `typmodin` for.
    let modified = if spelling.element == spelling.name {
        oid
    } else {
        spelling.resolve(kv, scope, &spelling.element)?
    };
    let typmod = spelling.typmod(modified)?;
    Ok((oid, typmod))
}

/// A written type name split the way the grammar reads it.
struct TypeSpelling<'a> {
    /// The whole string as written, which is what the diagnostics echo and
    /// what a reported position counts into.
    written: &'a str,
    /// The type name with its modifier group lifted out, so
    /// `timestamp(4) with time zone` is `timestamp with time zone`.
    name: String,
    /// [`Self::name`] without its array suffix — the type whose `typmodin`
    /// reads the modifier, because `LookupTypeName` applies the modifier to the
    /// element and only then switches to the array. Equal to `name` when
    /// nothing was subscripted.
    element: String,
    /// The text between the parentheses, or `None` when none were written.
    modifier: Option<&'a str>,
    /// Whether the last name part was written in double quotes. A quoted name
    /// is an ordinary `pg_type.typname` lookup that carries no implicit
    /// modifier, which is the whole difference between `bit` and `"bit"`.
    quoted: bool,
}

impl<'a> TypeSpelling<'a> {
    fn read(written: &'a str) -> Self {
        // An empty or all-whitespace string is the one refusal in
        // `typeStringToTypeName` that PostgreSQL reports *softly*, because it
        // is checked before the parser runs and so goes through `ereturn`
        // rather than the grammar. It falls out of the name resolver below as
        // the same soft 42602 crabka already reported for it, so there is no
        // early return here.
        let trimmed = written.trim();
        // The subscripts come off first, because everything left of them is
        // one element type: `numeric(10,2)[]` and `double precision[]` both
        // hide their real shape behind the brackets. PostgreSQL discards the
        // declared bound and the declared *number* of dimensions, so one
        // normalised `[]` stands for whatever was written.
        let (base, array) = match top_level(trimmed, '[') {
            Some(bracket) => (trimmed[..bracket].trim_end(), true),
            None => (trimmed, false),
        };
        let (head, modifier, tail) = match split_modifier(base) {
            Some((open, close)) => (
                base[..open].trim_end(),
                Some(base[open + 1..close].trim()),
                base[close + 1..].trim(),
            ),
            None => (base, None, ""),
        };
        // Whatever follows the modifier is a grammar keyword tail, which is
        // part of the type word: `timestamp(4) with time zone`.
        let element = if tail.is_empty() {
            head.to_string()
        } else {
            format!("{head} {tail}")
        };
        let name = if array {
            format!("{element}[]")
        } else {
            element.clone()
        };
        Self {
            written,
            quoted: last_part_is_quoted(head),
            element,
            name,
            modifier,
        }
    }

    /// The oid a name resolves to, by the compound-spelling table when the name
    /// has a space in it and by the ordinary resolver otherwise.
    fn resolve(
        &self,
        kv: &dyn Kv,
        scope: &crate::relname::ResolutionScope,
        name: &str,
    ) -> Result<i32, ExecError> {
        if second_word(name).is_none() {
            return crate::exec::resolve_type_name(kv, scope, name);
        }
        let folded = name
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        let resolved = match folded.strip_suffix("[]") {
            Some(element) => {
                ColumnType::from_builtin_sql_name(element).and_then(ColumnType::array_of)
            }
            None => ColumnType::from_builtin_sql_name(&folded),
        };
        resolved
            .and_then(|ty| i32::try_from(ty.oid()).ok())
            .ok_or_else(|| self.syntax_error())
    }

    /// The typmod the resolved type packs this modifier list into, or `-1` when
    /// the type carries none.
    fn typmod(&self, oid: i32) -> Result<i32, ExecError> {
        use crabka_pgtypes::oids;

        let family = TypmodFamily::of(oid);
        let Some(written) = self.modifier else {
            // `BIT` and `CHARACTER` are the two productions the grammar hands a
            // modifier the writer did not: both mean length one. Only those
            // *keywords* do, so `"bit"` in quotes and `bpchar` under
            // PostgreSQL's own internal name both stay unmodified even though
            // all four name the same two types.
            if self.quoted {
                return Ok(-1);
            }
            let keyword = self.element.to_ascii_lowercase();
            return Ok(match (keyword.as_str(), u32::try_from(oid)) {
                ("bit", Ok(oids::BIT)) => 1,
                ("char" | "character" | "nchar", Ok(oids::BPCHAR)) => 1 + VARHDRSZ,
                _ => -1,
            });
        };
        let parts = split_top_level_commas(written)
            .into_iter()
            .map(str::trim)
            .map(|part| {
                part.parse::<i32>()
                    .map_err(|_| type_modifier_not_constant())
            })
            .collect::<Result<Vec<i32>, ExecError>>()?;
        family.pack(oid, &parts, &self.name)
    }

    /// The grammar's own complaint about a string that is not a type name,
    /// pointed at the token it stopped on and carrying the CONTEXT
    /// `typeStringToTypeName` pushes around the parse.
    ///
    /// The position counts into the *type string*, not into the statement, and
    /// PostgreSQL reports it that way: the caret under
    /// `pg_input_error_info('incorrect type name syntax', 'regtype')` lands on
    /// character 11 of the statement because 11 is where `type` sits in the
    /// argument. Reproducing the offset reproduces the misplaced caret, which
    /// is what the expected output holds.
    fn syntax_error(&self) -> ExecError {
        let Some((offset, token)) = second_word(self.written) else {
            return invalid_type_name(self.written);
        };
        ExecError::Remote(
            crabka_pgwire::error::PgError::error(
                "42601",
                format!("syntax error at or near \"{token}\""),
            )
            .with_position(self.written[..offset].chars().count() + 1)
            .with_context(format!("invalid type name \"{}\"", self.written)),
        )
    }
}

/// `VARHDRSZ`, which the character types add to their declared length "for
/// largely historical reasons" — enough client code reads a `varchar` typmod
/// as length plus four that PostgreSQL will not change it.
const VARHDRSZ: i32 = 4;

/// The offsets of the first top-level parenthesis pair, or `None` when the
/// spelling carries no modifier group. A parenthesis inside double quotes is
/// part of an identifier and is not one.
fn split_modifier(written: &str) -> Option<(usize, usize)> {
    let open = top_level(written, '(')?;
    let mut in_quote = false;
    written
        .char_indices()
        .skip_while(|&(index, _)| index <= open)
        .find(|&(_, c)| {
            if c == '"' {
                in_quote = !in_quote;
            }
            c == ')' && !in_quote
        })
        .map(|(close, _)| (open, close))
}

/// The offset of the first `wanted` outside double quotes.
fn top_level(written: &str, wanted: char) -> Option<usize> {
    let mut in_quote = false;
    written
        .char_indices()
        .find(|&(_, c)| {
            if c == '"' {
                in_quote = !in_quote;
            }
            c == wanted && !in_quote
        })
        .map(|(index, _)| index)
}

/// The offset and text of the second whitespace-separated word, or `None` for a
/// one-word name. Whitespace inside double quotes belongs to the identifier.
fn second_word(name: &str) -> Option<(usize, &str)> {
    let mut in_quote = false;
    let mut boundary = None;
    for (index, c) in name.char_indices() {
        match c {
            '"' => in_quote = !in_quote,
            c if c.is_whitespace() && !in_quote && boundary.is_none() => boundary = Some(index),
            c if !c.is_whitespace() && boundary.is_some() => {
                let rest = &name[index..];
                let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
                return Some((index, &rest[..end]));
            }
            _ => {}
        }
    }
    None
}

/// Was the last dotted part of `name` written in double quotes?
fn last_part_is_quoted(name: &str) -> bool {
    let mut in_quote = false;
    let mut last_dot = None;
    for (index, c) in name.char_indices() {
        match c {
            '"' => in_quote = !in_quote,
            '.' if !in_quote => last_dot = Some(index),
            _ => {}
        }
    }
    let part = match last_dot {
        Some(dot) => &name[dot + 1..],
        None => name,
    };
    part.trim_start().starts_with('"')
}

/// How a type reads its modifier list — one `typmodin` per shape, named for the
/// shape rather than the type because five types share three of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypmodFamily {
    /// `varchar(n)` / `char(n)`: the length plus `VARHDRSZ`.
    Length,
    /// `numeric(p[,s])`: precision and scale packed into one word.
    PrecisionScale,
    /// `time(p)` / `timestamp(p)`: the fractional-seconds precision, stored raw.
    Seconds,
    /// `interval(p)`: the precision with the field-range mask above it.
    Interval,
    /// `bit(n)` / `bit varying(n)`: the bit count, stored raw.
    Bit,
    /// A type with no `typmodin` at all, which refuses any modifier.
    None,
}

impl TypmodFamily {
    fn of(oid: i32) -> Self {
        use crabka_pgtypes::oids;

        let Ok(oid) = u32::try_from(oid) else {
            return Self::None;
        };
        match oid {
            oids::VARCHAR | oids::BPCHAR => Self::Length,
            oids::NUMERIC => Self::PrecisionScale,
            oids::TIME | oids::TIMETZ | oids::TIMESTAMP | oids::TIMESTAMPTZ => Self::Seconds,
            oids::INTERVAL => Self::Interval,
            oids::BIT | oids::VARBIT => Self::Bit,
            _ => Self::None,
        }
    }

    /// Run the `typmodin` this family names over a parsed modifier list.
    fn pack(self, oid: i32, parts: &[i32], name: &str) -> Result<i32, ExecError> {
        use crabka_pgtypes::oids;

        /// `MaxAttrSize`, the ceiling `anychar_typmodin` puts on a declared
        /// character length.
        const MAX_ATTR_SIZE: i32 = 10 * 1024 * 1024;
        /// `MAX_TIME_PRECISION` and `MAX_TIMESTAMP_PRECISION`, which are equal.
        const MAX_SECONDS_PRECISION: i32 = 6;
        /// `INTERVAL_FULL_RANGE`: every field, which is what `INTERVAL(p)` with
        /// no field qualifier means.
        const INTERVAL_FULL_RANGE: i32 = 0x7fff;

        match (self, parts) {
            (Self::None, _) => Err(modifier_not_allowed(name)),
            (Self::Length, [length]) => {
                // `bpchartypmodin` calls itself `char` and `varchartypmodin`
                // calls itself `varchar`, neither the SQL spelling.
                let spelled = if u32::try_from(oid) == Ok(oids::BPCHAR) {
                    "char"
                } else {
                    "varchar"
                };
                if *length < 1 {
                    return Err(invalid_parameter(format!(
                        "length for type {spelled} must be at least 1"
                    )));
                }
                if *length > MAX_ATTR_SIZE {
                    return Err(invalid_parameter(format!(
                        "length for type {spelled} cannot exceed {MAX_ATTR_SIZE}"
                    )));
                }
                Ok(length + VARHDRSZ)
            }
            (Self::PrecisionScale, [precision] | [precision, _]) => {
                if !(1..=1000).contains(precision) {
                    return Err(invalid_parameter(format!(
                        "NUMERIC precision {precision} must be between 1 and 1000"
                    )));
                }
                let scale = parts.get(1).copied().unwrap_or(0);
                if !(-1000..=1000).contains(&scale) {
                    return Err(invalid_parameter(format!(
                        "NUMERIC scale {scale} must be between -1000 and 1000"
                    )));
                }
                Ok(((precision << 16) | (scale & 0x7ff)) + VARHDRSZ)
            }
            // The one arity error PostgreSQL words for the type rather than
            // generically, and the only reason it is reachable at all: the
            // grammar admits any expression list here, so `numeric(1,2,3)`
            // parses and `numerictypmodin` is what refuses it.
            (Self::PrecisionScale, _) => Err(invalid_parameter("invalid NUMERIC type modifier")),
            (Self::Seconds | Self::Interval, [precision]) => {
                let tz = matches!(u32::try_from(oid), Ok(oids::TIMETZ | oids::TIMESTAMPTZ));
                let spelled = if matches!(u32::try_from(oid), Ok(oids::TIME | oids::TIMETZ)) {
                    "TIME"
                } else if self == Self::Interval {
                    "INTERVAL"
                } else {
                    "TIMESTAMP"
                };
                if *precision < 0 {
                    let zone = if tz { " WITH TIME ZONE" } else { "" };
                    return Err(invalid_parameter(format!(
                        "{spelled}({precision}){zone} precision must not be negative"
                    )));
                }
                // Over the ceiling PostgreSQL clamps and reports a WARNING.
                // Known gap: the clamp is reproduced and the WARNING is not,
                // because nothing on this path can raise one.
                let precision = (*precision).min(MAX_SECONDS_PRECISION);
                Ok(if self == Self::Interval {
                    (INTERVAL_FULL_RANGE << 16) | precision
                } else {
                    precision
                })
            }
            (Self::Bit, [length]) => {
                let spelled = if u32::try_from(oid) == Ok(oids::VARBIT) {
                    "bit varying"
                } else {
                    "bit"
                };
                if *length < 1 {
                    return Err(invalid_parameter(format!(
                        "length for type {spelled} must be at least 1"
                    )));
                }
                Ok(*length)
            }
            _ => Err(invalid_parameter("invalid type modifier")),
        }
    }
}

/// `typeStringToTypeName`'s refusal of a string that holds no type name at all.
fn invalid_type_name(written: &str) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42601",
        message: format!("invalid type name \"{written}\""),
    }
}

/// `typenameTypeMod`'s refusal of a modifier on a type with no `typmodin`.
fn modifier_not_allowed(name: &str) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42601",
        message: format!("type modifier is not allowed for type \"{name}\""),
    }
}

/// `typenameTypeMod`'s refusal of a modifier that is not a plain constant.
fn type_modifier_not_constant() -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42601",
        message: "type modifiers must be simple constants or identifiers".into(),
    }
}

/// 22023 `invalid_parameter_value`, which every `typmodin` raises and none of
/// them softens — so a bad modifier escapes `to_regtype` rather than turning
/// into NULL.
fn invalid_parameter(message: impl Into<String>) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "22023",
        message: message.into(),
    }
}

// ------------------------------------------------- regconfig/regdictionary ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TextSearch {
    Config,
    Dictionary,
}

impl TextSearch {
    const fn kind(self) -> crabka_pgparser::ast::TextSearchObjectKind {
        match self {
            Self::Config => crabka_pgparser::ast::TextSearchObjectKind::Configuration,
            Self::Dictionary => crabka_pgparser::ast::TextSearchObjectKind::Dictionary,
        }
    }

    const fn noun(self) -> &'static str {
        match self {
            Self::Config => "text search configuration",
            Self::Dictionary => "text search dictionary",
        }
    }
}

/// `regconfigin` / `regdictionaryin`. crabka declares every text-search object
/// in `pg_catalog`, so a qualifier other than that one never matches.
fn resolve_text_search(kv: &dyn Kv, written: &str, what: TextSearch) -> Result<i32, ExecError> {
    let (schema, name, dotted) = qualified(written)?;
    let found = schema
        .as_deref()
        .is_none_or(|schema| schema == "pg_catalog")
        && crate::text_search_catalog::catalog_rows(kv, what.kind())?
            .iter()
            .any(|(candidate, _)| *candidate == name);
    if found {
        Ok(crate::text_search_catalog::object_oid(&name))
    } else {
        Err(ExecError::UndefinedObject(format!(
            "{} \"{dotted}\" does not exist",
            what.noun()
        )))
    }
}

fn text_search_name(kv: &dyn Kv, oid: i32, what: TextSearch) -> Result<Option<String>, ExecError> {
    Ok(crate::text_search_catalog::catalog_rows(kv, what.kind())?
        .into_iter()
        .find(|(name, _)| crate::text_search_catalog::object_oid(name) == oid)
        .map(|(name, _)| quote(name)))
}

// ------------------------------------------------------------ regnamespace ---

/// `regnamespacein`: one name part, matched against `pg_namespace`.
fn resolve_namespace(kv: &dyn Kv, written: &str) -> Result<i32, ExecError> {
    let name = single_part(written)?;
    namespace_oid(kv, &name)?.ok_or(ExecError::Catalog(
        crabka_pgcatalog::CatalogError::UndefinedSchema(name),
    ))
}

fn namespace_oid(kv: &dyn Kv, name: &str) -> Result<Option<i32>, ExecError> {
    Ok(crate::exec::pg_namespace_rows(kv)?
        .iter()
        .find(|row| row.get(1) == Some(&Datum::Text(name.to_string())))
        .and_then(|row| match row.first() {
            Some(Datum::Int4(oid)) => Some(*oid),
            _ => None,
        }))
}

fn namespace_name(kv: &dyn Kv, oid: i32) -> Result<Option<String>, ExecError> {
    Ok(crate::exec::pg_namespace_rows(kv)?
        .iter()
        .find(|row| row.first() == Some(&Datum::Int4(oid)))
        .and_then(|row| match row.get(1) {
            Some(Datum::Text(name)) => Some(name.clone()),
            _ => None,
        }))
}

// ------------------------------------------------------------------ regrole ---

/// `regrolein`: one name part, matched against `pg_authid`.
fn resolve_role(kv: &dyn Kv, written: &str) -> Result<i32, ExecError> {
    let name = single_part(written)?;
    crate::catalog_rel::role_oids(kv)?
        .get(&name)
        .copied()
        .ok_or_else(|| ExecError::UndefinedObject(format!("role \"{name}\" does not exist")))
}

fn role_name(kv: &dyn Kv, oid: i32) -> Result<Option<String>, ExecError> {
    Ok(crate::catalog_rel::role_oids(kv)?
        .into_iter()
        .find(|(_, candidate)| *candidate == oid)
        .map(|(name, _)| name))
}

// ------------------------------------------------------------ regcollation ---

/// `regcollationin`. The "for encoding" clause is part of the message rather
/// than a lookup key here, because crabka is UTF-8 only; PostgreSQL's own test
/// suppresses the message for exactly that reason.
fn resolve_collation(written: &str) -> Result<i32, ExecError> {
    let (schema, name, dotted) = qualified(written)?;
    if schema
        .as_deref()
        .is_none_or(|schema| schema == "pg_catalog")
        && let Some((oid, ..)) = crate::catalog_rel::BUILTIN_COLLATIONS
            .iter()
            .find(|(_, candidate, ..)| *candidate == name)
    {
        return Ok(*oid);
    }
    Err(ExecError::UndefinedObject(format!(
        "collation \"{dotted}\" for encoding \"UTF8\" does not exist"
    )))
}

fn collation_name(oid: i32) -> Option<String> {
    crate::catalog_rel::BUILTIN_COLLATIONS
        .iter()
        .find(|(candidate, ..)| *candidate == oid)
        .map(|(_, name, ..)| quote((*name).to_string()))
}

// ------------------------------------------------------------- the functions ---

/// The `to_reg*` half of the family, plus the type-as-function spelling
/// (`regclass('pg_class')`) `regproc.sql` uses throughout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegFunc {
    /// `regfoo(x)` — the cast, written as a function call.
    Cast(RegKind),
    /// `to_regfoo(x)` — the same resolution, with a soft error answering NULL.
    Soft(RegKind),
    /// `to_regtypemod(x)` — the *other* half of the same type-name parse.
    /// `to_regtype` keeps the oid and throws the modifier away; this keeps the
    /// modifier and throws the oid away, which is why the two are always
    /// written as a pair.
    TypeMod,
}

/// Classify a (lowercased) function name. Only the nine `to_reg*` functions
/// PostgreSQL actually declares are here: there is no `to_regconfig` or
/// `to_regdictionary`, so those two names stay 42883.
fn reg_func(name: &str) -> Option<RegFunc> {
    if name == "to_regtypemod" {
        return Some(RegFunc::TypeMod);
    }
    let Some(bare) = name.strip_prefix("to_") else {
        return reg_kind_named(name).map(RegFunc::Cast);
    };
    match reg_kind_named(bare)? {
        RegKind::Config | RegKind::Dictionary => None,
        kind => Some(RegFunc::Soft(kind)),
    }
}

fn reg_kind_named(name: &str) -> Option<RegKind> {
    ColumnType::from_sql_name(name).and_then(RegKind::of)
}

/// Is `name` one of this family's functions? The dispatch point in
/// [`eval`](crate::eval).
pub(crate) fn is_reg_func(name: &str) -> bool {
    reg_func(&name.to_ascii_lowercase()).is_some()
}

/// Statically infer a call's result type, validating arity and — for the
/// `to_reg*` half — the argument type.
///
/// # Errors
///
/// 42883 for an unknown name, a bad arity, or a `to_reg*` argument that is not
/// a string.
pub(crate) fn reg_func_result_type(fc: &FuncCall, scope: &Scope) -> Result<ColumnType, ExecError> {
    let f = reg_func(&fc.name.to_ascii_lowercase())
        .ok_or_else(|| crate::func::undefined_function(&fc.name))?;
    let args = crate::func::checked_args(fc)?;
    if args.len() != 1 {
        return Err(crate::func::undefined_function_spelled(
            &fc.name, args, scope,
        ));
    }
    let accepted = crate::func::is_unknown_arg(&args[0]) || {
        let from = crate::eval::infer_type(&args[0], scope)?;
        match f {
            RegFunc::Cast(kind) => type_as_function_accepts(from, kind),
            // `to_reg*` is a plain function declared over `text`. PostgreSQL has
            // no implicit anything-to-text, so an integer argument is 42883
            // rather than a silent conversion.
            RegFunc::Soft(_) | RegFunc::TypeMod => from.is_string(),
        }
    };
    if !accepted {
        return Err(crate::func::undefined_function_spelled(
            &fc.name, args, scope,
        ));
    }
    Ok(match f {
        RegFunc::Cast(kind) | RegFunc::Soft(kind) => kind.column_type(),
        // `prorettype => 'int4'`, not an oid: a typmod is a plain integer, and
        // `format_type`'s second parameter is what consumes it.
        RegFunc::TypeMod => ColumnType::Int4,
    })
}

/// Which argument types the type-as-function spelling accepts.
///
/// `ParseFuncOrColumn` reads `regfoo(x)` as a coercion only when `x` is an
/// unknown literal, is already the target type, or reaches it by a
/// `COERCION_PATH_RELABELTYPE` — a *binary* `pg_cast` entry — plus the I/O
/// conversion every string type has. That is narrower than the cast operator:
/// `pg_cast` reaches `regclass` from `int2` and `int8` through conversion
/// **functions**, so `1::int8::regclass` is fine while `regclass(1::int8)` is
/// `function regclass(bigint) does not exist`. Verified against 18.4.
fn type_as_function_accepts(from: ColumnType, kind: RegKind) -> bool {
    if from.is_string() || matches!(from, ColumnType::Int4 | ColumnType::Oid) {
        return true;
    }
    let Some(source) = RegKind::of(from) else {
        return false;
    };
    // The only binary `reg* → reg*` entries in `pg_cast` are the two pairs that
    // describe the same object with and without its argument types.
    source == kind
        || matches!(
            (source, kind),
            (RegKind::Proc, RegKind::Procedure)
                | (RegKind::Procedure, RegKind::Proc)
                | (RegKind::Oper, RegKind::Operator)
                | (RegKind::Operator, RegKind::Oper)
        )
}

/// Evaluate a call.
///
/// # Errors
///
/// 42883 for an unknown name or bad arity, and whatever the named type's input
/// function raises — except through `to_reg*`, which answers NULL for every
/// error PostgreSQL reports softly.
pub(crate) fn eval_reg_func(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let f = reg_func(&fc.name.to_ascii_lowercase())
        .ok_or_else(|| crate::func::undefined_function(&fc.name))?;
    let args = crate::func::checked_args(fc)?;
    require_arity(fc, args.len() == 1)?;
    let value = eval_child(&args[0])?;
    if value == Datum::Null {
        return Ok(Datum::Null);
    }
    match f {
        RegFunc::Cast(kind) => cast_datum(kind, &value, ctx),
        RegFunc::Soft(kind) => match cast_datum(kind, &value, ctx) {
            Ok(datum) => Ok(datum),
            Err(error) if soft(&error) => Ok(Datum::Null),
            Err(error) => Err(error),
        },
        RegFunc::TypeMod => match type_modifier_of(&value, ctx) {
            Ok(typmod) => Ok(typmod),
            Err(error) if soft(&error) => Ok(Datum::Null),
            Err(error) => Err(error),
        },
    }
}

/// `to_regtypemod`: the typmod half of the type-name parse.
///
/// Unlike `to_regtype` this never goes through `regtypein`, so it has no
/// numeric-oid or `-` shortcut: `to_regtypemod('23')` parses `23` as a type
/// *name* and finds nothing. Without a catalog to resolve against there is no
/// answer at all, which is the same NULL a missing type gives.
fn type_modifier_of(value: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    let (Some(kv), Datum::Text(written)) = (ctx.catalog(), value) else {
        return Ok(Datum::Null);
    };
    let (_, typmod) = parse_type_string(kv, ctx.resolution(), written)?;
    Ok(Datum::Int4(typmod))
}

/// One `x::regfoo`, catalog-resolved when a catalog is in scope and through the
/// pure cast otherwise — the same two-step the `Cast` expression takes, so the
/// function spelling and the operator spelling cannot diverge.
fn cast_datum(kind: RegKind, value: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    if let Some(resolved) = reg_cast(kind, value, ctx)? {
        return Ok(resolved);
    }
    Ok(crabka_pgtypes::cast::cast_in(
        value,
        kind.column_type(),
        ctx.output_style(),
    )?)
}
