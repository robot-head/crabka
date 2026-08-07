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
            None => resolve(kind, text.trim(), kv)?,
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
) -> Result<RegclassValue, ExecError> {
    if kind == RegKind::Class {
        return crate::exec::regclass_by_oid(kv, oid);
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
fn resolve(kind: RegKind, written: &str, kv: &dyn Kv) -> Result<i32, ExecError> {
    match kind {
        RegKind::Type => crate::exec::resolve_type_name(kv, written),
        RegKind::Proc => resolve_proc(kv, written),
        RegKind::Procedure => resolve_procedure(kv, written),
        RegKind::Oper => resolve_oper(written),
        RegKind::Operator => resolve_operator(kv, written),
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
    Ok(crate::routine::pg_proc_rows(kv)?
        .iter()
        .filter(|row| row.get(1) == Some(&Datum::Text(name.to_string())))
        .filter(|row| wanted.is_none_or(|oid| row.get(2) == Some(&Datum::Int4(oid))))
        .filter_map(|row| match row.first() {
            Some(Datum::Int4(oid)) => Some(*oid),
            _ => None,
        })
        .collect())
}

/// `regprocout`: the bare function name when that name would find this row and
/// only this row, and the schema-qualified name when it would not — which is
/// what makes an overloaded `abs` print as `pg_catalog.abs`.
fn proc_name(kv: &dyn Kv, oid: i32) -> Result<Option<String>, ExecError> {
    let rows = crate::routine::pg_proc_rows(kv)?;
    let Some(row) = rows
        .iter()
        .find(|row| row.first() == Some(&Datum::Int4(oid)))
    else {
        return Ok(None);
    };
    let Some(Datum::Text(name)) = row.get(1) else {
        return Ok(None);
    };
    let unique = rows
        .iter()
        .filter(|other| other.get(1) == Some(&Datum::Text(name.clone())))
        .count()
        == 1;
    if unique {
        return Ok(Some(quote(name.clone())));
    }
    let schema = match row.get(2) {
        Some(Datum::Int4(namespace)) => namespace_name(kv, *namespace)?,
        _ => None,
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
fn resolve_procedure(kv: &dyn Kv, written: &str) -> Result<i32, ExecError> {
    let (name, args) = split_name_and_arg_types(kv, written, false)?;
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
    let rows = crate::routine::pg_proc_rows(kv)?;
    let Some(row) = rows
        .iter()
        .find(|row| row.first() == Some(&Datum::Int4(oid)))
    else {
        return Ok(None);
    };
    let (Some(Datum::Text(name)), Some(Datum::OidVector(args))) = (row.get(1), row.get(19)) else {
        return Ok(None);
    };
    let rendered = args
        .elems
        .iter()
        .filter_map(|arg| match arg {
            Datum::Int4(oid) => Some(crate::exec::regtype_name(*oid)),
            _ => None,
        })
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
fn resolve_operator(kv: &dyn Kv, written: &str) -> Result<i32, ExecError> {
    let (name, args) = split_name_and_arg_types(kv, written, true)?;
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
            args.push(crate::exec::resolve_type_name(kv, arg)?);
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
}

/// Classify a (lowercased) function name. Only the nine `to_reg*` functions
/// PostgreSQL actually declares are here: there is no `to_regconfig` or
/// `to_regdictionary`, so those two names stay 42883.
fn reg_func(name: &str) -> Option<RegFunc> {
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
            RegFunc::Soft(_) => from.is_string(),
        }
    };
    if !accepted {
        return Err(crate::func::undefined_function_spelled(
            &fc.name, args, scope,
        ));
    }
    Ok(match f {
        RegFunc::Cast(kind) | RegFunc::Soft(kind) => kind.column_type(),
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
    }
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
