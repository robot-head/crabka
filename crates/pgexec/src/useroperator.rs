//! User-defined operators: `CREATE OPERATOR` and `DROP OPERATOR`.
//!
//! An operator is a catalog row and nothing else. It names a function, its
//! operand types and its result type, and it carries the commutator and negator
//! links a planner rewrites a qualification with. Everything here is about
//! keeping those links honest.
//!
//! # The links are a mutual invariant, not a field
//!
//! `PostgreSQL`'s `OperatorCreate` calls `OperatorUpd`, which writes each link
//! in *both* directions: defining `!==` with `NEGATOR = ===` also sets
//! `===`.oprnegate to the new operator. Dropping `!==` must therefore clear
//! that back-reference, or `pg_operator` keeps an oid that resolves to nothing.
//! The `drop_operator` regression file exists to catch exactly that, with two
//! anti-join queries over `oprcom` and `oprnegate`. So [`create`] writes both
//! directions and [`drop_operators`] clears every surviving reference to the
//! oid it removes.
//!
//! # A defined operator is not yet a usable one
//!
//! `CREATE OPERATOR === (…)` records the operator. It does **not** make
//! `a === b` evaluate. An expression's operator is resolved in the parser, into
//! a closed `BinaryOp` enum, so a catalog row is not reachable from a query and
//! `SELECT a === b` stays a syntax error. User operators can only become usable
//! when expression operator resolution moves from the parser to the catalog,
//! which is a change of a different size than this module.
//!
//! # Deliberate divergences
//!
//! * **No shell operators.** `PostgreSQL` invents a placeholder row with
//!   `oprcode = 0` when a `COMMUTATOR` or `NEGATOR` names an operator that does
//!   not exist yet, and fills it in when that operator is defined. This module
//!   refuses instead. A shell is a `pg_operator` row whose implementation
//!   function does not exist, and the consistency this module keeps would then
//!   have to tolerate one.
//! * **A built-in cannot take a back-link.** The built-in half of
//!   `pg_operator` is a static fixture, so a user operator may point *at* a
//!   built-in but cannot write the reverse link into it. A built-in whose link
//!   column is free is refused rather than left asymmetric.
//! * **No dependency tracking.** Nothing records that an object depends on an
//!   operator, so `CASCADE` and `RESTRICT` have nothing to act on.
//! * **Estimators are resolved by name only.** `RESTRICT` and `JOIN` name
//!   functions whose declared arguments include `internal`, which no statement
//!   can produce; the whole-signature check `PostgreSQL` makes is therefore not
//!   reproduced.

use crabka_pgcatalog::UserOperator;
use crabka_pgkv::{Kv, WriteOp};
use crabka_pgparser::ast::{
    CreateOperatorStmt, OperatorName, OperatorSignature, RelationRef, RoutineType,
};
use crabka_pgtypes::Datum;
use crabka_pgwire::engine::QueryResult;

use crate::{error::ExecError, relname::ResolutionScope};

/// The attribute spellings `DefineOperator` still reads, and reads as `MERGES`.
///
/// They named the support operators a merge join once needed. `DefineOperator`
/// dropped the operators and kept the spellings, so writing one is not an
/// unrecognized attribute and earns no warning.
const OBSOLETE_MERGE_ATTRIBUTES: [&str; 4] = ["sort1", "sort2", "ltcmp", "gtcmp"];

/// `pg_type.oid` of `boolean`. Only a boolean-valued operator may carry a
/// negator, a selectivity estimator, or a merge/hash flag.
const BOOL_TYPE_OID: u32 = 16;

/// 42P13, which `DefineOperator` and `OperatorCreate` raise for every way a
/// definition can be incomplete or self-contradictory.
fn invalid_definition(message: impl Into<String>) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42P13",
        message: message.into(),
    }
}

/// 42883, without the `HINT` a failed expression operator resolution adds: a
/// `DROP` that names an operator which is not there is not a call a cast could
/// rescue.
fn undefined_operator(message: impl Into<String>) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42883",
        message: message.into(),
    }
}

// -------------------------------------------------------------- resolution

/// One resolved operand type: its oid, and the name `PostgreSQL` renders it
/// with. A written `int4` is reported back as `integer`, so the written
/// spelling cannot be kept.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Operand {
    oid: u32,
    name: String,
}

/// The operand a written type denotes. `None` in and `None` out is the operand
/// a prefix operator does not have.
fn resolve_operand(kv: &dyn Kv, ty: Option<&RoutineType>) -> Result<Option<Operand>, ExecError> {
    let Some(ty) = ty else {
        return Ok(None);
    };
    let resolved = crate::routine::resolve_routine_type(kv, ty, false)?;
    let oid = resolved.column.map_or_else(
        || {
            crate::routine::TYPE_OIDS
                .iter()
                .find(|(name, _)| *name == resolved.name)
                .map_or(0, |(_, oid)| *oid)
        },
        |column| i32::try_from(column.oid()).unwrap_or_default(),
    );
    if oid <= 0 {
        return Err(ExecError::Unsupported(format!(
            "type {} has no oid this catalog can record in pg_operator",
            resolved.name
        )));
    }
    Ok(Some(Operand {
        oid: oid.unsigned_abs(),
        name: resolved.name,
    }))
}

fn operand_oid(operand: Option<&Operand>) -> u32 {
    operand.map_or(0, |operand| operand.oid)
}

/// `format_operator`'s rendering — `=(integer,integer)`, with `NONE` in the
/// operand a prefix operator does not have. Used where `PostgreSQL` names an
/// operator as an object.
fn format_operator(symbol: &str, left: Option<&Operand>, right: Option<&Operand>) -> String {
    let side = |operand: Option<&Operand>| {
        operand.map_or_else(|| "NONE".to_string(), |operand| operand.name.clone())
    };
    format!("{symbol}({},{})", side(left), side(right))
}

/// `op_signature_string`'s rendering — `integer === integer`, with the left
/// operand simply absent for a prefix operator. Used where `PostgreSQL` names
/// an operator as a call.
fn signature_string(symbol: &str, left: Option<&Operand>, right: Option<&Operand>) -> String {
    let right = right.map_or_else(String::new, |operand| operand.name.clone());
    match left {
        Some(left) => format!("{} {symbol} {right}", left.name),
        None => format!("{symbol} {right}"),
    }
}

/// An operator a written name resolved to, from whichever half of
/// `pg_operator` holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Resolved {
    oid: u32,
    symbol: String,
    /// The stored row, absent for a built-in — whose row lives in a static
    /// fixture that no statement can write to.
    stored: Option<UserOperator>,
    commutator_oid: u32,
    negator_oid: u32,
}

impl Resolved {
    /// The link this operator already carries in `kind`'s column.
    fn link(&self, kind: Link) -> u32 {
        match kind {
            Link::Commutator => self.commutator_oid,
            Link::Negator => self.negator_oid,
        }
    }
}

/// The operator `name` denotes over exactly these operand types, or `None`.
///
/// This is `OpernameGetOprid`: a qualified name is looked for in the schema it
/// wrote, and a bare one in search-path order — so a user operator in `public`
/// cannot hide a built-in that the implicit `pg_catalog` entry finds first.
fn find_operator(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    name: &OperatorName,
    left: u32,
    right: u32,
) -> Result<Option<Resolved>, ExecError> {
    for schema in lookup_schemas(kv, scope, name.schema.as_deref())? {
        if schema == crate::search_path::PG_CATALOG {
            if let Some(found) = builtin_operator(&name.symbol, left, right) {
                return Ok(Some(found));
            }
            continue;
        }
        let stored =
            crabka_pgcatalog::get_user_operator(kv, &schema, &name.symbol, left, right)?;
        if let Some(stored) = stored {
            return Ok(Some(Resolved {
                oid: stored.oid,
                symbol: stored.symbol.clone(),
                commutator_oid: stored.commutator_oid,
                negator_oid: stored.negator_oid,
                stored: Some(stored),
            }));
        }
    }
    Ok(None)
}

/// The schemas a written name is looked for in: the one it qualified itself
/// with, or the whole search path.
fn lookup_schemas(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    written: Option<&str>,
) -> Result<Vec<String>, ExecError> {
    match written {
        Some(schema) => Ok(vec![schema.to_string()]),
        None => scope.visible_schemas(kv),
    }
}

/// The pinned built-in operator of this signature, read out of the generated
/// catalog fixture.
fn builtin_operator(symbol: &str, left: u32, right: u32) -> Option<Resolved> {
    let oid = |value: i32| value.unsigned_abs();
    crate::builtin_operators::BUILTIN_OPERATORS
        .iter()
        .find(|row| row.1 == symbol && oid(row.5) == left && oid(row.6) == right)
        .map(|row| Resolved {
            oid: oid(row.0),
            symbol: row.1.to_string(),
            stored: None,
            commutator_oid: oid(row.8),
            negator_oid: oid(row.9),
        })
}

/// The symbol the operator `oid` carries, for the error that names the third
/// operator an existing link already points at. `None` for an oid that resolves
/// to nothing, which `get_opname` also reports by falling back to the number.
fn symbol_of_oid(kv: &dyn Kv, oid: u32) -> Option<String> {
    if let Some(row) = crate::builtin_operators::BUILTIN_OPERATORS
        .iter()
        .find(|row| row.0.unsigned_abs() == oid)
    {
        return Some(row.1.to_string());
    }
    crabka_pgcatalog::list_user_operators(kv)
        .ok()?
        .into_iter()
        .find(|operator| operator.oid == oid)
        .map(|operator| operator.symbol)
}

/// The function `name` names over exactly `args`, as `LookupFuncName` finds it:
/// the declared argument types must match, with no coercion of any kind.
///
/// The search runs over the projected `pg_proc` — built-in rows and user rows
/// alike — because the oid stored in `oprcode` has to be the oid that
/// `pg_operator JOIN pg_proc` resolves. Reading it from anywhere else would let
/// the two disagree.
fn lookup_function(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    name: &RelationRef,
    args: &[&Operand],
) -> Result<(u32, u32), ExecError> {
    let wanted: Vec<i32> = args
        .iter()
        .map(|operand| i32::try_from(operand.oid).unwrap_or_default())
        .collect();
    let rows = crate::routine::pg_proc_rows(kv)?;
    for schema in lookup_schemas(kv, scope, name.schema.as_deref())? {
        let namespace = crate::catalog_rel::namespace_oid(&schema);
        let found = rows.iter().find(|row| {
            row.get(1) == Some(&Datum::Text(name.name.clone()))
                && row.get(2) == Some(&Datum::Int4(namespace))
                && argument_oids(row).as_deref() == Some(&wanted[..])
        });
        if let (Some(Datum::Int4(oid)), Some(Datum::Int4(result))) =
            (found.and_then(|row| row.first()), found.and_then(|row| row.get(18)))
        {
            return Ok((oid.unsigned_abs(), result.unsigned_abs()));
        }
    }
    Err(undefined_operator(format!(
        "function {}({}) does not exist",
        name.name,
        args.iter()
            .map(|operand| operand.name.clone())
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

/// `pg_proc.proargtypes` of one projected row.
fn argument_oids(row: &[Datum]) -> Option<Vec<i32>> {
    let Some(Datum::OidVector(vector)) = row.get(19) else {
        return None;
    };
    vector
        .elems
        .iter()
        .map(|elem| match elem {
            Datum::Int4(oid) => Some(*oid),
            _ => None,
        })
        .collect()
}

// ------------------------------------------------------------------ CREATE

/// The `operator attribute "…" not recognized` warnings a definition earns, in
/// the order it wrote them.
///
/// Separate from [`create`] because `DefineOperator` emits every one of these
/// *before* it reports the missing function, and a `Result` cannot carry both a
/// refusal and the diagnostics that precede it. The caller emits these, then
/// calls [`create`].
pub(crate) fn unrecognized_attribute_warnings(stmt: &CreateOperatorStmt) -> Vec<String> {
    stmt.unrecognized_options
        .iter()
        .filter(|option| !OBSOLETE_MERGE_ATTRIBUTES.contains(&option.as_str()))
        .map(|option| format!("operator attribute \"{option}\" not recognized"))
        .collect()
}

/// `CREATE OPERATOR [schema.]symbol (…)`.
///
/// The checks run in `DefineOperator`'s order, because that order is what the
/// corpus pins: a definition missing both its function and its operands reports
/// the function.
///
/// # Errors
///
/// Propagates catalog read errors, and `PostgreSQL`'s definition-time refusals:
/// 42P13 for an incomplete or self-contradictory definition, 42883 for a
/// function that does not exist, and 42723 for a signature that is already
/// defined.
pub(crate) fn create(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    stmt: &CreateOperatorStmt,
    owner: &str,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let can_merge = stmt.merges
        || stmt
            .unrecognized_options
            .iter()
            .any(|option| OBSOLETE_MERGE_ATTRIBUTES.contains(&option.as_str()));
    let Some(function) = &stmt.function else {
        return Err(invalid_definition("operator function must be specified"));
    };
    let left = resolve_operand(kv, stmt.left_type.as_ref())?;
    let right = resolve_operand(kv, stmt.right_type.as_ref())?;
    if left.is_none() && right.is_none() {
        return Err(invalid_definition(
            "operator argument types must be specified",
        ));
    }
    if right.is_none() {
        return Err(invalid_definition(
            "operator right argument type must be specified\nDETAIL:  Postfix operators are not \
             supported.",
        ));
    }
    let arguments: Vec<&Operand> = left.iter().chain(right.iter()).collect();
    let (code_oid, result_type_oid) = lookup_function(kv, scope, function, &arguments)?;
    validate_parameters(stmt, left.is_some(), result_type_oid, can_merge)?;

    let schema = creation_schema(kv, scope, stmt.name.schema.as_deref())?;
    let left_type_oid = operand_oid(left.as_ref());
    let right_type_oid = operand_oid(right.as_ref());
    if crabka_pgcatalog::get_user_operator(
        kv,
        &schema,
        &stmt.name.symbol,
        left_type_oid,
        right_type_oid,
    )?
    .is_some()
    {
        return Err(ExecError::FunctionError {
            sqlstate: "42723",
            message: format!("operator {} already exists", stmt.name.symbol),
        });
    }

    let (oid, cursor) = crabka_pgcatalog::allocate_user_operator_oid(kv)?;
    let mine = Identity {
        oid,
        schema: &schema,
        symbol: &stmt.name.symbol,
        left_type_oid,
        right_type_oid,
    };
    let commutator = resolve_link(kv, scope, stmt.commutator.as_ref(), &mine, Link::Commutator)?;
    let negator = resolve_link(kv, scope, stmt.negator.as_ref(), &mine, Link::Negator)?;
    if negator.as_ref().is_some_and(|target| target.oid == oid) {
        return Err(invalid_definition("operator cannot be its own negator"));
    }

    let operator = UserOperator {
        oid,
        schema,
        symbol: stmt.name.symbol.clone(),
        owner: owner.to_string(),
        kind: if left.is_some() { 'b' } else { 'l' },
        left_type_oid,
        right_type_oid,
        result_type_oid,
        code_oid,
        commutator_oid: commutator.as_ref().map_or(0, |target| target.oid),
        negator_oid: negator.as_ref().map_or(0, |target| target.oid),
        restrict_oid: estimator(kv, scope, stmt.restrict.as_ref())?,
        join_oid: estimator(kv, scope, stmt.join.as_ref())?,
        can_merge,
        can_hash: stmt.hashes,
    };
    let mut ops = vec![cursor];
    ops.extend(crabka_pgcatalog::put_user_operator_ops(&operator));
    ops.extend(back_links(kv, oid, &[
        (Link::Commutator, commutator),
        (Link::Negator, negator),
    ])?);
    Ok((
        QueryResult::Command {
            tag: "CREATE OPERATOR".into(),
        },
        ops,
    ))
}

/// The namespace a `CREATE` puts its operator in: the one it qualified itself
/// with, or the first existing search-path entry.
fn creation_schema(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    written: Option<&str>,
) -> Result<String, ExecError> {
    match written {
        Some(schema) => {
            if crabka_pgcatalog::schema_exists(kv, schema)? {
                Ok(schema.to_string())
            } else {
                Err(crabka_pgcatalog::CatalogError::UndefinedSchema(schema.to_string()).into())
            }
        }
        None => scope
            .creation_schema(kv)?
            .ok_or(ExecError::NoSchemaSelected),
    }
}

/// The operator being defined, before its row exists.
struct Identity<'a> {
    oid: u32,
    schema: &'a str,
    symbol: &'a str,
    left_type_oid: u32,
    right_type_oid: u32,
}

/// Which link is being resolved. The two differ in the operand order their
/// target carries, and in what a self-reference means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Link {
    Commutator,
    Negator,
}

impl Link {
    fn word(self) -> &'static str {
        match self {
            Self::Commutator => "commutator",
            Self::Negator => "negator",
        }
    }
}

/// Resolve a `COMMUTATOR` or `NEGATOR` reference to the operator it links to.
///
/// A reference that names the operator being defined resolves to that
/// operator's own oid — the self-link `PostgreSQL` fills in after the insert.
/// A reference to an operator that does not exist is refused, because this
/// module builds no shells.
fn resolve_link(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    name: Option<&OperatorName>,
    mine: &Identity<'_>,
    kind: Link,
) -> Result<Option<Resolved>, ExecError> {
    let Some(name) = name else {
        return Ok(None);
    };
    // The commutator has reversed operand types; the negator has the same ones.
    let (left, right) = match kind {
        Link::Commutator => (mine.right_type_oid, mine.left_type_oid),
        Link::Negator => (mine.left_type_oid, mine.right_type_oid),
    };
    if let Some(found) = find_operator(kv, scope, name, left, right)? {
        return Ok(Some(found));
    }
    // Not in the catalog. `get_other_operator` calls that self-linkage when the
    // reference would name the operator being defined, and builds a shell
    // otherwise.
    if creation_schema(kv, scope, name.schema.as_deref())? == mine.schema
        && name.symbol == mine.symbol
        && left == mine.left_type_oid
        && right == mine.right_type_oid
    {
        return Ok(Some(Resolved {
            oid: mine.oid,
            symbol: mine.symbol.to_string(),
            stored: None,
            commutator_oid: 0,
            negator_oid: 0,
        }));
    }
    Err(ExecError::Unsupported(format!(
        "{kind} operator {name} does not exist: a {kind} must already exist, because this catalog \
         does not create shell operators",
        kind = kind.word(),
    )))
}

/// `OperatorUpd`'s `!isDelete` half: point the linked operators back at the new
/// one.
///
/// Both links may name the same operator, as `CREATE OPERATOR |> (… NEGATOR =
/// <|, COMMUTATOR = <|)` does. The edits are therefore folded onto one working
/// copy per target and written once: two independent writes to one key would
/// keep only the second, and the pair would come out half-linked.
///
/// A target that already links to a *third* operator is `PostgreSQL`'s error,
/// reproduced here, because rewriting that link would silently break the third
/// operator's own pair.
fn back_links(
    kv: &dyn Kv,
    oid: u32,
    links: &[(Link, Option<Resolved>)],
) -> Result<Vec<WriteOp>, ExecError> {
    let mut edited: Vec<UserOperator> = Vec::new();
    for (kind, target) in links {
        let Some(target) = target else {
            continue;
        };
        // A self-link needs no second write: the row being created already
        // carries its own oid in that column.
        if target.oid == oid {
            continue;
        }
        let held = edited
            .iter()
            .find(|operator| operator.oid == target.oid)
            .map_or_else(|| target.link(*kind), |operator| link_of(operator, *kind));
        if held == oid {
            continue;
        }
        if held != 0 {
            let third = symbol_of_oid(kv, held).unwrap_or_else(|| held.to_string());
            return Err(invalid_definition(format!(
                "{kind} operator {} is already the {kind} of operator {third}",
                target.symbol,
                kind = kind.word(),
            )));
        }
        let Some(stored) = &target.stored else {
            return Err(ExecError::Unsupported(format!(
                "operator {} is a built-in, so the reverse {} link cannot be recorded; \
                 PostgreSQL would write it into the system catalog",
                target.symbol,
                kind.word(),
            )));
        };
        let slot = match edited.iter().position(|operator| operator.oid == target.oid) {
            Some(index) => &mut edited[index],
            None => {
                edited.push(stored.clone());
                edited.last_mut().expect("just pushed")
            }
        };
        *link_of_mut(slot, *kind) = oid;
    }
    Ok(edited
        .iter()
        .flat_map(crabka_pgcatalog::put_user_operator_ops)
        .collect())
}

fn link_of(operator: &UserOperator, kind: Link) -> u32 {
    match kind {
        Link::Commutator => operator.commutator_oid,
        Link::Negator => operator.negator_oid,
    }
}

fn link_of_mut(operator: &mut UserOperator, kind: Link) -> &mut u32 {
    match kind {
        Link::Commutator => &mut operator.commutator_oid,
        Link::Negator => &mut operator.negator_oid,
    }
}

/// `OperatorValidateParams`: the attributes an operator of this shape may not
/// carry.
fn validate_parameters(
    stmt: &CreateOperatorStmt,
    binary: bool,
    result_type_oid: u32,
    can_merge: bool,
) -> Result<(), ExecError> {
    if !binary {
        if stmt.commutator.is_some() {
            return Err(invalid_definition(
                "only binary operators can have commutators",
            ));
        }
        if stmt.join.is_some() {
            return Err(invalid_definition(
                "only binary operators can have join selectivity",
            ));
        }
        if can_merge {
            return Err(invalid_definition("only binary operators can merge join"));
        }
        if stmt.hashes {
            return Err(invalid_definition("only binary operators can hash"));
        }
    }
    if result_type_oid != BOOL_TYPE_OID {
        if stmt.negator.is_some() {
            return Err(invalid_definition("only boolean operators can have negators"));
        }
        if stmt.restrict.is_some() {
            return Err(invalid_definition(
                "only boolean operators can have restriction selectivity",
            ));
        }
        if stmt.join.is_some() {
            return Err(invalid_definition(
                "only boolean operators can have join selectivity",
            ));
        }
        if can_merge {
            return Err(invalid_definition("only boolean operators can merge join"));
        }
        if stmt.hashes {
            return Err(invalid_definition("only boolean operators can hash"));
        }
    }
    Ok(())
}

/// A `RESTRICT` or `JOIN` estimator's oid.
fn estimator(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    name: Option<&RelationRef>,
) -> Result<u32, ExecError> {
    let Some(name) = name else {
        return Ok(0);
    };
    let rows = crate::routine::pg_proc_rows(kv)?;
    for schema in lookup_schemas(kv, scope, name.schema.as_deref())? {
        let namespace = crate::catalog_rel::namespace_oid(&schema);
        let found = rows.iter().find(|row| {
            row.get(1) == Some(&Datum::Text(name.name.clone()))
                && row.get(2) == Some(&Datum::Int4(namespace))
        });
        if let Some(Datum::Int4(oid)) = found.and_then(|row| row.first()) {
            return Ok(oid.unsigned_abs());
        }
    }
    Err(undefined_operator(format!(
        "function {} does not exist",
        name.name
    )))
}

// -------------------------------------------------------------------- DROP

/// What one `DROP OPERATOR` produced.
///
/// The `skipping` notices travel back to the caller rather than out, because
/// the session owns the notice queue and this module cannot reach it.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct DropOutcome {
    pub(crate) ops: Vec<WriteOp>,
    pub(crate) notices: Vec<String>,
}

/// `DROP OPERATOR [IF EXISTS] signature [, …]`.
///
/// Every reference to a removed operator is cleared, in both link columns and
/// on every surviving operator — not only on the two rows the dropped operator
/// happened to point at. `PostgreSQL` clears only those two and reaches the
/// same state because the links are mutual; the scan is what makes the result
/// hold even where they once were not.
///
/// # Errors
///
/// Propagates catalog read errors; 42601 for the postfix spelling
/// `PostgreSQL` 14 removed; 2BP01 for a built-in, which is pinned; and 42883
/// for an operator that does not exist and was not named with `IF EXISTS`.
pub(crate) fn drop_operators(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    if_exists: bool,
    operators: &[OperatorSignature],
    cascade: bool,
) -> Result<(QueryResult, DropOutcome), ExecError> {
    // Nothing in this catalog records a dependency on an operator, so CASCADE
    // and RESTRICT have nothing to act on -- the same position DROP AGGREGATE
    // is in.
    let _ = cascade;
    // The user half is loaded once and edited in memory, so that dropping two
    // operators that link to each other in one statement clears both links. A
    // per-signature re-read would see the catalog as it was before the
    // statement started, and would put the stale row back.
    let mut surviving = crabka_pgcatalog::list_user_operators(kv)?;
    let mut outcome = DropOutcome::default();
    let mut dropped: Vec<u32> = Vec::new();
    for signature in operators {
        let left = resolve_operand(kv, signature.left_type.as_ref())?;
        let right = resolve_operand(kv, signature.right_type.as_ref())?;
        if right.is_none() {
            return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                "42601",
                "postfix operators are not supported",
            )));
        }
        let found = find_operator(
            kv,
            scope,
            &signature.name,
            operand_oid(left.as_ref()),
            operand_oid(right.as_ref()),
        )?;
        let Some(found) = found else {
            if if_exists {
                outcome.notices.push(format!(
                    "operator {} does not exist, skipping",
                    signature.name
                ));
                continue;
            }
            return Err(undefined_operator(format!(
                "operator does not exist: {}",
                signature_string(&signature.name.symbol, left.as_ref(), right.as_ref())
            )));
        };
        let Some(stored) = found.stored else {
            return Err(ExecError::DependentObjectsStillExist(format!(
                "cannot drop operator {} because it is required by the database system",
                format_operator(&signature.name.symbol, left.as_ref(), right.as_ref())
            )));
        };
        outcome
            .ops
            .extend(crabka_pgcatalog::drop_user_operator_ops(&stored));
        surviving.retain(|operator| operator.oid != stored.oid);
        dropped.push(stored.oid);
    }
    // `OperatorUpd(…, isDelete = true)`: no surviving row may name an oid that
    // pg_operator no longer holds.
    for operator in &mut surviving {
        let stale = dropped.contains(&operator.commutator_oid)
            || dropped.contains(&operator.negator_oid);
        if !stale {
            continue;
        }
        if dropped.contains(&operator.commutator_oid) {
            operator.commutator_oid = 0;
        }
        if dropped.contains(&operator.negator_oid) {
            operator.negator_oid = 0;
        }
        outcome
            .ops
            .extend(crabka_pgcatalog::put_user_operator_ops(operator));
    }
    Ok((
        QueryResult::Command {
            tag: "DROP OPERATOR".into(),
        },
        outcome,
    ))
}

// ----------------------------------------------------------------- catalog

/// The `pg_operator` rows for the operators this database defines.
///
/// # Errors
///
/// Propagates catalog read errors.
pub(crate) fn pg_operator_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let owners = crate::catalog_rel::role_oids(kv)?;
    Ok(crabka_pgcatalog::list_user_operators(kv)?
        .into_iter()
        .map(|operator| {
            let oid = |value: u32| Datum::Int4(i32::try_from(value).unwrap_or_default());
            vec![
                oid(operator.oid),
                Datum::Text(operator.symbol),
                Datum::Int4(crate::catalog_rel::namespace_oid(&operator.schema)),
                Datum::Int4(owners.get(&operator.owner).copied().unwrap_or_default()),
                Datum::Text(operator.kind.to_string()),
                Datum::Bool(operator.can_merge),
                Datum::Bool(operator.can_hash),
                oid(operator.left_type_oid),
                oid(operator.right_type_oid),
                oid(operator.result_type_oid),
                oid(operator.commutator_oid),
                oid(operator.negator_oid),
                oid(operator.code_oid),
                oid(operator.restrict_oid),
                oid(operator.join_oid),
            ]
        })
        .collect())
}
