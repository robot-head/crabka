//! `CREATE CAST` / `DROP CAST`, and the conversion a recorded cast performs.
//!
//! gres decides most cast legality in `crabka_pgtypes::cast`, in hand-written
//! match arms over [`ColumnType`] pairs. Those arms have no catalog handle and
//! cannot, so a cast the *user* declared is resolved a rung up: the executor
//! consults this module first and only falls through to the built-in rules when
//! no user cast covers the pair.
//!
//! `WITHOUT FUNCTION` and `WITH INOUT` are the two the type system can honour
//! truthfully. A binary-coercible pair is two names for one byte image, and
//! gres performs it by re-reading the source's `send` bytes as the target. An
//! I/O conversion is the source's text form parsed as the target, which is what
//! gres would run for the declared `typoutput`/`typinput` pair anyway: a base
//! type's values live in its representation type's `Datum`, and it is that
//! type's I/O that renders and reads them. Where the declared input function
//! would have read a form the representation type does not, the conversion
//! fails loudly rather than producing some other value.
//!
use crabka_pgcatalog::UserCast;
use crabka_pgkv::{Kv, WriteOp};
use crabka_pgparser::ast::{CastContext, CastMethod, Expr, FuncArgs, FuncCall};
use crabka_pgtypes::{ColumnType, Datum, encoding::encode_binary};
use crabka_pgwire::engine::QueryResult;

use crate::{clock::EvalCtx, error::ExecError, scope::Scope};

/// Load every catalog-stored cast into the process registry.
///
/// The same reasoning as [`crate::usertype::hydrate`]: plan-time cast legality
/// is decided by a pure function with no catalog handle, so the durable set has
/// to be published to the process before this session plans anything.
///
/// # Errors
///
/// Propagates catalog read errors.
pub(crate) fn hydrate(kv: &dyn Kv) -> Result<(), ExecError> {
    publish(kv)
}

/// Republish the durable cast set, after DDL that changed it committed.
///
/// # Errors
///
/// Propagates catalog read errors.
pub(crate) fn publish(kv: &dyn Kv) -> Result<(), ExecError> {
    let declared = declared_casts(kv)?;
    crabka_pgtypes::usercast::publish(declared);
    Ok(())
}

/// Apply the current catalog's cast change to the process registry.
///
/// # Errors
///
/// Propagates catalog read errors.
pub(crate) fn publish_delta(
    before: &[crabka_pgcatalog::UserCast],
    kv: &dyn Kv,
) -> Result<(), ExecError> {
    let before = declared_from(before);
    let after = declared_casts(kv)?;
    crabka_pgtypes::usercast::publish_catalog_delta(&before, &after);
    Ok(())
}

fn declared_casts(kv: &dyn Kv) -> Result<Vec<crabka_pgtypes::usercast::DeclaredCast>, ExecError> {
    Ok(declared_from(&crabka_pgcatalog::list_user_casts(kv)?))
}

fn declared_from(
    casts: &[crabka_pgcatalog::UserCast],
) -> Vec<crabka_pgtypes::usercast::DeclaredCast> {
    casts
        .iter()
        .map(|cast| crabka_pgtypes::usercast::DeclaredCast {
            source: cast.source,
            target: cast.target,
            method: match cast.method {
                'i' => crabka_pgtypes::usercast::CastMethod::InOut,
                'f' => crabka_pgtypes::usercast::CastMethod::Function,
                _ => crabka_pgtypes::usercast::CastMethod::Binary,
            },
        })
        .collect()
}

/// The type whose bytes a value of `ty` actually is.
///
/// For everything but a user-defined base type this is `ty` itself. A base
/// type's `LIKE = T` copies `T`'s `typlen`/`typbyval`/`typalign`, and gres holds
/// the value in `T`'s `Datum`, so `T` is the physical type.
#[must_use]
pub(crate) fn physical_type(ty: ColumnType) -> ColumnType {
    match ty {
        ColumnType::Base(base) => *base.representation,
        other => other,
    }
}

/// Perform a binary-coercible conversion: re-read the value's `send` image as
/// the target's physical type.
///
/// When the two physical types agree the value passes through untouched, which
/// is the `xfloat4 → float4` half of a `LIKE`-built pair. When they differ the
/// bytes are reinterpreted, which is the `integer → xfloat4` half and the whole
/// point of the exercise: `1::xfloat4::float4` is the float whose bit pattern
/// is 1, not the float 1.
///
/// # Errors
///
/// 22P03 when the source's image is not a valid image of the target — which a
/// physically compatible pair cannot produce, but a corrupt catalog could.
pub(crate) fn coerce_binary(
    value: &Datum,
    source: ColumnType,
    target: ColumnType,
    time_zone: &jiff::tz::TimeZone,
) -> Result<Datum, ExecError> {
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let from = physical_type(source);
    let to = physical_type(target);
    if from == to {
        return Ok(value.clone());
    }
    let bytes = encode_binary(value);
    crate::session::decode_binary_value(&bytes, to, time_zone).map_err(ExecError::Remote)
}

/// Perform an I/O conversion: render the value in the source's text form and
/// read it back as the target's.
///
/// This is `COERCION_METHOD_INOUT` — `typoutput` then `typinput`. gres runs the
/// *representation* type's pair rather than the routines the type declared,
/// which is the same substitution it makes everywhere a base type's value is
/// rendered, and exact whenever the declared pair is the representation type's
/// (`textin`/`textout` on a varlena base type is the case `create_cast` builds).
/// Where it is not, the parse fails and says so.
///
/// # Errors
///
/// Whatever the target's text input reports for a form it cannot read, and
/// 22021 if the source's text form is not valid UTF-8.
pub(crate) fn coerce_inout(
    value: &Datum,
    target: ColumnType,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let rendered = crabka_pgtypes::encoding::encode_text_in(value, ctx.output_style());
    let rendered = String::from_utf8(rendered).map_err(|_| {
        ExecError::Remote(crabka_pgwire::error::PgError::error(
            "22021",
            "cast source value has no valid text form".to_string(),
        ))
    })?;
    let converted = crate::eval::cast_value_in_at(
        &Datum::Text(rendered),
        physical_type(target),
        ctx.output_style(),
        ctx.now,
    )?;
    // A domain target keeps its constraints: `cast_value_in` converts to the
    // base and stops there, exactly as it does on the built-in cast path.
    crate::usertype::check_domain(target, &converted, ctx)?;
    Ok(converted)
}

/// Apply a user-declared cast of `expr` to `target`, or `None` when the pair
/// carries none and the built-in rules should have it.
///
/// The source type has to be *inferred* rather than read off the value: a base
/// type's `Datum` is its representation type's, so `Datum::Float4` alone cannot
/// say whether it came from a `float4` or from an `xfloat4`. Callers gate this
/// on [`crabka_pgtypes::usercast::any_declared`], so a server with no declared
/// cast never pays the inference.
///
/// # Errors
///
/// Propagates the conversion's own errors.
pub(crate) fn coerce_declared(
    expr: &Expr,
    target: ColumnType,
    value: &Datum,
    scope: &Scope,
    ctx: &EvalCtx,
) -> Result<Option<Datum>, ExecError> {
    // An operand whose type does not infer cannot be the source of a declared
    // cast either; leave the built-in path to produce its own diagnosis.
    let Ok(source) = crate::eval::infer_type(expr, scope) else {
        return Ok(None);
    };
    let Some(method) = crabka_pgtypes::usercast::declared_method(source.oid(), target.oid()) else {
        return Ok(None);
    };
    match method {
        crabka_pgtypes::usercast::CastMethod::Binary => {
            coerce_binary(value, source, target, &ctx.time_zone).map(Some)
        }
        crabka_pgtypes::usercast::CastMethod::InOut => coerce_inout(value, target, ctx).map(Some),
        crabka_pgtypes::usercast::CastMethod::Function => {
            coerce_function(expr, value, source, target, ctx).map(Some)
        }
    }
}

fn coerce_function(
    expr: &Expr,
    value: &Datum,
    source: ColumnType,
    target: ColumnType,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let catalog = crate::routine::scalar_runtime_catalog().ok_or_else(|| {
        ExecError::Unsupported("function-backed casts require a statement runtime".into())
    })?;
    let cast = crabka_pgcatalog::get_user_cast(catalog.as_ref(), source.oid(), target.oid())?
        .ok_or_else(|| ExecError::UndefinedObject("function-backed cast disappeared".into()))?;
    let oid = cast.function.parse::<i32>().map_err(|_| {
        ExecError::InvalidObjectDefinition("function-backed cast has an invalid routine oid".into())
    })?;
    let routine = crate::routine::routine_by_oid(catalog.as_ref(), oid)?.ok_or_else(|| {
        ExecError::UndefinedFunction(format!("cast function with oid {oid} does not exist"))
    })?;
    let call = FuncCall {
        name: routine.name.clone(),
        distinct: false,
        args: FuncArgs::Exprs(vec![expr.clone()]),
        order_by: Vec::new(),
        within_group: false,
        filter: None,
        sql_syntax: false,
    };
    let value = crate::routine::eval_plpgsql_scalar_with(&call, ctx, |_| Ok(value.clone()))
        .ok_or_else(|| {
            ExecError::Unsupported("cast function is not available to the scalar runtime".into())
        })??;
    let returned = crate::routine::declared_scalar_result_type(&routine).ok_or_else(|| {
        ExecError::InvalidObjectDefinition("cast function must return a scalar type".into())
    })?;
    coerce_implicit_result(catalog.as_ref(), value, returned, target, ctx)
}

fn coerce_implicit_result(
    kv: &dyn Kv,
    value: Datum,
    source: ColumnType,
    target: ColumnType,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    if source == target {
        return Ok(value);
    }
    if let Some(cast) = crabka_pgcatalog::get_user_cast(kv, source.oid(), target.oid())?
        && cast.context == 'i'
    {
        return match cast.method {
            'b' => coerce_binary(&value, source, target, &ctx.time_zone),
            'i' => coerce_inout(&value, target, ctx),
            _ => Err(ExecError::Unsupported(
                "an implicit cast function result cannot use another function-backed cast".into(),
            )),
        };
    }
    if crate::routine::implicitly_coercible(source, target) {
        let converted = crate::eval::cast_value_in_at(&value, target, ctx.output_style(), ctx.now)?;
        crate::usertype::check_domain(target, &converted, ctx)?;
        return Ok(converted);
    }
    Err(ExecError::TypeMismatch(format!(
        "cast function result is of type {} but target type is {}",
        source.name(),
        target.name()
    )))
}

/// `CREATE CAST (source AS target) { WITH FUNCTION | WITHOUT FUNCTION | WITH INOUT }`.
///
/// Mirrors `CreateCast`'s branches. `WITHOUT FUNCTION` requires the two types
/// to share a physical representation, and neither may be a container type, an
/// enum or a domain — all of those embed an oid or carry constraints that a
/// pass-through would silently discard; see [`reject_non_coercible`] for where
/// gres's physical test differs from PostgreSQL's. `WITH INOUT` goes through
/// the text form. `WITH FUNCTION` persists the chosen routine oid and invokes
/// it through the statement's scalar-routine runtime.
///
/// # Errors
///
/// 42710 when the pair already has a cast, 42P17 for every physical
/// incompatibility PostgreSQL rejects, plus ordinary function-resolution errors
/// for `WITH FUNCTION`.
pub(crate) fn create_cast(
    kv: &dyn Kv,
    source: ColumnType,
    target: ColumnType,
    method: &CastMethod,
    context: CastContext,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let (recorded, function) = match method {
        CastMethod::WithoutFunction => ('b', String::new()),
        CastMethod::WithInout => ('i', String::new()),
        CastMethod::WithFunction { name, .. } => {
            let routine = resolve_cast_function(kv, name, source, target)?;
            ('f', routine.oid.to_string())
        }
    };
    if source == target {
        return Err(ExecError::InvalidObjectDefinition(
            "source data type and target data type are the same".into(),
        ));
    }
    // `CreateCast` applies the physical checks to `WITHOUT FUNCTION` alone. An
    // I/O conversion goes through the text form and needs no shared layout.
    if recorded == 'b' {
        reject_non_coercible(source, target)?;
    }
    if crabka_pgcatalog::get_user_cast(kv, source.oid(), target.oid())?.is_some() {
        return Err(ExecError::DuplicateObject(format!(
            "cast from type {} to type {} already exists",
            source.name(),
            target.name()
        )));
    }
    let cast = UserCast {
        // Replaced by the durable counter in `create_user_cast_ops`.
        oid: 0,
        source: source.oid(),
        target: target.oid(),
        method: recorded,
        context: match context {
            CastContext::Explicit => 'e',
            CastContext::Assignment => 'a',
            CastContext::Implicit => 'i',
        },
        function,
    };
    Ok((
        QueryResult::Command {
            tag: "CREATE CAST".to_string(),
        },
        crabka_pgcatalog::create_user_cast_ops(kv, &cast)?,
    ))
}

fn resolve_cast_function(
    kv: &dyn Kv,
    name: &str,
    source: ColumnType,
    target: ColumnType,
) -> Result<crabka_pgcatalog::routine::Routine, ExecError> {
    let routine = crate::routine::resolve_call(kv, name, &[crate::eval::ArgType::Known(source)])?
        .ok_or_else(|| {
        ExecError::UndefinedFunction(format!("function {name}({}) does not exist", source.name()))
    })?;
    let params: Vec<_> = routine.input_params().collect();
    if params.len() != 1 || params[0].ty.column != Some(source) {
        return Err(ExecError::InvalidObjectDefinition(format!(
            "cast function {} must take exactly one {} argument",
            routine.identity(),
            source.name()
        )));
    }
    let returned = crate::routine::declared_scalar_result_type(&routine).ok_or_else(|| {
        ExecError::InvalidObjectDefinition(format!(
            "cast function {} must return a scalar type",
            routine.identity()
        ))
    })?;
    if returned != target
        && !crate::routine::implicitly_coercible(returned, target)
        && !crabka_pgcatalog::get_user_cast(kv, returned.oid(), target.oid())?
            .is_some_and(|cast| cast.context == 'i' && cast.method != 'f')
    {
        return Err(ExecError::InvalidObjectDefinition(format!(
            "cast function {} returns {}, not {}",
            routine.identity(),
            returned.name(),
            target.name()
        )));
    }
    Ok(routine)
}

/// `DROP CAST [IF EXISTS] (source AS target)`.
///
/// # Errors
///
/// 42704 when no such cast exists and `IF EXISTS` was not written.
pub(crate) fn drop_cast(
    kv: &dyn Kv,
    source: ColumnType,
    target: ColumnType,
    if_exists: bool,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let tag = QueryResult::Command {
        tag: "DROP CAST".to_string(),
    };
    let Some(cast) = crabka_pgcatalog::get_user_cast(kv, source.oid(), target.oid())? else {
        if if_exists {
            return Ok((tag, Vec::new()));
        }
        return Err(ExecError::UndefinedObject(format!(
            "cast from type {} to type {} does not exist",
            source.name(),
            target.name()
        )));
    };
    let mut ops = crabka_pgcatalog::drop_user_cast_ops(source.oid(), target.oid());
    ops.push(crabka_pgcatalog::set_comment_op(
        "cast",
        crabka_pgcatalog::CommentObject::Named(&cast.oid.to_string()),
        None,
    ));
    Ok((tag, ops))
}

/// Every rejection `CreateCast` applies to a `WITHOUT FUNCTION` pair, in
/// PostgreSQL's order, and then one more that is gres's own.
///
/// PostgreSQL's physical check compares `typlen`, `typbyval` and `typalign`.
/// gres models only `typlen`, so the test here is weaker by exactly the pairs
/// that agree on width and disagree on alignment — `uuid` and `point` are the
/// pair that exists. It refuses nothing PostgreSQL accepts.
///
/// The last check has no PostgreSQL counterpart: a pair that differ in their
/// physical type can be layout-compatible and still be one gres cannot
/// *perform*, because it holds values as typed `Datum`s and reinterprets them
/// through the binary codec. Refusing at definition time is what keeps `pg_cast`
/// from carrying a row the cast path would then decline to honour. A pair that
/// share a physical type needs no reinterpretation and skips the check.
fn reject_non_coercible(source: ColumnType, target: ColumnType) -> Result<(), ExecError> {
    let refuse = |message: &str| Err(ExecError::InvalidObjectDefinition(message.to_string()));
    if source.type_size() != target.type_size() {
        return refuse("source and target data types are not physically compatible");
    }
    // A composite, an array, a range and an enum all embed an oid in the value,
    // so two of them are never the same bytes however their headers measure up.
    if matches!(source, ColumnType::Record(_)) || matches!(target, ColumnType::Record(_)) {
        return refuse("composite data types are not binary-compatible");
    }
    if source.array_element().is_some() || target.array_element().is_some() {
        return refuse("array data types are not binary-compatible");
    }
    if matches!(source, ColumnType::Range(_) | ColumnType::Multirange(_))
        || matches!(target, ColumnType::Range(_) | ColumnType::Multirange(_))
    {
        return refuse("range data types are not binary-compatible");
    }
    if matches!(source, ColumnType::Enum(_)) || matches!(target, ColumnType::Enum(_)) {
        return refuse("enum data types are not binary-compatible");
    }
    // A domain is excluded both ways: to its base is already allowed, and from
    // its base has to run the constraint checks a pass-through would skip.
    if matches!(source, ColumnType::Domain(_)) || matches!(target, ColumnType::Domain(_)) {
        return refuse("domain data types must not be marked binary-compatible");
    }
    let (from, to) = (physical_type(source), physical_type(target));
    // Two names for one physical type is the pass-through case: `coerce_binary`
    // hands the `Datum` back untouched, so there is no reinterpretation to be
    // capable of. That is the whole of `text → casttesttype (LIKE = text)`, and
    // it has to be settled before the width probe, which a varlena cannot pass.
    if from == to {
        return Ok(());
    }
    if !decodes_fixed_width(from) || !decodes_fixed_width(to) {
        return Err(ExecError::Unsupported(format!(
            "a binary-coercible cast between {} and {} is not supported: gres reinterprets a \
             value through its binary representation, and one of the two has none",
            source.name(),
            target.name()
        )));
    }
    Ok(())
}

/// Whether gres can read a `ty` value back out of a `type_size()`-wide buffer.
///
/// This is what keeps `CREATE CAST … WITHOUT FUNCTION` from recording a pair
/// the cast path could not then perform: a type whose `recv` gres has not
/// written is refused at definition time rather than at first use.
fn decodes_fixed_width(ty: ColumnType) -> bool {
    let width = ty.type_size();
    let Ok(width) = usize::try_from(width) else {
        return false;
    };
    crate::session::decode_binary_value(&vec![0u8; width], ty, &jiff::tz::TimeZone::UTC).is_ok()
}
