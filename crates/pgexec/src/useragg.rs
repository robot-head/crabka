//! User-defined aggregates: `CREATE`/`DROP`/`ALTER AGGREGATE`.
//!
//! An aggregate is stored as a [`Routine`] whose [`RoutineKind`] is
//! [`RoutineKind::Aggregate`], carrying an [`AggregateDefinition`]. That choice
//! is what makes `pg_proc.prokind = 'a'`, `\da`, `DROP`, renaming and ownership
//! work without a second object family: everything the routine catalog already
//! does for a function applies unchanged.
//!
//! Execution reuses the built-in aggregate loop in [`crate::agg`]. The
//! transition function is compiled once per call site into an expression over a
//! two-part synthetic scope — the running state, then the call's arguments — so
//! folding a row is one ordinary [`crate::eval::eval`], not a fresh routine
//! resolution. A `plpgsql` transition function keeps its call node instead and
//! goes through the scalar runtime, exactly as it does in any other expression.
//!
//! Deliberately unimplemented, and recorded rather than rejected so the
//! catalog still describes what was written: the moving-aggregate family
//! (`MSFUNC`/`MSTYPE`/`MINVFUNC`/`MINITCOND`), parallel aggregation
//! (`COMBINEFUNC`/`SERIALFUNC`/`DESERIALFUNC`, which have no plan to run in),
//! `SORTOP`, `SSPACE`, `FINALFUNC_EXTRA` and `FINALFUNC_MODIFY`.

use std::sync::Arc;

use crabka_pgcatalog::routine::{
    AggregateDefinition, ParamMode, Routine, RoutineKind, RoutineParam, RoutineResult, RoutineType,
    drop_routine_ops, get_routine, put_routine_ops, routines_named, signature_identity,
};
use crabka_pgkv::{Kv, WriteOp};
use crabka_pgparser::ast::{
    AggregateArgs, AggregateOption, AggregateSignature, AlterRoutineAction, CreateAggregateStmt,
    Expr, FuncArgs, FuncCall,
};
use crabka_pgtypes::{ColumnType, Datum, ElemType};
use crabka_pgwire::engine::QueryResult;

use crate::{
    clock::EvalCtx,
    error::ExecError,
    scope::{ColumnBinding, Scope},
};

/// The qualifier of the synthetic scope a transition or final expression is
/// evaluated against. A `$` cannot begin an unquoted identifier, so no user
/// relation can collide with it — the same trick [`crate::scope`] uses for its
/// positional and correlated qualifiers.
const AGG_QUALIFIER: &str = "$agg";

/// The pseudo-types an aggregate's state or argument may be declared as.
///
/// `anyenum` is on `PostgreSQL`'s list and in its error text, but no aggregate
/// in the corpus declares one, and this engine has no enum state to resolve.
const POLYMORPHIC_TYPES: &[&str] = &[
    "anyelement",
    "anyarray",
    "anynonarray",
    "anyenum",
    "anyrange",
    "anymultirange",
    "anycompatible",
    "anycompatiblearray",
    "anycompatiblenonarray",
    "anycompatiblerange",
    "anycompatiblemultirange",
];

fn is_polymorphic(name: &str) -> bool {
    POLYMORPHIC_TYPES.contains(&name)
}

fn invalid_definition(message: impl Into<String>) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42P13",
        message: message.into(),
    }
}

/// 42883 without the `HINT` the ordinary undefined-function path adds.
///
/// `PostgreSQL` prints the bare line for an aggregate's support-function
/// lookup and for a missing aggregate, because neither is a call site a cast
/// could rescue.
fn undefined_aggregate(message: impl Into<String>) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42883",
        message: message.into(),
    }
}

// ------------------------------------------------------------------- CREATE

/// `CREATE [OR REPLACE] AGGREGATE`.
///
/// # Errors
///
/// Propagates catalog read errors, and `PostgreSQL`'s own definition-time
/// refusals: a missing `SFUNC`/`STYPE`, an unresolvable transition data type,
/// and a transition or final function whose signature the definition does not
/// name.
pub(crate) fn create(
    kv: &dyn Kv,
    stmt: &CreateAggregateStmt,
    owner: &str,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let routine = build(kv, stmt, owner)?;
    let identity = routine.identity();
    if let Some(existing) = get_routine(kv, &identity)? {
        if !stmt.or_replace {
            return Err(ExecError::FunctionError {
                sqlstate: "42723",
                message: format!(
                    "function \"{}\" already exists with same argument types",
                    routine.name
                ),
            });
        }
        if existing.kind != RoutineKind::Aggregate {
            return Err(crate::routine::wrong_routine_kind(format!(
                "cannot change routine kind\nDETAIL:  \"{}\" is a {}.",
                existing.name,
                existing.kind.word()
            )));
        }
        if existing.result != routine.result {
            return Err(invalid_definition(format!(
                "cannot change return type of existing function\nHINT:  Use DROP AGGREGATE {} \
                 first.",
                existing.identity()
            )));
        }
    }
    let ops = put_routine_ops(kv, &routine)?;
    Ok((
        QueryResult::Command {
            tag: "CREATE AGGREGATE".into(),
        },
        ops,
    ))
}

/// The declared argument types of the aggregate being defined, in either
/// spelling. The old-style form carries its single argument in `BASETYPE`,
/// where `'ANY'` means "takes one argument of any type" — this engine records
/// that the same way `(*)` is recorded, since it has no `"any"` value model.
fn declared_args(
    kv: &dyn Kv,
    stmt: &CreateAggregateStmt,
    options: &Collected,
) -> Result<Vec<RoutineParam>, ExecError> {
    let args = match &stmt.args {
        Some(AggregateArgs::Star) => Vec::new(),
        None => match options.basetype.value() {
            Some(ty) => vec![RoutineParam {
                name: None,
                mode: ParamMode::In,
                ty: crate::routine::resolve_routine_type(kv, ty, false)?,
                default: None,
            }],
            // `(*)`, an absent BASETYPE, and `BASETYPE = 'ANY'` all describe an
            // aggregate with no declared argument type.
            None => Vec::new(),
        },
        Some(AggregateArgs::Args(args)) => args
            .iter()
            .map(|arg| {
                Ok(RoutineParam {
                    name: arg.name.clone(),
                    mode: ParamMode::In,
                    ty: crate::routine::resolve_routine_type(kv, &arg.ty, false)?,
                    default: None,
                })
            })
            .collect::<Result<Vec<_>, ExecError>>()?,
    };
    Ok(args)
}

/// The options an aggregate definition supplied, after folding the numbered
/// (`SFUNC1`/`STYPE1`/`INITCOND1`) spellings onto the plain ones.
#[derive(Debug, Default)]
struct Collected {
    sfunc: Option<String>,
    stype: Option<crabka_pgparser::ast::RoutineType>,
    finalfunc: Option<String>,
    /// `Unwritten` and an explicit `INITCOND = NULL` are different: the second
    /// is still "the state starts NULL", and both are spelled NULL, but only
    /// the first lets a strict transition function bootstrap from the first row.
    initcond: Written<String>,
    basetype: Written<crabka_pgparser::ast::RoutineType>,
    unimplemented: Vec<String>,
}

/// Whether an option was written, and with what.
///
/// `PostgreSQL` distinguishes "absent" from "written as NULL" for `INITCOND`
/// and from "written as `'ANY'`" for `BASETYPE`, so a plain `Option` cannot
/// carry the answer.
#[derive(Debug, Default, PartialEq, Eq)]
enum Written<T> {
    #[default]
    Absent,
    Null,
    Value(T),
}

impl<T> Written<T> {
    fn value(&self) -> Option<&T> {
        match self {
            Self::Value(value) => Some(value),
            _ => None,
        }
    }

    fn from_option(value: Option<T>) -> Self {
        match value {
            Some(value) => Self::Value(value),
            None => Self::Null,
        }
    }
}

impl Collected {
    fn of(options: &[AggregateOption]) -> Self {
        let mut collected = Self::default();
        for option in options {
            match option {
                AggregateOption::SFunc(name) => collected.sfunc = Some(name.clone()),
                AggregateOption::SType(ty) => collected.stype = Some(ty.clone()),
                AggregateOption::FinalFunc(name) => collected.finalfunc = Some(name.clone()),
                AggregateOption::InitCond(value) => {
                    collected.initcond = Written::from_option(value.clone());
                }
                AggregateOption::BaseType(ty) => {
                    collected.basetype = Written::from_option(ty.clone());
                }
                AggregateOption::Unimplemented { name, value } => {
                    collected.unimplemented.push(format!("{name}={value}"));
                }
                AggregateOption::Hypothetical => {
                    collected.unimplemented.push("hypothetical=true".into());
                }
            }
        }
        collected
    }
}

fn build(kv: &dyn Kv, stmt: &CreateAggregateStmt, owner: &str) -> Result<Routine, ExecError> {
    let options = Collected::of(&stmt.options);
    let params = declared_args(kv, stmt, &options)?;
    let Some(sfunc) = options.sfunc.clone() else {
        return Err(invalid_definition("aggregate sfunc must be specified"));
    };
    let Some(stype) = &options.stype else {
        return Err(invalid_definition("aggregate stype must be specified"));
    };
    let transtype = crate::routine::resolve_routine_type(kv, stype, false)?;
    // PostgreSQL resolves a polymorphic state type from the call's arguments,
    // so a definition that declares one without a polymorphic argument has no
    // way to ever pin it down.
    if is_polymorphic(&transtype.name) && !params.iter().any(|param| is_polymorphic(&param.ty.name))
    {
        return Err(ExecError::FunctionError {
            sqlstate: "42P13",
            message: format!(
                "cannot determine transition data type\nDETAIL:  A result of type {} requires at \
                 least one input of type anyelement, anyarray, anynonarray, anyenum, anyrange, or \
                 anymultirange.",
                transtype.name
            ),
        });
    }
    let mut wanted = vec![transtype.clone()];
    wanted.extend(params.iter().map(|param| param.ty.clone()));
    // The lookup is the validation: PostgreSQL refuses a definition whose
    // support function does not exist with exactly this signature.
    lookup(kv, &sfunc, &wanted)?;
    let result = match &options.finalfunc {
        Some(finalfunc) => {
            let function = lookup(kv, finalfunc, std::slice::from_ref(&transtype))?;
            function.result.clone()
        }
        None => RoutineResult::Type {
            ty: transtype.clone(),
            setof: false,
        },
    };
    Ok(Routine {
        oid: 0,
        name: stmt.name.clone(),
        kind: RoutineKind::Aggregate,
        params,
        result,
        // PostgreSQL's pg_proc row for an aggregate names the internal language
        // and the dummy entry point; matching it keeps `\df+` and
        // `pg_get_functiondef` from inventing a body that does not exist.
        language: "internal".into(),
        body: "aggregate_dummy".into(),
        object_file: None,
        body_form: crabka_pgcatalog::routine::BodyForm::Source,
        volatility: 'i',
        parallel: 'u',
        strict: false,
        security_definer: false,
        leakproof: false,
        cost: 1.0,
        rows: 0.0,
        config: Vec::new(),
        owner: owner.to_string(),
        aggregate: Some(AggregateDefinition {
            transfn: sfunc,
            transtype,
            finalfn: options.finalfunc.clone(),
            initcond: options.initcond.value().cloned(),
            unimplemented: options.unimplemented,
        }),
    })
}

/// Find the routine `name` that an aggregate definition names for `wanted`
/// argument types.
///
/// `PostgreSQL` matches an aggregate's support function on the *declared* types
/// rather than by ordinary overload resolution: a concrete parameter does not
/// accept a pseudo-type argument, which is what makes
/// `CREATE AGGREGATE … (BASETYPE = anyelement, SFUNC = tfnp)` fail against
/// `tfnp(int[], int)`.
fn lookup(kv: &dyn Kv, name: &str, wanted: &[RoutineType]) -> Result<Routine, ExecError> {
    let candidates = routines_named(kv, name)?;
    candidates
        .into_iter()
        .find(|candidate| {
            candidate.kind == RoutineKind::Function
                && candidate.input_params().count() == wanted.len()
                && candidate
                    .input_params()
                    .zip(wanted)
                    .all(|(param, want)| accepts(&param.ty, want))
        })
        .ok_or_else(|| {
            undefined_aggregate(format!(
                "function {name}({}) does not exist",
                wanted
                    .iter()
                    .map(|ty| ty.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })
}

/// Does a parameter declared `declared` accept an aggregate support argument of
/// type `wanted`?
fn accepts(declared: &RoutineType, wanted: &RoutineType) -> bool {
    let same_resolved_type = declared.column.is_some() && declared.column == wanted.column;
    if declared.name == wanted.name || same_resolved_type {
        return true;
    }
    if !is_polymorphic(&declared.name) {
        // A concrete parameter never accepts a pseudo-type.
        return false;
    }
    if is_polymorphic(&wanted.name) {
        // Two different pseudo-types: only an element-shaped parameter takes
        // an element-shaped argument, and likewise for arrays.
        return polymorphic_shape(&declared.name) == polymorphic_shape(&wanted.name);
    }
    let Some(column) = wanted.column else {
        return false;
    };
    match polymorphic_shape(&declared.name) {
        Shape::Array => column.array_element().is_some(),
        Shape::NonArray => column.array_element().is_none(),
        Shape::Range => matches!(column, ColumnType::Range(_)),
        Shape::Multirange => matches!(column, ColumnType::Multirange(_)),
        Shape::Element => true,
    }
}

/// The value shape a polymorphic type name constrains its argument to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    Element,
    Array,
    NonArray,
    Range,
    Multirange,
}

fn polymorphic_shape(name: &str) -> Shape {
    match name {
        "anyarray" | "anycompatiblearray" => Shape::Array,
        "anynonarray" | "anycompatiblenonarray" | "anyenum" => Shape::NonArray,
        "anyrange" | "anycompatiblerange" => Shape::Range,
        "anymultirange" | "anycompatiblemultirange" => Shape::Multirange,
        _ => Shape::Element,
    }
}

// --------------------------------------------------------------- DROP/ALTER

/// `DROP AGGREGATE [IF EXISTS] name(sig) […]`.
///
/// # Errors
///
/// Propagates catalog read errors, and 42883 when a named aggregate does not
/// exist and `IF EXISTS` was not written.
pub(crate) fn drop_aggregates(
    kv: &dyn Kv,
    if_exists: bool,
    aggregates: &[AggregateSignature],
    cascade: bool,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    // Nothing in this catalog records a dependency on an aggregate, so CASCADE
    // and RESTRICT have nothing to act on -- the same position `DROP TYPE`'s
    // hand-rolled cascade started from.
    let _ = cascade;
    let mut ops = Vec::new();
    for signature in aggregates {
        match resolve_signature(kv, signature)? {
            Some(routine) => ops.extend(drop_routine_ops(&routine.identity())),
            // PostgreSQL emits `NOTICE: … skipping` here; no DROP … IF EXISTS
            // in this engine does yet.
            None if if_exists => {}
            None => {
                return Err(undefined_aggregate(format!(
                    "aggregate {} does not exist",
                    spelled(signature)
                )));
            }
        }
    }
    Ok((
        QueryResult::Command {
            tag: "DROP AGGREGATE".into(),
        },
        ops,
    ))
}

/// `ALTER AGGREGATE name(sig) RENAME TO`/`OWNER TO`/`SET SCHEMA`.
///
/// # Errors
///
/// Propagates catalog read errors, 42883 for an aggregate that does not exist,
/// and 42723 when a rename would collide.
pub(crate) fn alter(
    kv: &dyn Kv,
    signature: &AggregateSignature,
    action: &AlterRoutineAction,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let Some(mut routine) = resolve_signature(kv, signature)? else {
        return Err(undefined_aggregate(format!(
            "aggregate {} does not exist",
            spelled(signature)
        )));
    };
    let mut ops = Vec::new();
    match action {
        AlterRoutineAction::RenameTo(new_name) => {
            let renamed = signature_identity(new_name, &routine.input_type_names());
            if get_routine(kv, &renamed)?.is_some() {
                return Err(ExecError::FunctionError {
                    sqlstate: "42723",
                    message: format!("function {new_name} already exists with same argument types"),
                });
            }
            ops.extend(drop_routine_ops(&routine.identity()));
            routine.name = new_name.clone();
        }
        AlterRoutineAction::OwnerTo(owner) => routine.owner = owner.clone(),
        AlterRoutineAction::SetSchema(schema) if schema != "public" => {
            return Err(ExecError::Unsupported(format!(
                "ALTER AGGREGATE … SET SCHEMA {schema} is not supported: user routines live in \
                 the public schema"
            )));
        }
        _ => {}
    }
    ops.extend(put_routine_ops(kv, &routine)?);
    Ok((
        QueryResult::Command {
            tag: "ALTER AGGREGATE".into(),
        },
        ops,
    ))
}

fn resolve_signature(
    kv: &dyn Kv,
    signature: &AggregateSignature,
) -> Result<Option<Routine>, ExecError> {
    let names: Vec<String> = match &signature.args {
        AggregateArgs::Star => Vec::new(),
        AggregateArgs::Args(args) => args
            .iter()
            .map(|arg| Ok(crate::routine::resolve_routine_type(kv, &arg.ty, false)?.name))
            .collect::<Result<Vec<_>, ExecError>>()?,
    };
    let identity = signature_identity(&signature.name, &names);
    Ok(get_routine(kv, &identity)?.filter(Routine::is_aggregate))
}

fn spelled(signature: &AggregateSignature) -> String {
    match &signature.args {
        AggregateArgs::Star => format!("{}(*)", signature.name),
        AggregateArgs::Args(args) => format!(
            "{}({})",
            signature.name,
            args.iter()
                .map(|arg| arg.ty.name.clone())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

// ------------------------------------------------------------- call resolution

/// Is `name` a user-defined aggregate in the catalog the statement runtime
/// exposes?
///
/// Answers `false` when no runtime is installed, which is what makes the
/// built-in aggregate surface behave identically whether or not this feature is
/// reachable from the caller's context.
pub(crate) fn exists(name: &str) -> bool {
    with_catalog(|kv| {
        routines_named(kv, name).is_ok_and(|found| found.iter().any(Routine::is_aggregate))
    })
    .unwrap_or(false)
}

fn with_catalog<T>(f: impl FnOnce(&dyn Kv) -> T) -> Option<T> {
    let catalog: Arc<dyn Kv> = crate::routine::scalar_runtime_catalog()?;
    Some(f(catalog.as_ref()))
}

/// A user aggregate resolved for one call site: everything folding a row needs,
/// computed once.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UserAggregate {
    /// The call's argument expressions, in declaration order.
    pub(crate) args: Vec<Expr>,
    /// The transition expression over [`Self::transition_scope`].
    transition: Expr,
    transition_scope: Scope,
    /// The final expression over a one-column state scope, if `FINALFUNC` was
    /// written.
    final_expr: Option<Expr>,
    final_scope: Scope,
    /// The resolved state type.
    state_type: ColumnType,
    /// The aggregate's resolved result type.
    pub(crate) result_type: ColumnType,
    /// `agginitval`, still in its text form; cast to the state type on first
    /// use so a polymorphic state resolves per call.
    initcond: Option<String>,
    /// A strict transition function skips NULL inputs and takes the first
    /// non-null argument as the state when there is no `INITCOND`.
    strict_transition: bool,
}

impl UserAggregate {
    /// The state a fresh group starts from.
    pub(crate) fn initial_state(&self, ctx: &EvalCtx) -> Result<Datum, ExecError> {
        match &self.initcond {
            Some(text) => {
                crate::eval::cast_value(&Datum::Text(text.clone()), self.state_type, &ctx.time_zone)
            }
            None => Ok(Datum::Null),
        }
    }

    /// Fold one row's already-evaluated arguments into `state`.
    pub(crate) fn fold(
        &self,
        state: &mut Datum,
        args: &[Datum],
        ctx: &EvalCtx,
    ) -> Result<(), ExecError> {
        if self.strict_transition {
            if args.iter().any(Datum::is_null) {
                return Ok(());
            }
            // PostgreSQL bootstraps a NULL state from the first non-null input
            // when the transition function is strict, which is only meaningful
            // when the state and the first argument share a type.
            if state.is_null()
                && self.initcond.is_none()
                && let Some(first) = args.first()
            {
                *state = first.clone();
                return Ok(());
            }
        }
        let mut row = Vec::with_capacity(args.len() + 1);
        row.push(state.clone());
        row.extend_from_slice(args);
        *state = crate::eval::eval(&self.transition, &self.transition_scope, &row, ctx)?;
        Ok(())
    }

    /// The group's answer, once every row has been folded.
    pub(crate) fn finish(&self, state: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
        let value = match &self.final_expr {
            Some(expr) => {
                crate::eval::eval(expr, &self.final_scope, std::slice::from_ref(state), ctx)?
            }
            None => state.clone(),
        };
        crate::eval::cast_value(&value, self.result_type, &ctx.time_zone)
    }
}

fn synthetic_scope(types: &[ColumnType]) -> Scope {
    Scope {
        columns: types
            .iter()
            .enumerate()
            .map(|(index, ty)| ColumnBinding {
                qualifier: Some(AGG_QUALIFIER.to_string()),
                name: format!("a{index}"),
                ty: *ty,
            })
            .collect(),
    }
}

fn synthetic_args(count: usize) -> Vec<Expr> {
    (0..count)
        .map(|index| Expr::Column {
            table: Some(AGG_QUALIFIER.to_string()),
            name: format!("a{index}"),
        })
        .collect()
}

/// Resolve a call of the user aggregate `name` over arguments of type `given`.
///
/// `None` means "no such user aggregate": the caller keeps its own built-in
/// resolution and error. `Some(Err(_))` is a real refusal.
pub(crate) fn resolve(
    name: &str,
    args: &[Expr],
    given: &[ColumnType],
) -> Option<Result<UserAggregate, ExecError>> {
    with_catalog(|kv| resolve_in(kv, name, args, given))?
}

fn resolve_in(
    kv: &dyn Kv,
    name: &str,
    args: &[Expr],
    given: &[ColumnType],
) -> Option<Result<UserAggregate, ExecError>> {
    let candidates = routines_named(kv, name).ok()?;
    let routine = candidates.into_iter().find(|candidate| {
        candidate.is_aggregate() && candidate.input_params().count() == args.len()
    })?;
    Some(compile(kv, &routine, args, given))
}

/// Compile a resolved aggregate's transition and final functions into
/// expressions over the synthetic scope.
fn compile(
    kv: &dyn Kv,
    routine: &Routine,
    args: &[Expr],
    given: &[ColumnType],
) -> Result<UserAggregate, ExecError> {
    let definition = routine.aggregate.as_ref().ok_or_else(|| {
        ExecError::Unsupported(format!("{} has no definition", routine.identity()))
    })?;
    // `definition.unimplemented` is not consulted: every option it holds is a
    // performance or parallelism hint that cannot change the answer. The one
    // option that would — an ordered-set aggregate — is refused in the grammar
    // and never reaches the catalog.
    let state_type = resolve_state_type(routine, definition, given)?;
    let mut argument_types = vec![state_type];
    argument_types.extend_from_slice(given);
    let transition_scope = synthetic_scope(&argument_types);
    let transition = compile_call(
        kv,
        &definition.transfn,
        &synthetic_args(argument_types.len()),
        &transition_scope,
    )?;
    let final_scope = synthetic_scope(&[state_type]);
    let final_expr = definition
        .finalfn
        .as_ref()
        .map(|name| compile_call(kv, name, &synthetic_args(1), &final_scope))
        .transpose()?;
    let result_type = match &final_expr {
        Some(expr) => crate::eval::infer_type(expr, &final_scope)?,
        None => state_type,
    };
    Ok(UserAggregate {
        args: args.to_vec(),
        transition,
        transition_scope,
        final_expr,
        final_scope,
        state_type,
        result_type,
        initcond: definition.initcond.clone(),
        strict_transition: transition_is_strict(kv, &definition.transfn),
    })
}

/// The state type this call runs with: the declared one, or — when it is
/// polymorphic — the shape the call's own argument types pin it to.
fn resolve_state_type(
    routine: &Routine,
    definition: &AggregateDefinition,
    given: &[ColumnType],
) -> Result<ColumnType, ExecError> {
    if let Some(column) = definition.transtype.column {
        return Ok(column);
    }
    let base = routine
        .input_params()
        .zip(given)
        .find(|(param, _)| is_polymorphic(&param.ty.name))
        .map(|(param, ty)| match polymorphic_shape(&param.ty.name) {
            Shape::Array => ty.array_element().map_or(*ty, ElemType::column_type),
            _ => *ty,
        });
    let base = base.ok_or_else(|| {
        ExecError::Unsupported(format!(
            "cannot resolve the transition data type of aggregate {}",
            routine.identity()
        ))
    })?;
    Ok(match polymorphic_shape(&definition.transtype.name) {
        Shape::Array => ColumnType::array_of(base).ok_or_else(|| {
            ExecError::Unsupported(format!(
                "type {} has no array type, so aggregate {} cannot run",
                base.name(),
                routine.identity()
            ))
        })?,
        _ => base,
    })
}

fn transition_is_strict(kv: &dyn Kv, name: &str) -> bool {
    routines_named(kv, name)
        .ok()
        .and_then(|found| found.into_iter().next())
        .is_some_and(|routine| routine.strict)
}

/// Turn a support-function call into an expression over `scope`: an inlined SQL
/// body where the routine model can inline one, and the call node itself
/// otherwise, which is what routes a `plpgsql` body through the scalar runtime.
fn compile_call(kv: &dyn Kv, name: &str, args: &[Expr], scope: &Scope) -> Result<Expr, ExecError> {
    let call = FuncCall {
        name: name.to_string(),
        distinct: false,
        args: FuncArgs::Exprs(args.to_vec()),
        order_by: Vec::new(),
        filter: None,
    };
    let given = crate::eval::static_arg_types(args, scope)?;
    match crate::routine::inline_scalar_call(kv, &call, &given) {
        Ok(Some(inlined)) => Ok(inlined),
        Ok(None) | Err(_) => Ok(Expr::Func(call)),
    }
}

// ----------------------------------------------------------------- catalog

/// The `pg_aggregate` rows for the aggregates this database defines.
///
/// Columns this engine does not implement are projected the way `PostgreSQL`
/// projects an aggregate that does not use them: `0` for an absent `regproc`
/// or `oid`, `false` for the `finalextra` flags, `r` (read-only) for the
/// `finalmodify` pair, and NULL for an absent init value.
///
/// # Errors
///
/// Propagates catalog read errors.
pub(crate) fn pg_aggregate_rows(kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let routines = crabka_pgcatalog::routine::list_routines(kv)?;
    let oid_of = |name: &str| -> i32 {
        routines
            .iter()
            .find(|routine| routine.name == name && !routine.is_aggregate())
            .map_or(0, |routine| i32::try_from(routine.oid).unwrap_or(0))
    };
    Ok(routines
        .iter()
        .filter(|routine| routine.is_aggregate())
        .filter_map(|routine| {
            let definition = routine.aggregate.as_ref()?;
            let transtype = definition
                .transtype
                .column
                .map_or(0, |ty| i32::try_from(ty.oid()).unwrap_or(0));
            Some(vec![
                Datum::Int4(i32::try_from(routine.oid).unwrap_or(0)),
                Datum::Text("n".into()),
                Datum::Int2(0),
                Datum::Int4(oid_of(&definition.transfn)),
                Datum::Int4(definition.finalfn.as_deref().map_or(0, oid_of)),
                Datum::Int4(0),
                Datum::Int4(0),
                Datum::Int4(0),
                Datum::Int4(0),
                Datum::Int4(0),
                Datum::Int4(0),
                Datum::Bool(false),
                Datum::Bool(false),
                Datum::Text("r".into()),
                Datum::Text("r".into()),
                Datum::Int4(0),
                Datum::Int4(transtype),
                Datum::Int4(0),
                Datum::Int4(0),
                Datum::Int4(0),
                definition.initcond.clone().map_or(Datum::Null, Datum::Text),
                Datum::Null,
            ])
        })
        .collect())
}
