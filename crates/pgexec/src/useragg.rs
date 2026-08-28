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
//! catalog still describes what was written: parallel execution (the definition
//! and catalog metadata for `COMBINEFUNC`/`SERIALFUNC`/`DESERIALFUNC` do
//! validate), `SORTOP` and `SSPACE`.

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
    scope::{ColumnBinding, Exposure, Scope},
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
pub(crate) fn undefined_aggregate(message: impl Into<String>) -> ExecError {
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
        if existing
            .aggregate
            .as_ref()
            .is_some_and(|definition| definition.kind != aggregate_kind(&routine))
        {
            return Err(crate::routine::wrong_routine_kind(format!(
                "cannot change routine kind\nDETAIL:  \"{}\" is an {}.",
                existing.name,
                aggregate_kind_word(existing.aggregate.as_ref().expect("aggregate checked").kind)
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

fn aggregate_kind(routine: &Routine) -> char {
    routine
        .aggregate
        .as_ref()
        .expect("aggregate definition is present")
        .kind
}

fn aggregate_kind_word(kind: char) -> &'static str {
    match kind {
        'n' => "ordinary aggregate function",
        'o' => "ordered-set aggregate function",
        'h' => "hypothetical-set aggregate function",
        _ => "aggregate function",
    }
}

/// The declared argument types of the aggregate being defined, in either
/// spelling. The old-style form carries its single argument in `BASETYPE`,
/// where `'ANY'` means "takes one argument of any type" — this engine records
/// that the same way `(*)` is recorded, since it has no `"any"` value model.
struct DeclaredArgs {
    params: Vec<RoutineParam>,
    direct_count: usize,
    ordered_count: usize,
}

fn declared_args(
    kv: &dyn Kv,
    stmt: &CreateAggregateStmt,
    options: &Collected,
) -> Result<DeclaredArgs, ExecError> {
    let (args, direct_count, ordered_count) = match &stmt.args {
        Some(AggregateArgs::Star) => (Vec::new(), 0, 0),
        None => match options.basetype.value() {
            Some(ty) => (
                vec![RoutineParam {
                    name: None,
                    mode: ParamMode::In,
                    ty: crate::routine::resolve_routine_type(kv, ty, false)?,
                    default: None,
                }],
                0,
                0,
            ),
            // `(*)`, an absent BASETYPE, and `BASETYPE = 'ANY'` all describe an
            // aggregate with no declared argument type.
            None => (Vec::new(), 0, 0),
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
            .collect::<Result<Vec<_>, ExecError>>()
            .map(|params| (params, 0, 0))?,
        Some(AggregateArgs::Ordered { direct, ordered }) => {
            let direct_count = direct.len();
            let ordered_count = ordered.len();
            direct
                .iter()
                .chain(ordered)
                .map(|arg| {
                    Ok(RoutineParam {
                        name: arg.name.clone(),
                        mode: ParamMode::In,
                        ty: crate::routine::resolve_routine_type(kv, &arg.ty, false)?,
                        default: None,
                    })
                })
                .collect::<Result<Vec<_>, ExecError>>()
                .map(|params| (params, direct_count, ordered_count))?
        }
    };
    Ok(DeclaredArgs {
        params: args,
        direct_count,
        ordered_count,
    })
}

/// The options an aggregate definition supplied, after folding the numbered
/// (`SFUNC1`/`STYPE1`/`INITCOND1`) spellings onto the plain ones.
#[derive(Debug, Default)]
struct Collected {
    sfunc: Option<String>,
    stype: Option<crabka_pgparser::ast::RoutineType>,
    finalfunc: Option<String>,
    combinefunc: Option<String>,
    serialfunc: Option<String>,
    deserialfunc: Option<String>,
    finalfunc_modify: Option<String>,
    parallel: Option<String>,
    /// `Unwritten` and an explicit `INITCOND = NULL` are different: the second
    /// is still "the state starts NULL", and both are spelled NULL, but only
    /// the first lets a strict transition function bootstrap from the first row.
    initcond: Written<String>,
    basetype: Written<crabka_pgparser::ast::RoutineType>,
    finalfunc_extra: bool,
    hypothetical: bool,
    msfunc: Option<String>,
    minvfunc: Option<String>,
    mstype: Option<crabka_pgparser::ast::RoutineType>,
    mfinalfunc: Option<String>,
    minitcond: Written<String>,
    mfinalfunc_extra: bool,
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
                AggregateOption::MSType(ty) => collected.mstype = Some(ty.clone()),
                AggregateOption::FinalFunc(name) => collected.finalfunc = Some(name.clone()),
                AggregateOption::InitCond(value) => {
                    collected.initcond = Written::from_option(value.clone());
                }
                AggregateOption::MInitCond(value) => {
                    collected.minitcond = Written::from_option(value.clone());
                }
                AggregateOption::BaseType(ty) => {
                    collected.basetype = Written::from_option(ty.clone());
                }
                AggregateOption::Unimplemented { name, value } if name == "finalfunc_extra" => {
                    collected.finalfunc_extra = value == "true";
                }
                AggregateOption::Unimplemented { name, value } if name == "combinefunc" => {
                    collected.combinefunc = Some(value.clone());
                }
                AggregateOption::Unimplemented { name, value } if name == "serialfunc" => {
                    collected.serialfunc = Some(value.clone());
                }
                AggregateOption::Unimplemented { name, value } if name == "deserialfunc" => {
                    collected.deserialfunc = Some(value.clone());
                }
                AggregateOption::Unimplemented { name, value } if name == "finalfunc_modify" => {
                    collected.finalfunc_modify = Some(value.clone());
                }
                AggregateOption::Unimplemented { name, value } if name == "parallel" => {
                    collected.parallel = Some(value.clone());
                }
                AggregateOption::Unimplemented { name, value } if name == "msfunc" => {
                    collected.msfunc = Some(value.clone());
                }
                AggregateOption::Unimplemented { name, value } if name == "minvfunc" => {
                    collected.minvfunc = Some(value.clone());
                }
                AggregateOption::Unimplemented { name, value } if name == "mfinalfunc" => {
                    collected.mfinalfunc = Some(value.clone());
                }
                AggregateOption::Unimplemented { name, value } if name == "mfinalfunc_extra" => {
                    collected.mfinalfunc_extra = value == "true";
                }
                AggregateOption::Unimplemented { name, value } => {
                    collected.unimplemented.push(format!("{name}={value}"));
                }
                AggregateOption::Hypothetical => {
                    collected.hypothetical = true;
                }
            }
        }
        collected
    }
}

fn build(kv: &dyn Kv, stmt: &CreateAggregateStmt, owner: &str) -> Result<Routine, ExecError> {
    let options = Collected::of(&stmt.options);
    let declared = declared_args(kv, stmt, &options)?;
    let kind = aggregate_definition_kind(&declared, &options);
    let params = declared.params;
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
    wanted.extend(
        params[declared.direct_count..]
            .iter()
            .map(|param| param.ty.clone()),
    );
    // The lookup is the validation: PostgreSQL refuses a definition whose
    // support function does not exist with exactly this signature.
    let transition = lookup(kv, &sfunc, &wanted)?;
    validate_parallel_definition(kv, &options, &transtype)?;
    validate_moving_definition(kv, &options, &transtype, &wanted, transition.strict())?;
    let result = match &options.finalfunc {
        Some(finalfunc) => {
            let mut wanted = vec![transtype.clone()];
            if options.finalfunc_extra {
                wanted.extend(params.iter().map(|param| param.ty.clone()));
            }
            let function = lookup(kv, finalfunc, &wanted)?;
            function.result()
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
        parallel: aggregate_parallel(&options)?,
        window: false,
        strict: false,
        security_definer: false,
        leakproof: false,
        cost: 1.0,
        rows: 0.0,
        config: Vec::new(),
        config_source: Vec::new(),
        owner: owner.to_string(),
        aggregate: Some(AggregateDefinition {
            kind,
            transfn: sfunc,
            transtype,
            finalfn: options.finalfunc.clone(),
            combinefn: options.combinefunc.clone(),
            serialfn: options.serialfunc.clone(),
            deserialfn: options.deserialfunc.clone(),
            finalfunc_modify: finalfunc_modify(&options)?,
            initcond: options.initcond.value().cloned(),
            moving_transfn: options.msfunc.clone(),
            moving_invtransfn: options.minvfunc.clone(),
            moving_transtype: options
                .mstype
                .as_ref()
                .map(|ty| crate::routine::resolve_routine_type(kv, ty, false))
                .transpose()?,
            moving_finalfn: options.mfinalfunc.clone(),
            moving_initcond: options.minitcond.value().cloned(),
            moving_initcond_specified: !matches!(options.minitcond, Written::Absent),
            moving_finalfunc_extra: options.mfinalfunc_extra,
            direct_args: declared.direct_count,
            ordered_args: declared.ordered_count,
            finalfunc_extra: options.finalfunc_extra,
            hypothetical: options.hypothetical,
            unimplemented: options.unimplemented,
        }),
    })
}

fn aggregate_definition_kind(declared: &DeclaredArgs, options: &Collected) -> char {
    if options.hypothetical {
        'h'
    } else if declared.ordered_count > 0 {
        'o'
    } else {
        'n'
    }
}

fn aggregate_parallel(options: &Collected) -> Result<char, ExecError> {
    match options.parallel.as_deref().unwrap_or("unsafe") {
        "safe" => Ok('s'),
        "restricted" => Ok('r'),
        "unsafe" => Ok('u'),
        _ => Err(invalid_definition(
            "parameter \"parallel\" must be SAFE, RESTRICTED, or UNSAFE",
        )),
    }
}

fn finalfunc_modify(options: &Collected) -> Result<char, ExecError> {
    match options.finalfunc_modify.as_deref().unwrap_or("read_only") {
        "read_only" => Ok('r'),
        "shareable" => Ok('s'),
        "read_write" => Ok('w'),
        value => Err(invalid_definition(format!(
            "parameter \"finalfunc_modify\" must be READ_ONLY, SHAREABLE, or READ_WRITE, not \"{value}\""
        ))),
    }
}

fn validate_parallel_definition(
    kv: &dyn Kv,
    options: &Collected,
    transtype: &RoutineType,
) -> Result<(), ExecError> {
    if options.serialfunc.is_some() != options.deserialfunc.is_some() {
        return Err(invalid_definition(
            "must specify both or neither of serialization and deserialization functions",
        ));
    }
    if let Some(serialfunc) = options.serialfunc.as_deref() {
        let serial = lookup(kv, serialfunc, std::slice::from_ref(transtype))?;
        ensure_return_type(
            "serialization",
            serialfunc,
            &serial,
            &RoutineType::builtin(ColumnType::Bytea),
        )?;
        let deserialfunc = options
            .deserialfunc
            .as_deref()
            .expect("serialization pair was checked");
        let deserial = lookup(
            kv,
            deserialfunc,
            &[RoutineType::builtin(ColumnType::Bytea), transtype.clone()],
        )?;
        ensure_return_type("deserialization", deserialfunc, &deserial, transtype)?;
    }
    if let Some(combinefunc) = options.combinefunc.as_deref() {
        let combine = lookup(kv, combinefunc, &[transtype.clone(), transtype.clone()])?;
        ensure_return_type("combine", combinefunc, &combine, transtype)?;
    }
    Ok(())
}

/// Validate the moving transition functions even though N17 owns their window
/// execution. A definition must not reach the catalog when the inverse changes
/// the state type or strictness; both faults are independent of window frames.
fn validate_moving_definition(
    kv: &dyn Kv,
    options: &Collected,
    transtype: &RoutineType,
    transition_types: &[RoutineType],
    transition_strict: bool,
) -> Result<(), ExecError> {
    let Some(msfunc) = options.msfunc.as_deref() else {
        return Ok(());
    };
    let mstype = options.mstype.as_ref().map_or_else(
        || Ok(transtype.clone()),
        |ty| crate::routine::resolve_routine_type(kv, ty, false),
    )?;
    let mut moving_types = transition_types.to_vec();
    moving_types[0] = mstype.clone();
    let moving = lookup(kv, msfunc, &moving_types)?;
    ensure_moving_return_type("moving transition", msfunc, &moving, &mstype)?;
    if let Some(minvfunc) = options.minvfunc.as_deref() {
        let inverse = lookup(kv, minvfunc, &moving_types)?;
        if inverse.strict() != transition_strict {
            return Err(invalid_definition(
                "strictness of aggregate's forward and inverse transition functions must match",
            ));
        }
        ensure_moving_return_type("inverse transition", minvfunc, &inverse, &mstype)?;
    }
    Ok(())
}

fn ensure_moving_return_type(
    role: &str,
    name: &str,
    function: &SupportRoutine,
    expected: &RoutineType,
) -> Result<(), ExecError> {
    let RoutineResult::Type { ty, setof: false } = function.result() else {
        return Err(invalid_definition(format!(
            "return type of {role} function {name} is not {}",
            expected.name
        )));
    };
    if ty != *expected {
        return Err(invalid_definition(format!(
            "return type of {role} function {name} is not {}",
            expected.name
        )));
    }
    Ok(())
}

fn ensure_return_type(
    role: &str,
    name: &str,
    function: &SupportRoutine,
    expected: &RoutineType,
) -> Result<(), ExecError> {
    let RoutineResult::Type { ty, setof: false } = function.result() else {
        return Err(invalid_definition(format!(
            "return type of {role} function {name} is not {}",
            expected.name
        )));
    };
    if ty != *expected {
        return Err(invalid_definition(format!(
            "return type of {role} function {name} is not {}",
            expected.name
        )));
    }
    Ok(())
}

/// Find the routine `name` that an aggregate definition names for `wanted`
/// argument types.
///
/// `PostgreSQL` matches an aggregate's support function on the *declared* types
/// rather than by ordinary overload resolution: a concrete parameter does not
/// accept a pseudo-type argument, which is what makes
/// `CREATE AGGREGATE … (BASETYPE = anyelement, SFUNC = tfnp)` fail against
/// `tfnp(int[], int)`.
enum SupportRoutine {
    User(Routine),
    Builtin { result: RoutineResult, strict: bool },
}

impl SupportRoutine {
    fn result(&self) -> RoutineResult {
        match self {
            Self::User(routine) => routine.result.clone(),
            Self::Builtin { result, .. } => result.clone(),
        }
    }

    fn strict(&self) -> bool {
        match self {
            Self::User(routine) => routine.strict,
            Self::Builtin { strict, .. } => *strict,
        }
    }
}

fn lookup(kv: &dyn Kv, name: &str, wanted: &[RoutineType]) -> Result<SupportRoutine, ExecError> {
    let user = routines_named(kv, name)?.into_iter().find(|candidate| {
        candidate.kind == RoutineKind::Function
            && candidate.input_params().count() == wanted.len()
            && candidate
                .input_params()
                .zip(wanted)
                .all(|(param, want)| accepts(&param.ty, want))
    });
    if let Some(routine) = user {
        return Ok(SupportRoutine::User(routine));
    }
    if let Some(builtin) = builtin_support(name, wanted)? {
        return Ok(builtin);
    }
    Err(undefined_aggregate(format!(
        "function {name}({}) does not exist",
        wanted
            .iter()
            .map(|ty| ty.name.clone())
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

/// Resolve an executable built-in support function from the same `pg_proc`
/// fixture that exposes its declared input, result, and strictness metadata.
fn builtin_support(
    name: &str,
    wanted: &[RoutineType],
) -> Result<Option<SupportRoutine>, ExecError> {
    let Some(row) = crate::routine::builtin_pg_proc_rows()?.into_iter().find(|row| {
        row.get(1) == Some(&Datum::Text(name.to_string()))
            && matches!(row.get(19), Some(Datum::OidVector(args)) if builtin_support_args_match(&args.elems, wanted))
    })
    else {
        return Ok(None);
    };
    let (Some(Datum::Int4(result_oid)), Some(Datum::Bool(strict)), Some(Datum::Bool(setof))) =
        (row.get(18), row.get(12), row.get(13))
    else {
        return Err(ExecError::Unsupported(
            "built-in pg_proc fixture is corrupt".into(),
        ));
    };
    let Datum::OidVector(args) = row
        .get(19)
        .expect("matched built-in support function has argument metadata")
    else {
        unreachable!("matched built-in support function has oidvector arguments")
    };
    let result = builtin_support_result_type(*result_oid as u32, &args.elems, wanted)?;
    Ok(Some(SupportRoutine::Builtin {
        result: RoutineResult::Type {
            ty: result,
            setof: *setof,
        },
        strict: *strict,
    }))
}

/// `pg_proc` records polymorphic support parameters as pseudo-type OIDs. Match
/// those against the concrete state and input types that an aggregate
/// definition supplies, while retaining exact OID matching for all other
/// built-ins.
fn builtin_support_args_match(declared: &[Datum], wanted: &[RoutineType]) -> bool {
    if declared.len() != wanted.len() {
        return false;
    }
    let mut array_type = None;
    let mut element_type = None;
    declared.iter().zip(wanted).all(|(declared, wanted)| {
        let Datum::Int4(declared) = declared else {
            return false;
        };
        match *declared as u32 {
            2277 | 5078 => {
                let Some(column @ ColumnType::Array(element)) = wanted.column else {
                    return false;
                };
                if !matches!(array_type, Some(bound) if bound != column) {
                    if array_type.is_none() {
                        array_type = Some(column);
                    }
                    match element_type {
                        Some(bound) => bound == element.column_type(),
                        None => true,
                    }
                } else {
                    false
                }
            }
            2283 | 5077 => {
                let Some(column) = wanted.column else {
                    return false;
                };
                if !matches!(element_type, Some(bound) if bound != column) {
                    if element_type.is_none() {
                        element_type = Some(column);
                    }
                    match array_type {
                        Some(ColumnType::Array(element)) => element.column_type() == column,
                        Some(_) => unreachable!("array binding only stores arrays"),
                        None => true,
                    }
                } else {
                    false
                }
            }
            _ => crate::routine::type_oid(wanted) == *declared,
        }
    })
}

/// Resolve an aggregate support function's declared result pseudo-type from
/// the concrete argument that bound it. `array_larger(anyarray, anyarray)` is
/// the first such support function; this also keeps the lookup rule aligned
/// with `pg_proc` for later `anyarray` support functions.
fn builtin_support_result_type(
    result_oid: u32,
    declared: &[Datum],
    wanted: &[RoutineType],
) -> Result<RoutineType, ExecError> {
    if matches!(result_oid, 2277 | 5078) {
        return declared
            .iter()
            .zip(wanted)
            .find_map(|(declared, wanted)| {
                matches!(declared, Datum::Int4(2277 | 5078))
                    .then_some(wanted.column)
                    .flatten()
            })
            .map(RoutineType::builtin)
            .ok_or_else(|| {
                ExecError::Unsupported("unbound anyarray built-in support result".into())
            });
    }
    crate::routine::routine_type_from_oid(result_oid)
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

pub(crate) fn resolve_signature(
    kv: &dyn Kv,
    signature: &AggregateSignature,
) -> Result<Option<Routine>, ExecError> {
    let names: Vec<String> = match &signature.args {
        AggregateArgs::Star => Vec::new(),
        AggregateArgs::Args(args) => args
            .iter()
            .map(|arg| Ok(crate::routine::resolve_routine_type(kv, &arg.ty, false)?.name))
            .collect::<Result<Vec<_>, ExecError>>()?,
        AggregateArgs::Ordered { direct, ordered } => direct
            .iter()
            .chain(ordered)
            .map(|arg| Ok(crate::routine::resolve_routine_type(kv, &arg.ty, false)?.name))
            .collect::<Result<Vec<_>, ExecError>>()?,
    };
    let identity = signature_identity(&signature.name, &names);
    Ok(get_routine(kv, &identity)?.filter(Routine::is_aggregate))
}

pub(crate) fn spelled(signature: &AggregateSignature) -> String {
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
        AggregateArgs::Ordered { direct, ordered } => format!(
            "{}({} ORDER BY {})",
            signature.name,
            direct
                .iter()
                .map(|arg| arg.ty.name.clone())
                .collect::<Vec<_>>()
                .join(", "),
            ordered
                .iter()
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

/// Does a user aggregate with this call arity exist in the statement catalog?
pub(crate) fn exists_with_call(name: &str, arity: usize, within_group: bool) -> bool {
    with_catalog(|kv| {
        routines_named(kv, name).is_ok_and(|found| {
            found.iter().any(|routine| {
                let Some(definition) = routine.aggregate.as_ref() else {
                    return false;
                };
                routine.is_aggregate()
                    && if within_group {
                        definition.ordered_args > 0 && definition.direct_args == arity
                    } else {
                        definition.ordered_args == 0 && routine.input_params().count() == arity
                    }
            })
        })
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
    /// `WITHIN GROUP`'s direct arguments, evaluated once per group and passed
    /// to an extra-argument final function rather than to each transition.
    direct_args: Vec<Expr>,
    /// The transition expression over [`Self::transition_scope`].
    transition: Expr,
    transition_scope: Scope,
    /// The final expression over a one-column state scope, if `FINALFUNC` was
    /// written.
    final_expr: Option<Expr>,
    final_scope: Scope,
    finalfunc_extra: bool,
    ordered_args: usize,
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
    moving: Option<MovingAggregate>,
}

/// The window-only transition path, kept separate because its state can differ
/// from the ordinary aggregate state.
#[derive(Debug, Clone, PartialEq)]
struct MovingAggregate {
    transition: Expr,
    transition_scope: Scope,
    final_expr: Option<Expr>,
    final_scope: Scope,
    state_type: ColumnType,
    initcond: Option<String>,
    initcond_specified: bool,
    strict_transition: bool,
    finalfunc_extra: bool,
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
    pub(crate) fn direct_values(
        &self,
        scope: &Scope,
        row: &[Datum],
        ctx: &EvalCtx,
    ) -> Result<Vec<Datum>, ExecError> {
        self.direct_args
            .iter()
            .map(|arg| crate::eval::eval(arg, scope, row, ctx))
            .collect()
    }

    pub(crate) fn finish(
        &self,
        state: &Datum,
        direct_args: &[Datum],
        ctx: &EvalCtx,
    ) -> Result<Datum, ExecError> {
        let value = match &self.final_expr {
            Some(expr) => {
                let mut row = Vec::with_capacity(1 + direct_args.len() + self.ordered_args);
                row.push(state.clone());
                if self.finalfunc_extra {
                    row.extend_from_slice(direct_args);
                    row.extend(std::iter::repeat_n(Datum::Null, self.ordered_args));
                }
                crate::eval::eval(expr, &self.final_scope, &row, ctx)?
            }
            None => state.clone(),
        };
        crate::eval::cast_value(&value, self.result_type, &ctx.time_zone)
    }

    pub(crate) fn has_moving_transition(&self) -> bool {
        self.moving.is_some()
    }

    pub(crate) fn moving_initial_state(&self, ctx: &EvalCtx) -> Result<Datum, ExecError> {
        let moving = self.moving.as_ref().expect("moving transition exists");
        match &moving.initcond {
            Some(text) => crate::eval::cast_value(
                &Datum::Text(text.clone()),
                moving.state_type,
                &ctx.time_zone,
            ),
            None => Ok(Datum::Null),
        }
    }

    pub(crate) fn moving_fold(
        &self,
        state: &mut Datum,
        args: &[Datum],
        ctx: &EvalCtx,
    ) -> Result<(), ExecError> {
        let moving = self.moving.as_ref().expect("moving transition exists");
        if moving.strict_transition {
            if args.iter().any(Datum::is_null) {
                return Ok(());
            }
            if state.is_null()
                && !moving.initcond_specified
                && let Some(first) = args.first()
            {
                *state = first.clone();
                return Ok(());
            }
        }
        let mut row = Vec::with_capacity(args.len() + 1);
        row.push(state.clone());
        row.extend_from_slice(args);
        *state = crate::eval::eval(&moving.transition, &moving.transition_scope, &row, ctx)?;
        Ok(())
    }

    pub(crate) fn moving_finish(&self, state: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
        let moving = self.moving.as_ref().expect("moving transition exists");
        let value = match &moving.final_expr {
            Some(expr) => {
                let mut row = vec![state.clone()];
                if moving.finalfunc_extra {
                    row.extend(std::iter::repeat_n(Datum::Null, self.args.len()));
                }
                crate::eval::eval(expr, &moving.final_scope, &row, ctx)?
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
                exposure: Exposure::Output,
                qualifier: Some(AGG_QUALIFIER.to_string()),
                name: format!("a{index}"),
                ty: *ty,
            })
            .collect(),
        ..Default::default()
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
    direct_args: &[Expr],
    ordered_args: &[Expr],
    given: &[ColumnType],
) -> Option<Result<UserAggregate, ExecError>> {
    with_catalog(|kv| resolve_in(kv, name, direct_args, ordered_args, given))?
}

fn resolve_in(
    kv: &dyn Kv,
    name: &str,
    direct_args: &[Expr],
    ordered_args: &[Expr],
    given: &[ColumnType],
) -> Option<Result<UserAggregate, ExecError>> {
    let candidates = routines_named(kv, name).ok()?;
    let routine = candidates.into_iter().find(|candidate| {
        let Some(definition) = candidate.aggregate.as_ref() else {
            return false;
        };
        candidate.is_aggregate()
            && if definition.ordered_args == 0 {
                ordered_args.is_empty() && candidate.input_params().count() == direct_args.len()
            } else {
                definition.direct_args == direct_args.len()
                    && definition.ordered_args == ordered_args.len()
            }
    })?;
    Some(compile(kv, &routine, direct_args, ordered_args, given))
}

/// Compile a resolved aggregate's transition and final functions into
/// expressions over the synthetic scope.
fn compile(
    kv: &dyn Kv,
    routine: &Routine,
    direct_args: &[Expr],
    ordered_args: &[Expr],
    given: &[ColumnType],
) -> Result<UserAggregate, ExecError> {
    let definition = routine.aggregate.as_ref().ok_or_else(|| {
        ExecError::Unsupported(format!("{} has no definition", routine.identity()))
    })?;
    // `definition.unimplemented` is not consulted: every option it holds is a
    // performance or parallelism hint that cannot change the answer.
    let state_type = resolve_state_type(routine, definition, given)?;
    let transition_given = if definition.ordered_args == 0 {
        given
    } else {
        &given[definition.direct_args..]
    };
    let mut argument_types = vec![state_type];
    argument_types.extend_from_slice(transition_given);
    let transition_scope = synthetic_scope(&argument_types);
    let transition = compile_call(
        kv,
        &definition.transfn,
        &synthetic_args(argument_types.len()),
        &transition_scope,
    )?;
    let mut final_types = vec![state_type];
    if definition.finalfunc_extra {
        final_types.extend_from_slice(given);
    }
    let final_scope = synthetic_scope(&final_types);
    let final_expr = definition
        .finalfn
        .as_ref()
        .map(|name| compile_call(kv, name, &synthetic_args(final_types.len()), &final_scope))
        .transpose()?;
    let result_type = match &final_expr {
        Some(expr) => crate::eval::infer_type(expr, &final_scope)?,
        None => state_type,
    };
    let moving = definition
        .moving_transfn
        .as_ref()
        .map(|name| {
            let state_type = definition.moving_transtype.as_ref().map_or_else(
                || Ok(state_type),
                |ty| resolve_declared_state_type(routine, ty, given),
            )?;
            let mut types = vec![state_type];
            types.extend_from_slice(transition_given);
            let transition_scope = synthetic_scope(&types);
            let transition =
                compile_call(kv, name, &synthetic_args(types.len()), &transition_scope)?;
            let mut final_types = vec![state_type];
            if definition.moving_finalfunc_extra {
                final_types.extend_from_slice(given);
            }
            let final_scope = synthetic_scope(&final_types);
            let final_expr = definition
                .moving_finalfn
                .as_ref()
                .map(|name| {
                    compile_call(kv, name, &synthetic_args(final_types.len()), &final_scope)
                })
                .transpose()?;
            Ok::<MovingAggregate, ExecError>(MovingAggregate {
                transition,
                transition_scope,
                final_expr,
                final_scope,
                state_type,
                initcond: definition.moving_initcond.clone(),
                initcond_specified: definition.moving_initcond_specified,
                strict_transition: transition_is_strict(kv, name, &types),
                finalfunc_extra: definition.moving_finalfunc_extra,
            })
        })
        .transpose()?;
    Ok(UserAggregate {
        args: if definition.ordered_args == 0 {
            direct_args.to_vec()
        } else {
            ordered_args.to_vec()
        },
        direct_args: (definition.ordered_args > 0)
            .then(|| direct_args.to_vec())
            .unwrap_or_default(),
        transition,
        transition_scope,
        final_expr,
        final_scope,
        finalfunc_extra: definition.finalfunc_extra,
        ordered_args: definition.ordered_args,
        state_type,
        result_type,
        initcond: definition.initcond.clone(),
        strict_transition: transition_is_strict(kv, &definition.transfn, &argument_types),
        moving,
    })
}

/// The state type this call runs with: the declared one, or — when it is
/// polymorphic — the shape the call's own argument types pin it to.
fn resolve_state_type(
    routine: &Routine,
    definition: &AggregateDefinition,
    given: &[ColumnType],
) -> Result<ColumnType, ExecError> {
    resolve_declared_state_type(routine, &definition.transtype, given)
}

fn resolve_declared_state_type(
    routine: &Routine,
    declared: &RoutineType,
    given: &[ColumnType],
) -> Result<ColumnType, ExecError> {
    if let Some(column) = declared.column {
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
    Ok(match polymorphic_shape(&declared.name) {
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

fn transition_is_strict(kv: &dyn Kv, name: &str, types: &[ColumnType]) -> bool {
    let wanted = types
        .iter()
        .copied()
        .map(RoutineType::builtin)
        .collect::<Vec<_>>();
    lookup(kv, name, &wanted).is_ok_and(|support| support.strict())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crabka_pgkv::{Kv, MemKv};

    use super::*;

    fn aggregate(name: &str, direct_args: usize, ordered_args: usize) -> Routine {
        Routine {
            oid: 1,
            name: name.into(),
            kind: RoutineKind::Aggregate,
            params: Vec::new(),
            result: RoutineResult::Unspecified,
            language: "internal".into(),
            body: "aggregate_dummy".into(),
            object_file: None,
            body_form: crabka_pgcatalog::routine::BodyForm::Source,
            volatility: 'i',
            parallel: 'u',
            window: false,
            strict: false,
            security_definer: false,
            leakproof: false,
            cost: 1.0,
            rows: 0.0,
            config: Vec::new(),
            config_source: Vec::new(),
            owner: "postgres".into(),
            aggregate: Some(AggregateDefinition {
                kind: 'n',
                transfn: "int8inc".into(),
                transtype: RoutineType::builtin(ColumnType::Int8),
                finalfn: None,
                combinefn: None,
                serialfn: None,
                deserialfn: None,
                finalfunc_modify: 'r',
                initcond: Some("0".into()),
                moving_transfn: None,
                moving_invtransfn: None,
                moving_transtype: None,
                moving_finalfn: None,
                moving_initcond: None,
                moving_initcond_specified: false,
                moving_finalfunc_extra: false,
                direct_args,
                ordered_args,
                finalfunc_extra: false,
                hypothetical: false,
                unimplemented: Vec::new(),
            }),
        }
    }

    #[test]
    fn within_group_classifies_only_ordered_set_aggregates() {
        let kv = MemKv::default();
        kv.write_batch(&put_routine_ops(&kv, &aggregate("plain", 0, 0)).expect("ops"))
            .expect("writes");
        kv.write_batch(&put_routine_ops(&kv, &aggregate("ordered", 1, 1)).expect("ops"))
            .expect("writes");
        let catalog: Arc<dyn Kv> = Arc::new(kv);

        crate::routine::with_scalar_runtime(&catalog, None, || {
            assert!(exists_with_call("plain", 0, false));
            assert!(!exists_with_call("plain", 0, true));
            assert!(exists_with_call("ordered", 1, true));
        });
    }

    #[test]
    fn builtin_support_binds_an_element_before_its_compatible_array() {
        assert!(builtin_support_args_match(
            &[Datum::Int4(5077), Datum::Int4(5078)],
            &[
                RoutineType::builtin(ColumnType::Int4),
                RoutineType::builtin(ColumnType::Array(ElemType::Int4)),
            ],
        ));
    }

    #[test]
    fn builtin_support_rejects_mismatched_declared_argument_types() {
        assert!(!builtin_support_args_match(
            &[Datum::Int4(701), Datum::Int4(701)],
            &[
                RoutineType::builtin(ColumnType::Float8),
                RoutineType::builtin(ColumnType::Int4),
            ],
        ));
        assert!(!builtin_support_args_match(
            &[Datum::Int4(5077), Datum::Int4(5078)],
            &[
                RoutineType::builtin(ColumnType::Int4),
                RoutineType::builtin(ColumnType::Array(ElemType::Int8)),
            ],
        ));
        assert!(!builtin_support_args_match(
            &[Datum::Int4(5078), Datum::Int4(5077)],
            &[
                RoutineType::builtin(ColumnType::Array(ElemType::Int8)),
                RoutineType::builtin(ColumnType::Int4),
            ],
        ));
    }

    #[test]
    fn aggregate_kind_words_name_each_postgres_aggregate_family() {
        assert_eq!(aggregate_kind_word('n'), "ordinary aggregate function");
        assert_eq!(aggregate_kind_word('o'), "ordered-set aggregate function");
        assert_eq!(
            aggregate_kind_word('h'),
            "hypothetical-set aggregate function"
        );
    }

    #[test]
    fn builtin_support_resolves_float8_addition() {
        let wanted = [
            RoutineType::builtin(ColumnType::Float8),
            RoutineType::builtin(ColumnType::Float8),
        ];
        assert!(crate::func::is_scalar("float8pl"));
        assert!(builtin_support_args_match(
            &[Datum::Int4(701), Datum::Int4(701)],
            &wanted,
        ));
        assert!(
            builtin_support("float8pl", &wanted)
                .expect("catalog")
                .is_some()
        );
    }
}

/// Turn a support-function call into an expression over `scope`: an inlined SQL
/// body where the routine model can inline one, and the call node itself
/// otherwise, which is what routes a `plpgsql` body through the scalar runtime.
fn compile_call(kv: &dyn Kv, name: &str, args: &[Expr], scope: &Scope) -> Result<Expr, ExecError> {
    let call = FuncCall {
        sql_syntax: false,
        name: name.to_string(),
        distinct: false,
        args: FuncArgs::Exprs(args.to_vec()),
        order_by: Vec::new(),
        within_group: false,
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
    let builtin_routines = crate::routine::builtin_pg_proc_rows()?;
    let regproc_of = |name: Option<&str>| -> Datum {
        let Some(name) = name else {
            return Datum::Regclass(crabka_pgtypes::RegclassValue::unresolved(0));
        };
        if let Some(routine) = routines
            .iter()
            .find(|routine| routine.name == name && !routine.is_aggregate())
        {
            return Datum::Regclass(crabka_pgtypes::RegclassValue::resolved(
                i32::try_from(routine.oid).unwrap_or(0),
                &routine.name,
            ));
        }
        builtin_routines
            .iter()
            .find(|routine| routine.get(1) == Some(&Datum::Text(name.to_string())))
            .and_then(|routine| match (routine.first(), routine.get(1)) {
                (Some(Datum::Int4(oid)), Some(Datum::Text(name))) => Some((*oid, name.clone())),
                _ => None,
            })
            .map_or_else(
                || Datum::Regclass(crabka_pgtypes::RegclassValue::unresolved(0)),
                |(oid, name)| Datum::Regclass(crabka_pgtypes::RegclassValue::resolved(oid, &name)),
            )
    };
    Ok(routines
        .iter()
        .filter(|routine| routine.is_aggregate())
        .filter_map(|routine| {
            let definition = routine.aggregate.as_ref()?;
            let transtype = crate::routine::type_oid(&definition.transtype);
            Some(vec![
                Datum::Int4(i32::try_from(routine.oid).unwrap_or(0)),
                Datum::Text(definition.kind.to_string()),
                Datum::Int2(i16::try_from(definition.direct_args).unwrap_or(0)),
                regproc_of(Some(&definition.transfn)),
                regproc_of(definition.finalfn.as_deref()),
                regproc_of(definition.combinefn.as_deref()),
                regproc_of(definition.serialfn.as_deref()),
                regproc_of(definition.deserialfn.as_deref()),
                regproc_of(None),
                regproc_of(None),
                regproc_of(None),
                Datum::Bool(false),
                Datum::Bool(false),
                Datum::Text(definition.finalfunc_modify.to_string()),
                Datum::Text("r".into()),
                Datum::Int4(0),
                Datum::Regclass(crabka_pgtypes::RegclassValue::resolved(
                    transtype,
                    &definition.transtype.name,
                )),
                Datum::Int4(0),
                Datum::Regclass(crabka_pgtypes::RegclassValue::unresolved(0)),
                Datum::Int4(0),
                definition.initcond.clone().map_or(Datum::Null, Datum::Text),
                Datum::Null,
            ])
        })
        .collect())
}
