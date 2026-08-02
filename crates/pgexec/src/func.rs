//! SP29: scalar (row) functions + the `||` concatenation operator.
//!
//! Like SP27's aggregates and SP28's predicates, every function here is a pure,
//! deterministic transform over a *single row's* already-MVCC-resolved Datums.
//! A whole table lives on one range (`RangeMap::range_for_table`), so a scalar
//! function executes entirely inside one `execute_read`/`eval` on one engine.
//! There is no cross-range scatter, no new lock/visibility rule, and no new
//! interleaving. This is exactly CLAUDE.md's "pure-data / single-node refactor"
//! carve-out, so SP29 ships NO Stateright model. A model of a scalar fold would
//! have an interleaving-free state space and would only restate these unit
//! tests.
//!
//! The dispatch mirrors SP28: scalar `eval` and the grouped evaluator
//! (`agg::eval_grouped`) share the pure combinators, and only the child-eval
//! closure differs (`eval_scalar` takes it as `FnMut(&Expr) -> Result<Datum>`).
//!
//! Supported: string `length`/`char_length`/`character_length`, `upper`,
//! `lower`, `btrim`/`ltrim`/`rtrim`, `substr`/`substring` (the comma form),
//! `replace`, `concat`; math `abs`, `mod`; null/conditional `coalesce`,
//! `nullif`, `greatest`, `least`. `||` is a binary operator handled in `eval`.

use std::cmp::Ordering;

use crabka_pgparser::ast::{Expr, FuncArgs, FuncCall};
use crabka_pgtypes::{
    ColumnType, Datum, ops,
    usertype::{MultirangeRef, RangeRef},
};

use crate::{clock::EvalCtx, error::ExecError, scope::Scope};

/// The scalar functions SP29 supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScalarFunc {
    Length,
    Upper,
    Lower,
    Btrim,
    Ltrim,
    Rtrim,
    Substr,
    Overlay,
    Replace,
    Concat,
    Abs,
    Mod,
    Coalesce,
    NullIf,
    Greatest,
    Least,
    // SP33: rounding family (type-preserving).
    Floor,
    Ceil,
    Round,
    Trunc,
    Sign,
    // SP33: transcendental family (always float8).
    Sqrt,
    Power,
    Exp,
    Ln,
    Log,
    Pi,
    // SP33: string family.
    Lpad,
    Rpad,
    Left,
    Right,
    Repeat,
    Reverse,
    Strpos,
    Initcap,
    Ascii,
    Chr,
    CurrentSetting,
    SetConfig,
    /// S3: one member of the advisory-lock family. The flags carry the spelling
    /// so the eight `pg_[try_]advisory[_xact]_lock[_shared]` names share one
    /// implementation.
    AdvisoryLock {
        /// `pg_try_advisory_*` returns a boolean and does not wait.
        try_only: bool,
        /// The `_xact` spellings release at transaction end.
        transactional: bool,
        /// The `_shared` spellings take a share lock.
        shared: bool,
    },
    /// S3: `pg_advisory_unlock` / `pg_advisory_unlock_shared`.
    AdvisoryUnlock {
        shared: bool,
    },
    /// S3: `pg_advisory_unlock_all`.
    AdvisoryUnlockAll,
    CurrentDatabase,
    GetDatabaseEncoding,
    CurrentSchema,
    CurrentUser,
    SessionUser,
    Version,
    FormatType,
    /// `pg_typeof(any)`: the argument's resolved type name.
    PgTypeof,
    /// `pg_input_is_valid(text, text)`: would the type's input function accept
    /// this string?
    PgInputIsValid,
    RangeConstructor(RangeRef),
    MultirangeConstructor(MultirangeRef),
    GenericMultirangeConstructor,
    IsEmpty,
    LowerInc,
    LowerInf,
    UpperInc,
    UpperInf,
    RangeContains,
    RangeContainedBy,
    RangeOverlaps,
    RangeAdjacent,
    RangeMinus,
    RangeMerge,
    MultirangePredicate,
    PgTableIsVisible,
    NextVal,
    CurrVal,
    SetVal,
    PgNotify,
}

/// Classify a lowercased function name. The lexer lowercases unquoted idents.
/// `None` means "not a known scalar function". The caller then tries the
/// aggregate path or reports an undefined function.
fn scalar_func(name: &str) -> Option<ScalarFunc> {
    Some(match name {
        "length" | "char_length" | "character_length" => ScalarFunc::Length,
        "upper" => ScalarFunc::Upper,
        "lower" => ScalarFunc::Lower,
        "btrim" => ScalarFunc::Btrim,
        "ltrim" => ScalarFunc::Ltrim,
        "rtrim" => ScalarFunc::Rtrim,
        "substr" | "substring" => ScalarFunc::Substr,
        "overlay" => ScalarFunc::Overlay,
        "replace" => ScalarFunc::Replace,
        "concat" => ScalarFunc::Concat,
        "abs" => ScalarFunc::Abs,
        "mod" => ScalarFunc::Mod,
        "coalesce" => ScalarFunc::Coalesce,
        "nullif" => ScalarFunc::NullIf,
        "greatest" => ScalarFunc::Greatest,
        "least" => ScalarFunc::Least,
        "floor" => ScalarFunc::Floor,
        "ceil" | "ceiling" => ScalarFunc::Ceil,
        "round" => ScalarFunc::Round,
        "trunc" => ScalarFunc::Trunc,
        "sign" => ScalarFunc::Sign,
        "sqrt" => ScalarFunc::Sqrt,
        "power" | "pow" => ScalarFunc::Power,
        "exp" => ScalarFunc::Exp,
        "ln" => ScalarFunc::Ln,
        "log" => ScalarFunc::Log,
        "pi" => ScalarFunc::Pi,
        "lpad" => ScalarFunc::Lpad,
        "rpad" => ScalarFunc::Rpad,
        "left" => ScalarFunc::Left,
        "right" => ScalarFunc::Right,
        "repeat" => ScalarFunc::Repeat,
        "reverse" => ScalarFunc::Reverse,
        "strpos" => ScalarFunc::Strpos,
        "initcap" => ScalarFunc::Initcap,
        "ascii" => ScalarFunc::Ascii,
        "chr" => ScalarFunc::Chr,
        "current_setting" => ScalarFunc::CurrentSetting,
        "set_config" => ScalarFunc::SetConfig,
        "pg_advisory_lock" => ScalarFunc::AdvisoryLock {
            try_only: false,
            transactional: false,
            shared: false,
        },
        "pg_advisory_lock_shared" => ScalarFunc::AdvisoryLock {
            try_only: false,
            transactional: false,
            shared: true,
        },
        "pg_advisory_xact_lock" => ScalarFunc::AdvisoryLock {
            try_only: false,
            transactional: true,
            shared: false,
        },
        "pg_advisory_xact_lock_shared" => ScalarFunc::AdvisoryLock {
            try_only: false,
            transactional: true,
            shared: true,
        },
        "pg_try_advisory_lock" => ScalarFunc::AdvisoryLock {
            try_only: true,
            transactional: false,
            shared: false,
        },
        "pg_try_advisory_lock_shared" => ScalarFunc::AdvisoryLock {
            try_only: true,
            transactional: false,
            shared: true,
        },
        "pg_try_advisory_xact_lock" => ScalarFunc::AdvisoryLock {
            try_only: true,
            transactional: true,
            shared: false,
        },
        "pg_try_advisory_xact_lock_shared" => ScalarFunc::AdvisoryLock {
            try_only: true,
            transactional: true,
            shared: true,
        },
        "pg_advisory_unlock" => ScalarFunc::AdvisoryUnlock { shared: false },
        "pg_advisory_unlock_shared" => ScalarFunc::AdvisoryUnlock { shared: true },
        "pg_advisory_unlock_all" => ScalarFunc::AdvisoryUnlockAll,
        "current_database" => ScalarFunc::CurrentDatabase,
        "getdatabaseencoding" => ScalarFunc::GetDatabaseEncoding,
        "current_schema" => ScalarFunc::CurrentSchema,
        "current_user" => ScalarFunc::CurrentUser,
        "session_user" => ScalarFunc::SessionUser,
        "version" => ScalarFunc::Version,
        "format_type" => ScalarFunc::FormatType,
        "pg_typeof" => ScalarFunc::PgTypeof,
        "pg_input_is_valid" => ScalarFunc::PgInputIsValid,
        "isempty" => ScalarFunc::IsEmpty,
        "lower_inc" => ScalarFunc::LowerInc,
        "lower_inf" => ScalarFunc::LowerInf,
        "upper_inc" => ScalarFunc::UpperInc,
        "upper_inf" => ScalarFunc::UpperInf,
        "range_contains" => ScalarFunc::RangeContains,
        "range_contained_by" => ScalarFunc::RangeContainedBy,
        "range_overlaps" => ScalarFunc::RangeOverlaps,
        "range_adjacent" => ScalarFunc::RangeAdjacent,
        "range_minus" => ScalarFunc::RangeMinus,
        "range_merge" => ScalarFunc::RangeMerge,
        "range_overlaps_multirange"
        | "multirange_overlaps_range"
        | "multirange_overlaps_multirange"
        | "multirange_contains_elem"
        | "multirange_contains_range"
        | "multirange_contains_multirange"
        | "elem_contained_by_multirange"
        | "range_contained_by_multirange"
        | "multirange_contained_by_multirange" => ScalarFunc::MultirangePredicate,
        "pg_table_is_visible" => ScalarFunc::PgTableIsVisible,
        "nextval" => ScalarFunc::NextVal,
        "currval" => ScalarFunc::CurrVal,
        "setval" => ScalarFunc::SetVal,
        "pg_notify" => ScalarFunc::PgNotify,
        "multirange" => ScalarFunc::GenericMultirangeConstructor,
        _ => match ColumnType::from_sql_name(name) {
            Some(ColumnType::Range(range)) => ScalarFunc::RangeConstructor(range),
            Some(ColumnType::Multirange(multirange)) => {
                ScalarFunc::MultirangeConstructor(multirange)
            }
            _ => return None,
        },
    })
}

/// Is `name` a known scalar function? (The dispatch point in `eval`/`infer_type`.)
///
/// The scalar surface spans four modules: this one plus `math_fn`, `string_fn`
/// and `regexp_fn`. No single file then owns hundreds of functions. They share
/// one dispatch point, so `eval`, `infer_type` and `agg::is_wrapping_scalar_func`
/// each need only ask this question once.
pub(crate) fn is_scalar(name: &str) -> bool {
    scalar_func(name).is_some()
        || crate::math_fn::is_math_func(name)
        || crate::string_fn::is_string_func(name)
        || crate::regexp_fn::is_regexp_func(name)
        || crate::text_search_fn::is_text_search_func(name)
}

/// The call a bare, unparenthesised `name` denotes, when `PostgreSQL` reserves
/// that name for a niladic function and does not leave it available as a
/// column reference.
///
/// `SELECT current_schema` is a function call on 18.4, because `CURRENT_SCHEMA`
/// is a keyword in its grammar and the name can never reach an identifier. The
/// lexer here hands it over as an ordinary identifier instead. The other
/// no-paren spellings (`current_date`, `session_user`, `localtimestamp`, …)
/// are already calls when they arrive, so this covers only the ones that are
/// not.
pub(crate) fn niladic_keyword_call(name: &str) -> Option<FuncCall> {
    matches!(name, "current_schema").then(|| FuncCall {
        name: name.to_string(),
        distinct: false,
        args: FuncArgs::Exprs(Vec::new()),
        filter: None,
    })
}

pub(crate) fn undefined_function(name: &str) -> ExecError {
    // `merge_action()` exists, but only inside a MERGE's RETURNING list — the
    // executor rewrites it to a binding there and never reaches this point.
    // Everywhere else PostgreSQL reports the misuse, not a missing function.
    if name.eq_ignore_ascii_case("merge_action") {
        return ExecError::Syntax(
            "MERGE_ACTION() can only be used in the RETURNING list of a MERGE command".into(),
        );
    }
    ExecError::UndefinedFunction(format!("function {name}(...) does not exist"))
}

/// `DISTINCT`/`ALL` has a meaning only for aggregates (PostgreSQL 42809). The
/// parser discards `ALL`, so only an explicit `DISTINCT` reaches here.
fn distinct_not_aggregate(name: &str) -> ExecError {
    ExecError::WrongObjectType(format!(
        "DISTINCT specified, but {name} is not an aggregate function"
    ))
}

/// The positional argument list of a scalar call. `f(*)` is never valid for a
/// scalar function (only `count(*)` is), so it is an undefined-function error.
fn exprs_of(fc: &FuncCall) -> Result<&[Expr], ExecError> {
    match &fc.args {
        FuncArgs::Exprs(v) => Ok(v),
        FuncArgs::Star => Err(undefined_function(&fc.name)),
    }
}

/// Reject the `DISTINCT` modifier (42809) and return the call's argument list.
/// Shared front-door check for both `scalar_result_type` and `eval_scalar`.
pub(crate) fn checked_args(fc: &FuncCall) -> Result<&[Expr], ExecError> {
    if fc.distinct {
        return Err(distinct_not_aggregate(&fc.name));
    }
    exprs_of(fc)
}

/// Statically infer a scalar call's result type, for RowDescription.
///
/// This validates the name and the arity. It also validates the argument types
/// where the result type depends on them, or where the function is strictly
/// typed. A bad name, arity, or argument type is 42883.
///
/// NB: this runs for PROJECTED expressions, through `resolve_projection`. A
/// scalar function that appears only in `WHERE`/`HAVING`/`ORDER BY` runs with no
/// separate type-resolution pass, which is a pre-existing trait of the engine
/// and is also true of arithmetic. So an argument-type misuse THERE surfaces at
/// runtime as 42804, not here as 42883. This per-clause difference is
/// documented.
pub(crate) fn scalar_result_type(fc: &FuncCall, scope: &Scope) -> Result<ColumnType, ExecError> {
    if crate::text_search_fn::is_text_search_func(&fc.name) {
        return crate::text_search_fn::text_search_result_type(fc, scope);
    }
    if crate::math_fn::is_math_func(&fc.name) {
        return crate::math_fn::math_func_result_type(fc, scope);
    }
    if crate::string_fn::is_string_func(&fc.name) {
        return crate::string_fn::string_func_result_type(fc, scope);
    }
    if crate::regexp_fn::is_regexp_func(&fc.name) {
        return crate::regexp_fn::regexp_func_result_type(fc, scope);
    }
    let f = scalar_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let n = args.len();
    match f {
        ScalarFunc::Length => {
            require_arity(fc, n == 1)?;
            if crate::eval::infer_type(&args[0], scope)? != ColumnType::TsVector {
                require_text(&args[0], scope)?;
            }
            Ok(ColumnType::Int4)
        }
        ScalarFunc::Upper | ScalarFunc::Lower => {
            require_arity(fc, n == 1)?;
            let ty = crate::eval::infer_type(&args[0], scope)?;
            match ty {
                ColumnType::Range(range) => Ok(*range.subtype),
                ColumnType::Multirange(multirange) => Ok(*multirange.range.subtype),
                _ => {
                    require_text(&args[0], scope)?;
                    Ok(ColumnType::Text)
                }
            }
        }
        ScalarFunc::Btrim | ScalarFunc::Ltrim | ScalarFunc::Rtrim => {
            require_arity(fc, n == 1 || n == 2)?;
            for a in args {
                require_text(a, scope)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::Substr => {
            require_arity(fc, n == 2 || n == 3)?;
            require_text(&args[0], scope)?;
            // Two shapes share the name, and PostgreSQL tells them apart by the
            // second argument's type: `substring(text, int [, int])` takes a
            // position, `substring(text, text [, text])` takes a pattern.
            if crate::eval::infer_type(&args[1], scope)?.is_string() {
                for a in &args[1..] {
                    require_text(a, scope)?;
                }
            } else {
                for a in &args[1..] {
                    require_int(a, scope)?;
                }
            }
            Ok(ColumnType::Text)
        }
        // `overlay(string, replacement, start [, count])` — the count defaults to
        // the replacement's own length, so the default replaces exactly as many
        // characters as it inserts.
        ScalarFunc::Overlay => {
            require_arity(fc, n == 3 || n == 4)?;
            require_text(&args[0], scope)?;
            require_text(&args[1], scope)?;
            for a in &args[2..] {
                require_int(a, scope)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::Replace => {
            require_arity(fc, n == 3)?;
            for a in args {
                require_text(a, scope)?;
            }
            Ok(ColumnType::Text)
        }
        // `concat` is VARIADIC "any": any number of arguments of any type, but
        // at least one — PostgreSQL has no zero-argument candidate, so a bare
        // `concat()` is 42883.
        ScalarFunc::Concat => {
            if args.is_empty() {
                return Err(undefined_function_spelled(&fc.name, args, scope));
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::Abs => {
            require_arity(fc, n == 1)?;
            // abs preserves the numeric type (int width, or SP30's float8).
            require_numeric(&args[0], scope)
        }
        ScalarFunc::Mod => {
            require_arity(fc, n == 2)?;
            // `mod` has no float8 candidate, so nothing prefers one overload.
            if args.iter().all(is_unknown_literal) {
                return Err(ambiguous_function(&fc.name, n));
            }
            // SP32: mod takes int OR numeric operands (PostgreSQL has no float8 mod);
            // a numeric operand makes the result numeric, else the int promotion.
            let lt = require_int_or_numeric(&args[0], scope)?;
            let rt = require_int_or_numeric(&args[1], scope)?;
            if lt.is_numeric() || rt.is_numeric() {
                Ok(ColumnType::Numeric(None))
            } else {
                Ok(promote(lt, rt))
            }
        }
        ScalarFunc::Coalesce | ScalarFunc::Greatest | ScalarFunc::Least | ScalarFunc::NullIf => {
            require_arity(
                fc,
                if f == ScalarFunc::NullIf {
                    n == 2
                } else {
                    n >= 1
                },
            )?;
            let ty = unify_args(f, args, scope)?;
            // PostgreSQL resolves the common type ignoring `unknown` literals,
            // then coerces each literal to it — at PLAN time, which is why
            // `coalesce(1, 'x')` is 22P02 even though the literal is never the
            // value returned.
            for a in args {
                if let Expr::StringLiteral(s) = a {
                    crabka_pgtypes::cast::cast(
                        &Datum::Text(s.clone()),
                        ty,
                        &jiff::tz::TimeZone::UTC,
                    )?;
                }
            }
            Ok(ty)
        }
        ScalarFunc::Floor | ScalarFunc::Ceil | ScalarFunc::Sign => {
            require_arity(fc, n == 1)?;
            // preserves the input numeric type (int2/int4/int8/numeric); `real`
            // has no overload of its own, so it resolves to the `float8` one.
            let t = require_numeric(&args[0], scope)?;
            Ok(float4_widens(t))
        }
        ScalarFunc::Round | ScalarFunc::Trunc => {
            require_arity(fc, n == 1 || n == 2)?;
            // `trunc` also covers `macaddr`, so — unlike `round` — it has no
            // preferred candidate to settle an all-`unknown` call.
            if f == ScalarFunc::Trunc && args.iter().all(is_unknown_literal) {
                return Err(ambiguous_function(&fc.name, n));
            }
            if n == 1 {
                let t = require_numeric(&args[0], scope)?;
                Ok(float4_widens(t))
            } else {
                // two-arg: numeric (or int promoted to numeric) first arg, int
                // second arg, → numeric. Neither float width has a 2-arg form,
                // so the scale argument is what makes an `unknown` value
                // resolve to `numeric` rather than the usual `float8`.
                if !is_unknown_literal(&args[0]) {
                    let t0 = float4_widens(require_numeric(&args[0], scope)?);
                    if t0 == ColumnType::Float8 {
                        return Err(no_matching_function());
                    }
                }
                require_int_or_null(&args[1], scope)?;
                Ok(ColumnType::Numeric(None))
            }
        }
        ScalarFunc::Sqrt | ScalarFunc::Exp | ScalarFunc::Ln | ScalarFunc::Log => {
            require_arity(fc, n == 1)?;
            let at = require_numeric(&args[0], scope)?;
            Ok(if at.is_numeric() {
                ColumnType::Numeric(None)
            } else {
                ColumnType::Float8
            })
        }
        ScalarFunc::Power => {
            require_arity(fc, n == 2)?;
            let a = require_numeric(&args[0], scope)?;
            let b = require_numeric(&args[1], scope)?;
            Ok(power_result_type(a, b))
        }
        ScalarFunc::Pi => {
            require_arity(fc, n == 0)?;
            Ok(ColumnType::Float8)
        }
        ScalarFunc::Lpad | ScalarFunc::Rpad => {
            require_arity(fc, n == 2 || n == 3)?;
            require_text(&args[0], scope)?;
            require_int(&args[1], scope)?;
            if n == 3 {
                require_text(&args[2], scope)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::Left | ScalarFunc::Right | ScalarFunc::Repeat => {
            require_arity(fc, n == 2)?;
            require_text(&args[0], scope)?;
            require_int(&args[1], scope)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::Reverse | ScalarFunc::Initcap => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], scope)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::Strpos => {
            require_arity(fc, n == 2)?;
            require_text(&args[0], scope)?;
            require_text(&args[1], scope)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::Ascii => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], scope)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::Chr => {
            require_arity(fc, n == 1)?;
            require_int(&args[0], scope)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::CurrentSetting => {
            require_arity(fc, n == 1 || n == 2)?;
            require_text(&args[0], scope)?;
            if n == 2 {
                require_bool(&args[1], scope)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::SetConfig => {
            require_arity(fc, n == 3)?;
            require_text(&args[0], scope)?;
            require_text(&args[1], scope)?;
            require_bool(&args[2], scope)?;
            Ok(ColumnType::Text)
        }
        // The waiting spellings return `void`; Gres reports it as text (the
        // same mapping `pg_notify` already documents), the `try_`/unlock
        // spellings return boolean exactly like PostgreSQL.
        ScalarFunc::AdvisoryLock { try_only, .. } => {
            require_arity(fc, n == 1 || n == 2)?;
            for arg in args.iter().take(n) {
                // An untyped NULL resolves against the `int8`/`(int4, int4)`
                // overloads exactly as any other unknown literal does, so it is
                // a strict-NULL call rather than a no-such-function error.
                require_int_or_null(arg, scope)?;
            }
            Ok(if try_only {
                ColumnType::Bool
            } else {
                ColumnType::Text
            })
        }
        ScalarFunc::AdvisoryUnlock { .. } => {
            require_arity(fc, n == 1 || n == 2)?;
            for arg in args.iter().take(n) {
                require_int_or_null(arg, scope)?;
            }
            Ok(ColumnType::Bool)
        }
        ScalarFunc::AdvisoryUnlockAll => {
            require_arity(fc, n == 0)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::CurrentDatabase
        | ScalarFunc::GetDatabaseEncoding
        | ScalarFunc::CurrentSchema
        | ScalarFunc::CurrentUser
        | ScalarFunc::SessionUser
        | ScalarFunc::Version => {
            require_arity(fc, n == 0)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::FormatType => {
            require_arity(fc, n == 2)?;
            require_int_or_null(&args[0], scope)?;
            require_int_or_null(&args[1], scope)?;
            Ok(ColumnType::Text)
        }
        // `pg_typeof` reports `regtype` in PostgreSQL; crabka has no regtype
        // column type, so it reports `text`. The value is identical — a regtype
        // renders as the type's name — but the RowDescription OID is 25, not
        // 2206. Documented divergence, shared with `pg_notify`'s void.
        ScalarFunc::PgTypeof => {
            require_arity(fc, n == 1)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::PgInputIsValid => {
            require_arity(fc, n == 2)?;
            require_text(&args[0], scope)?;
            require_text(&args[1], scope)?;
            Ok(ColumnType::Bool)
        }
        ScalarFunc::RangeConstructor(range) => {
            require_arity(fc, (1..=3).contains(&n))?;
            Ok(ColumnType::Range(range))
        }
        ScalarFunc::MultirangeConstructor(multirange) => Ok(ColumnType::Multirange(multirange)),
        ScalarFunc::GenericMultirangeConstructor => {
            require_arity(fc, n == 1)?;
            let ColumnType::Range(range) = crate::eval::infer_type(&args[0], scope)? else {
                return Err(no_matching_function());
            };
            ColumnType::multirange_for_range(range).ok_or_else(no_matching_function)
        }
        ScalarFunc::IsEmpty
        | ScalarFunc::LowerInc
        | ScalarFunc::LowerInf
        | ScalarFunc::UpperInc
        | ScalarFunc::UpperInf => {
            require_arity(fc, n == 1)?;
            if !matches!(
                crate::eval::infer_type(&args[0], scope)?,
                ColumnType::Range(_) | ColumnType::Multirange(_)
            ) {
                return Err(no_matching_function());
            }
            Ok(ColumnType::Bool)
        }
        ScalarFunc::RangeContains
        | ScalarFunc::RangeContainedBy
        | ScalarFunc::RangeOverlaps
        | ScalarFunc::RangeAdjacent => {
            require_arity(fc, n == 2)?;
            Ok(ColumnType::Bool)
        }
        ScalarFunc::MultirangePredicate => {
            require_arity(fc, n == 2)?;
            Ok(ColumnType::Bool)
        }
        ScalarFunc::RangeMinus | ScalarFunc::RangeMerge => {
            if matches!(scalar_func(&fc.name), Some(ScalarFunc::RangeMerge))
                && n == 1
                && matches!(
                    crate::eval::infer_type(&args[0], scope)?,
                    ColumnType::Multirange(_)
                )
            {
                let ColumnType::Multirange(multirange) = crate::eval::infer_type(&args[0], scope)?
                else {
                    unreachable!()
                };
                return Ok(ColumnType::Range(multirange.range));
            }
            require_arity(fc, n == 2)?;
            let left = crate::eval::infer_type(&args[0], scope)?;
            let right = crate::eval::infer_type(&args[1], scope)?;
            if matches!((left, right), (ColumnType::Range(a), ColumnType::Range(b)) if a == b) {
                Ok(left)
            } else {
                Err(no_matching_function())
            }
        }
        ScalarFunc::PgTableIsVisible => {
            require_arity(fc, n == 1)?;
            require_int(&args[0], scope)?;
            Ok(ColumnType::Bool)
        }
        ScalarFunc::NextVal | ScalarFunc::CurrVal => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], scope)?;
            Ok(ColumnType::Int8)
        }
        ScalarFunc::SetVal => {
            require_arity(fc, n == 2 || n == 3)?;
            require_text(&args[0], scope)?;
            require_int(&args[1], scope)?;
            if n == 3 {
                require_bool(&args[2], scope)?;
            }
            Ok(ColumnType::Int8)
        }
        // PostgreSQL's `pg_notify` returns `void`, which crabka has no column
        // type for; it reports `text` and evaluates to the empty string, which
        // renders identically to `void` on the wire. Documented divergence: the
        // RowDescription type OID is 25, not 2278.
        ScalarFunc::PgNotify => {
            require_arity(fc, n == 2)?;
            require_text(&args[0], scope)?;
            require_text(&args[1], scope)?;
            Ok(ColumnType::Text)
        }
    }
}

/// Evaluate a scalar call. `eval_child` evaluates each argument expression
/// against the current row. It is the SAME `eval` for a scalar context and
/// `agg::eval_grouped` for a grouped context, so the two share the combinators
/// and only the closure differs. Short-circuiting functions (`coalesce`) and
/// the lazy ones evaluate arguments only as far as they need to.
pub(crate) fn eval_scalar(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    if crate::text_search_fn::is_text_search_func(&fc.name) {
        return crate::text_search_fn::eval_text_search(fc, ctx, eval_child);
    }
    if crate::math_fn::is_math_func(&fc.name) {
        return crate::math_fn::eval_math(fc, ctx, eval_child);
    }
    if crate::string_fn::is_string_func(&fc.name) {
        return crate::string_fn::eval_string(fc, ctx, eval_child);
    }
    if crate::regexp_fn::is_regexp_func(&fc.name) {
        return crate::regexp_fn::eval_regexp(fc, ctx, eval_child);
    }
    let f = scalar_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    match f {
        // coalesce returns the first non-NULL argument, NOT evaluating the rest
        // (so `coalesce(x, 1/0)` with x non-null never divides by zero).
        ScalarFunc::Coalesce => {
            require_arity(fc, !args.is_empty())?;
            for a in args {
                let v = eval_child(a)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            Ok(Datum::Null)
        }
        // `pg_typeof` reports its argument's resolved type. Crabka resolves that
        // from the evaluated Datum (plus the cast, when the expression is one),
        // because the scalar evaluator has no scope to re-infer from: a NULL
        // value whose type comes from a *column* therefore reports `unknown`
        // where PostgreSQL reports the column's type. Documented divergence.
        ScalarFunc::PgTypeof => {
            require_arity(fc, args.len() == 1)?;
            let value = eval_child(&args[0])?;
            Ok(Datum::Text(typeof_name(&args[0], &value)))
        }
        ScalarFunc::Greatest | ScalarFunc::Least => {
            require_arity(fc, !args.is_empty())?;
            let want_greater = matches!(f, ScalarFunc::Greatest);
            let vals = resolved_args(args, ctx, &mut eval_child)?;
            let mut best: Option<Datum> = None;
            for v in vals {
                if v.is_null() {
                    continue; // greatest/least ignore NULL arguments
                }
                best = Some(match best {
                    None => v,
                    Some(cur) => {
                        let replace = match ops::compare(&v, &cur)? {
                            Some(Ordering::Greater) => want_greater,
                            Some(Ordering::Less) => !want_greater,
                            _ => false, // Equal (both non-null, so never None)
                        };
                        if replace { v } else { cur }
                    }
                });
            }
            Ok(best.unwrap_or(Datum::Null))
        }
        ScalarFunc::NullIf => {
            require_arity(fc, args.len() == 2)?;
            let vals = resolved_args(args, ctx, &mut eval_child)?;
            let [a, b] = vals.as_slice() else {
                return Err(undefined_function(&fc.name));
            };
            let (a, b) = (a.clone(), b.clone());
            // NULLIF(a, b) = NULL when a = b, else a. `compare` is None if either
            // is NULL (so a NULL `a` falls through to `Ok(a)` = NULL).
            match ops::compare(&a, &b)? {
                Some(Ordering::Equal) => Ok(Datum::Null),
                _ => Ok(a),
            }
        }
        // `format_type` is NOT strict in its typmod: a NULL there means "no
        // modifier", so only a NULL OID yields NULL.
        ScalarFunc::FormatType => {
            require_arity(fc, args.len() == 2)?;
            let oid = eval_child(&args[0])?;
            let typmod = eval_child(&args[1])?;
            if oid.is_null() {
                return Ok(Datum::Null);
            }
            let typmod = match &typmod {
                Datum::Null => -1,
                other => int_arg(other)?,
            };
            Ok(Datum::Text(format_type(int_arg(&oid)?, typmod)))
        }
        // `pg_notify` is NOT strict: PostgreSQL substitutes the empty string for
        // a NULL channel or payload, so `pg_notify(NULL, 'x')` raises the same
        // empty-channel error as `pg_notify('', 'x')` rather than returning NULL.
        ScalarFunc::PgNotify => {
            require_arity(fc, args.len() == 2)?;
            let channel = eval_child(&args[0])?;
            let payload = eval_child(&args[1])?;
            eval_pg_notify(&channel, &payload, ctx)
        }
        // Eager, strict-or-concat functions: evaluate every argument first.
        _ => {
            let mut vals = args
                .iter()
                .map(&mut eval_child)
                .collect::<Result<Vec<_>, _>>()?;
            coerce_unknown_numeric_args(f, args, &mut vals, ctx)?;
            eval_eager(f, fc, &vals, ctx)
        }
    }
}

/// Coerce `unknown` literal arguments of the numeric-family scalar functions
/// into the type the call resolved to, so `sqrt('4')` and `mod('9', 4)` compute
/// and do not complain about a text argument. PostgreSQL does this coercion at
/// plan time. The scalar evaluator has no scope, so it re-derives the target
/// from the arguments that DID carry a type.
fn coerce_unknown_numeric_args(
    f: ScalarFunc,
    args: &[Expr],
    vals: &mut [Datum],
    ctx: &EvalCtx,
) -> Result<(), ExecError> {
    if !args.iter().any(is_unknown_literal) {
        return Ok(());
    }
    let target = match f {
        // The rounding pair's two-argument form is `numeric(value, int)`; its
        // one-argument form has a preferred `float8` candidate like the rest.
        ScalarFunc::Round | ScalarFunc::Trunc if args.len() == 2 => ColumnType::Numeric(None),
        ScalarFunc::Abs
        | ScalarFunc::Floor
        | ScalarFunc::Ceil
        | ScalarFunc::Round
        | ScalarFunc::Trunc
        | ScalarFunc::Sign
        | ScalarFunc::Sqrt
        | ScalarFunc::Power
        | ScalarFunc::Exp
        | ScalarFunc::Ln
        | ScalarFunc::Log => ColumnType::Float8,
        // `mod` has no float8 candidate, so a typed operand picks the overload.
        ScalarFunc::Mod => args
            .iter()
            .zip(vals.iter())
            .filter(|(a, _)| !is_unknown_literal(a))
            .find_map(|(_, v)| v.column_type())
            .unwrap_or(ColumnType::Numeric(None)),
        _ => return Ok(()),
    };
    for (i, (a, v)) in args.iter().zip(vals.iter_mut()).enumerate() {
        if !is_unknown_literal(a) || v.is_null() {
            continue;
        }
        // The rounding pair's second argument is always the int scale.
        let to = if i == 1 && matches!(f, ScalarFunc::Round | ScalarFunc::Trunc) {
            ColumnType::Int4
        } else {
            target
        };
        *v = crabka_pgtypes::cast::cast(v, to, &ctx.time_zone)?;
    }
    Ok(())
}

/// Apply an eager scalar function to its already-evaluated arguments. Every
/// function here except `concat` is strict, and returns NULL for any NULL
/// argument. `concat` skips NULLs and never returns NULL.
fn eval_eager(
    f: ScalarFunc,
    fc: &FuncCall,
    vals: &[Datum],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    if let ScalarFunc::Concat = f {
        // `concat` renders each argument via its canonical wire text encoding,
        // using the session zone from `ctx` (so `Timestamptz` agrees with DataRow).
        let tz = &ctx.time_zone;
        let mut s = String::new();
        for v in vals {
            if !v.is_null() {
                s.push_str(&text_render(v, tz));
            }
        }
        return Ok(Datum::Text(s));
    }
    if let ScalarFunc::RangeConstructor(range) = f {
        return eval_range_constructor(range, fc, vals, ctx);
    }
    if let ScalarFunc::MultirangeConstructor(multirange) = f {
        let ranges = vals
            .iter()
            .map(|value| {
                crabka_pgtypes::cast::cast(
                    value,
                    ColumnType::Range(multirange.range),
                    &ctx.time_zone,
                )
                .and_then(|value| match value {
                    Datum::Range(range) => Ok(range),
                    _ => unreachable!(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        return crabka_pgtypes::multirange::from_ranges(multirange, ranges)
            .map(Datum::Multirange)
            .map_err(ExecError::from);
    }
    if let ScalarFunc::GenericMultirangeConstructor = f {
        let [Datum::Range(range)] = vals else {
            return Err(undefined_function(&fc.name));
        };
        let ColumnType::Multirange(ty) = ColumnType::multirange_for_range(range.ty)
            .ok_or_else(|| undefined_function(&fc.name))?
        else {
            unreachable!()
        };
        return crabka_pgtypes::multirange::from_ranges(ty, vec![range.clone()])
            .map(Datum::Multirange)
            .map_err(ExecError::from);
    }
    // Strict: a NULL argument short-circuits to NULL.
    if vals.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    if let (ScalarFunc::RangeMerge, [Datum::Multirange(multirange)]) = (f, vals) {
        return Ok(Datum::Range(
            match (multirange.ranges.first(), multirange.ranges.last()) {
                (Some(first), Some(last)) => crabka_pgtypes::range::merge(first, last)?,
                _ => crabka_pgtypes::RangeValue {
                    ty: multirange.ty.range,
                    lower: None,
                    upper: None,
                    lower_inclusive: false,
                    upper_inclusive: false,
                    empty: true,
                },
            },
        ));
    }
    match f {
        ScalarFunc::Length => {
            require_arity(fc, vals.len() == 1)?;
            let n = match &vals[0] {
                Datum::TsVector(vector) => vector.len(),
                value => text_arg(value)?.chars().count(),
            };
            i32::try_from(n)
                .map(Datum::Int4)
                .map_err(|_| ExecError::Type(crabka_pgtypes::TypeError::Overflow))
        }
        ScalarFunc::Upper => {
            require_arity(fc, vals.len() == 1)?;
            match &vals[0] {
                Datum::Range(range) => Ok(range
                    .upper
                    .as_deref()
                    .filter(|_| !range.empty)
                    .cloned()
                    .unwrap_or(Datum::Null)),
                Datum::Multirange(multirange) => Ok(multirange
                    .ranges
                    .last()
                    .and_then(|range| range.upper.as_deref())
                    .cloned()
                    .unwrap_or(Datum::Null)),
                value => Ok(Datum::Text(text_arg(value)?.to_uppercase())),
            }
        }
        ScalarFunc::Lower => {
            require_arity(fc, vals.len() == 1)?;
            match &vals[0] {
                Datum::Range(range) => Ok(range
                    .lower
                    .as_deref()
                    .filter(|_| !range.empty)
                    .cloned()
                    .unwrap_or(Datum::Null)),
                Datum::Multirange(multirange) => Ok(multirange
                    .ranges
                    .first()
                    .and_then(|range| range.lower.as_deref())
                    .cloned()
                    .unwrap_or(Datum::Null)),
                value => Ok(Datum::Text(text_arg(value)?.to_lowercase())),
            }
        }
        ScalarFunc::IsEmpty
        | ScalarFunc::LowerInc
        | ScalarFunc::LowerInf
        | ScalarFunc::UpperInc
        | ScalarFunc::UpperInf => {
            require_arity(fc, vals.len() == 1)?;
            let value = match &vals[0] {
                Datum::Range(range) => match f {
                    ScalarFunc::IsEmpty => range.empty,
                    ScalarFunc::LowerInc => !range.empty && range.lower_inclusive,
                    ScalarFunc::LowerInf => !range.empty && range.lower.is_none(),
                    ScalarFunc::UpperInc => !range.empty && range.upper_inclusive,
                    ScalarFunc::UpperInf => !range.empty && range.upper.is_none(),
                    _ => unreachable!(),
                },
                Datum::Multirange(multirange) => match f {
                    ScalarFunc::IsEmpty => multirange.ranges.is_empty(),
                    ScalarFunc::LowerInc => multirange
                        .ranges
                        .first()
                        .is_some_and(|range| range.lower_inclusive),
                    ScalarFunc::LowerInf => multirange
                        .ranges
                        .first()
                        .is_some_and(|range| range.lower.is_none()),
                    ScalarFunc::UpperInc => multirange
                        .ranges
                        .last()
                        .is_some_and(|range| range.upper_inclusive),
                    ScalarFunc::UpperInf => multirange
                        .ranges
                        .last()
                        .is_some_and(|range| range.upper.is_none()),
                    _ => unreachable!(),
                },
                value => return Err(type_error(&fc.name, value)),
            };
            Ok(Datum::Bool(value))
        }
        ScalarFunc::RangeContains
        | ScalarFunc::RangeContainedBy
        | ScalarFunc::RangeOverlaps
        | ScalarFunc::RangeAdjacent
        | ScalarFunc::RangeMinus
        | ScalarFunc::RangeMerge => {
            require_arity(fc, vals.len() == 2)?;
            let (Datum::Range(left), Datum::Range(right)) = (&vals[0], &vals[1]) else {
                return Err(type_error(&fc.name, &vals[0]));
            };
            Ok(match f {
                ScalarFunc::RangeContains => {
                    Datum::Bool(crabka_pgtypes::range::contains_range(left, right)?)
                }
                ScalarFunc::RangeContainedBy => {
                    Datum::Bool(crabka_pgtypes::range::contains_range(right, left)?)
                }
                ScalarFunc::RangeOverlaps => {
                    Datum::Bool(crabka_pgtypes::range::overlaps(left, right)?)
                }
                ScalarFunc::RangeAdjacent => {
                    Datum::Bool(crabka_pgtypes::range::adjacent(left, right)?)
                }
                ScalarFunc::RangeMinus => {
                    Datum::Range(crabka_pgtypes::range::difference(left, right)?)
                }
                ScalarFunc::RangeMerge => Datum::Range(crabka_pgtypes::range::merge(left, right)?),
                _ => unreachable!(),
            })
        }
        ScalarFunc::MultirangePredicate => eval_multirange_predicate(&fc.name, vals),
        ScalarFunc::Btrim | ScalarFunc::Ltrim | ScalarFunc::Rtrim => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let s = text_arg(&vals[0])?;
            // The optional second argument is the set of characters to strip
            // (default: ASCII/Unicode whitespace).
            let trimmed = match vals.get(1) {
                None => trim_ws(f, s),
                Some(chars) => {
                    let set: Vec<char> = text_arg(chars)?.chars().collect();
                    trim_set(f, s, &set)
                }
            };
            Ok(Datum::Text(trimmed))
        }
        ScalarFunc::Substr => {
            require_arity(fc, vals.len() == 2 || vals.len() == 3)?;
            let s = text_arg(&vals[0])?;
            // The pattern forms, distinguished at runtime the same way the plan
            // gate distinguishes them: a text second argument is a regexp.
            if let Datum::Text(pattern) = &vals[1] {
                return match vals.get(2) {
                    // `SUBSTRING(s SIMILAR pattern ESCAPE esc)`.
                    Some(escape) => {
                        let escape = text_arg(escape)?.chars().next();
                        Ok(crate::pattern::similar_substring(s, pattern, escape)?
                            .map_or(Datum::Null, Datum::Text))
                    }
                    // `SUBSTRING(s FROM posix_pattern)`.
                    None => posix_substring(s, pattern),
                };
            }
            let start = int_arg(&vals[1])?;
            let count = match vals.get(2) {
                None => None,
                Some(c) => Some(int_arg(c)?),
            };
            substr(s, start, count)
        }
        ScalarFunc::Overlay => {
            require_arity(fc, vals.len() == 3 || vals.len() == 4)?;
            let s = text_arg(&vals[0])?;
            let replacement = text_arg(&vals[1])?;
            let start = int_arg(&vals[2])?;
            let count = match vals.get(3) {
                Some(c) => int_arg(c)?,
                None => replacement.chars().count() as i64,
            };
            overlay(s, replacement, start, count)
        }
        ScalarFunc::Replace => {
            require_arity(fc, vals.len() == 3)?;
            let (s, from, to) = (
                text_arg(&vals[0])?,
                text_arg(&vals[1])?,
                text_arg(&vals[2])?,
            );
            // PostgreSQL `replace` leaves the string unchanged when `from` is empty.
            let out = if from.is_empty() {
                s.to_string()
            } else {
                s.replace(from, to)
            };
            Ok(Datum::Text(out))
        }
        ScalarFunc::Abs => {
            require_arity(fc, vals.len() == 1)?;
            match &vals[0] {
                // `abs((-32768)::int2)` has no int2 result — 22003, like PostgreSQL.
                Datum::Int2(n) => n.checked_abs().map(Datum::Int2).ok_or_else(|| {
                    ExecError::Type(crabka_pgtypes::TypeError::out_of_range_for("smallint"))
                }),
                Datum::Int4(n) => n
                    .checked_abs()
                    .map(Datum::Int4)
                    .ok_or(ExecError::Type(crabka_pgtypes::TypeError::Overflow)),
                Datum::Int8(n) => n
                    .checked_abs()
                    .map(Datum::Int8)
                    .ok_or(ExecError::Type(crabka_pgtypes::TypeError::Overflow)),
                // SP30: abs over float8 (always representable, no overflow trap).
                Datum::Float4(f) => Ok(Datum::Float4(f.abs())),
                Datum::Float8(f) => Ok(Datum::Float8(f.abs())),
                // SP32: abs over numeric.
                Datum::Numeric(d) => Ok(Datum::Numeric(crabka_pgtypes::numeric::abs(d))),
                other => Err(type_error("abs", other)),
            }
        }
        ScalarFunc::Mod => {
            require_arity(fc, vals.len() == 2)?;
            Ok(ops::rem(&vals[0], &vals[1])?)
        }
        ScalarFunc::Floor | ScalarFunc::Ceil | ScalarFunc::Sign => {
            require_arity(fc, vals.len() == 1)?;
            round_family(f, &vals[0], None)
        }
        ScalarFunc::Round | ScalarFunc::Trunc => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let scale = match vals.get(1) {
                None => None,
                Some(s) => Some(int_arg(s)?),
            };
            round_family(f, &vals[0], scale)
        }
        ScalarFunc::Sqrt => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return crabka_pgtypes::numeric::num_sqrt(d)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            let x = as_f64(&vals[0])?;
            if x < 0.0 {
                return Err(domain(
                    "2201F",
                    "cannot take square root of a negative number",
                ));
            }
            Ok(Datum::Float8(x.sqrt()))
        }
        ScalarFunc::Exp => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return crabka_pgtypes::numeric::num_exp(d)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            finite_or_overflow(as_f64(&vals[0])?.exp())
        }
        ScalarFunc::Ln => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return crabka_pgtypes::numeric::num_ln(d)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            let x = as_f64(&vals[0])?;
            if x <= 0.0 {
                return Err(domain(
                    "2201E",
                    "cannot take logarithm of a non-positive number",
                ));
            }
            Ok(Datum::Float8(x.ln()))
        }
        ScalarFunc::Log => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return crabka_pgtypes::numeric::num_log10(d)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            let x = as_f64(&vals[0])?;
            if x <= 0.0 {
                return Err(domain(
                    "2201E",
                    "cannot take logarithm of a non-positive number",
                ));
            }
            Ok(Datum::Float8(x.log10()))
        }
        ScalarFunc::Power => {
            require_arity(fc, vals.len() == 2)?;
            let any_num =
                matches!(&vals[0], Datum::Numeric(_)) || matches!(&vals[1], Datum::Numeric(_));
            let any_f64 =
                matches!(&vals[0], Datum::Float8(_)) || matches!(&vals[1], Datum::Float8(_));
            if any_num && !any_f64 {
                let b = to_numeric(&vals[0])?;
                let e = to_numeric(&vals[1])?;
                return crabka_pgtypes::numeric::num_power(&b, &e)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            power(as_f64(&vals[0])?, as_f64(&vals[1])?)
        }
        ScalarFunc::Pi => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Float8(std::f64::consts::PI))
        }
        ScalarFunc::Lpad | ScalarFunc::Rpad => {
            require_arity(fc, vals.len() == 2 || vals.len() == 3)?;
            let s = text_arg(&vals[0])?;
            let width = int_arg(&vals[1])?;
            let fill = match vals.get(2) {
                None => " ",
                Some(d) => text_arg(d)?,
            };
            Ok(Datum::Text(pad(f, s, width, fill)?))
        }
        ScalarFunc::Left | ScalarFunc::Right => {
            require_arity(fc, vals.len() == 2)?;
            let s = text_arg(&vals[0])?;
            let n = int_arg(&vals[1])?;
            Ok(Datum::Text(left_right(f, s, n)))
        }
        ScalarFunc::Repeat => {
            require_arity(fc, vals.len() == 2)?;
            let s = text_arg(&vals[0])?;
            let n = int_arg(&vals[1])?;
            repeat_str(s, n)
        }
        ScalarFunc::Reverse => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Text(text_arg(&vals[0])?.chars().rev().collect()))
        }
        ScalarFunc::Initcap => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Text(initcap(text_arg(&vals[0])?)))
        }
        ScalarFunc::Strpos => {
            require_arity(fc, vals.len() == 2)?;
            Ok(Datum::Int4(strpos(
                text_arg(&vals[0])?,
                text_arg(&vals[1])?,
            )))
        }
        ScalarFunc::Ascii => {
            require_arity(fc, vals.len() == 1)?;
            let code = text_arg(&vals[0])?.chars().next().map_or(0, |c| c as i32);
            Ok(Datum::Int4(code))
        }
        ScalarFunc::Chr => {
            require_arity(fc, vals.len() == 1)?;
            chr(int_arg(&vals[0])?)
        }
        ScalarFunc::CurrentSetting => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let missing_ok = vals.get(1).map(bool_arg).transpose()?.unwrap_or(false);
            match crate::session::current_setting_runtime(text_arg(&vals[0])?, missing_ok)? {
                Some(value) => Ok(Datum::Text(value)),
                None => Ok(Datum::Null),
            }
        }
        ScalarFunc::SetConfig => {
            require_arity(fc, vals.len() == 3)?;
            let name = text_arg(&vals[0])?;
            let value = text_arg(&vals[1])?;
            let local = bool_arg(&vals[2])?;
            crate::session::set_config_runtime(name, value, local).map(Datum::Text)
        }
        ScalarFunc::AdvisoryLock {
            try_only,
            transactional,
            shared,
        } => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let key = advisory_key(vals)?;
            let granted =
                crate::session::advisory_lock_runtime(key, shared, transactional, !try_only)?;
            Ok(if try_only {
                Datum::Bool(granted)
            } else {
                Datum::Text(String::new())
            })
        }
        ScalarFunc::AdvisoryUnlock { shared } => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let key = advisory_key(vals)?;
            crate::session::advisory_unlock_runtime(key, shared).map(Datum::Bool)
        }
        ScalarFunc::AdvisoryUnlockAll => {
            require_arity(fc, vals.is_empty())?;
            crate::session::advisory_unlock_all_runtime()?;
            Ok(Datum::Text(String::new()))
        }
        ScalarFunc::NextVal => {
            require_arity(fc, vals.len() == 1)?;
            let name = text_arg(&vals[0])?.to_string();
            let runtime = ctx.sequence.as_ref().ok_or_else(|| {
                ExecError::Unsupported("sequence functions require a SQL session".into())
            })?;
            let (value, staged) =
                runtime
                    .manager
                    .nextval_written(&*runtime.kv, ctx.resolution(), &name)?;
            if let Some(staged) = staged {
                runtime
                    .pending
                    .lock()
                    .expect("pending sequences")
                    .stage(staged);
            }
            runtime
                .currvals
                .lock()
                .expect("sequence currvals")
                .insert(name, value);
            Ok(Datum::Int8(value))
        }
        ScalarFunc::CurrVal => {
            require_arity(fc, vals.len() == 1)?;
            let name = text_arg(&vals[0])?;
            let runtime = ctx.sequence.as_ref().ok_or_else(|| {
                ExecError::Unsupported("sequence functions require a SQL session".into())
            })?;
            let value = runtime
                .currvals
                .lock()
                .expect("sequence currvals")
                .get(name)
                .copied()
                .ok_or_else(|| {
                    ExecError::ObjectNotInPrerequisiteState(format!(
                        "currval of sequence \"{name}\" is not yet defined in this session"
                    ))
                })?;
            Ok(Datum::Int8(value))
        }
        ScalarFunc::SetVal => {
            require_arity(fc, vals.len() == 2 || vals.len() == 3)?;
            let name = text_arg(&vals[0])?.to_string();
            let value = int_arg(&vals[1])?;
            let is_called = vals.get(2).map(bool_arg).transpose()?.unwrap_or(true);
            let runtime = ctx.sequence.as_ref().ok_or_else(|| {
                ExecError::Unsupported("sequence functions require a SQL session".into())
            })?;
            let (value, staged) = runtime.manager.setval_written(
                &*runtime.kv,
                ctx.resolution(),
                &name,
                value,
                is_called,
            )?;
            if let Some(staged) = staged {
                runtime
                    .pending
                    .lock()
                    .expect("pending sequences")
                    .stage(staged);
            }
            runtime
                .currvals
                .lock()
                .expect("sequence currvals")
                .insert(name, value);
            Ok(Datum::Int8(value))
        }
        ScalarFunc::CurrentDatabase => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text("postgres".into()))
        }
        ScalarFunc::GetDatabaseEncoding => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text("UTF8".into()))
        }
        // The schema a `CREATE` with no qualifier lands in — the first
        // `search_path` entry that names an existing schema, and NULL when the
        // path names none. Verified against `postgres:18.4`, where
        // `SET search_path = notme; SELECT current_schema` is NULL rather than
        // `public`.
        ScalarFunc::CurrentSchema => {
            require_arity(fc, vals.is_empty())?;
            let kv = ctx.catalog().ok_or_else(|| {
                ExecError::Unsupported("current_schema requires a SQL session".into())
            })?;
            Ok(ctx
                .resolution()
                .creation_schema(kv)?
                .map_or(Datum::Null, Datum::Text))
        }
        ScalarFunc::CurrentUser => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text(ctx.current_user.clone()))
        }
        ScalarFunc::SessionUser => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text(ctx.session_user.clone()))
        }
        ScalarFunc::Version => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text(crabka_pgcatalog::server_version_string()))
        }
        ScalarFunc::PgInputIsValid => {
            require_arity(fc, vals.len() == 2)?;
            let input = text_arg(&vals[0])?;
            let type_name = text_arg(&vals[1])?;
            Ok(Datum::Bool(
                input_error(input, type_name, &ctx.time_zone)?.is_none(),
            ))
        }
        ScalarFunc::PgTableIsVisible => {
            require_arity(fc, vals.len() == 1)?;
            let _oid = int_arg(&vals[0])?;
            Ok(Datum::Bool(true))
        }
        // concat / coalesce / nullif / greatest / least are handled before here.
        _ => unreachable!("non-eager scalar function reached eval_eager"),
    }
}

fn eval_multirange_predicate(name: &str, vals: &[Datum]) -> Result<Datum, ExecError> {
    if vals.len() != 2 {
        return Err(undefined_function(name));
    }
    let result = match (name, &vals[0], &vals[1]) {
        ("range_overlaps_multirange", Datum::Range(range), Datum::Multirange(multirange))
        | ("multirange_overlaps_range", Datum::Multirange(multirange), Datum::Range(range)) => {
            crabka_pgtypes::multirange::overlaps_range(multirange, range)?
        }
        ("multirange_overlaps_multirange", Datum::Multirange(left), Datum::Multirange(right)) => {
            crabka_pgtypes::multirange::overlaps(left, right)?
        }
        ("multirange_contains_elem", Datum::Multirange(multirange), element)
        | ("elem_contained_by_multirange", element, Datum::Multirange(multirange)) => {
            crabka_pgtypes::multirange::contains_element(multirange, element)?
        }
        ("multirange_contains_range", Datum::Multirange(multirange), Datum::Range(range))
        | ("range_contained_by_multirange", Datum::Range(range), Datum::Multirange(multirange)) => {
            crabka_pgtypes::multirange::contains_range(multirange, range)?
        }
        ("multirange_contains_multirange", Datum::Multirange(left), Datum::Multirange(right)) => {
            crabka_pgtypes::multirange::contains(left, right)?
        }
        (
            "multirange_contained_by_multirange",
            Datum::Multirange(left),
            Datum::Multirange(right),
        ) => crabka_pgtypes::multirange::contains(right, left)?,
        _ => return Err(type_error(name, &vals[0])),
    };
    Ok(Datum::Bool(result))
}

pub(crate) fn input_error(
    input: &str,
    type_name: &str,
    time_zone: &jiff::tz::TimeZone,
) -> Result<Option<crabka_pgtypes::TypeError>, ExecError> {
    let ty = ColumnType::from_sql_name(type_name).ok_or_else(|| ExecError::FunctionError {
        sqlstate: "42704",
        message: format!("type \"{type_name}\" does not exist"),
    })?;
    Ok(crabka_pgtypes::cast::cast(&Datum::Text(input.to_string()), ty, time_zone).err())
}

// ---- argument-type helpers ----

/// 42883 for an argument whose static type a function does not accept (PG's
/// "no function matches the given name and argument types").
pub(crate) fn no_matching_function() -> ExecError {
    ExecError::UndefinedFunction("no function matches the given name and argument types".into())
}

/// Require the argument to statically type as a string. A bare `NULL` qualifies,
/// because it types as text. Otherwise the function does not exist for it
/// (42883).
///
/// `varchar(n)` and `char(n)` count. PostgreSQL declares its string functions on
/// `text` alone, but `varchar` and `bpchar` are binary-coercible to it, so a call
/// like `length(a_varchar_column)` resolves through that implicit cast. A match
/// on `Text` alone would reject every string function on a `varchar`/`char`
/// column.
fn require_text(arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    if crate::eval::infer_type(arg, scope)?.is_string() {
        Ok(())
    } else {
        Err(no_matching_function())
    }
}

/// Require the argument to statically type as an integer. Returns that width.
fn require_int(arg: &Expr, scope: &Scope) -> Result<ColumnType, ExecError> {
    match crate::eval::infer_type(arg, scope)? {
        t @ (ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8) => Ok(t),
        _ => Err(no_matching_function()),
    }
}

fn require_bool(arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    match crate::eval::infer_type(arg, scope)? {
        ColumnType::Bool => Ok(()),
        _ => Err(no_matching_function()),
    }
}

/// SP32: require an int OR numeric argument. These are the `mod` operand types,
/// because PostgreSQL has no `float8` modulo.
fn require_int_or_numeric(arg: &Expr, scope: &Scope) -> Result<ColumnType, ExecError> {
    // An `unknown` operand constrains nothing; the caller's other operand picks
    // the overload, and `mod`'s widest candidate settles an all-unknown pair —
    // which `scalar_result_type` has already rejected as ambiguous.
    if is_unknown_literal(arg) {
        return Ok(ColumnType::Int4);
    }
    let t = crate::eval::infer_type(arg, scope)?;
    if matches!(t, ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8) || t.is_numeric() {
        Ok(t)
    } else {
        Err(no_matching_function())
    }
}

/// SP30/SP32: require a numeric argument (int4/int8/float8/numeric). Returns
/// that type so the caller (`abs`) can preserve it.
/// This folds `real` onto `double precision` for the functions PostgreSQL
/// overloads on `float8` but not `float4` (`floor`, `ceil`, `round`, `trunc`,
/// `sign`, `sqrt`, …). Those calls resolve through the implicit widening cast,
/// so their result is `double precision`, not `real`.
fn float4_widens(t: ColumnType) -> ColumnType {
    if t == ColumnType::Float4 {
        ColumnType::Float8
    } else {
        t
    }
}

fn require_numeric(arg: &Expr, scope: &Scope) -> Result<ColumnType, ExecError> {
    // An `unknown` argument constrains nothing, and every candidate set that
    // reaches here prefers `float8` — which is why `sqrt(NULL)` is a `double
    // precision` NULL rather than an unresolvable call.
    if is_unknown_literal(arg) {
        return Ok(ColumnType::Float8);
    }
    let t = crate::eval::infer_type(arg, scope)?;
    if matches!(
        t,
        ColumnType::Int2
            | ColumnType::Int4
            | ColumnType::Int8
            | ColumnType::Float4
            | ColumnType::Float8
    ) || t.is_numeric()
    {
        Ok(t)
    } else {
        Err(no_matching_function())
    }
}

/// Unify every argument's type into one. This is PostgreSQL's
/// `select_common_type` for `COALESCE`/`GREATEST`/`LEAST`/`NULLIF`. The choice
/// ignores `unknown` inputs, which are a bare `NULL` and an unadorned string
/// literal, and an argument list that is entirely `unknown` resolves to `text`.
/// Incompatible known types are 42804, spelled the way PostgreSQL spells them,
/// with the construct's name.
fn unify_args(f: ScalarFunc, args: &[Expr], scope: &Scope) -> Result<ColumnType, ExecError> {
    let mut acc: Option<ColumnType> = None;
    for a in args {
        if is_unknown_literal(a) {
            continue;
        }
        acc = crate::eval::unify_branch(acc, a, scope).map_err(|e| name_mismatch(f, e))?;
    }
    Ok(acc.unwrap_or(ColumnType::Text))
}

/// Is `e` an argument PostgreSQL still calls `unknown` at this point?
fn is_unknown_literal(e: &Expr) -> bool {
    matches!(e, Expr::StringLiteral(_) | Expr::NullLiteral)
}

/// Evaluate `args` and apply PostgreSQL's common-type resolution to the result.
/// This coerces every `unknown` string literal to the type the other arguments
/// settled on. `greatest`/`least`/`nullif` all compare their arguments against
/// one another, so they need one common type before `ops::compare` runs.
/// Without it, `greatest(1, '2')` would try to order an integer against text.
fn resolved_args(
    args: &[Expr],
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Vec<Datum>, ExecError> {
    let mut vals = args
        .iter()
        .map(&mut eval_child)
        .collect::<Result<Vec<_>, _>>()?;
    let common = args
        .iter()
        .zip(&vals)
        .filter(|(a, _)| !is_unknown_literal(a))
        .filter_map(|(_, v)| v.column_type())
        .try_fold(None, |acc: Option<ColumnType>, t| match acc {
            None => Ok(Some(t)),
            Some(a) => crate::eval::unify_types(a, t).map(Some),
        })?;
    let Some(common) = common else {
        return Ok(vals);
    };
    for (a, v) in args.iter().zip(&mut vals) {
        if is_unknown_literal(a) && !v.is_null() {
            *v = crabka_pgtypes::cast::cast(v, common, &ctx.time_zone)?;
        }
    }
    Ok(vals)
}

/// The type name `pg_typeof` reports: the evaluated value's own type. It falls
/// back to an explicit cast's target when the value is NULL, and to PostgreSQL's
/// `unknown` for a literal that never acquired one.
fn typeof_name(arg: &Expr, value: &Datum) -> String {
    if is_unknown_literal(arg) {
        return "unknown".into();
    }
    // A domain's *value* is a base-type value; only the expression records that
    // it went through the domain, so an explicit cast to one is read off the
    // node ahead of the value.
    if let Expr::Cast {
        ty: ty @ ColumnType::Domain(_),
        ..
    } = arg
    {
        return type_display_name(*ty);
    }
    if let Some(t) = value.column_type() {
        return type_display_name(t);
    }
    match arg {
        Expr::Cast { ty, .. } => type_display_name(*ty),
        _ => "unknown".into(),
    }
}

/// The name `pg_typeof`/`format_type` print for a type: an array renders as
/// `element[]`, everything else as its bare SQL name.
fn type_display_name(t: ColumnType) -> String {
    match t {
        ColumnType::Array(elem) => elem.array_name().to_string(),
        other => other.name().to_string(),
    }
}

/// Require an integer argument, or a bare `NULL`. PostgreSQL resolves such a
/// `NULL` to the parameter's own type and does not reject it.
fn require_int_or_null(arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    if matches!(arg, Expr::NullLiteral) {
        return Ok(());
    }
    require_int(arg, scope).map(|_| ())
}

/// PostgreSQL `format_type(oid, typmod)`: the SQL-standard spelling of a type,
/// with its modifier applied. An unrecognized OID is `-`, matching PostgreSQL's
/// placeholder for a type that no longer exists.
fn format_type(oid: i64, typmod: i64) -> String {
    let Ok(oid) = u32::try_from(oid) else {
        return "-".to_string();
    };
    let Some((base, kind)) = builtin_format_type(oid) else {
        return "-".to_string();
    };
    let modifier = if typmod < 0 {
        String::new()
    } else {
        type_modifier(kind, typmod)
    };
    let (element, suffix) = match base.strip_suffix("[]") {
        Some(element) => (element, "[]"),
        None => (base, ""),
    };
    if modifier.is_empty() {
        return format!("{element}{suffix}");
    }
    // `bpchar` is PostgreSQL's internal name for an unmodified blank-padded
    // char; once a length is attached it prints as the SQL spelling.
    let element = if element == "bpchar" {
        "character"
    } else {
        element
    };
    // A fractional-seconds precision goes right after the type word, before the
    // `with`/`without time zone` tail: `timestamp(3) with time zone`.
    match (kind, element.split_once(' ')) {
        (TypmodKind::Seconds, Some((head, tail))) => {
            format!("{head}{modifier} {tail}{suffix}")
        }
        _ => format!("{element}{modifier}{suffix}"),
    }
}

/// How a type spells its `typmod`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypmodKind {
    /// Nothing ever prints a modifier (`integer`, `text`, …).
    None,
    /// A character length, stored with the 4-byte varlena header included.
    Length,
    /// `numeric(precision, scale)`, packed into one int32.
    PrecisionScale,
    /// A fractional-seconds precision (`timestamp(3)`), printed before the
    /// `with/without time zone` tail.
    Seconds,
}

fn type_modifier(kind: TypmodKind, typmod: i64) -> String {
    match kind {
        TypmodKind::None => String::new(),
        TypmodKind::Length => format!("({})", typmod - 4),
        TypmodKind::PrecisionScale => {
            let packed = typmod - 4;
            format!("({},{})", (packed >> 16) & 0xffff, packed & 0xffff)
        }
        TypmodKind::Seconds => format!("({typmod})"),
    }
}

/// The built-in types `format_type` knows, as `(printed name, typmod spelling)`.
/// The name carries a `[]` suffix for an array type, and the modifier goes in
/// before that suffix (`character varying(6)[]`).
fn builtin_format_type(oid: u32) -> Option<(&'static str, TypmodKind)> {
    use TypmodKind::{Length, None as NoMod, PrecisionScale, Seconds};
    Some(match oid {
        16 => ("boolean", NoMod),
        17 => ("bytea", NoMod),
        18 => ("\"char\"", NoMod),
        19 => ("name", NoMod),
        20 => ("bigint", NoMod),
        21 => ("smallint", NoMod),
        23 => ("integer", NoMod),
        25 => ("text", NoMod),
        26 => ("oid", NoMod),
        114 => ("json", NoMod),
        142 => ("xml", NoMod),
        199 => ("json[]", NoMod),
        700 => ("real", NoMod),
        701 => ("double precision", NoMod),
        705 => ("unknown", NoMod),
        1000 => ("boolean[]", NoMod),
        1001 => ("bytea[]", NoMod),
        1005 => ("smallint[]", NoMod),
        1007 => ("integer[]", NoMod),
        1009 => ("text[]", NoMod),
        1014 => ("bpchar[]", Length),
        1015 => ("character varying[]", Length),
        1016 => ("bigint[]", NoMod),
        1021 => ("real[]", NoMod),
        1022 => ("double precision[]", NoMod),
        1042 => ("bpchar", Length),
        1043 => ("character varying", Length),
        1082 => ("date", NoMod),
        1083 => ("time without time zone", Seconds),
        1114 => ("timestamp without time zone", Seconds),
        1115 => ("timestamp without time zone[]", Seconds),
        1182 => ("date[]", NoMod),
        1183 => ("time without time zone[]", Seconds),
        1184 => ("timestamp with time zone", Seconds),
        1185 => ("timestamp with time zone[]", Seconds),
        1186 => ("interval", NoMod),
        1187 => ("interval[]", NoMod),
        1231 => ("numeric[]", PrecisionScale),
        1266 => ("time with time zone", Seconds),
        1700 => ("numeric", PrecisionScale),
        2205 => ("regclass", NoMod),
        2206 => ("regtype", NoMod),
        2278 => ("void", NoMod),
        2950 => ("uuid", NoMod),
        2951 => ("uuid[]", NoMod),
        3802 => ("jsonb", NoMod),
        3807 => ("jsonb[]", NoMod),
        3614 => ("tsvector", NoMod),
        3615 => ("tsquery", NoMod),
        3643 => ("tsvector[]", NoMod),
        3645 => ("tsquery[]", NoMod),
        _ => return Option::None,
    })
}

/// PostgreSQL prefixes the "types … cannot be matched" message with the
/// construct's SQL name (`COALESCE types integer and text cannot be matched`).
fn name_mismatch(f: ScalarFunc, e: ExecError) -> ExecError {
    let name = match f {
        ScalarFunc::Coalesce => "COALESCE",
        ScalarFunc::Greatest => "GREATEST",
        ScalarFunc::Least => "LEAST",
        _ => "NULLIF",
    };
    match e {
        ExecError::TypeMismatch(message) => ExecError::TypeMismatch(format!("{name} {message}")),
        other => other,
    }
}

/// Is `e` an argument PostgreSQL still calls `unknown`? This is public so the
/// sibling function modules can apply the same overload-resolution rules.
pub(crate) fn is_unknown_arg(e: &Expr) -> bool {
    is_unknown_literal(e)
}

/// 42725: PostgreSQL cannot choose between a function's overloads, because
/// every argument is still `unknown` and no candidate is preferred. The
/// families that have two or more equally-good numeric overloads raise it
/// (`gcd`, `lcm`, `to_hex`, the two-argument `random`).
pub(crate) fn ambiguous_function(name: &str, arity: usize) -> ExecError {
    let spelled = vec!["unknown"; arity].join(", ");
    ExecError::FunctionError {
        sqlstate: "42725",
        message: format!("function {name}({spelled}) is not unique"),
    }
}

/// 42883 that spells out the argument types PostgreSQL could not match, so a
/// bad call reads `function concat() does not exist` and not a generic `(...)`.
pub(crate) fn undefined_function_spelled(name: &str, args: &[Expr], scope: &Scope) -> ExecError {
    let spelled: Vec<&str> = args
        .iter()
        .map(|a| {
            if is_unknown_literal(a) {
                "unknown"
            } else {
                crate::eval::infer_type(a, scope).map_or("unknown", ColumnType::name)
            }
        })
        .collect();
    ExecError::UndefinedFunction(format!(
        "function {name}({}) does not exist",
        spelled.join(", ")
    ))
}

fn promote(a: ColumnType, b: ColumnType) -> ColumnType {
    if a == ColumnType::Int2 && b == ColumnType::Int2 {
        ColumnType::Int2
    } else if matches!(a, ColumnType::Int2 | ColumnType::Int4)
        && matches!(b, ColumnType::Int2 | ColumnType::Int4)
    {
        ColumnType::Int4
    } else {
        ColumnType::Int8
    }
}

pub(crate) fn require_arity(fc: &FuncCall, ok: bool) -> Result<(), ExecError> {
    if ok {
        Ok(())
    } else {
        Err(undefined_function(&fc.name))
    }
}

/// A text argument at runtime. A non-text Datum here means the call sat in a
/// non-projected position, so `scalar_result_type` never type-checked it.
/// PostgreSQL rejects it at plan time (42883); Gres surfaces it at runtime
/// (42804).
/// Queue one `pg_notify(channel, payload)` on the session's pending
/// notification list. This is the same list a `NOTIFY` statement writes to, so
/// both deliver at the same point (commit, or the end of an autocommit
/// statement) and dedup against each other.
fn eval_pg_notify(channel: &Datum, payload: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    let channel = nullable_text_arg(channel)?;
    let payload = nullable_text_arg(payload)?;
    let pending = ctx
        .notify
        .as_ref()
        .ok_or_else(|| ExecError::Unsupported("pg_notify requires a SQL session".into()))?;
    pending
        .lock()
        .expect("notify pending mutex")
        .queue_notify(channel, payload)
        .map_err(crate::session::notify_queue_error)?;
    Ok(Datum::Text(String::new()))
}

/// A text argument of a non-strict function, with PostgreSQL's NULL-as-empty
/// -string conversion.
fn nullable_text_arg(d: &Datum) -> Result<&str, ExecError> {
    match d {
        Datum::Null => Ok(""),
        other => text_arg(other),
    }
}

fn text_arg(d: &Datum) -> Result<&str, ExecError> {
    match d {
        Datum::Text(s) => Ok(s),
        other => Err(type_error("function", other)),
    }
}

/// An integer argument at runtime, promoted to i64.
/// The advisory-lock key: the single `int8` spelling, or `PostgreSQL`'s packing
/// of the two-`int4` spelling into one `int8`.
fn advisory_key(vals: &[Datum]) -> Result<i64, ExecError> {
    match vals {
        [key] => int_arg(key),
        [high, low] => {
            let high = i32::try_from(int_arg(high)?)
                .map_err(|_| ExecError::InvalidParameterValue("advisory lock key".into()))?;
            let low = i32::try_from(int_arg(low)?)
                .map_err(|_| ExecError::InvalidParameterValue("advisory lock key".into()))?;
            Ok(crate::lockmgr::AdvisoryLockManager::pack_key(high, low))
        }
        _ => Err(ExecError::InvalidParameterValue("advisory lock key".into())),
    }
}

pub(crate) fn int_arg(d: &Datum) -> Result<i64, ExecError> {
    match d {
        Datum::Int4(n) => Ok(i64::from(*n)),
        Datum::Int8(n) => Ok(*n),
        other => Err(type_error("function", other)),
    }
}

fn bool_arg(d: &Datum) -> Result<bool, ExecError> {
    match d {
        Datum::Bool(value) => Ok(*value),
        other => Err(type_error("function", other)),
    }
}

pub(crate) fn type_error(what: &str, got: &Datum) -> ExecError {
    ExecError::TypeMismatch(format!(
        "{what} does not accept an argument of type {}",
        got.column_type().map(|t| t.name()).unwrap_or("unknown")
    ))
}

/// The canonical text rendering of a non-NULL Datum, which is the wire text
/// encoding. So `concat` agrees with the DataRow output and with the `||`
/// operator.
pub(crate) fn text_render(d: &Datum, tz: &jiff::tz::TimeZone) -> String {
    String::from_utf8(crabka_pgtypes::encoding::encode_text(d, tz))
        .expect("a Datum's text encoding is always valid UTF-8")
}

// ---- rounding helpers (SP33) ----

/// Rounding-family value transform. `scale` is `Some` only for the two-arg
/// `round`/`trunc` form, which always yields numeric and promotes an int first
/// argument to numeric. The one-arg form preserves the input numeric type.
fn round_family(f: ScalarFunc, v: &Datum, scale: Option<i64>) -> Result<Datum, ExecError> {
    use crabka_pgtypes::numeric as num;
    if let Some(n) = scale {
        let bd = match v {
            Datum::Int2(i) => num::from_i64(i64::from(*i)),
            Datum::Int4(i) => num::from_i64(i64::from(*i)),
            Datum::Int8(i) => num::from_i64(*i),
            Datum::Numeric(d) => d.clone(),
            other => return Err(type_error("function", other)),
        };
        return Ok(Datum::Numeric(match f {
            ScalarFunc::Round => num::round(&bd, n),
            ScalarFunc::Trunc => num::trunc(&bd, n),
            _ => unreachable!("scale is only set for round/trunc"),
        }));
    }
    match v {
        Datum::Int2(_) | Datum::Int4(_) | Datum::Int8(_) => match f {
            ScalarFunc::Sign => sign_int(v),
            _ => Ok(v.clone()), // floor/ceil/round/trunc of an integer is itself
        },
        // `real` has no overload of its own here, so PostgreSQL widens it to
        // `double precision` — the result type `float4_widens` already reports.
        Datum::Float4(x) => round_family(f, &Datum::Float8(f64::from(*x)), None),
        Datum::Float8(x) => Ok(Datum::Float8(match f {
            ScalarFunc::Floor => x.floor(),
            ScalarFunc::Ceil => x.ceil(),
            ScalarFunc::Round => x.round_ties_even(), // PG float8 round = half-to-even
            ScalarFunc::Trunc => x.trunc(),
            ScalarFunc::Sign => float_sign(*x),
            _ => unreachable!(),
        })),
        Datum::Numeric(d) => Ok(Datum::Numeric(match f {
            ScalarFunc::Floor => num::floor(d),
            ScalarFunc::Ceil => num::ceil(d),
            ScalarFunc::Round => num::round(d, 0),
            ScalarFunc::Trunc => num::trunc(d, 0),
            ScalarFunc::Sign => num::sign(d),
            _ => unreachable!(),
        })),
        other => Err(type_error("function", other)),
    }
}

/// `sign` of an integer, with its width preserved.
fn sign_int(v: &Datum) -> Result<Datum, ExecError> {
    Ok(match v {
        Datum::Int2(n) => Datum::Int2(n.signum()),
        Datum::Int4(n) => Datum::Int4(n.signum()),
        Datum::Int8(n) => Datum::Int8(n.signum()),
        other => return Err(type_error("sign", other)),
    })
}

// ---- transcendental helpers (SP33) ----

/// Coerce any numeric Datum (int4/int8/float8/numeric) to f64 for the
/// transcendental functions, which always compute in float8.
fn as_f64(d: &Datum) -> Result<f64, ExecError> {
    Ok(match d {
        Datum::Int2(n) => f64::from(*n),
        Datum::Int4(n) => f64::from(*n),
        Datum::Int8(n) => *n as f64,
        Datum::Float4(x) => f64::from(*x),
        Datum::Float8(x) => *x,
        Datum::Numeric(d) => crabka_pgtypes::numeric::to_f64(d),
        other => return Err(type_error("function", other)),
    })
}

/// Build a domain error carrying its PostgreSQL SQLSTATE.
pub(crate) fn domain(sqlstate: &'static str, message: &'static str) -> ExecError {
    ExecError::Type(crabka_pgtypes::TypeError::Domain { sqlstate, message })
}

/// Wrap an f64 result and map an overflow to infinity onto 22003. This matches
/// the engine's float8 arithmetic, which treats a finite-to-infinite overflow as
/// out of range.
fn finite_or_overflow(x: f64) -> Result<Datum, ExecError> {
    if x.is_infinite() {
        Err(ExecError::Type(crabka_pgtypes::TypeError::Overflow))
    } else {
        Ok(Datum::Float8(x))
    }
}

/// PostgreSQL power result type. It is float8 if any operand is float8. If not,
/// it is numeric if any operand is numeric. Otherwise it is float8, the all-int
/// case, which is PG's preferred type.
fn power_result_type(a: ColumnType, b: ColumnType) -> ColumnType {
    let (a, b) = (float4_widens(a), float4_widens(b));
    if a == ColumnType::Float8 || b == ColumnType::Float8 {
        ColumnType::Float8
    } else if a.is_numeric() || b.is_numeric() {
        ColumnType::Numeric(None)
    } else {
        ColumnType::Float8
    }
}

/// Promote an int4/int8/numeric Datum to a [`NumericValue`] (for the numeric
/// power path, where one operand may be an integer).
pub(crate) fn to_numeric(d: &Datum) -> Result<crabka_pgtypes::numeric::NumericValue, ExecError> {
    match d {
        Datum::Int2(n) => Ok(crabka_pgtypes::numeric::from_i64(i64::from(*n))),
        Datum::Int4(n) => Ok(crabka_pgtypes::numeric::from_i64(i64::from(*n))),
        Datum::Int8(n) => Ok(crabka_pgtypes::numeric::from_i64(*n)),
        Datum::Numeric(d) => Ok(d.clone()),
        other => Err(type_error("power", other)),
    }
}

/// `power(base, exp)` with PostgreSQL's domain checks (2201F).
fn power(base: f64, exp: f64) -> Result<Datum, ExecError> {
    if base == 0.0 && exp < 0.0 {
        return Err(domain(
            "2201F",
            "zero raised to a negative power is undefined",
        ));
    }
    if base < 0.0 && exp.fract() != 0.0 {
        return Err(domain(
            "2201F",
            "a negative number raised to a non-integer power yields a complex result",
        ));
    }
    finite_or_overflow(base.powf(exp))
}

/// `sign` of a float8: −1 / 0 / 1, and `NaN` for `NaN` (PostgreSQL `dsign`).
fn float_sign(x: f64) -> f64 {
    if x.is_nan() {
        f64::NAN
    } else if x > 0.0 {
        1.0
    } else if x < 0.0 {
        -1.0
    } else {
        0.0
    }
}

fn eval_range_constructor(
    ty: RangeRef,
    fc: &FuncCall,
    vals: &[Datum],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    require_arity(fc, (1..=3).contains(&vals.len()))?;
    if vals.len() == 1 {
        return crabka_pgtypes::cast::cast(&vals[0], ColumnType::Range(ty), &ctx.time_zone)
            .map_err(ExecError::from);
    }
    if vals.get(2).is_some_and(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let bounds = vals.get(2).map_or(Ok("[)"), text_arg)?;
    let [left, right] = bounds.as_bytes() else {
        return Err(ExecError::Type(crabka_pgtypes::TypeError::Coded {
            sqlstate: "22023",
            message: "range constructor flags argument must be two characters".into(),
        }));
    };
    if !matches!(left, b'[' | b'(') || !matches!(right, b']' | b')') {
        return Err(ExecError::Type(crabka_pgtypes::TypeError::Coded {
            sqlstate: "22023",
            message: "range constructor flags argument must contain one of '(', '[' followed by one of ')', ']'".into(),
        }));
    }
    let cast_bound = |value: &Datum| -> Result<Option<Box<Datum>>, ExecError> {
        if value.is_null() {
            Ok(None)
        } else {
            Ok(Some(Box::new(crabka_pgtypes::cast::cast(
                value,
                *ty.subtype,
                &ctx.time_zone,
            )?)))
        }
    };
    let value = crabka_pgtypes::RangeValue {
        ty,
        lower: cast_bound(&vals[0])?,
        upper: cast_bound(&vals[1])?,
        lower_inclusive: *left == b'[',
        upper_inclusive: *right == b']',
        empty: false,
    };
    let literal = crabka_pgtypes::range::to_text(&value, |bound| {
        String::from_utf8(crabka_pgtypes::encoding::encode_text_in(
            bound,
            ctx.output_style(),
        ))
        .expect("a Datum's text encoding is always valid UTF-8")
    });
    Ok(Datum::Range(crabka_pgtypes::range::parse(
        &literal,
        ty,
        &ctx.time_zone,
    )?))
}

// ---- string helpers ----

fn trim_ws(f: ScalarFunc, s: &str) -> String {
    match f {
        ScalarFunc::Ltrim => s.trim_start().to_string(),
        ScalarFunc::Rtrim => s.trim_end().to_string(),
        _ => s.trim().to_string(), // btrim
    }
}

fn trim_set(f: ScalarFunc, s: &str, set: &[char]) -> String {
    let in_set = |c: char| set.contains(&c);
    match f {
        ScalarFunc::Ltrim => s.trim_start_matches(in_set).to_string(),
        ScalarFunc::Rtrim => s.trim_end_matches(in_set).to_string(),
        _ => s.trim_matches(in_set).to_string(), // btrim
    }
}

/// PostgreSQL `substr(string, start [, count])`. `start` is 1-based, and
/// characters before position 1 count against `count`. A negative `count` is an
/// error (22011). A NULL argument already short-circuited to NULL in
/// `eval_eager`.
/// `substring(string, posix_pattern)` returns the first match, or the first
/// parenthesized subexpression when the pattern has one, and NULL when the
/// pattern does not match at all.
fn posix_substring(s: &str, pattern: &str) -> Result<Datum, ExecError> {
    let regex = regex::Regex::new(pattern).map_err(|_| {
        ExecError::Type(crabka_pgtypes::TypeError::Domain {
            sqlstate: "2201B",
            message: "invalid regular expression",
        })
    })?;
    let Some(captures) = regex.captures(s) else {
        return Ok(Datum::Null);
    };
    // PostgreSQL reserves group 1 for exactly this: with parentheses the group
    // is the result, without them the whole match is.
    Ok(captures
        .get(1)
        .or_else(|| captures.get(0))
        .map_or(Datum::Null, |m| Datum::Text(m.as_str().to_string())))
}

/// `overlay(string, replacement, start, count)`: replace `count` characters
/// that start at `start` with `replacement`.
///
/// PostgreSQL defines it as `substring(s, 1, start - 1) || replacement ||
/// substring(s, start + count)`. That is why a `start` of 0 with a positive
/// `count` is a negative-length substring error and not an insertion at the
/// front.
fn overlay(s: &str, replacement: &str, start: i64, count: i64) -> Result<Datum, ExecError> {
    let prefix = substr(s, 1, Some(start - 1))?;
    let suffix = substr(s, start.saturating_add(count), None)?;
    let (Datum::Text(prefix), Datum::Text(suffix)) = (prefix, suffix) else {
        unreachable!("substr of text is text");
    };
    Ok(Datum::Text(format!("{prefix}{replacement}{suffix}")))
}

fn substr(s: &str, start: i64, count: Option<i64>) -> Result<Datum, ExecError> {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    // The window is [start, end) in 1-based positions, clamped to [1, len+1).
    let end = match count {
        None => len + 1,
        Some(c) => {
            if c < 0 {
                return Err(ExecError::FunctionError {
                    sqlstate: "22011",
                    message: "negative substring length not allowed".into(),
                });
            }
            start.saturating_add(c)
        }
    };
    let lo = start.max(1);
    let hi = end.min(len + 1);
    if lo >= hi {
        return Ok(Datum::Text(String::new()));
    }
    let out: String = chars[(lo - 1) as usize..(hi - 1) as usize].iter().collect();
    Ok(Datum::Text(out))
}

// ---- string-family helpers (SP33) ----

/// PostgreSQL's ~1 GB field-size limit. It guards `repeat`/`lpad`/`rpad` against
/// an adversarially huge string. The engine raises 54000, "requested length too
/// large", and does not abort the process on an out-of-memory allocation.
const MAX_FIELD_SIZE: usize = 1 << 30;

/// 54000 (`program_limit_exceeded`): a call asked a string function to produce
/// a field larger than the engine permits.
fn length_too_large() -> ExecError {
    domain("54000", "requested length too large")
}

/// `lpad`/`rpad`: pad `s` to `width` chars with `fill`. When `s` is longer than
/// `width`, both forms truncate it to its first `width` chars. A `width <= 0`
/// yields the empty string. An empty `fill` that cannot pad leaves `s`
/// unchanged. A `width` beyond [`MAX_FIELD_SIZE`] is 54000, and not an OOM
/// allocation.
fn pad(f: ScalarFunc, s: &str, width: i64, fill: &str) -> Result<String, ExecError> {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    if width <= 0 {
        return Ok(String::new());
    }
    if width as usize > MAX_FIELD_SIZE {
        return Err(length_too_large());
    }
    if len >= width {
        return Ok(chars[..width as usize].iter().collect());
    }
    let fill_chars: Vec<char> = fill.chars().collect();
    if fill_chars.is_empty() {
        return Ok(s.to_string());
    }
    let pad_len = (width - len) as usize;
    let padding: String = fill_chars.iter().cycle().take(pad_len).collect();
    Ok(match f {
        ScalarFunc::Lpad => format!("{padding}{s}"),
        _ => format!("{s}{padding}"), // Rpad
    })
}

/// `left`/`right`: the first or last `n` chars. A negative `n` drops `|n|` chars
/// from the far end, as PostgreSQL does.
fn left_right(f: ScalarFunc, s: &str, n: i64) -> String {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    let take = if n < 0 { (len + n).max(0) } else { n.min(len) };
    match f {
        ScalarFunc::Left => chars[..take as usize].iter().collect(),
        _ => chars[(len - take) as usize..].iter().collect(), // Right
    }
}

/// `repeat(s, n)`: an `n <= 0` yields the empty string. A guard keeps the result
/// within the field-size limit.
fn repeat_str(s: &str, n: i64) -> Result<Datum, ExecError> {
    if n <= 0 {
        return Ok(Datum::Text(String::new()));
    }
    let n = n as usize;
    match s.len().checked_mul(n) {
        Some(total) if total <= MAX_FIELD_SIZE => Ok(Datum::Text(s.repeat(n))),
        _ => Err(length_too_large()),
    }
}

/// `initcap(s)`: uppercase the first alphanumeric of each word, lowercase the rest.
fn initcap(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_alnum = false;
    for c in s.chars() {
        if c.is_alphanumeric() {
            if prev_alnum {
                out.extend(c.to_lowercase());
            } else {
                out.extend(c.to_uppercase());
            }
            prev_alnum = true;
        } else {
            out.push(c);
            prev_alnum = false;
        }
    }
    out
}

/// `strpos(s, sub)`: the 1-based char index of the first `sub` in `s`, or `0`
/// when `sub` is absent. An empty `sub` matches at position `1`, as PostgreSQL
/// does.
fn strpos(s: &str, sub: &str) -> i32 {
    if sub.is_empty() {
        return 1;
    }
    match s.find(sub) {
        None => 0,
        Some(byte_idx) => (s[..byte_idx].chars().count() + 1) as i32,
    }
}

/// `chr(n)`: the one-character string for Unicode code point `n`. `0` or an
/// out-of-range / surrogate code point is 54000.
fn chr(n: i64) -> Result<Datum, ExecError> {
    if n == 0 {
        return Err(domain("54000", "null character not permitted"));
    }
    match u32::try_from(n).ok().and_then(char::from_u32) {
        Some(c) => Ok(Datum::Text(c.to_string())),
        None => Err(domain(
            "54000",
            "requested character too large for encoding",
        )),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::{Column, RelationName, Table};
    use crabka_pgparser::parser::parse_expr_for_test as pexpr;

    use super::*;

    fn table() -> Table {
        Table {
            id: 1,
            name: RelationName::public("t"),
            columns: vec![
                Column::new("s", ColumnType::Text),
                Column::new("n", ColumnType::Int4),
            ],
            sharded: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        }
    }

    fn table_n() -> Table {
        Table {
            id: 1,
            name: RelationName::public("t"),
            columns: vec![Column::new("qn", ColumnType::Numeric(None))],
            sharded: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        }
    }

    /// The table's single-relation scope, or the empty (FROM-less) scope.
    fn scope_of(t: Option<&Table>) -> Scope {
        match t {
            Some(t) => Scope::single(t, &t.name.name),
            None => Scope::empty(),
        }
    }

    /// Evaluate a scalar-function expression with no row context.
    fn ev(sql: &str) -> Datum {
        let ctx = crate::clock::EvalCtx::test_default();
        crate::eval::eval(&pexpr(sql).expect("parse"), &Scope::empty(), &[], &ctx).expect("eval")
    }

    /// The SQL-standard call forms that spell their arguments with keywords.
    /// Every expectation here comes from PostgreSQL 18.4, including the
    /// clipping and error cases, because the keyword spellings and the comma
    /// spellings must agree and both must agree with the oracle.
    #[test]
    fn sql_standard_keyword_argument_call_forms_match_postgresql() {
        let cases: &[(&str, &str)] = &[
            // SUBSTRING: FROM/FOR, either alone, and the comma spelling.
            ("substring('abcdef' FROM 2 FOR 3)", "bcd"),
            ("substring('abcdef' FROM 2)", "bcdef"),
            ("substring('abcdef' FOR 3)", "abc"),
            ("substring('abcdef', 2, 3)", "bcd"),
            // A start before position 1 spends the count getting there.
            ("substring('abcdef' FROM 0 FOR 3)", "ab"),
            ("substring('abcdef' FROM -1 FOR 3)", "a"),
            // The pattern forms: the first capture group if there is one, else
            // the whole match.
            ("substring('abcdef' FROM 'b.d')", "bcd"),
            ("substring('abcdef' FROM '(b)(.d)')", "b"),
            (
                "substring('abcdef' SIMILAR '%#\"b_d#\"%' ESCAPE '#')",
                "bcd",
            ),
            // TRIM: the side chooses btrim/ltrim/rtrim, and omitted characters
            // mean spaces.
            ("trim(' x ')", "x"),
            ("trim(both from ' x ')", "x"),
            ("trim(leading 'x' from 'xxa')", "a"),
            ("trim(trailing 'x' from 'axx')", "a"),
            ("trim(both 'x' from 'xxaxx')", "a"),
            ("trim('x' from 'xxaxx')", "a"),
            ("trim(leading from '  xxa')", "xxa"),
            // OVERLAY's count defaults to the replacement's own length, so the
            // default replaces exactly as much as it inserts.
            ("overlay('abcdef' placing 'ZZ' from 2 for 3)", "aZZef"),
            ("overlay('abcdef' placing 'ZZ' from 2)", "aZZdef"),
            ("overlay('abcdef' placing 'ZZ' from 2 for 0)", "aZZbcdef"),
            ("overlay('abcdef' placing '' from 2 for 2)", "adef"),
        ];
        for (expr, expected) in cases {
            let got = ev(expr);
            assert!(
                got == Datum::Text((*expected).into()),
                "{expr}: {got:?} != {expected}"
            );
        }

        // POSITION reverses its arguments relative to `strpos`, and returns an
        // integer rather than text.
        for (expr, expected) in [
            ("position('b' in 'abc')", 2),
            ("position('z' in 'abc')", 0),
            ("position('' in 'abc')", 1),
        ] {
            assert!(ev(expr) == Datum::Int4(expected), "{expr}");
        }

        // A pattern that does not match is NULL, not the empty string.
        assert!(ev("substring('abcdef' FROM 'x')") == Datum::Null);

        // PostgreSQL raises 22011 (substring_error), not a type error, and
        // `overlay` inherits it through the substring it is defined as.
        assert!(ec_eval("substring('abcdef' FROM 2 FOR -1)") == "22011");
        assert!(ec_eval("overlay('abcdef' placing 'ZZ' from 0)") == "22011");
    }

    /// SQLSTATE of a runtime eval error (no row context).
    fn ec_eval(sql: &str) -> String {
        let ctx = crate::clock::EvalCtx::test_default();
        crate::eval::eval(&pexpr(sql).expect("parse"), &Scope::empty(), &[], &ctx)
            .expect_err("expected error")
            .into_pg()
            .code
    }

    fn err_code(sql: &str, t: Option<&Table>) -> String {
        // Drive both the static (projection) and runtime path: infer first (this
        // is what a projected expression hits), falling back to eval.
        let ctx = crate::clock::EvalCtx::test_default();
        let e = pexpr(sql).expect("parse");
        let scope = scope_of(t);
        crate::eval::infer_type(&e, &scope)
            .err()
            .or_else(|| crate::eval::eval(&e, &scope, &[Datum::Null, Datum::Null], &ctx).err())
            .expect("expected error")
            .into_pg()
            .code
    }

    #[test]
    fn string_length_upper_lower() {
        assert_eq!(ev("length('hello')"), Datum::Int4(5));
        assert_eq!(ev("char_length('abc')"), Datum::Int4(3));
        assert_eq!(ev("character_length('')"), Datum::Int4(0));
        assert_eq!(ev("upper('aBc')"), Datum::Text("ABC".into()));
        assert_eq!(ev("lower('aBc')"), Datum::Text("abc".into()));
        // strict: NULL argument → NULL.
        assert_eq!(ev("length(null)"), Datum::Null);
        assert_eq!(ev("upper(null)"), Datum::Null);
    }

    #[test]
    fn range_constructors_and_accessors_keep_typed_bounds() {
        let text = |sql: &str| {
            String::from_utf8(crabka_pgtypes::encoding::encode_text(
                &ev(sql),
                &jiff::tz::TimeZone::UTC,
            ))
            .expect("range output is UTF-8")
        };
        assert_eq!(text("int4range(1, 4, '(]')"), "[2,5)");
        assert_eq!(text("int4range(int4range(1, 4))"), "[1,4)");
        assert_eq!(text("int4multirange()"), "{}");
        assert_eq!(text("multirange(int4range(1, 4))"), "{[1,4)}");
        assert_eq!(
            text("lower(int4multirange(int4range(1, 4), int4range(8, 10)))"),
            "1"
        );
        assert_eq!(
            text("upper(int4multirange(int4range(1, 4), int4range(8, 10)))"),
            "10"
        );
        assert_eq!(text("int4range(1, 4)::int4multirange"), "{[1,4)}");
        assert_eq!(
            text("int4multirange(int4range(5, 8), int4range(1, 5))"),
            "{[1,8)}"
        );
        assert_eq!(
            ev("int4range(1, 10) @> int4multirange(int4range(2, 4), int4range(6, 8))"),
            Datum::Bool(true)
        );
        assert_eq!(
            ev("int4range(1, 3) && int4multirange(int4range(2, 4), int4range(6, 8))"),
            Datum::Bool(true)
        );
        assert_eq!(
            ev("int4multirange(int4range(2, 4), int4range(6, 8)) @> '3'"),
            Datum::Bool(true)
        );
        assert_eq!(
            ev("int4multirange(int4range(2, 4), int4range(6, 8)) << int4range(9, 10)"),
            Datum::Bool(true)
        );
        assert_eq!(
            ev(
                "int4multirange(int4range(2, 4), int4range(6, 8)) &< int4multirange(int4range(7, 10))"
            ),
            Datum::Bool(true)
        );
        assert_eq!(
            text(
                "int4multirange(int4range(1, 5), int4range(10, 15)) * int4multirange(int4range(3, 12))"
            ),
            "{[3,5),[10,12)}"
        );
        assert_eq!(
            text(
                "int4multirange(int4range(1, 5), int4range(10, 15)) - int4multirange(int4range(3, 12))"
            ),
            "{[1,3),[12,15)}"
        );
        assert_eq!(
            ev("multirange_contains_range(int4multirange(int4range(1, 5)), int4range(2, 4))"),
            Datum::Bool(true)
        );
        assert_eq!(
            text("range_merge(int4multirange(int4range(1, 5), int4range(10, 15)))"),
            "[1,15)"
        );
        assert_eq!(ev("lower(int4range(1, 4))"), Datum::Int4(1));
        assert_eq!(ev("upper(int4range(1, 4))"), Datum::Int4(4));
        assert_eq!(ev("isempty('empty'::int4range)"), Datum::Bool(true));
        assert_eq!(ev("lower_inf(int4range(null, 4))"), Datum::Bool(true));
        assert_eq!(ev("upper_inc(int4range(1, 4, '[]'))"), Datum::Bool(false));
        assert_eq!(ev("numrange(1.0, 3.0) @> 2.0"), Datum::Bool(true));
        assert_eq!(
            ev("numrange(1.0, 3.0) && numrange(2.0, 4.0)"),
            Datum::Bool(true)
        );
        assert_eq!(
            ev("numrange(1.0, 2.0) -|- numrange(2.0, 3.0)"),
            Datum::Bool(true)
        );
        assert_eq!(
            ev("numrange(1.0, 2.0) << numrange(3.0, 4.0)"),
            Datum::Bool(true)
        );
        assert_eq!(
            ev("numrange(1.0, 2.0) &< numrange(1.5, 3.0)"),
            Datum::Bool(true)
        );
        assert_eq!(text("numrange(1.0, 2.0) + numrange(2.0, 3.0)"), "[1.0,3.0)");
        assert_eq!(text("numrange(1.0, 2.0) + '[2.0,3.0)'"), "[1.0,3.0)");
        assert_eq!(text("numrange(1.0, 2.0) * numrange(1.5, 3.0)"), "[1.5,2.0)");
        assert_eq!(text("numrange(1.0, 2.0) - numrange(1.5, 3.0)"), "[1.0,1.5)");
        assert_eq!(
            text("numrange(1.0, 3.0) - numrange(1.0, 3.0, '()')"),
            "[1.0,1.0]"
        );
        assert_eq!(
            ev("numrange(1.0, 2.0, '[]') &< numrange(1.0, 2.0)"),
            Datum::Bool(false)
        );
        assert_eq!(
            text("range_merge(numrange(1.0, 2.0), numrange(2.5, 3.0))"),
            "[1.0,3.0)"
        );
        let _ = crabka_pgtypes::usertype::register(
            "range_constructor_test",
            crabka_pgtypes::usertype::UserTypeBody::Range(crabka_pgtypes::usertype::RangeBody {
                subtype: ColumnType::Text,
                collation: None,
            }),
        );
        assert_eq!(text("range_constructor_test('a', 'z')"), "[a,z)");
        crabka_pgtypes::usertype::unregister("range_constructor_test");
    }

    #[test]
    fn trims_default_and_with_set() {
        assert_eq!(ev("btrim('  hi  ')"), Datum::Text("hi".into()));
        assert_eq!(ev("ltrim('  hi  ')"), Datum::Text("hi  ".into()));
        assert_eq!(ev("rtrim('  hi  ')"), Datum::Text("  hi".into()));
        assert_eq!(ev("btrim('xxhixx', 'x')"), Datum::Text("hi".into()));
        assert_eq!(ev("ltrim('xyhi', 'xy')"), Datum::Text("hi".into()));
        // a NULL character-set argument → NULL (strict).
        assert_eq!(ev("btrim('hi', null)"), Datum::Null);
    }

    #[test]
    fn substr_and_replace() {
        assert_eq!(ev("substr('abcdef', 2, 3)"), Datum::Text("bcd".into()));
        assert_eq!(ev("substring('abcdef', 4)"), Datum::Text("def".into()));
        // start before position 1: the count is consumed from position 1.
        assert_eq!(ev("substr('abcdef', 0, 2)"), Datum::Text("a".into()));
        assert_eq!(ev("substr('abc', 5)"), Datum::Text("".into()));
        assert_eq!(
            ev("replace('a.b.c', '.', '-')"),
            Datum::Text("a-b-c".into())
        );
        // A negative substring length is PostgreSQL's 22011 (substring_error).
        let ctx = crate::clock::EvalCtx::test_default();
        let err = crate::eval::eval(
            &pexpr("substr('abc', 1, -1)").expect("p"),
            &Scope::empty(),
            &[],
            &ctx,
        )
        .expect_err("neg len");
        assert_eq!(err.into_pg().code, "22011");
    }

    #[test]
    fn concat_skips_nulls_and_renders_each() {
        assert_eq!(ev("concat('a', 'b', 'c')"), Datum::Text("abc".into()));
        assert_eq!(ev("concat('x', null, 'y')"), Datum::Text("xy".into()));
        assert_eq!(ev("concat(1, '+', 2)"), Datum::Text("1+2".into()));
        // all-NULL (and zero-arg) concat is the empty string, never NULL.
        assert_eq!(ev("concat(null, null)"), Datum::Text("".into()));
        assert_eq!(ev("concat()"), Datum::Text("".into()));
    }

    #[test]
    fn abs_and_mod() {
        let ctx = crate::clock::EvalCtx::test_default();
        let num = |s: &str| Datum::Numeric(crabka_pgtypes::numeric::parse(s).expect("n"));
        assert_eq!(ev("abs(-5)"), Datum::Int4(5));
        assert_eq!(ev("abs(7)"), Datum::Int4(7));
        // SP32: a bare decimal is numeric, so abs over it is numeric (float8 abs
        // is reached via an explicit cast).
        assert_eq!(ev("abs(-2.5)"), num("2.5"));
        assert_eq!(ev("abs(2.5)"), num("2.5"));
        assert_eq!(ev("abs(-2.5::float8)"), Datum::Float8(2.5));
        assert_eq!(ev("mod(11, 3)"), Datum::Int4(2));
        assert_eq!(ev("mod(-11, 3)"), Datum::Int4(-2));
        // SP32: numeric mod (the remainder takes the dividend's sign).
        assert_eq!(ev("mod(7.5, 2)"), num("1.5"));
        assert_eq!(ev("abs(null)"), Datum::Null);
        // abs overflow at i32::MIN is 22003. A bare `-2147483648` literal is a
        // negated int8, so the overflow only arises on an actual int4 column
        // value; evaluate `abs(n)` against a row holding Int4(i32::MIN).
        let t = table();
        let err = crate::eval::eval(
            &pexpr("abs(n)").expect("p"),
            &scope_of(Some(&t)),
            &[Datum::Null, Datum::Int4(i32::MIN)],
            &ctx,
        )
        .expect_err("overflow");
        assert_eq!(err.into_pg().code, "22003");
        // mod by zero is 22012.
        let err = crate::eval::eval(&pexpr("mod(1, 0)").expect("p"), &Scope::empty(), &[], &ctx)
            .expect_err("div0");
        assert_eq!(err.into_pg().code, "22012");
    }

    #[test]
    fn coalesce_short_circuits_and_nullif() {
        assert_eq!(ev("coalesce(null, null, 3)"), Datum::Int4(3));
        assert_eq!(ev("coalesce(null, null)"), Datum::Null);
        // short-circuit: the un-taken `1/0` branch is never evaluated.
        assert_eq!(ev("coalesce(7, 1/0)"), Datum::Int4(7));
        assert_eq!(ev("nullif(5, 5)"), Datum::Null);
        assert_eq!(ev("nullif(5, 6)"), Datum::Int4(5));
        assert_eq!(ev("nullif(null, 1)"), Datum::Null);
    }

    #[test]
    fn greatest_least_ignore_nulls() {
        assert_eq!(ev("greatest(3, 7, 2)"), Datum::Int4(7));
        assert_eq!(ev("least(3, 7, 2)"), Datum::Int4(2));
        assert_eq!(ev("greatest(null, 4, null)"), Datum::Int4(4));
        assert_eq!(ev("least('b', 'a', 'c')"), Datum::Text("a".into()));
        assert_eq!(ev("greatest(null, null)"), Datum::Null);
    }

    #[test]
    fn result_types_for_row_description() {
        let t = table();
        let scope = scope_of(Some(&t));
        let ty = |sql: &str| crate::eval::infer_type(&pexpr(sql).expect("p"), &scope).expect("ty");
        assert_eq!(ty("length(s)"), ColumnType::Int4);
        assert_eq!(ty("upper(s)"), ColumnType::Text);
        assert_eq!(ty("substr(s, 1, 2)"), ColumnType::Text);
        assert_eq!(ty("concat(s, n)"), ColumnType::Text);
        assert_eq!(ty("abs(n)"), ColumnType::Int4);
        assert_eq!(ty("mod(n, 2)"), ColumnType::Int4);
        // coalesce(int4, int8) unifies to int8.
        assert_eq!(ty("coalesce(n, 3000000000)"), ColumnType::Int8);
        assert_eq!(ty("nullif(s, 'x')"), ColumnType::Text);
        // `||` is text; one operand text is enough.
        assert_eq!(ty("'id=' || n"), ColumnType::Text);
    }

    #[test]
    fn database_encoding_is_utf8() {
        let expr = pexpr("getdatabaseencoding()").expect("parse");
        assert_eq!(
            crate::eval::infer_type(&expr, &Scope::empty()).expect("type"),
            ColumnType::Text
        );
        assert_eq!(ev("getdatabaseencoding()"), Datum::Text("UTF8".into()));
        assert_eq!(err_code("getdatabaseencoding(1)", None), "42883");
    }

    #[test]
    fn error_surface() {
        let t = table();
        // unknown function → 42883.
        assert_eq!(err_code("frobnicate(s)", Some(&t)), "42883");
        // wrong arity → 42883.
        assert_eq!(err_code("length(s, s)", Some(&t)), "42883");
        // bad argument type in a projected position → 42883.
        assert_eq!(err_code("upper(n)", Some(&t)), "42883");
        assert_eq!(err_code("abs(s)", Some(&t)), "42883");
        // `int || int` (neither operand text) → 42883.
        assert_eq!(err_code("n || n", Some(&t)), "42883");
        // incompatible coalesce/greatest types → 42804.
        assert_eq!(err_code("coalesce(n, s)", Some(&t)), "42804");
        // DISTINCT on a scalar function → 42809.
        assert_eq!(err_code("upper(distinct s)", Some(&t)), "42809");
    }

    #[test]
    fn concat_operator_evaluates_and_propagates_null() {
        assert_eq!(ev("'a' || 'b' || 'c'"), Datum::Text("abc".into()));
        assert_eq!(ev("'id=' || 5"), Datum::Text("id=5".into()));
        assert_eq!(ev("'x' || null"), Datum::Null);
    }

    #[test]
    fn rounding_family_preserves_type() {
        let num = |s: &str| Datum::Numeric(crabka_pgtypes::numeric::parse(s).expect("n"));
        // int in → int out (unchanged)
        assert_eq!(ev("floor(5)"), Datum::Int4(5));
        assert_eq!(ev("ceil(5)"), Datum::Int4(5));
        assert_eq!(ev("trunc(5)"), Datum::Int4(5));
        assert_eq!(ev("round(5)"), Datum::Int4(5));
        assert_eq!(ev("sign(-7)"), Datum::Int4(-1));
        // numeric in → numeric out
        assert_eq!(ev("floor(2.9)"), num("2"));
        assert_eq!(ev("ceiling(2.1)"), num("3"));
        assert_eq!(ev("round(2.5)"), num("3"));
        assert_eq!(ev("round(2.567, 2)"), num("2.57"));
        assert_eq!(ev("trunc(2.99)"), num("2"));
        assert_eq!(ev("trunc(2.567, 1)"), num("2.5"));
        assert_eq!(ev("sign(-0.3)"), num("-1"));
        // float8 in → float8 out (round half-to-even)
        assert_eq!(ev("floor(2.9::float8)"), Datum::Float8(2.0));
        assert_eq!(ev("round(2.5::float8)"), Datum::Float8(2.0)); // half-to-even
        assert_eq!(ev("round(3.5::float8)"), Datum::Float8(4.0));
        assert_eq!(ev("sign(-3.0::float8)"), Datum::Float8(-1.0));
        // two-arg round/trunc on an int → numeric
        assert_eq!(ev("round(1234, -2)"), num("1200"));
        // strict NULL
        assert_eq!(ev("floor(null)"), Datum::Null);
    }

    #[test]
    fn rounding_family_types_and_errors() {
        let t = table();
        let ty = |sql: &str| {
            crate::eval::infer_type(&pexpr(sql).expect("p"), &scope_of(Some(&t))).expect("ty")
        };
        assert_eq!(ty("floor(n)"), ColumnType::Int4);
        assert_eq!(ty("round(2.5)"), ColumnType::Numeric(None));
        assert_eq!(ty("floor(2.5::float8)"), ColumnType::Float8);
        assert_eq!(ty("round(2.5, 1)"), ColumnType::Numeric(None));
        // two-arg round on a float8 first arg → 42883 (PG has no round(float8,int)).
        assert_eq!(err_code("round(2.5::float8, 1)", Some(&t)), "42883");
        // non-numeric arg → 42883.
        assert_eq!(err_code("floor(s)", Some(&t)), "42883");
    }

    #[test]
    fn transcendental_family_returns_float8() {
        assert_eq!(ev("sqrt(4)"), Datum::Float8(2.0));
        assert_eq!(ev("sqrt(2.25::float8)"), Datum::Float8(1.5));
        assert_eq!(ev("power(2, 10)"), Datum::Float8(1024.0));
        assert_eq!(ev("pow(2, 0.5::float8)"), Datum::Float8(2.0_f64.sqrt()));
        assert_eq!(ev("exp(0)"), Datum::Float8(1.0));
        assert_eq!(ev("ln(1)"), Datum::Float8(0.0));
        assert_eq!(ev("log(1000)"), Datum::Float8(3.0));
        assert_eq!(ev("pi()"), Datum::Float8(std::f64::consts::PI));
        // strict NULL
        assert_eq!(ev("sqrt(null)"), Datum::Null);
    }

    #[test]
    fn string_family_values() {
        assert_eq!(ev("lpad('hi', 5)"), Datum::Text("   hi".into()));
        assert_eq!(ev("lpad('hi', 5, '*')"), Datum::Text("***hi".into()));
        assert_eq!(ev("lpad('hello', 3)"), Datum::Text("hel".into()));
        assert_eq!(ev("rpad('hi', 5, 'ab')"), Datum::Text("hiaba".into()));
        assert_eq!(ev("rpad('hello', 3)"), Datum::Text("hel".into()));
        assert_eq!(ev("left('abcdef', 2)"), Datum::Text("ab".into()));
        assert_eq!(ev("left('abcdef', -2)"), Datum::Text("abcd".into()));
        assert_eq!(ev("right('abcdef', 2)"), Datum::Text("ef".into()));
        assert_eq!(ev("right('abcdef', -2)"), Datum::Text("cdef".into()));
        assert_eq!(ev("repeat('ab', 3)"), Datum::Text("ababab".into()));
        assert_eq!(ev("repeat('ab', 0)"), Datum::Text("".into()));
        assert_eq!(ev("reverse('abc')"), Datum::Text("cba".into()));
        assert_eq!(
            ev("initcap('hello WORLD')"),
            Datum::Text("Hello World".into())
        );
        assert_eq!(ev("strpos('abcde', 'cd')"), Datum::Int4(3));
        assert_eq!(ev("strpos('abcde', 'xy')"), Datum::Int4(0));
        assert_eq!(ev("strpos('abc', '')"), Datum::Int4(1));
        assert_eq!(ev("ascii('A')"), Datum::Int4(65));
        assert_eq!(ev("ascii('')"), Datum::Int4(0));
        assert_eq!(ev("chr(65)"), Datum::Text("A".into()));
        // strict NULL
        assert_eq!(ev("lpad(null, 5)"), Datum::Null);
        assert_eq!(ev("reverse(null)"), Datum::Null);
    }

    #[test]
    fn string_family_types_and_errors() {
        let t = table();
        let ty = |sql: &str| {
            crate::eval::infer_type(&pexpr(sql).expect("p"), &scope_of(Some(&t))).expect("ty")
        };
        assert_eq!(ty("lpad(s, 5)"), ColumnType::Text);
        assert_eq!(ty("strpos(s, 'x')"), ColumnType::Int4);
        assert_eq!(ty("ascii(s)"), ColumnType::Int4);
        assert_eq!(ty("chr(n)"), ColumnType::Text);
        // chr(0) and an out-of-range code point → 54000.
        assert_eq!(ec_eval("chr(0)"), "54000");
        assert_eq!(ec_eval("chr(99999999999)"), "54000");
        // wrong arg type → 42883.
        assert_eq!(err_code("left(n, 2)", Some(&t)), "42883");
        assert_eq!(err_code("ascii(n)", Some(&t)), "42883");
        // an adversarially huge lpad/rpad width or repeat count is 54000
        // ("requested length too large"), guarded against OOM — not a process abort.
        assert_eq!(ec_eval("lpad('x', 9999999999)"), "54000");
        assert_eq!(ec_eval("rpad('x', 9999999999)"), "54000");
        assert_eq!(ec_eval("repeat('x', 9999999999)"), "54000");
    }

    #[test]
    fn transcendental_domain_errors() {
        let t = table();
        let ty = |sql: &str| {
            crate::eval::infer_type(&pexpr(sql).expect("p"), &scope_of(Some(&t))).expect("ty")
        };
        assert_eq!(ty("sqrt(n)"), ColumnType::Float8);
        assert_eq!(ty("pi()"), ColumnType::Float8);
        // sqrt(negative) → 2201F
        assert_eq!(ec_eval("sqrt(-1)"), "2201F");
        // ln/log of a non-positive number → 2201E
        assert_eq!(ec_eval("ln(0)"), "2201E");
        assert_eq!(ec_eval("ln(-1)"), "2201E");
        assert_eq!(ec_eval("log(0)"), "2201E");
        // zero to a negative power → 2201F
        assert_eq!(ec_eval("power(0, -1)"), "2201F");
        // wrong arity → 42883
        assert_eq!(err_code("pi(1)", Some(&t)), "42883");
        assert_eq!(err_code("power(2)", Some(&t)), "42883");
    }

    #[test]
    fn transcendentals_are_numeric_for_numeric_input() {
        let num = |s: &str| Datum::Numeric(crabka_pgtypes::numeric::parse(s).expect("n"));
        // numeric in -> numeric out (oracle-validated exact values from pgtypes unit tests)
        assert_eq!(ev("sqrt(2.0)"), num("1.414213562373095"));
        assert_eq!(ev("exp(1.0)"), num("2.7182818284590452"));
        assert_eq!(ev("ln(2.0)"), num("0.6931471805599453"));
        assert_eq!(ev("power(2.0, 3.0)"), num("8.0000000000000000"));
        // int in -> float8 out (unchanged)
        assert_eq!(ev("sqrt(4)"), Datum::Float8(2.0));
        assert_eq!(ev("exp(0)"), Datum::Float8(1.0));
        // float8 in -> float8 out (unchanged)
        assert_eq!(ev("sqrt(4.0::float8)"), Datum::Float8(2.0));
        // strict NULL
        assert_eq!(ev("sqrt(null)"), Datum::Null);
        // numeric-path domain errors (2201E/2201F) and overflow (22003) surface
        // end-to-end from SQL — never a panic, hang, or silently-wrong value.
        assert_eq!(ec_eval("sqrt(-1::numeric)"), "2201F");
        assert_eq!(ec_eval("ln(0::numeric)"), "2201E");
        assert_eq!(ec_eval("power(0::numeric, -1::numeric)"), "2201F");
        assert_eq!(ec_eval("exp(6000::numeric)"), "22003");
        assert_eq!(ec_eval("power(10::numeric, 200000::numeric)"), "22003");
    }

    #[test]
    fn transcendental_result_types() {
        let t = table_n();
        let ty = |sql: &str| {
            crate::eval::infer_type(&pexpr(sql).expect("p"), &scope_of(Some(&t))).expect("ty")
        };
        assert_eq!(ty("sqrt(qn)"), ColumnType::Numeric(None)); // qn is numeric
        assert_eq!(ty("ln(qn)"), ColumnType::Numeric(None));
        assert_eq!(ty("sqrt(4)"), ColumnType::Float8); // int literal
        assert_eq!(ty("sqrt(4.0::float8)"), ColumnType::Float8);
        assert_eq!(ty("power(qn, 2)"), ColumnType::Numeric(None)); // numeric base
        assert_eq!(ty("power(2, 3)"), ColumnType::Float8); // all-int
    }
}
