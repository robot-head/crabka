//! The SQL surface of the system identifier types — `pg_lsn`'s arithmetic and
//! the handful of functions `PostgreSQL` declares only over `xid`/`xid8`.
//!
//! Only `pg_lsn` has arithmetic at all. `pg_operator` gives it three entries:
//! `pg_lsn - pg_lsn` returning `numeric`, and `pg_lsn ± numeric` returning
//! `pg_lsn` (with a reflected `numeric + pg_lsn`). The other five identifier
//! types have no arithmetic whatever — `'1'::oid + 1` is 42883 — which is why
//! this table is exhaustive rather than deferring to the numeric family.

use crabka_pgparser::ast::{BinaryOp, Expr, FuncCall};
use crabka_pgtypes::{ColumnType, Datum, sysid};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    eval::infer_type,
    func::{checked_args, require_arity, undefined_function},
    scope::Scope,
};

/// The functions declared only over the system identifier types.
fn sysid_func(name: &str) -> Option<SysidFunc> {
    Some(match name {
        "xid8cmp" => SysidFunc::Xid8Cmp,
        "xid8_larger" => SysidFunc::Xid8Larger,
        "xid8_smaller" => SysidFunc::Xid8Smaller,
        // `xid(xid8)` is the function-call spelling of the one cast in the
        // family; there is no `xid8(xid)` going the other way.
        "xid" => SysidFunc::ToXid,
        // `pg_lsn(numeric)` is a plain function, not a `pg_cast` entry, so
        // `1::numeric::pg_lsn` is 42846 while `pg_lsn(1::numeric)` works.
        "pg_lsn" => SysidFunc::ToPgLsn,
        "pg_lsn_cmp" => SysidFunc::PgLsnCmp,
        "pg_lsn_larger" => SysidFunc::PgLsnLarger,
        "pg_lsn_smaller" => SysidFunc::PgLsnSmaller,
        _ => return None,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SysidFunc {
    /// `xid8cmp(xid8, xid8) → integer`.
    Xid8Cmp,
    Xid8Larger,
    Xid8Smaller,
    /// `xid(xid8) → xid`.
    ToXid,
    /// `pg_lsn(numeric) → pg_lsn`.
    ToPgLsn,
    /// `pg_lsn_cmp(pg_lsn, pg_lsn) → integer`.
    PgLsnCmp,
    PgLsnLarger,
    PgLsnSmaller,
}

/// Is `name` one of this module's functions? (`func::is_scalar` folds this in.)
pub(crate) fn is_sysid_func(name: &str) -> bool {
    sysid_func(name).is_some()
}

/// Statically infer a system identifier call's result type, validating its
/// arity and argument types.
pub(crate) fn sysid_func_result_type(
    fc: &FuncCall,
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    let f = sysid_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let (params, result) = signature(f);
    if args.len() != params.len() {
        return Err(wrong_signature(fc, args, scope));
    }
    for (index, want) in params.iter().enumerate() {
        if !accepts(&args[index], *want, scope)? {
            return Err(wrong_signature(fc, args, scope));
        }
    }
    Ok(result)
}

/// Each function's declared parameter list and result type, as `pg_proc`
/// records them.
fn signature(f: SysidFunc) -> (&'static [ColumnType], ColumnType) {
    const XID8_PAIR: &[ColumnType] = &[ColumnType::Xid8, ColumnType::Xid8];
    const LSN_PAIR: &[ColumnType] = &[ColumnType::PgLsn, ColumnType::PgLsn];
    const ONE_XID8: &[ColumnType] = &[ColumnType::Xid8];
    const ONE_NUMERIC: &[ColumnType] = &[ColumnType::Numeric(None)];
    match f {
        SysidFunc::Xid8Cmp => (XID8_PAIR, ColumnType::Int4),
        SysidFunc::Xid8Larger | SysidFunc::Xid8Smaller => (XID8_PAIR, ColumnType::Xid8),
        SysidFunc::ToXid => (ONE_XID8, ColumnType::Xid),
        SysidFunc::ToPgLsn => (ONE_NUMERIC, ColumnType::PgLsn),
        SysidFunc::PgLsnCmp => (LSN_PAIR, ColumnType::Int4),
        SysidFunc::PgLsnLarger | SysidFunc::PgLsnSmaller => (LSN_PAIR, ColumnType::PgLsn),
    }
}

/// Does `arg` satisfy a parameter declared `want`? An `unknown` literal always
/// does — the parameter type is what its input function will be.
fn accepts(arg: &Expr, want: ColumnType, scope: &Scope) -> Result<bool, ExecError> {
    if crate::eval::is_unknown_literal(arg) {
        return Ok(true);
    }
    let ty = infer_type(arg, scope)?;
    Ok(ty.storage_type() == want.storage_type() || (want.is_numeric() && ty.is_numeric()))
}

/// 42883 naming the argument types the call actually carried, which is what
/// PostgreSQL prints — never a literal `(...)`.
fn wrong_signature(fc: &FuncCall, args: &[Expr], scope: &Scope) -> ExecError {
    let spelled: Vec<&str> = args
        .iter()
        .map(|arg| {
            if crate::eval::is_unknown_literal(arg) {
                "unknown"
            } else {
                infer_type(arg, scope).map_or("unknown", ColumnType::name)
            }
        })
        .collect();
    ExecError::UndefinedFunction(format!(
        "function {}({}) does not exist",
        fc.name,
        spelled.join(", ")
    ))
}

/// Evaluate a system identifier function call.
pub(crate) fn eval_sysid(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let f = sysid_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(eval_child(arg)?);
    }
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    match f {
        SysidFunc::Xid8Cmp => {
            require_arity(fc, values.len() == 2)?;
            let (a, b) = (xid8_arg(fc, &values[0])?, xid8_arg(fc, &values[1])?);
            Ok(Datum::Int4(compare_code(a, b)))
        }
        SysidFunc::Xid8Larger | SysidFunc::Xid8Smaller => {
            require_arity(fc, values.len() == 2)?;
            let (a, b) = (xid8_arg(fc, &values[0])?, xid8_arg(fc, &values[1])?);
            let larger = f == SysidFunc::Xid8Larger;
            Ok(Datum::Xid8(if (a > b) == larger { a } else { b }))
        }
        SysidFunc::ToXid => {
            require_arity(fc, values.len() == 1)?;
            let value = xid8_arg(fc, &values[0])?;
            #[expect(
                clippy::cast_possible_truncation,
                reason = "xid8toxid keeps the low 32 bits; the epoch is what xid8 adds"
            )]
            Ok(Datum::Xid(value as u32))
        }
        SysidFunc::ToPgLsn => {
            require_arity(fc, values.len() == 1)?;
            let Datum::Numeric(value) = coerce(&values[0], ColumnType::Numeric(None), ctx)? else {
                return Err(wrong_arg(fc, &values[0]));
            };
            Ok(Datum::PgLsn(sysid::lsn_from_numeric(&value)?))
        }
        SysidFunc::PgLsnCmp => {
            require_arity(fc, values.len() == 2)?;
            let (a, b) = (lsn_arg(fc, &values[0])?, lsn_arg(fc, &values[1])?);
            Ok(Datum::Int4(compare_code(a, b)))
        }
        SysidFunc::PgLsnLarger | SysidFunc::PgLsnSmaller => {
            require_arity(fc, values.len() == 2)?;
            let (a, b) = (lsn_arg(fc, &values[0])?, lsn_arg(fc, &values[1])?);
            let larger = f == SysidFunc::PgLsnLarger;
            Ok(Datum::PgLsn(if (a > b) == larger { a } else { b }))
        }
    }
}

/// `xid8cmp` / `pg_lsn_cmp`: -1, 0 or 1.
fn compare_code<T: Ord>(a: T, b: T) -> i32 {
    match a.cmp(&b) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

/// Does `op` have a `pg_lsn` overload at all? No other type in the family has
/// arithmetic, so this is the whole set.
fn is_sysid_operator(op: BinaryOp) -> bool {
    matches!(op, BinaryOp::Add | BinaryOp::Sub)
}

/// The static result type of a system identifier operator, or `None` when no
/// operand belongs to the family.
pub(crate) fn sysid_operator_result_type(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    scope: &Scope,
) -> Result<Option<ColumnType>, ExecError> {
    let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
    let family = |ty: ColumnType| {
        matches!(
            ty.storage_type(),
            ColumnType::Oid
                | ColumnType::Xid
                | ColumnType::Xid8
                | ColumnType::Cid
                | ColumnType::Tid
                | ColumnType::PgLsn
        )
    };
    if !family(lt) && !family(rt) {
        return Ok(None);
    }
    if !matches!(
        op,
        BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Mod
            | BinaryOp::Pow
    ) {
        return Ok(None);
    }
    let numeric = |ty: ColumnType| {
        matches!(
            ty.storage_type(),
            ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8
        ) || ty.is_numeric()
    };
    // An `unknown` literal beside a `pg_lsn` under `-` is a `pg_lsn`, because
    // PostgreSQL prefers the exact-match candidate; under `+` there is no
    // `pg_lsn + pg_lsn`, so the literal becomes the `numeric` the one candidate
    // wants.
    let lt = resolve_unknown(lt, left, op, rt);
    let rt = resolve_unknown(rt, right, op, lt);
    let result = match op {
        BinaryOp::Sub if lt == ColumnType::PgLsn && rt == ColumnType::PgLsn => {
            ColumnType::Numeric(None)
        }
        BinaryOp::Add | BinaryOp::Sub if lt == ColumnType::PgLsn && numeric(rt) => {
            ColumnType::PgLsn
        }
        // Only `+` has a reflected form: `1::numeric - '0/1'::pg_lsn` does not
        // exist.
        BinaryOp::Add if numeric(lt) && rt == ColumnType::PgLsn => ColumnType::PgLsn,
        _ => {
            return Err(crate::eval::undefined_operator(
                crate::eval::op_spelling(op),
                lt,
                rt,
            ));
        }
    };
    Ok(Some(result))
}

/// Apply a system identifier operator, or return `None` when no operand is one.
pub(crate) fn apply_sysid_operator(
    op: BinaryOp,
    left: &Datum,
    right: &Datum,
) -> Result<Option<Datum>, ExecError> {
    if !is_sysid_operator(op) {
        return Ok(None);
    }
    let result = match (op, left, right) {
        (BinaryOp::Sub, Datum::PgLsn(a), Datum::PgLsn(b)) => {
            Datum::Numeric(sysid::lsn_diff(*a, *b))
        }
        (BinaryOp::Add, Datum::PgLsn(a), Datum::Numeric(n)) => Datum::PgLsn(sysid::lsn_add(*a, n)?),
        (BinaryOp::Sub, Datum::PgLsn(a), Datum::Numeric(n)) => Datum::PgLsn(sysid::lsn_sub(*a, n)?),
        // The reflected form is a SQL-language function in PostgreSQL
        // (`numeric_pl_pg_lsn` is `SELECT $2 + $1`), so its failures carry the
        // CONTEXT line naming it — the one observable difference between
        // `lsn + n` and `n + lsn`.
        (BinaryOp::Add, Datum::Numeric(n), Datum::PgLsn(a)) => {
            Datum::PgLsn(sysid::lsn_add(*a, n).map_err(reflected_add_context)?)
        }
        (_, Datum::PgLsn(_) | Datum::Null, Datum::PgLsn(_) | Datum::Null)
            if left.is_null() || right.is_null() =>
        {
            Datum::Null
        }
        _ => return Ok(None),
    };
    Ok(Some(result))
}

/// Re-raise a `numeric + pg_lsn` failure with the CONTEXT PostgreSQL adds for
/// the SQL-language wrapper the reflected operator is implemented as.
fn reflected_add_context(error: crabka_pgtypes::TypeError) -> ExecError {
    ExecError::Remote(
        crabka_pgwire::error::PgError::error(error.sqlstate(), error.to_string())
            .with_context("SQL function \"numeric_pl_pg_lsn\" statement 1"),
    )
}

fn resolve_unknown(ty: ColumnType, expr: &Expr, op: BinaryOp, other: ColumnType) -> ColumnType {
    if !crate::eval::is_unknown_literal(expr) || other.storage_type() != ColumnType::PgLsn {
        return ty;
    }
    if op == BinaryOp::Sub {
        ColumnType::PgLsn
    } else {
        ColumnType::Numeric(None)
    }
}

fn xid8_arg(fc: &FuncCall, value: &Datum) -> Result<u64, ExecError> {
    match value {
        Datum::Xid8(v) => Ok(*v),
        Datum::Text(text) => Ok(crabka_pgtypes::sysid::uint64_in(text, "xid8")?),
        other => Err(wrong_arg(fc, other)),
    }
}

fn lsn_arg(fc: &FuncCall, value: &Datum) -> Result<u64, ExecError> {
    match value {
        Datum::PgLsn(v) => Ok(*v),
        Datum::Text(text) => Ok(crabka_pgtypes::sysid::lsn_in(text)?),
        other => Err(wrong_arg(fc, other)),
    }
}

fn coerce(value: &Datum, to: ColumnType, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    Ok(crabka_pgtypes::cast::cast_in(
        value,
        to,
        ctx.output_style(),
    )?)
}

fn wrong_arg(fc: &FuncCall, value: &Datum) -> ExecError {
    wrong_arg_type(fc, value.column_type().unwrap_or(ColumnType::Text))
}

fn wrong_arg_type(fc: &FuncCall, ty: ColumnType) -> ExecError {
    ExecError::UndefinedFunction(format!(
        "function {}({}) does not exist",
        fc.name,
        ty.name()
    ))
}
