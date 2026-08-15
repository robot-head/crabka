//! The SQL surface of `money` — its arithmetic operators and the three
//! functions `PostgreSQL` declares only over it.
//!
//! `money` is deliberately unfriendly to type resolution: `pg_operator` gives
//! it `*` and `/` against each of `int2`, `int4`, `int8`, `float4` and `float8`
//! individually, `+` and `-` only against itself, and a `/` against itself whose
//! result is `float8`. There is no unary minus, no `%`, and no `abs`, so an
//! expression that looks like it should work often does not — which is why the
//! operator table here is exhaustive rather than falling back on the numeric
//! family's rules.

use crabka_pgparser::ast::{BinaryOp, Expr, FuncCall};
use crabka_pgtypes::{ColumnType, Datum, money};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    eval::infer_type,
    func::{checked_args, require_arity, undefined_function, undefined_function_spelled},
    scope::Scope,
};

/// The functions declared only over `money`.
fn money_func(name: &str) -> Option<MoneyFunc> {
    Some(match name {
        "cash_words" => MoneyFunc::Words,
        "cashlarger" => MoneyFunc::Larger,
        "cashsmaller" => MoneyFunc::Smaller,
        // `money(x)` is the function-call spelling of the casts from `int4`,
        // `int8` and `numeric`.
        "money" => MoneyFunc::Coerce,
        _ => return None,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MoneyFunc {
    /// `cash_words(money)` — "One hundred twenty three dollars and zero cents".
    Words,
    /// `cashlarger(money, money)`.
    Larger,
    /// `cashsmaller(money, money)`.
    Smaller,
    /// `money(int4)` / `money(int8)` / `money(numeric)`.
    Coerce,
}

/// Is `name` one of this module's functions? (`func::is_scalar` folds this in.)
pub(crate) fn is_money_func(name: &str) -> bool {
    money_func(name).is_some()
}

/// Statically infer a money call's result type, validating its arity and
/// argument types.
pub(crate) fn money_func_result_type(
    fc: &FuncCall,
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    let f = money_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    match f {
        MoneyFunc::Words => {
            require_arity(fc, args.len() == 1)?;
            require_money(fc, args, 0, scope)?;
            Ok(ColumnType::Text)
        }
        MoneyFunc::Larger | MoneyFunc::Smaller => {
            require_arity(fc, args.len() == 2)?;
            require_money(fc, args, 0, scope)?;
            require_money(fc, args, 1, scope)?;
            Ok(ColumnType::Money)
        }
        MoneyFunc::Coerce => {
            require_arity(fc, args.len() == 1)?;
            if !crate::eval::is_unknown_literal(&args[0])
                && !matches!(
                    infer_type(&args[0], scope)?.storage_type(),
                    ColumnType::Int4 | ColumnType::Int8 | ColumnType::Numeric(_)
                )
            {
                return Err(undefined_function_spelled(&fc.name, args, scope));
            }
            Ok(ColumnType::Money)
        }
    }
}

/// Evaluate a money function call.
pub(crate) fn eval_money(
    fc: &FuncCall,
    _ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let f = money_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(eval_child(arg)?);
    }
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    match f {
        MoneyFunc::Words => {
            require_arity(fc, values.len() == 1)?;
            Ok(Datum::Text(money::words(money_arg(fc, &values[0])?)))
        }
        MoneyFunc::Larger | MoneyFunc::Smaller => {
            require_arity(fc, values.len() == 2)?;
            let (a, b) = (money_arg(fc, &values[0])?, money_arg(fc, &values[1])?);
            Ok(Datum::Money(if f == MoneyFunc::Larger {
                money::larger(a, b)
            } else {
                money::smaller(a, b)
            }))
        }
        MoneyFunc::Coerce => {
            require_arity(fc, values.len() == 1)?;
            Ok(Datum::Money(match &values[0] {
                Datum::Int4(n) => money::from_int4(*n)?,
                Datum::Int8(n) => money::from_int8(*n)?,
                Datum::Numeric(value) => money::from_numeric(value)?,
                Datum::Text(text) => money::parse(text)?,
                other => {
                    return Err(ExecError::UndefinedFunction(format!(
                        "function money({}) does not exist",
                        other.column_type().map_or("unknown", ColumnType::name)
                    )));
                }
            }))
        }
    }
}

/// Does `op` have a `money` overload at all?
fn is_money_operator(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div
    )
}

/// The static result type of a money operator, or `None` when neither operand
/// is a `money`.
pub(crate) fn money_operator_result_type(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    scope: &Scope,
) -> Result<Option<ColumnType>, ExecError> {
    if !is_money_operator(op) {
        return Ok(None);
    }
    let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
    if lt != ColumnType::Money && rt != ColumnType::Money {
        return Ok(None);
    }
    // An `unknown` literal beside a `money` is a `money`, which is what makes
    // `m + '123'` and `m = '$123.00'` work.
    let lt = resolve_unknown(lt, left);
    let rt = resolve_unknown(rt, right);
    // `money` has no `numeric` operator, but `numeric → float8` is an implicit
    // cast, so `m * 2.5` resolves to `cash_mul_flt8` — and therefore rounds
    // with `rint` rather than truncating the way the integer forms do.
    let scalar = |ty: ColumnType| {
        matches!(
            ty,
            ColumnType::Int2
                | ColumnType::Int4
                | ColumnType::Int8
                | ColumnType::Float4
                | ColumnType::Float8
        ) || ty.is_numeric()
    };
    let result = match op {
        BinaryOp::Add | BinaryOp::Sub if lt == ColumnType::Money && rt == ColumnType::Money => {
            ColumnType::Money
        }
        // `money / money` is the one operator whose result leaves the type.
        BinaryOp::Div if lt == ColumnType::Money && rt == ColumnType::Money => ColumnType::Float8,
        BinaryOp::Mul | BinaryOp::Div if lt == ColumnType::Money && scalar(rt) => ColumnType::Money,
        // Only `*` has a reflected form; `2 / '1'::money` does not exist.
        BinaryOp::Mul if scalar(lt) && rt == ColumnType::Money => ColumnType::Money,
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

/// Apply a money operator, or return `None` when neither operand is a `money`.
pub(crate) fn apply_money_operator(
    op: BinaryOp,
    left: &Datum,
    right: &Datum,
) -> Result<Option<Datum>, ExecError> {
    if !is_money_operator(op) {
        return Ok(None);
    }
    if !matches!(left, Datum::Money(_)) && !matches!(right, Datum::Money(_)) {
        return Ok(None);
    }
    if left.is_null() || right.is_null() {
        return Ok(Some(Datum::Null));
    }
    // `scalar * money` is the only reflected form, and multiplication commutes,
    // so normalising the operand order here loses nothing.
    let (Datum::Money(cash), other) = (left, right) else {
        let Datum::Money(cash) = right else {
            return Ok(None);
        };
        if op != BinaryOp::Mul {
            return Ok(None);
        }
        return apply_scalar(op, *cash, left).map(Some);
    };
    match (op, other) {
        (BinaryOp::Add, Datum::Money(b)) => Ok(Some(Datum::Money(money::add(*cash, *b)?))),
        (BinaryOp::Sub, Datum::Money(b)) => Ok(Some(Datum::Money(money::sub(*cash, *b)?))),
        (BinaryOp::Div, Datum::Money(b)) => Ok(Some(Datum::Float8(money::div_cash(*cash, *b)?))),
        // A bare literal beside a `money` in `+`, `-` or `/` is another
        // `money`; beside `*` it is ambiguous in PostgreSQL too, and resolves
        // to `money` only for `+`/`-`/`/`.
        (BinaryOp::Add | BinaryOp::Sub | BinaryOp::Div, Datum::Text(text)) => {
            let b = money::parse(text)?;
            apply_money_operator(op, left, &Datum::Money(b))
        }
        (BinaryOp::Mul | BinaryOp::Div, _) => apply_scalar(op, *cash, other).map(Some),
        _ => Ok(None),
    }
}

/// `cash_mul_*` / `cash_div_*` — the scalar operand decides integer or float
/// arithmetic, and the two round differently: the integer forms truncate
/// toward zero while the float ones use `rint`.
fn apply_scalar(op: BinaryOp, cash: i64, scalar: &Datum) -> Result<Datum, ExecError> {
    let divide = op == BinaryOp::Div;
    let result = match scalar {
        Datum::Int2(n) => integer_op(divide, cash, i64::from(*n))?,
        Datum::Int4(n) => integer_op(divide, cash, i64::from(*n))?,
        Datum::Int8(n) => integer_op(divide, cash, *n)?,
        Datum::Float4(f) => float_op(divide, cash, f64::from(*f))?,
        Datum::Float8(f) => float_op(divide, cash, *f)?,
        // A `numeric` operand reaches `cash_mul_flt8`/`cash_div_flt8` through
        // its implicit widening, so it is float arithmetic and not exact.
        Datum::Numeric(value) => float_op(divide, cash, crabka_pgtypes::numeric::to_f64(value))?,
        other => {
            return Err(crate::eval::undefined_operator(
                crate::eval::op_spelling(op),
                ColumnType::Money,
                other.column_type().unwrap_or(ColumnType::Text),
            ));
        }
    };
    Ok(Datum::Money(result))
}

fn integer_op(divide: bool, cash: i64, n: i64) -> Result<i64, ExecError> {
    Ok(if divide {
        money::div_int64(cash, n)?
    } else {
        money::mul_int64(cash, n)?
    })
}

fn float_op(divide: bool, cash: i64, f: f64) -> Result<i64, ExecError> {
    Ok(if divide {
        money::div_float8(cash, f)?
    } else {
        money::mul_float8(cash, f)?
    })
}

/// An `unknown` literal beside a `money` operand takes `money`'s type, the way
/// PostgreSQL's operator resolution picks the only candidate that fits.
fn resolve_unknown(ty: ColumnType, expr: &Expr) -> ColumnType {
    if crate::eval::is_unknown_literal(expr) {
        ColumnType::Money
    } else {
        ty
    }
}

fn require_money(
    fc: &FuncCall,
    args: &[Expr],
    index: usize,
    scope: &Scope,
) -> Result<(), ExecError> {
    if crate::eval::is_unknown_literal(&args[index])
        || infer_type(&args[index], scope)?.storage_type() == ColumnType::Money
    {
        return Ok(());
    }
    Err(undefined_function_spelled(&fc.name, args, scope))
}

fn money_arg(fc: &FuncCall, value: &Datum) -> Result<i64, ExecError> {
    match value {
        Datum::Money(cash) => Ok(*cash),
        Datum::Text(text) => Ok(money::parse(text)?),
        other => Err(ExecError::UndefinedFunction(format!(
            "function {}({}) does not exist",
            fc.name,
            other.column_type().map_or("unknown", ColumnType::name)
        ))),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgparser::ast::BinaryOp;
    use crabka_pgtypes::{Datum, money};

    use super::{apply_money_operator, is_money_func};

    fn cash(text: &str) -> Datum {
        Datum::Money(money::parse(text).expect("valid money"))
    }

    fn apply(op: BinaryOp, left: &Datum, right: &Datum) -> Datum {
        apply_money_operator(op, left, right)
            .expect("no error")
            .expect("the operand pair selects a money operator")
    }

    fn rendered(value: &Datum) -> String {
        match value {
            Datum::Money(value) => money::to_text(*value),
            other => panic!("expected money, got {other:?}"),
        }
    }

    /// Integer division TRUNCATES and float division ROUNDS, which is the whole
    /// reason `'878.08'::money / 11` differs by a cent between the two.
    #[test]
    fn division_rounds_for_floats_and_truncates_for_integers() {
        let value = cash("878.08");
        for (divisor, expected) in [
            (Datum::Int8(11), "$79.82"),
            (Datum::Int4(11), "$79.82"),
            (Datum::Int2(11), "$79.82"),
            (Datum::Float8(11.0), "$79.83"),
            (Datum::Float4(11.0), "$79.83"),
        ] {
            let result = apply(BinaryOp::Div, &value, &divisor);
            assert!(rendered(&result) == expected, "{divisor:?}");
        }
    }

    /// `money / money` leaves the type entirely.
    #[test]
    fn money_divided_by_money_is_a_float() {
        assert!(apply(BinaryOp::Div, &cash("123"), &cash("2")) == Datum::Float8(61.5));
        let error =
            apply_money_operator(BinaryOp::Div, &cash("1"), &cash("0")).expect_err("zero divisor");
        assert!(error.into_pg().message == "division by zero");
    }

    /// Only `*` has a reflected form, and a bare literal is a `money` beside
    /// `+`, `-` and `/` but never beside `*`, whose candidates span every width.
    #[test]
    fn resolves_the_reflected_and_literal_forms() {
        assert!(rendered(&apply(BinaryOp::Mul, &Datum::Int4(2), &cash("123"))) == "$246.00");
        assert!(rendered(&apply(BinaryOp::Mul, &cash("123"), &Datum::Int4(2))) == "$246.00");
        assert!(
            rendered(&apply(
                BinaryOp::Add,
                &cash("123"),
                &Datum::Text("123.45".into())
            )) == "$246.45"
        );
        assert!(
            rendered(&apply(
                BinaryOp::Sub,
                &cash("123"),
                &Datum::Text("123.45".into())
            )) == "-$0.45"
        );
        // `2 / money` does not exist, so the right-hand-money path declines.
        assert!(
            apply_money_operator(BinaryOp::Div, &Datum::Int4(2), &cash("1"))
                .expect("no error")
                .is_none()
        );
    }

    /// A `numeric` operand reaches the float8 operator through its implicit
    /// widening, which is why it rounds rather than dividing exactly.
    #[test]
    fn a_numeric_operand_widens_to_float8() {
        let numeric = |text: &str| {
            Datum::Numeric(crabka_pgtypes::numeric::parse(text).expect("valid numeric"))
        };
        assert!(rendered(&apply(BinaryOp::Mul, &cash("1"), &numeric("2.5"))) == "$2.50");
        assert!(rendered(&apply(BinaryOp::Div, &cash("878.08"), &numeric("11"))) == "$79.83");
    }

    #[test]
    fn overflow_and_null_behave_as_postgresql_does() {
        let error =
            apply_money_operator(BinaryOp::Add, &cash("92233720368547758.07"), &cash("0.01"))
                .expect_err("overflow");
        assert!(error.into_pg().message == "money out of range");
        for op in [BinaryOp::Add, BinaryOp::Sub, BinaryOp::Mul, BinaryOp::Div] {
            assert!(apply(op, &cash("1"), &Datum::Null) == Datum::Null, "{op:?}");
            assert!(apply(op, &Datum::Null, &cash("1")) == Datum::Null, "{op:?}");
        }
    }

    /// The operators must decline every pair with no `money` in it, so the
    /// numeric family keeps `+ - * /` for itself.
    #[test]
    fn declines_operands_that_are_not_money() {
        for op in [BinaryOp::Add, BinaryOp::Sub, BinaryOp::Mul, BinaryOp::Div] {
            assert!(
                apply_money_operator(op, &Datum::Int4(1), &Datum::Int4(2))
                    .expect("no error")
                    .is_none(),
                "{op:?}"
            );
        }
        // `money` has no modulo, so `%` is declined outright.
        assert!(
            apply_money_operator(BinaryOp::Mod, &cash("1"), &Datum::Int4(2))
                .expect("no error")
                .is_none()
        );
        // A `money` beside an unrelated value declines rather than coercing.
        assert!(apply_money_operator(BinaryOp::Mul, &cash("1"), &Datum::Bool(true)).is_err());
    }

    #[test]
    fn owns_only_the_money_only_function_names() {
        for name in ["cash_words", "cashlarger", "cashsmaller", "money"] {
            assert!(is_money_func(name), "{name}");
        }
        for name in ["abs", "numeric", "int4", "float8", "text"] {
            assert!(!is_money_func(name), "{name}");
        }
    }
}
