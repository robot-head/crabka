//! The SQL surface of `bit` and `bit varying` — the operators, and the
//! functions `PostgreSQL` declares only over these two types.
//!
//! `bit` and `bit varying` are binary-coercible to each other, so almost every
//! operator here is declared over `bit` alone and simply accepts either
//! spelling; the one exception is `||`, which `pg_operator` declares over `bit
//! varying` and which therefore *returns* `bit varying`. That asymmetry is why
//! `pg_typeof(B'1' || B'0')` is `bit varying` while `pg_typeof(B'1' & B'0')` is
//! `bit`.
//!
//! The functions the two types share with `text` and `bytea` — `length`,
//! `octet_length`, `bit_length`, `position`, `substring`, `overlay` — are not
//! here. They live with their text counterparts, so that adding a bit overload
//! cannot accidentally take the name away from the string one.

use crabka_pgparser::ast::{BinaryOp, Expr, FuncCall};
use crabka_pgtypes::{BitString, BitwiseOp, ColumnType, Datum};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    eval::infer_type,
    func::{checked_args, require_arity, undefined_function, undefined_function_spelled},
    scope::Scope,
};

/// Is `ty` one of the two bit-string types?
pub(crate) fn is_bit_type(ty: ColumnType) -> bool {
    matches!(ty, ColumnType::Bit(_) | ColumnType::VarBit(_))
}

/// The functions declared only over `bit` — everything else the type supports
/// is an overload of a name `text` or `bytea` already owns.
fn bit_func(name: &str) -> Option<BitFunc> {
    Some(match name {
        "get_bit" => BitFunc::GetBit,
        "set_bit" => BitFunc::SetBit,
        "bit_count" => BitFunc::BitCount,
        // `varbit(x)` is the function-call spelling of a cast to `bit varying`.
        // The matching `bit(x)` is deliberately absent: `bit` is a reserved
        // word, so PostgreSQL rejects the unquoted call as a syntax error and
        // only `"bit"(x)` reaches the function. This lexer lowercases unquoted
        // identifiers and keeps quoted ones in the same `Ident` token, so the
        // two spellings are indistinguishable here — and accepting a call
        // PostgreSQL rejects is the worse of the two errors.
        "varbit" => BitFunc::Coerce,
        _ => return None,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitFunc {
    /// `get_bit(bit, int)` — bit `n`, counting from zero at the left.
    GetBit,
    /// `set_bit(bit, int, int)`.
    SetBit,
    /// `bit_count(bit)` — how many bits are set, as a `bigint`.
    BitCount,
    /// `varbit(bit varying)` / `"bit"(bit)` — the function-call cast spelling.
    Coerce,
}

/// Is `name` one of this module's functions? (`func::is_scalar` folds this in.)
pub(crate) fn is_bit_func(name: &str) -> bool {
    bit_func(name).is_some()
}

/// Statically infer a bit-function call's result type, validating its arity and
/// argument types.
pub(crate) fn bit_func_result_type(fc: &FuncCall, scope: &Scope) -> Result<ColumnType, ExecError> {
    let f = bit_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    match f {
        BitFunc::GetBit => {
            require_arity(fc, args.len() == 2)?;
            require_bit_or_bytea(fc, args, scope)?;
            require_integer(fc, args, 1, scope)?;
            Ok(ColumnType::Int4)
        }
        BitFunc::SetBit => {
            require_arity(fc, args.len() == 3)?;
            let subject = require_bit_or_bytea(fc, args, scope)?;
            require_integer(fc, args, 1, scope)?;
            require_integer(fc, args, 2, scope)?;
            Ok(subject)
        }
        BitFunc::BitCount => {
            require_arity(fc, args.len() == 1)?;
            require_bit_or_bytea(fc, args, scope)?;
            Ok(ColumnType::Int8)
        }
        BitFunc::Coerce => {
            require_arity(fc, args.len() == 1)?;
            require_bit(fc, args, scope)?;
            Ok(ColumnType::VarBit(None))
        }
    }
}

/// Evaluate a bit-function call.
pub(crate) fn eval_bit(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let f = bit_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(eval_child(arg)?);
    }
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    match f {
        BitFunc::GetBit => {
            require_arity(fc, values.len() == 2)?;
            if let Datum::Bytea(bytes) = &values[0] {
                return byte_get_bit(bytes, int64_arg(&values[1])?);
            }
            let bits = bit_arg(fc, &values[0], ctx)?;
            Ok(Datum::Int4(bits.get_bit(int_arg(&values[1])?)?))
        }
        BitFunc::SetBit => {
            require_arity(fc, values.len() == 3)?;
            if let Datum::Bytea(bytes) = &values[0] {
                return byte_set_bit(bytes, int64_arg(&values[1])?, int_arg(&values[2])?);
            }
            let bits = bit_arg(fc, &values[0], ctx)?;
            Ok(Datum::BitString(
                bits.set_bit(int_arg(&values[1])?, int_arg(&values[2])?)?,
            ))
        }
        BitFunc::BitCount => {
            require_arity(fc, values.len() == 1)?;
            if let Datum::Bytea(bytes) = &values[0] {
                return Ok(Datum::Int8(
                    bytes.iter().map(|b| i64::from(b.count_ones())).sum(),
                ));
            }
            Ok(Datum::Int8(bit_arg(fc, &values[0], ctx)?.count_ones()))
        }
        BitFunc::Coerce => {
            require_arity(fc, values.len() == 1)?;
            let bits = bit_arg(fc, &values[0], ctx)?;
            Ok(Datum::BitString(bits.relabel(true)))
        }
    }
}

/// Does `op` have a bit-string overload at all? `<<` and `>>` are shared with
/// the integer shifts and the network containment tests, and `&`/`|`/`#` with
/// the integer bitwise operators, so the operand types are what select this
/// family.
fn is_bit_operator(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Concat
            | BinaryOp::BitAnd
            | BinaryOp::BitOr
            | BinaryOp::BitXor
            | BinaryOp::Shl
            | BinaryOp::Shr
    )
}

/// The static result type of a bit-string operator, or `None` when neither
/// operand makes this the bit-string reading of the spelling.
pub(crate) fn bit_operator_result_type(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    scope: &Scope,
) -> Result<Option<ColumnType>, ExecError> {
    if !is_bit_operator(op) {
        return Ok(None);
    }
    let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
    if !is_bit_type(lt) && !is_bit_type(rt) {
        return Ok(None);
    }
    // `||` is the one operator PostgreSQL declares over `bit varying`, so it is
    // also the one whose result is `bit varying`. Everything else is declared
    // over `bit` and reports `bit`, whichever spelling the operands used.
    let result = match op {
        BinaryOp::Concat if is_bit_type(lt) && is_bit_type(rt) => ColumnType::VarBit(None),
        BinaryOp::BitAnd | BinaryOp::BitOr | BinaryOp::BitXor
            if is_bit_type(lt) && is_bit_type(rt) =>
        {
            ColumnType::Bit(None)
        }
        // `bitshiftleft(bit, integer)` / `bitshiftright(bit, integer)` — the
        // count is an `int4`, and there is no reflected form.
        BinaryOp::Shl | BinaryOp::Shr if is_bit_type(lt) && is_integral(rt) => {
            ColumnType::Bit(None)
        }
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

/// Apply a bit-string operator, or return `None` when the operands do not make
/// this the bit-string reading of the spelling.
pub(crate) fn apply_bit_operator(
    op: BinaryOp,
    left: &Datum,
    right: &Datum,
) -> Result<Option<Datum>, ExecError> {
    if !is_bit_operator(op) {
        return Ok(None);
    }
    if !matches!(left, Datum::BitString(_)) && !matches!(right, Datum::BitString(_)) {
        return Ok(None);
    }
    if left.is_null() || right.is_null() {
        return Ok(Some(Datum::Null));
    }
    // A bare literal beside a bit-string operand is a bit string in every one of
    // these operators — except the shift count, which is an integer.
    let shift = matches!(op, BinaryOp::Shl | BinaryOp::Shr);
    let (Datum::BitString(a), _) = (left, right) else {
        return Ok(None);
    };
    if shift {
        let count = match right {
            Datum::Int2(n) => i32::from(*n),
            Datum::Int4(n) => *n,
            Datum::Int8(n) => i32::try_from(*n)
                .map_err(|_| ExecError::Type(crabka_pgtypes::TypeError::Overflow))?,
            Datum::Text(text) => text
                .trim()
                .parse::<i32>()
                .map_err(|_| ExecError::Type(crabka_pgtypes::TypeError::Overflow))?,
            _ => return Ok(None),
        };
        return Ok(Some(Datum::BitString(if op == BinaryOp::Shl {
            a.shift_left(count)
        } else {
            a.shift_right(count)
        })));
    }
    let b = match right {
        Datum::BitString(b) => b.clone(),
        // The literal takes the operand's own type, which for `||` is
        // `bit varying` and for the bitwise operators `bit`.
        Datum::Text(text) => BitString::parse(text, op == BinaryOp::Concat)?,
        _ => return Ok(None),
    };
    let result = match op {
        BinaryOp::Concat => a.concat(&b)?,
        BinaryOp::BitAnd => a.bitwise(&b, BitwiseOp::And)?,
        BinaryOp::BitOr => a.bitwise(&b, BitwiseOp::Or)?,
        BinaryOp::BitXor => a.bitwise(&b, BitwiseOp::Xor)?,
        _ => return Ok(None),
    };
    Ok(Some(Datum::BitString(result)))
}

fn is_integral(ty: ColumnType) -> bool {
    matches!(ty, ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8)
}

/// Require a bit-string first argument: either bit type, or an `unknown`
/// literal PostgreSQL would coerce into one. The 42883 names EVERY argument's
/// type, as PostgreSQL's does.
fn require_bit(fc: &FuncCall, args: &[Expr], scope: &Scope) -> Result<(), ExecError> {
    if crate::eval::is_unknown_literal(&args[0]) || is_bit_type(subject_type(&args[0], scope)?) {
        return Ok(());
    }
    Err(undefined_function_spelled(&fc.name, args, scope))
}

/// `get_bit`, `set_bit` and `bit_count` each have a `bytea` overload alongside
/// the bit-string one, and the two differ in more than the argument type: the
/// `bytea` forms index from the LEAST significant bit of each byte and take a
/// `bigint` index. The declared subject type is returned so `set_bit` can report
/// the right result type.
fn require_bit_or_bytea(
    fc: &FuncCall,
    args: &[Expr],
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    // An `unknown` literal cannot choose between the two overloads, which is
    // PostgreSQL's 42725 rather than a missing function.
    if crate::eval::is_unknown_literal(&args[0]) {
        return Err(ExecError::FunctionError {
            sqlstate: "42725",
            message: format!("function {}(unknown) is not unique", fc.name),
        });
    }
    match subject_type(&args[0], scope)? {
        ColumnType::Bytea => Ok(ColumnType::Bytea),
        ty if is_bit_type(ty) => Ok(ColumnType::Bit(None)),
        _ => Err(undefined_function_spelled(&fc.name, args, scope)),
    }
}

fn require_integer(
    fc: &FuncCall,
    args: &[Expr],
    index: usize,
    scope: &Scope,
) -> Result<(), ExecError> {
    if crate::eval::is_unknown_literal(&args[index])
        || is_integral(subject_type(&args[index], scope)?)
    {
        return Ok(());
    }
    Err(undefined_function_spelled(&fc.name, args, scope))
}

/// An argument's type with any domain unwrapped: PostgreSQL's function
/// resolution coerces a domain to its base type, so `get_bit(b, dom)` where
/// `dom` is over `int` resolves.
fn subject_type(arg: &Expr, scope: &Scope) -> Result<ColumnType, ExecError> {
    Ok(infer_type(arg, scope)?.storage_type())
}

/// `byteaGetBit` — bit `n` of a `bytea`, counted from the LEAST significant bit
/// of byte `n / 8`, the opposite convention from a bit string's.
fn byte_get_bit(bytes: &[u8], index: i64) -> Result<Datum, ExecError> {
    let bits = i64::try_from(bytes.len()).unwrap_or(i64::MAX) * 8;
    if index < 0 || index >= bits {
        return Err(byte_index_out_of_range(index, bits));
    }
    let byte = bytes[usize::try_from(index / 8).expect("checked against the length")];
    Ok(Datum::Int4(i32::from((byte >> (index % 8)) & 1)))
}

/// `byteaSetBit`.
fn byte_set_bit(bytes: &[u8], index: i64, value: i32) -> Result<Datum, ExecError> {
    let bits = i64::try_from(bytes.len()).unwrap_or(i64::MAX) * 8;
    if index < 0 || index >= bits {
        return Err(byte_index_out_of_range(index, bits));
    }
    if value != 0 && value != 1 {
        return Err(ExecError::FunctionError {
            sqlstate: "22023",
            message: "new bit must be 0 or 1".into(),
        });
    }
    let mut out = bytes.to_vec();
    let mask = 1u8 << (index % 8);
    let byte = &mut out[usize::try_from(index / 8).expect("checked against the length")];
    if value == 1 {
        *byte |= mask;
    } else {
        *byte &= !mask;
    }
    Ok(Datum::Bytea(out))
}

fn byte_index_out_of_range(index: i64, bits: i64) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "2202E",
        message: format!("index {index} out of valid range, 0..{}", bits - 1),
    }
}

/// A bit-string argument's value, running `bit_in` over an `unknown` literal the
/// way PostgreSQL's coercion of the argument would.
fn bit_arg(fc: &FuncCall, value: &Datum, _ctx: &EvalCtx) -> Result<BitString, ExecError> {
    match value {
        Datum::BitString(bits) => Ok(bits.clone()),
        Datum::Text(text) => Ok(BitString::parse(text, false)?),
        other => Err(ExecError::UndefinedFunction(format!(
            "function {}({}) does not exist",
            fc.name,
            other.column_type().map_or("unknown", ColumnType::name)
        ))),
    }
}

/// The `bigint` index the `bytea` overloads take.
fn int64_arg(value: &Datum) -> Result<i64, ExecError> {
    match value {
        Datum::Int2(n) => Ok(i64::from(*n)),
        Datum::Int4(n) => Ok(i64::from(*n)),
        Datum::Int8(n) => Ok(*n),
        other => Err(ExecError::TypeMismatch(format!(
            "argument of bit function must be integer, not {}",
            other.column_type().map_or("unknown", ColumnType::name)
        ))),
    }
}

fn int_arg(value: &Datum) -> Result<i32, ExecError> {
    match value {
        Datum::Int2(n) => Ok(i32::from(*n)),
        Datum::Int4(n) => Ok(*n),
        Datum::Int8(n) => {
            i32::try_from(*n).map_err(|_| ExecError::Type(crabka_pgtypes::TypeError::Overflow))
        }
        Datum::Text(text) => text
            .trim()
            .parse::<i32>()
            .map_err(|_| ExecError::Type(crabka_pgtypes::TypeError::Overflow)),
        other => Err(ExecError::TypeMismatch(format!(
            "argument of bit function must be integer, not {}",
            other.column_type().map_or("unknown", ColumnType::name)
        ))),
    }
}

/// `bitlength` — the bit count, which is what `length(bit)` and
/// `bit_length(bit)` both call.
pub(crate) fn length(bits: &BitString) -> i32 {
    i32::try_from(bits.len()).unwrap_or(i32::MAX)
}

/// `bitsubstr` / `bitsubstr_no_len`, whose declared result is `bit`.
pub(crate) fn substring(
    bits: &BitString,
    start: i32,
    count: Option<i32>,
) -> Result<Datum, ExecError> {
    Ok(Datum::BitString(bits.substring(start, count)?))
}

/// `bit_overlay`, whose replacement is coerced to a bit string the way
/// PostgreSQL coerces the argument of `overlay(bit, bit, int [, int])`.
pub(crate) fn overlay(
    bits: &BitString,
    replacement: &Datum,
    start: i32,
    count: Option<i32>,
) -> Result<Datum, ExecError> {
    let replacement = match replacement {
        Datum::BitString(value) => value.clone(),
        Datum::Text(text) => BitString::parse(text, false)?,
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "function overlay(bit, {}, integer) does not exist",
                other.column_type().map_or("unknown", ColumnType::name)
            )));
        }
    };
    Ok(Datum::BitString(bits.overlay(
        &replacement,
        start,
        count,
    )?))
}

/// `bitposition` — the one-based index of one bit string in another, with an
/// `unknown` literal on either side read as a bit string.
pub(crate) fn position(haystack: &Datum, needle: &Datum) -> Result<Option<i32>, ExecError> {
    let coerce = |value: &Datum| -> Result<Option<BitString>, ExecError> {
        match value {
            Datum::BitString(bits) => Ok(Some(bits.clone())),
            Datum::Text(text) => Ok(Some(BitString::parse(text, false)?)),
            _ => Ok(None),
        }
    };
    match (coerce(haystack)?, coerce(needle)?) {
        (Some(haystack), Some(needle)) => Ok(Some(haystack.position(&needle))),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgparser::ast::BinaryOp;
    use crabka_pgtypes::{BitString, ColumnType, Datum};

    use super::{apply_bit_operator, is_bit_func, is_bit_type};

    fn bits(text: &str) -> Datum {
        Datum::BitString(BitString::parse(text, false).expect("valid bit string"))
    }

    fn varbits(text: &str) -> Datum {
        Datum::BitString(BitString::parse(text, true).expect("valid bit string"))
    }

    fn text_of(value: &Datum) -> String {
        match value {
            Datum::BitString(value) => value.to_text(),
            other => panic!("expected a bit string, got {other:?}"),
        }
    }

    fn apply(op: BinaryOp, left: &Datum, right: &Datum) -> Datum {
        apply_bit_operator(op, left, right)
            .expect("no error")
            .expect("the operand pair selects a bit-string operator")
    }

    /// `||` is declared over `bit varying` and everything else over `bit`, so
    /// the result's own label differs even when both operands were `bit`.
    #[test]
    fn operators_carry_postgresqls_declared_result_type() {
        let a = bits("1010");
        let b = bits("0110");
        for (op, expected, varying) in [
            (BinaryOp::Concat, "10100110", true),
            (BinaryOp::BitAnd, "0010", false),
            (BinaryOp::BitOr, "1110", false),
            (BinaryOp::BitXor, "1100", false),
        ] {
            let result = apply(op, &a, &b);
            assert!(text_of(&result) == expected, "{op:?}");
            let Datum::BitString(result) = &result else {
                panic!("expected a bit string");
            };
            assert!(result.varying == varying, "{op:?}");
        }
        // A `bit varying` operand reaches the same `bit`-declared operator.
        assert!(text_of(&apply(BinaryOp::BitAnd, &varbits("1010"), &b)) == "0010");
    }

    /// The shift count is an integer, not a bit string, so a literal beside a
    /// shift must not be read as one.
    #[test]
    fn shifts_take_an_integer_count() {
        let value = bits("1101100000000000");
        for (op, count, expected) in [
            (BinaryOp::Shl, Datum::Int4(1), "1011000000000000"),
            (BinaryOp::Shr, Datum::Int4(1), "0110110000000000"),
            (BinaryOp::Shr, Datum::Int2(8), "0000000011011000"),
            (BinaryOp::Shr, Datum::Int8(-1), "1011000000000000"),
        ] {
            assert!(text_of(&apply(op, &value, &count)) == expected, "{op:?}");
        }
    }

    /// An `unknown` literal beside a bit string is a bit string in the value
    /// operators — and `||` reads it as `bit varying`, matching the operator it
    /// resolves to.
    #[test]
    fn an_unknown_literal_beside_a_bit_string_is_one() {
        let value = bits("1010");
        assert!(text_of(&apply(BinaryOp::Concat, &value, &Datum::Text("01".into()))) == "101001");
        assert!(
            text_of(&apply(
                BinaryOp::BitAnd,
                &value,
                &Datum::Text("0110".into())
            )) == "0010"
        );
    }

    /// The operators must not claim a spelling when no operand is a bit string:
    /// `&` and `<<` belong to the integer and network families too.
    #[test]
    fn declines_operands_that_are_not_bit_strings() {
        for op in [
            BinaryOp::Concat,
            BinaryOp::BitAnd,
            BinaryOp::BitOr,
            BinaryOp::BitXor,
            BinaryOp::Shl,
            BinaryOp::Shr,
        ] {
            assert!(
                apply_bit_operator(op, &Datum::Int4(1), &Datum::Int4(2))
                    .expect("no error")
                    .is_none(),
                "{op:?}"
            );
        }
        // A bit string with an operator it has no overload for is also declined,
        // so the caller reports the missing operator rather than this module.
        assert!(
            apply_bit_operator(BinaryOp::Add, &bits("1"), &bits("0"))
                .expect("no error")
                .is_none()
        );
        // A bit string beside an unrelated value declines rather than coercing.
        assert!(
            apply_bit_operator(BinaryOp::BitAnd, &bits("1010"), &Datum::Bool(true))
                .expect("no error")
                .is_none()
        );
    }

    #[test]
    fn a_null_operand_beside_a_bit_string_is_null() {
        for op in [BinaryOp::Concat, BinaryOp::BitAnd, BinaryOp::Shl] {
            assert!(
                apply(op, &bits("1010"), &Datum::Null) == Datum::Null,
                "{op:?}"
            );
            assert!(
                apply(op, &Datum::Null, &bits("1010")) == Datum::Null,
                "{op:?}"
            );
        }
    }

    /// `bit(x)` is deliberately absent: PostgreSQL rejects the unquoted call as
    /// a syntax error, and this lexer cannot tell it from the quoted `"bit"(x)`.
    #[test]
    fn owns_only_the_bit_only_function_names() {
        for name in ["get_bit", "set_bit", "bit_count", "varbit"] {
            assert!(is_bit_func(name), "{name}");
        }
        for name in [
            "bit",
            "length",
            "substring",
            "overlay",
            "position",
            "octet_length",
        ] {
            assert!(!is_bit_func(name), "{name}");
        }
    }

    #[test]
    fn recognises_both_spellings_of_the_type() {
        assert!(is_bit_type(ColumnType::Bit(None)));
        assert!(is_bit_type(ColumnType::Bit(Some(11))));
        assert!(is_bit_type(ColumnType::VarBit(None)));
        assert!(is_bit_type(ColumnType::VarBit(Some(11))));
        for ty in [
            ColumnType::Text,
            ColumnType::Bytea,
            ColumnType::Int4,
            ColumnType::Money,
        ] {
            assert!(!is_bit_type(ty), "{ty:?}");
        }
    }
}
