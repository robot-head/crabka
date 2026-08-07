//! `PostgreSQL`'s network-address functions and operators — the SQL surface of
//! `inet`, `cidr`, `macaddr` and `macaddr8`.
//!
//! `cidr` is binary-coercible to `inet` in `PostgreSQL`, so every operator and
//! most functions here are declared over `inet` and simply accept either
//! spelling; the ones that behave differently (`abbrev`, `set_masklen`) branch
//! on the value's own `is_cidr` flag, which is what selects the `cidr` overload
//! in `PostgreSQL`'s catalog.

use crabka_pgparser::ast::{BinaryOp, Expr, FuncCall};
use crabka_pgtypes::{ColumnType, Datum, Inet, TypeError};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    eval::{infer_type, is_unknown_literal},
    func::{checked_args, require_arity, type_error, undefined_function},
    scope::Scope,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NetworkFunc {
    /// `host(inet)` — the address with no netmask.
    Host,
    /// `text(inet)` — `network_show`, which always appends the netmask. Also
    /// the function-call spelling of a cast to `text` for any other argument.
    Text,
    /// `family(inet)` — 4 or 6.
    Family,
    /// `abbrev(inet)` / `abbrev(cidr)`.
    Abbrev,
    /// `broadcast(inet)`.
    Broadcast,
    /// `network(inet)` — returns `cidr`.
    Network,
    /// `masklen(inet)`.
    Masklen,
    /// `netmask(inet)`.
    Netmask,
    /// `hostmask(inet)`.
    Hostmask,
    /// `set_masklen(inet, int)` / `set_masklen(cidr, int)`.
    SetMasklen,
    /// `inet_merge(inet, inet)` — returns `cidr`.
    InetMerge,
    /// `inet_same_family(inet, inet)`.
    InetSameFamily,
    /// `macaddr8_set7bit(macaddr8)`.
    MacAddr8Set7Bit,
    /// `inet(x)` — the function-call spelling of a cast.
    ToInet,
    /// `cidr(x)` — `cidr(inet)` where the argument is already an `inet`, the
    /// function-call spelling of a cast otherwise.
    ToCidr,
    /// `macaddr(x)`.
    ToMacAddr,
    /// `macaddr8(x)`.
    ToMacAddr8,
}

fn network_func(name: &str) -> Option<NetworkFunc> {
    Some(match name {
        "host" => NetworkFunc::Host,
        "text" => NetworkFunc::Text,
        "family" => NetworkFunc::Family,
        "abbrev" => NetworkFunc::Abbrev,
        "broadcast" => NetworkFunc::Broadcast,
        "network" => NetworkFunc::Network,
        "masklen" => NetworkFunc::Masklen,
        "netmask" => NetworkFunc::Netmask,
        "hostmask" => NetworkFunc::Hostmask,
        "set_masklen" => NetworkFunc::SetMasklen,
        "inet_merge" => NetworkFunc::InetMerge,
        "inet_same_family" => NetworkFunc::InetSameFamily,
        "macaddr8_set7bit" => NetworkFunc::MacAddr8Set7Bit,
        "inet" => NetworkFunc::ToInet,
        "cidr" => NetworkFunc::ToCidr,
        "macaddr" => NetworkFunc::ToMacAddr,
        "macaddr8" => NetworkFunc::ToMacAddr8,
        _ => return None,
    })
}

pub(crate) fn is_network_func(name: &str) -> bool {
    network_func(name).is_some()
}

/// Is this a type in the `inet`/`cidr` family?
fn is_inet_family(ty: ColumnType) -> bool {
    matches!(ty, ColumnType::Inet | ColumnType::Cidr)
}

/// `PostgreSQL` resolves a bare `'…'` argument or operand in the `inet`/`cidr`
/// category to `inet`, the category's preferred type.
fn preferred_inet(ty: ColumnType, expr: &Expr) -> ColumnType {
    if is_unknown_literal(expr) {
        ColumnType::Inet
    } else {
        ty
    }
}

pub(crate) fn network_func_result_type(
    fc: &FuncCall,
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    let function = network_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let arg_type = |index: usize| -> Result<ColumnType, ExecError> {
        Ok(preferred_inet(
            infer_type(&args[index], scope)?,
            &args[index],
        ))
    };
    match function {
        // `text(inet)` is `network_show`, a function on the network types
        // alone. Resolving it for any argument would answer where PostgreSQL
        // reports that no function matches -- `rowtypes` asserts exactly that
        // for `text(fullname)` over a composite.
        NetworkFunc::Text | NetworkFunc::Host | NetworkFunc::Abbrev => {
            require_arity(fc, args.len() == 1)?;
            require_inet(fc, arg_type(0)?)?;
            Ok(ColumnType::Text)
        }
        NetworkFunc::Family | NetworkFunc::Masklen => {
            require_arity(fc, args.len() == 1)?;
            require_inet(fc, arg_type(0)?)?;
            Ok(ColumnType::Int4)
        }
        NetworkFunc::Broadcast | NetworkFunc::Netmask | NetworkFunc::Hostmask => {
            require_arity(fc, args.len() == 1)?;
            require_inet(fc, arg_type(0)?)?;
            Ok(ColumnType::Inet)
        }
        // `network` and `inet_merge` are declared to return `cidr`.
        NetworkFunc::Network => {
            require_arity(fc, args.len() == 1)?;
            require_inet(fc, arg_type(0)?)?;
            Ok(ColumnType::Cidr)
        }
        NetworkFunc::InetMerge => {
            require_arity(fc, args.len() == 2)?;
            require_inet(fc, arg_type(0)?)?;
            require_inet(fc, arg_type(1)?)?;
            Ok(ColumnType::Cidr)
        }
        NetworkFunc::InetSameFamily => {
            require_arity(fc, args.len() == 2)?;
            require_inet(fc, arg_type(0)?)?;
            require_inet(fc, arg_type(1)?)?;
            Ok(ColumnType::Bool)
        }
        // The two `set_masklen` overloads differ in result type as well as in
        // behaviour: the `cidr` one clears the host bits, the `inet` one does
        // not.
        NetworkFunc::SetMasklen => {
            require_arity(fc, args.len() == 2)?;
            let ty = arg_type(0)?;
            require_inet(fc, ty)?;
            Ok(ty)
        }
        NetworkFunc::MacAddr8Set7Bit => {
            require_arity(fc, args.len() == 1)?;
            Ok(ColumnType::MacAddr8)
        }
        NetworkFunc::ToInet => coercion_result_type(fc, args, scope, ColumnType::Inet),
        NetworkFunc::ToCidr => coercion_result_type(fc, args, scope, ColumnType::Cidr),
        NetworkFunc::ToMacAddr => coercion_result_type(fc, args, scope, ColumnType::MacAddr),
        NetworkFunc::ToMacAddr8 => coercion_result_type(fc, args, scope, ColumnType::MacAddr8),
    }
}

/// A `typename(expr)` call is `PostgreSQL`'s function-call cast spelling, which
/// its parser accepts whenever the cast itself exists. Anything else keeps
/// `PostgreSQL`'s "function `name`(`type`) does not exist" wording.
fn coercion_result_type(
    fc: &FuncCall,
    args: &[Expr],
    scope: &Scope,
    target: ColumnType,
) -> Result<ColumnType, ExecError> {
    require_arity(fc, args.len() == 1)?;
    let source = infer_type(&args[0], scope)?;
    if is_unknown_literal(&args[0])
        || source.is_string()
        || crabka_pgtypes::cast::cast_allowed(source, target)
    {
        return Ok(target);
    }
    Err(ExecError::UndefinedFunction(format!(
        "function {}({}) does not exist",
        fc.name,
        source.name()
    )))
}

fn require_inet(fc: &FuncCall, ty: ColumnType) -> Result<(), ExecError> {
    if is_inet_family(ty) {
        return Ok(());
    }
    Err(ExecError::UndefinedFunction(format!(
        "function {}({}) does not exist",
        fc.name,
        ty.name()
    )))
}

/// The one `inet`/`cidr` operand a function was called with, resolving an
/// `unknown` literal (already evaluated to text) to `inet`.
fn inet_arg(name: &str, value: &Datum) -> Result<Inet, ExecError> {
    match value {
        Datum::Inet(value) => Ok(*value),
        Datum::Text(text) => Ok(Inet::parse(text, false).map_err(ExecError::Type)?),
        other => Err(type_error(name, other)),
    }
}

pub(crate) fn eval_network(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let function = network_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let values = args
        .iter()
        .map(&mut eval_child)
        .collect::<Result<Vec<_>, _>>()?;
    // `text(NULL)` is still a cast, and every network function is strict.
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let one = |name: &str| -> Result<Inet, ExecError> {
        match values.as_slice() {
            [value] => inet_arg(name, value),
            _ => Err(undefined_function(&fc.name)),
        }
    };
    let two = |name: &str| -> Result<(Inet, Inet), ExecError> {
        match values.as_slice() {
            [left, right] => Ok((inet_arg(name, left)?, inet_arg(name, right)?)),
            _ => Err(undefined_function(&fc.name)),
        }
    };
    match function {
        NetworkFunc::Host => Ok(Datum::Text(one("host")?.host())),
        // `text(inet)` is `network_show`; for anything else the call is the
        // function spelling of a cast to `text`.
        NetworkFunc::Text => match values.as_slice() {
            [Datum::Inet(value)] => Ok(Datum::Text(value.show())),
            [value] => cast_to(value, ColumnType::Text, ctx),
            _ => Err(undefined_function(&fc.name)),
        },
        NetworkFunc::Family => Ok(Datum::Int4(one("family")?.family_number())),
        NetworkFunc::Abbrev => Ok(Datum::Text(one("abbrev")?.abbrev())),
        NetworkFunc::Broadcast => Ok(Datum::Inet(one("broadcast")?.broadcast())),
        NetworkFunc::Network => Ok(Datum::Inet(one("network")?.network())),
        NetworkFunc::Masklen => Ok(Datum::Int4(one("masklen")?.masklen())),
        NetworkFunc::Netmask => Ok(Datum::Inet(one("netmask")?.netmask())),
        NetworkFunc::Hostmask => Ok(Datum::Inet(one("hostmask")?.hostmask())),
        NetworkFunc::SetMasklen => match values.as_slice() {
            [address, bits] => {
                let address = inet_arg("set_masklen", address)?;
                let bits = masklen_arg(bits)?;
                let result = if address.is_cidr {
                    address.set_cidr_masklen(bits)
                } else {
                    address.set_masklen(bits)
                };
                Ok(Datum::Inet(result.map_err(ExecError::Type)?))
            }
            _ => Err(undefined_function(&fc.name)),
        },
        NetworkFunc::InetMerge => {
            let (left, right) = two("inet_merge")?;
            Ok(Datum::Inet(left.merge(&right).map_err(ExecError::Type)?))
        }
        NetworkFunc::InetSameFamily => {
            let (left, right) = two("inet_same_family")?;
            Ok(Datum::Bool(left.same_family(&right)))
        }
        NetworkFunc::MacAddr8Set7Bit => match values.as_slice() {
            [Datum::MacAddr8(value)] => Ok(Datum::MacAddr8(value.set7bit())),
            [Datum::Text(text)] => Ok(Datum::MacAddr8(
                crabka_pgtypes::MacAddr8::parse(text)
                    .map_err(ExecError::Type)?
                    .set7bit(),
            )),
            [got] => Err(type_error("macaddr8_set7bit", got)),
            _ => Err(undefined_function(&fc.name)),
        },
        NetworkFunc::ToInet => coerce_one(fc, &values, ColumnType::Inet, ctx),
        NetworkFunc::ToCidr => coerce_one(fc, &values, ColumnType::Cidr, ctx),
        NetworkFunc::ToMacAddr => coerce_one(fc, &values, ColumnType::MacAddr, ctx),
        NetworkFunc::ToMacAddr8 => coerce_one(fc, &values, ColumnType::MacAddr8, ctx),
    }
}

/// `set_masklen`'s second argument is an `int4`; `-1` means "as wide as the
/// family allows".
fn masklen_arg(value: &Datum) -> Result<i32, ExecError> {
    match value {
        Datum::Int2(bits) => Ok(i32::from(*bits)),
        Datum::Int4(bits) => Ok(*bits),
        Datum::Int8(bits) => i32::try_from(*bits)
            .map_err(|_| ExecError::Type(TypeError::out_of_range_for("integer"))),
        got => Err(type_error("set_masklen", got)),
    }
}

fn coerce_one(
    fc: &FuncCall,
    values: &[Datum],
    target: ColumnType,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    match values {
        [value] => cast_to(value, target, ctx),
        _ => Err(undefined_function(&fc.name)),
    }
}

fn cast_to(value: &Datum, target: ColumnType, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    crate::eval::cast_value(value, target, &ctx.time_zone)
}

// ---------------------------------------------------------------------------
// Operators
// ---------------------------------------------------------------------------

/// Is this type one of the two hardware-address types?
fn is_mac_family(ty: ColumnType) -> bool {
    matches!(ty, ColumnType::MacAddr | ColumnType::MacAddr8)
}

/// Which network operator, if any, a `BinaryOp` can denote.
fn is_network_operator(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Shl
            | BinaryOp::Shr
            | BinaryOp::ContainedByOrEq
            | BinaryOp::ContainsOrEq
            | BinaryOp::Overlaps
            | BinaryOp::BitAnd
            | BinaryOp::BitOr
            | BinaryOp::Add
            | BinaryOp::Sub
    )
}

/// The static result type of a network operator, or `None` when this operand
/// pair does not select one.
///
/// `<<`, `>>`, `&&`, `&`, `|`, `+` and `-` are all shared with other families,
/// so only an operand actually in the `inet`/`cidr` family claims them here.
pub(crate) fn network_operator_result_type(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    scope: &Scope,
) -> Result<Option<ColumnType>, ExecError> {
    if !is_network_operator(op) && !matches!(op, BinaryOp::ContainedByOrEq | BinaryOp::ContainsOrEq)
    {
        return Ok(None);
    }
    let (lt, rt) = (infer_type(left, scope)?, infer_type(right, scope)?);
    // `macaddr & macaddr` and `macaddr | macaddr` (and the `macaddr8` pair) are
    // the only network operators outside the inet/cidr family.
    if matches!(op, BinaryOp::BitAnd | BinaryOp::BitOr) && (is_mac_family(lt) || is_mac_family(rt))
    {
        let resolved = if is_mac_family(lt) { lt } else { rt };
        return Ok(Some(resolved));
    }
    // At least one side must be a network type for these spellings to mean the
    // network operator rather than the bitwise/arithmetic/range one.
    if !is_inet_family(lt) && !is_inet_family(rt) {
        return Ok(None);
    }
    let (lt, rt) = (preferred_inet(lt, left), preferred_inet(rt, right));
    let integral =
        |ty: ColumnType| matches!(ty, ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8);
    let result = match op {
        BinaryOp::Shl | BinaryOp::Shr | BinaryOp::ContainedByOrEq | BinaryOp::ContainsOrEq
            if is_inet_family(lt) && is_inet_family(rt) =>
        {
            ColumnType::Bool
        }
        BinaryOp::Overlaps if is_inet_family(lt) && is_inet_family(rt) => ColumnType::Bool,
        BinaryOp::BitAnd | BinaryOp::BitOr if is_inet_family(lt) && is_inet_family(rt) => {
            ColumnType::Inet
        }
        // `inet + bigint` and `bigint + inet`; `inet - bigint`; `inet - inet`.
        BinaryOp::Add if is_inet_family(lt) && integral(rt) => ColumnType::Inet,
        BinaryOp::Add if integral(lt) && is_inet_family(rt) => ColumnType::Inet,
        BinaryOp::Sub if is_inet_family(lt) && integral(rt) => ColumnType::Inet,
        BinaryOp::Sub if is_inet_family(lt) && is_inet_family(rt) => ColumnType::Int8,
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

/// Apply a network operator to two already-evaluated operands, or `None` when
/// neither operand is a network address (so the shared spellings keep their
/// bitwise, range and arithmetic meanings).
pub(crate) fn apply_network_operator(
    op: BinaryOp,
    left: &Datum,
    right: &Datum,
) -> Result<Option<Datum>, ExecError> {
    // `&` and `|` over the hardware-address types are bytewise and have no
    // other overload for these operands.
    if matches!(op, BinaryOp::BitAnd | BinaryOp::BitOr)
        && let Some(result) = apply_mac_bitwise(op, left, right)?
    {
        return Ok(Some(result));
    }
    // The two `<<=`/`>>=` spellings exist only for this family, so they answer
    // even for a NULL operand — everything else needs a network operand to
    // claim the spelling at all.
    let network_pair = matches!(left, Datum::Inet(_)) || matches!(right, Datum::Inet(_));
    let only_ours = matches!(op, BinaryOp::ContainedByOrEq | BinaryOp::ContainsOrEq);
    if !network_pair && !only_ours {
        return Ok(None);
    }
    if !is_network_operator(op) {
        return Ok(None);
    }
    if left.is_null() || right.is_null() {
        return Ok(Some(Datum::Null));
    }
    let (Datum::Inet(a), other) = (left, right) else {
        // A network operand on the right only: `bigint + inet` and the
        // containment operators with an `unknown` literal on the left.
        let Datum::Inet(b) = right else {
            return Ok(None);
        };
        return apply_with_right_network(op, left, b).map(Some);
    };
    let result = match (op, other) {
        (BinaryOp::Shl, Datum::Inet(b)) => Datum::Bool(a.is_subnet_of(b)),
        (BinaryOp::ContainedByOrEq, Datum::Inet(b)) => Datum::Bool(a.is_subnet_of_or_eq(b)),
        (BinaryOp::Shr, Datum::Inet(b)) => Datum::Bool(a.is_supernet_of(b)),
        (BinaryOp::ContainsOrEq, Datum::Inet(b)) => Datum::Bool(a.is_supernet_of_or_eq(b)),
        (BinaryOp::Overlaps, Datum::Inet(b)) => Datum::Bool(a.overlaps(b)),
        (BinaryOp::BitAnd, Datum::Inet(b)) => Datum::Inet(a.and(b).map_err(ExecError::Type)?),
        (BinaryOp::BitOr, Datum::Inet(b)) => Datum::Inet(a.or(b).map_err(ExecError::Type)?),
        (BinaryOp::Sub, Datum::Inet(b)) => Datum::Int8(a.difference(b).map_err(ExecError::Type)?),
        // A bare literal beside a network operand is an `inet` in every one of
        // these operators, so parse it rather than falling through.
        (_, Datum::Text(text)) => {
            let b = Inet::parse(text, false).map_err(ExecError::Type)?;
            return apply_network_operator(op, left, &Datum::Inet(b));
        }
        (BinaryOp::Add, value) => Datum::Inet(
            a.add_offset(offset_operand(op, left, value)?)
                .map_err(ExecError::Type)?,
        ),
        (BinaryOp::Sub, value) => {
            let offset = offset_operand(op, left, value)?;
            Datum::Inet(
                a.add_offset(offset.checked_neg().ok_or_else(out_of_range)?)
                    .map_err(ExecError::Type)?,
            )
        }
        _ => return Ok(None),
    };
    Ok(Some(result))
}

/// `macaddr & macaddr` / `macaddr | macaddr` and the `macaddr8` pair, resolving
/// a bare literal on either side to the other operand's width.
fn apply_mac_bitwise(
    op: BinaryOp,
    left: &Datum,
    right: &Datum,
) -> Result<Option<Datum>, ExecError> {
    let and = op == BinaryOp::BitAnd;
    match (left, right) {
        (Datum::MacAddr(a), Datum::MacAddr(b)) => {
            Ok(Some(Datum::MacAddr(if and { a.and(b) } else { a.or(b) })))
        }
        (Datum::MacAddr8(a), Datum::MacAddr8(b)) => {
            Ok(Some(Datum::MacAddr8(if and { a.and(b) } else { a.or(b) })))
        }
        (Datum::MacAddr(_) | Datum::MacAddr8(_), Datum::Null)
        | (Datum::Null, Datum::MacAddr(_) | Datum::MacAddr8(_)) => Ok(Some(Datum::Null)),
        (Datum::MacAddr(_), Datum::Text(text)) => {
            let right =
                Datum::MacAddr(crabka_pgtypes::MacAddr::parse(text).map_err(ExecError::Type)?);
            apply_mac_bitwise(op, left, &right)
        }
        (Datum::MacAddr8(_), Datum::Text(text)) => {
            let right =
                Datum::MacAddr8(crabka_pgtypes::MacAddr8::parse(text).map_err(ExecError::Type)?);
            apply_mac_bitwise(op, left, &right)
        }
        (Datum::Text(text), Datum::MacAddr(_)) => {
            let left =
                Datum::MacAddr(crabka_pgtypes::MacAddr::parse(text).map_err(ExecError::Type)?);
            apply_mac_bitwise(op, &left, right)
        }
        (Datum::Text(text), Datum::MacAddr8(_)) => {
            let left =
                Datum::MacAddr8(crabka_pgtypes::MacAddr8::parse(text).map_err(ExecError::Type)?);
            apply_mac_bitwise(op, &left, right)
        }
        _ => Ok(None),
    }
}

fn apply_with_right_network(op: BinaryOp, left: &Datum, right: &Inet) -> Result<Datum, ExecError> {
    match (op, left) {
        // `bigint + inet` is the commuted `inet + bigint`.
        (BinaryOp::Add, value) if integral_datum(value).is_some() => Ok(Datum::Inet(
            right
                .add_offset(offset_operand(op, &Datum::Inet(*right), value)?)
                .map_err(ExecError::Type)?,
        )),
        // A bare literal on the left of a network operator is an `inet`.
        (_, Datum::Text(text)) => {
            let a = Inet::parse(text, false).map_err(ExecError::Type)?;
            apply_network_operator(op, &Datum::Inet(a), &Datum::Inet(*right))
                .map(|result| result.unwrap_or(Datum::Null))
        }
        _ => Err(crate::eval::undefined_operator_for(
            op,
            left,
            &Datum::Inet(*right),
        )),
    }
}

fn integral_datum(value: &Datum) -> Option<i64> {
    match value {
        Datum::Int2(n) => Some(i64::from(*n)),
        Datum::Int4(n) => Some(i64::from(*n)),
        Datum::Int8(n) => Some(*n),
        _ => None,
    }
}

fn offset_operand(op: BinaryOp, left: &Datum, value: &Datum) -> Result<i64, ExecError> {
    integral_datum(value).ok_or_else(|| crate::eval::undefined_operator_for(op, left, value))
}

fn out_of_range() -> ExecError {
    ExecError::Type(TypeError::OutOfRange {
        message: "result is out of range".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn inet(text: &str) -> Datum {
        Datum::Inet(Inet::parse(text, false).expect("valid inet"))
    }

    fn cidr(text: &str) -> Datum {
        Datum::Inet(Inet::parse(text, true).expect("valid cidr"))
    }

    fn apply(op: BinaryOp, left: &Datum, right: &Datum) -> Datum {
        apply_network_operator(op, left, right)
            .expect("no error")
            .expect("the operand pair selects a network operator")
    }

    #[test]
    fn containment_operators_answer_over_mixed_inet_and_cidr_operands() {
        let cases: [(BinaryOp, &str, &str, bool); 8] = [
            (BinaryOp::Shl, "192.168.1.0/25", "192.168.1.0/24", true),
            (BinaryOp::Shl, "192.168.1.0/24", "192.168.1.0/24", false),
            (
                BinaryOp::ContainedByOrEq,
                "192.168.1.0/24",
                "192.168.1.0/24",
                true,
            ),
            (BinaryOp::Shr, "192.168.1.0/24", "192.168.1.0/25", true),
            (
                BinaryOp::ContainsOrEq,
                "192.168.1.0/24",
                "192.168.1.0/24",
                true,
            ),
            (BinaryOp::Overlaps, "10.1.2.3/8", "10.0.0.0/32", true),
            (BinaryOp::Overlaps, "11.1.2.3/8", "10.0.0.0/8", false),
            (BinaryOp::Shl, "10.0.0.0/8", "::/8", false),
        ];
        for (op, left, right, expected) in cases {
            assert!(
                apply(op, &inet(left), &cidr(right)) == Datum::Bool(expected),
                "{left} {op:?} {right}"
            );
        }
    }

    #[test]
    fn arithmetic_operators_resolve_from_the_operand_values() {
        assert!(apply(BinaryOp::Add, &inet("127.0.0.1"), &Datum::Int4(257)) == inet("127.0.1.2"));
        // `bigint + inet` is the commuted form.
        assert!(apply(BinaryOp::Add, &Datum::Int8(257), &inet("127.0.0.1")) == inet("127.0.1.2"));
        assert!(apply(BinaryOp::Sub, &inet("127.0.1.2"), &Datum::Int4(257)) == inet("127.0.0.1"));
        assert!(apply(BinaryOp::Sub, &inet("127.0.0.2"), &inet("127.0.0.1")) == Datum::Int8(1));
        assert!(
            apply(
                BinaryOp::BitAnd,
                &inet("192.168.1.226/24"),
                &cidr("192.168.1")
            ) == inet("192.168.1.0/24")
        );
        assert!(
            apply(
                BinaryOp::BitOr,
                &inet("192.168.1.226/24"),
                &cidr("192.168.1")
            ) == inet("192.168.1.226/24")
        );
    }

    #[test]
    fn a_bare_literal_beside_a_network_operand_is_an_inet() {
        assert!(
            apply(
                BinaryOp::Shl,
                &Datum::Text("192.168.1.0/25".into()),
                &cidr("192.168.1")
            ) == Datum::Bool(true)
        );
        assert!(
            apply(
                BinaryOp::ContainsOrEq,
                &cidr("192.168.1"),
                &Datum::Text("192.168.1.226".into())
            ) == Datum::Bool(true)
        );
    }

    #[test]
    fn non_network_operands_leave_the_shared_spellings_alone() {
        for op in [
            BinaryOp::Shl,
            BinaryOp::Shr,
            BinaryOp::Overlaps,
            BinaryOp::BitAnd,
            BinaryOp::BitOr,
            BinaryOp::Add,
            BinaryOp::Sub,
        ] {
            assert!(
                apply_network_operator(op, &Datum::Int4(1), &Datum::Int4(2)).expect("no error")
                    == None,
                "{op:?}"
            );
        }
    }

    #[test]
    fn a_null_operand_makes_a_network_operator_null() {
        assert!(apply(BinaryOp::Shl, &Datum::Null, &cidr("192.168.1")) == Datum::Null);
        assert!(apply(BinaryOp::ContainedByOrEq, &inet("1.2.3.4"), &Datum::Null) == Datum::Null);
    }

    #[test]
    fn overflow_propagates_postgres_out_of_range() {
        let error = apply_network_operator(
            BinaryOp::Add,
            &inet("127.0.0.1"),
            &Datum::Int8(10_000_000_000),
        )
        .expect_err("out of range");
        assert!(error.into_pg().code == "22003");
    }
}
