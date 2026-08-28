use super::*;

pub(crate) fn read_generated_reads<'a>(
    table: &Table,
    refs: Option<&'a crate::scope::StatementRefs>,
    qualifier: &'a str,
) -> crate::scope::GeneratedReads<'a> {
    match refs {
        Some(refs) if !table.row_security => crate::scope::GeneratedReads::of(refs, qualifier),
        _ => crate::scope::GeneratedReads::every(),
    }
}

/// The jsonpath type an assignment target stores, if it stores one — the two
/// types whose input function is not reachable as a cast arm.
pub(crate) fn jsonpath_assignment_base(target: ColumnType) -> Option<ColumnType> {
    let base = target.storage_type();
    matches!(
        base,
        ColumnType::JsonPath | ColumnType::Array(crabka_pgtypes::ElemType::JsonPath)
    )
    .then_some(base)
}

/// Resolve a bare string literal — a value PostgreSQL still types `unknown` —
/// against the type it is being assigned to.
///
/// `unknown` has no storage and no operators of its own: an unadorned `'…'`
/// takes the assignment target's type, parsed by that type's input function.
/// That is what makes `SET id = '[11,12)'` an `int4range` and `FOR VALUES IN
/// ('[1,2)')` an `int4range` bound. It applies to the *literal* only — a
/// genuine `text` expression keeps its type, so `SET int_col = text_col` is
/// still 42804.
///
/// The resolution is an assignment, not an explicit cast: `'abcd'` into a
/// `varchar(3)` is 22001, where `'abcd'::varchar(3)` would have truncated.
pub(crate) fn resolve_unknown_literal(
    text: &str,
    target: ColumnType,
    ctx: &crate::clock::EvalCtx,
) -> Result<Datum, ExecError> {
    // A domain literal is parsed by the *base* type's input function; the
    // domain's own constraints are [`coerce`]'s job, on the parsed value.
    let base = match target {
        ColumnType::Domain(domain) => *domain.base,
        other => other,
    };
    let value = Datum::Text(text.to_owned());
    if let Some(jsonpath) = jsonpath_assignment_base(base) {
        return crate::eval::cast_value_in_at(&value, jsonpath, ctx.output_style(), ctx.now);
    }
    match base {
        // `bytea_in` is the escape/hex decoder rather than a cast arm.
        ColumnType::Bytea => Ok(Datum::Bytea(crate::session::decode_bytea_text(text)?)),
        // The session's styles, not the canonical ones: PostgreSQL runs the
        // target type's input function here, and `date_in`/`timestamp_in` read
        // `DateStyle`'s field order. A stored `'97/02/10'` has to mean what the
        // same literal means when it is written as a cast.
        _ => crate::eval::cast_assign_value_in_at(&value, base, ctx.output_style(), ctx.now),
    }
}

/// Evaluate an assignment's right-hand side, giving an unadorned string literal
/// the `unknown` treatment of [`resolve_unknown_literal`]. Every other
/// expression evaluates on its own and then faces [`coerce`]'s assignment
/// rules, which callers apply to this function's result.
pub(crate) fn eval_assignment_value(
    expr: &Expr,
    target: ColumnType,
    scope: &Scope,
    values: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<Datum, ExecError> {
    match expr {
        Expr::StringLiteral(text) => resolve_unknown_literal(text, target, ctx),
        _ => crate::eval::eval(expr, scope, values, ctx),
    }
}

/// Store a column default without evaluating expressions that must run for each
/// inserted row.
pub(crate) fn default_from_expr(
    expr: &Expr,
    target: ColumnType,
    ctx: &crate::clock::EvalCtx,
) -> Result<ColumnDefault, ExecError> {
    if matches!(
        expr,
        Expr::StringLiteral(_)
            | Expr::IntLiteral(_)
            | Expr::NumericLiteral(_)
            | Expr::BitStringLiteral(_)
            | Expr::BoolLiteral(_)
            | Expr::NullLiteral
    ) || matches!(
        (expr, target),
        (
            Expr::Cast {
                ty: ColumnType::Regclass,
                ..
            },
            ColumnType::Regclass
        )
    ) {
        let value = eval_assignment_value(expr, target, &Scope::empty(), &[], ctx)?;
        let coerced = coerce_default(value.clone(), target, ctx)?;
        ensure_default_can_be_persisted(&coerced)?;
        return Ok(ColumnDefault::Value(stored_default_value(value, coerced)));
    }

    let source = crate::eval::infer_type(expr, &Scope::empty())?;
    if !crabka_pgtypes::cast::assignment_cast_allowed(source, target) {
        return Err(ExecError::TypeMismatch(format!(
            "column is of type {} but expression is of type {}",
            target.name(),
            source.name(),
        )));
    }
    Ok(ColumnDefault::Expression(crate::viewdef::expression_text(
        expr,
        ctx.output_style(),
    )))
}

/// Coerce the result of a stored column default.
pub(crate) fn coerce_default(
    value: Datum,
    target: ColumnType,
    ctx: &crate::clock::EvalCtx,
) -> Result<Datum, ExecError> {
    coerce(value, target, ctx)
}

/// Coerce an evaluated value into a target column type (assignment context). `ctx`
/// supplies the session zone for any temporal numeric conversion.
pub(crate) fn coerce(
    value: crabka_pgtypes::Datum,
    target: crabka_pgtypes::ColumnType,
    ctx: &crate::clock::EvalCtx,
) -> Result<crabka_pgtypes::Datum, ExecError> {
    use crabka_pgtypes::{ColumnType, Datum, TypeError, string::Coercion};
    // Assignment to a domain column coerces to the domain's base type and then
    // has to satisfy the domain's own constraints — PostgreSQL applies them at
    // every assignment, not only at an explicit cast.
    if let ColumnType::Domain(domain) = target {
        let base = coerce(value, *domain.base, ctx)?;
        crate::usertype::check_domain(target, &base, ctx)?;
        return Ok(base);
    }
    // Assignment to a composite column accepts a record of the same shape, and
    // a `record` built by a bare `ROW(…)` is coerced field by field into the
    // target's attribute types.
    if let (Datum::Record(_), ColumnType::Record(Some(_))) = (&value, target) {
        return Ok(crabka_pgtypes::cast::cast_in(
            &value,
            target,
            ctx.output_style(),
        )?);
    }
    if let (Datum::Enum(e), ColumnType::Enum(named)) = (&value, target)
        && e.ty == named
    {
        return Ok(value);
    }
    // The eleven `reg*` types share one datum, which carries the oid and the
    // rendering but not which of them produced it — so a store has only the
    // target column's type to go on. That is enough: the row encoding keeps the
    // oid and nothing else, and the name is re-derived from the *column's* type
    // on the way out. The value passes through unchanged, which keeps a stored
    // `regclass` DEFAULT deparsing as the literal it was written as.
    if matches!(&value, Datum::Regclass(_)) && target.is_reg() {
        return Ok(value);
    }
    if matches!(target, ColumnType::Temporal(_, _)) {
        return crate::eval::cast_assign_value_in_at(&value, target, ctx.output_style(), ctx.now);
    }
    if target == ColumnType::JsonPath {
        return match value {
            Datum::Null => Ok(Datum::Null),
            Datum::JsonPath(text) => Ok(Datum::JsonPath(text)),
            other => Err(ExecError::TypeMismatch(format!(
                "column is of type jsonpath but expression is of type {}",
                other.column_type().map_or("unknown", ColumnType::name),
            ))),
        };
    }
    if target == ColumnType::Array(crabka_pgtypes::ElemType::JsonPath) {
        return match value {
            Datum::Null => Ok(Datum::Null),
            Datum::Array(array) if array.elem == crabka_pgtypes::ElemType::JsonPath => {
                Ok(Datum::Array(array))
            }
            other => Err(ExecError::TypeMismatch(format!(
                "column is of type jsonpath[] but expression is of type {}",
                other.column_type().map_or("unknown", ColumnType::name),
            ))),
        };
    }
    // SP32: assignment to a `numeric` column — any numeric-family value (int4/
    // int8/float8/numeric) converts, applying the column's `(p,s)` modifier (round
    // + overflow). A `text` value still needs an explicit cast (handled by the
    // catch-all below); NULL falls through to the `(Null, _)` arm.
    if target.is_numeric()
        && matches!(
            value,
            Datum::Int4(_) | Datum::Int8(_) | Datum::Float8(_) | Datum::Numeric(_)
        )
    {
        return Ok(crabka_pgtypes::cast::cast_in(
            &value,
            target,
            ctx.output_style(),
        )?);
    }
    Ok(match (value, target) {
        (Datum::Null, _) => Datum::Null,
        (Datum::Bool(b), ColumnType::Bool) => Datum::Bool(b),
        (Datum::Int4(n), ColumnType::Int4) => Datum::Int4(n),
        (Datum::Int4(n), ColumnType::Int8) => Datum::Int8(i64::from(n)),
        (Datum::Int8(n), ColumnType::Int8) => Datum::Int8(n),
        (Datum::Int8(n), ColumnType::Int4) => i32::try_from(n)
            .map(Datum::Int4)
            .map_err(|_| TypeError::Overflow)?,
        (Datum::Text(s), ColumnType::Text) => Datum::Text(s),
        (Datum::Text(s), ColumnType::Aclitem | ColumnType::Refcursor) => Datum::Text(s),
        (value @ Datum::Text(_), ColumnType::Name) => {
            crabka_pgtypes::cast::cast_assign_in(&value, ColumnType::Name, ctx.output_style())?
        }
        (Datum::Text(s), ColumnType::Varchar(limit)) => Datum::Text(
            crabka_pgtypes::string::apply_varchar_typmod(&s, limit, Coercion::Assignment)?,
        ),
        (Datum::Text(s), ColumnType::Char(limit)) => Datum::Text(
            crabka_pgtypes::string::apply_char_typmod(&s, limit, Coercion::Assignment)?,
        ),
        (value, target @ (ColumnType::Varchar(_) | ColumnType::Char(_))) => {
            crabka_pgtypes::cast::cast_assign_in(&value, target, ctx.output_style())?
        }
        (Datum::Text(s), ColumnType::Uuid) => {
            Datum::Text(crabka_pgtypes::uuid::UuidBytes::parse(&s)?.to_canonical_text())
        }
        (Datum::Bytea(bytes), ColumnType::Bytea) => Datum::Bytea(bytes),
        (Datum::Text(s), ColumnType::Bytea) => Datum::Bytea(crate::session::decode_bytea_text(&s)?),
        // SP30: float8 assignment casts. int → float8 is the standard widening;
        // float8 → int rounds half-to-even (PG's float→int assignment cast) and
        // range-checks (out of range / non-finite → 22003).
        (Datum::Float8(f), ColumnType::Float8) => Datum::Float8(f),
        (Datum::Int4(n), ColumnType::Float8) => Datum::Float8(f64::from(n)),
        (Datum::Int8(n), ColumnType::Float8) => Datum::Float8(n as f64),
        (Datum::Float8(f), ColumnType::Int4) => {
            let r = f.round_ties_even();
            if r.is_finite() && (i32::MIN as f64..=i32::MAX as f64).contains(&r) {
                Datum::Int4(r as i32)
            } else {
                return Err(TypeError::Overflow.into());
            }
        }
        (Datum::Float8(f), ColumnType::Int8) => {
            let r = f.round_ties_even();
            if r.is_finite() && (i64::MIN as f64..=i64::MAX as f64).contains(&r) {
                Datum::Int8(r as i64)
            } else {
                return Err(TypeError::Overflow.into());
            }
        }
        // SP32: assignment of a numeric value into a non-numeric numeric-family
        // column (→ numeric column is handled by the pre-check above). numeric→int
        // rounds half-away-from-zero with a range check (22003); numeric→float8 may
        // become ±Infinity for an out-of-range magnitude.
        (Datum::Numeric(d), ColumnType::Float8) => {
            Datum::Float8(crabka_pgtypes::numeric::to_f64(&d))
        }
        (Datum::Numeric(d), ColumnType::Int4) => {
            crabka_pgtypes::numeric::to_i32(&d).map(Datum::Int4)?
        }
        (Datum::Numeric(d), ColumnType::Int8) => {
            crabka_pgtypes::numeric::to_i64(&d).map(Datum::Int8)?
        }
        // SP37: date/time assignment — same-type pass-through (no implicit
        // cross-type coercion between temporal types; mismatches hit the catch-all).
        (Datum::Date(d), ColumnType::Date) => Datum::Date(d),
        (Datum::Time(t), ColumnType::Time) => Datum::Time(t),
        (Datum::Timestamp(ts), ColumnType::Timestamp) => Datum::Timestamp(ts),
        (Datum::Timestamptz(ts), ColumnType::Timestamptz) => Datum::Timestamptz(ts),
        (Datum::Interval(iv), ColumnType::Interval) => Datum::Interval(iv),
        // jsonb / array assignment. A string value runs the target type's input
        // function (`jsonb_in` / `array_in` — 22P02 on malformed input), the same
        // literal-assignment shape the `uuid` and `bytea` arms above use; an
        // array value converts element-wise so `ARRAY[1,2]` (int4[]) stores into
        // a `bigint[]` column. `cast` implements all four conversions.
        // `bit(n)` rejects a length mismatch on a store and `bit varying(n)`
        // an over-long value, where the explicit cast the catch-all would use
        // pads or truncates silently.
        (value @ Datum::BitString(_), ty @ (ColumnType::Bit(_) | ColumnType::VarBit(_))) => {
            crabka_pgtypes::cast::cast_assign_in(&value, ty, ctx.output_style())?
        }
        // `json → jsonb` and `jsonb → json` are assignment-level casts in
        // `pg_cast`, so storing one in a column of the other is allowed and runs
        // the target's input function over the source's output.
        (
            value @ (Datum::Text(_) | Datum::Jsonb(_) | Datum::Json(_)),
            ty @ (ColumnType::Jsonb | ColumnType::Json),
        )
        | (value @ (Datum::Text(_) | Datum::Array(_)), ty @ ColumnType::Array(_)) => {
            // `cast_assign_in`, because this is a store: an over-long element
            // of a `varchar(n)[]` column is 22001, not a silent truncation.
            crabka_pgtypes::cast::cast_assign_in(&value, ty, ctx.output_style())?
        }
        (v, target) => {
            // Assignment-context implicit casts — PostgreSQL's pg_cast
            // castcontext 'i'/'a' pairs and I/O conversions into string types.
            if let Some(from) = v.column_type()
                && crabka_pgtypes::cast::assignment_cast_allowed(from, target)
            {
                return Ok(crabka_pgtypes::cast::cast_in(
                    &v,
                    target,
                    ctx.output_style(),
                )?);
            }
            return Err(ExecError::TypeMismatch(format!(
                "column is of type {} but expression is of type {}",
                target.name(),
                v.column_type().map(|t| t.name()).unwrap_or("unknown"),
            )));
        }
    })
}
