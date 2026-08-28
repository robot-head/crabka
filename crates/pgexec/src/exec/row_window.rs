use super::*;

/// SP28: drop the first `offset` rows then keep at most `limit` (negative values
/// clamp to 0). Shared by the row and aggregate output paths.
pub(crate) fn apply_offset_limit<T>(rows: &mut Vec<T>, offset: Option<i64>, limit: Option<i64>) {
    if let Some(off) = offset {
        let n = usize::try_from(off.max(0))
            .unwrap_or(usize::MAX)
            .min(rows.len());
        rows.drain(0..n);
    }
    if let Some(limit) = limit {
        let n = usize::try_from(limit.max(0)).unwrap_or(usize::MAX);
        rows.truncate(n);
    }
}

/// Which clause a row count came from, for the error a negative one raises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowCountClause {
    Limit,
    Offset,
}

impl RowCountClause {
    fn name(self) -> &'static str {
        match self {
            Self::Limit => "LIMIT",
            Self::Offset => "OFFSET",
        }
    }

    /// PostgreSQL's distinct SQLSTATEs for the two negative-count errors
    /// (`invalid_row_count_in_limit_clause` / `…_in_result_offset_clause`).
    fn negative_sqlstate(self) -> &'static str {
        match self {
            Self::Limit => "2201W",
            Self::Offset => "2201X",
        }
    }
}

/// Evaluate a `LIMIT`/`OFFSET` expression to a row count.
///
/// PostgreSQL evaluates each once, against no input row, and casts the result to
/// `bigint`. A NULL means "no bound" for both clauses (`LIMIT NULL` is `LIMIT
/// ALL`), and a negative count is an error naming the clause.
pub(crate) fn eval_row_count(
    expr: Option<&Expr>,
    clause: RowCountClause,
    ctx: &crate::clock::EvalCtx,
) -> Result<Option<i64>, ExecError> {
    let Some(expr) = expr else {
        return Ok(None);
    };
    let value = crate::eval::eval(expr, &Scope::empty(), &[], ctx)?;
    if value.is_null() {
        return Ok(None);
    }
    // PostgreSQL coerces the count to bigint by ASSIGNMENT, which only the
    // numeric types and an untyped literal satisfy. Anything else is 42804 with
    // the offending type named — not the 42846 an explicit cast would raise, so
    // `LIMIT '2'::text` and `LIMIT true` must be rejected before the cast.
    if !row_count_coercible(expr, &value) {
        return Err(ExecError::TypeMismatch(format!(
            "argument of {} must be type bigint, not type {}",
            clause.name(),
            value.column_type().map_or("unknown", ColumnType::name)
        )));
    }
    let Datum::Int8(count) = crabka_pgtypes::cast::cast(&value, ColumnType::Int8, &ctx.time_zone)?
    else {
        return Err(ExecError::TypeMismatch(format!(
            "argument of {} must be type bigint",
            clause.name()
        )));
    };
    if count < 0 {
        return Err(ExecError::FunctionError {
            sqlstate: clause.negative_sqlstate(),
            message: format!("{} must not be negative", clause.name()),
        });
    }
    Ok(Some(count))
}

/// May this `LIMIT`/`OFFSET` value be coerced to `bigint`?
///
/// The numeric types have assignment casts to `bigint`; `text` does not, which
/// is why `LIMIT '2'` (an `unknown` literal, resolved as bigint) works where
/// `LIMIT '2'::text` does not.
fn row_count_coercible(expr: &Expr, value: &Datum) -> bool {
    match value {
        Datum::Int2(_)
        | Datum::Int4(_)
        | Datum::Int8(_)
        | Datum::Float4(_)
        | Datum::Float8(_)
        | Datum::Numeric(_) => true,
        Datum::Text(_) => matches!(expr, Expr::StringLiteral(_) | Expr::BitStringLiteral(_)),
        _ => false,
    }
}

/// The evaluated row-count window of a query expression's tail.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RowWindow {
    pub(crate) offset: Option<i64>,
    pub(crate) limit: Option<i64>,
    pub(crate) with_ties: bool,
}

/// Evaluate a query expression's `OFFSET`/`LIMIT`/`WITH TIES` tail, folding any
/// subquery inside the counts first so it reads under the same snapshot.
pub(crate) fn query_row_window(
    read_ctx: &crate::subquery::SubCtx<'_>,
    q: &crabka_pgparser::ast::QueryExpr,
) -> Result<RowWindow, ExecError> {
    let ctx = read_ctx.eval_ctx;
    let (limit, offset) = crate::subquery::resolve_row_counts(read_ctx, q)?;
    Ok(RowWindow {
        offset: eval_row_count(offset.as_ref(), RowCountClause::Offset, ctx)?,
        limit: eval_row_count(limit.as_ref(), RowCountClause::Limit, ctx)?,
        with_ties: q.with_ties,
    })
}

/// Apply `OFFSET`/`LIMIT` to rows already sorted by `order_by` and carrying
/// their sort keys.
///
/// `WITH TIES` extends the limit through every row whose ORDER BY key equals the
/// last row the plain limit admits, so the cut never splits a group of equal
/// keys. Without it this is exactly [`apply_offset_limit`].
pub(crate) fn apply_row_window<T>(
    mut keyed: Vec<(Vec<Datum>, T)>,
    window: RowWindow,
    order_by: &[crabka_pgparser::ast::OrderItem],
) -> Vec<T> {
    apply_offset_limit(&mut keyed, window.offset, None);
    if let Some(limit) = window.limit {
        let keep = usize::try_from(limit.max(0)).unwrap_or(usize::MAX);
        let mut end = keep.min(keyed.len());
        if window.with_ties && end > 0 {
            let last = keyed[end - 1].0.clone();
            while end < keyed.len() && order_cmp(&keyed[end].0, &last, order_by).is_eq() {
                end += 1;
            }
        }
        keyed.truncate(end);
    }
    keyed.into_iter().map(|(_, row)| row).collect()
}
