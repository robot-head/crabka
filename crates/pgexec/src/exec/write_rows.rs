//! Shared INSERT, UPDATE, and COPY row construction.

use super::*;

/// Resolve INSERT target column indices: explicit `(cols...)` mapped to their
/// catalog positions (42703 on miss), or all columns in declared order.
pub(super) fn resolve_targets(
    t: &Table,
    columns: &Option<Vec<String>>,
) -> Result<Vec<usize>, ExecError> {
    match columns {
        Some(cols) => {
            let mut seen = std::collections::HashSet::new();
            if let Some(repeated) = cols.iter().find(|column| !seen.insert(*column)) {
                // 42701: naming a column twice makes the statement's intent
                // undecidable, and PostgreSQL says so rather than letting the
                // second value quietly win.
                return Err(ExecError::DuplicateOutputColumn(repeated.clone()));
            }
            cols.iter()
                .map(|c| {
                    t.column_index(c)
                        .ok_or_else(|| ExecError::UndefinedColumn(c.clone()))
                })
                .collect::<Result<_, _>>()
        }
        None => Ok((0..t.columns.len()).collect()),
    }
}

/// [`resolve_targets`] under `CopyGetAttnums`'s rule about generated columns:
/// the default list leaves one out, and an explicit list may not name one.
///
/// `COPY` supplies field values, and a generated column's value is produced by
/// the write rather than supplied — so unlike `INSERT`, which accepts the
/// column with `DEFAULT` written against it, a copy has no spelling for it at
/// all. Leaving it in the implicit list demanded a field per row for a column
/// nothing may assign, which is 22P04 on the first line of every
/// `COPY t FROM stdin` over a relation that has one.
pub(crate) fn resolve_copy_targets(
    t: &Table,
    columns: &Option<Vec<String>>,
) -> Result<Vec<usize>, ExecError> {
    let slots = resolve_targets(t, columns)?;
    if columns.is_none() {
        return Ok(slots
            .into_iter()
            .filter(|slot| t.columns[*slot].generated.is_none())
            .collect());
    }
    for slot in &slots {
        let column = &t.columns[*slot];
        if column.generated.is_some() {
            return Err(copy_generated_column(&column.name));
        }
    }
    Ok(slots)
}

/// `PostgreSQL`'s refusal of a generated column written in a `COPY` column
/// list, raised by every path that resolves such a list.
///
/// Upstream raises it from `CopyGetAttnums`, which both directions go through,
/// so one spelling serves both. On the load side the timing matters as much as
/// the text: the session refuses while the statement is still being analysed,
/// which keeps the server out of copy-in mode — psql then reads the rest of its
/// script as SQL rather than as data.
pub(crate) fn copy_generated_column(column: &str) -> ExecError {
    ExecError::Remote(
        crabka_pgwire::error::PgError::error(
            "42P10",
            format!("column \"{column}\" is a generated column"),
        )
        .with_detail("Generated columns cannot be used in COPY."),
    )
}

/// The column slots an `INSERT` fills, given how many expressions its source
/// supplies per row.
///
/// `PostgreSQL` (`transformInsertRow`) checks the two directions differently. Too
/// many expressions is always an error. Too few is an error only when the
/// statement wrote an explicit column list. With no list, the implicit target
/// list is truncated to the source width, and the columns past it take their
/// defaults. This is why `INSERT INTO t3 SELECT a, b FROM s` is legal against a
/// three-column table.
pub(super) fn resolve_insert_targets(
    t: &Table,
    columns: &Option<Vec<String>>,
    indirections: &Option<Vec<Vec<TargetIndirection>>>,
    width: usize,
) -> Result<Vec<usize>, ExecError> {
    let mut target_idx = resolve_insert_target_slots(t, columns, indirections)?;
    if width > target_idx.len() {
        return Err(ExecError::Syntax(
            "INSERT has more expressions than target columns".into(),
        ));
    }
    if width < target_idx.len() {
        if columns.is_some() {
            return Err(ExecError::Syntax(
                "INSERT has more target columns than expressions".into(),
            ));
        }
        target_idx.truncate(width);
    }
    Ok(target_idx)
}

/// [`resolve_targets`] under INSERT's indirection rule: a base column may
/// occur more than once only when every occurrence writes through indirection.
pub(super) fn resolve_insert_target_slots(
    t: &Table,
    columns: &Option<Vec<String>>,
    indirections: &Option<Vec<Vec<TargetIndirection>>>,
) -> Result<Vec<usize>, ExecError> {
    let (Some(columns), Some(indirections)) = (columns, indirections) else {
        return resolve_targets(t, columns);
    };
    let mut seen: std::collections::HashMap<&String, bool> = std::collections::HashMap::new();
    for (column, indirections) in columns.iter().zip(indirections) {
        let indirect = !indirections.is_empty();
        if let Some(all_subscripted) = seen.get_mut(column) {
            if !*all_subscripted || !indirect {
                return Err(ExecError::DuplicateOutputColumn(column.clone()));
            }
        } else {
            seen.insert(column, indirect);
        }
    }
    columns
        .iter()
        .map(|column| {
            t.column_index(column)
                .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))
        })
        .collect()
}

/// The row a write path starts from: every column's `DEFAULT`, evaluated only
/// for the columns the statement did not supply a value for.
///
/// A skip of the supplied ones is not just saved work. A `DEFAULT` can be a
/// side effect. `nextval('s')` is one, and it is what a `SERIAL` column and both
/// flavours of `GENERATED … AS IDENTITY` desugar to. `PostgreSQL` advances the
/// sequence only for a column it actually defaults. So `INSERT INTO t (id, b)
/// VALUES (100, 'x')` leaves the sequence untouched, and the next generated id is
/// the one that insert would otherwise have burned. The choice is per row and
/// per column: in `VALUES (100, 'a'), (DEFAULT, 'b')` only the second row
/// advances, and a supplied identity column does not stop a *different*
/// identity column in the same row from advancing. All verified against
/// `postgres:18.4`.
///
/// A supplied slot is left `Null` here and overwritten by the caller, which is
/// also how an explicit `DEFAULT` keyword gets its value. That one does advance
/// the sequence, because the column really is taking its default.
pub(super) fn unsupplied_defaults(
    table: &Table,
    target_idx: &[usize],
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Datum>, ExecError> {
    let mut supplied = vec![false; table.columns.len()];
    for &slot in target_idx {
        supplied[slot] = true;
    }
    table
        .columns
        .iter()
        .zip(supplied)
        .map(|(column, supplied)| {
            if supplied {
                Ok(Datum::Null)
            } else {
                default_value(column, ctx)
            }
        })
        .collect()
}

/// Assemble one proposed `INSERT` row: the supplied values coerced into place,
/// every unsupplied column at its default.
///
/// The row is deliberately NOT settled here — see [`finish_written_row`], which
/// runs after the relation's `BEFORE ROW` triggers have had their say.
pub(super) fn build_insert_row(
    table: &Table,
    target_idx: &[usize],
    row_exprs: &[Expr],
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Datum>, ExecError> {
    let mut row = unsupplied_defaults(table, target_idx, ctx)?;
    for (slot, expr) in target_idx.iter().zip(row_exprs.iter()) {
        // A `GENERATED ALWAYS` column takes its value from its expression, so
        // the only value a statement may name for it is `DEFAULT`.
        if table.columns[*slot].generated.is_some() && !matches!(expr, Expr::Default) {
            return Err(ExecError::GeneratedColumnWrite {
                message: format!(
                    "cannot insert a non-DEFAULT value into column \"{}\"",
                    table.columns[*slot].name
                ),
                column: table.columns[*slot].name.clone(),
            });
        }
        let value = match expr {
            Expr::Default => default_value(&table.columns[*slot], ctx)?,
            _ => {
                let target = table.columns[*slot].ty;
                let value = eval_assignment_value(expr, target, &Scope::empty(), &[], ctx)?;
                coerce(value, target, ctx)?
            }
        };
        row[*slot] = value;
    }
    Ok(row)
}

/// Assemble an INSERT row whose target list may assign through fields or
/// subscripts. The target value starts from its default, just as an UPDATE
/// indirect target reads the current value before replacing one path.
pub(super) fn build_insert_row_with_subscripts(
    table: &Table,
    target_idx: &[usize],
    indirections: &Option<Vec<Vec<TargetIndirection>>>,
    row_exprs: &[Expr],
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Datum>, ExecError> {
    let Some(indirections) = indirections else {
        return build_insert_row(table, target_idx, row_exprs, ctx);
    };
    let mut initial = row_exprs.to_vec();
    for (expr, indirections) in initial.iter_mut().zip(indirections) {
        if !indirections.is_empty() {
            *expr = Expr::Default;
        }
    }
    let mut row = build_insert_row(table, target_idx, &initial, ctx)?;
    for ((slot, expr), indirections) in target_idx.iter().zip(row_exprs).zip(indirections) {
        if indirections.is_empty() {
            continue;
        }
        let target = target_indirection_type(table.columns[*slot].ty, indirections)?;
        let value = eval_assignment_value(expr, target, &Scope::empty(), &[], ctx)?;
        row[*slot] = assign_target_indirections(
            &row[*slot],
            table.columns[*slot].ty,
            indirections,
            &value,
            &Scope::empty(),
            &[],
            ctx,
        )?;
    }
    Ok(row)
}

/// [`build_insert_row`] for one `COPY … FROM` line, and unsettled for the same
/// reason: the relation's `BEFORE ROW` triggers see the row first.
pub(super) fn build_copy_row(
    table: &Table,
    target_idx: &[usize],
    row: &crate::copyfmt::CopyRow<'_>,
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Datum>, ExecError> {
    let mut built = unsupplied_defaults(table, target_idx, ctx)?;
    for (slot, value) in target_idx.iter().zip(row.values.iter()) {
        let column = &table.columns[*slot];
        let target = column.ty;
        let converted = match value {
            Some(value) if let Some(base) = jsonpath_assignment_base(target) => {
                crate::eval::cast_value_in_at(
                    &Datum::Text(value.clone()),
                    base,
                    ctx.output_style(),
                    ctx.now,
                )
                .and_then(|value| coerce(value, target, ctx))
            }
            // A COPY field runs the column type's input function under
            // *assignment* rules, so an over-long `varchar(n)` is 22001 rather
            // than the silent truncation an explicit cast would do — and the
            // date arms read the session's `DateStyle` field order.
            Some(value) => crate::eval::cast_assign_value_in_at(
                &Datum::Text(value.clone()),
                target,
                ctx.output_style(),
                ctx.now,
            ),
            None => coerce(Datum::Null, target, ctx),
        };
        // An input conversion is the one failure PostgreSQL can name a column
        // for: it is running that column's input function and holds the field
        // it was handed. Everything after this loop judges the assembled row,
        // and reports the line alone.
        built[*slot] = converted.map_err(|error| {
            copy_row_context(
                error,
                &table.name.name,
                row,
                crate::copyfmt::CopyContext::Column {
                    name: &column.name,
                    value: value.as_deref(),
                },
            )
        })?;
    }
    Ok(built)
}

/// Refuse a row whose field count does not match the columns being copied into.
///
/// `PostgreSQL` reports the two directions differently and gives both 22P04:
/// too few fields names the first column left unsupplied, too many says only
/// that there were more. Both carry the whole line as `CONTEXT`, because
/// neither failure belongs to a column that was read.
pub(super) fn copy_row_width(
    table: &Table,
    target_idx: &[usize],
    row: &crate::copyfmt::CopyRow<'_>,
) -> Result<(), ExecError> {
    if row.values.len() == target_idx.len() {
        return Ok(());
    }
    let error = match target_idx.get(row.values.len()) {
        Some(slot) => ExecError::Remote(crabka_pgwire::error::PgError::error(
            "22P04",
            format!(
                "missing data for column \"{}\"",
                table.columns[*slot].name.clone()
            ),
        )),
        None => ExecError::Remote(crabka_pgwire::error::PgError::error(
            "22P04",
            "extra data after last expected column",
        )),
    };
    Err(copy_row_context(
        error,
        &table.name.name,
        row,
        crate::copyfmt::CopyContext::Line { raw: row.raw },
    ))
}

/// Attach the `CONTEXT` a per-row `COPY … FROM` failure carries.
///
/// The relation is named unqualified whatever the statement wrote, because this
/// is `RelationGetRelationName` and not a regclass.
///
/// A context already on the error is kept and this line goes *below* it, which
/// is how a `plpgsql` trigger firing on a copied row reports both its own frame
/// and the row that reached it. Contexts stack outward, so each frame appends.
pub(super) fn copy_row_context(
    error: ExecError,
    relation: &str,
    row: &crate::copyfmt::CopyRow<'_>,
    at: CopyContext<'_>,
) -> ExecError {
    with_copy_context(error, crate::copyfmt::copy_context(relation, row.line, at))
}

/// Add `context` below whatever context `error` already carried.
pub(crate) fn with_copy_context(error: ExecError, context: String) -> ExecError {
    let reported = error.into_pg();
    let stacked = match reported
        .diagnostics
        .as_ref()
        .and_then(|fields| fields.context.clone())
    {
        Some(existing) => format!("{existing}\n{context}"),
        None => context,
    };
    ExecError::Remote(reported.with_context(stacked))
}

/// The per-row work every write path shares once the row it will actually
/// store is settled.
///
/// Compute `GENERATED … STORED` columns, then enforce the domain, then `NOT
/// NULL`, then the table's `CHECK` constraints. That is `PostgreSQL`'s order,
/// and the reason a generated column can satisfy a `CHECK` that references it.
///
/// "Actually store" is the whole point of *when* this runs.
/// [`crate::trigger::fire_before_row`] is the caller for every write path that
/// fires row triggers, and it calls this AFTER the last `BEFORE ROW` trigger
/// has returned its replacement row. A generated column settled before the
/// trigger would keep whatever the trigger then assigned to it — a `STORED`
/// column holding a value its own expression never produced — and a constraint
/// checked before the trigger would judge a row nobody stores. The three write
/// paths that fire no row trigger at all — a view's `INSTEAD OF` insert and
/// update, and a sharded `COPY` — call it themselves.
pub(crate) fn finish_written_row(
    table: &Table,
    row: &mut [Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    apply_generated_columns(table, row, ctx)?;
    // The constraints are checked against a *logically* complete row: `row`
    // itself keeps the NULL placeholder a virtual column stores, and the
    // expansion happens on a copy only when a constraint could read it.
    let checked = if virtual_generated_needed_for_constraints(table) {
        let mut expanded = row.to_vec();
        expand_virtual_generated_row(
            table,
            &mut expanded,
            ctx,
            crate::scope::GeneratedReads::every(),
        )?;
        std::borrow::Cow::Owned(expanded)
    } else {
        std::borrow::Cow::Borrowed(&*row)
    };
    for (column, value) in table.columns.iter().zip(checked.iter()) {
        crate::usertype::check_domain(column.ty, value, ctx)?;
    }
    enforce_not_null(table, &checked, ctx)?;
    if table.checks.is_empty() {
        return Ok(());
    }
    let checks = compile_check_constraints(table)?;
    enforce_check_constraints(table, &checks, &checked, ctx)
}

pub(super) fn enforce_not_null(
    table: &Table,
    row: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    for (column, value) in table.columns.iter().zip(row.iter()) {
        if column.not_null && value.is_null() {
            return Err(ExecError::NotNullViolation {
                column: column.name.clone(),
                // Unqualified whatever schema the relation is in and whatever
                // the statement wrote: PostgreSQL builds this message from
                // `RelationGetRelationName`, which is the bare `relname`.
                table: table.name.name.clone(),
                row: Some(ddl_partition::failing_row_detail(row, ctx)),
            });
        }
    }
    Ok(())
}

pub(super) fn default_value(
    column: &Column,
    ctx: &crate::clock::EvalCtx,
) -> Result<Datum, ExecError> {
    let Some(default) = &column.default else {
        return Ok(Datum::Null);
    };
    match default {
        // A stored `regclass` default holds only the oid, so the name it prints
        // is derived here — the same output-time resolution a scanned value
        // gets, which is what lets `RETURNING` print the relation's current
        // name rather than the bare number.
        ColumnDefault::Value(Datum::Regclass(value)) => match ctx.catalog() {
            Some(catalog) => {
                regclass_by_oid(catalog, ctx.resolution(), value.oid).map(Datum::Regclass)
            }
            None => Ok(Datum::Regclass(value.clone())),
        },
        ColumnDefault::Value(value) => Ok(value.clone()),
        ColumnDefault::Expression(source) => {
            let expr = crabka_pgparser::parser::parse_expression(source)?;
            let value = eval_assignment_value(&expr, column.ty, &Scope::empty(), &[], ctx)?;
            coerce_default(value, column.ty, ctx)
        }
        ColumnDefault::NextVal(sequence) => {
            let runtime = ctx.sequence.as_ref().ok_or_else(|| {
                ExecError::Unsupported("sequence defaults require a SQL session".into())
            })?;
            let (value, staged) =
                runtime
                    .manager
                    .nextval(&*runtime.kv, ctx.resolution(), sequence)?;
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
                .insert(sequence.clone(), value);
            coerce(Datum::Int8(value), column.ty, ctx)
        }
    }
}
