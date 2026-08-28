//! DML source rows and assignment resolution.

use super::{correlated_bind::resolve_uncorrelated_derived_projections, *};

/// The `FROM`/`USING` relation joined to a DML target, materialized once for the
/// whole statement. The plain (unjoined) form is the degenerate case with one
/// empty source row, so both share one code path.
pub(crate) struct DmlSource {
    /// Target columns first, then the source relation's columns, then the
    /// target's hidden system columns.
    ///
    /// The system columns go last rather than beside the target's own, which is
    /// where every scan puts them, because `RETURNING` reads this row as two
    /// blocks: the target's declared columns, which it replaces with the
    /// statement's post-image, and everything after them, which it keeps. A
    /// system column in the first block would be overwritten by the post-image
    /// and project some other column's value — a `ctid` that silently reads as
    /// the first column of the row.
    pub(crate) scope: Scope,
    rows: Vec<Vec<Datum>>,
    joined: bool,
    /// The target's system columns and the values they take. Stamped onto each
    /// candidate row by [`Self::first_match`], from the identity the write loop
    /// holds — never from the row, which does not carry one.
    stamp: crate::scope::SystemStamp,
}

impl DmlSource {
    /// `refs` is what the statement asks the target to carry, and `None` is a
    /// target that has no storage identity to answer with — the view-write
    /// path, whose rows come out of the view's own query. A view is otherwise
    /// an ordinary `Table` here, so left to
    /// [`crate::scope::SystemColumns::of`] it would be offered a `ctid` it
    /// cannot produce a value for.
    pub(crate) fn build(
        write_ctx: &WriteContext<'_>,
        ctes: &crate::cte::CteContext,
        table: &Table,
        qualifier: &str,
        from: &[crabka_pgparser::ast::TableExpr],
        refs: Option<&crate::scope::StatementRefs>,
    ) -> Result<Self, ExecError> {
        let mut scope = Scope::single(table, qualifier);
        let stamp = crate::scope::SystemColumns::of(refs, table).stamp(table.id)?;
        if from.is_empty() {
            stamp.extend_scope(&mut scope, qualifier);
            return Ok(Self {
                scope,
                rows: vec![Vec::new()],
                joined: false,
                stamp,
            });
        }
        let base = write_ctx.read_ctx(ctes);
        let read = base.with_refs_opt(refs);
        // The target relation is not in the FROM/USING items' scope — SQL puts
        // it out of their reach, and `LATERAL` cannot bring it back — so a name
        // only the target supplies is that prohibition, not a missing entry.
        let kind = if from.iter().any(item_is_lateral) {
            OuterReference::LateralTarget
        } else {
            OuterReference::Target
        };
        let rel = build_from(&read, from, None, None, None, None)
            .map_err(|error| explain_outer_reference(error, &scope, kind))?;
        scope.extend(&rel.scope);
        stamp.extend_scope(&mut scope, qualifier);
        Ok(Self {
            scope,
            rows: rel.rows,
            joined: true,
            stamp,
        })
    }

    /// The predicate the index-probe planner may use to narrow the target scan.
    /// A joined statement's `WHERE` mentions source columns the probe cannot
    /// resolve, so it falls back to a full scan.
    pub(crate) fn probe_filter<'f>(&self, filter: Option<&'f Expr>) -> Option<&'f Expr> {
        if self.joined { None } else { filter }
    }

    /// Bind the statement's `WHERE` against the combined scope, once, before
    /// the candidate loop. The index probe keeps the source spelling — it reads
    /// column NAMES to match an index — so only [`Self::first_match`] takes the
    /// bound form.
    pub(crate) fn bind_filter(
        &self,
        filter: Option<&Expr>,
    ) -> Result<Option<crate::bind::BoundExpr>, ExecError> {
        crate::bind::bind_optional(filter, &self.scope)
    }

    /// Resolve only the subqueries independent of the DML target/source row.
    pub(crate) fn resolve_filter(
        &self,
        read_ctx: &crate::subquery::SubCtx<'_>,
        filter: Option<&Expr>,
    ) -> Result<Option<Expr>, ExecError> {
        filter
            .map(|filter| {
                let filter =
                    resolve_uncorrelated_derived_projections(read_ctx, filter, &self.scope)?;
                let filter = materialize_correlated_exists(read_ctx, &filter, &self.scope)?;
                crate::subquery::resolve_expr_skipping(read_ctx, &filter, &mut |node| {
                    expression_contains_correlated_subquery(read_ctx, node, &self.scope)
                })
            })
            .transpose()
    }

    /// The first source row that satisfies `filter` for this target row, as the
    /// combined row expressions resolve against. `None` means the target row is
    /// not affected by the statement.
    ///
    /// `identity` is the target row's storage identity — its rowid — which the
    /// system columns are derived from. It is what makes a `ctid` in a `WHERE`
    /// name the row the statement is about to write, and not some other: the
    /// write loop reads it from the same candidate it is holding the row of,
    /// and passes both here together.
    pub(crate) fn first_match(
        &self,
        filter: Option<&crate::bind::BoundExpr>,
        target_row: &[Datum],
        identity: u64,
        xmin: u64,
        ctx: &crate::clock::EvalCtx,
    ) -> Result<Option<Vec<Datum>>, ExecError> {
        let filter = filter.map(crate::bind::BoundExpr::expr);
        for source_row in &self.rows {
            let mut combined = target_row.to_vec();
            combined.extend_from_slice(source_row);
            self.stamp
                .extend_row(&mut combined, identity, xmin, 0, 0, 0);
            if row_matches(filter, &self.scope, &combined, ctx)? {
                return Ok(Some(combined));
            }
        }
        Ok(None)
    }

    /// The correlated counterpart to [`Self::first_match`].
    pub(super) fn first_match_correlated(
        &self,
        read_ctx: &crate::subquery::SubCtx<'_>,
        filter: Option<&Expr>,
        target_row: &[Datum],
        identity: u64,
        xmin: u64,
        binder: &mut LateralBinder<'_>,
    ) -> Result<Option<Vec<Datum>>, ExecError> {
        for source_row in &self.rows {
            let mut combined = target_row.to_vec();
            combined.extend_from_slice(source_row);
            self.stamp
                .extend_row(&mut combined, identity, xmin, 0, 0, 0);
            if row_matches_correlated(read_ctx, filter, &self.scope, &combined, binder)? {
                return Ok(Some(combined));
            }
        }
        Ok(None)
    }
}

/// Materialize the inner keys of a top-level `EXISTS` equality once per DML
/// statement. The rewritten `IN` predicate has the same WHERE truth table and
/// avoids rebuilding an uncorrelated FROM relation for every target row.
fn materialize_correlated_exists(
    read_ctx: &crate::subquery::SubCtx<'_>,
    filter: &Expr,
    outer: &Scope,
) -> Result<Expr, ExecError> {
    use crabka_pgparser::ast::{DistinctClause, QueryBody, SetExpr};

    let Expr::Exists(query) = filter else {
        return Ok(filter.clone());
    };
    if query.with.is_some()
        || !query.order_by.is_empty()
        || query.limit.is_some()
        || query.offset.is_some()
        || query.with_ties
        || query.locking.is_some()
    {
        return Ok(filter.clone());
    }
    let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
        return Ok(filter.clone());
    };
    if !matches!(select.distinct, DistinctClause::All)
        || !select.group_by.is_empty()
        || select.grouping.is_some()
        || select.having.is_some()
        || !select.windows.is_empty()
        || !select.window_calls.is_empty()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.with_ties
        || select.locking.is_some()
    {
        return Ok(filter.clone());
    }
    let Some(Expr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
    }) = select.filter.as_ref()
    else {
        return Ok(filter.clone());
    };
    let nulls = vec![Datum::Null; outer.width()];
    let mut binder =
        LateralBinder::new(read_ctx.catalog_kv, read_ctx.fctx.resolution, read_ctx.ctes);
    let (_, left_is_outer) = binder.bind_expr(left, outer, &nulls)?;
    let (_, right_is_outer) = binder.bind_expr(right, outer, &nulls)?;
    let (outer_expr, inner_expr) = match (left_is_outer, right_is_outer) {
        (true, false) => (left, right),
        (false, true) => (right, left),
        _ => return Ok(filter.clone()),
    };
    let mut keys = query.clone();
    let SetExpr::Query(QueryBody::Select(select)) = &mut keys.body else {
        unreachable!("the EXISTS query was a SELECT")
    };
    select.projection = vec![crabka_pgparser::ast::SelectItem::Expr {
        expr: (**inner_expr).clone(),
        alias: None,
    }];
    select.filter = None;
    let relation = crate::query::query_to_relation_with_ctes(read_ctx, &keys)?;
    if relation.scope.width() != 1 {
        return Ok(filter.clone());
    }
    let ty = relation.scope.ty_at(0);
    Ok(Expr::InList {
        expr: Box::new((**outer_expr).clone()),
        list: relation
            .rows
            .into_iter()
            .map(|mut row| Expr::Const {
                value: row.remove(0),
                ty,
            })
            .collect(),
        negated: false,
    })
}

/// One `SET` target after analysis: the column slot plus how its new value is
/// produced.
pub(crate) enum AssignedValue<'a> {
    /// Evaluated against the joined row, per affected row.
    Expr(&'a Expr),
    /// Already computed: a multi-column `= (SELECT …)`, which `PostgreSQL`
    /// evaluates once when the sub-select does not reference the target.
    Value(Datum),
    /// `SET r.field = e` / `SET j['a'][0] = e`: the write puts the new value
    /// into the column's current value at the ordered indirect path.
    Indirect {
        indirections: &'a [TargetIndirection],
        value: &'a Expr,
    },
    IndirectValue {
        indirections: &'a [TargetIndirection],
        value: Datum,
    },
}

/// Resolve every `SET` entry to a column slot and a value source, raising
/// `PostgreSQL`'s analysis errors up front: 42703 for an unknown column, 42701
/// for a column assigned twice, and 42601 for an arity mismatch on the
/// multi-column forms.
pub(crate) fn resolve_assignments<'a>(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    table: &Table,
    assignments: &'a [crabka_pgparser::ast::Assignment],
) -> Result<Vec<(usize, AssignedValue<'a>)>, ExecError> {
    use crabka_pgparser::ast::AssignmentValue;

    validate_assignment_targets(table, assignments)?;
    let mut out: Vec<(usize, AssignedValue<'a>)> = Vec::new();
    for assignment in assignments {
        let slots = assignment
            .targets
            .iter()
            .map(|column| {
                table
                    .column_index(column)
                    .expect("validated assignment target")
            })
            .collect::<Vec<_>>();
        match &assignment.value {
            AssignmentValue::Expr(expr) if !assignment.indirections.is_empty() => {
                debug_assert_eq!(slots.len(), 1, "single-target assignment");
                out.push((
                    slots[0],
                    AssignedValue::Indirect {
                        indirections: &assignment.indirections,
                        value: expr,
                    },
                ));
            }
            AssignmentValue::Expr(expr) => {
                debug_assert_eq!(slots.len(), 1, "single-target assignment");
                out.push((slots[0], AssignedValue::Expr(expr)));
            }
            AssignmentValue::Row(items) => {
                if items.len() != slots.len() {
                    return Err(assignment_arity_error(slots.len(), items.len()));
                }
                for (offset, (slot, expr)) in slots.iter().zip(items).enumerate() {
                    let value = if offset == 0 && !assignment.indirections.is_empty() {
                        AssignedValue::Indirect {
                            indirections: &assignment.indirections,
                            value: expr,
                        }
                    } else {
                        AssignedValue::Expr(expr)
                    };
                    out.push((*slot, value));
                }
            }
            AssignmentValue::Subquery(query) => {
                let read = write_ctx.read_ctx(ctes);
                let rel = crate::query::query_to_relation(&read, query)?;
                if rel.scope.width() != slots.len() {
                    return Err(assignment_arity_error(slots.len(), rel.scope.width()));
                }
                if rel.rows.len() > 1 {
                    return Err(ExecError::CardinalityViolation);
                }
                // A sub-select that returns no row assigns NULL to every target,
                // exactly as a zero-row scalar subquery evaluates to NULL.
                for (offset, slot) in slots.iter().enumerate() {
                    let value = rel
                        .rows
                        .first()
                        .map_or(Datum::Null, |row| row[offset].clone());
                    let value = if offset == 0 && !assignment.indirections.is_empty() {
                        AssignedValue::IndirectValue {
                            indirections: &assignment.indirections,
                            value,
                        }
                    } else {
                        AssignedValue::Value(value)
                    };
                    out.push((*slot, value));
                }
            }
        }
    }
    let mut seen = HashSet::new();
    for (slot, value) in &out {
        // Indirect entries update the column in place rather than replacing it,
        // so `SET j['a'] = …, j['b'] = …` is legal in PostgreSQL and each
        // one sees the previous one's result.
        if matches!(value, AssignedValue::Indirect { .. }) {
            continue;
        }
        if !seen.insert(*slot) {
            // PostgreSQL reports a repeated assignment target as a syntax
            // error (42601), not as a duplicate-object error.
            return Err(ExecError::Syntax(format!(
                "multiple assignments to same column \"{}\"",
                table.columns[*slot].name
            )));
        }
    }
    Ok(out)
}

/// Reject unknown and system-column write targets before a DML path begins
/// scanning rows.  In particular, a `MERGE` must reject them even when its
/// source and target do not join.
pub(crate) fn validate_assignment_targets(
    table: &Table,
    assignments: &[crabka_pgparser::ast::Assignment],
) -> Result<(), ExecError> {
    for column in assignments
        .iter()
        .flat_map(|assignment| &assignment.targets)
    {
        if table.column_index(column).is_none() {
            // A system column is not unknown, it is unassignable, and
            // `PostgreSQL` separates the two. A relation that declares a
            // column of the name — only a view may — resolves it above.
            return Err(if crate::scope::is_system_column(column) {
                ExecError::AssignSystemColumn(column.clone())
            } else {
                ExecError::UndefinedColumn(column.clone())
            });
        }
    }
    Ok(())
}

/// Apply the only writable `pg_class` fields Gres needs before it has a
/// general writable system-catalog implementation.
///
/// PostgreSQL lets a superuser adjust these planner estimates. They are not
/// heap rows here: the catalog projection is synthesized, while the values are
/// durable relation metadata. Keeping this seam narrow avoids pretending the
/// other synthesized catalog fields are writable.
pub(crate) fn update_pg_class_statistics(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    alias: Option<&str>,
    assignments: &[crabka_pgparser::ast::Assignment],
    from: &[crabka_pgparser::ast::TableExpr],
    filter: Option<&Expr>,
    returning: Option<&crabka_pgparser::ast::Returning>,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    if !crate::rls::role_is_superuser(write_ctx.catalog_kv, write_ctx.fctx.effective_role())? {
        return Err(ExecError::PermissionDenied {
            kind: "table",
            relation: "pg_class".into(),
        });
    }
    if !from.is_empty() {
        return Err(ExecError::Unsupported(
            "UPDATE pg_class with FROM is not supported".into(),
        ));
    }

    let table = virtual_catalog_table("pg_class");
    let qualifier = alias.unwrap_or("pg_class");
    let scope = Scope::single(&table, qualifier);
    let targets = resolve_assignments(write_ctx, ctes, &table, assignments)?;
    for (slot, _) in &targets {
        if !matches!(
            table.columns[*slot].name.as_str(),
            "reltuples" | "relpages" | "relallvisible"
        ) {
            return Err(ExecError::Unsupported(format!(
                "UPDATE pg_class does not support column \"{}\"",
                table.columns[*slot].name
            )));
        }
    }
    let filter = crate::bind::bind_optional(filter, &scope)?;
    let spec = ReturningSpec::new(&table, qualifier, returning, Some(&scope), false)?;
    let relations = crabka_pgcatalog::list_tables(write_ctx.catalog_kv)?
        .into_iter()
        .map(|table| {
            Ok((
                crate::catalog_rel::table_relation_oid(table.id)?,
                table.name,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, ExecError>>()?;
    let mut ops = Vec::new();
    let mut returned_rows = Vec::new();
    let mut updated = 0_u64;
    for (ordinal, row) in catalog_rows::pg_class_rows(write_ctx.catalog_kv)?
        .into_iter()
        .enumerate()
    {
        let Some(Datum::Int4(oid)) = row.first() else {
            return Err(ExecError::Kv(crabka_pgkv::KvError::CorruptRow(
                "pg_class row has no oid".into(),
            )));
        };
        let Some(relation) = relations.get(oid) else {
            continue;
        };
        if !row_matches(
            filter.as_ref().map(crate::bind::BoundExpr::expr),
            &scope,
            &row,
            write_ctx.eval_ctx,
        )? {
            continue;
        }
        let next = apply_assignments(&table, &targets, &scope, &row, write_ctx.eval_ctx)?;
        for (slot, _) in &targets {
            match (&*table.columns[*slot].name, &next[*slot]) {
                ("reltuples", Datum::Float4(value)) => {
                    ops.push(crate::relstats::set_reltuples_op(relation, *value));
                }
                ("relpages", Datum::Int4(value)) => {
                    ops.push(crate::relstats::set_relpages_op(relation, *value));
                }
                ("relallvisible", Datum::Int4(value)) => {
                    ops.push(crate::relstats::set_relallvisible_op(relation, *value));
                }
                (column, Datum::Null) => {
                    return Err(ExecError::NotNullViolation {
                        column: column.into(),
                        table: "pg_class".into(),
                        row: None,
                    });
                }
                (column, _) => {
                    return Err(ExecError::Unsupported(format!(
                        "UPDATE pg_class received an invalid value for column \"{column}\""
                    )));
                }
            }
        }
        if returning.is_some() {
            let ordinal = u64::try_from(ordinal)
                .map_err(|_| ExecError::Unsupported("pg_class has too many rows".into()))?
                .checked_add(1)
                .ok_or_else(|| ExecError::Unsupported("pg_class has too many rows".into()))?;
            returned_rows.push(ReturnedRow::updated(
                next,
                row,
                Vec::new(),
                ordinal,
                ordinal,
                0,
                0,
                0,
                0,
            ));
        }
        updated += 1;
    }
    spec.outcome(
        format!("UPDATE {updated}"),
        returned_rows,
        write_ctx.eval_ctx,
    )
    .map(|outcome| (outcome, ops))
}

fn assignment_arity_error(targets: usize, values: usize) -> ExecError {
    ExecError::Syntax(format!(
        "number of columns ({targets}) does not match number of values ({values})"
    ))
}

/// Apply the resolved assignments to a copy of the target row.
///
/// Unsettled, as [`build_insert_row`] is: the post-image an `UPDATE` proposes
/// reaches its `BEFORE ROW` triggers before [`finish_written_row`] judges it.
pub(crate) fn apply_assignments(
    table: &Table,
    targets: &[(usize, AssignedValue<'_>)],
    scope: &Scope,
    joined_row: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Datum>, ExecError> {
    let mut next = joined_row[..table.columns.len()].to_vec();
    for (idx, value) in targets {
        // As on the INSERT side, `DEFAULT` is the only value an `UPDATE` may
        // name for a generated column.
        if table.columns[*idx].generated.is_some()
            && !matches!(value, AssignedValue::Expr(Expr::Default))
        {
            return Err(ExecError::GeneratedColumnWrite {
                message: format!(
                    "column \"{}\" can only be updated to DEFAULT",
                    table.columns[*idx].name
                ),
                column: table.columns[*idx].name.clone(),
            });
        }
        let raw = match value {
            AssignedValue::Value(value) => value.clone(),
            AssignedValue::Expr(Expr::Default) => default_value(&table.columns[*idx], ctx)?,
            AssignedValue::Expr(expr) => {
                eval_assignment_value(expr, table.columns[*idx].ty, scope, joined_row, ctx)?
            }
            // An indirect target reads the column's *current* value, so a
            // second entry for the same column sees the first one's result.
            AssignedValue::Indirect {
                indirections,
                value,
            } => {
                let target = target_indirection_type(table.columns[*idx].ty, indirections)?;
                let new_value = eval_assignment_value(value, target, scope, joined_row, ctx)?;
                assign_target_indirections(
                    &next[*idx],
                    table.columns[*idx].ty,
                    indirections,
                    &new_value,
                    scope,
                    joined_row,
                    ctx,
                )?
            }
            AssignedValue::IndirectValue {
                indirections,
                value,
            } => assign_target_indirections(
                &next[*idx],
                table.columns[*idx].ty,
                indirections,
                value,
                scope,
                joined_row,
                ctx,
            )?,
        };
        next[*idx] = coerce(raw, table.columns[*idx].ty, ctx)?;
    }
    Ok(next)
}
