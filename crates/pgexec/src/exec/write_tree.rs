use super::*;

/// `UPDATE`/`DELETE` over a partitioned parent: the same statement runs against
/// each leaf in turn and the affected-row counts are summed.
///
/// Divergence from `PostgreSQL`: an `UPDATE` that moves a row out of its own
/// partition's bound is 23514 here (`new row for relation … violates partition
/// constraint`), where `PostgreSQL` deletes the row from its old partition and
/// re-inserts it into the new one. A refusal is the correctness-preserving
/// choice. The alternative stores a row in a partition whose bound it does not
/// satisfy, and every later read would answer that wrongly.
pub(super) async fn partitioned_dml(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let resolution = write_ctx.eval_ctx.resolution();
    let (parent, verb, returning) = match stmt {
        Statement::Update {
            table, returning, ..
        } => (table, "UPDATE", returning),
        Statement::Delete {
            table, returning, ..
        } => (table, "DELETE", returning),
        _ => unreachable!("the caller matched an UPDATE or a DELETE"),
    };
    let parent = resolve_relation(
        write_ctx.catalog_kv,
        resolution,
        parent,
        SchemaDisposition::Reference,
    )?;
    let mut ops = Vec::new();
    let mut affected: u64 = 0;
    let mut returned: Option<Relation> = None;
    let parent_table = crabka_pgcatalog::get_table(write_ctx.catalog_kv, &parent)?;
    for leaf in crate::partition::leaves_of(write_ctx.catalog_kv, &parent)? {
        // The per-leaf body resolves `RETURNING` against the leaf's own column
        // order, so a leaf whose columns are ordered differently would
        // contribute rows in a different shape. That only arises for a leaf
        // attached from a table declared out of order, and it is refused rather
        // than answered with mismatched columns. Without `RETURNING` no row
        // shape escapes the statement, so the mismatch cannot be observed and
        // the write proceeds -- which is what lets `TRUNCATE` reach such a
        // partition, since it runs as an unqualified `DELETE`.
        let leaf_table = crabka_pgcatalog::get_table(write_ctx.catalog_kv, &leaf)?;
        if returning.is_some()
            && column_mapping(&parent_table, &leaf_table)?
                .iter()
                .enumerate()
                .any(|(expected, actual)| expected != *actual)
        {
            return Err(ExecError::Unsupported(format!(
                "{verb} over a partitioned table is not supported when a partition declares its \
                 columns in a different order than its parent: partition \"{leaf}\" does"
            )));
        }
        let per_leaf = retarget_tree_dml(stmt, &leaf, &parent);
        // The partitioned parent owns no rows, but it is the relation the
        // statement named, so it is the one whose ACL and policies decide the
        // write. See [`WriteContext::governing`].
        let leaf_ctx = WriteContext {
            governing: Some(write_ctx.governing.unwrap_or(&parent_table)),
            ..*write_ctx
        };
        let (outcome, leaf_ops) = Box::pin(execute_write_body(
            &leaf_ctx,
            ctes,
            &per_leaf,
            writes,
            Reach::Storage,
        ))
        .await?;
        ops.extend(leaf_ops);
        // The per-leaf body already rendered its own count into the tag; the
        // parent's tag is their sum.
        affected += affected_from_tag(&outcome.tag);
        if let Some(rows) = outcome.returning {
            match &mut returned {
                Some(accumulated) => accumulated.rows.extend(rows.rows),
                None => returned = Some(rows),
            }
        }
    }
    Ok((
        WriteOutcome {
            tag: format!("{verb} {affected}"),
            returning: returned,
        },
        ops,
    ))
}

/// The row count a completed write rendered into its command tag.
fn affected_from_tag(tag: &str) -> u64 {
    tag.rsplit(' ')
        .next()
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or_default()
}

/// Point one relation's `UPDATE`/`DELETE` at a single relation below it.
///
/// Two things travel with the target beyond its name.
///
/// `only` is set because the caller has already enumerated the whole tree:
/// without it a child with children of its own would expand again and write
/// every grandchild once per path to it. It says that and nothing more — the
/// caller passes [`Reach::Storage`] alongside, and that, not this flag, is what
/// decides whether a partitioned target still expands into its leaves. The two
/// questions used to share this one boolean, which is how `ONLY` came to be
/// ignored on a partitioned parent; see [`Reach`]. The flag survives because
/// the sharded-write gate reads it for the inheritance question alone.
///
/// The alias is the subtle one. Every expression in the statement resolves
/// against [`table_qualifier`], which falls back to the *table's own name* when
/// no alias is given — so simply renaming the target silently moves the
/// qualifier from `parent` to `child`, and `UPDATE parent SET … WHERE
/// parent.x = 1` stops resolving with "missing FROM-clause entry for table
/// parent". Pinning the alias to the name the statement was written against
/// keeps every qualified reference — in the filter, in a `SET` right-hand side
/// and in `RETURNING` — resolving to the same thing it did before the rewrite.
fn retarget_tree_dml(
    stmt: &Statement,
    target: &crabka_pgcatalog::RelationName,
    named: &crabka_pgcatalog::RelationName,
) -> Statement {
    let mut per_relation = stmt.clone();
    match &mut per_relation {
        Statement::Update {
            table, only, alias, ..
        }
        | Statement::Delete {
            table, only, alias, ..
        } => {
            *table = crabka_pgparser::ast::RelationRef::qualified(&target.schema, &target.name);
            *only = true;
            if alias.is_none() {
                *alias = Some(named.name.clone());
            }
        }
        _ => unreachable!("the caller matched an UPDATE or a DELETE"),
    }
    per_relation
}

/// `UPDATE parent` / `DELETE FROM parent` over a table-inheritance tree, which
/// `PostgreSQL` applies to the parent *and* every relation below it unless the
/// statement said `ONLY`.
///
/// This mirrors [`inherited_scan`] on the read side, and has to: leaving it out
/// is not a missing feature but a wrong answer, because `SELECT count(*) FROM
/// parent` already counts the child rows that `DELETE FROM parent` then walks
/// past. The tree is a DAG rather than a chain — `d INHERITS (b, c)` under
/// `b, c INHERITS a` is reachable from `a` twice — so the relation list comes
/// from [`crate::inheritance::descendants`], which names each one once, and in
/// the same order the read side appends them.
///
/// Unlike a partitioned write, this never moves a row: `PostgreSQL` updates an
/// inheritance child's row in place even when the new value would have suited
/// the parent, so each relation's rows are simply written where they already
/// live.
pub(super) async fn inherited_dml(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let resolution = write_ctx.eval_ctx.resolution();
    let (named, verb) = match stmt {
        Statement::Update { table, .. } => (table, "UPDATE"),
        Statement::Delete { table, .. } => (table, "DELETE"),
        _ => unreachable!("the caller matched an UPDATE or a DELETE"),
    };
    let named = resolve_relation(
        write_ctx.catalog_kv,
        resolution,
        named,
        SchemaDisposition::Reference,
    )?;
    let parent = crabka_pgcatalog::get_table(write_ctx.catalog_kv, &named)?;
    let mut relations = vec![named.clone()];
    relations.extend(crate::inheritance::descendants(
        write_ctx.catalog_kv,
        &named,
    )?);
    let stmt = &reshape_returning_for_tree(write_ctx, stmt, &parent, &relations)?;
    let mut ops = Vec::new();
    let mut affected: u64 = 0;
    let mut returned: Option<Relation> = None;
    for relation in relations {
        let per_relation = retarget_tree_dml(stmt, &relation, &named);
        // Every relation in the tree is written under the named parent's ACL
        // and policies. See [`WriteContext::governing`].
        let child_ctx = WriteContext {
            governing: Some(write_ctx.governing.unwrap_or(&parent)),
            ..*write_ctx
        };
        let (outcome, child_ops) = Box::pin(execute_write_body(
            &child_ctx,
            ctes,
            &per_relation,
            writes,
            Reach::Storage,
        ))
        .await?;
        ops.extend(child_ops);
        affected += affected_from_tag(&outcome.tag);
        if let Some(rows) = outcome.returning {
            match &mut returned {
                Some(accumulated) => accumulated.rows.extend(rows.rows),
                None => returned = Some(rows),
            }
        }
    }
    Ok((
        WriteOutcome {
            tag: format!("{verb} {affected}"),
            returning: returned,
        },
        ops,
    ))
}

/// Pin a tree write's `RETURNING *` to the parent's column list.
///
/// A `*` is expanded per relation against whatever that relation stores, so a
/// child declared `(b, a)` would contribute its rows in the wrong order and a
/// child with an extra column would contribute an extra field —
/// `PostgreSQL` reports every row of the hierarchy in the *parent's* shape.
/// Naming the parent's columns explicitly, once, makes each relation produce
/// that shape; they resolve in every child because inheritance guarantees the
/// name is there, whatever ordinal it sits at.
///
/// The rewrite is skipped when no relation below the parent departs from its
/// column list, which is the overwhelmingly common case (`CREATE TABLE c ()
/// INHERITS (p)`) — there `*` already expands identically everywhere, and the
/// statement is passed through untouched.
fn reshape_returning_for_tree(
    write_ctx: &WriteContext<'_>,
    stmt: &Statement,
    parent: &Table,
    relations: &[crabka_pgcatalog::RelationName],
) -> Result<Statement, ExecError> {
    let (returning, from, alias) = match stmt {
        Statement::Update {
            returning,
            from,
            alias,
            ..
        } => (returning, from.as_slice(), alias),
        Statement::Delete {
            returning,
            using,
            alias,
            ..
        } => (returning, using.as_slice(), alias),
        _ => unreachable!("the caller matched an UPDATE or a DELETE"),
    };
    let qualifier = alias.clone().unwrap_or_else(|| parent.name.name.clone());
    let Some(returning) = returning else {
        return Ok(stmt.clone());
    };
    let spans_target = |item: &SelectItem| match item {
        SelectItem::Wildcard => true,
        SelectItem::QualifiedWildcard(q) => *q == qualifier,
        SelectItem::Expr { .. } => false,
    };
    if !returning.items.iter().any(spans_target) {
        return Ok(stmt.clone());
    }
    let mut uniform = true;
    for relation in relations {
        let child = crabka_pgcatalog::get_table(write_ctx.catalog_kv, relation)?;
        uniform &= child.columns.len() == parent.columns.len()
            && column_mapping(parent, &child)?
                .iter()
                .enumerate()
                .all(|(expected, actual)| expected == *actual);
    }
    if uniform {
        return Ok(stmt.clone());
    }
    // A bare `*` also spans the FROM/USING relations, and their columns cannot
    // be named here without resolving each source item's own shape. Refusing is
    // the honest answer: silently dropping them would report the wrong row.
    if !from.is_empty() && returning.items.contains(&SelectItem::Wildcard) {
        return Err(ExecError::Unsupported(format!(
            "RETURNING * over an inheritance tree is not supported alongside FROM/USING when a \
             relation below \"{}\" declares the parent's columns differently; name the columns \
             explicitly",
            parent.name
        )));
    }
    let target_columns: Vec<SelectItem> = parent
        .columns
        .iter()
        .map(|column| SelectItem::Expr {
            expr: Expr::Column {
                table: Some(qualifier.clone()),
                name: column.name.clone(),
            },
            alias: Some(column.name.clone()),
        })
        .collect();
    let items = returning
        .items
        .iter()
        .flat_map(|item| {
            if spans_target(item) {
                target_columns.clone()
            } else {
                vec![item.clone()]
            }
        })
        .collect();
    let reshaped = crabka_pgparser::ast::Returning {
        items,
        ..returning.clone()
    };
    let mut out = stmt.clone();
    match &mut out {
        Statement::Update { returning, .. } | Statement::Delete { returning, .. } => {
            *returning = Some(reshaped);
        }
        _ => unreachable!("the caller matched an UPDATE or a DELETE"),
    }
    Ok(out)
}

/// Describe a rejected row for this write's caller, or answer `None` when that
/// caller may not be shown it. A forwarder to the one gate,
/// [`crate::rls::describe_row`], which is where the rule lives.
pub(super) fn describe_row(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    row: &[Datum],
    modified: &[String],
) -> Option<String> {
    let describer = write_ctx.describer();
    crate::rls::describe_row(
        &describer.privileges(write_ctx.catalog_kv),
        &describer.security(write_ctx.catalog_kv),
        table,
        row,
        modified,
        write_ctx.eval_ctx,
    )
}

/// The same decision for a description that names only the partition key —
/// `PostgreSQL`'s `ExecBuildSlotPartitionKeyDescription`, which is a separate
/// function upstream with a separate rule.
///
/// It has no `modifiedCols` fallback: a caller without table-level `SELECT`
/// must hold it on *every* key column or be told nothing, and there is no
/// trimmed middle form. With no column-level grants in this engine that
/// collapses to the table-level test.
pub(super) fn may_describe_key(write_ctx: &WriteContext<'_>, table: &Table) -> bool {
    let describer = write_ctx.describer();
    let kv = write_ctx.catalog_kv;
    matches!(
        crate::rls::decide(
            &describer.security(kv),
            table,
            crabka_pgcatalog::policy::PolicyCommand::Select,
        ),
        Ok(crate::rls::RowSecurity::Open)
    ) && matches!(
        crate::privilege::holds(
            &describer.privileges(kv),
            &table.name,
            &table.owner,
            crate::privilege::Privilege::Select,
        ),
        Ok(true)
    )
}

/// A row written straight into a leaf partition must still satisfy the bound of
/// that leaf *and of every ancestor above it*, `PostgreSQL`'s implicit
/// per-partition `CHECK`, reported as 23514.
///
/// The whole chain, not the immediate parent alone: `PostgreSQL` builds the
/// constraint with `RelationGetPartitionQual`, which walks up to the root, so a
/// row that falls inside a leaf's own bound and outside its grandparent's is
/// still refused. Checking one level would store a row that no routed `INSERT`
/// could ever have put there, and that the parent would then never return.
///
/// Whichever level declines it, the error names the relation the statement
/// wrote and quotes that relation's row, because that is the row the caller
/// offered.
pub(super) fn check_partition_constraint(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    row: &[Datum],
    modified: &[String],
) -> Result<(), ExecError> {
    let kv = write_ctx.catalog_kv;
    let mut current = table.clone();
    let mut current_row = row.to_vec();
    while let Some((parent, bound)) = crate::partition::parent_of(kv, &current.name)? {
        let parent_table = crabka_pgcatalog::get_table(kv, &parent)?;
        let Some(scheme) = crate::partition::scheme_of(kv, &parent)? else {
            return Ok(());
        };
        let ordinals = column_mapping(&parent_table, &current)?;
        let parent_row = ordinals
            .iter()
            .map(|ordinal| current_row.get(*ordinal).cloned().unwrap_or(Datum::Null))
            .collect::<Vec<_>>();
        let siblings = crate::partition::partitions_of(kv, &parent)?;
        if !crate::partition::satisfies(
            &scheme,
            &parent_table.columns,
            &bound,
            &siblings,
            &parent_row,
        )? {
            return Err(ExecError::PartitionConstraintViolation {
                relation: table.name.to_string(),
                row: describe_row(write_ctx, table, row, modified),
            });
        }
        current = parent_table;
        current_row = parent_row;
    }
    Ok(())
}

/// `INSERT` into a partitioned parent: every proposed row is routed to the leaf
/// its partition key selects and written there.
///
/// The rows are built against the *parent's* column list, so defaults, coercion
/// and `NOT NULL` all come from the parent. They are then permuted into the
/// chosen leaf's own column order on the way out, so a leaf attached with its
/// columns in a different order still stores them correctly.
pub(super) async fn partitioned_insert(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let resolution = write_ctx.eval_ctx.resolution();
    let Statement::Insert {
        table,
        alias,
        columns,
        indirections,
        source,
        on_conflict,
        returning,
        ..
    } = stmt
    else {
        unreachable!("the caller matched an INSERT")
    };
    let catalog_kv = write_ctx.catalog_kv;
    let ctx = write_ctx.eval_ctx;
    if on_conflict.is_some() {
        return Err(ExecError::Unsupported(
            "INSERT … ON CONFLICT into a partitioned table is not supported: conflict arbitration \
             would have to span every partition's indexes, which are enforced per partition here"
                .into(),
        ));
    }
    let parent = crabka_pgcatalog::get_table(
        catalog_kv,
        &resolve_relation(catalog_kv, resolution, table, SchemaDisposition::Reference)?,
    )?;
    let (target_idx, rows) =
        insert_source_rows(write_ctx, ctes, &parent, columns, indirections, source)?;
    let mut ops: Vec<crabka_pgkv::WriteOp> = Vec::new();
    if rows.is_empty() {
        return Ok((WriteOutcome::command("INSERT 0 0".into()), ops));
    }
    let mut returned_rows = returning
        .as_ref()
        .map(|_| Vec::with_capacity(rows.len()))
        .unwrap_or_default();
    // A partitioned target has no rows of its own, but `RETURNING` sees the
    // leaf that received each row. Build the parent-shaped visible scope once;
    // each returned row below stamps it with that leaf's system values.
    let refs = crate::scope::StatementRefs::of_write(stmt);
    let qualifier = table_qualifier(&parent, alias);
    let mut returning_scope = Scope::single(&parent, qualifier);
    crate::scope::SystemColumns::of(Some(&refs), &parent)
        .stamp(parent.id)?
        .extend_scope(&mut returning_scope, qualifier);
    let mut inserted: u64 = 0;
    // Resolved once per leaf rather than once per row: a relation in no foreign
    // key must pay one boolean test per write, not a catalog read.
    let mut leaf_fk: HashMap<TableId, crate::fk::StatementFkContext> = HashMap::new();
    // PostgreSQL judges a routed row by the policies of the relation the
    // statement named, never the leaf's own — the same rule the read gate
    // applies to a partition tree.
    let supplied = WriteContext::modified_columns(&parent, &target_idx);
    let check = write_ctx.row_check(
        &parent,
        crabka_pgcatalog::policy::PolicyCommand::Insert,
        &supplied,
    )?;
    for row_exprs in &rows {
        let full =
            build_insert_row_with_subscripts(&parent, &target_idx, indirections, row_exprs, ctx)?;
        let (leaf, leaf_row) = route_row_to_leaf(write_ctx, &parent, &full)?;
        let Some(leaf_row) = crate::trigger::fire_before_row(
            catalog_kv,
            crate::trigger::WriteTarget {
                table: &leaf,
                check: &check,
            },
            crate::trigger::DmlEvent::Insert,
            &[],
            None,
            Some(leaf_row),
            ctx,
        )?
        else {
            continue;
        };
        check_partition_constraint(write_ctx, &leaf, &leaf_row, &supplied)?;
        let local_indexes = writable_local_indexes(catalog_kv, &leaf)?;
        let fk_ctx = match leaf_fk.entry(leaf.id) {
            std::collections::hash_map::Entry::Occupied(slot) => slot.into_mut(),
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(crate::fk::StatementFkContext::resolve(catalog_kv, &leaf)?)
            }
        };
        // One rowid per row rather than one block per statement: rows of the
        // same statement can land in different leaves, and each leaf has its
        // own rowid space.
        let (rowid, seq_op) = write_ctx.seq.alloc(write_ctx.kv, leaf.id, 1)?;
        ops.extend(seq_op);
        enforce_unique_local_indexes(write_ctx, &leaf, &local_indexes, rowid, &leaf_row, writes)
            .await?;
        if !fk_ctx.is_empty() {
            writes.fk_checks.after_insert(fk_ctx, rowid, &leaf_row)?;
        }
        if returning.is_some() {
            let ordinals = column_mapping(&parent, &leaf)?;
            let returned = ordinals
                .iter()
                .map(|ordinal| leaf_row[*ordinal].clone())
                .collect();
            let mut source = Vec::new();
            crate::scope::SystemColumns::of(Some(&refs), &leaf)
                .stamp(leaf.id)?
                .extend_row(
                    &mut source,
                    rowid,
                    write_ctx.xid,
                    0,
                    write_ctx.command_id,
                    0,
                );
            returned_rows.push(ReturnedRow {
                new: Some(returned),
                old: None,
                source,
                old_xmin: 0,
                old_xmax: 0,
                old_cmin: 0,
                old_cmax: 0,
                new_xmin: write_ctx.xid,
                new_xmax: 0,
                new_cmin: write_ctx.command_id,
                new_cmax: 0,
                action: None,
                old_identity: NO_ROW_IDENTITY,
                new_identity: rowid,
            });
        }
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(leaf.id, rowid, write_ctx.xid),
            value: encode_table_tuple(
                &leaf,
                write_ctx.xid,
                crabka_pgmvcc::xid::INVALID_XID,
                write_ctx.command_id,
                0,
                &leaf_row,
            ),
        });
        ops.extend(local_index_entry_ops(
            &leaf,
            &local_indexes,
            rowid,
            &leaf_row,
        )?);
        crate::trigger::fire_after_row(
            catalog_kv,
            &leaf,
            crate::trigger::DmlEvent::Insert,
            &[],
            None,
            Some(&leaf_row),
            ctx,
        )?;
        inserted += 1;
    }
    let spec = ReturningSpec::new(
        &parent,
        qualifier,
        returning.as_ref(),
        Some(&returning_scope),
        false,
    )?;
    Ok((
        spec.outcome(format!("INSERT 0 {inserted}"), returned_rows, ctx)?,
        ops,
    ))
}
