use super::*;

/// The op recording a temporary namespace, when it is not recorded already.
///
/// The engine creates a temporary namespace on behalf of a session that first
/// puts something in it, never a statement that names it. `CREATE SCHEMA`
/// refuses every `pg_`-prefixed name, as `PostgreSQL` does.
pub(crate) fn ensure_schema_ops(
    kv: &dyn Kv,
    schema: &str,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    if crabka_pgcatalog::schema_exists(kv, schema)? {
        return Ok(Vec::new());
    }
    Ok(vec![crabka_pgcatalog::create_temp_schema_op(schema)])
}

/// The batch that removes every relation and user type `schema` holds, whatever
/// kind each is, together with everything outside the schema that depends on
/// one of them:
/// dropping a table here is [the same drop](drop_table_and_dependents_ops) a
/// `DROP TABLE … CASCADE` performs, so a foreign key or view in another schema
/// goes with its referent rather than outliving it, and a partition stored
/// elsewhere goes with its parent.
///
/// `DROP SCHEMA … CASCADE` is one caller; the others are the three points a
/// temporary namespace is emptied: `DISCARD TEMP`, the end of a session, and
/// the purge a session runs over its own namespace before it first uses it, in
/// case a crashed backend of the same id left rows behind.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub(crate) fn drop_schema_contents_ops(
    kv: &dyn Kv,
    schema: &str,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let contents = crabka_pgcatalog::schema_contents(kv, schema)?;
    // The partitions of a table in `schema` go with their parent even when they
    // live outside it, so they are part of the batch and have to be known before
    // any of it is emitted: a foreign key whose child is in that set neither
    // blocks the drop nor needs an op of its own.
    let mut partitions = HashSet::new();
    for relation in &contents {
        if crabka_pgcatalog::get_table(kv, relation).is_ok() {
            partitions.extend(crate::partition::descendants(kv, relation)?);
        }
    }
    let dropping: HashSet<_> = contents.iter().chain(partitions.iter()).cloned().collect();
    let mut ops = Vec::new();
    let mut handled: HashSet<crabka_pgcatalog::RelationName> = HashSet::new();
    // Parents first, striking off each partition they carry. Whatever still
    // stands afterwards is emitted on its own account — a partition whose parent
    // is in another schema, or a cycle in the partition metadata that leaves the
    // batch rootless — so no relation in the schema is left behind.
    for parents_first in [true, false] {
        for relation in &contents {
            if handled.contains(relation) || (parents_first && partitions.contains(relation)) {
                continue;
            }
            handled.insert(relation.clone());
            if crabka_pgcatalog::get_view(kv, relation).is_ok() {
                ops.extend(drop_view_with_triggers_ops(kv, relation)?);
            } else if let Ok(table) = crabka_pgcatalog::get_table(kv, relation) {
                handled.extend(crate::partition::descendants(kv, relation)?);
                ops.extend(drop_table_and_dependents_ops(kv, &table, &dropping, true)?);
            } else {
                ops.extend(crabka_pgcatalog::drop_sequence_ops(kv, relation)?);
            }
        }
    }
    ops.extend(crate::inheritance::drop_metadata_ops(kv, &dropping)?);
    ops.extend(crate::usertype::drop_schema_types_ops(kv, schema)?);
    Ok(ops)
}

/// The batch that drops one table with everything that depends on it: the stored
/// views over it, the foreign keys that reference it, and the partitions that
/// hang off it, wherever those live, because a dependency in another schema is
/// still a dependency.
///
/// `dropping` names the relations the same statement already removes. A
/// dependency inside that set neither blocks the drop nor needs an op of its own,
/// since it goes away with its own relation; that is what lets `DROP TABLE p, c`,
/// a mutually referencing pair, and `DROP SCHEMA … CASCADE` succeed.
///
/// Without `cascade` a dependency outside that set is a 2BP01 refusal. With it,
/// `PostgreSQL` splits the two kinds: a referencing *constraint* is dropped and
/// its child table survives, while a dependent view is dropped outright. A
/// partition is neither. It has no independent existence, so it goes with its
/// parent whether or not `CASCADE` was written.
///
/// Inheritance links are *not* settled here. A child's parent list has to be
/// rewritten against the whole statement's removal set rather than against one
/// name out of it, so [`crate::inheritance::drop_metadata_ops`] is called once,
/// by the caller, over every relation the statement takes away: its targets and
/// the partitions hanging off them. That function says what a per-relation
/// rewrite corrupted.
///
/// # Errors
///
/// Returns undefined-relation, dependent-object, and storage/corruption errors
/// from the catalog KV seam.
pub(crate) fn drop_table_and_dependents_ops(
    kv: &dyn Kv,
    table: &Table,
    dropping: &HashSet<crabka_pgcatalog::RelationName>,
    cascade: bool,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let name = &table.name;
    let table_ops = crabka_pgcatalog::drop_table_ops(kv, name)?;
    let mut ops = Vec::new();
    let dependents: Vec<_> = dependent_view_chain(kv, name, None)?
        .into_iter()
        .filter(|(view, _)| !dropping.contains(view))
        .collect();
    if !dependents.is_empty() {
        if !cascade {
            return Err(dependent_objects_error(
                kv,
                &format!(
                    "cannot drop {} {name} because other objects depend on it",
                    stored_relation_kind(table)
                ),
                &dependents,
            ));
        }
        for (view, _) in &dependents {
            ops.extend(drop_dependent_relation_ops(kv, view)?);
        }
    }
    ops.extend(drop_blocking_foreign_keys(kv, table, dropping, cascade)?);
    for descendant in crate::partition::descendants(kv, name)? {
        if let Ok(descendant_table) = crabka_pgcatalog::get_table(kv, &descendant) {
            ops.extend(crabka_pgcatalog::trigger::drop_triggers_for_table_ops(
                kv,
                descendant_table.id,
            )?);
        }
        ops.extend(crabka_pgcatalog::drop_table_ops(kv, &descendant)?);
        ops.extend(crate::partition::drop_metadata_ops(kv, &descendant)?);
        // Only the departing relation's own statistics go. A parent losing its
        // last child keeps its `relhassubclass` latch, which is the stale
        // window `ANALYZE` is what closes.
        ops.extend(crate::relstats::drop_metadata_ops(&descendant));
    }
    ops.extend(crate::partition::drop_metadata_ops(kv, name)?);
    ops.extend(crate::relstats::drop_metadata_ops(name));
    ops.extend(crabka_pgcatalog::trigger::drop_triggers_for_table_ops(
        kv, table.id,
    )?);
    ops.extend(table_ops);
    Ok(ops)
}

/// A materialized view seen as the [`crabka_pgcatalog::View`] the dependency
/// machinery understands, or `None` for any other stored relation.
///
/// The synthesized record is not stored and is never written back — it exists so
/// one walker can answer "what does this relation's query read" for both kinds.
pub(crate) fn materialized_as_view(
    table: crabka_pgcatalog::Table,
) -> Option<crabka_pgcatalog::View> {
    let matview = table.materialized?;
    Some(crabka_pgcatalog::View {
        name: table.name,
        definition: matview.definition,
        owner: table.owner,
        columns: table.columns,
        options: crabka_pgcatalog::ViewOptions::default(),
    })
}

/// Drop one relation that a `CASCADE` reached because its query reads the
/// relation being dropped.
///
/// A dependent is a view or a materialized view, and the two go away by
/// different batteries: a view is a catalog record, a materialized view is a
/// stored relation with a heap and indexes. Dispatching here rather than at each
/// call site is what keeps `DROP TABLE … CASCADE` and `DROP VIEW … CASCADE` from
/// each having to know.
pub(crate) fn drop_dependent_relation_ops(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    if let Ok(table) = crabka_pgcatalog::get_table(kv, name)
        && table.materialized.is_some()
    {
        let mut ops = crabka_pgcatalog::trigger::drop_triggers_for_table_ops(kv, table.id)?;
        ops.extend(crabka_pgcatalog::drop_table_ops(kv, name)?);
        return Ok(ops);
    }
    drop_view_with_triggers_ops(kv, name)
}

pub(crate) fn drop_view_with_triggers_ops(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let view_id = crate::catalog_rel::view_oids(kv)?
        .get(name)
        .copied()
        .and_then(|oid| u32::try_from(oid).ok());
    let mut ops = crabka_pgcatalog::drop_view_ops(kv, name)?;
    if let Some(view_id) = view_id {
        ops.extend(crabka_pgcatalog::trigger::drop_triggers_for_table_ops(
            kv, view_id,
        )?);
        ops.extend(crabka_pgcatalog::rule::drop_rules_for_table_ops(
            kv, view_id,
        )?);
    }
    Ok(ops)
}

/// How many table ids a DDL statement will allocate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableIdDemand {
    /// The statement creates no relation of its own.
    None,
    /// The statement creates exactly this many relations.
    Fixed(usize),
    /// The count is not knowable before the statement runs.
    Unbounded,
}

/// True when `stmt` writes the `TEMPORARY` (or `TEMP`) keyword.
pub(crate) fn ddl_requests_temporary(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateTable {
            temporary: true,
            ..
        } | Statement::CreateView {
            temporary: true,
            ..
        }
    )
}

/// The schema qualifier a relation-creating statement wrote, if it wrote one.
pub(crate) fn ddl_created_qualifier(stmt: &Statement) -> Option<&str> {
    let reference = match stmt {
        Statement::CreateTable { name, .. }
        | Statement::CreateForeignTable { name, .. }
        | Statement::CreateView { name, .. } => name,
        Statement::ImportForeignSchema { into_schema, .. } => return Some(into_schema),
        _ => return None,
    };
    reference.schema.as_deref()
}

/// The table ids `stmt` will allocate, so the session can claim them before it
/// takes the catalog lock.
pub(crate) fn ddl_table_id_demand(stmt: &Statement) -> TableIdDemand {
    match stmt {
        Statement::CreateTable { .. } | Statement::CreateForeignTable { .. } => {
            TableIdDemand::Fixed(1)
        }
        // One per `CREATE TABLE` written inside the element list, because each
        // of them creates a relation of its own.
        Statement::CreateSchema { elements, .. } => TableIdDemand::Fixed(
            elements
                .iter()
                .filter(|element| matches!(element, Statement::CreateTable { .. }))
                .count(),
        ),
        // One foreign table per table the scanner discovers, which is only known
        // once the remote schema has been read.
        Statement::ImportForeignSchema { .. } => TableIdDemand::Unbounded,
        _ => TableIdDemand::None,
    }
}

pub(crate) fn ddl_unique_local_relation(
    stmt: &Statement,
) -> Option<&crabka_pgparser::ast::RelationRef> {
    match stmt {
        Statement::CreateIndex {
            unique: true,
            placement: crabka_pgparser::ast::IndexPlacement::Local,
            table,
            ..
        }
        // ADD PRIMARY KEY / ADD UNIQUE back-validates and backfills a local
        // unique index, so it must wait out in-flight writers exactly like
        // CREATE UNIQUE INDEX does.
        => Some(table),
        Statement::AlterTable { table, actions, .. }
            if actions.iter().any(|action| {
                matches!(
                action,
                crabka_pgparser::ast::AlterTableAction::AddConstraint(
                    crabka_pgparser::ast::TableConstraint {
                        kind: crabka_pgparser::ast::TableConstraintKind::PrimaryKey { .. }
                            | crabka_pgparser::ast::TableConstraintKind::Unique { .. },
                        ..
                    }
                )
                )
            }) => Some(table),
        // A relation is not visible to DML until CREATE TABLE's catalog batch
        // lands, so its inline unique-index backfill has no concurrent writer.
        _ => None,
    }
}

/// The `NOTICE` a `CASCADE` drop owes for the objects it takes with it.
///
/// PostgreSQL reports one dropped dependent inline and several as a count plus
/// a `DETAIL` line each:
///
/// ```text
/// NOTICE:  drop cascades to view d_mid
///
/// NOTICE:  drop cascades to 3 other objects
/// DETAIL:  drop cascades to view m1
/// drop cascades to view m3
/// drop cascades to view m2
/// ```
///
/// Returns `(message, detail)`, empty when the statement drops nothing beyond
/// its target -- which includes every non-`CASCADE` drop, since one that would
/// cascade is refused before it reaches here.
pub(crate) fn cascade_drop_notice(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    stmt: &Statement,
) -> Result<Option<(String, Option<String>)>, ExecError> {
    let reference = match stmt {
        Statement::DropTable {
            names,
            cascade: true,
            ..
        } => match names.as_slice() {
            // `DROP TABLE a, b` cascades over the whole list; reporting it
            // needs the union in PostgreSQL's order, which the single-name
            // form does not tell us. Left alone rather than reported wrongly.
            [only] => only,
            _ => return Ok(None),
        },
        Statement::DropForeignTable {
            names,
            cascade: true,
            ..
        } => match names.as_slice() {
            [only] => only,
            _ => return Ok(None),
        },
        Statement::DropView {
            name,
            cascade: true,
            ..
        } => name,
        Statement::DropMaterializedView {
            names,
            cascade: true,
            ..
        } => match names.as_slice() {
            [only] => only,
            _ => return Ok(None),
        },
        // `DROP SCHEMA … CASCADE` reports the schema's own contents as well as
        // whatever outside it goes with them, so it collects its objects rather
        // than walking out from one target.
        Statement::DropSchema {
            names,
            cascade: true,
            ..
        } => {
            let mut lines = Vec::new();
            for schema in names {
                lines.extend(schema_cascade_lines(kv, resolution, schema)?);
            }
            return Ok(cascade_notice(lines));
        }
        // `DROP TYPE … CASCADE` names the routines whose signatures used the
        // type and the casts that had it at either end, both in creation order,
        // exactly as `performDeletion` walks `pg_depend`.
        Statement::DropType {
            names,
            cascade: true,
            ..
        } => {
            let mut lines = Vec::new();
            for reference in names {
                lines.extend(crate::usertype::type_cascade_lines(
                    kv, resolution, reference,
                )?);
            }
            return Ok(cascade_notice(lines));
        }
        // `ALTER TABLE … DROP COLUMN … CASCADE` takes the row-security policies
        // that read the column, which is the one dependent this engine drops
        // rather than refuses.
        Statement::AlterTable { table, actions, .. } => {
            let dropped: Vec<&String> = actions
                .iter()
                .filter_map(|action| match action {
                    crabka_pgparser::ast::AlterTableAction::DropColumn {
                        column,
                        cascade: true,
                        ..
                    } => Some(column),
                    _ => None,
                })
                .collect();
            if dropped.is_empty() {
                return Ok(None);
            }
            let Ok(name) = resolve_relation(kv, resolution, table, SchemaDisposition::Reference)
            else {
                return Ok(None);
            };
            let Ok(relation) = crabka_pgcatalog::get_table(kv, &name) else {
                return Ok(None);
            };
            let mut lines = Vec::new();
            for column in dropped {
                lines.extend(
                    policies_reading_column(kv, &relation, column)?
                        .iter()
                        .map(|policy| policy_dependency_line(&name, &policy.name)),
                );
            }
            return Ok(cascade_notice(
                lines
                    .into_iter()
                    .map(|line| format!("drop cascades to {line}"))
                    .collect(),
            ));
        }
        _ => return Ok(None),
    };
    let column = None;
    let Ok(name) = resolve_relation(kv, resolution, reference, SchemaDisposition::Reference) else {
        return Ok(None);
    };
    let mut lines: Vec<_> = crate::inheritance::descendants(kv, &name)?
        .iter()
        .map(|child| cascade_line(kv, resolution, child))
        .collect();
    lines.extend(
        dependent_view_chain(kv, &name, column)?
            .iter()
            .map(|(view, _)| cascade_line(kv, resolution, view)),
    );
    Ok(cascade_notice(lines))
}

/// The `NOTICE` an `IF NOT EXISTS` create owes when the object is already there.
///
/// `PostgreSQL` reports the skip in place of the `42P07` the bare spelling
/// raises, and it calls a relation of any kind — table, index, sequence,
/// materialized view — a `relation`. Computed before the statement runs,
/// because afterwards the object exists either way and the skip is invisible.
///
/// `CREATE SCHEMA IF NOT EXISTS` with an element list is refused outright, so
/// it never skips and owes nothing.
/// `resolution` is a thunk because every statement passes through here and
/// building a scope parses the search path; only the arms below ever need one.
pub(crate) fn skipped_create_notice(
    kv: &dyn Kv,
    resolution: impl FnOnce() -> crate::relname::ResolutionScope,
    current_user: &str,
    session_user: &str,
    stmt: &Statement,
) -> Result<Option<crabka_pgwire::error::PgError>, ExecError> {
    /// The relation the skip names, spelled as `PostgreSQL` spells it: bare,
    /// because the statement named it bare too.
    fn skipped(name: &crabka_pgcatalog::RelationName) -> crabka_pgwire::error::PgError {
        crabka_pgwire::error::PgError::notice(format!(
            "relation \"{}\" already exists, skipping",
            name.name
        ))
    }

    // Each arm asks exactly what its statement asks before it skips, so the
    // notice cannot claim a skip the statement did not take.
    let reference = match stmt {
        Statement::CreateSchema {
            name: Some(name),
            if_not_exists: true,
            elements,
            ..
        } if elements.is_empty() => {
            return Ok(crabka_pgcatalog::schema_exists(kv, name)?.then(|| {
                crabka_pgwire::error::PgError::notice(format!(
                    "schema \"{name}\" already exists, skipping"
                ))
            }));
        }
        Statement::CreateServer {
            name,
            if_not_exists: true,
            ..
        } => {
            return Ok(crabka_pgcatalog::get_server(kv, name).is_ok().then(|| {
                crabka_pgwire::error::PgError::notice(format!(
                    "server \"{name}\" already exists, skipping"
                ))
            }));
        }
        Statement::CreateUserMapping {
            user,
            server,
            if_not_exists: true,
            ..
        } => {
            use crabka_pgparser::ast::RoleSpec;
            let user = match user {
                RoleSpec::Name(name) => name.as_str(),
                RoleSpec::CurrentUser | RoleSpec::CurrentRole => current_user,
                RoleSpec::SessionUser => session_user,
                RoleSpec::Public => crabka_pgcatalog::PUBLIC_ROLE,
            };
            if !crabka_pgcatalog::role_is_nameable(kv, user)?
                || crabka_pgcatalog::get_server(kv, server).is_err()
            {
                return Ok(None);
            }
            return Ok(crabka_pgcatalog::get_user_mapping(kv, user, server)
                .is_ok()
                .then(|| {
                    crabka_pgwire::error::PgError::notice(format!(
                        "user mapping for \"{user}\" already exists for server \"{server}\", skipping"
                    ))
                }));
        }
        // `CREATE SEQUENCE` borrows the `CREATE INDEX` variant, tagging `table`
        // with the sentinel the executor splits the two arms on.
        Statement::CreateIndex {
            name: Some(name),
            table,
            if_not_exists: true,
            ..
        } if table.schema.is_none() && table.name == "__crabka_sequence__" => {
            let resolution = &resolution();
            let Ok(name) = resolve_relation(kv, resolution, name, SchemaDisposition::Creation)
            else {
                return Ok(None);
            };
            return Ok(crabka_pgcatalog::get_sequence(kv, &name)
                .is_ok()
                .then(|| skipped(&name)));
        }
        // An index name is never qualified: an index lands in its table's
        // schema, whatever the session's creation slot is.
        Statement::CreateIndex {
            name: Some(name),
            table,
            if_not_exists: true,
            ..
        } => {
            let resolution = &resolution();
            let (Ok(table), Ok(index)) = (
                resolve_relation(kv, resolution, table, SchemaDisposition::Utility),
                resolve_relation(kv, resolution, name, SchemaDisposition::Utility),
            ) else {
                return Ok(None);
            };
            let index = table.sibling(&index.name);
            return Ok(crabka_pgcatalog::get_index(kv, &index)
                .is_ok()
                .then(|| skipped(&index)));
        }
        Statement::CreateTable {
            name,
            if_not_exists: true,
            ..
        }
        | Statement::CreateTableAs {
            name,
            if_not_exists: true,
            ..
        }
        | Statement::CreateMaterializedView {
            name,
            if_not_exists: true,
            ..
        } => name,
        _ => return Ok(None),
    };
    let Ok(name) = resolve_relation(kv, &resolution(), reference, SchemaDisposition::Creation)
    else {
        return Ok(None);
    };
    Ok(crabka_pgcatalog::get_table(kv, &name)
        .is_ok()
        .then(|| skipped(&name)))
}

/// The `NOTICE` an `IF EXISTS` foreign-object drop owes when it skips a missing
/// object. This runs before DDL so the absence is still observable.
pub(crate) fn skipped_drop_notice(
    kv: &dyn Kv,
    resolution: impl FnOnce() -> crate::relname::ResolutionScope,
    current_user: &str,
    session_user: &str,
    stmt: &Statement,
) -> Result<Option<crabka_pgwire::error::PgError>, ExecError> {
    let missing = |kind: &str, name: &str| {
        crabka_pgwire::error::PgError::notice(format!("{kind} \"{name}\" does not exist, skipping"))
    };
    match stmt {
        Statement::DropFdw {
            name,
            if_exists: true,
            ..
        } => Ok(crabka_pgcatalog::get_fdw(kv, name)
            .is_err()
            .then(|| missing("foreign-data wrapper", name))),
        Statement::DropServer {
            name,
            if_exists: true,
            ..
        } => Ok(crabka_pgcatalog::get_server(kv, name)
            .is_err()
            .then(|| missing("server", name))),
        Statement::DropForeignTable {
            names,
            if_exists: true,
            ..
        } => {
            let resolution = resolution();
            for reference in names {
                let Ok(name) =
                    resolve_relation(kv, &resolution, reference, SchemaDisposition::Utility)
                else {
                    continue;
                };
                if relation_kind(kv, &name).is_none() {
                    return Ok(Some(missing("foreign table", &name.name)));
                }
            }
            Ok(None)
        }
        Statement::DropUserMapping {
            user,
            server,
            if_exists: true,
            ..
        } => {
            use crabka_pgparser::ast::RoleSpec;
            let user = match user {
                RoleSpec::Name(name) => name.as_str(),
                RoleSpec::CurrentUser | RoleSpec::CurrentRole => current_user,
                RoleSpec::SessionUser => session_user,
                RoleSpec::Public => crabka_pgcatalog::PUBLIC_ROLE,
            };
            if !crabka_pgcatalog::role_is_nameable(kv, user)? {
                return Ok(Some(missing("role", user)));
            }
            if crabka_pgcatalog::get_server(kv, server).is_err() {
                return Ok(Some(missing("server", server)));
            }
            Ok(crabka_pgcatalog::get_user_mapping(kv, user, server)
                .is_err()
                .then(|| {
                    crabka_pgwire::error::PgError::notice(format!(
                        "user mapping for \"{user}\" does not exist for server \"{server}\", skipping"
                    ))
                }))
        }
        _ => Ok(None),
    }
}

/// The most dependent objects a `CASCADE` names to the client before the rest
/// become a count. `PostgreSQL`'s `MAX_REPORTED_DEPS`, which exists because
/// "client software may not deal well with enormous error strings".
pub(crate) const MAX_REPORTED_DEPS: usize = 100;

/// Fold the per-object lines into the `(message, detail)` a `CASCADE` reports.
///
/// Three shapes, all `PostgreSQL`'s: nothing dropped says nothing; a single
/// object is named inline with no `DETAIL` at all; and several become a count
/// with one `DETAIL` line each. Past [`MAX_REPORTED_DEPS`] the surplus is
/// summarized on a final line and the count still names the *total*, so
/// dropping 102 objects reads `drop cascades to 102 other objects` above 100
/// lines and `and 2 other objects (see server log for list)`.
pub(crate) fn cascade_notice(mut lines: Vec<String>) -> Option<(String, Option<String>)> {
    let total = lines.len();
    match total {
        0 => None,
        1 => Some((lines.remove(0), None)),
        _ => {
            let withheld = total.saturating_sub(MAX_REPORTED_DEPS);
            lines.truncate(MAX_REPORTED_DEPS);
            let mut detail = lines.join("\n");
            if withheld > 0 {
                use std::fmt::Write as _;
                let plural = if withheld == 1 { "object" } else { "objects" };
                write!(
                    detail,
                    "\nand {withheld} other {plural} (see server log for list)"
                )
                .expect("writing to a String cannot fail");
            }
            Some((
                format!("drop cascades to {total} other objects"),
                Some(detail),
            ))
        }
    }
}

/// One `drop cascades to <kind> <name>` line for a relation.
pub(crate) fn cascade_line(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    name: &crabka_pgcatalog::RelationName,
) -> String {
    format!(
        "drop cascades to {} {}",
        relation_kind(kv, name).unwrap_or("view"),
        message_relation_name(kv, resolution, name),
    )
}

/// A relation's name as a message writes it: bare when the search path makes it
/// visible, schema-qualified when it does not, and each part quoted where a
/// bare identifier would not read back as itself — `PostgreSQL`'s
/// `quote_qualified_identifier` over `RelationIsVisible`.
pub(crate) fn message_relation_name(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    name: &crabka_pgcatalog::RelationName,
) -> String {
    let bare = crabka_pgparser::ast::RelationRef::bare(name.name.clone());
    let visible = resolve_relation(kv, resolution, &bare, SchemaDisposition::Reference)
        .is_ok_and(|resolved| resolved == *name);
    let printed = crate::catalog_fn::quote_identifier(&name.name);
    if visible {
        printed
    } else {
        format!(
            "{}.{printed}",
            crate::catalog_fn::quote_identifier(&name.schema)
        )
    }
}

/// Every object `DROP SCHEMA … CASCADE` takes with it, as the lines it reports.
///
/// `PostgreSQL` walks its dependency graph out from each of the schema's own
/// objects in the order `pg_depend` recorded them — creation order — reporting
/// everything that depends on one directly after it. This reproduces that shape
/// from the catalog's durable cross-kind creation order. A partition is an
/// internal dependent of its parent, so it is removed but not reported as an
/// independent schema member.
pub(crate) fn schema_cascade_lines(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    schema: &str,
) -> Result<Vec<String>, ExecError> {
    if !crabka_pgcatalog::schema_exists(kv, schema)? {
        return Ok(Vec::new());
    }
    let contents = crabka_pgcatalog::schema_contents(kv, schema)?;
    let mut roots = Vec::new();
    // A `SERIAL` or identity column's sequence belongs to the column rather
    // than to the schema: upstream records it as an *internal* dependency, and
    // `reportDependentObjects` never names one of those. So the sequence goes
    // silently, while a sequence someone wrote `CREATE SEQUENCE` for is
    // reported like any other relation.
    let mut owned = HashSet::new();
    for name in &contents {
        if let Ok(table) = crabka_pgcatalog::get_table(kv, name) {
            owned.extend(table.columns.iter().filter_map(
                |column| match column.default.as_ref()? {
                    ColumnDefault::NextVal(sequence) => Some(sequence.clone()),
                    ColumnDefault::Value(_) | ColumnDefault::Expression(_) => None,
                },
            ));
        }
    }
    let contents = contents.into_iter().collect::<HashSet<_>>();
    for name in &contents {
        if owned.contains(&name.to_string()) || crate::partition::parent_of(kv, name)?.is_some() {
            continue;
        }
        roots.push((
            crabka_pgcatalog::creation_order(kv, name)?.unwrap_or(u64::MAX),
            name.clone(),
            cascade_line(kv, resolution, name),
        ));
    }
    let mut seen: HashSet<crabka_pgcatalog::RelationName> = HashSet::new();
    // A composite, enum or domain type goes with its schema too, and every one
    // of the three is `type` in this message — `DROP SCHEMA` over a domain
    // reports `drop cascades to type s.d`, not `domain`.
    let types = crabka_pgcatalog::list_user_types(kv)?;
    let visible = resolution.visible_schemas(kv)?;
    for user_type in types.iter().filter(|ty| ty.schema == schema) {
        // Type visibility is its own question — the search path is the same,
        // but what shadows a type is another *type* of that name, not a
        // relation. A type is written bare when its schema is the first visible
        // one holding a type by that name.
        let shadowing = visible.iter().find(|candidate| {
            types
                .iter()
                .any(|other| other.name == user_type.name && other.schema == **candidate)
        });
        let printed = crate::catalog_fn::quote_identifier(&user_type.name);
        let name = if shadowing.is_some_and(|first| first == schema) {
            printed
        } else {
            format!(
                "{}.{printed}",
                crate::catalog_fn::quote_identifier(&user_type.schema)
            )
        };
        let type_name = crabka_pgcatalog::RelationName::new(&user_type.schema, &user_type.name);
        roots.push((
            crabka_pgcatalog::creation_order(kv, &type_name)?.unwrap_or(u64::MAX),
            type_name,
            format!("drop cascades to type {name}"),
        ));
    }
    roots.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let mut lines = Vec::new();
    for (_, root, line) in roots {
        if seen.insert(root.clone()) {
            lines.push(line);
        }
        for (dependent, _) in dependent_view_chain(kv, &root, None)? {
            if !contents.contains(&dependent) && seen.insert(dependent.clone()) {
                lines.push(cascade_line(kv, resolution, &dependent));
            }
        }
    }
    Ok(lines)
}
