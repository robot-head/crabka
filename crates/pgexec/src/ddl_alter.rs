//! DDL and catalog code carved out of `exec`.

use super::*;

pub(crate) fn execute_ddl(
    kv: &dyn Kv,
    stmt: &Statement,
    fctx: ForeignCtx,
    check_function_bodies: bool,
) -> Result<(QueryResult, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let resolution = fctx.resolution;
    match stmt {
        Statement::Utility(UtilityStatement::TextSearch(ddl)) => {
            let (tag, ops) = crate::text_search_catalog::execute(kv, ddl)?;
            Ok((command(tag), ops))
        }
        Statement::CreateStatistics(stats) => crate::statistics_ddl::create(kv, stats, fctx),
        Statement::AlterStatistics { name, action } => crate::statistics_ddl::alter(
            kv,
            resolve_relation(kv, resolution, name, SchemaDisposition::Utility)?,
            action,
            fctx,
        ),
        Statement::DropStatistics { names, if_exists } => {
            crate::statistics_ddl::drop(kv, names, *if_exists, fctx)
        }
        Statement::CreateRule(rule) => crate::rewrite_rules::create(
            kv,
            rule,
            resolve_relation(kv, resolution, &rule.table, SchemaDisposition::Reference)?,
        ),
        Statement::AlterRule {
            name,
            table,
            action,
        } => crate::rewrite_rules::alter(
            kv,
            name,
            resolve_relation(kv, resolution, table, SchemaDisposition::Reference)?,
            action,
        ),
        Statement::DropRule {
            name,
            table,
            if_exists,
            ..
        } => crate::rewrite_rules::drop(
            kv,
            name,
            resolve_relation(kv, resolution, table, SchemaDisposition::Reference)?,
            *if_exists,
        ),
        Statement::CreateTrigger(trigger) => {
            let table =
                resolve_relation(kv, resolution, &trigger.table, SchemaDisposition::Reference)?;
            let referenced = trigger
                .referenced_table
                .as_ref()
                .map(|name| resolve_relation(kv, resolution, name, SchemaDisposition::Reference))
                .transpose()?;
            crate::trigger::create(kv, trigger, table, referenced)
        }
        Statement::AlterTrigger {
            name,
            table,
            action,
        } => crate::trigger::alter(
            kv,
            name,
            resolve_relation(kv, resolution, table, SchemaDisposition::Reference)?,
            action,
        ),
        Statement::DropTrigger {
            name,
            table,
            if_exists,
            ..
        } => crate::trigger::drop(
            kv,
            name,
            resolve_relation(kv, resolution, table, SchemaDisposition::Reference)?,
            *if_exists,
        ),
        Statement::CreatePolicy(policy) => crate::policy_ddl::create(
            kv,
            policy,
            resolve_relation(kv, resolution, &policy.table, SchemaDisposition::Reference)?,
            fctx,
        ),
        Statement::AlterPolicy {
            name,
            table,
            action,
        } => crate::policy_ddl::alter(
            kv,
            name,
            resolve_relation(kv, resolution, table, SchemaDisposition::Reference)?,
            action,
            fctx,
        ),
        Statement::DropPolicy {
            name,
            table,
            if_exists,
            ..
        } => crate::policy_ddl::drop(
            kv,
            name,
            resolve_relation(kv, resolution, table, SchemaDisposition::Reference)?,
            *if_exists,
            fctx,
        ),
        Statement::CreateEventTrigger(trigger) => {
            crate::trigger::create_event(kv, trigger, fctx.current_user)
        }
        Statement::AlterEventTrigger { name, action } => {
            crate::trigger::alter_event(kv, name, action)
        }
        Statement::DropEventTrigger {
            name, if_exists, ..
        } => crate::trigger::drop_event(kv, name, *if_exists),
        // P2: SQL routines. Definition, lifecycle and catalog storage live in
        // `routine`; only the DDL routing is here.
        Statement::CreateRoutine(routine) => crate::routine::create(
            kv,
            fctx.resolution,
            routine,
            fctx.effective_role(),
            check_function_bodies,
        ),
        Statement::DropRoutine {
            object,
            if_exists,
            routines,
            cascade,
        } => crate::routine::drop_routines(kv, *object, *if_exists, routines, *cascade),
        Statement::AlterRoutine {
            object,
            routine,
            action,
        } => crate::routine::alter(kv, *object, routine, action, fctx.effective_role()),
        // P6: user-defined aggregates. Stored as routines, so only the
        // definition-time rules and the aggregate evaluator are new.
        Statement::CreateAggregate(aggregate) => {
            crate::useragg::create(kv, aggregate, fctx.current_user)
        }
        Statement::DropAggregate {
            if_exists,
            aggregates,
            cascade,
        } => crate::useragg::drop_aggregates(kv, *if_exists, aggregates, *cascade),
        Statement::AlterAggregate { aggregate, action } => {
            crate::useragg::alter(kv, aggregate, action)
        }
        // T5: user-defined types. Definition, lifecycle and catalog storage
        // live in `usertype`; only the DDL routing is here.
        Statement::CreateType { name, definition } => crate::usertype::create_type(
            kv,
            resolution,
            &resolve_relation(kv, resolution, name, SchemaDisposition::Creation)?,
            definition,
        ),
        Statement::AlterType { name, action } => {
            crate::usertype::alter_type(kv, &resolve_user_type(kv, resolution, name)?, action)
        }
        Statement::DropType {
            names,
            if_exists,
            cascade,
        } => crate::usertype::drop_types(
            kv,
            &resolve_user_types(kv, resolution, names)?,
            *if_exists,
            *cascade,
            false,
        ),
        // T5: a user-declared cast. `usercast` owns the physical-compatibility
        // rules and the conversion; only the routing is here.
        Statement::CreateCast {
            source,
            target,
            method,
            context,
        } => crate::usercast::create_cast(kv, *source, *target, method, *context),
        Statement::DropCast {
            source,
            target,
            if_exists,
            ..
        } => crate::usercast::drop_cast(kv, *source, *target, *if_exists),
        Statement::CreateAccessMethod {
            name,
            kind,
            handler,
        } => create_access_method(kv, name, *kind, handler),
        Statement::CreateDomain {
            name,
            base,
            collation,
            constraints,
        } => {
            // The collation itself has no effect — every collation the engine
            // has orders text by byte value — but writing one over a base type
            // that cannot carry one is PostgreSQL's 42804, reported before the
            // domain is created.
            if collation.is_some() {
                crate::eval::require_collatable(*base)?;
            }
            crate::usertype::create_domain(
                kv,
                &resolve_relation(kv, resolution, name, SchemaDisposition::Creation)?,
                *base,
                constraints,
            )
        }
        Statement::AlterDomain { name, action } => {
            crate::usertype::alter_domain(kv, &resolve_user_type(kv, resolution, name)?, action)
        }
        Statement::DropDomain {
            names,
            if_exists,
            cascade,
        } => crate::usertype::drop_types(
            kv,
            &resolve_user_types(kv, resolution, names)?,
            *if_exists,
            *cascade,
            true,
        ),
        Statement::CreateTable {
            name,
            columns,
            constraints,
            sharded,
            sharding,
            if_not_exists,
            temporary,
            of_type,
            typed_options,
            like,
            inherits,
            on_commit,
            partition_by,
            partition_of,
            tablespace,
            access_method,
        } => {
            if (partition_by.is_some() || partition_of.is_some()) && *sharded {
                return Err(crate::partition::reject_sharded_partitioned());
            }
            let disposition = if *temporary {
                SchemaDisposition::TemporaryCreation
            } else {
                SchemaDisposition::Creation
            };
            let name = &resolve_relation(kv, resolution, name, disposition)?;
            // `CREATE TABLE pg_temp.t` and a `search_path` whose creation slot
            // is the temporary namespace both make a temporary relation without
            // the keyword, so persistence follows the schema the name landed in
            // rather than the keyword that was written.
            let temporary = crabka_pgcatalog::is_temp_schema(&name.schema);
            if on_commit.is_some() && !temporary {
                return Err(ExecError::InvalidTableDefinition(
                    "ON COMMIT can only be used on temporary tables".into(),
                ));
            }
            if *if_not_exists && crabka_pgcatalog::get_table(kv, name).is_ok() {
                return Ok((command("CREATE TABLE"), Vec::new()));
            }
            let access_method = match access_method.as_deref() {
                Some(method) => Some(resolve_table_access_method_oid(kv, method)?),
                None if partition_by.is_some() => None,
                None => Some(resolve_table_access_method_oid(
                    kv,
                    fctx.default_table_access_method,
                )?),
            };
            crate::usertype::ensure_relation_type_name_available(kv, name)?;
            let ddl_ctx = crate::clock::EvalCtx::for_ddl(resolution, fctx.catalog);
            let typed_type = of_type
                .as_ref()
                .map(|reference| {
                    let type_name = resolve_user_type(kv, resolution, reference)?;
                    let ty = match crabka_pgcatalog::get_user_type(kv, &type_name)? {
                        Some(ty) => ty,
                        None if crabka_pgcatalog::get_table(kv, &type_name).is_ok() => {
                            return Err(ExecError::WrongObjectType(format!(
                                "type {type_name} is the row type of another table\nDETAIL:  A typed table must use a stand-alone composite type created with CREATE TYPE."
                            )));
                        }
                        None => {
                            return Err(ExecError::UndefinedObject(format!(
                                "type \"{type_name}\" does not exist"
                            )));
                        }
                    };
                    ty.fields()
                        .ok_or_else(|| {
                            ExecError::WrongObjectType(format!(
                                "type {type_name} is not a composite type"
                            ))
                        })
                        .map(|fields| {
                            (
                                ty.oid,
                                fields
                                    .iter()
                                    .map(|field| crabka_pgparser::ast::ColumnDef {
                                        name: field.name.clone(),
                                        ty: field.ty,
                                        serial: None,
                                        collation: None,
                                        constraints: Vec::new(),
                                    })
                                    .collect::<Vec<_>>(),
                            )
                        })
                })
                .transpose()?;
            let (typed_type_oid, mut columns) = typed_type.map_or_else(
                || (None, columns.clone()),
                |(oid, columns)| (Some(oid), columns),
            );
            let mut seen_typed_options = std::collections::HashSet::new();
            for option in typed_options {
                if !seen_typed_options.insert(&option.column) {
                    return Err(ExecError::DuplicateOutputColumn(option.column.clone()));
                }
                let Some(column) = columns
                    .iter_mut()
                    .find(|column| column.name == option.column)
                else {
                    return Err(ExecError::UndefinedColumn(option.column.clone()));
                };
                if typed_type_oid.is_some() {
                    for constraint in &option.constraints {
                        match constraint.kind {
                            crabka_pgparser::ast::ColumnConstraintKind::Identity(_) => {
                                return Err(ExecError::Unsupported(
                                    "identity columns are not supported on typed tables".into(),
                                ));
                            }
                            crabka_pgparser::ast::ColumnConstraintKind::Generated(_) => {
                                return Err(ExecError::Unsupported(
                                    "generated columns are not supported on typed tables".into(),
                                ));
                            }
                            _ => {}
                        }
                    }
                }
                column.collation = option.collation.clone();
                column.constraints.extend(option.constraints.clone());
            }
            let inheritance_parents = inherits
                .iter()
                .map(|parent| {
                    resolve_relation(kv, resolution, parent, SchemaDisposition::Reference)
                })
                .collect::<Result<Vec<_>, _>>()?;
            // A partition declares no columns of its own: it inherits the
            // parent's list, along with the parent's CHECK constraints, and may
            // only add qualifiers to what it inherits.
            let (mut cols, checks, serial_sequences, pending_indexes, pending_foreign_keys) =
                match partition_of {
                    Some(spec) => {
                        partition_definition(kv, name, spec, constraints, like, &ddl_ctx)?
                    }
                    None if inheritance_parents.is_empty() => create_table_definition(
                        kv,
                        name,
                        &columns,
                        constraints,
                        like,
                        &[],
                        &ddl_ctx,
                    )?,
                    None => inherited_table_definition(
                        kv,
                        name,
                        &inheritance_parents,
                        &columns,
                        constraints,
                        like,
                        &ddl_ctx,
                    )?,
                };
            // A table-level `NOT NULL c` may name a column the statement did not
            // declare itself, so it is applied here, where `LIKE`, `INHERITS`
            // and `PARTITION OF` have all contributed their columns already.
            apply_table_not_null_constraints(&mut cols, constraints, name)?;
            // `fk::resolve_foreign_key` refuses a sharded relation itself, but
            // `Table` carries no partition flag, so this is the only place that
            // knows a partitioned relation is being defined.
            if (partition_by.is_some() || partition_of.is_some())
                && let Some(pending) = pending_foreign_keys.first()
            {
                return Err(reject_partitioned_foreign_key(&pending.name));
            }
            // The whole list, after `LIKE` and `INHERITS` have contributed: no
            // spelling of the statement may put a system column name on a
            // relation with storage. `CREATE TABLE AS` and `SELECT INTO` build
            // one of these and run it, so they are covered here too.
            crate::scope::reject_system_column_names(cols.iter().map(|c| c.name.as_str()))?;
            let partition_scheme = partition_by
                .as_ref()
                .map(|spec| partition_scheme_from_ast(spec, &cols, &pending_indexes))
                .transpose()?;
            let attachment = partition_of
                .as_ref()
                .map(|spec| partition_attachment(kv, name, spec, &cols, &ddl_ctx))
                .transpose()?;
            if *sharded && !pending_indexes.is_empty() {
                return Err(ExecError::Unsupported(
                    "PRIMARY KEY and UNIQUE constraints on sharded tables are not supported until global enforcement exists".into(),
                ));
            }
            let sharding = sharding.as_ref().map(hash_sharding_from_ast).transpose()?;
            ensure_hash_shard_key_types_are_supported(&cols, sharding.as_ref())?;
            let (id, mut ops) = crabka_pgcatalog::create_table_with_sharding_ops(
                kv,
                name,
                cols.clone(),
                crabka_pgcatalog::TableOptions {
                    sharded: *sharded,
                    row_security: false,
                    force_row_security: false,
                },
                sharding.as_ref(),
                checks.clone(),
                fctx.table_creation(),
            )?;
            if let Some(oid) = typed_type_oid {
                ops.push(crabka_pgcatalog::set_typed_table_type_op(name, oid));
            }
            for privilege in crabka_pgcatalog::default_table_privileges_of(
                kv,
                fctx.effective_role(),
                &name.schema,
            )? {
                if privilege.grant {
                    ops.extend(crabka_pgcatalog::grant_table_privileges_ops(
                        kv,
                        name,
                        &[privilege.grantee],
                        &[privilege.privilege],
                    )?);
                } else if privilege.grantee == fctx.effective_role() {
                    ops.push(crabka_pgcatalog::revoke_owner_table_privilege_op(
                        name,
                        &privilege.privilege,
                    ));
                }
            }
            if let Some(tablespace) = tablespace {
                let oid = resolve_relation_tablespace_oid(kv, tablespace)?;
                ops.push(crabka_pgcatalog::set_relation_tablespace_op(name, oid));
            }
            if let Some(oid) = access_method {
                ops.push(crabka_pgcatalog::set_relation_access_method_op(name, oid));
            }
            let table = crabka_pgcatalog::Table {
                id,
                name: name.clone(),
                owner: fctx.effective_role().to_string(),
                columns: cols,
                sharded: *sharded,
                row_security: false,
                force_row_security: false,
                sharding,
                foreign: None,
                materialized: None,
                checks,
            };
            for index in &pending_indexes {
                if !matches!(
                    index.method,
                    crabka_pgcatalog::IndexMethod::Btree | crabka_pgcatalog::IndexMethod::Hash
                ) {
                    continue;
                }
                for column in &index.columns {
                    let column = table
                        .columns
                        .iter()
                        .find(|candidate| candidate.name == *column)
                        .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))?;
                    validate_default_index_opclass(column.ty, index.method)?;
                }
            }
            let index_ops =
                crabka_pgcatalog::create_indexes_on_table_ops(kv, &table, &pending_indexes)?;
            let staged_indexes = staged_indexes_of(&index_ops, &pending_indexes);
            ops.extend(index_ops);
            if !pending_foreign_keys.is_empty() {
                // Resolved here rather than in `create_table_definition`
                // because `CREATE TABLE t (… REFERENCES t …)` names a relation
                // no catalog read can find: the in-flight column and index
                // lists, under the ids this batch allocates, are the parent.
                let relation = crate::fk::FkRelation {
                    id,
                    name,
                    columns: &table.columns,
                    indexes: &staged_indexes,
                    sharded: *sharded,
                };
                // One cursor for the whole statement: every clause reads the
                // same stored counter, so the ids have to ascend in memory or
                // two constraints on one column would tie.
                let mut foreign_key_ids = crabka_pgcatalog::ForeignKeyIds::default();
                for pending in &pending_foreign_keys {
                    let foreign_key = crate::fk::resolve_foreign_key(
                        kv,
                        resolution,
                        &relation,
                        &crate::fk::ForeignKeyRequest {
                            id: foreign_key_ids.allocate(kv)?,
                            name: Some(&pending.name),
                            columns: &pending.columns,
                            reference: &pending.reference,
                            attributes: pending.attributes,
                            // PostgreSQL ignores NOT VALID here: a relation
                            // being created has no stored rows to validate.
                            validated: true,
                            self_reference: Some(&relation),
                        },
                    )?;
                    // After the key resolves, which is where PostgreSQL tests
                    // it: a `REFERENCES` naming a relation that does not exist,
                    // or a key no unique index backs, is reported first.
                    reject_foreign_key_over_generated(
                        &table.columns,
                        &pending.columns,
                        &pending.reference,
                    )?;
                    ops.extend(crabka_pgcatalog::create_foreign_key_ops(kv, &foreign_key)?);
                }
            }
            for (sequence_name, sequence) in serial_sequences {
                ops.extend(crabka_pgcatalog::create_sequence_ops(
                    kv,
                    &sequence_name,
                    sequence,
                )?);
            }
            if let Some(scheme) = &partition_scheme {
                ops.extend(crate::partition::put_scheme_ops(name, scheme));
            }
            if let Some((parent, bound)) = &attachment {
                ops.extend(crate::partition::attach_ops(parent, name, bound));
                let parent = crabka_pgcatalog::get_table(kv, parent)?;
                ops.extend(crate::trigger::clone_new_partition_triggers(
                    kv, &parent, &table,
                )?);
            }
            if !inheritance_parents.is_empty() {
                ops.extend(crate::inheritance::attach_ops(name, &inheritance_parents));
            }
            if temporary {
                ops.splice(..0, ensure_schema_ops(kv, &name.schema)?);
            }
            Ok((
                QueryResult::Command {
                    tag: "CREATE TABLE".into(),
                },
                ops,
            ))
        }
        Statement::DropTable {
            names,
            if_exists,
            cascade,
        } => {
            // All-or-nothing across the name list, matching PostgreSQL: ops are
            // only applied after every name resolves (or is skipped by
            // IF EXISTS), so a missing name without IF EXISTS drops nothing.
            let mut ops = Vec::new();
            let mut tag = "DROP TABLE";
            // A foreign key whose CHILD is itself being dropped never blocks:
            // `DROP TABLE p, c` and a mutually referencing pair dropped together
            // both succeed. The whole set is resolved before any name is
            // processed, because the list is all-or-nothing.
            // `DROP SEQUENCE` reaches this arm with its names tagged. The tag
            // rides the relation's own name, so it comes off before the
            // qualifier is resolved and goes back on afterwards: `DROP SEQUENCE
            // public.s` has to reach the same sequence as `DROP SEQUENCE s`.
            let mut targets = Vec::with_capacity(names.len());
            for reference in names {
                let tagged = reference.name.strip_prefix("__crabka_sequence__:");
                let bare = crabka_pgparser::ast::RelationRef {
                    schema: reference.schema.clone(),
                    name: tagged.unwrap_or(&reference.name).to_string(),
                };
                match resolve_relation(kv, resolution, &bare, SchemaDisposition::Utility) {
                    Ok(resolved) => targets.push((resolved, tagged.is_some())),
                    // `DROP TABLE IF EXISTS nope.t` skips that one name and
                    // still drops the rest of the list, as PostgreSQL does.
                    Err(error) if *if_exists && is_missing_schema(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            let dropping: std::collections::HashSet<_> =
                targets.iter().map(|(name, _)| name.clone()).collect();
            // What the statement takes away, which is more than what it names:
            // a partition goes with its parent. The inheritance links are
            // settled against this whole set once the loop below has run, so
            // that a child of two departing parents is rewritten from the set
            // rather than once per parent.
            let mut removed = dropping.clone();
            for (name, is_sequence) in &targets {
                if !is_sequence {
                    removed.extend(crate::partition::descendants(kv, name)?);
                }
            }
            for (name, is_sequence) in &targets {
                if *is_sequence {
                    tag = "DROP SEQUENCE";
                    if let Some(error) = drop_kind_mismatch(kv, name, "sequence") {
                        return Err(error);
                    }
                    match crabka_pgcatalog::drop_sequence_ops(kv, name) {
                        Ok(sequence_ops) => ops.extend(sequence_ops),
                        Err(crabka_pgcatalog::CatalogError::UndefinedSequence(_)) if *if_exists => {
                        }
                        Err(error) => return Err(error.into()),
                    }
                } else {
                    // A relation of another kind is 42809 whether or not
                    // IF EXISTS was written: the relation exists, so there is
                    // nothing for IF EXISTS to waive.
                    if let Some(error) = drop_kind_mismatch(kv, name, "table") {
                        return Err(error);
                    }
                    // The kind matched, so a synthesised catalog relation that
                    // reaches here is a system catalog, and `PostgreSQL`'s
                    // `DropErrorMsgWrongType` has already had its say: what is
                    // left is the privilege refusal, which `IF EXISTS` does not
                    // waive either.
                    if let Some(error) = system_catalog_wrong_kind(name) {
                        return Err(error);
                    }
                    match crabka_pgcatalog::get_table(kv, name) {
                        Ok(table) => ops.extend(drop_table_and_dependents_ops(
                            kv, &table, &dropping, *cascade,
                        )?),
                        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) if *if_exists => {}
                        Err(crabka_pgcatalog::CatalogError::UndefinedTable(missing)) => {
                            return Err(ExecError::UndefinedRelationOfKind {
                                kind: "table",
                                name: missing,
                            });
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            ops.extend(crate::inheritance::drop_metadata_ops(kv, &removed)?);
            Ok((command(tag), ops))
        }
        Statement::CreateSchema {
            name,
            authorization,
            if_not_exists,
            elements,
        } => {
            // `CREATE SCHEMA AUTHORIZATION role` names the schema after the
            // role, so the keyword spellings have to be resolved before the
            // name is taken: `AUTHORIZATION CURRENT_ROLE` names the schema
            // after the session's role, not after the word `current_role`.
            let owner = match authorization {
                Some(spec) => resolve_new_owner(kv, fctx, spec)?,
                None => fctx.current_user.to_string(),
            };
            let name = match name {
                Some(name) => name.clone(),
                None => owner.clone(),
            };
            if !elements.is_empty() {
                // An element resolves against a schema that does not exist yet,
                // and DDL builds one batch rather than committing as it goes.
                // So the elements read the catalog with the batch so far folded
                // over it, and each one's own ops join that view before the
                // next element reads it. The session commits the lot together,
                // which is what makes the whole `CREATE SCHEMA` atomic.
                let mut ops = crabka_pgcatalog::create_schema_ops(kv, &name, &owner)?;
                let staged = StagedKv::new(kv, &ops);
                // `CreateSchemaCommand` prepends the new schema to
                // `search_path` for exactly as long as the elements run, so an
                // unqualified name inside one finds what the list creates. The
                // elements are also created as the schema's owner, not as the
                // session's role, which is what `AUTHORIZATION` is for.
                let scope = resolution.for_stored_body(&name);
                let inner = ForeignCtx {
                    current_user: &owner,
                    resolution: &scope,
                    ..fctx
                };
                for element in &crate::schema_element::plan(&name, elements)? {
                    let (_, element_ops) =
                        execute_ddl(&staged, element, inner, check_function_bodies)?;
                    staged.stage(&element_ops);
                    ops.extend(element_ops);
                }
                return Ok((command("CREATE SCHEMA"), ops));
            }
            // `IF NOT EXISTS` waives only the duplicate: an unacceptable name is
            // still unacceptable, so the reserved-prefix refusal has to come out
            // of `create_schema_ops` rather than be short-circuited before it.
            let ops = match crabka_pgcatalog::create_schema_ops(kv, &name, &owner) {
                Err(crabka_pgcatalog::CatalogError::DuplicateSchema(_)) if *if_not_exists => {
                    Vec::new()
                }
                result => result?,
            };
            Ok((command("CREATE SCHEMA"), ops))
        }
        Statement::AlterSchema { name, action } => {
            use crabka_pgparser::ast::AlterSchemaAction;
            let ops = match action {
                AlterSchemaAction::OwnerTo(owner) => {
                    // A schema's new owner is checked the way a relation's is.
                    // Before this shared a resolver with `ALTER TABLE`, a
                    // schema could be handed to a name no role held, leaving it
                    // owned by something no ownership test can match.
                    let owner = resolve_new_owner(kv, fctx, owner)?;
                    crabka_pgcatalog::set_schema_owner_ops(kv, name, &owner)?
                }
                AlterSchemaAction::RenameTo(new_name) => rename_schema_ops(kv, name, new_name)?,
            };
            Ok((command("ALTER SCHEMA"), ops))
        }
        Statement::DropSchema {
            names,
            if_exists,
            cascade,
        } => {
            let mut schemas = Vec::new();
            for name in names {
                if *if_exists && !crabka_pgcatalog::schema_exists(kv, name)? {
                    continue;
                }
                schemas.push((name, crabka_pgcatalog::drop_schema_ops(kv, name, *cascade)?));
            }
            let mut ops = Vec::new();
            for (name, schema_ops) in schemas {
                if *cascade {
                    ops.extend(drop_schema_contents_ops(kv, name)?);
                }
                ops.extend(schema_ops);
            }
            Ok((command("DROP SCHEMA"), ops))
        }
        Statement::AlterTable {
            table,
            if_exists,
            only,
            actions,
        } => match resolve_relation(kv, resolution, table, SchemaDisposition::Utility) {
            Ok(name) => alter_table_ops(kv, &name, *if_exists, *only, actions, fctx),
            // `ALTER TABLE IF EXISTS nope.t` skips rather than reporting the
            // schema, as PostgreSQL does.
            Err(error) if *if_exists && is_missing_schema(&error) => {
                Ok((command("ALTER TABLE"), Vec::new()))
            }
            Err(error) => Err(error),
        },
        Statement::Comment {
            object_kind,
            object_name,
            rule_table,
            aggregate,
            cast,
            comment,
        } => comment_ops(
            kv,
            resolution,
            object_kind,
            object_name,
            rule_table.as_ref(),
            aggregate.as_ref(),
            cast.as_ref(),
            comment.as_deref(),
            fctx.effective_role(),
        ),
        Statement::CreateView {
            name,
            recursive,
            definition,
            query,
            or_replace,
            temporary,
            columns: aliases,
            options,
        } => {
            let (definition, query) = if *recursive {
                recursive_view_definition(name, definition, aliases)?
            } else {
                (definition.clone(), query.clone())
            };
            // The body is analysed before the view's own name is placed,
            // because what it reads decides where the view can go: a view over
            // a temporary relation is itself temporary whether or not `TEMP`
            // was written, so a qualifier naming an ordinary schema is refused.
            // `postgres:18.4` reports the two in that order.
            let sources = validate_view_definition(kv, resolution, &query)?;
            let temporary = *temporary
                || sources
                    .iter()
                    .any(|source| crabka_pgcatalog::is_temp_schema(&source.schema));
            let disposition = if temporary {
                SchemaDisposition::TemporaryCreation
            } else {
                SchemaDisposition::Creation
            };
            let name = &resolve_relation(kv, resolution, name, disposition)?;
            crate::usertype::ensure_relation_type_name_available(kv, name)?;
            let described = crate::query::describe_query_expr(kv, resolution, &query)?;
            // `VIEW name (a, b, c)` renames the output columns positionally; too
            // many names is PostgreSQL's own 42P10.
            if let Some(aliases) = aliases
                && aliases.len() > described.len()
            {
                return Err(ExecError::InvalidColumnReference(
                    "CREATE VIEW specifies more column names than columns".into(),
                ));
            }
            let columns = described
                .into_iter()
                .enumerate()
                .map(|(index, field)| {
                    let name = aliases
                        .as_ref()
                        .and_then(|aliases| aliases.get(index).cloned())
                        .unwrap_or(field.name);
                    Ok(Column::new(name, column_type_from_oid(field.type_oid)?))
                })
                .collect::<Result<Vec<_>, ExecError>>()?;
            // Two output columns of the same name would define a relation whose
            // columns cannot be told apart, so PostgreSQL refuses the view (42701)
            // before creating anything — the same rule `CREATE TABLE AS` applies.
            let mut seen = std::collections::HashSet::new();
            for column in &columns {
                if !seen.insert(column.name.as_str()) {
                    return Err(ExecError::DuplicateOutputColumn(column.name.clone()));
                }
            }
            // `OR REPLACE` over an existing VIEW redefines it in place, provided
            // the new query keeps every existing output column. A non-view
            // relation of that name is still 42P07, as it is without OR REPLACE.
            let options = crabka_pgcatalog::ViewOptions {
                security_invoker: options.security_invoker,
                security_barrier: options.security_barrier,
                check_option: options.check_option.map(catalog_check_option),
            };
            // A check option is a promise about writes, so it may only be made
            // by a view writes can be rewritten through. PostgreSQL raises this
            // at CREATE time rather than leaving a view whose option can never
            // fire, and puts the disqualifying clause in the HINT.
            if options.check_option.is_some()
                && let Some(detail) = crate::viewwrite::query_refusal(&query)
            {
                return Err(ExecError::CheckOptionUnsupported(detail));
            }
            let ops = if *or_replace && crabka_pgcatalog::get_view(kv, name).is_ok() {
                let existing = crabka_pgcatalog::get_view(kv, name)?;
                check_view_columns_replaceable(&existing.columns, &columns, name)?;
                // `OR REPLACE` redefines a view rather than creating one, so it
                // keeps the owner it already had — PostgreSQL's `CREATE OR
                // REPLACE VIEW` does not transfer ownership, and letting it
                // would let anyone who may replace a view also take its grants.
                vec![crabka_pgcatalog::put_view_op(&crabka_pgcatalog::View {
                    name: name.clone(),
                    definition: definition.clone(),
                    owner: existing.owner,
                    columns,
                    options,
                })]
            } else {
                let mut created = ensure_schema_ops(kv, &name.schema)?;
                created.extend(crabka_pgcatalog::create_view_ops(
                    kv,
                    name,
                    definition.clone(),
                    columns,
                    options,
                    fctx.effective_role(),
                )?);
                created
            };
            Ok((command("CREATE VIEW"), ops))
        }
        Statement::DropView {
            name,
            if_exists,
            cascade,
        } => {
            let name = &match resolve_relation(kv, resolution, name, SchemaDisposition::Utility) {
                Ok(name) => name,
                Err(error) if *if_exists && is_missing_schema(&error) => {
                    return Ok((command("DROP VIEW"), Vec::new()));
                }
                Err(error) => return Err(error),
            };
            if let Some(error) = drop_kind_mismatch(kv, name, "view") {
                return Err(error);
            }
            let ops = match drop_view_with_triggers_ops(kv, name) {
                Ok(mut ops) => {
                    // A view may itself be read by other views. PostgreSQL
                    // refuses the drop unless CASCADE is written, and then drops
                    // the dependents too.
                    let dependents = dependent_view_chain(kv, name, None)?;
                    if !dependents.is_empty() {
                        if !*cascade {
                            return Err(dependent_objects_error(
                                kv,
                                &format!(
                                    "cannot drop view {name} because other objects depend on it"
                                ),
                                &dependents,
                            ));
                        }
                        for (view, _) in &dependents {
                            ops.extend(drop_dependent_relation_ops(kv, view)?);
                        }
                    }
                    ops
                }
                Err(ExecError::Catalog(crabka_pgcatalog::CatalogError::UndefinedTable(_)))
                    if *if_exists =>
                {
                    Vec::new()
                }
                Err(error) => return Err(error),
            };
            Ok((command("DROP VIEW"), ops))
        }
        // The relation half of `CREATE MATERIALIZED VIEW`: the heap, its column
        // list, and the query it will be refilled from, created unpopulated. The
        // session drives the data half — running the query and flipping the
        // flag — because that is a write, and DDL here commits one catalog batch
        // that cannot also carry rows.
        Statement::CreateMaterializedView {
            name,
            if_not_exists,
            columns: aliases,
            definition,
            query,
            tablespace,
            access_method,
            with_data: _,
        } => {
            let _ = tablespace;
            let name = &resolve_relation(kv, resolution, name, SchemaDisposition::Creation)?;
            if *if_not_exists && crabka_pgcatalog::get_table(kv, name).is_ok() {
                return Ok((command("CREATE MATERIALIZED VIEW"), Vec::new()));
            }
            let access_method = Some(resolve_table_access_method_oid(
                kv,
                access_method
                    .as_deref()
                    .unwrap_or(fctx.default_table_access_method),
            )?);
            crate::usertype::ensure_relation_type_name_available(kv, name)?;
            let described = crate::query::describe_query_expr(kv, resolution, query)?;
            if let Some(aliases) = aliases
                && aliases.len() > described.len()
            {
                return Err(ExecError::Syntax(
                    "too many column names were specified".into(),
                ));
            }
            let columns = described
                .into_iter()
                .enumerate()
                .map(|(index, field)| {
                    let name = aliases
                        .as_ref()
                        .and_then(|aliases| aliases.get(index).cloned())
                        .unwrap_or(field.name);
                    Ok(Column::new(name, column_type_from_oid(field.type_oid)?))
                })
                .collect::<Result<Vec<_>, ExecError>>()?;
            let mut seen = std::collections::HashSet::new();
            for column in &columns {
                if !seen.insert(column.name.as_str()) {
                    return Err(ExecError::DuplicateOutputColumn(column.name.clone()));
                }
            }
            // A materialized view has storage, so it is one of the relkinds
            // `CheckAttributeNamesTypes` covers — unlike the plain view a few
            // arms above, which is exempt and may name a column `ctid`.
            crate::scope::reject_system_column_names(columns.iter().map(|c| c.name.as_str()))?;
            // Created unpopulated whatever `WITH DATA` said: the flag is what
            // makes a scan legal, and it is only true once the rows are actually
            // there. A `WITH DATA` create that fails partway therefore leaves
            // nothing readable rather than an empty relation claiming to hold
            // the query's answer.
            // The text as written is stored, exactly as `CREATE VIEW` stores
            // its own: `REFRESH` re-runs it, so whatever schema qualification
            // the author supplied has to survive to be re-resolved.
            let matview = crabka_pgcatalog::MaterializedView {
                definition: definition.clone(),
                populated: false,
            };
            let mut ops = ensure_schema_ops(kv, &name.schema)?;
            let (_, created) = crabka_pgcatalog::create_table_with_options_ops(
                kv,
                name,
                columns,
                crabka_pgcatalog::TableOptions::default(),
                Vec::new(),
                crabka_pgcatalog::TableCreation {
                    owner: fctx.effective_role(),
                    id: fctx.table_id(),
                    materialized: Some(&matview),
                },
            )?;
            ops.extend(created);
            if let Some(oid) = access_method {
                ops.push(crabka_pgcatalog::set_relation_access_method_op(name, oid));
            }
            Ok((command("CREATE MATERIALIZED VIEW"), ops))
        }
        // The catalog half of `REFRESH MATERIALIZED VIEW`: the population flag,
        // nothing else. The session has already emptied and (for `WITH DATA`)
        // refilled the heap, so this is what makes the new contents readable —
        // or, for `WITH NO DATA`, what makes the emptied relation an error to
        // scan rather than a relation that answers zero rows.
        Statement::RefreshMaterializedView {
            name,
            concurrently: _,
            with_data,
        } => {
            let name = &resolve_relation(kv, resolution, name, SchemaDisposition::Utility)?;
            let table = require_materialized_view(kv, name)?;
            Ok((
                command("REFRESH MATERIALIZED VIEW"),
                vec![crabka_pgcatalog::set_materialized_populated_op(
                    &table, *with_data,
                )],
            ))
        }
        // A materialized view is a stored relation, so it drops through the
        // table batteries — the heap, its indexes and its privileges all have to
        // go — while the *name* it answers to is checked against `relkind` first,
        // exactly as `DROP TABLE` and `DROP VIEW` check theirs.
        Statement::DropMaterializedView {
            names,
            if_exists,
            cascade,
        } => {
            let mut targets = Vec::with_capacity(names.len());
            for reference in names {
                match resolve_relation(kv, resolution, reference, SchemaDisposition::Utility) {
                    Ok(resolved) => targets.push(resolved),
                    Err(error) if *if_exists && is_missing_schema(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            let dropping: std::collections::HashSet<_> = targets.iter().cloned().collect();
            let mut ops = Vec::new();
            for name in &targets {
                if let Some(error) = drop_kind_mismatch(kv, name, "materialized view") {
                    return Err(error);
                }
                match crabka_pgcatalog::get_table(kv, name) {
                    Ok(table) => ops.extend(drop_table_and_dependents_ops(
                        kv, &table, &dropping, *cascade,
                    )?),
                    Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) if *if_exists => {}
                    Err(crabka_pgcatalog::CatalogError::UndefinedTable(missing)) => {
                        return Err(ExecError::UndefinedRelationOfKind {
                            kind: "materialized view",
                            name: missing,
                        });
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            // No syntax makes a materialized view inherit, be inherited from, or
            // be partitioned, so the targets are the whole removal set and this
            // sweep finds nothing. It stays because dropping a stored relation
            // without it is the mistake this batch exists to prevent, and one
            // statement quietly exempt from it is how that mistake returns.
            ops.extend(crate::inheritance::drop_metadata_ops(kv, &dropping)?);
            Ok((command("DROP MATERIALIZED VIEW"), ops))
        }
        Statement::CreateIndex {
            name,
            table,
            keys,
            unique,
            placement,
            if_not_exists,
            concurrently,
            method,
            include,
            nulls_not_distinct,
            predicate,
            tablespace,
        } => {
            let _ = concurrently;
            // `CREATE SEQUENCE` borrows this variant, naming the sequence in
            // `name` and tagging `table` with a sentinel no relation can carry.
            if table.schema.is_none() && table.name == "__crabka_sequence__" {
                let encoded: Vec<String> = keys.iter().map(|key| key.text.clone()).collect();
                let sequence = sequence_from_encoded_options(&encoded)?;
                let name = name
                    .as_ref()
                    .ok_or_else(|| ExecError::Syntax("CREATE SEQUENCE requires a name".into()))?;
                let name = &resolve_relation(kv, resolution, name, SchemaDisposition::Creation)?;
                if *if_not_exists && crabka_pgcatalog::get_sequence(kv, name).is_ok() {
                    return Ok((command("CREATE SEQUENCE"), Vec::new()));
                }
                crate::usertype::ensure_relation_type_name_available(kv, name)?;
                let ops = crabka_pgcatalog::create_sequence_ops(kv, name, sequence)?;
                return Ok((command("CREATE SEQUENCE"), ops));
            }
            let table = &resolve_relation(kv, resolution, table, SchemaDisposition::Utility)?;
            // `DefineIndex` opens the relation, refuses the relkinds that carry
            // no index, and only then consults `allowSystemTableMods`, so the
            // kind is reported ahead of the privilege for a relation that is
            // both.
            if let Some(error) = create_index_wrong_kind(kv, table) {
                return Err(error);
            }
            if let Some(error) = system_catalog_wrong_kind(table) {
                return Err(error);
            }
            // An index name is never qualified: an index lands in its table's
            // schema, so only the sequence spelling above can carry one.
            let index = name
                .as_ref()
                .map(|name| resolve_relation(kv, resolution, name, SchemaDisposition::Utility))
                .transpose()?;
            let name = table.sibling(index_name_or_default(
                kv,
                index.as_ref().map(|name| name.name.as_str()),
                table,
                keys,
            ));
            let index_method = match method.as_deref() {
                None | Some("btree") => crabka_pgcatalog::IndexMethod::Btree,
                Some("hash") => crabka_pgcatalog::IndexMethod::Hash,
                Some("gist") => crabka_pgcatalog::IndexMethod::Gist,
                Some("gin") => crabka_pgcatalog::IndexMethod::Gin,
                Some("spgist") => crabka_pgcatalog::IndexMethod::Spgist,
                Some(method) => {
                    return Err(ExecError::Unsupported(format!(
                        "index access method \"{method}\" is not supported"
                    )));
                }
            };
            let columns = index_key_columns(keys, predicate.as_deref())?;
            let key_options = index_key_options(keys);
            if *if_not_exists && crabka_pgcatalog::get_index(kv, &name).is_ok() {
                return Ok((command("CREATE INDEX"), Vec::new()));
            }
            crate::usertype::ensure_index_name_available(kv, &name)?;
            let name = &name;
            let columns = &columns;
            let placement = match placement {
                crabka_pgparser::ast::IndexPlacement::Local => {
                    crabka_pgcatalog::IndexPlacement::Local
                }
                crabka_pgparser::ast::IndexPlacement::Global => {
                    crabka_pgcatalog::IndexPlacement::Global
                }
            };
            if *unique && placement == crabka_pgcatalog::IndexPlacement::Global {
                return Err(ExecError::Unsupported(
                    "unique global indexes are not supported until global enforcement exists"
                        .into(),
                ));
            }
            let table_meta = crabka_pgcatalog::get_table(kv, table)?;
            if *unique {
                reject_unique_index_with_foreign_partition(kv, &table_meta)?;
            }
            reject_index_over_virtual_generated(&table_meta, columns, None)?;
            validate_index_opclasses(kv, resolution, &table_meta, keys, index_method)?;
            validate_index_expressions(&table_meta, keys, *unique, placement, index_method)?;
            validate_index_predicate(&table_meta, predicate.as_deref())?;
            validate_index_method(&table_meta, columns, *unique, placement, index_method)?;
            if *nulls_not_distinct && !unique {
                return Err(ExecError::Unsupported(
                    "NULLS NOT DISTINCT is only supported for unique indexes".into(),
                ));
            }
            let (id, mut ops) = crabka_pgcatalog::create_index_with_method_and_predicate_ops(
                kv,
                &name.name,
                table,
                columns.clone(),
                *unique,
                placement,
                index_method,
                predicate.clone(),
                include.clone(),
                key_options.clone(),
                *nulls_not_distinct,
            )?;
            if let Some(tablespace) = tablespace {
                let oid = resolve_relation_tablespace_oid(kv, tablespace)?;
                ops.push(crabka_pgcatalog::set_relation_tablespace_op(name, oid));
            }
            if placement == crabka_pgcatalog::IndexPlacement::Local {
                reject_unwritable_local_index(&table_meta)?;
                let index = crabka_pgcatalog::Index {
                    id,
                    name: name.name.clone(),
                    table: table.clone(),
                    table_id: table_meta.id,
                    columns: columns.clone(),
                    key_options,
                    include: include.clone(),
                    predicate: predicate.clone(),
                    nulls_not_distinct: *nulls_not_distinct,
                    unique: *unique,
                    placement,
                    method: index_method,
                    constraint: None,
                    clustered: false,
                    without_overlaps: false,
                    deferral: crabka_pgcatalog::ConstraintDeferral::Immediate,
                };
                let ddl_ctx = crate::clock::EvalCtx::for_ddl(resolution, fctx.catalog);
                ops.extend(local_index_backfill_ops(
                    kv,
                    &table_meta,
                    &index,
                    fctx.own_xid,
                    &IndexBuild::new(&fctx, &ddl_ctx),
                )?);
            }
            Ok((command("CREATE INDEX"), ops))
        }
        Statement::DropIndex {
            name,
            if_exists,
            cascade,
        } => {
            let name = &match resolve_relation(kv, resolution, name, SchemaDisposition::Utility) {
                Ok(name) => name,
                Err(error) if *if_exists && is_missing_schema(&error) => {
                    return Ok((command("DROP INDEX"), Vec::new()));
                }
                Err(error) => return Err(error),
            };
            if let Some(error) = drop_kind_mismatch(kv, name, "index") {
                return Err(error);
            }
            let (index, mut ops) = match crabka_pgcatalog::drop_index_ops(kv, name) {
                Ok(result) => result,
                Err(crabka_pgcatalog::CatalogError::UndefinedIndex(_)) if *if_exists => {
                    return Ok((command("DROP INDEX"), Vec::new()));
                }
                Err(error) => return Err(error.into()),
            };
            if index.placement == crabka_pgcatalog::IndexPlacement::Global {
                return Err(ExecError::Unsupported(
                    "dropping global indexes is not supported until distributed index cleanup exists"
                        .into(),
                ));
            }
            // A foreign key that chose this index as the one proving its
            // referenced columns unique depends on it; CASCADE drops the
            // referencing constraint, not the referencing relation.
            let dependents = crate::fk::dependents_blocking_index_drop(kv, &index)?;
            if !dependents.is_empty() {
                if !*cascade {
                    return Err(ExecError::DependentForeignKeys(Box::new(
                        crate::error::ForeignKeyDependents {
                            dropped: crate::error::DroppedObject::Index(index.name.clone()),
                            dependents,
                        },
                    )));
                }
                for dependent in &dependents {
                    let child = crabka_pgcatalog::get_table(kv, &dependent.table)?;
                    let (_, drop_ops) = crabka_pgcatalog::drop_foreign_key_ops(
                        kv,
                        child.id,
                        &dependent.constraint,
                    )?;
                    ops.extend(drop_ops);
                }
            }
            for (key, _) in kv.scan_prefix(&crabka_pgkv::key::secondary_index_prefix(
                index.table_id,
                index.id,
            ))? {
                ops.push(crabka_pgkv::WriteOp::Delete { key });
            }
            Ok((command("DROP INDEX"), ops))
        }
        Statement::AlterIndex { name, action } => {
            use crabka_pgparser::ast::AlterIndexAction;

            let name = &resolve_relation(kv, resolution, name, SchemaDisposition::Utility)?;
            if let Some(error) = system_catalog_wrong_kind(name) {
                return Err(error);
            }
            // A relation of another kind is 42809 naming what was asked for,
            // not the index catalog's 42704: `ALTER INDEX t` on a table has to
            // say `"t" is not an index` rather than claim no index is there.
            // No `HINT` rides this one — that is `DROP`'s family alone.
            if let Some(error) = wrong_kind(kv, name, |kind| {
                (kind != "index").then(|| {
                    ExecError::WrongObjectType(format!("\"{}\" is not an index", name.name))
                })
            }) {
                return Err(error);
            }
            let index = crabka_pgcatalog::get_index(kv, name)?;
            match action {
                AlterIndexAction::SetTablespace(tablespace) => {
                    let oid = resolve_relation_tablespace_oid(kv, tablespace)?;
                    Ok((
                        command("ALTER INDEX"),
                        vec![crabka_pgcatalog::set_relation_tablespace_op(name, oid)],
                    ))
                }
                AlterIndexAction::SetStatistics { column, target: _ } => {
                    if !(1..=32_767).contains(column) {
                        return Err(ExecError::InvalidParameterValueMessage(
                            "column number must be in range from 1 to 32767".into(),
                        ));
                    }
                    let position =
                        usize::try_from(*column - 1).expect("validated positive attribute number");
                    let Some(key) = index.columns.get(position) else {
                        return Err(ExecError::UndefinedColumn(format!(
                            "number {column} of relation \"{}\"",
                            index.name
                        )));
                    };
                    if crabka_pgcatalog::index_key_expression(key).is_none() {
                        return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                            "42809",
                            format!(
                                "cannot alter statistics on non-expression column \"{key}\" of index \"{}\"",
                                index.name
                            ),
                        )
                        .with_hint("Alter statistics on table column instead.")));
                    }
                    // The planner does not yet consume per-index expression
                    // statistics, but accepting the valid statement keeps the
                    // DDL result and catalog shape aligned until P2 persists it.
                    Ok((command("ALTER INDEX"), Vec::new()))
                }
                // The written options were checked against the reloption
                // catalog at parse time. Crabka's index storage has no page
                // fill to tune and no pending list to hold, so an accepted
                // option changes nothing that a query can observe — which is
                // also PostgreSQL's outcome for an index that has none.
                AlterIndexAction::SetStorageParameters(_)
                | AlterIndexAction::ResetStorageParameters(_) => {
                    Ok((command("ALTER INDEX"), Vec::new()))
                }
            }
        }
        Statement::AlterView {
            name,
            if_exists,
            action,
        } => {
            use crabka_pgparser::ast::{AlterViewAction, ViewOptionName, ViewOptionSetting};

            let name = &match resolve_relation(kv, resolution, name, SchemaDisposition::Utility) {
                Ok(name) => name,
                Err(error) if *if_exists && is_missing_schema(&error) => {
                    return Ok((command("ALTER VIEW"), Vec::new()));
                }
                Err(error) => return Err(error),
            };
            let mut view = match crabka_pgcatalog::get_view(kv, name) {
                Ok(view) => view,
                Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) if *if_exists => {
                    return Ok((command("ALTER VIEW"), Vec::new()));
                }
                // A relation of that name which is not a view is PostgreSQL's
                // 42809, not a missing-relation report: `ALTER VIEW t` on a
                // table must say so rather than claim `t` does not exist. The
                // system-catalog refusal outranks it, as it does for every
                // other `ALTER` spelling.
                Err(error @ crabka_pgcatalog::CatalogError::UndefinedTable(_)) => {
                    return Err(system_catalog_wrong_kind(name).unwrap_or_else(|| {
                        // A synthesised *view* is the one kind that reaches
                        // here and is not a mismatch: it is a view with no
                        // record to rewrite, so it keeps the lookup's own
                        // error rather than being told it is not a view.
                        match relation_kind(kv, name) {
                            Some(kind) if kind != "view" => ExecError::WrongObjectType(format!(
                                "\"{}\" is not a view",
                                name.name
                            )),
                            _ => error.into(),
                        }
                    }));
                }
                Err(error) => return Err(error.into()),
            };
            // Owner-only, like every other statement that redefines what a
            // relation means. It matters more here than for a table: the
            // owner is the identity the view's body reads under, so a role
            // that could rewrite it could hand its own rights to anyone.
            crate::privilege::require_ownership(
                kv,
                &view.name,
                &view.owner,
                crate::privilege::RelationKind::View,
                fctx.effective_role(),
            )?;
            match action {
                AlterViewAction::OwnerTo(role) => {
                    view.owner = resolve_new_owner(kv, fctx, role)?;
                }
                // `SET` moves only the options written; `RESET` returns the
                // ones written to their default — false for the booleans and
                // unset for the check option, which is what makes a parent's
                // cascade stop reaching a view whose option has been reset.
                AlterViewAction::SetOptions(settings) => {
                    for setting in settings {
                        match setting {
                            ViewOptionSetting::SecurityInvoker(value) => {
                                view.options.security_invoker = *value;
                            }
                            ViewOptionSetting::SecurityBarrier(value) => {
                                view.options.security_barrier = *value;
                            }
                            ViewOptionSetting::CheckOption(level) => {
                                view.options.check_option = Some(catalog_check_option(*level));
                            }
                        }
                    }
                }
                AlterViewAction::ResetOptions(names) => {
                    for option in names {
                        match option {
                            ViewOptionName::SecurityInvoker => {
                                view.options.security_invoker = false;
                            }
                            ViewOptionName::SecurityBarrier => {
                                view.options.security_barrier = false;
                            }
                            ViewOptionName::CheckOption => view.options.check_option = None,
                        }
                    }
                }
            }
            Ok((
                command("ALTER VIEW"),
                vec![crabka_pgcatalog::put_view_op(&view)],
            ))
        }
        Statement::CreateRole {
            name,
            can_login,
            member_of,
            options,
        } => {
            crate::privilege::require_role_create(kv, fctx.effective_role(), *options)?;
            // `IN ROLE r` writes exactly the membership `GRANT r TO …` writes,
            // so it passes exactly the same gate. Every named role is checked
            // before the first membership is written.
            for role in member_of {
                crate::privilege::require_role_grant(
                    kv,
                    fctx.effective_role(),
                    role,
                    crate::privilege::RoleGrant::Grant,
                )?;
            }
            let mut attributes = crabka_pgcatalog::RoleAttributes::default();
            let login = apply_role_options(&mut attributes, *can_login, *options);
            let ops = crabka_pgcatalog::create_role_with_memberships_ops(
                kv, name, login, attributes, member_of,
            )?;
            Ok((command("CREATE ROLE"), ops))
        }
        Statement::AlterRole { name, options } => {
            // The name is resolved first: `PostgreSQL` reports an unknown role
            // as unknown even when the session could not have altered it.
            let role = crabka_pgcatalog::get_role(kv, name)?;
            crate::privilege::require_role_alter(kv, fctx.effective_role(), name, *options)?;
            let mut attributes = role.attributes;
            let login = apply_role_options(&mut attributes, role.can_login, *options);
            let ops = crabka_pgcatalog::alter_role_ops(kv, name, login, attributes)?;
            Ok((command("ALTER ROLE"), ops))
        }
        Statement::DropRole { names, if_exists } => {
            crate::privilege::require_role_drop(kv, fctx.effective_role())?;
            let mut ops = Vec::new();
            for name in names {
                match crabka_pgcatalog::drop_role_ops(kv, name) {
                    Ok(role_ops) => {
                        reject_foreign_role_dependencies(kv, name)?;
                        ops.extend(role_ops);
                    }
                    Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) if *if_exists => {}
                    Err(error) => return Err(error.into()),
                }
            }
            Ok((command("DROP ROLE"), ops))
        }
        Statement::GrantTablePrivileges {
            privileges,
            tables,
            grantees,
        } => {
            let names = resolve_grantable_relations(kv, resolution, tables)?;
            let grantees = resolve_grantees(kv, fctx, grantees)?;
            let mut ops = Vec::new();
            for name in &names {
                ops.extend(privilege_grant_ops(
                    kv,
                    name,
                    &grantees,
                    privileges,
                    PrivilegeGrant::Grant,
                )?);
            }
            Ok((command("GRANT"), ops))
        }
        // `WITH ADMIN OPTION` / `ADMIN OPTION FOR` are dropped: a membership is
        // stored as a bare key with no payload, so there is nowhere to record
        // the admin right and nothing that would read it back.
        Statement::GrantRoles {
            roles,
            members,
            admin_option: _,
        } => {
            let members = require_role_memberships(
                kv,
                fctx,
                roles,
                members,
                crate::privilege::RoleGrant::Grant,
            )?;
            let ops = crabka_pgcatalog::grant_role_memberships_ops(kv, roles, &members)?;
            Ok((command("GRANT ROLE"), ops))
        }
        Statement::RevokeRoles {
            roles,
            members,
            admin_option,
        } => {
            let members = require_role_memberships(
                kv,
                fctx,
                roles,
                members,
                crate::privilege::RoleGrant::Revoke,
            )?;
            // The names are checked either way; `ADMIN OPTION FOR` then keeps
            // the membership and strips only the admin right, which is all this
            // catalog does not hold, so it writes nothing.
            let ops = crabka_pgcatalog::revoke_role_memberships_ops(kv, roles, &members)?;
            let ops = if *admin_option { Vec::new() } else { ops };
            Ok((command("REVOKE ROLE"), ops))
        }
        Statement::GrantSchemaPrivileges {
            privileges,
            schemas,
            grantees,
        } => {
            let grantees = resolve_grantees(kv, fctx, grantees)?;
            let ops =
                crabka_pgcatalog::grant_schema_privileges_ops(kv, schemas, &grantees, privileges)?;
            Ok((command("GRANT"), ops))
        }
        Statement::GrantForeignPrivileges {
            target,
            privileges,
            names,
            grantees,
            grant_option,
        } => {
            let target = foreign_privilege_target(*target);
            require_foreign_grant_authority(kv, target, names, fctx)?;
            let grantees = resolve_grantees(kv, fctx, grantees)?;
            let privileges = foreign_privilege_names(privileges)?;
            let ops = crabka_pgcatalog::grant_foreign_privileges_with_option_as_ops(
                kv,
                target,
                names,
                &grantees,
                &privileges,
                fctx.effective_role(),
                *grant_option,
            )?;
            Ok((command("GRANT"), ops))
        }
        Statement::RevokeTablePrivileges {
            privileges,
            tables,
            grantees,
        } => {
            let names = resolve_grantable_relations(kv, resolution, tables)?;
            let grantees = resolve_grantees(kv, fctx, grantees)?;
            let mut ops = Vec::new();
            for name in &names {
                ops.extend(privilege_grant_ops(
                    kv,
                    name,
                    &grantees,
                    privileges,
                    PrivilegeGrant::Revoke,
                )?);
            }
            Ok((command("REVOKE"), ops))
        }
        Statement::RevokeSchemaPrivileges {
            privileges,
            schemas,
            grantees,
        } => {
            let grantees = resolve_grantees(kv, fctx, grantees)?;
            let ops =
                crabka_pgcatalog::revoke_schema_privileges_ops(kv, schemas, &grantees, privileges)?;
            Ok((command("REVOKE"), ops))
        }
        Statement::RevokeForeignPrivileges {
            target,
            privileges,
            names,
            grantees,
            grant_option_only,
            cascade,
        } => {
            let target = foreign_privilege_target(*target);
            require_foreign_ownership(kv, target, names, fctx)?;
            let grantees = resolve_grantees(kv, fctx, grantees)?;
            let privileges = foreign_privilege_names(privileges)?;
            let ops = match crabka_pgcatalog::revoke_foreign_privileges_with_option_as_ops(
                kv,
                target,
                names,
                &grantees,
                &privileges,
                fctx.effective_role(),
                *grant_option_only,
                *cascade,
            ) {
                Ok(ops) => ops,
                Err(crabka_pgcatalog::CatalogError::DependentPrivileges) => {
                    return Err(ExecError::Remote(
                        crabka_pgwire::error::PgError::error("2BP01", "dependent privileges exist")
                            .with_hint("Use CASCADE to revoke them too."),
                    ));
                }
                Err(error) => return Err(error.into()),
            };
            Ok((command("REVOKE"), ops))
        }
        Statement::AlterDefaultTablePrivileges {
            role,
            schemas,
            privileges,
            grantees,
            grant,
        } => {
            let owner = match role {
                Some(role) => resolve_new_owner(kv, fctx, role)?,
                None => fctx.effective_role().to_string(),
            };
            crate::privilege::require_role_alter(
                kv,
                fctx.effective_role(),
                &owner,
                crabka_pgparser::ast::RoleOptions::default(),
            )?;
            let grantees = resolve_grantees(kv, fctx, grantees)?;
            let privileges = privileges
                .iter()
                .map(|privilege| privilege.name.clone())
                .collect::<Vec<_>>();
            let ops = crabka_pgcatalog::alter_default_table_privileges_ops(
                kv,
                &owner,
                schemas,
                &grantees,
                &privileges,
                *grant,
            )?;
            Ok((command("ALTER DEFAULT PRIVILEGES"), ops))
        }
        Statement::CreateFdw {
            if_not_exists,
            name,
            handler,
            validator,
            options,
        } => {
            require_fdw_create_superuser(kv, name, fctx)?;
            if let Some(handler) = handler {
                validate_fdw_routine(kv, handler, &[], "fdw_handler")?;
            }
            if let Some(validator) = validator {
                validate_fdw_routine(kv, validator, &["text[]", "oid"], "void")?;
            }
            validate_postgresql_fdw_options(
                validator.as_deref(),
                options.iter().map(|(name, _)| name),
                PostgresqlFdwOptionContext::Wrapper,
            )?;
            let ops = ignore_foreign_duplicate(
                crabka_pgcatalog::create_fdw_with_routines_owned_ops(
                    kv,
                    name,
                    handler.as_deref(),
                    validator.as_deref(),
                    options.clone(),
                    fctx.effective_role(),
                ),
                crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper,
                name,
                *if_not_exists,
            )?
            .unwrap_or_default();
            Ok((command("CREATE FOREIGN DATA WRAPPER"), ops))
        }
        Statement::AlterFdw {
            name,
            rename_to,
            owner_to,
            handler,
            validator,
            options,
        } => {
            if let Some(owner) = owner_to {
                let owner = resolve_new_owner(kv, fctx, owner)?;
                require_fdw_owner_superuser(kv, name, &owner)?;
                require_fdw_alter_superuser(kv, name, fctx)?;
                require_foreign_ownership(
                    kv,
                    crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper,
                    std::slice::from_ref(name),
                    fctx,
                )?;
                return Ok((
                    command("ALTER FOREIGN DATA WRAPPER"),
                    crabka_pgcatalog::set_fdw_owner_ops(kv, name, &owner)?,
                ));
            }
            require_fdw_alter_superuser(kv, name, fctx)?;
            require_foreign_ownership(
                kv,
                crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper,
                std::slice::from_ref(name),
                fctx,
            )?;
            let ops = if let Some(rename_to) = rename_to {
                crabka_pgcatalog::rename_fdw_ops(kv, name, rename_to)?
            } else {
                if let Some(Some(handler)) = handler {
                    validate_fdw_routine(kv, handler, &[], "fdw_handler")?;
                }
                if let Some(Some(validator)) = validator {
                    validate_fdw_routine(kv, validator, &["text[]", "oid"], "void")?;
                }
                let current = crabka_pgcatalog::get_fdw(kv, name)?;
                let effective_validator = validator
                    .as_ref()
                    .map_or(current.validator.as_deref(), |validator| {
                        validator.as_deref()
                    });
                validate_postgresql_fdw_options(
                    effective_validator,
                    options
                        .iter()
                        .flat_map(|options| options.iter())
                        .map(|option| match option {
                            crabka_pgparser::ast::ForeignOptionAction::Add { name, .. }
                            | crabka_pgparser::ast::ForeignOptionAction::Set { name, .. }
                            | crabka_pgparser::ast::ForeignOptionAction::Drop { name } => name,
                        }),
                    PostgresqlFdwOptionContext::Wrapper,
                )?;
                let option_mutations = options.as_deref().map(foreign_option_mutations);
                crabka_pgcatalog::alter_fdw_ops(
                    kv,
                    name,
                    handler.as_ref().map(Option::as_deref),
                    validator.as_ref().map(Option::as_deref),
                    option_mutations.as_deref(),
                )?
            };
            Ok((command("ALTER FOREIGN DATA WRAPPER"), ops))
        }
        Statement::DropFdw {
            name,
            if_exists,
            cascade,
        } => {
            if crabka_pgcatalog::get_fdw(kv, name).is_ok() {
                require_foreign_ownership(
                    kv,
                    crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper,
                    std::slice::from_ref(name),
                    fctx,
                )?;
            }
            if !cascade {
                reject_fdw_dependents(kv, name)?;
            }
            let ops = match crabka_pgcatalog::drop_fdw_with_dependents_ops(kv, name, *cascade) {
                Ok(ops) => ops,
                Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) if *if_exists => Vec::new(),
                Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) => {
                    return Err(foreign_object_missing(
                        crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper,
                        name,
                    ));
                }
                Err(error) => return Err(error.into()),
            };
            Ok((command("DROP FOREIGN DATA WRAPPER"), ops))
        }
        Statement::CreateServer {
            if_not_exists,
            name,
            wrapper,
            server_type,
            version,
            options,
        } => {
            require_foreign_usage(
                kv,
                crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper,
                wrapper,
                fctx,
            )?;
            let fdw = crabka_pgcatalog::get_fdw(kv, wrapper)?;
            validate_postgresql_fdw_options(
                fdw.validator.as_deref(),
                options.iter().map(|(name, _)| name),
                PostgresqlFdwOptionContext::Server,
            )?;
            let ops = ignore_foreign_duplicate(
                crabka_pgcatalog::create_server_with_identity_owned_ops(
                    kv,
                    name,
                    wrapper,
                    server_type.as_deref(),
                    version.as_deref(),
                    options.clone(),
                    fctx.effective_role(),
                ),
                crabka_pgcatalog::ForeignPrivilegeTarget::Server,
                name,
                *if_not_exists,
            )?
            .unwrap_or_default();
            Ok((command("CREATE SERVER"), ops))
        }
        Statement::DropServer {
            name,
            if_exists,
            cascade,
        } => {
            if crabka_pgcatalog::get_server(kv, name).is_ok() {
                require_foreign_ownership(
                    kv,
                    crabka_pgcatalog::ForeignPrivilegeTarget::Server,
                    std::slice::from_ref(name),
                    fctx,
                )?;
            }
            if !cascade {
                reject_server_dependents(kv, name)?;
            }
            let ops = match crabka_pgcatalog::drop_server_with_dependents_ops(kv, name, *cascade) {
                Ok(ops) => ops,
                Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) if *if_exists => Vec::new(),
                Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) => {
                    return Err(foreign_object_missing(
                        crabka_pgcatalog::ForeignPrivilegeTarget::Server,
                        name,
                    ));
                }
                Err(error) => return Err(error.into()),
            };
            Ok((command("DROP SERVER"), ops))
        }
        Statement::CreateUserMapping {
            if_not_exists,
            user,
            server,
            options,
        } => {
            let resolved_user = role_spec_name(user, fctx);
            require_mapping_role(kv, resolved_user)?;
            require_user_mapping_authority(kv, resolved_user, server, fctx)?;
            validate_postgresql_user_mapping_options(
                kv,
                server,
                options.iter().map(|(name, _)| name),
            )?;
            let ops = match crabka_pgcatalog::create_user_mapping_ops(
                kv,
                resolved_user,
                server,
                options.clone(),
            ) {
                Ok(ops) => ops,
                Err(crabka_pgcatalog::CatalogError::DuplicateObject(_)) if *if_not_exists => {
                    Vec::new()
                }
                Err(crabka_pgcatalog::CatalogError::DuplicateObject(_)) => {
                    return Err(user_mapping_exists(resolved_user, server));
                }
                Err(error) => return Err(error.into()),
            };
            Ok((command("CREATE USER MAPPING"), ops))
        }
        Statement::DropUserMapping {
            user,
            server,
            if_exists,
            cascade: _,
        } => {
            // No object can depend on this one in this engine, so CASCADE and
            // RESTRICT are indistinguishable; both are accepted.

            let resolved_user = role_spec_name(user, fctx);
            if *if_exists
                && (!crabka_pgcatalog::role_is_nameable(kv, resolved_user)?
                    || crabka_pgcatalog::get_server(kv, server).is_err())
            {
                return Ok((command("DROP USER MAPPING"), Vec::new()));
            }
            require_mapping_role(kv, resolved_user)?;
            require_user_mapping_authority(kv, resolved_user, server, fctx)?;
            let ops = match crabka_pgcatalog::drop_user_mapping_ops(kv, resolved_user, server) {
                Ok(ops) => ops,
                Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) if *if_exists => Vec::new(),
                Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) => {
                    return Err(user_mapping_missing(resolved_user, server));
                }
                Err(error) => return Err(error.into()),
            };
            Ok((command("DROP USER MAPPING"), ops))
        }
        Statement::CreateForeignTable {
            if_not_exists,
            name,
            columns,
            constraints,
            column_options,
            like,
            inherits,
            partition_of,
            server,
            options,
        } => {
            let name = resolve_relation(kv, resolution, name, SchemaDisposition::Creation)?;
            require_foreign_usage(
                kv,
                crabka_pgcatalog::ForeignPrivilegeTarget::Server,
                server,
                fctx,
            )?;
            reject_foreign_table_index_constraints(columns, constraints)?;
            crate::usertype::ensure_relation_type_name_available(kv, &name)?;
            let ddl_ctx = crate::clock::EvalCtx::for_ddl(resolution, fctx.catalog);
            let inheritance_parents = inherits
                .iter()
                .map(|parent| {
                    resolve_relation(kv, resolution, parent, SchemaDisposition::Reference)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let (mut cols, checks, _, _, _) = match partition_of {
                Some(spec) => partition_definition(kv, &name, spec, constraints, like, &ddl_ctx)?,
                None if inheritance_parents.is_empty() => {
                    create_table_definition(kv, &name, columns, constraints, like, &[], &ddl_ctx)?
                }
                None => inherited_table_definition(
                    kv,
                    &name,
                    &inheritance_parents,
                    columns,
                    constraints,
                    like,
                    &ddl_ctx,
                )?,
            };
            apply_table_not_null_constraints(&mut cols, constraints, &name)?;
            for column in &mut cols {
                if column.identity.take().is_some() {
                    column.default = None;
                }
            }
            crate::scope::reject_system_column_names(cols.iter().map(|c| c.name.as_str()))?;
            let attachment = partition_of
                .as_ref()
                .map(|spec| partition_attachment(kv, &name, spec, &cols, &ddl_ctx))
                .transpose()?;
            if let Some((parent, _)) = &attachment {
                crate::exec::ddl_partition::reject_foreign_partition_with_unique_index(
                    kv, parent, &name, false,
                )?;
            }
            let mut ops = ignore_duplicate(
                crabka_pgcatalog::create_foreign_table_ops(
                    kv,
                    &name,
                    cols,
                    server,
                    options.clone(),
                    column_options.clone(),
                    checks,
                    fctx.table_creation(),
                ),
                *if_not_exists,
            )?
            .map_or_else(Vec::new, |(_id, ops)| ops);
            if let Some((parent, bound)) = attachment {
                ops.extend(crate::partition::attach_ops(&parent, &name, &bound));
            }
            if !inheritance_parents.is_empty() {
                ops.extend(crate::inheritance::attach_ops(&name, &inheritance_parents));
            }
            Ok((command("CREATE FOREIGN TABLE"), ops))
        }
        Statement::DropForeignTable {
            names,
            if_exists,
            cascade,
        } => {
            let mut targets = Vec::with_capacity(names.len());
            for reference in names {
                // A foreign table shares the ordinary table catalog key, so
                // the ordinary table drop path removes it (catalog entry,
                // sequence, rows, and dependent relations).
                let name =
                    &resolve_relation(kv, resolution, reference, SchemaDisposition::Utility)?;
                // Sharing that key is exactly why the kind has to be checked:
                // `DROP FOREIGN TABLE t` must not drop an ordinary table.
                if let Some(error) = drop_kind_mismatch(kv, name, "foreign table") {
                    return Err(error);
                }
                match crabka_pgcatalog::get_table(kv, name) {
                    Ok(table) => targets.push(table),
                    Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) if *if_exists => {}
                    Err(crabka_pgcatalog::CatalogError::UndefinedTable(missing)) => {
                        return Err(ExecError::UndefinedRelationOfKind {
                            kind: "foreign table",
                            name: missing,
                        });
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            let mut dropping: std::collections::HashSet<_> =
                targets.iter().map(|table| table.name.clone()).collect();
            if !*cascade {
                for table in &targets {
                    let dependents: Vec<_> = crate::inheritance::children_of(kv, &table.name)?
                        .into_iter()
                        .filter(|child| !dropping.contains(child))
                        .map(|child| (child, table.name.clone()))
                        .collect();
                    if !dependents.is_empty() {
                        return Err(dependent_objects_error(
                            kv,
                            &format!(
                                "cannot drop foreign table {} because other objects depend on it",
                                table.name
                            ),
                            &dependents,
                        ));
                    }
                }
            }
            let mut removed = targets.clone();
            if *cascade {
                for table in &targets {
                    for descendant in crate::inheritance::descendants(kv, &table.name)? {
                        if dropping.insert(descendant.clone()) {
                            removed.push(crabka_pgcatalog::get_table(kv, &descendant)?);
                        }
                    }
                }
            }
            let mut ops = Vec::new();
            for table in &removed {
                ops.extend(drop_table_and_dependents_ops(
                    kv, table, &dropping, *cascade,
                )?);
            }
            ops.extend(crate::inheritance::drop_metadata_ops(kv, &dropping)?);
            Ok((command("DROP FOREIGN TABLE"), ops))
        }
        Statement::AlterServer {
            name,
            rename_to,
            owner_to,
            version,
            options,
        } => {
            require_foreign_ownership(
                kv,
                crabka_pgcatalog::ForeignPrivilegeTarget::Server,
                std::slice::from_ref(name),
                fctx,
            )?;
            let ops = if let Some(rename_to) = rename_to {
                crabka_pgcatalog::rename_server_ops(kv, name, rename_to)?
            } else if let Some(owner) = owner_to {
                let owner = resolve_new_owner(kv, fctx, owner)?;
                require_new_owner_role(kv, fctx, &owner)?;
                let server = foreign_object_lookup(
                    crabka_pgcatalog::ForeignPrivilegeTarget::Server,
                    name,
                    crabka_pgcatalog::get_server(kv, name),
                )?;
                require_foreign_usage(
                    kv,
                    crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper,
                    &server.wrapper,
                    fctx,
                )?;
                crabka_pgcatalog::set_server_owner_ops(kv, name, &owner)?
            } else {
                let server = crabka_pgcatalog::get_server(kv, name)?;
                let fdw = crabka_pgcatalog::get_fdw(kv, &server.wrapper)?;
                validate_postgresql_fdw_options(
                    fdw.validator.as_deref(),
                    options
                        .iter()
                        .flat_map(|options| options.iter())
                        .map(|option| match option {
                            crabka_pgparser::ast::ForeignOptionAction::Add { name, .. }
                            | crabka_pgparser::ast::ForeignOptionAction::Set { name, .. }
                            | crabka_pgparser::ast::ForeignOptionAction::Drop { name } => name,
                        }),
                    PostgresqlFdwOptionContext::Server,
                )?;
                let option_mutations = options.as_deref().map(foreign_option_mutations);
                crabka_pgcatalog::alter_server_ops(
                    kv,
                    name,
                    version.as_deref(),
                    option_mutations.as_deref(),
                )?
            };
            Ok((command("ALTER SERVER"), ops))
        }
        Statement::AlterUserMapping {
            user,
            server,
            options,
        } => {
            let user = role_spec_name(user, fctx);
            require_mapping_role(kv, user)?;
            require_user_mapping_authority(kv, user, server, fctx)?;
            validate_postgresql_user_mapping_options(
                kv,
                server,
                options.iter().map(|option| match option {
                    crabka_pgparser::ast::ForeignOptionAction::Add { name, .. }
                    | crabka_pgparser::ast::ForeignOptionAction::Set { name, .. }
                    | crabka_pgparser::ast::ForeignOptionAction::Drop { name } => name,
                }),
            )?;
            let ops = match crabka_pgcatalog::alter_user_mapping_options_ops(
                kv,
                user,
                server,
                &foreign_option_mutations(options),
            ) {
                Ok(ops) => ops,
                Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) => {
                    return Err(user_mapping_missing(user, server));
                }
                Err(error) => return Err(error.into()),
            };
            Ok((command("ALTER USER MAPPING"), ops))
        }
        // SP40: IMPORT FOREIGN SCHEMA discovers the server's tables through the
        // registered scanner (the `kafka_fdw` seam enumerates Kafka topics and
        // derives each topic's value columns from its Schema Registry subject),
        // then materializes a foreign table per discovered table through the same
        // write-op seam as local DDL.
        //
        // `remote_schema` is accepted but unused in phase 1 (Kafka has no
        // nested schemas). `INTO <schema>` names the local schema every
        // discovered table lands in, which must exist — 3F000 otherwise, as for
        // any other qualifier.
        Statement::ImportForeignSchema {
            remote_schema: _,
            selector,
            server,
            into_schema,
            options,
        } => {
            require_foreign_usage(
                kv,
                crabka_pgcatalog::ForeignPrivilegeTarget::Server,
                server,
                fctx,
            )?;
            // Resolve the server (42704 if undefined) and the current user's
            // optional mapping (falling back to PUBLIC, then no credentials).
            let srv = crabka_pgcatalog::get_server(kv, server)?;
            crate::exec::foreign_scan::require_server_handler(kv, &srv)?;
            let mapping =
                crabka_pgcatalog::get_user_mapping_or_public(kv, fctx.current_user, server)?;
            // A scanner must be registered (the `kafka` feature is built in).
            let scanner = fctx.scanner.ok_or_else(|| {
                ExecError::Unsupported("foreign tables require the `kafka` feature".into())
            })?;
            let filter = crate::foreign::ImportFilter::from_selector(selector);
            let tables =
                scanner.import_schema_with_options(&srv, mapping.as_ref(), &filter, options)?;
            let mut ops = Vec::new();
            // Every table lands in one batch, so the counter cannot be read
            // again between them — an unapplied bump is invisible to the next
            // read, and each table would be created under the same id. The
            // batch allocates from a cursor of its own and owes exactly one
            // bump, which is sound because the session holds the counter lock
            // for the whole statement.
            let mut cursor: Option<crabka_pgcatalog::TableId> = None;
            for table in tables {
                let into = resolve_relation(
                    kv,
                    resolution,
                    &crabka_pgparser::ast::RelationRef::qualified(into_schema, &table.name),
                    SchemaDisposition::Creation,
                )?;
                crate::usertype::ensure_relation_type_name_available(kv, &into)?;
                // The remote schema chooses these names, so this is the one
                // creation path where the refusal reports something the session
                // did not write. It is still the right answer: the relation
                // would have a column no reference could reach.
                crate::scope::reject_system_column_names(
                    table.columns.iter().map(|c| c.name.as_str()),
                )?;
                let id = match cursor {
                    Some(next) => next,
                    None => crabka_pgcatalog::read_next_table_id(kv)?,
                };
                cursor = Some(id + 1);
                let (_id, mut table_ops) = crabka_pgcatalog::create_foreign_table_ops(
                    kv,
                    &into,
                    table.columns,
                    &srv.name,
                    table.options,
                    Vec::new(),
                    Vec::new(),
                    crabka_pgcatalog::TableCreation {
                        owner: fctx.effective_role(),
                        id: crabka_pgcatalog::TableIdSource::Reserved(id),
                        materialized: None,
                    },
                )?;
                ops.append(&mut table_ops);
            }
            if let Some(next) = cursor {
                ops.push(crabka_pgcatalog::set_next_table_id_op(next));
            }
            Ok((command("IMPORT FOREIGN SCHEMA"), ops))
        }
        _ => Err(ExecError::Unsupported("not a DDL statement".into())),
    }
}

fn foreign_option_mutations(
    options: &[crabka_pgparser::ast::ForeignOptionAction],
) -> Vec<crabka_pgcatalog::ForeignOptionMutation> {
    options
        .iter()
        .map(|option| match option {
            crabka_pgparser::ast::ForeignOptionAction::Add { name, value } => {
                crabka_pgcatalog::ForeignOptionMutation::Add {
                    name: name.clone(),
                    value: value.clone(),
                }
            }
            crabka_pgparser::ast::ForeignOptionAction::Set { name, value } => {
                crabka_pgcatalog::ForeignOptionMutation::Set {
                    name: name.clone(),
                    value: value.clone(),
                }
            }
            crabka_pgparser::ast::ForeignOptionAction::Drop { name } => {
                crabka_pgcatalog::ForeignOptionMutation::Drop { name: name.clone() }
            }
        })
        .collect()
}

fn reject_foreign_role_dependencies(kv: &dyn Kv, role: &str) -> Result<(), ExecError> {
    let mut dependents = Vec::new();
    for fdw in crabka_pgcatalog::list_fdws(kv)? {
        if fdw.owner == role {
            dependents.push(format!("owner of foreign-data wrapper {}", fdw.name));
        }
        if crabka_pgcatalog::list_foreign_privileges(
            kv,
            crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper,
            &fdw.name,
        )?
        .iter()
        .any(|(grantee, _, _)| grantee == role)
        {
            dependents.push(format!("privileges for foreign-data wrapper {}", fdw.name));
        }
    }
    for server in crabka_pgcatalog::list_servers(kv)? {
        if server.owner == role {
            dependents.push(format!("owner of server {}", server.name));
        }
        if crabka_pgcatalog::list_foreign_privileges(
            kv,
            crabka_pgcatalog::ForeignPrivilegeTarget::Server,
            &server.name,
        )?
        .iter()
        .any(|(grantee, _, _)| grantee == role)
        {
            dependents.push(format!("privileges for server {}", server.name));
        }
    }
    for mapping in crabka_pgcatalog::list_user_mappings(kv)? {
        if mapping.user == role {
            dependents.push(format!(
                "user mapping for {role} on server {}",
                mapping.server
            ));
        }
    }
    if dependents.is_empty() {
        return Ok(());
    }
    Err(ExecError::Remote(
        crabka_pgwire::error::PgError::error(
            "2BP01",
            format!("role \"{role}\" cannot be dropped because some objects depend on it"),
        )
        .with_detail(dependents.join("\n")),
    ))
}

fn reject_fdw_dependents(kv: &dyn Kv, name: &str) -> Result<(), ExecError> {
    let servers = crabka_pgcatalog::list_servers(kv)?
        .into_iter()
        .filter(|server| server.wrapper == name)
        .collect::<Vec<_>>();
    if servers.is_empty() {
        return Ok(());
    }
    let mut dependents = Vec::new();
    for server in servers {
        dependents.push(format!(
            "server {} depends on foreign-data wrapper {name}",
            server.name
        ));
        for mapping in crabka_pgcatalog::list_user_mappings(kv)?
            .into_iter()
            .filter(|mapping| mapping.server == server.name)
        {
            dependents.push(format!(
                "user mapping for {} on server {} depends on server {}",
                mapping.user, server.name, server.name
            ));
        }
    }
    Err(foreign_dependents_error(
        format!("cannot drop foreign-data wrapper {name} because other objects depend on it"),
        dependents,
    ))
}

fn reject_server_dependents(kv: &dyn Kv, name: &str) -> Result<(), ExecError> {
    let mut dependents = crabka_pgcatalog::list_user_mappings(kv)?
        .into_iter()
        .filter(|mapping| mapping.server == name)
        .map(|mapping| {
            format!(
                "user mapping for {} on server {name} depends on server {name}",
                mapping.user
            )
        })
        .collect::<Vec<_>>();
    dependents.extend(
        crabka_pgcatalog::list_tables(kv)?
            .into_iter()
            .filter(|table| {
                table
                    .foreign
                    .as_ref()
                    .is_some_and(|foreign| foreign.server == name)
            })
            .map(|table| format!("foreign table {} depends on server {name}", table.name)),
    );
    if dependents.is_empty() {
        return Ok(());
    }
    Err(foreign_dependents_error(
        format!("cannot drop server {name} because other objects depend on it"),
        dependents,
    ))
}

fn foreign_dependents_error(message: String, dependents: Vec<String>) -> ExecError {
    ExecError::Remote(
        crabka_pgwire::error::PgError::error("2BP01", message)
            .with_detail(dependents.join("\n"))
            .with_hint("Use DROP ... CASCADE to drop the dependent objects too."),
    )
}

fn require_mapping_role(kv: &dyn Kv, role: &str) -> Result<(), ExecError> {
    if role == crabka_pgcatalog::PUBLIC_ROLE || crabka_pgcatalog::role_is_nameable(kv, role)? {
        Ok(())
    } else {
        Err(undefined_role(role))
    }
}

fn user_mapping_exists(user: &str, server: &str) -> ExecError {
    ExecError::Remote(crabka_pgwire::error::PgError::error(
        "42710",
        format!("user mapping for \"{user}\" already exists for server \"{server}\""),
    ))
}

fn user_mapping_missing(user: &str, server: &str) -> ExecError {
    ExecError::Remote(crabka_pgwire::error::PgError::error(
        "42704",
        format!("user mapping for \"{user}\" does not exist for server \"{server}\""),
    ))
}

fn reject_foreign_table_index_constraints(
    columns: &[crabka_pgparser::ast::ColumnDef],
    constraints: &[crabka_pgparser::ast::TableConstraint],
) -> Result<(), ExecError> {
    use crabka_pgparser::ast::ColumnConstraintKind;

    let column_kind = columns
        .iter()
        .flat_map(|column| &column.constraints)
        .find_map(|constraint| match constraint.kind {
            ColumnConstraintKind::PrimaryKey => Some("primary key"),
            ColumnConstraintKind::Unique { .. } => Some("unique"),
            ColumnConstraintKind::References(_) => Some("foreign key"),
            _ => None,
        });
    let table_kind = constraints
        .iter()
        .find_map(|constraint| foreign_table_index_constraint_kind(&constraint.kind));
    let Some(kind) = column_kind.or(table_kind) else {
        return Ok(());
    };
    Err(ExecError::Unsupported(format!(
        "{kind} constraints are not supported on foreign tables"
    )))
}

fn foreign_table_index_constraint_kind(
    kind: &crabka_pgparser::ast::TableConstraintKind,
) -> Option<&'static str> {
    use crabka_pgparser::ast::TableConstraintKind;

    match kind {
        TableConstraintKind::PrimaryKey { .. } => Some("primary key"),
        TableConstraintKind::Unique { .. } => Some("unique"),
        TableConstraintKind::ForeignKey { .. } => Some("foreign key"),
        TableConstraintKind::Exclude { .. } => Some("exclusion"),
        TableConstraintKind::NotNull { .. } | TableConstraintKind::Check(_) => None,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PostgresqlFdwOptionContext {
    Wrapper,
    Server,
    UserMapping,
}

fn validate_postgresql_fdw_options<'a>(
    validator: Option<&str>,
    mut names: impl Iterator<Item = &'a String>,
    context: PostgresqlFdwOptionContext,
) -> Result<(), ExecError> {
    if validator != Some("postgresql_fdw_validator") {
        return Ok(());
    }
    let valid = match context {
        PostgresqlFdwOptionContext::Wrapper => &[][..],
        PostgresqlFdwOptionContext::Server => {
            &["host", "hostaddr", "port", "dbname", "connect_timeout"]
        }
        PostgresqlFdwOptionContext::UserMapping => &["user", "password"],
    };
    let Some(name) = names.find(|name| !valid.contains(&name.as_str())) else {
        return Ok(());
    };
    let error = crabka_pgwire::error::PgError::error("HV00D", format!("invalid option \"{name}\""));
    let error = match context {
        PostgresqlFdwOptionContext::Wrapper => {
            error.with_hint("There are no valid options in this context.")
        }
        PostgresqlFdwOptionContext::UserMapping if name == "username" => {
            error.with_hint("Perhaps you meant the option \"user\".")
        }
        PostgresqlFdwOptionContext::Server | PostgresqlFdwOptionContext::UserMapping => error,
    };
    Err(ExecError::Remote(error))
}

fn validate_postgresql_user_mapping_options<'a>(
    kv: &dyn Kv,
    server: &str,
    names: impl Iterator<Item = &'a String>,
) -> Result<(), ExecError> {
    let server = foreign_object_lookup(
        crabka_pgcatalog::ForeignPrivilegeTarget::Server,
        server,
        crabka_pgcatalog::get_server(kv, server),
    )?;
    let fdw = crabka_pgcatalog::get_fdw(kv, &server.wrapper)?;
    validate_postgresql_fdw_options(
        fdw.validator.as_deref(),
        names,
        PostgresqlFdwOptionContext::UserMapping,
    )
}

/// Validate an FDW support routine before storing its name in the catalog.
///
/// The PostgreSQL fixture supplies `postgresql_fdw_validator`; other support
/// routines are ordinary user functions and resolve from the routine catalog.
fn validate_fdw_routine(
    kv: &dyn Kv,
    written: &str,
    input_types: &[&str],
    expected_return: &str,
) -> Result<(), ExecError> {
    if written == "postgresql_fdw_validator" && input_types == ["text[]", "oid"] {
        return Ok(());
    }
    let name = written.strip_prefix("public.").unwrap_or(written);
    let routine = crabka_pgcatalog::routine::routines_named(kv, name)?
        .into_iter()
        .find(|routine| {
            routine
                .input_params()
                .map(|parameter| parameter.ty.name.as_str())
                .eq(input_types.iter().copied())
        })
        .ok_or_else(|| {
            ExecError::UndefinedFunction(format!(
                "function {written}({}) does not exist",
                input_types.join(", ")
            ))
        })?;
    let returns_expected = matches!(
        routine.result,
        crabka_pgcatalog::routine::RoutineResult::Type { ref ty, setof: false }
            if ty.name == expected_return
    );
    if returns_expected {
        return Ok(());
    }
    Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
        "42804",
        format!("function {written} must return type {expected_return}"),
    )))
}

fn create_access_method(
    kv: &dyn Kv,
    name: &str,
    kind: crabka_pgparser::ast::AccessMethodKind,
    handler: &str,
) -> Result<(QueryResult, Vec<crabka_pgkv::WriteOp>), ExecError> {
    use crabka_pgparser::ast::AccessMethodKind;

    let kind = match kind {
        AccessMethodKind::Index => crabka_pgcatalog::AccessMethodKind::Index,
        AccessMethodKind::Table => crabka_pgcatalog::AccessMethodKind::Table,
    };
    let expected = match kind {
        crabka_pgcatalog::AccessMethodKind::Index => "index_am_handler",
        crabka_pgcatalog::AccessMethodKind::Table => "table_am_handler",
    };
    let actual = match handler {
        "bthandler" | "hashhandler" | "gisthandler" | "ginhandler" | "spghandler"
        | "brinhandler" => "index_am_handler",
        "heap_tableam_handler" => "table_am_handler",
        _ => {
            return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                "42883",
                format!("function {handler}(internal) does not exist"),
            )));
        }
    };
    if actual != expected {
        return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
            "42804",
            format!("function {handler} must return type {expected}"),
        )));
    }
    if matches!(
        name,
        "heap" | "btree" | "hash" | "gist" | "gin" | "spgist" | "brin"
    ) {
        return Err(ExecError::DuplicateObject(format!(
            "access method \"{name}\" already exists"
        )));
    }
    Ok((
        command("CREATE ACCESS METHOD"),
        crabka_pgcatalog::create_access_method_ops(kv, name, kind, handler)?,
    ))
}

pub(crate) fn resolve_table_access_method_oid(kv: &dyn Kv, name: &str) -> Result<u32, ExecError> {
    if name == "heap" {
        return Ok(2);
    }
    if crate::catalog_rel::access_method_oid(name).is_some() {
        return Err(ExecError::WrongObjectType(format!(
            "access method \"{name}\" is not of type TABLE"
        )));
    }
    let method = match crabka_pgcatalog::get_access_method(kv, name) {
        Ok(method) => method,
        Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) => {
            return Err(ExecError::UndefinedObject(format!(
                "access method \"{name}\" does not exist"
            )));
        }
        Err(error) => return Err(error.into()),
    };
    if method.kind != crabka_pgcatalog::AccessMethodKind::Table {
        return Err(ExecError::WrongObjectType(format!(
            "access method \"{name}\" is not of type TABLE"
        )));
    }
    Ok(method.oid)
}

/// `CREATE RECURSIVE VIEW name (columns) AS body` is PostgreSQL's shorthand
/// for a view whose stored query is a recursive CTE named `name`.
fn recursive_view_definition(
    name: &crabka_pgparser::ast::RelationRef,
    definition: &str,
    columns: &Option<Vec<String>>,
) -> Result<(String, crabka_pgparser::ast::QueryExpr), ExecError> {
    let name = crate::catalog_fn::quote_identifier(&name.name);
    let columns = columns.as_ref().map_or_else(String::new, |columns| {
        format!(
            " ({})",
            columns
                .iter()
                .map(|column| crate::catalog_fn::quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        )
    });
    let definition =
        format!("WITH RECURSIVE {name}{columns} AS ({definition}) SELECT * FROM {name}");
    let statements = crabka_pgparser::parse(&definition)?;
    let [crabka_pgparser::ast::Statement::Query(query)] = statements.as_slice() else {
        unreachable!("a generated recursive view body is one query")
    };
    Ok((definition, query.clone()))
}

/// The in-progress state of one `ALTER TABLE` statement: later subcommands see
/// the effect of earlier ones, and everything is emitted as one atomic batch.
pub(crate) struct AlterTableState {
    pub(crate) table: Table,
    /// The open transaction's xid, when this `ALTER TABLE` runs inside one.
    ///
    /// Held on the state rather than passed to each validation because every
    /// subcommand that back-validates needs it and a statement has exactly one:
    /// a parameter would be eight chances to forget, and forgetting reads as a
    /// constraint that passed.
    pub(crate) own_xid: Option<u64>,
    /// Row versions, rewritten in place by column add/drop/type changes.
    pub(crate) rows: Option<Vec<RowVersion>>,
    pub(crate) ops: Vec<crabka_pgkv::WriteOp>,
    /// Names of secondary indexes dropped by this statement; a later action must
    /// not resurrect them.
    pub(crate) dropped_indexes: Vec<String>,
    /// Indexes created by this statement. They are not in the catalog yet, so a
    /// later action that has to rebuild them cannot find them by listing.
    pub(crate) created_indexes: Vec<crabka_pgcatalog::Index>,
    /// Columns already retyped by this statement; `PostgreSQL` refuses a second
    /// type change for the same column in one `ALTER TABLE`.
    pub(crate) retyped_columns: Vec<String>,
    /// The regular inheritance parents after this statement's subcommands.
    pub(crate) inheritance_parents: Option<Vec<crabka_pgcatalog::RelationName>>,
    /// Foreign keys this statement added. They are not in the catalog yet, so a
    /// later subcommand can only find them here: a name collision check, a
    /// `VALIDATE`, or a `DROP`.
    pub(crate) created_foreign_keys: Vec<crabka_pgcatalog::ForeignKey>,
    /// Names of foreign keys this statement dropped; a later subcommand must not
    /// resurrect them from the catalog.
    pub(crate) dropped_foreign_keys: Vec<String>,
    /// Creation-order ids for the foreign keys this statement adds. One cursor
    /// spans every subcommand, because none of their records reach the KV until
    /// the whole batch commits. Two `ADD CONSTRAINT`s that read the stored
    /// counter would otherwise tie, and a tie has no defined firing order.
    pub(crate) foreign_key_ids: crabka_pgcatalog::ForeignKeyIds,
}

/// A store with one statement's not-yet-committed write batch layered over it.
///
/// A statement's ops only reach the KV when the session commits the whole batch,
/// so anything that has to read *this* statement's own effects cannot read the
/// store directly. Two things do:
///
/// - the end-of-statement referential drain, whose whole premise is that the
///   statement's rows already exist. `INSERT INTO t (id, boss) VALUES (1, 1)`
///   against a self-referencing foreign key succeeds because the parent row the
///   check looks for is the one the statement just staged;
/// - a foreign key back-validated by a multi-subcommand `ALTER TABLE`, which has
///   to resolve the relation as this statement has already rewritten it, with
///   an added column or a rebuilt index, and not as storage still holds it.
///
/// Read-only through the [`Kv`] trait: the real batch is written by the session
/// once the statement is complete. [`StagedKv::stage`] is the one way the
/// overlay grows, and the drain's referential actions are the one caller. An
/// action's ops belong in the view the next action reads, which is what makes a
/// second constraint's action operate on the row's *current* image and a cascade
/// cycle come back around to a row that reads as deleted.
pub(crate) struct StagedKv<'a> {
    pub(crate) base: &'a dyn Kv,
    /// `None` marks a key the batch deletes.
    pub(crate) staged: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
}

impl<'a> StagedKv<'a> {
    pub(crate) fn new(base: &'a dyn Kv, ops: &[crabka_pgkv::WriteOp]) -> Self {
        let view = Self {
            base,
            staged: Mutex::new(BTreeMap::new()),
        };
        view.stage(ops);
        view
    }

    /// Fold more ops into the overlay, so every later read through this view
    /// sees them.
    ///
    /// The lock spans the fold and nothing else, and no read here awaits. So
    /// the interior mutability costs one uncontended acquire per KV
    /// operation.
    pub(crate) fn stage(&self, ops: &[crabka_pgkv::WriteOp]) {
        let mut staged = self.staged.lock().expect("staged write-batch mutex");
        for op in ops {
            match op {
                crabka_pgkv::WriteOp::Put { key, value }
                | crabka_pgkv::WriteOp::ConditionalPut { key, value, .. } => {
                    staged.insert(key.clone(), Some(value.clone()));
                }
                crabka_pgkv::WriteOp::Delete { key } => {
                    staged.insert(key.clone(), None);
                }
            }
        }
    }
}

/// Layer the staged writes over a base scan: a staged key replaces (or removes)
/// whatever the base returned for it, and a staged key the base never had joins
/// the result.
pub(crate) fn merge_staged<'s>(
    staged: &'s BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    mut pairs: crabka_pgkv::KvScan,
    overlay: impl Iterator<Item = (&'s Vec<u8>, &'s Vec<u8>)>,
) -> crabka_pgkv::KvScan {
    pairs.retain(|(key, _)| !staged.contains_key(key));
    for (key, value) in overlay {
        pairs.push((key.clone(), value.clone()));
    }
    pairs.sort_by(|left, right| left.0.cmp(&right.0));
    pairs
}

pub(crate) fn staged_kv_is_read_only() -> crabka_pgkv::KvError {
    crabka_pgkv::KvError::Io("the staged write-batch view is read-only".into())
}

impl Kv for StagedKv<'_> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, crabka_pgkv::KvError> {
        let staged = self
            .staged
            .lock()
            .expect("staged write-batch mutex")
            .get(key)
            .cloned();
        match staged {
            Some(value) => Ok(value),
            None => self.base.get(key),
        }
    }

    fn put(&self, _key: Vec<u8>, _value: Vec<u8>) -> Result<(), crabka_pgkv::KvError> {
        Err(staged_kv_is_read_only())
    }

    fn delete(&self, _key: &[u8]) -> Result<(), crabka_pgkv::KvError> {
        Err(staged_kv_is_read_only())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<crabka_pgkv::KvScan, crabka_pgkv::KvError> {
        let base = self.base.scan_prefix(prefix)?;
        let staged = self.staged.lock().expect("staged write-batch mutex");
        // Ranged rather than a walk of the whole batch: a bulk COPY stages one
        // op per row and the drain probes once per queued check.
        let overlay = staged
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .filter_map(|(key, value)| value.as_ref().map(|value| (key, value)));
        Ok(merge_staged(&staged, base, overlay))
    }

    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<crabka_pgkv::KvScan, crabka_pgkv::KvError> {
        let base = self.base.scan_range(start, end)?;
        let staged = self.staged.lock().expect("staged write-batch mutex");
        let overlay = staged
            .range(start.to_vec()..end.to_vec())
            .filter_map(|(key, value)| value.as_ref().map(|value| (key, value)));
        Ok(merge_staged(&staged, base, overlay))
    }

    fn write_batch(&self, _ops: &[crabka_pgkv::WriteOp]) -> Result<(), crabka_pgkv::KvError> {
        Err(staged_kv_is_read_only())
    }
}

impl AlterTableState {
    pub(crate) fn new(table: Table, own_xid: Option<u64>) -> Self {
        Self {
            table,
            own_xid,
            rows: None,
            ops: Vec::new(),
            dropped_indexes: Vec::new(),
            created_indexes: Vec::new(),
            retyped_columns: Vec::new(),
            inheritance_parents: None,
            created_foreign_keys: Vec::new(),
            dropped_foreign_keys: Vec::new(),
            foreign_key_ids: crabka_pgcatalog::ForeignKeyIds::default(),
        }
    }

    pub(crate) fn rows_mut(&mut self, kv: &dyn Kv) -> Result<&mut Vec<RowVersion>, ExecError> {
        if self.rows.is_none() {
            self.rows = Some(scan_all_row_versions(kv, &self.table)?);
        }
        Ok(self.rows.as_mut().expect("row versions were just loaded"))
    }

    /// The table's live rows *as this statement has already rewritten them*.
    ///
    /// Back-validation and index backfill must never re-read storage: an
    /// earlier subcommand in the same batch may have added, dropped, or
    /// retyped a column, so the stored versions no longer have the shape the
    /// working `table` describes. Reading them under the working column list
    /// mismatches row width against scope width.
    /// The relation's live rows as this statement has reshaped them so far,
    /// *logically* complete: every virtual generated column is filled in from
    /// the expression the statement leaves behind.
    ///
    /// Every caller is validating something — a `NOT NULL`, a `CHECK`, a unique
    /// key — against the values the relation will report once the statement
    /// commits, and for a virtual column those values exist nowhere else. The
    /// expansion is done here rather than at each caller so no validation can
    /// silently test the NULL placeholder instead.
    pub(crate) fn live_rows(
        &mut self,
        kv: &dyn Kv,
        ctx: &crate::clock::EvalCtx,
    ) -> Result<Vec<(u64, u64, Vec<Datum>)>, ExecError> {
        self.rows_mut(kv)?;
        let versions = self.rows.as_ref().expect("row versions were just loaded");
        let mut live = live_row_versions(kv, &self.table, versions, self.own_xid)?;
        for (_, _, row) in &mut live {
            expand_virtual_generated_row(
                &self.table,
                row,
                ctx,
                crate::scope::GeneratedReads::every(),
            )?;
        }
        Ok(live)
    }

    pub(crate) fn column_index(&self, column: &str) -> Result<usize, ExecError> {
        self.table
            .column_index(column)
            .ok_or_else(|| ExecError::UndefinedTableColumn {
                column: column.to_string(),
                table: self.table.name.to_string(),
            })
    }

    /// The relation's indexes with this statement's own creations and drops
    /// folded in. That is what a foreign key added here must resolve its
    /// referenced index against.
    pub(crate) fn current_indexes(
        &self,
        kv: &dyn Kv,
    ) -> Result<Vec<crabka_pgcatalog::Index>, ExecError> {
        let mut indexes = crabka_pgcatalog::list_table_indexes(kv, &self.table.name)?;
        indexes.retain(|index| !self.dropped_indexes.contains(&index.name));
        for index in &self.created_indexes {
            if !indexes.iter().any(|listed| listed.name == index.name) {
                indexes.push(index.clone());
            }
        }
        Ok(indexes)
    }

    /// The foreign keys the relation carries *as the child*, with this
    /// statement's own additions and drops folded in.
    pub(crate) fn current_foreign_keys(
        &self,
        kv: &dyn Kv,
    ) -> Result<Vec<crabka_pgcatalog::ForeignKey>, ExecError> {
        let mut keys = crabka_pgcatalog::list_table_foreign_keys(kv, self.table.id)?;
        keys.retain(|fk| !self.dropped_foreign_keys.contains(&fk.name));
        for fk in &self.created_foreign_keys {
            if !keys.iter().any(|listed| listed.name == fk.name) {
                keys.push(fk.clone());
            }
        }
        Ok(keys)
    }

    /// Every constraint name the relation uses, of every kind. `PostgreSQL`
    /// keeps one namespace per relation, so a new constraint must step around
    /// all three kinds.
    pub(crate) fn taken_constraint_names(&self, kv: &dyn Kv) -> Result<Vec<String>, ExecError> {
        let mut taken: Vec<String> = self
            .table
            .checks
            .iter()
            .map(|check| check.name.clone())
            .collect();
        taken.extend(
            self.current_indexes(kv)?
                .into_iter()
                .map(|index| index.name),
        );
        taken.extend(self.current_foreign_keys(kv)?.into_iter().map(|fk| fk.name));
        Ok(taken)
    }

    /// The catalog as this statement has already rewritten it: the working
    /// column and `CHECK` lists, plus every catalog op staged so far.
    pub(crate) fn staged_catalog<'a>(&self, kv: &'a dyn Kv) -> Result<StagedKv<'a>, ExecError> {
        let mut ops =
            crabka_pgcatalog::replace_table_schema_ops(kv, &self.table.name, &self.table)?;
        ops.extend_from_slice(&self.ops);
        Ok(StagedKv::new(kv, &ops))
    }
}

/// Validate added constraints against the column shape PostgreSQL presents to
/// them. PostgreSQL executes every `DROP COLUMN` pass before its constraint-add
/// passes, regardless of the subcommands' written order. Without this preflight,
/// `ADD UNIQUE (a), DROP COLUMN a` built a staged index and then removed `a`,
/// while PostgreSQL rejects the whole statement with 42703.
pub(crate) fn validate_alter_constraint_columns(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    table: &Table,
    actions: &[crabka_pgparser::ast::AlterTableAction],
) -> Result<(), ExecError> {
    use crabka_pgparser::ast::{AlterTableAction as Action, TableConstraintKind as Constraint};

    let mut columns: HashSet<String> = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    for action in actions {
        if let Action::DropColumn { column, .. } = action {
            columns.remove(column);
        }
    }
    // ADD COLUMN runs after DROP COLUMN but before either kind of added
    // constraint, so a dropped name re-added in the same statement is visible.
    for action in actions {
        if let Action::AddColumn { column, .. } = action {
            columns.insert(column.name.clone());
        }
    }

    for action in actions {
        let Action::AddConstraint(constraint) = action else {
            continue;
        };
        match &constraint.kind {
            Constraint::PrimaryKey { columns: keys, .. }
            | Constraint::Unique { columns: keys, .. } => {
                if let Some(missing) = keys.iter().find(|column| !columns.contains(*column)) {
                    return Err(ExecError::UndefinedIndexColumn(missing.clone()));
                }
            }
            Constraint::NotNull { column, .. } => {
                if !columns.contains(column) {
                    return Err(ExecError::UndefinedTableColumn {
                        column: column.clone(),
                        table: table.name.to_string(),
                    });
                }
            }
            Constraint::ForeignKey {
                columns: referencing,
                references,
                ..
            } => {
                if let Some(missing) = referencing.iter().find(|column| !columns.contains(*column))
                {
                    return Err(ExecError::UndefinedForeignKeyColumn(missing.clone()));
                }
                let referenced = resolve_relation(
                    kv,
                    resolution,
                    &references.table,
                    SchemaDisposition::Utility,
                )?;
                if referenced == table.name
                    && let Some(missing) = references
                        .columns
                        .iter()
                        .find(|column| !columns.contains(*column))
                {
                    return Err(ExecError::UndefinedForeignKeyColumn(missing.clone()));
                }
            }
            Constraint::Check(_) | Constraint::Exclude { .. } => {}
        }
    }
    Ok(())
}

/// `ALTER TABLE [IF EXISTS] name <action> [, …]`.
pub(crate) fn alter_table_ops(
    kv: &dyn Kv,
    table_name: &crabka_pgcatalog::RelationName,
    if_exists: bool,
    only: bool,
    actions: &[crabka_pgparser::ast::AlterTableAction],
    fctx: ForeignCtx<'_>,
) -> Result<(QueryResult, Vec<crabka_pgkv::WriteOp>), ExecError> {
    use crabka_pgparser::ast::AlterTableAction as Action;

    let resolution = fctx.resolution;

    // `PostgreSQL` tests this in the range-var callback every `ALTER` spelling
    // shares, before the subcommand list is looked at and before the relkind
    // is, so a system catalog gets the same privilege refusal whichever
    // subcommand was written — `ALTER SEQUENCE pg_class` included, which would
    // otherwise be a kind mismatch. `IF EXISTS` does not suppress it, because
    // the relation exists.
    if let Some(error) = system_catalog_wrong_kind(table_name) {
        return Err(error);
    }

    // RENAME TO is a statement of its own in PostgreSQL's grammar, so it never
    // shares a comma list and keeps its dedicated catalog path.
    if let [Action::RenameTable { new_name }] = actions {
        // `RENAME TO` never moves a relation between schemas: the new name is
        // unqualified and lands beside the old one, exactly as in PostgreSQL.
        let new_name = &table_name.sibling(new_name);
        if crabka_pgcatalog::get_table(kv, table_name).is_ok() {
            crate::usertype::ensure_relation_type_name_available(kv, new_name)?;
        }
        return match crabka_pgcatalog::rename_table_ops(kv, table_name, new_name) {
            Ok(mut ops) => {
                ops.extend(rename_relation_comment_ops(kv, table_name, new_name)?);
                ops.extend(rename_table_view_ops(kv, table_name, new_name)?);
                ops.extend(rename_name_keyed_metadata_ops(kv, table_name, new_name)?);
                if let Ok(table) = crabka_pgcatalog::get_table(kv, table_name) {
                    for mut trigger in crabka_pgcatalog::trigger::triggers_for_table(kv, table.id)?
                    {
                        trigger.table = new_name.clone();
                        ops.extend(crabka_pgcatalog::trigger::put_trigger_ops(kv, &trigger)?);
                    }
                }
                Ok((command("ALTER TABLE"), ops))
            }
            Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) if if_exists => {
                Ok((command("ALTER TABLE"), Vec::new()))
            }
            Err(error) => Err(error.into()),
        };
    }

    if let [Action::SetSchema(schema)] = actions {
        let table = match crabka_pgcatalog::get_table(kv, table_name) {
            Ok(table) => table,
            Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) if if_exists => {
                return Ok((command("ALTER TABLE"), Vec::new()));
            }
            Err(error) => return Err(error.into()),
        };
        if !crabka_pgcatalog::schema_exists(kv, schema)? {
            return Err(crabka_pgcatalog::CatalogError::UndefinedSchema(schema.clone()).into());
        }
        crate::privilege::require_ownership(
            kv,
            table_name,
            &table.owner,
            crate::privilege::RelationKind::Table,
            fctx.effective_role(),
        )?;
        if !crabka_pgcatalog::has_schema_privilege(kv, schema, fctx.effective_role(), "CREATE")? {
            return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                "42501",
                format!("permission denied for schema {schema}"),
            )));
        }
        let new_name = crabka_pgcatalog::RelationName::new(schema, &table_name.name);
        crate::usertype::ensure_relation_type_name_available(kv, &new_name)?;
        let mut ops = crabka_pgcatalog::move_relation_to_schema_ops(kv, table_name, &new_name)?;
        ops.extend(rename_relation_comment_ops(kv, table_name, &new_name)?);
        ops.extend(rename_name_keyed_metadata_ops(kv, table_name, &new_name)?);
        for mut trigger in crabka_pgcatalog::trigger::triggers_for_table(kv, table.id)? {
            trigger.table = new_name.clone();
            ops.extend(crabka_pgcatalog::trigger::put_trigger_ops(kv, &trigger)?);
        }
        return Ok((command("ALTER TABLE"), ops));
    }

    if let [action @ (Action::OfType(_) | Action::NotOfType)] = actions {
        let table = match crabka_pgcatalog::get_table(kv, table_name) {
            Ok(table) => table,
            Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) if if_exists => {
                return Ok((command("ALTER TABLE"), Vec::new()));
            }
            Err(error) => return Err(error.into()),
        };
        crate::privilege::require_ownership(
            kv,
            table_name,
            &table.owner,
            crate::privilege::RelationKind::Table,
            fctx.effective_role(),
        )?;
        if !crate::inheritance::parents_of(kv, table_name)?.is_empty() {
            return Err(ExecError::WrongObjectType(
                "typed tables cannot inherit".into(),
            ));
        }
        let op = match action {
            Action::OfType(reference) => {
                let type_name = resolve_user_type(kv, resolution, reference)?;
                let ty = crabka_pgcatalog::get_user_type(kv, &type_name)?.ok_or_else(|| {
                    ExecError::UndefinedObject(format!("type \"{type_name}\" does not exist"))
                })?;
                let fields = ty.fields().ok_or_else(|| {
                    ExecError::WrongObjectType(format!("type {type_name} is not a composite type"))
                })?;
                for (index, field) in fields.iter().enumerate() {
                    let Some(column) = table.columns.get(index) else {
                        return Err(ExecError::InvalidTableDefinition(format!(
                            "table is missing column \"{}\"",
                            field.name
                        )));
                    };
                    if column.name != field.name {
                        return Err(ExecError::InvalidTableDefinition(format!(
                            "table has column \"{}\" where type requires \"{}\"",
                            column.name, field.name
                        )));
                    }
                    if column.ty != field.ty {
                        return Err(ExecError::InvalidTableDefinition(format!(
                            "table \"{}\" has different type for column \"{}\"",
                            table_name.name, field.name
                        )));
                    }
                }
                if let Some(column) = table.columns.get(fields.len()) {
                    return Err(ExecError::InvalidTableDefinition(format!(
                        "table has extra column \"{}\"",
                        column.name
                    )));
                }
                crabka_pgcatalog::set_typed_table_type_op(table_name, ty.oid)
            }
            Action::NotOfType => crabka_pgcatalog::clear_typed_table_type_op(table_name),
            _ => unreachable!("only OF and NOT OF reach the typed-table path"),
        };
        return Ok((command("ALTER TABLE"), vec![op]));
    }

    if let [Action::RenameColumn { column, new_name }] = actions
        && let Ok(mut view) = crabka_pgcatalog::get_view(kv, table_name)
    {
        let index = view
            .columns
            .iter()
            .position(|candidate| candidate.name == *column)
            .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))?;
        crate::scope::reject_system_column_names([new_name.as_str()])?;
        if view
            .columns
            .iter()
            .any(|candidate| candidate.name == *new_name)
        {
            return Err(ExecError::DuplicateColumn {
                column: new_name.clone(),
                table: table_name.to_string(),
            });
        }
        view.columns[index].name = new_name.clone();
        return Ok((
            command("ALTER TABLE"),
            vec![crabka_pgcatalog::put_view_op(&view)],
        ));
    }

    let fetched = crabka_pgcatalog::get_table(kv, table_name);
    // A relation of any other kind is still a relation, so PostgreSQL reports
    // the *subcommand* as unsupported for it rather than claiming the relation
    // is missing — and IF EXISTS does not suppress that, because the relation
    // exists. A materialized view is stored as a table here and so arrives
    // through the `Ok` side; asking the fetched record for its kind is what
    // catches it without costing an ordinary table a second lookup.
    let kind = match &fetched {
        Ok(table) => Some(stored_relation_kind(table)),
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => relation_kind(kv, table_name),
        Err(_) => None,
    };
    if let Some(kind) = kind
        && kind != "table"
        && kind != "foreign table"
        && let Some(error) = alter_action_wrong_kind(table_name, actions, kind)
    {
        return Err(error);
    }
    if kind == Some("foreign table")
        && actions
            .iter()
            .any(|action| matches!(action, Action::AlterConstraint { .. }))
    {
        return Err(relkind_not_supported(
            format!(
                "ALTER action ALTER CONSTRAINT cannot be performed on relation \"{}\"",
                table_name.name
            ),
            "foreign table",
        ));
    }
    if kind == Some("foreign table")
        && let Some(kind) = actions.iter().find_map(|action| {
            let Action::AddConstraint(constraint) = action else {
                return None;
            };
            foreign_table_index_constraint_kind(&constraint.kind)
        })
    {
        return Err(ExecError::Unsupported(format!(
            "{kind} constraints are not supported on foreign tables"
        )));
    }
    if kind == Some("foreign table")
        && actions
            .iter()
            .any(|action| matches!(action, Action::SetType { using: Some(_), .. }))
    {
        return Err(ExecError::WrongObjectType(format!(
            "\"{}\" is not a table",
            table_name.name
        )));
    }
    if kind == Some("foreign table")
        && actions
            .iter()
            .any(|action| matches!(action, Action::SetType { .. }))
        && let Some(rowtype) = crate::catalog_rel::relation_rowtype(kv, table_name)?
        && let Some((dependent, column)) = crate::usertype::column_using_type(kv, rowtype.oid)?
    {
        return Err(ExecError::Unsupported(format!(
            "cannot alter foreign table \"{}\" because column \"{}.{}\" uses its row type",
            table_name.name, dependent.name, column
        )));
    }
    let table = match fetched {
        Ok(table) => table,
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) if if_exists => {
            return Ok((command("ALTER TABLE"), Vec::new()));
        }
        Err(error) => return Err(error.into()),
    };
    if actions
        .iter()
        .filter(|action| matches!(action, Action::SetAccessMethod(_)))
        .nth(1)
        .is_some()
    {
        return Err(ExecError::Syntax(
            "cannot have multiple SET ACCESS METHOD subcommands".into(),
        ));
    }
    reject_typed_table_alter(kv, &table, actions)?;
    validate_alter_constraint_columns(kv, resolution, &table, actions)?;
    if only {
        reject_only_that_would_skip_descendants(kv, &table, actions)?;
    }
    let columns_before: HashSet<String> = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let mut state = AlterTableState::new(table, fctx.own_xid);
    let mut ordered_actions = actions.iter().collect::<Vec<_>>();
    ordered_actions.sort_by_key(|action| alter_table_action_pass(action));
    for action in &ordered_actions {
        alter_table_action_ops(kv, &mut state, action, fctx)?;
    }

    // The schema record is written once, after every action has folded into the
    // working column/CHECK lists.
    let mut ops = alter_table_state_ops(kv, table_name, &mut state)?;

    // Every descendant repeats the column-shape subcommands against its own
    // catalog record, so a partition or inheritance child can never fall out of
    // step with the shape its parent presents. `ONLY` skipped this above, and
    // the guard it went through refused the spellings PostgreSQL will not let
    // stop here.
    let recursed = if only {
        Vec::new()
    } else {
        ordered_actions
            .iter()
            .copied()
            .filter(|action| {
                action_recurses_to_descendants(action)
                    && !skipped_by_existence_check(action, &columns_before)
            })
            .collect::<Vec<_>>()
    };
    if !recursed.is_empty() {
        for descendant in column_shape_descendants(kv, table_name)? {
            ops.extend(alter_descendant_ops(
                kv,
                &descendant,
                &state.table,
                &recursed,
                fctx,
            )?);
        }
    }
    Ok((command("ALTER TABLE"), ops))
}

fn reject_typed_table_alter(
    kv: &dyn Kv,
    table: &Table,
    actions: &[crabka_pgparser::ast::AlterTableAction],
) -> Result<(), ExecError> {
    if crabka_pgcatalog::typed_table_type(kv, &table.name)?.is_none() {
        return Ok(());
    }
    use crabka_pgparser::ast::AlterTableAction as Action;
    for action in actions {
        let message = match action {
            Action::AddColumn { .. } => "cannot add column to typed table",
            Action::DropColumn { .. } => "cannot drop column from typed table",
            Action::RenameColumn { .. } => "cannot rename column of typed table",
            Action::SetType { .. } => "cannot alter column type of typed table",
            Action::Inherit(_) | Action::NoInherit(_) => "cannot change inheritance of typed table",
            _ => continue,
        };
        return Err(ExecError::WrongObjectType(message.into()));
    }
    Ok(())
}

/// The catalog and row writes one relation's finished [`AlterTableState`] owes.
///
/// Shared by the named relation and by every descendant the statement recursed
/// into, so a child's schema record and rewritten rows are staged by exactly
/// the same code that stages the parent's.
pub(crate) fn alter_table_state_ops(
    kv: &dyn Kv,
    table_name: &crabka_pgcatalog::RelationName,
    state: &mut AlterTableState,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let mut ops = crabka_pgcatalog::replace_table_schema_ops(kv, table_name, &state.table)?;
    if let Some(rows) = state.rows.take() {
        for (key, xmin, xmax, cmin, cmax, row) in rows {
            ops.push(crabka_pgkv::WriteOp::Put {
                key,
                value: encode_table_tuple(&state.table, xmin, xmax, cmin, cmax, &row),
            });
        }
    }
    if let Some(parents) = state.inheritance_parents.take() {
        ops.extend(crate::inheritance::replace_parents_ops(
            kv, table_name, &parents,
        )?);
    }
    ops.append(&mut state.ops);
    Ok(ops)
}

/// The subcommands that reshape a relation's *columns*, which `PostgreSQL`
/// propagates down the inheritance and partition trees.
///
/// Constraint and storage subcommands are deliberately absent: they have their
/// own recursion rules, and applying them here would create a second copy of a
/// constraint on every child.
pub(crate) fn action_recurses_to_descendants(
    action: &crabka_pgparser::ast::AlterTableAction,
) -> bool {
    use crabka_pgparser::ast::AlterTableAction as Action;

    matches!(
        action,
        Action::AddColumn { .. }
            | Action::DropColumn { .. }
            | Action::RenameColumn { .. }
            | Action::SetType { .. }
            | Action::SetNotNull(_)
            | Action::DropNotNull(_)
            | Action::SetDefault { .. }
            | Action::DropDefault(_)
            | Action::SetExpression { .. }
            | Action::DropExpression { .. }
    ) || added_not_null_column(action).is_some()
}

/// The column an `ADD [CONSTRAINT n] NOT NULL c` names, for the paths that
/// treat the subcommand as the column write it is.
pub(crate) fn added_not_null_column(
    action: &crabka_pgparser::ast::AlterTableAction,
) -> Option<&str> {
    let crabka_pgparser::ast::AlterTableAction::AddConstraint(constraint) = action else {
        return None;
    };
    match &constraint.kind {
        crabka_pgparser::ast::TableConstraintKind::NotNull { column, .. } => Some(column.as_str()),
        _ => None,
    }
}

/// Whether the named relation abandoned the subcommand on its own existence
/// check — `ADD COLUMN IF NOT EXISTS` for a column it already had, `DROP COLUMN
/// IF EXISTS` for one it never had.
///
/// `PostgreSQL` drops the whole subcommand at that point, descendants included.
/// Recursing anyway would let `ADD COLUMN IF NOT EXISTS` report a type conflict
/// against a child, for a statement PostgreSQL treats as a no-op.
pub(crate) fn skipped_by_existence_check(
    action: &crabka_pgparser::ast::AlterTableAction,
    columns_before: &HashSet<String>,
) -> bool {
    use crabka_pgparser::ast::AlterTableAction as Action;

    match action {
        Action::AddColumn {
            if_not_exists: true,
            column,
            ..
        } => columns_before.contains(&column.name),
        Action::DropColumn {
            column,
            if_exists: true,
            ..
        } => !columns_before.contains(column),
        _ => false,
    }
}

/// Every relation that takes its column shape from `parent`, however deep and
/// through whichever tree.
///
/// One walk over both link kinds rather than two walks over one each: a
/// partition can be declared `INHERITS`-style below an inheritance child, and a
/// tree mixing the two would otherwise be visited only down to the first link
/// of the other kind. The visited set makes the inheritance DAG — where a
/// diamond's foot is reachable by two paths — yield each relation once, so a
/// column is added to it once.
pub(crate) fn column_shape_descendants(
    kv: &dyn Kv,
    parent: &crabka_pgcatalog::RelationName,
) -> Result<Vec<crabka_pgcatalog::RelationName>, ExecError> {
    let mut out = Vec::new();
    let mut seen: HashSet<crabka_pgcatalog::RelationName> =
        std::iter::once(parent.clone()).collect();
    let mut pending = vec![parent.clone()];
    while let Some(name) = pending.pop() {
        for child in direct_children(kv, &name)? {
            if seen.insert(child.clone()) {
                out.push(child.clone());
                pending.push(child);
            }
        }
    }
    out.sort();
    Ok(out)
}

/// The relations one level below `name` in either tree.
pub(crate) fn direct_children(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Result<Vec<crabka_pgcatalog::RelationName>, ExecError> {
    let mut children = crate::partition::partitions_of(kv, name)?
        .into_iter()
        .map(|partition| partition.name)
        .collect::<Vec<_>>();
    children.extend(crate::inheritance::children_of(kv, name)?);
    Ok(children)
}

/// Refuse an `ALTER TABLE ONLY` whose subcommand `PostgreSQL` will not let stop
/// at the named relation.
///
/// The rule is per-subcommand and differs between the two trees. `ADD COLUMN`,
/// `RENAME COLUMN` and `ALTER COLUMN … TYPE` are refused whenever *any*
/// descendant exists, because the change would leave the children a different
/// shape from the parent that presents them. `DROP COLUMN` and `SET NOT NULL`
/// are refused only for a partitioned table: on an inheritance parent
/// PostgreSQL treats them as local changes, which is representable here.
pub(crate) fn reject_only_that_would_skip_descendants(
    kv: &dyn Kv,
    table: &Table,
    actions: &[crabka_pgparser::ast::AlterTableAction],
) -> Result<(), ExecError> {
    use crabka_pgparser::ast::AlterTableAction as Action;

    if !actions.iter().any(action_recurses_to_descendants)
        || direct_children(kv, &table.name)?.is_empty()
    {
        return Ok(());
    }
    let partitioned = crate::partition::scheme_of(kv, &table.name)?.is_some();
    let refuse = |message: String, hint: Option<&str>| {
        Err(ExecError::OnlyWouldSkipDescendants {
            message,
            hint: hint.map(ToString::to_string),
        })
    };
    for action in actions {
        match action {
            Action::AddColumn { .. } => {
                return refuse("column must be added to child tables too".into(), None);
            }
            Action::DropColumn { .. } if partitioned => {
                return refuse(
                    "cannot drop column from only the partitioned table when partitions exist"
                        .into(),
                    Some("Do not specify the ONLY keyword."),
                );
            }
            Action::RenameColumn { column, .. } => {
                return refuse(
                    format!("inherited column \"{column}\" must be renamed in child tables too"),
                    None,
                );
            }
            Action::SetType { column, .. } => {
                return refuse(
                    format!(
                        "type of inherited column \"{column}\" must be changed in child tables too"
                    ),
                    None,
                );
            }
            Action::SetNotNull(_) if partitioned => {
                return refuse(
                    "constraint must be added to child tables too".into(),
                    Some("Do not specify the ONLY keyword."),
                );
            }
            other if partitioned && added_not_null_column(other).is_some() => {
                return refuse(
                    "constraint must be added to child tables too".into(),
                    Some("Do not specify the ONLY keyword."),
                );
            }
            _ => {}
        }
    }
    Ok(())
}

/// Repeat the column-shape subcommands against one descendant.
///
/// `parent` is the *named* relation as this statement has already rewritten it,
/// not the descendant's immediate parent: an added column is copied from the
/// shape the statement produced, so a grandchild receives exactly the column
/// its root ancestor now has — same type, default, and NOT NULL — rather than a
/// second independent resolution of the same `ColumnDef`.
pub(crate) fn alter_descendant_ops(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
    parent: &Table,
    actions: &[&crabka_pgparser::ast::AlterTableAction],
    fctx: ForeignCtx<'_>,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let mut state = AlterTableState::new(crabka_pgcatalog::get_table(kv, name)?, fctx.own_xid);
    for action in actions {
        alter_descendant_action_ops(kv, &mut state, action, parent, fctx)?;
    }
    alter_table_state_ops(kv, name, &mut state)
}

/// One column-shape subcommand as a descendant sees it.
///
/// `ADD COLUMN` is the only one that cannot simply be replayed. Everything else
/// runs the parent's own arm, skipped when the descendant has no column of that
/// name — a `DROP COLUMN` that a `RENAME` already carried past it, say.
pub(crate) fn alter_descendant_action_ops(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    action: &crabka_pgparser::ast::AlterTableAction,
    parent: &Table,
    fctx: ForeignCtx<'_>,
) -> Result<(), ExecError> {
    use crabka_pgparser::ast::AlterTableAction as Action;

    let named_column = match action {
        Action::AddColumn { column, .. } => Some(column.name.as_str()),
        Action::DropColumn { column, .. }
        | Action::RenameColumn { column, .. }
        | Action::SetType { column, .. }
        | Action::SetDefault { column, .. }
        | Action::SetNotNull(column)
        | Action::DropNotNull(column)
        | Action::DropDefault(column)
        | Action::SetExpression { column, .. }
        | Action::DropExpression { column, .. } => Some(column.as_str()),
        other => added_not_null_column(other),
    };
    match action {
        Action::AddColumn { column, .. } => {
            inherit_column_ops(kv, state, &column.name, parent, fctx)
        }
        // A descendant that never had the column has nothing to do; the
        // statement is still the parent's, so it must not fail here.
        _ if named_column.is_some_and(|column| state.table.column_index(column).is_none()) => {
            Ok(())
        }
        Action::DropNotNull(column) => drop_column_not_null(kv, state, column, false),
        _ => alter_table_action_ops(kv, state, action, fctx),
    }
}

/// Apply `DROP NOT NULL`, allowing a recursive parent operation to carry its
/// new flag to descendants before that parent schema is committed.
fn drop_column_not_null(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    column: &str,
    check_parent: bool,
) -> Result<(), ExecError> {
    let index = state.column_index(column)?;
    if replica_identity_index_uses_column(kv, &state.table, column)? {
        return Err(ExecError::InvalidTableDefinition(format!(
            "column \"{column}\" is in index used as replica identity"
        )));
    }
    if check_parent && let Some((parent, _)) = crate::partition::parent_of(kv, &state.table.name)? {
        let parent = crabka_pgcatalog::get_table(kv, &parent)?;
        if parent
            .column_index(column)
            .is_some_and(|index| parent.columns[index].not_null)
        {
            return Err(ExecError::InvalidTableDefinition(format!(
                "column \"{column}\" is marked NOT NULL in parent table"
            )));
        }
    }
    state.table.columns[index].not_null = false;
    Ok(())
}

/// Give one descendant the column its ancestor just gained.
///
/// A descendant that already declares the name *merges* rather than gaining a
/// second column — that is how `PostgreSQL` reconciles a child written with the
/// column spelled out by hand — and merging is only possible when the two
/// declarations agree on type.
pub(crate) fn inherit_column_ops(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    column: &str,
    parent: &Table,
    fctx: ForeignCtx<'_>,
) -> Result<(), ExecError> {
    let Some(inherited) = parent
        .column_index(column)
        .map(|index| parent.columns[index].clone())
    else {
        // The parent's own pass must have added it; a column it does not have
        // is not one a descendant can inherit.
        return Ok(());
    };
    if let Some(index) = state.table.column_index(column) {
        if state.table.columns[index].ty != inherited.ty {
            return Err(ExecError::ChildColumnTypeMismatch {
                child: state.table.name.name.clone(),
                column: column.to_string(),
            });
        }
        return Ok(());
    }
    let fill = match &inherited.default {
        Some(ColumnDefault::Value(value)) => value.clone(),
        _ => Datum::Null,
    };
    let added = state.table.columns.len();
    let generated = inherited.generated.is_some();
    let not_null = inherited.not_null;
    let table_name = state.table.name.clone();
    for (_, _, _, _, _, row) in state.rows_mut(kv)? {
        row.push(fill.clone());
    }
    state.table.columns.push(inherited);
    let ddl_ctx = crate::clock::EvalCtx::for_ddl(fctx.resolution, fctx.catalog);
    if generated {
        validate_generation_expressions(&state.table)?;
        backfill_generated_column(kv, state, added, &ddl_ctx)?;
    }
    if not_null {
        for (_rowid, _xmin, row) in &state.live_rows(kv, &ddl_ctx)? {
            if row.get(added).is_none_or(Datum::is_null) {
                return Err(ExecError::ColumnContainsNullValues {
                    column: column.to_string(),
                    table: table_name.to_string(),
                });
            }
        }
    }
    Ok(())
}

/// The phase-2 pass order PostgreSQL assigns to the ALTER TABLE subcommands
/// crabka supports. Written order is preserved within a pass.
pub(crate) fn alter_table_action_pass(action: &crabka_pgparser::ast::AlterTableAction) -> u8 {
    use crabka_pgparser::ast::AlterTableAction as Action;

    match action {
        Action::DropColumn { .. }
        | Action::DropNotNull(_)
        | Action::DropDefault(_)
        | Action::DropExpression { .. }
        | Action::DropConstraint { .. } => 0,
        Action::SetType { .. } => 1,
        Action::AddColumn { .. } => 2,
        // PostgreSQL first examines ADD CONSTRAINT, then queues its actual
        // work after the column-attribute pass. Crabka executes it directly,
        // so order the effective work rather than the examination pass.
        Action::SetNotNull(_) => 3,
        // `ADD [CONSTRAINT n] NOT NULL c` writes the same flag as
        // `ALTER COLUMN c SET NOT NULL`, so it shares that pass: a primary key
        // added in the same statement must see the column already not-null.
        _ if added_not_null_column(action).is_some() => 3,
        Action::AddConstraint(constraint)
            if matches!(
                constraint.kind,
                crabka_pgparser::ast::TableConstraintKind::PrimaryKey { .. }
                    | crabka_pgparser::ast::TableConstraintKind::Unique { .. }
                    | crabka_pgparser::ast::TableConstraintKind::Exclude { .. }
            ) =>
        {
            4
        }
        // `SET EXPRESSION` lands with the other column-attribute writes, after
        // `ADD COLUMN`: one statement may add a generated column and reword its
        // expression in the same breath.
        Action::AddConstraint(_)
        | Action::SetDefault { .. }
        | Action::SetStatistics { .. }
        | Action::SetStorage { .. }
        | Action::SetAttributeOptions { .. }
        | Action::AlterForeignColumnOptions { .. }
        | Action::AlterForeignTableOptions { .. }
        | Action::SetExpression { .. } => 5,
        Action::SetSchema(_) => unreachable!("SET SCHEMA is handled before ALTER TABLE passes"),
        Action::OfType(_) | Action::NotOfType => {
            unreachable!("OF and NOT OF are handled before ALTER TABLE passes")
        }
        Action::Inherit(_) | Action::NoInherit(_) => 6,
        Action::RenameTable { .. }
        | Action::RenameColumn { .. }
        | Action::RenameConstraint { .. }
        | Action::AlterConstraint { .. }
        | Action::ValidateConstraint(_)
        | Action::SetStorageParameters(_)
        | Action::ResetStorageParameters(_)
        | Action::SetTablespace(_)
        | Action::SetAccessMethod(_)
        | Action::OwnerTo(_)
        | Action::SetTriggerMode { .. }
        | Action::SetRuleMode { .. }
        | Action::EnableRowSecurity
        | Action::DisableRowSecurity
        | Action::ForceRowSecurity
        | Action::NoForceRowSecurity
        | Action::AttachPartition { .. }
        | Action::DetachPartition { .. }
        | Action::ClusterOn(_)
        | Action::SetWithoutCluster
        | Action::SetWithoutOids
        | Action::SetReplicaIdentity(_)
        | Action::Unsupported(_) => 6,
    }
}

/// How `PostgreSQL` names an `ALTER TABLE` subcommand in the 42809 it raises
/// when the relation's kind does not support it.
pub(crate) fn alter_action_label(action: &crabka_pgparser::ast::AlterTableAction) -> &'static str {
    use crabka_pgparser::ast::AlterTableAction as Action;

    match action {
        Action::AddColumn { .. } => "ADD COLUMN",
        Action::DropColumn { .. } => "DROP COLUMN",
        Action::SetType { .. } => "ALTER COLUMN ... SET DATA TYPE",
        Action::SetNotNull(_) => "ALTER COLUMN ... SET NOT NULL",
        Action::DropNotNull(_) => "ALTER COLUMN ... DROP NOT NULL",
        Action::SetDefault { .. } => "ALTER COLUMN ... SET DEFAULT",
        Action::SetStatistics { .. } => "ALTER COLUMN ... SET STATISTICS",
        Action::SetStorage { .. } => "ALTER COLUMN ... SET STORAGE",
        Action::SetAttributeOptions { .. } => "ALTER COLUMN ... SET",
        Action::DropDefault(_) => "ALTER COLUMN ... DROP DEFAULT",
        Action::AlterForeignColumnOptions { .. } => "ALTER COLUMN ... OPTIONS",
        Action::AlterForeignTableOptions { .. } => "OPTIONS",
        Action::SetExpression { .. } => "ALTER COLUMN ... SET EXPRESSION",
        Action::DropExpression { .. } => "ALTER COLUMN ... DROP EXPRESSION",
        Action::AddConstraint(_) => "ADD CONSTRAINT",
        Action::AlterConstraint { .. } => "ALTER CONSTRAINT",
        Action::DropConstraint { .. } => "DROP CONSTRAINT",
        Action::ValidateConstraint(_) => "VALIDATE CONSTRAINT",
        Action::RenameColumn { .. } => "RENAME COLUMN",
        Action::RenameConstraint { .. } => "RENAME CONSTRAINT",
        Action::RenameTable { .. } => "RENAME",
        Action::SetStorageParameters(_) => "SET",
        Action::ResetStorageParameters(_) => "RESET",
        Action::SetTablespace(_) => "SET TABLESPACE",
        Action::SetSchema(_) => unreachable!("SET SCHEMA is handled before ALTER TABLE passes"),
        Action::OfType(_) | Action::NotOfType => {
            unreachable!("OF and NOT OF are handled before ALTER TABLE passes")
        }
        Action::Inherit(_) => "INHERIT",
        Action::NoInherit(_) => "NO INHERIT",
        Action::SetAccessMethod(_) => "SET ACCESS METHOD",
        Action::OwnerTo(_) => "OWNER TO",
        // `PostgreSQL` spells the exact form back, so the selector and the mode
        // both show: `ENABLE TRIGGER ALL`, `DISABLE TRIGGER` for a named one,
        // and `ENABLE REPLICA TRIGGER` / `ENABLE ALWAYS TRIGGER` for the two
        // session-replication modes, which never name a selector suffix.
        Action::SetTriggerMode { selector, mode } => {
            use crabka_pgparser::ast::{TriggerEnableMode, TriggerSelector};

            match (mode, selector) {
                (TriggerEnableMode::Replica, _) => "ENABLE REPLICA TRIGGER",
                (TriggerEnableMode::Always, _) => "ENABLE ALWAYS TRIGGER",
                (TriggerEnableMode::Origin, TriggerSelector::All) => "ENABLE TRIGGER ALL",
                (TriggerEnableMode::Origin, TriggerSelector::User) => "ENABLE TRIGGER USER",
                (TriggerEnableMode::Origin, TriggerSelector::Named(_)) => "ENABLE TRIGGER",
                (TriggerEnableMode::Disabled, TriggerSelector::All) => "DISABLE TRIGGER ALL",
                (TriggerEnableMode::Disabled, TriggerSelector::User) => "DISABLE TRIGGER USER",
                (TriggerEnableMode::Disabled, TriggerSelector::Named(_)) => "DISABLE TRIGGER",
            }
        }
        Action::SetRuleMode { mode, .. } => {
            use crabka_pgparser::ast::TriggerEnableMode;

            match mode {
                TriggerEnableMode::Origin => "ENABLE RULE",
                TriggerEnableMode::Replica => "ENABLE REPLICA RULE",
                TriggerEnableMode::Always => "ENABLE ALWAYS RULE",
                TriggerEnableMode::Disabled => "DISABLE RULE",
            }
        }
        // The statement is spelled `ROW LEVEL SECURITY`; the diagnostic drops
        // the `LEVEL`, which is how `PostgreSQL` names these four subcommands.
        Action::EnableRowSecurity => "ENABLE ROW SECURITY",
        Action::DisableRowSecurity => "DISABLE ROW SECURITY",
        Action::ForceRowSecurity => "FORCE ROW SECURITY",
        Action::NoForceRowSecurity => "NO FORCE ROW SECURITY",
        Action::AttachPartition { .. } => "ATTACH PARTITION",
        Action::DetachPartition { .. } => "DETACH PARTITION",
        Action::ClusterOn(_) => "CLUSTER ON",
        Action::SetWithoutCluster => "SET WITHOUT CLUSTER",
        Action::SetWithoutOids => "SET WITHOUT OIDS",
        Action::SetReplicaIdentity(_) => "REPLICA IDENTITY",
        Action::Unsupported(_) => "ALTER",
    }
}

/// Whether `PostgreSQL` lets an `ALTER TABLE` subcommand run against a relation
/// of kind `kind`, for the kinds that are not a table.
///
/// This is `PostgreSQL`'s `ATSimplePermissions` mask, one subcommand at a time:
/// each declares the relation kinds it accepts, and a name of any other kind is
/// refused before the subcommand does any work. There is no rule behind the
/// shape of it — `SET DEFAULT` works on a view and not on a materialized view,
/// `SET TABLESPACE` the other way round — so every cell here was measured
/// against `PostgreSQL` 18.4 rather than reasoned about.
///
/// A relation kind that can *hold* the subcommand still has to be one this
/// engine can carry it out on; this answers only whether `PostgreSQL` refuses
/// on kind alone.
pub(crate) fn alter_action_allows(
    action: &crabka_pgparser::ast::AlterTableAction,
    kind: &str,
) -> bool {
    use crabka_pgparser::ast::AlterTableAction as Action;

    match action {
        Action::AlterForeignColumnOptions { .. } | Action::AlterForeignTableOptions { .. } => {
            kind == "foreign table"
        }
        // Every kind can be handed to another role.
        Action::OwnerTo(_) => true,
        // A view's columns take defaults, which is what an INSTEAD OF trigger
        // and an auto-updatable view both read.
        Action::SetDefault { .. } | Action::DropDefault(_) => kind == "view",
        // Renaming a column, and the storage-parameter pair, reach everything
        // with named columns or reloptions — every kind but a sequence.
        Action::RenameColumn { .. }
        | Action::RenameConstraint { .. }
        | Action::SetStorageParameters(_)
        | Action::ResetStorageParameters(_) => kind != "sequence",
        // Only the two kinds with storage of their own can be moved.
        Action::SetTablespace(_) => kind == "index" || kind == "materialized view",
        Action::SetAccessMethod(_) => kind == "table" || kind == "materialized view",
        // A materialized view has a heap and can carry a clustered index; the
        // index it then names is checked after this.
        Action::ClusterOn(_) | Action::SetWithoutCluster => kind == "materialized view",
        // A subcommand this engine's grammar did not recognize cannot be
        // looked up in the mask at all, so it is passed through to the
        // unsupported-subcommand refusal that already names it. Claiming a kind
        // rules it out would put a confident wrong answer in front of an honest
        // one: `ALTER MATERIALIZED VIEW … SET SCHEMA` parses this way, and
        // PostgreSQL performs it.
        Action::Unsupported(_) => true,
        // Everything else is a table-only subcommand.
        _ => false,
    }
}

/// The refusal `ALTER TABLE` owes when `name` is a relation of a kind some
/// subcommand does not accept, or `None` when every subcommand accepts it.
///
/// `PostgreSQL` prepares the subcommands in the order they were written and
/// stops at the first one the kind rules out, so a list whose first entry is
/// fine and whose second is not reports the second.
///
/// Renaming is worded differently from the rest: it comes from the rename path
/// rather than the subcommand table, and says `cannot rename columns of
/// relation "x"` instead of naming an action at all.
pub(crate) fn alter_action_wrong_kind(
    name: &crabka_pgcatalog::RelationName,
    actions: &[crabka_pgparser::ast::AlterTableAction],
    kind: &str,
) -> Option<ExecError> {
    use crabka_pgparser::ast::AlterTableAction as Action;

    let refused = actions
        .iter()
        .find(|action| !alter_action_allows(action, kind))?;
    let message = if matches!(
        refused,
        Action::RenameColumn { .. } | Action::RenameConstraint { .. }
    ) {
        format!("cannot rename columns of relation \"{}\"", name.name)
    } else {
        format!(
            "ALTER action {} cannot be performed on relation \"{}\"",
            alter_action_label(refused),
            name.name
        )
    };
    Some(relkind_not_supported(message, kind))
}

/// The name a parsed [`RoleSpec`] denotes for this session.
///
/// This is the one place a session concept meets a role position. The parser
/// cannot do it — it has no session — and the catalog must not, because
/// `CURRENT_USER` is not a fact about stored records. `PUBLIC` resolves to its
/// own name and is refused, or not, by whichever caller knows whether a
/// pseudo-role is admissible there.
///
/// `CURRENT_ROLE` is `PostgreSQL`'s synonym for `CURRENT_USER`, not a third
/// role.
///
/// [`RoleSpec`]: crabka_pgparser::ast::RoleSpec
pub(crate) fn role_spec_name<'a>(
    spec: &'a crabka_pgparser::ast::RoleSpec,
    fctx: ForeignCtx<'a>,
) -> &'a str {
    use crabka_pgparser::ast::RoleSpec;
    // A session carrying `PUBLIC` authenticated as nobody and acts as the
    // bootstrap superuser, so both keyword spellings answer with that rather
    // than with a pseudo-role that owns nothing. This is
    // [`ForeignCtx::effective_role`]'s rule, restated for a borrow that has to
    // outlive the context value.
    let held = |name: &'a str| {
        if name == crabka_pgcatalog::PUBLIC_ROLE {
            crabka_pgcatalog::BOOTSTRAP_ROLE
        } else {
            name
        }
    };
    match spec {
        RoleSpec::Name(name) => name,
        RoleSpec::CurrentUser | RoleSpec::CurrentRole => held(fctx.current_user),
        RoleSpec::SessionUser => held(fctx.session_user),
        RoleSpec::Public => crabka_pgcatalog::PUBLIC_ROLE,
    }
}

/// Which way a table privilege statement moves the grants it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrivilegeGrant {
    Grant,
    Revoke,
}

/// The writes one `GRANT`/`REVOKE … ON <relation>` owes, relation-wide entries
/// and column-scoped entries alike.
///
/// The column list hangs off each privilege and not off the statement, so
/// `GRANT SELECT (a), UPDATE ON t TO r` is one column grant and one
/// relation-wide grant, and this walks the list in written order rather than
/// deciding once for the statement.
///
/// A column-scoped entry is *stored*, in its own catalog namespace, and not
/// dropped on the floor. What it does not yet do is admit a read: the read path
/// still asks for the relation-wide `SELECT`, so a role holding only column
/// grants is refused the whole relation where `PostgreSQL` would let it read
/// the granted columns. That is narrower than `PostgreSQL`, which is the safe
/// direction to be wrong in, and it is pinned by a test so that widening it is
/// a deliberate act. Accepting the statement and recording nothing would have
/// been the unsafe direction — `information_schema.column_privileges` and
/// `pg_attribute.attacl` would have answered that no grant existed on a
/// database where one had been made.
///
/// # Errors
///
/// Returns 0LP01 for a privilege `PostgreSQL` does not allow on a column,
/// 42703 for a column the relation does not have, or storage/corruption errors
/// from the catalog KV seam.
pub(crate) fn privilege_grant_ops(
    kv: &dyn Kv,
    relation: &crabka_pgcatalog::RelationName,
    grantees: &[String],
    privileges: &[crabka_pgparser::ast::PrivilegeSpec],
    direction: PrivilegeGrant,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let mut ops = Vec::new();
    for spec in privileges {
        let named = [spec.name.clone()];
        if spec.columns.is_empty() {
            ops.extend(match direction {
                PrivilegeGrant::Grant => {
                    crabka_pgcatalog::grant_table_privileges_ops(kv, relation, grantees, &named)?
                }
                PrivilegeGrant::Revoke => {
                    crabka_pgcatalog::revoke_table_privileges_ops(kv, relation, grantees, &named)?
                }
            });
            continue;
        }
        require_column_grantable(&spec.name)?;
        let columns = resolve_granted_columns(kv, relation, &spec.columns)?;
        ops.extend(match direction {
            PrivilegeGrant::Grant => crabka_pgcatalog::grant_column_privileges_ops(
                kv, relation, &columns, grantees, &named,
            )?,
            PrivilegeGrant::Revoke => crabka_pgcatalog::revoke_column_privileges_ops(
                kv, relation, &columns, grantees, &named,
            )?,
        });
    }
    Ok(ops)
}

/// `PostgreSQL`'s `ACL_ALL_RIGHTS_COLUMN` check: four of the eight relation
/// privileges can be granted on a column, and naming any of the other four
/// with a column list is 0LP01 rather than a grant on the whole relation.
pub(crate) fn require_column_grantable(privilege: &str) -> Result<(), ExecError> {
    let named = privilege.to_ascii_uppercase();
    if named == "ALL"
        || named == "ALL PRIVILEGES"
        || crabka_pgcatalog::COLUMN_PRIVILEGES.contains(&named.as_str())
    {
        return Ok(());
    }
    Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
        "0LP01",
        format!("invalid privilege type {named} for column"),
    )))
}

/// The columns a column-scoped grant names, checked against the relation.
///
/// A synthesised catalog relation is grantable, so its column list has to be
/// reachable here too — `GRANT SELECT (prosrc) ON pg_proc` is a statement
/// `pg_dump` writes and the upstream `init_privs` test runs.
pub(crate) fn resolve_granted_columns(
    kv: &dyn Kv,
    relation: &crabka_pgcatalog::RelationName,
    written: &[String],
) -> Result<Vec<String>, ExecError> {
    let held = grantable_relation_columns(kv, relation)?;
    for column in written {
        if !held.iter().any(|name| name == column) {
            return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                "42703",
                format!(
                    "column \"{column}\" of relation \"{}\" does not exist",
                    relation.name
                ),
            )));
        }
    }
    Ok(written.to_vec())
}

/// Every column name a grantable relation has, stored or synthesised.
pub(crate) fn grantable_relation_columns(
    kv: &dyn Kv,
    relation: &crabka_pgcatalog::RelationName,
) -> Result<Vec<String>, ExecError> {
    if let Some(key) = virtual_table(&virtual_lookup_key(relation)) {
        return Ok(virtual_catalog_columns(key)
            .into_iter()
            .map(|column| column.name)
            .collect());
    }
    match crabka_pgcatalog::get_table(kv, relation) {
        Ok(table) => Ok(table
            .columns
            .into_iter()
            .map(|column| column.name)
            .collect()),
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => {
            Ok(crabka_pgcatalog::get_view(kv, relation)?
                .columns
                .into_iter()
                .map(|column| column.name)
                .collect())
        }
        Err(error) => Err(error.into()),
    }
}

/// `PostgreSQL`'s refusal of a role name nobody holds.
///
/// The catalog seam calls this an undefined *object*, because it answers for
/// every kind of record it stores. In a role position `PostgreSQL` says `role`,
/// and the regression corpus compares the sentence.
pub(crate) fn undefined_role(role: &str) -> ExecError {
    ExecError::UndefinedObject(format!("role \"{role}\" does not exist"))
}

/// Every relation a `GRANT`/`REVOKE` of privileges names, resolved and checked
/// before any grantee is.
///
/// One statement naming several relations is several grants, each carrying the
/// whole privilege set, and none of them is written unless all the names are
/// good. The whole list also comes before the grantee list, which is the order
/// `PostgreSQL` reports the two in: `GRANT … ON t, nosuchtable TO nosuchrole`
/// names the relation, not the role.
///
/// # Errors
///
/// Returns 42P01 for a relation that does not exist, 42809 for one that holds
/// no privileges to grant, or storage/corruption errors from the catalog KV
/// seam.
pub(crate) fn resolve_grantable_relations(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    tables: &[crabka_pgparser::ast::RelationRef],
) -> Result<Vec<crabka_pgcatalog::RelationName>, ExecError> {
    tables
        .iter()
        .map(|table| {
            let name = resolve_relation(kv, resolution, table, SchemaDisposition::Utility)?;
            require_grantable_relation(kv, &name)?;
            Ok(name)
        })
        .collect()
}

/// The grantees a `GRANT`/`REVOKE` of privileges names.
///
/// `PUBLIC` is a grantee here — it is the one role every session holds — so it
/// passes through as its own name and the catalog stores the grant under it.
///
/// # Errors
///
/// Returns 42704 for a name no role holds, or storage/corruption errors from
/// the catalog KV seam.
pub(crate) fn resolve_grantees(
    kv: &dyn Kv,
    fctx: ForeignCtx<'_>,
    grantees: &[crabka_pgparser::ast::RoleSpec],
) -> Result<Vec<String>, ExecError> {
    grantees
        .iter()
        .map(|spec| {
            let role = role_spec_name(spec, fctx);
            if crabka_pgcatalog::role_is_nameable(kv, role)? {
                Ok(role.to_string())
            } else {
                Err(undefined_role(role))
            }
        })
        .collect()
}

fn foreign_privilege_target(
    target: crabka_pgparser::ast::ForeignPrivilegeTarget,
) -> crabka_pgcatalog::ForeignPrivilegeTarget {
    match target {
        crabka_pgparser::ast::ForeignPrivilegeTarget::DataWrapper => {
            crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper
        }
        crabka_pgparser::ast::ForeignPrivilegeTarget::Server => {
            crabka_pgcatalog::ForeignPrivilegeTarget::Server
        }
    }
}

fn foreign_privilege_names(
    privileges: &[crabka_pgparser::ast::PrivilegeSpec],
) -> Result<Vec<String>, ExecError> {
    if let Some(spec) = privileges.iter().find(|spec| !spec.columns.is_empty()) {
        return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
            "0LP01",
            format!("invalid privilege type {} for foreign object", spec.name),
        )));
    }
    Ok(privileges.iter().map(|spec| spec.name.clone()).collect())
}

fn require_foreign_ownership(
    kv: &dyn Kv,
    target: crabka_pgcatalog::ForeignPrivilegeTarget,
    names: &[String],
    fctx: ForeignCtx<'_>,
) -> Result<(), ExecError> {
    for name in names {
        let owner = match target {
            crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper => {
                foreign_object_lookup(target, name, crabka_pgcatalog::get_fdw(kv, name))?.owner
            }
            crabka_pgcatalog::ForeignPrivilegeTarget::Server => {
                foreign_object_lookup(target, name, crabka_pgcatalog::get_server(kv, name))?.owner
            }
        };
        let kind = match target {
            crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper => "foreign-data wrapper",
            crabka_pgcatalog::ForeignPrivilegeTarget::Server => "foreign server",
        };
        if !crabka_pgcatalog::role_has_privs_of(kv, fctx.effective_role(), &owner)?
            && !crate::rls::role_is_superuser(kv, fctx.effective_role())?
        {
            return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                "42501",
                format!("must be owner of {kind} {name}"),
            )));
        }
    }
    Ok(())
}

/// `CREATE FOREIGN DATA WRAPPER` installs arbitrary code hooks, so PostgreSQL
/// reserves it for superusers even when the wrapper has no handler yet.
fn require_fdw_create_superuser(
    kv: &dyn Kv,
    name: &str,
    fctx: ForeignCtx<'_>,
) -> Result<(), ExecError> {
    if crate::rls::role_is_superuser(kv, fctx.effective_role())? {
        return Ok(());
    }
    Err(ExecError::Remote(
        crabka_pgwire::error::PgError::error(
            "42501",
            format!("permission denied to create foreign-data wrapper \"{name}\""),
        )
        .with_hint("Must be superuser to create a foreign-data wrapper."),
    ))
}

fn require_fdw_owner_superuser(kv: &dyn Kv, name: &str, owner: &str) -> Result<(), ExecError> {
    if crate::rls::role_is_superuser(kv, owner)? {
        return Ok(());
    }
    Err(ExecError::Remote(
        crabka_pgwire::error::PgError::error(
            "42501",
            format!("permission denied to change owner of foreign-data wrapper \"{name}\""),
        )
        .with_hint("Must be superuser to change owner of a foreign-data wrapper."),
    ))
}

fn require_fdw_alter_superuser(
    kv: &dyn Kv,
    name: &str,
    fctx: ForeignCtx<'_>,
) -> Result<(), ExecError> {
    if crate::rls::role_is_superuser(kv, fctx.effective_role())? {
        return Ok(());
    }
    Err(ExecError::Remote(
        crabka_pgwire::error::PgError::error(
            "42501",
            format!("permission denied to alter foreign-data wrapper \"{name}\""),
        )
        .with_hint("Must be superuser to alter a foreign-data wrapper."),
    ))
}

fn require_new_owner_role(kv: &dyn Kv, fctx: ForeignCtx<'_>, owner: &str) -> Result<(), ExecError> {
    if crate::rls::role_is_superuser(kv, fctx.effective_role())?
        || crabka_pgcatalog::role_can_set(kv, fctx.effective_role(), owner)?
    {
        return Ok(());
    }
    Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
        "42501",
        format!("must be able to SET ROLE \"{owner}\""),
    )))
}

fn require_foreign_grant_authority(
    kv: &dyn Kv,
    target: crabka_pgcatalog::ForeignPrivilegeTarget,
    names: &[String],
    fctx: ForeignCtx<'_>,
) -> Result<(), ExecError> {
    for name in names {
        if crate::catalog_fn::foreign_usage_grant_option_is_held(
            kv,
            target,
            name,
            fctx.effective_role(),
        )? {
            continue;
        }
        let kind = match target {
            crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper => "foreign-data wrapper",
            crabka_pgcatalog::ForeignPrivilegeTarget::Server => "foreign server",
        };
        return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
            "42501",
            format!("permission denied for {kind} {name}"),
        )));
    }
    Ok(())
}

fn require_user_mapping_authority(
    kv: &dyn Kv,
    mapped_user: &str,
    server: &str,
    fctx: ForeignCtx<'_>,
) -> Result<(), ExecError> {
    let owner = foreign_object_lookup(
        crabka_pgcatalog::ForeignPrivilegeTarget::Server,
        server,
        crabka_pgcatalog::get_server(kv, server),
    )?
    .owner;
    let role = fctx.effective_role();
    if crate::rls::role_is_superuser(kv, role)?
        || crabka_pgcatalog::role_has_privs_of(kv, role, &owner)?
    {
        return Ok(());
    }
    if mapped_user == role {
        return require_foreign_usage(
            kv,
            crabka_pgcatalog::ForeignPrivilegeTarget::Server,
            server,
            fctx,
        );
    }
    Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
        "42501",
        format!("must be owner of foreign server {server}"),
    )))
}

fn require_foreign_usage(
    kv: &dyn Kv,
    target: crabka_pgcatalog::ForeignPrivilegeTarget,
    name: &str,
    fctx: ForeignCtx<'_>,
) -> Result<(), ExecError> {
    require_foreign_usage_for_role(kv, target, name, fctx.effective_role())
}

fn require_foreign_usage_for_role(
    kv: &dyn Kv,
    target: crabka_pgcatalog::ForeignPrivilegeTarget,
    name: &str,
    role: &str,
) -> Result<(), ExecError> {
    if crate::catalog_fn::foreign_usage_is_held(kv, target, name, role)
        .map_err(|error| foreign_usage_object_error(target, name, error))?
    {
        return Ok(());
    }
    let kind = match target {
        crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper => "foreign-data wrapper",
        crabka_pgcatalog::ForeignPrivilegeTarget::Server => "foreign server",
    };
    Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
        "42501",
        format!("permission denied for {kind} {name}"),
    )))
}

fn foreign_usage_object_error(
    target: crabka_pgcatalog::ForeignPrivilegeTarget,
    name: &str,
    error: ExecError,
) -> ExecError {
    if matches!(
        error,
        ExecError::Catalog(crabka_pgcatalog::CatalogError::UndefinedObject(_))
    ) {
        return foreign_object_missing(target, name);
    }
    error
}

fn foreign_object_lookup<T>(
    target: crabka_pgcatalog::ForeignPrivilegeTarget,
    name: &str,
    result: Result<T, crabka_pgcatalog::CatalogError>,
) -> Result<T, ExecError> {
    result.map_err(|error| match error {
        crabka_pgcatalog::CatalogError::UndefinedObject(_) => foreign_object_missing(target, name),
        error => error.into(),
    })
}

fn foreign_object_missing(
    target: crabka_pgcatalog::ForeignPrivilegeTarget,
    name: &str,
) -> ExecError {
    let kind = match target {
        crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper => "foreign-data wrapper",
        crabka_pgcatalog::ForeignPrivilegeTarget::Server => "server",
    };
    ExecError::Remote(crabka_pgwire::error::PgError::error(
        "42704",
        format!("{kind} \"{name}\" does not exist"),
    ))
}

fn ignore_foreign_duplicate<T>(
    result: Result<T, crabka_pgcatalog::CatalogError>,
    target: crabka_pgcatalog::ForeignPrivilegeTarget,
    name: &str,
    if_not_exists: bool,
) -> Result<Option<T>, ExecError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(crabka_pgcatalog::CatalogError::DuplicateObject(_)) if if_not_exists => Ok(None),
        Err(crabka_pgcatalog::CatalogError::DuplicateObject(_)) => {
            let kind = match target {
                crabka_pgcatalog::ForeignPrivilegeTarget::DataWrapper => "foreign-data wrapper",
                crabka_pgcatalog::ForeignPrivilegeTarget::Server => "server",
            };
            Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                "42710",
                format!("{kind} \"{name}\" already exists"),
            )))
        }
        Err(error) => Err(error.into()),
    }
}

/// The role an `OWNER TO` or `CREATE SCHEMA AUTHORIZATION` clause names,
/// validated the way `PostgreSQL` validates one.
///
/// Shared by every relation kind that can be handed over, so a view and a table
/// cannot disagree about who may receive one.
///
/// # Errors
///
/// Returns 42704 when the name is `PUBLIC` or belongs to no role, or
/// storage/corruption errors from the catalog KV seam.
pub(crate) fn resolve_new_owner(
    kv: &dyn Kv,
    fctx: ForeignCtx<'_>,
    spec: &crabka_pgparser::ast::RoleSpec,
) -> Result<String, ExecError> {
    let role = role_spec_name(spec, fctx);
    // `PUBLIC` is a pseudo-role with no `pg_authid` row, so PostgreSQL answers
    // a handover to it the same way it answers a handover to a name nobody
    // holds. Letting it through would leave a relation owned by something no
    // ownership test can ever match.
    if role == crabka_pgcatalog::PUBLIC_ROLE || !crabka_pgcatalog::role_is_nameable(kv, role)? {
        return Err(undefined_role(role));
    }
    Ok(role.to_string())
}

#[expect(
    clippy::too_many_lines,
    reason = "one arm per PostgreSQL ALTER TABLE subcommand; splitting them hides the \
              shared working-state contract"
)]
pub(crate) fn alter_table_action_ops(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    action: &crabka_pgparser::ast::AlterTableAction,
    fctx: ForeignCtx<'_>,
) -> Result<(), ExecError> {
    use crabka_pgparser::ast::AlterTableAction as Action;

    let resolution = fctx.resolution;
    let own_xid = fctx.own_xid;
    let catalog = fctx.catalog;
    let ddl_ctx = crate::clock::EvalCtx::for_ddl(resolution, catalog);
    let build = IndexBuild::new(&fctx, &ddl_ctx);
    let table_name = state.table.name.clone();
    match action {
        Action::AddColumn {
            if_not_exists,
            column,
            options,
        } => {
            // Before `IF NOT EXISTS`, which is where `PostgreSQL` puts it:
            // `check_for_column_name_collision` finds a system column by its
            // negative attnum and raises whatever the clause said, because the
            // name is taken by something `ADD COLUMN IF NOT EXISTS` cannot
            // decide it already added.
            crate::scope::reject_system_column_names([column.name.as_str()])?;
            if state.table.column_index(&column.name).is_some() {
                if *if_not_exists {
                    return Ok(());
                }
                return Err(ExecError::DuplicateColumn {
                    column: column.name.clone(),
                    table: table_name.to_string(),
                });
            }
            if !options.is_empty() && state.table.foreign.is_none() {
                return Err(ExecError::WrongObjectType(format!(
                    "relation \"{table_name}\" is not a foreign table"
                )));
            }
            let mut sequences = Vec::new();
            let declares_primary_key = column.constraints.iter().any(|c| {
                matches!(
                    c.kind,
                    crabka_pgparser::ast::ColumnConstraintKind::PrimaryKey
                )
            });
            let primary_key_columns = if declares_primary_key {
                HashSet::from([column.name.as_str()])
            } else {
                HashSet::new()
            };
            let catalog_column = column_from_ast(
                &table_name,
                column,
                &ddl_ctx,
                &mut sequences,
                &primary_key_columns,
            )?;
            let fill = match &catalog_column.default {
                Some(ColumnDefault::Value(value)) => value.clone(),
                _ => Datum::Null,
            };
            let added = state.table.columns.len();
            let generated = catalog_column.generated.is_some();
            let not_null = catalog_column.not_null;
            for (_, _, _, _, _, row) in state.rows_mut(kv)? {
                row.push(fill.clone());
            }
            state.table.columns.push(catalog_column);
            if !options.is_empty() {
                state
                    .table
                    .foreign
                    .as_mut()
                    .expect("foreign column options require a foreign table")
                    .column_options
                    .push((column.name.clone(), options.clone()));
            }
            // A generation expression added here gets exactly the DDL-time
            // analysis it gets inside CREATE TABLE, and PostgreSQL's table
            // rewrite then computes it for every row that already exists — an
            // added generated column never leaves the rows behind it NULL.
            if generated {
                validate_generation_expressions(&state.table)?;
                backfill_generated_column(kv, state, added, &ddl_ctx)?;
            }
            // NOT NULL is checked against the values the column will really
            // hold, so a generated column whose expression is non-NULL for
            // every existing row satisfies it.
            if not_null {
                for (_rowid, _xmin, row) in &state.live_rows(kv, &ddl_ctx)? {
                    if row.get(added).is_none_or(Datum::is_null) {
                        return Err(ExecError::ColumnContainsNullValues {
                            column: column.name.clone(),
                            table: table_name.to_string(),
                        });
                    }
                }
            }
            for (sequence_name, sequence) in sequences {
                state.ops.extend(crabka_pgcatalog::create_sequence_ops(
                    kv,
                    &sequence_name,
                    sequence,
                )?);
            }
            for constraint in &column.constraints {
                match &constraint.kind {
                    crabka_pgparser::ast::ColumnConstraintKind::Check(predicate) => {
                        add_check_constraint(
                            state,
                            constraint.name.clone(),
                            &predicate.text,
                            true,
                            constraint.attributes.no_inherit,
                            kv,
                            &ddl_ctx,
                        )?;
                    }
                    crabka_pgparser::ast::ColumnConstraintKind::PrimaryKey
                    | crabka_pgparser::ast::ColumnConstraintKind::Unique { .. } => {
                        let primary_key = matches!(
                            constraint.kind,
                            crabka_pgparser::ast::ColumnConstraintKind::PrimaryKey
                        );
                        add_constraint_index(
                            kv,
                            state,
                            &AddConstraintIndex {
                                name: constraint.name.as_deref(),
                                columns: std::slice::from_ref(&column.name),
                                primary_key,
                                without_overlaps: false,
                                deferral: constraint_deferral(constraint.attributes),
                            },
                            &build,
                        )?;
                    }
                    // `ADD COLUMN a int REFERENCES p (id)` is a one-column
                    // FOREIGN KEY on the column just added.
                    crabka_pgparser::ast::ColumnConstraintKind::References(reference) => {
                        add_foreign_key_constraint(
                            kv,
                            state,
                            &AddForeignKey {
                                name: constraint.name.as_deref(),
                                columns: std::slice::from_ref(&column.name),
                                reference,
                                attributes: constraint.attributes,
                            },
                            own_xid,
                            &ddl_ctx,
                        )?;
                    }
                    _ => {}
                }
            }
            Ok(())
        }
        Action::DropColumn {
            column,
            if_exists,
            cascade,
        } => {
            if state.table.column_index(column).is_none() {
                if *if_exists {
                    return Ok(());
                }
                return Err(ExecError::UndefinedTableColumn {
                    column: column.clone(),
                    table: table_name.to_string(),
                });
            }
            let dependents = dependent_view_names(kv, &table_name, Some(column))?;
            let generated = generated_columns_reading(&state.table, column);
            let trigger_dependents = triggers_referencing_column(kv, &state.table, column)?;
            if !dependents.is_empty() || !generated.is_empty() || !trigger_dependents.is_empty() {
                if !*cascade {
                    // PostgreSQL names each blocking object on its own DETAIL
                    // line, in the order `performDeletion` reports them, and
                    // ends with the CASCADE hint.
                    let detail = generated
                        .iter()
                        .map(|name| {
                            format!(
                                "column {name} of table {table_name} depends on \
                                 column {column} of table {table_name}"
                            )
                        })
                        .chain(dependents.iter().map(|view| {
                            format!("view {view} depends on column {column} of table {table_name}")
                        }))
                        .chain(trigger_dependents.iter().map(|trigger| {
                            format!(
                                "trigger {trigger} on table {table_name} depends on column {column} of table {table_name}"
                            )
                        }))
                        .collect::<Vec<_>>()
                        .join("\n");
                    return Err(ExecError::Remote(
                        crabka_pgwire::error::PgError::error(
                            "2BP01",
                            format!(
                                "cannot drop column {column} of table {table_name} because \
                                 other objects depend on it"
                            ),
                        )
                        .with_detail(detail)
                        .with_hint("Use DROP ... CASCADE to drop the dependent objects too."),
                    ));
                }
                for view in &dependents {
                    state.ops.extend(drop_view_with_triggers_ops(kv, view)?);
                }
                // CASCADE takes the dependent generated columns with it. They
                // are removed first, so `index` still addresses the target.
                for name in &generated {
                    drop_table_column(kv, state, name, *cascade)?;
                }
            }
            drop_table_column(kv, state, column, *cascade)
        }
        Action::SetNotNull(column) => set_column_not_null(kv, state, column, &ddl_ctx),
        Action::DropNotNull(column) => drop_column_not_null(kv, state, column, true),
        Action::Inherit(reference) => {
            let parent_name =
                resolve_relation(kv, resolution, reference, SchemaDisposition::Reference)?;
            let fetched = crabka_pgcatalog::get_table(kv, &parent_name);
            let kind = match &fetched {
                Ok(table) => Some(stored_relation_kind(table)),
                Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => {
                    relation_kind(kv, &parent_name)
                }
                Err(_) => None,
            };
            if let Some(kind) = kind
                && kind != "table"
                && kind != "foreign table"
            {
                return Err(inherit_wrong_kind(&parent_name, kind));
            }
            let parent = fetched?;
            if parent_name == table_name
                || crate::inheritance::descendants(kv, &table_name)?.contains(&parent_name)
            {
                return Err(ExecError::CircularInheritance);
            }
            for parent_column in &parent.columns {
                let Some(index) = state.table.column_index(&parent_column.name) else {
                    return Err(ExecError::ChildMissingColumn(parent_column.name.clone()));
                };
                let child_column = &state.table.columns[index];
                if child_column.ty != parent_column.ty {
                    return Err(ExecError::InvalidTableDefinition(format!(
                        "child table \"{}\" has different type for column \"{}\"",
                        table_name.name, parent_column.name
                    )));
                }
                if child_column.collation != parent_column.collation {
                    return Err(ExecError::ChildColumnCollationMismatch {
                        child: table_name.name.clone(),
                        column: parent_column.name.clone(),
                    });
                }
                if parent_column.not_null && !child_column.not_null {
                    return Err(ExecError::InvalidTableDefinition(format!(
                        "column \"{}\" in child table must be marked NOT NULL",
                        parent_column.name
                    )));
                }
            }
            for parent_check in parent.checks.iter().filter(|check| !check.no_inherit) {
                if !state
                    .table
                    .checks
                    .iter()
                    .any(|check| check.name == parent_check.name && check.expr == parent_check.expr)
                {
                    return Err(ExecError::InvalidObjectDefinition(format!(
                        "child table is missing constraint \"{}\"",
                        parent_check.name
                    )));
                }
            }
            let parents = state
                .inheritance_parents
                .get_or_insert(crate::inheritance::parents_of(kv, &table_name)?);
            if parents.contains(&parent_name) {
                return Err(ExecError::InvalidObjectDefinition(format!(
                    "relation \"{}\" would be inherited from more than once",
                    parent_name.name
                )));
            }
            parents.push(parent_name);
            Ok(())
        }
        Action::NoInherit(reference) => {
            let parent_name =
                resolve_relation(kv, resolution, reference, SchemaDisposition::Reference)?;
            let parents = state
                .inheritance_parents
                .get_or_insert(crate::inheritance::parents_of(kv, &table_name)?);
            let Some(index) = parents.iter().position(|parent| parent == &parent_name) else {
                return Err(ExecError::InvalidObjectDefinition(format!(
                    "relation \"{}\" is not a parent of relation \"{}\"",
                    parent_name.name, table_name.name
                )));
            };
            parents.remove(index);
            Ok(())
        }
        Action::SetDefault { column, expr } => {
            let index = state.column_index(column)?;
            let ty = state.table.columns[index].ty;
            state.table.columns[index].default = Some(default_from_expr(expr, ty, &ddl_ctx)?);
            Ok(())
        }
        Action::SetStatistics { column, target } => {
            let target = i16::try_from(*target).map_err(|_| {
                ExecError::InvalidParameterValueMessage(
                    "statistics target must be between -1 and 10000".into(),
                )
            })?;
            if !(-1..=10_000).contains(&target) {
                return Err(ExecError::InvalidParameterValueMessage(
                    "statistics target must be between -1 and 10000".into(),
                ));
            }
            let index = state.column_index(column)?;
            state.table.columns[index].statistics_target = target;
            Ok(())
        }
        Action::SetStorage { column, storage } => {
            let storage = match storage.to_ascii_lowercase().as_str() {
                "plain" => b'p',
                "external" => b'e',
                "extended" => b'x',
                "main" => b'm',
                _ => {
                    return Err(ExecError::InvalidParameterValueMessage(format!(
                        "invalid storage type \"{storage}\""
                    )));
                }
            };
            let index = state.column_index(column)?;
            state.table.columns[index].storage = Some(storage);
            Ok(())
        }
        Action::SetAttributeOptions { column, options } => {
            let index = state.column_index(column)?;
            for (name, value) in options {
                let value = value.as_ref().ok_or_else(|| {
                    ExecError::InvalidParameterValueMessage(format!(
                        "option \"{name}\" requires a value"
                    ))
                })?;
                if let Some((_, stored)) = state.table.columns[index]
                    .attribute_options
                    .iter_mut()
                    .find(|(stored, _)| stored == name)
                {
                    *stored = value.clone();
                } else {
                    state.table.columns[index]
                        .attribute_options
                        .push((name.clone(), value.clone()));
                }
            }
            Ok(())
        }
        Action::DropDefault(column) => {
            let index = state.column_index(column)?;
            state.table.columns[index].default = None;
            Ok(())
        }
        Action::AlterForeignColumnOptions { column, options } => {
            if crate::scope::is_system_column(column) {
                return Err(ExecError::Unsupported(format!(
                    "cannot alter system column \"{column}\""
                )));
            }
            state.column_index(column)?;
            let foreign = state.table.foreign.as_mut().ok_or_else(|| {
                ExecError::WrongObjectType(format!(
                    "relation \"{table_name}\" is not a foreign table"
                ))
            })?;
            let changes = foreign_option_mutations(options);
            if let Some(index) = foreign
                .column_options
                .iter()
                .position(|(name, _)| name == column)
            {
                let updated = crabka_pgcatalog::apply_foreign_option_mutations(
                    &foreign.column_options[index].1,
                    &changes,
                )?;
                if updated.is_empty() {
                    foreign.column_options.remove(index);
                } else {
                    foreign.column_options[index].1 = updated;
                }
            } else {
                let updated = crabka_pgcatalog::apply_foreign_option_mutations(&[], &changes)?;
                if !updated.is_empty() {
                    foreign.column_options.push((column.clone(), updated));
                }
            }
            Ok(())
        }
        Action::AlterForeignTableOptions { options } => {
            let foreign = state.table.foreign.as_mut().ok_or_else(|| {
                ExecError::WrongObjectType(format!(
                    "relation \"{table_name}\" is not a foreign table"
                ))
            })?;
            foreign.options = crabka_pgcatalog::apply_foreign_option_mutations(
                &foreign.options,
                &foreign_option_mutations(options),
            )?;
            Ok(())
        }
        Action::SetExpression { column, predicate } => {
            let index = state.column_index(column)?;
            let Some(kind) = state.table.columns[index]
                .generated
                .as_ref()
                .map(|g| g.kind)
            else {
                return Err(ExecError::NotAGeneratedColumn {
                    column: column.clone(),
                    table: table_name.to_string(),
                });
            };
            // A `CHECK` over a virtual column would have to be revalidated
            // against values that exist nowhere yet, so `PostgreSQL` refuses
            // the subcommand outright rather than half-checking it.
            if kind == crabka_pgcatalog::GeneratedKind::Virtual && !state.table.checks.is_empty() {
                return Err(ExecError::UnsupportedOnVirtualGenerated {
                    subcommand: crate::error::VirtualGeneratedSubcommand::SetExpressionWithChecks,
                    column: column.clone(),
                    table: table_name.to_string(),
                });
            }
            state.table.columns[index].generated = Some(crabka_pgcatalog::GeneratedColumn {
                expr: predicate.text.clone(),
                kind,
            });
            validate_generation_expressions(&state.table)?;
            // For a STORED column this rewrites the value every row holds. For
            // a VIRTUAL one it only fills the working set, which
            // `encode_table_tuple` blanks again on the way out — the rows are
            // untouched, and the next read is what produces the new value.
            backfill_generated_column(kv, state, index, &ddl_ctx)?;
            if state.table.columns[index].not_null {
                for (_rowid, _xmin, row) in &state.live_rows(kv, &ddl_ctx)? {
                    if row.get(index).is_none_or(Datum::is_null) {
                        return Err(ExecError::ColumnContainsNullValues {
                            column: column.clone(),
                            table: table_name.to_string(),
                        });
                    }
                }
            }
            Ok(())
        }
        Action::DropExpression { column, if_exists } => {
            let index = state.column_index(column)?;
            let Some(kind) = state.table.columns[index]
                .generated
                .as_ref()
                .map(|g| g.kind)
            else {
                // `IF EXISTS` is about the expression, not the column: a column
                // that carries none leaves the subcommand a no-op.
                if *if_exists {
                    return Ok(());
                }
                return Err(ExecError::NotAGeneratedColumn {
                    column: column.clone(),
                    table: table_name.to_string(),
                });
            };
            // Dropping the expression would have to leave the column holding
            // its last computed values, and a virtual column has never written
            // any down. `PostgreSQL` 18 refuses rather than materializing them.
            if kind == crabka_pgcatalog::GeneratedKind::Virtual {
                return Err(ExecError::UnsupportedOnVirtualGenerated {
                    subcommand: crate::error::VirtualGeneratedSubcommand::DropExpression,
                    column: column.clone(),
                    table: table_name.to_string(),
                });
            }
            // A stored column keeps every value it already computed and simply
            // stops being generated.
            state.table.columns[index].generated = None;
            Ok(())
        }
        Action::SetTablespace(tablespace) => {
            let oid = resolve_relation_tablespace_oid(kv, tablespace)?;
            state.ops.push(crabka_pgcatalog::set_relation_tablespace_op(
                &table_name,
                oid,
            ));
            Ok(())
        }
        Action::SetAccessMethod(method) => {
            if method.is_none() && crate::partition::scheme_of(kv, &table_name)?.is_some() {
                state
                    .ops
                    .push(crabka_pgcatalog::clear_relation_access_method_op(
                        &table_name,
                    ));
            } else {
                let oid = resolve_table_access_method_oid(
                    kv,
                    method
                        .as_deref()
                        .unwrap_or(fctx.default_table_access_method),
                )?;
                state
                    .ops
                    .push(crabka_pgcatalog::set_relation_access_method_op(
                        &table_name,
                        oid,
                    ));
            }
            Ok(())
        }
        Action::SetType {
            column,
            ty,
            collation,
            using,
        } => {
            let index = state.column_index(column)?;
            reject_partition_key_column(kv, &table_name, "alter", column)?;
            // The written `COLLATE` is checked against the *new* type, before
            // any rows are rewritten: `ALTER COLUMN id TYPE int COLLATE "C"` is
            // PostgreSQL's 42804 and must not leave the column half-changed.
            if collation.is_some() {
                crate::eval::require_collatable(*ty)?;
            }
            if state.retyped_columns.iter().any(|name| name == column) {
                return Err(ExecError::Unsupported(format!(
                    "cannot alter type of column \"{column}\" twice"
                )));
            }
            if !generated_columns_reading(&state.table, column).is_empty() {
                return Err(ExecError::Unsupported(
                    "cannot alter type of a column used by a generated column".into(),
                ));
            }
            let from = state.table.columns[index].ty;
            if using.is_none() && !alter_type_cast_allowed(from, *ty) {
                return Err(ExecError::TypeMismatch(format!(
                    "column \"{column}\" cannot be cast automatically to type {}",
                    ty.name()
                )));
            }
            // A stored view reads the column through its old type, and this
            // catalog cannot re-plan the view's text against the new one.
            if !dependent_view_names(kv, &table_name, Some(column))?.is_empty() {
                return Err(ExecError::Unsupported(
                    "cannot alter type of a column used by a view or rule".into(),
                ));
            }
            let scope = Scope::single(&state.table, &table_name.name);
            let rewritten = state
                .rows_mut(kv)?
                .iter()
                .map(|(_, xmin, xmax, _, _, row)| {
                    let cast = match using {
                        Some(expr) => eval_assignment_value(expr, *ty, &scope, row, &ddl_ctx)
                            .and_then(|value| coerce(value, *ty, &ddl_ctx)),
                        None => {
                            let value = row.get(index).cloned().unwrap_or(Datum::Null);
                            if value.is_null() {
                                Ok(Datum::Null)
                            } else {
                                // `ALTER COLUMN … TYPE` without `USING` is the
                                // implicit `column::newtype`, which reads the
                                // session's `DateStyle` when the old column is
                                // a string and the new one a date or time.
                                crabka_pgtypes::cast::cast_in(&value, *ty, ddl_ctx.output_style())
                                    .map_err(ExecError::from)
                            }
                        }
                    };
                    // A version no snapshot can reach again must not be able to
                    // fail the rewrite: PostgreSQL's own table rewrite discards
                    // dead tuples, so a value the user already deleted cannot
                    // block the type change.
                    match cast {
                        Ok(value) => Ok(value),
                        Err(error) => {
                            if version_is_settled_dead(kv, *xmin, *xmax)? {
                                Ok(Datum::Null)
                            } else {
                                Err(error)
                            }
                        }
                    }
                })
                .collect::<Result<Vec<_>, ExecError>>()?;
            for ((_, _, _, _, _, row), value) in state.rows_mut(kv)?.iter_mut().zip(rewritten) {
                if index < row.len() {
                    row[index] = value;
                }
            }
            state.table.columns[index].ty = *ty;
            // A retype resets the column to the new type's own collation unless
            // the statement names one, which is why `ALTER … TYPE text` on a
            // `COLLATE "C"` column drops the C rather than carrying it over.
            state.table.columns[index].collation = collation.clone();
            // Every CHECK is stored as source text and re-resolved on write, so
            // one that no longer type-checks has to fail the ALTER rather than
            // leave a table nothing can be written to.
            let checks = std::mem::take(&mut state.table.checks);
            let revalidated = checks
                .iter()
                .try_for_each(|check| validate_check_predicate(&state.table, &check.expr));
            state.table.checks = checks;
            revalidated?;
            rebuild_indexes_on_column(kv, state, column, &build)?;
            state
                .ops
                .extend(clear_statistics_data_referencing_column_ops(
                    kv,
                    &state.table,
                    column,
                    index,
                )?);
            state.retyped_columns.push(column.clone());
            Ok(())
        }
        Action::AddConstraint(constraint) => match &constraint.kind {
            // `ADD [CONSTRAINT n] NOT NULL c` is PostgreSQL 17's table-level
            // spelling of `ALTER COLUMN c SET NOT NULL`, and Crabka stores it as
            // exactly that: one flag on the column, always valid and always
            // inherited. The two attributes that would make it something else
            // have nowhere to be recorded, so they are refused rather than
            // dropped.
            crabka_pgparser::ast::TableConstraintKind::NotNull { column, no_inherit } => {
                reject_not_valid(constraint.attributes.not_valid, "NOT NULL")?;
                if *no_inherit {
                    return Err(no_inherit_not_null_unsupported());
                }
                set_column_not_null(kv, state, column, &ddl_ctx)
            }
            crabka_pgparser::ast::TableConstraintKind::Check(predicate) => add_check_constraint(
                state,
                constraint.name.clone(),
                &predicate.text,
                !constraint.attributes.not_valid,
                constraint.attributes.no_inherit,
                kv,
                &ddl_ctx,
            ),
            crabka_pgparser::ast::TableConstraintKind::PrimaryKey {
                columns,
                without_overlaps,
            } => {
                reject_not_valid(constraint.attributes.not_valid, "PRIMARY KEY")?;
                add_constraint_index(
                    kv,
                    state,
                    &AddConstraintIndex {
                        name: constraint.name.as_deref(),
                        columns,
                        primary_key: true,
                        without_overlaps: *without_overlaps,
                        deferral: constraint_deferral(constraint.attributes),
                    },
                    &build,
                )
            }
            crabka_pgparser::ast::TableConstraintKind::Unique {
                columns,
                without_overlaps,
                ..
            } => {
                reject_not_valid(constraint.attributes.not_valid, "UNIQUE")?;
                add_constraint_index(
                    kv,
                    state,
                    &AddConstraintIndex {
                        name: constraint.name.as_deref(),
                        columns,
                        primary_key: false,
                        without_overlaps: *without_overlaps,
                        deferral: constraint_deferral(constraint.attributes),
                    },
                    &build,
                )
            }
            // `reject_not_valid` is deliberately NOT called here: `NOT VALID`
            // applies to CHECK *and* FOREIGN KEY, the two kinds PostgreSQL can
            // validate lazily.
            crabka_pgparser::ast::TableConstraintKind::ForeignKey {
                columns,
                period,
                references,
            } => {
                reject_temporal_foreign_key(*period, references.period)?;
                add_foreign_key_constraint(
                    kv,
                    state,
                    &AddForeignKey {
                        name: constraint.name.as_deref(),
                        columns,
                        reference: references,
                        attributes: constraint.attributes,
                    },
                    own_xid,
                    &ddl_ctx,
                )
            }
            crabka_pgparser::ast::TableConstraintKind::Exclude { method, elements } => {
                reject_not_valid(constraint.attributes.not_valid, "EXCLUDE")?;
                let new_index = exclusion_constraint_index(
                    constraint.name.as_deref(),
                    &state.table.name,
                    &state.table.columns,
                    method,
                    elements,
                )?;
                add_exclusion_constraint(kv, state, new_index, &IndexBuild::new(&fctx, &ddl_ctx))
            }
        },
        Action::DropConstraint {
            name,
            if_exists,
            cascade,
        } => {
            if let Some(position) = state.table.checks.iter().position(|c| c.name == *name) {
                state.table.checks.remove(position);
                return Ok(());
            }
            if drop_foreign_key_constraint(kv, state, name) {
                return Ok(());
            }
            let index_exists = crabka_pgcatalog::list_table_indexes(kv, &table_name)?
                .into_iter()
                .any(|index| index.name == *name && index.constraint.is_some());
            if index_exists {
                // The dependents hang off the constraint's backing index, which
                // is why PostgreSQL's DETAIL names the index where the primary
                // message names the constraint.
                return drop_index_by_name(
                    kv,
                    state,
                    name,
                    &crate::error::DroppedObject::Constraint {
                        name: name.clone(),
                        table: table_name.to_string(),
                    },
                    *cascade,
                );
            }
            if *if_exists {
                return Ok(());
            }
            Err(ExecError::UndefinedRelationConstraint {
                name: name.clone(),
                table: table_name.to_string(),
            })
        }
        Action::RenameColumn { column, new_name } => {
            // PostgreSQL's RENAME COLUMN reports the bare column lookup here,
            // without naming the relation as the other subcommands do.
            let index = state
                .table
                .column_index(column)
                .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))?;
            // The last route to a relation carrying a system column name, and
            // the one that stays open after every creation path is closed.
            crate::scope::reject_system_column_names([new_name.as_str()])?;
            if state.table.column_index(new_name).is_some() {
                return Err(ExecError::DuplicateColumn {
                    column: new_name.clone(),
                    table: table_name.to_string(),
                });
            }
            rename_column_dependencies(kv, state, column, new_name)?;
            state.table.columns[index].name = new_name.clone();
            Ok(())
        }
        Action::AlterConstraint { name, spec } => alter_constraint(kv, state, name, *spec),
        Action::RenameConstraint { name, new_name } => {
            if let Some(check) = state.table.checks.iter_mut().find(|c| c.name == *name) {
                check.name = new_name.clone();
                return Ok(());
            }
            if let Some(mut foreign_key) = state
                .current_foreign_keys(kv)?
                .into_iter()
                .find(|fk| fk.name == *name)
            {
                // The by-table catalog key carries the constraint name, so a
                // rename moves the record rather than rewriting it in place.
                state
                    .ops
                    .extend(crabka_pgcatalog::drop_foreign_key_ops(kv, state.table.id, name)?.1);
                state.dropped_foreign_keys.push(name.clone());
                foreign_key.name = new_name.clone();
                state
                    .ops
                    .extend(crabka_pgcatalog::put_foreign_key_ops(&foreign_key));
                state.created_foreign_keys.push(foreign_key);
                return Ok(());
            }
            let Some(mut index) = crabka_pgcatalog::list_table_indexes(kv, &table_name)?
                .into_iter()
                .find(|index| index.name == *name && index.constraint.is_some())
            else {
                return Err(ExecError::UndefinedConstraint {
                    name: name.clone(),
                    table: table_name.to_string(),
                });
            };
            let (_, drop_ops) =
                crabka_pgcatalog::drop_constraint_index_ops(kv, &table_name.sibling(name))?;
            state.ops.extend(drop_ops);
            index.name = new_name.clone();
            state.ops.extend(crabka_pgcatalog::put_index_ops(&index));
            Ok(())
        }
        Action::ValidateConstraint(name) => {
            if let Some(position) = state.table.checks.iter().position(|c| c.name == *name) {
                if state.table.checks[position].validated {
                    return Ok(());
                }
                let mut probe = state.table.clone();
                probe.checks = vec![state.table.checks[position].clone()];
                let compiled = compile_check_constraints(&probe)?;
                for (_, _, row) in &state.live_rows(kv, &ddl_ctx)? {
                    if let Err(ExecError::CheckViolation { constraint, .. }) =
                        enforce_check_constraints(&probe, &compiled, row, &ddl_ctx)
                    {
                        return Err(ExecError::CheckViolationOnExistingRows {
                            table: table_name.to_string(),
                            constraint,
                        });
                    }
                }
                state.table.checks[position].validated = true;
                return Ok(());
            }
            if let Some(mut foreign_key) = state
                .current_foreign_keys(kv)?
                .into_iter()
                .find(|fk| fk.name == *name)
            {
                if foreign_key.validated {
                    return Ok(());
                }
                validate_foreign_key_against_state(kv, state, &foreign_key, own_xid, &ddl_ctx)?;
                foreign_key.validated = true;
                state
                    .ops
                    .extend(crabka_pgcatalog::put_foreign_key_ops(&foreign_key));
                state.created_foreign_keys.retain(|fk| fk.name != *name);
                state.created_foreign_keys.push(foreign_key);
                return Ok(());
            }
            let index_exists = crabka_pgcatalog::list_table_indexes(kv, &table_name)?
                .into_iter()
                .any(|index| index.name == *name && index.constraint.is_some());
            if index_exists {
                // PostgreSQL refuses to VALIDATE an index-backed constraint.
                return Err(ExecError::WrongObjectType(format!(
                    "cannot validate constraint \"{name}\" of relation \"{table_name}\""
                )));
            }
            Err(ExecError::UndefinedRelationConstraint {
                name: name.clone(),
                table: table_name.to_string(),
            })
        }
        // Heap storage parameters have no counterpart in Crabka's storage
        // model, and PostgreSQL's observable outcome for a table that has none
        // is the same: the command succeeds and changes no queryable state.
        Action::SetStorageParameters(_) | Action::ResetStorageParameters(_) => Ok(()),
        Action::OwnerTo(role) => {
            // The schema record is rewritten once, after every action; folding
            // the new owner into the working table is what makes it durable.
            state.table.owner = resolve_new_owner(kv, fctx, role)?;
            Ok(())
        }
        // The four row-security subcommands fold into the working relation the
        // way OWNER TO does; the schema record is written once, after every
        // action. A sharded relation is refused rather than flagged: its writes
        // go through the timestamp path, which cannot evaluate a policy, so the
        // flag would be stored and never enforced.
        Action::EnableRowSecurity | Action::DisableRowSecurity => {
            // Owner-only, like the policy DDL: DISABLE by a role that does not
            // own the relation would strip its protection outright, which is
            // the one ALTER TABLE subcommand that can grant rows.
            crate::policy_ddl::require_owner(kv, &state.table, fctx)?;
            crate::rls::refuse_sharded_row_security(&state.table)?;
            state.table.row_security = matches!(action, Action::EnableRowSecurity);
            Ok(())
        }
        Action::ForceRowSecurity | Action::NoForceRowSecurity => {
            crate::policy_ddl::require_owner(kv, &state.table, fctx)?;
            crate::rls::refuse_sharded_row_security(&state.table)?;
            state.table.force_row_security = matches!(action, Action::ForceRowSecurity);
            Ok(())
        }
        Action::SetTriggerMode { selector, mode } => {
            state.ops.extend(crate::trigger::set_table_trigger_mode(
                kv,
                &state.table,
                selector,
                *mode,
            )?);
            Ok(())
        }
        Action::SetRuleMode { name, mode } => {
            state.ops.extend(crate::rewrite_rules::set_enabled(
                kv,
                state.table.name.clone(),
                name,
                *mode,
            )?);
            Ok(())
        }
        Action::RenameTable { .. } => Err(ExecError::Syntax(
            "RENAME TO cannot be combined with other ALTER TABLE subcommands".into(),
        )),
        Action::AttachPartition { partition, bound } => {
            let partition =
                &resolve_relation(kv, resolution, partition, SchemaDisposition::Utility)?;
            let build = IndexBuild::new(&fctx, &ddl_ctx);
            let ops =
                attach_partition_ops(kv, &state.table, partition, bound, state.own_xid, &build)?;
            state.ops.extend(ops);
            Ok(())
        }
        Action::DetachPartition {
            partition,
            concurrently,
            finalize,
        } => {
            // `CONCURRENTLY` and `FINALIZE` describe how PostgreSQL splits the
            // detach across two transactions to avoid a long lock. DDL here is
            // one catalog batch under the global catalog lock, so both spellings
            // detach exactly as the plain form does.
            let _ = (concurrently, finalize);
            let partition =
                &resolve_relation(kv, resolution, partition, SchemaDisposition::Utility)?;
            let (parent, _) = crate::partition::parent_of(kv, partition)?
                .filter(|(parent, _)| *parent == table_name)
                .ok_or_else(|| {
                    if crabka_pgcatalog::get_table(kv, partition).is_err() {
                        ExecError::Catalog(crabka_pgcatalog::CatalogError::UndefinedTable(
                            partition.to_string(),
                        ))
                    } else {
                        ExecError::NotAPartitionOf {
                            partition: partition.to_string(),
                            parent: table_name.to_string(),
                        }
                    }
                })?;
            state
                .ops
                .extend(crate::trigger::drop_partition_trigger_clones(
                    kv,
                    &state.table,
                    partition,
                )?);
            state
                .ops
                .extend(crate::partition::detach_ops(&parent, partition));
            Ok(())
        }
        Action::ClusterOn(index) => {
            reject_cluster_mark_on_partitioned(kv, &state.table)?;
            let index = cluster_index_named(kv, &state.table, index)?;
            state.ops.extend(record_clustered_index_ops(
                kv,
                &state.table,
                Some(&index.name),
            )?);
            Ok(())
        }
        Action::SetWithoutCluster => {
            reject_cluster_mark_on_partitioned(kv, &state.table)?;
            state
                .ops
                .extend(record_clustered_index_ops(kv, &state.table, None)?);
            Ok(())
        }
        Action::SetWithoutOids => Ok(()),
        Action::SetSchema(_) => unreachable!("SET SCHEMA is handled before ALTER TABLE passes"),
        Action::OfType(_) | Action::NotOfType => {
            unreachable!("OF and NOT OF are handled before ALTER TABLE passes")
        }
        Action::SetReplicaIdentity(identity) => {
            let identity = replica_identity_for_action(kv, state, identity)?;
            state.ops.extend(crabka_pgcatalog::set_replica_identity_ops(
                state.table.id,
                &identity,
            ));
            Ok(())
        }
        Action::Unsupported(label) => Err(ExecError::Unsupported(format!(
            "ALTER TABLE subcommand is not supported: {label}"
        ))),
    }
}

fn replica_identity_for_action(
    kv: &dyn Kv,
    state: &AlterTableState,
    identity: &crabka_pgparser::ast::ReplicaIdentity,
) -> Result<crabka_pgcatalog::ReplicaIdentity, ExecError> {
    use crabka_pgparser::ast::ReplicaIdentity as Action;

    let name = match identity {
        Action::Default => return Ok(crabka_pgcatalog::ReplicaIdentity::Default),
        Action::Full => return Ok(crabka_pgcatalog::ReplicaIdentity::Full),
        Action::Nothing => return Ok(crabka_pgcatalog::ReplicaIdentity::Nothing),
        Action::UsingIndex(name) => name,
    };
    let index = state
        .current_indexes(kv)?
        .into_iter()
        .find(|index| index.name == *name)
        .or_else(|| crabka_pgcatalog::get_index(kv, &state.table.name.sibling(name)).ok())
        .ok_or_else(|| {
            ExecError::UndefinedObject(format!(
                "index \"{name}\" for table \"{}\" does not exist",
                state.table.name.name
            ))
        })?;
    if index.table != state.table.name {
        return Err(ExecError::WrongObjectType(format!(
            "\"{name}\" is not an index for table \"{}\"",
            state.table.name.name
        )));
    }
    if !index.unique {
        return Err(ExecError::WrongObjectType(format!(
            "cannot use non-unique index \"{name}\" as replica identity"
        )));
    }
    if index.deferral.is_deferrable() {
        return Err(ExecError::WrongObjectType(format!(
            "cannot use non-immediate index \"{name}\" as replica identity"
        )));
    }
    if index
        .columns
        .iter()
        .any(|key| crabka_pgcatalog::index_key_expression(key).is_some())
    {
        return Err(ExecError::WrongObjectType(format!(
            "cannot use expression index \"{name}\" as replica identity"
        )));
    }
    for column in &index.columns {
        let column = state
            .table
            .columns
            .iter()
            .find(|candidate| candidate.name == *column)
            .ok_or_else(|| ExecError::UndefinedIndexColumn(column.clone()))?;
        if !column.not_null {
            return Err(ExecError::WrongObjectType(format!(
                "index \"{name}\" cannot be used as replica identity because column \"{}\" is nullable",
                column.name
            )));
        }
    }
    Ok(crabka_pgcatalog::ReplicaIdentity::Index(index.name))
}

fn replica_identity_index_uses_column(
    kv: &dyn Kv,
    table: &Table,
    column: &str,
) -> Result<bool, ExecError> {
    let crabka_pgcatalog::ReplicaIdentity::Index(name) =
        crabka_pgcatalog::replica_identity(kv, table.id)?
    else {
        return Ok(false);
    };
    Ok(crabka_pgcatalog::list_table_indexes(kv, &table.name)?
        .iter()
        .find(|index| index.name == name)
        .is_some_and(|index| index.columns.iter().any(|key| key == column)))
}

/// A partitioned relation has no heap of its own, so neither of its indexes can
/// carry the clustered mark — `PostgreSQL` reports both spellings the same way.
pub(crate) fn reject_cluster_mark_on_partitioned(
    kv: &dyn Kv,
    table: &Table,
) -> Result<(), ExecError> {
    if crate::partition::is_partitioned(kv, &table.name)? {
        return Err(ExecError::Unsupported(
            "cannot mark index clustered in partitioned table".into(),
        ));
    }
    Ok(())
}

/// Validate and record `ALTER TABLE parent ATTACH PARTITION child <bound>`.
///
/// The candidate must have every column the parent has (42804 otherwise), and
/// every row it already stores must satisfy the bound being attached (23514
/// otherwise). `PostgreSQL` scans the table before it will attach it.
pub(crate) fn attach_partition_ops(
    kv: &dyn Kv,
    parent: &Table,
    child: &crabka_pgcatalog::RelationName,
    bound: &crabka_pgparser::ast::PartitionBound,
    own_xid: Option<u64>,
    build: &IndexBuild<'_>,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let ctx = build.ctx();
    let scheme = crate::partition::scheme_of(kv, &parent.name)?
        .ok_or_else(|| ExecError::NotPartitioned(parent.name.to_string()))?;
    let candidate = crabka_pgcatalog::get_table(kv, child)?;
    if candidate.foreign.is_some() {
        crate::exec::ddl_partition::reject_foreign_partition_with_unique_index(
            kv,
            &parent.name,
            child,
            true,
        )?;
    }
    if let Some(missing) = parent
        .columns
        .iter()
        .find(|column| candidate.column_index(&column.name).is_none())
    {
        return Err(ExecError::ChildMissingColumn(missing.name.clone()));
    }
    for parent_check in &parent.checks {
        if !candidate
            .checks
            .iter()
            .any(|check| check.name == parent_check.name && check.expr == parent_check.expr)
        {
            return Err(ExecError::InvalidObjectDefinition(format!(
                "child table is missing constraint \"{}\"",
                parent_check.name
            )));
        }
    }
    // A partition's columns have to be declared exactly as the parent declares
    // them, collation included: PostgreSQL compares the two declarations rather
    // than what they do, so `char(2) COLLATE "POSIX"` cannot join a parent whose
    // column says `COLLATE "C"` even where both order text by byte value.
    for column in &parent.columns {
        let Some(index) = candidate.column_index(&column.name) else {
            continue;
        };
        if candidate.columns[index].collation != column.collation {
            return Err(ExecError::ChildColumnCollationMismatch {
                child: child.name.clone(),
                column: column.name.clone(),
            });
        }
    }
    if crate::partition::parent_of(kv, child)?.is_some() {
        return Err(ExecError::InvalidObjectDefinition(format!(
            "\"{child}\" is already a partition"
        )));
    }
    // Attaching a table to itself, or to one of its own descendants, would make
    // the partition metadata cyclic. PostgreSQL calls that "circular inheritance
    // not allowed" (42P07), and it must be refused here rather than stored: a
    // cycle turns every later walk of the tree — DROP above all — into an
    // unbounded loop.
    if *child == parent.name || crate::partition::descendants(kv, child)?.contains(&parent.name) {
        return Err(ExecError::CircularInheritance);
    }
    let resolved = resolve_partition_bound(bound, &scheme, &parent.columns, ctx)?;
    let siblings = crate::partition::partitions_of(kv, &parent.name)?;
    crate::partition::check_bound_shape(scheme.strategy, &resolved)?;
    crate::partition::check_hash_bound(&resolved)?;
    crate::partition::check_range_not_empty(child, &resolved)?;
    crate::partition::check_no_overlap(scheme.strategy, child, &resolved, &siblings)?;

    // PostgreSQL maps the candidate's columns onto the parent's by NAME, so a
    // table declared in a different column order still attaches.
    let ordinals = parent
        .columns
        .iter()
        .map(|column| {
            candidate
                .column_index(&column.name)
                .ok_or_else(|| ExecError::ChildMissingColumn(column.name.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let versions = scan_all_row_versions(kv, &candidate)?;
    for (_, _, stored) in live_row_versions(kv, &candidate, &versions, own_xid)? {
        let row = ordinals
            .iter()
            .map(|ordinal| stored.get(*ordinal).cloned().unwrap_or(Datum::Null))
            .collect::<Vec<_>>();
        if !crate::partition::satisfies(&scheme, &parent.columns, &resolved, &siblings, &row)? {
            return Err(ExecError::PartitionConstraintViolationOnExistingRows(
                child.to_string(),
            ));
        }
    }
    let mut ops = crate::partition::attach_ops(&parent.name, child, &resolved);
    ops.extend(crate::trigger::clone_partition_triggers(kv, parent, child)?);
    ops.extend(attached_partition_index_ops(
        kv, parent, child, own_xid, build,
    )?);
    Ok(ops)
}

/// `NOT VALID` applies only to constraints `PostgreSQL` can validate lazily:
/// `CHECK` and `FOREIGN KEY`. An index-backed constraint must be built now.
pub(crate) fn reject_not_valid(not_valid: bool, kind: &str) -> Result<(), ExecError> {
    if not_valid {
        return Err(ExecError::Unsupported(format!(
            "{kind} constraints cannot be marked NOT VALID"
        )));
    }
    Ok(())
}

/// What kind of constraint an `ALTER TABLE … ALTER CONSTRAINT` name resolves
/// to. `PostgreSQL` decides every one of the subcommand's refusals from
/// `pg_constraint.contype` alone, so this is the whole lookup result.
pub(crate) enum AlteredConstraint {
    ForeignKey(Box<crabka_pgcatalog::ForeignKey>),
    NotNull,
    /// A `CHECK`, or a `PRIMARY KEY`/`UNIQUE`/`EXCLUDE` and its backing index.
    Other,
}

/// Resolve the constraint an `ALTER CONSTRAINT` names, in the order
/// `PostgreSQL`'s single `pg_constraint` scan would find it. The not-null names
/// come last because they are derived from the column rather than stored, so a
/// real constraint of the same name always wins.
pub(crate) fn find_altered_constraint(
    kv: &dyn Kv,
    state: &AlterTableState,
    name: &str,
) -> Result<Option<AlteredConstraint>, ExecError> {
    if state.table.checks.iter().any(|check| check.name == name) {
        return Ok(Some(AlteredConstraint::Other));
    }
    if let Some(foreign_key) = state
        .current_foreign_keys(kv)?
        .into_iter()
        .find(|fk| fk.name == name)
    {
        return Ok(Some(AlteredConstraint::ForeignKey(Box::new(foreign_key))));
    }
    if crabka_pgcatalog::list_table_indexes(kv, &state.table.name)?
        .iter()
        .any(|index| index.name == name && index.constraint.is_some())
    {
        return Ok(Some(AlteredConstraint::Other));
    }
    if state.table.columns.iter().any(|column| {
        column.not_null
            && crate::catalog_rel::not_null_constraint_name(&state.table.name, &column.name) == name
    }) {
        return Ok(Some(AlteredConstraint::NotNull));
    }
    Ok(None)
}

/// `ALTER TABLE … ALTER CONSTRAINT <name> …`.
///
/// `PostgreSQL` admits a deferrability or enforceability change on a foreign key
/// alone, and an inheritability change on a not-null alone; every other pairing
/// is a 42809 that names the constraint. Those refusals are reproduced here word
/// for word, because they are the whole observable behaviour of the subcommand
/// for constraints Crabka does not let it touch.
///
/// Of the three properties Crabka can then be asked to write, only deferrability
/// has somewhere to go: a foreign key records it, and the write path reads the
/// record live. Enforceability has no counterpart at all — Crabka checks every
/// constraint it stores — and a not-null's inheritability is fixed by the column
/// flag being copied to every child. Both are refused rather than accepted and
/// dropped.
pub(crate) fn alter_constraint(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    name: &str,
    spec: crabka_pgparser::ast::AlterConstraintSpec,
) -> Result<(), ExecError> {
    let table_name = state.table.name.to_string();
    let found = find_altered_constraint(kv, state, name)?.ok_or_else(|| {
        ExecError::UndefinedRelationConstraint {
            name: name.to_string(),
            table: table_name.clone(),
        }
    })?;
    let foreign_key = match &found {
        AlteredConstraint::ForeignKey(foreign_key) => Some(foreign_key.as_ref()),
        AlteredConstraint::NotNull | AlteredConstraint::Other => None,
    };
    if spec.deferrability.is_some() && foreign_key.is_none() {
        return Err(ExecError::WrongObjectType(format!(
            "constraint \"{name}\" of relation \"{table_name}\" is not a foreign key constraint"
        )));
    }
    if spec.enforced.is_some() && foreign_key.is_none() {
        return Err(ExecError::WrongObjectType(format!(
            "cannot alter enforceability of constraint \"{name}\" of relation \"{table_name}\""
        )));
    }
    if spec.inherit.is_some() && !matches!(found, AlteredConstraint::NotNull) {
        return Err(ExecError::WrongObjectType(format!(
            "constraint \"{name}\" of relation \"{table_name}\" is not a not-null constraint"
        )));
    }
    if spec.enforced.is_some() {
        return Err(ExecError::Unsupported(
            "ALTER TABLE … ALTER CONSTRAINT … [NOT] ENFORCED is not supported: Crabka checks \
             every constraint it stores"
                .to_string(),
        ));
    }
    // `NO INHERIT` is the only inheritability a column flag cannot express;
    // `INHERIT` asks for what Crabka already does, so it is a no-op rather than
    // a refusal.
    if spec.inherit == Some(false) {
        return Err(no_inherit_not_null_unsupported());
    }
    if let Some((deferrable, initially_deferred)) = spec.deferrability
        && let Some(foreign_key) = foreign_key
    {
        let mut updated = foreign_key.clone();
        updated.deferrable = deferrable;
        updated.initially_deferred = initially_deferred;
        state
            .ops
            .extend(crabka_pgcatalog::put_foreign_key_ops(&updated));
        state.created_foreign_keys.retain(|fk| fk.name != name);
        state.created_foreign_keys.push(updated);
    }
    Ok(())
}

/// The refusal `NO INHERIT` on a not-null constraint earns.
///
/// Crabka stores a not-null as a flag on the column, and a flag is copied to
/// every child a table gets. There is nowhere to record that one should not be.
/// Accepting the clause and dropping it would leave a child claiming a
/// constraint its parent said it must not have, so the statement is refused
/// while the clause has no home.
pub(crate) fn no_inherit_not_null_unsupported() -> ExecError {
    ExecError::Unsupported(
        "NOT NULL … NO INHERIT is not supported: Crabka stores a not-null as a column flag, \
         which every child inherits"
            .to_string(),
    )
}

/// Set a column's not-null flag, refusing the change when a row already stored
/// holds a null there. Shared by `ALTER COLUMN … SET NOT NULL` and by
/// `ADD [CONSTRAINT n] NOT NULL <column>`, which mean the same thing.
pub(crate) fn set_column_not_null(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    column: &str,
    ddl_ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    let index = state.column_index(column)?;
    let table_name = state.table.name.to_string();
    for (_rowid, _xmin, row) in &state.live_rows(kv, ddl_ctx)? {
        if row.get(index).is_none_or(Datum::is_null) {
            return Err(ExecError::ColumnContainsNullValues {
                column: column.to_string(),
                table: table_name,
            });
        }
    }
    state.table.columns[index].not_null = true;
    Ok(())
}

/// One `FOREIGN KEY` clause as a DDL statement writes it, in either spelling:
/// `[CONSTRAINT <name>] FOREIGN KEY (…) REFERENCES …` or a column-level
/// `REFERENCES`. Shared by `CREATE TABLE` and every `ALTER TABLE` subcommand
/// that can carry one.
pub(crate) struct AddForeignKey<'a> {
    pub(crate) name: Option<&'a str>,
    pub(crate) columns: &'a [String],
    pub(crate) reference: &'a crabka_pgparser::ast::ForeignKeyRef,
    pub(crate) attributes: crabka_pgparser::ast::ConstraintAttributes,
}

/// Add one `FOREIGN KEY` and back-validate it against the relation's live rows
/// first.
///
/// `NOT VALID` is the exception. It stores the constraint without that scan,
/// and the constraint still governs every subsequent write.
///
/// The scan reads [`AlterTableState::live_rows`] and resolves through
/// [`AlterTableState::staged_catalog`], never storage, so a constraint added in
/// the same batch as the column it keys on sees the rewritten rows and the
/// rewritten column list.
pub(crate) fn add_foreign_key_constraint(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    request: &AddForeignKey<'_>,
    own_xid: Option<u64>,
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    let resolution = ctx.resolution();
    let table_name = state.table.name.clone();
    let taken = state.taken_constraint_names(kv)?;
    let name = match request.name {
        Some(name) => {
            if taken.iter().any(|used| used == name) {
                return Err(ExecError::DuplicateObject(format!(
                    "constraint \"{name}\" for relation \"{table_name}\" already exists"
                )));
            }
            name.to_string()
        }
        None => {
            let taken: Vec<&str> = taken.iter().map(String::as_str).collect();
            unique_constraint_name(
                &taken,
                &crate::fk::default_foreign_key_name(&table_name, request.columns),
            )
        }
    };
    // `Table` carries no partition flag, so the DDL caller is the only place
    // that can refuse a partitioned relation.
    if crate::partition::is_partitioned(kv, &table_name)?
        || crate::partition::parent_of(kv, &table_name)?.is_some()
    {
        return Err(reject_partitioned_foreign_key(&name));
    }
    let indexes = state.current_indexes(kv)?;
    let id = state.foreign_key_ids.allocate(kv)?;
    let foreign_key = {
        // `REFERENCES` naming this same relation resolves against the working
        // column and index lists — an index added earlier in this statement is
        // not in the catalog yet.
        let child = crate::fk::FkRelation {
            id: state.table.id,
            name: &table_name,
            columns: &state.table.columns,
            indexes: &indexes,
            sharded: state.table.sharded,
        };
        crate::fk::resolve_foreign_key(
            kv,
            resolution,
            &child,
            &crate::fk::ForeignKeyRequest {
                id,
                name: Some(&name),
                columns: request.columns,
                reference: request.reference,
                attributes: request.attributes,
                validated: !request.attributes.not_valid,
                self_reference: Some(&child),
            },
        )?
    };
    // After the key resolves, as `CREATE TABLE` tests it and as PostgreSQL
    // does: the missing relation and the unbacked key are reported first.
    reject_foreign_key_over_generated(&state.table.columns, request.columns, request.reference)?;
    if foreign_key.validated {
        validate_foreign_key_against_state(kv, state, &foreign_key, own_xid, ctx)?;
    }
    state
        .ops
        .extend(crabka_pgcatalog::create_foreign_key_ops(kv, &foreign_key)?);
    state.created_foreign_keys.push(foreign_key);
    Ok(())
}

/// Run one foreign key's back-validation scan over the relation's in-flight
/// rows, resolving names through the statement's staged catalog.
///
/// DDL runs against a single KV handle standing in for the local store, the
/// catalog and range 0's global clog alike, under an all-committed snapshot plus
/// the open transaction's own xid. That is the same visibility a unique-index
/// backfill uses, and for the same reason: `BEGIN; INSERT …; ALTER TABLE … ADD
/// FOREIGN KEY` must validate against the rows this transaction has written.
pub(crate) fn validate_foreign_key_against_state(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    foreign_key: &crabka_pgcatalog::ForeignKey,
    own_xid: Option<u64>,
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    let rows: Vec<Vec<Datum>> = state
        .live_rows(kv, ctx)?
        .into_iter()
        .map(|(_, _, row)| row)
        .collect();
    let staged = state.staged_catalog(kv)?;
    let snapshot = all_committed_snapshot();
    let exec = crate::fk::FkExecContext {
        catalog_kv: &staged,
        kv,
        global: kv,
        global_snapshot: &snapshot,
        snapshot: &snapshot,
        xid: own_xid.unwrap_or(crabka_pgmvcc::xid::INVALID_XID),
        eval_ctx: ctx,
    };
    crate::fk::validate_foreign_key_rows(&exec, foreign_key, &rows)
}

/// Drop the foreign key `name` names, if the relation carries one, returning
/// whether it did.
///
/// A constraint this same statement added is only staged, so its puts are pulled
/// back out of the batch rather than paired with deletes of keys that were never
/// written.
pub(crate) fn drop_foreign_key_constraint(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    name: &str,
) -> bool {
    let Some(position) = state
        .created_foreign_keys
        .iter()
        .position(|fk| fk.name == name)
    else {
        let Ok((_, ops)) = crabka_pgcatalog::drop_foreign_key_ops(kv, state.table.id, name) else {
            return false;
        };
        state.ops.extend(ops);
        state.dropped_foreign_keys.push(name.to_string());
        return true;
    };
    let staged = state.created_foreign_keys.remove(position);
    let keys: Vec<Vec<u8>> = crabka_pgcatalog::put_foreign_key_ops(&staged)
        .into_iter()
        .filter_map(|op| match op {
            crabka_pgkv::WriteOp::Put { key, .. } => Some(key),
            _ => None,
        })
        .collect();
    state
        .ops
        .retain(|op| !matches!(op, crabka_pgkv::WriteOp::Put { key, .. } if keys.contains(key)));
    state.dropped_foreign_keys.push(name.to_string());
    true
}

/// Reject a `CHECK` predicate `PostgreSQL` refuses at DDL time, before it can be
/// stored and start failing every later write.
///
/// `PostgreSQL` runs full parse analysis over the predicate when the constraint
/// is created, so an unknown column, a subquery, an aggregate, or a non-boolean
/// result is an error on the `CREATE TABLE` / `ALTER TABLE`. It is never a
/// table that accepts the DDL and then rejects (or silently mis-filters) its
/// own rows.
pub(crate) fn validate_check_predicate(table: &Table, predicate: &str) -> Result<(), ExecError> {
    use crabka_pgparser::ast::Expr;

    let expr = crabka_pgparser::parser::parse_expression(predicate)?;
    let scope = Scope::single(table, &table.name.name);
    let mut rejection: Option<ExecError> = None;
    crate::grouping::visit_expr(&expr, &mut |node| {
        if rejection.is_some() {
            return;
        }
        rejection = match node {
            Expr::ScalarSubquery(_)
            | Expr::Exists(_)
            | Expr::InSubquery { .. }
            | Expr::Quantified { .. } => Some(ExecError::Unsupported(
                "cannot use subquery in check constraint".into(),
            )),
            Expr::Column {
                table: qualifier,
                name,
            } => scope
                .resolve(qualifier.as_deref(), name)
                .err()
                .map(|_| ExecError::UndefinedColumn(name.clone())),
            _ => None,
        };
    });
    if let Some(error) = rejection {
        return Err(error);
    }
    if crate::agg::contains_aggregate(&expr) {
        return Err(ExecError::Grouping(
            "aggregate functions are not allowed in check constraints".into(),
        ));
    }
    crate::eval::check_predicate_resolves(&expr, &scope)?;
    // An `unknown` literal predicate adopts boolean, exactly as PostgreSQL
    // coerces it: `CHECK ('t')` is a valid always-true constraint, and
    // `CHECK ('abc')` fails inside boolean's input function (22P02) rather than
    // being rejected for having the wrong type.
    if crate::eval::is_unknown_literal(&expr) {
        let ctx = crate::clock::EvalCtx::test_default();
        let literal = crate::eval::eval(&expr, &scope, &[], &ctx)?;
        if !literal.is_null() {
            crabka_pgtypes::cast::cast(&literal, ColumnType::Bool, &ctx.time_zone)?;
        }
        return Ok(());
    }
    let result = crate::eval::infer_type(&expr, &scope)?;
    if result != ColumnType::Bool {
        return Err(ExecError::TypeMismatch(format!(
            "argument of CHECK must be type boolean, not type {}",
            result.name()
        )));
    }
    Ok(())
}

/// Add one `CHECK` and back-validate it against the table's live rows first.
///
/// `PostgreSQL` refuses the constraint and does not leave violating rows.
/// `valid` is false for `NOT VALID`, which stores the constraint without that
/// scan. It still governs every subsequent write.
pub(crate) fn add_check_constraint(
    state: &mut AlterTableState,
    name: Option<String>,
    predicate: &str,
    valid: bool,
    no_inherit: bool,
    kv: &dyn Kv,
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    validate_check_predicate(&state.table, predicate)?;
    let column_names: Vec<String> = state.table.columns.iter().map(|c| c.name.clone()).collect();
    let default = default_check_name(&state.table.name, predicate, &column_names);
    // A generated name takes the lowest free numeric suffix, so only an
    // explicit `CONSTRAINT <name>` can collide here.
    let taken: Vec<&str> = state
        .table
        .checks
        .iter()
        .map(|check| check.name.as_str())
        .collect();
    let name = name.unwrap_or_else(|| unique_constraint_name(&taken, &default));
    if state.table.checks.iter().any(|check| check.name == name)
        || constraint_index_named(kv, state, &name)?
    {
        return Err(ExecError::DuplicateObject(format!(
            "constraint \"{name}\" for relation \"{}\" already exists",
            state.table.name
        )));
    }
    let check = crabka_pgcatalog::CheckConstraint {
        name,
        expr: predicate.to_string(),
        validated: valid,
        no_inherit,
    };
    let mut probe = state.table.clone();
    probe.checks = vec![check.clone()];
    let compiled = compile_check_constraints(&probe)?;
    if !valid {
        // NOT VALID skips the existing-row scan; the constraint is still
        // enforced for every subsequent write.
        state.table.checks.push(check);
        return Ok(());
    }
    let live = state.live_rows(kv, ctx)?;
    for (_, _, row) in &live {
        if let Err(ExecError::CheckViolation { constraint, .. }) =
            enforce_check_constraints(&probe, &compiled, row, ctx)
        {
            return Err(ExecError::CheckViolationOnExistingRows {
                table: state.table.name.to_string(),
                constraint,
            });
        }
    }
    state.table.checks.push(check);
    Ok(())
}

/// True when a stored `CHECK` predicate mentions `column`; used to drop the
/// constraints that a `DROP COLUMN` invalidates.
pub(crate) fn check_references_column(predicate: &str, column: &str, columns: &[String]) -> bool {
    let mut referenced = false;
    if let Ok(tokens) = crabka_pgparser::lexer::lex(predicate) {
        for (token, _) in &tokens {
            if let crabka_pgparser::token::Token::Ident(word) = token
                && word == column
                && columns.iter().all(|name| name != word)
            {
                referenced = true;
            }
        }
    }
    referenced
}

/// One `ALTER TABLE … ADD [CONSTRAINT c] { PRIMARY KEY | UNIQUE } (…)` clause.
pub(crate) struct AddConstraintIndex<'a> {
    /// The explicit `CONSTRAINT <name>` label, when one was written.
    pub(crate) name: Option<&'a str>,
    pub(crate) columns: &'a [String],
    /// True for `PRIMARY KEY`, false for `UNIQUE`.
    pub(crate) primary_key: bool,
    /// `WITHOUT OVERLAPS` was written on the last key column.
    pub(crate) without_overlaps: bool,
    /// The check point the constraint's `[NOT] DEFERRABLE` tail asks for.
    pub(crate) deferral: crabka_pgcatalog::ConstraintDeferral,
}

pub(crate) fn add_constraint_index(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    request: &AddConstraintIndex<'_>,
    build: &IndexBuild<'_>,
) -> Result<(), ExecError> {
    let ctx = build.ctx();
    let AddConstraintIndex {
        name,
        columns,
        primary_key,
        without_overlaps,
        deferral,
    } = *request;
    // One constraint namespace per relation: an explicit name a CHECK on this
    // table already holds is 42710, whatever kind the new constraint is.
    if let Some(name) = name
        && state.table.checks.iter().any(|check| check.name == name)
    {
        return Err(ExecError::DuplicateObject(format!(
            "constraint \"{name}\" for relation \"{}\" already exists",
            state.table.name
        )));
    }
    if state.table.sharded {
        return Err(ExecError::Unsupported(
            "PRIMARY KEY and UNIQUE constraints on sharded tables are not supported until \
             global enforcement exists"
                .into(),
        ));
    }
    reject_index_over_virtual_generated(
        &state.table,
        columns,
        Some(if primary_key {
            &crabka_pgcatalog::IndexConstraint::PrimaryKey
        } else {
            &crabka_pgcatalog::IndexConstraint::Unique
        }),
    )?;
    if primary_key
        && crabka_pgcatalog::list_table_indexes(kv, &state.table.name)?
            .iter()
            .any(|index| {
                index.constraint == Some(crabka_pgcatalog::IndexConstraint::PrimaryKey)
                    && !state.dropped_indexes.contains(&index.name)
            })
    {
        return Err(ExecError::InvalidTableDefinition(format!(
            "multiple primary keys for table \"{}\" are not allowed",
            state.table.name
        )));
    }
    if without_overlaps {
        validate_without_overlaps_key(columns, &state.table.columns)?;
    }
    let key_column_indices = columns
        .iter()
        .map(|column| state.column_index(column))
        .collect::<Result<Vec<_>, _>>()?;
    if !without_overlaps {
        for index in &key_column_indices {
            validate_default_index_opclass(
                state.table.columns[*index].ty,
                crabka_pgcatalog::IndexMethod::Btree,
            )?;
        }
    }
    let rows = state.live_rows(kv, ctx)?;
    let mut new_index = create_table_constraint_index(
        &state.table.name,
        columns,
        primary_key,
        without_overlaps,
        deferral,
    );
    if let Some(name) = name {
        new_index.name = name.to_string();
    }
    let (index, index_ops) =
        crabka_pgcatalog::create_constraint_index_ops(kv, &state.table, &new_index)?;
    // PostgreSQL builds the unique index before it attaches NOT NULL, so
    // duplicate data is 23505 even when the key column also holds NULLs. A
    // temporal key is held apart by `&&` instead, which no key probe can
    // answer, so it back-validates pairwise like the exclusion constraint it
    // is — and reports the same 23P01 when a stored pair already overlaps.
    let backfill = if without_overlaps {
        validate_no_exclusion_conflicts(&state.table, &index, &rows, ctx)?;
        Vec::new()
    } else {
        local_index_backfill_ops_for_rows(kv, &rows, &state.table, &index, build)?
    };
    if primary_key {
        for (_rowid, _xmin, row) in &rows {
            for (column, column_index) in columns.iter().zip(&key_column_indices) {
                if row.get(*column_index).is_none_or(Datum::is_null) {
                    return Err(ExecError::ColumnContainsNullValues {
                        column: column.clone(),
                        table: state.table.name.to_string(),
                    });
                }
            }
        }
        for index in key_column_indices {
            state.table.columns[index].not_null = true;
        }
    }
    state.ops.extend(index_ops);
    state.ops.extend(backfill);
    let clones = descendant_constraint_clone_ops(kv, state, &index, build)?;
    state.ops.extend(clones);
    state.created_indexes.push(index);
    Ok(())
}

pub(crate) fn add_exclusion_constraint(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    new_index: crabka_pgcatalog::NewIndex,
    build: &IndexBuild<'_>,
) -> Result<(), ExecError> {
    let ctx = build.ctx();
    if state.table.sharded {
        return Err(ExecError::Unsupported(
            "exclusion constraints on sharded tables are not supported".into(),
        ));
    }
    let rows = state.live_rows(kv, ctx)?;
    let (index, index_ops) =
        crabka_pgcatalog::create_constraint_index_ops(kv, &state.table, &new_index)?;
    validate_no_exclusion_conflicts(&state.table, &index, &rows, ctx)?;
    state.ops.extend(index_ops);
    let clones = descendant_constraint_clone_ops(kv, state, &index, build)?;
    state.ops.extend(clones);
    state.created_indexes.push(index);
    Ok(())
}

/// Put a copy of a freshly added constraint index on every partition below the
/// relation it was added to, and refuse the constraint outright when no copy
/// could enforce it.
///
/// A key added to a partitioned parent is in exactly the position a key
/// declared with the parent is: the parent holds no rows, so the copies are the
/// enforcement. The refusal has to be repeated here because
/// [`partition_scheme_from_ast`] only sees the keys written by the `CREATE
/// TABLE` that declared the partitioning.
pub(crate) fn descendant_constraint_clone_ops(
    kv: &dyn Kv,
    state: &AlterTableState,
    index: &crabka_pgcatalog::Index,
    build: &IndexBuild<'_>,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let Some(scheme) = crate::partition::scheme_of(kv, &state.table.name)? else {
        return Ok(Vec::new());
    };
    if index.unique {
        reject_unique_index_with_foreign_partition(kv, &state.table)?;
    }
    reject_incomplete_partitioned_key(&scheme.keys, &index.columns, index.constraint.as_ref())?;
    let relations = crate::partition::descendants(kv, &state.table.name)?;
    clone_indexes_onto_partitions(
        kv,
        std::slice::from_ref(index),
        &relations,
        state.own_xid,
        build,
    )
}

/// Back-validate the rows a table already holds against an exclusion-enforced
/// index — an explicit `EXCLUDE`, or a `WITHOUT OVERLAPS` key.
///
/// `PostgreSQL` reports the failure as `could not create exclusion constraint`
/// with no DETAIL, unlike the runtime violation, and the same message covers a
/// temporal primary key: to the index build there is no difference.
pub(crate) fn validate_no_exclusion_conflicts(
    table: &Table,
    index: &crabka_pgcatalog::Index,
    rows: &[(u64, u64, Vec<Datum>)],
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    let Some(operators) = index.exclusion_operators() else {
        return Ok(());
    };
    for (offset, (_rowid, _xmin, row)) in rows.iter().enumerate() {
        let left = indexed_values(table, index, row)?;
        // An empty range would slip through every overlap test, so a stored
        // one blocks the constraint before any pair is compared.
        reject_empty_without_overlaps(table, index, &left)?;
        for (_rowid, _xmin, other) in &rows[..offset] {
            let right = indexed_values(table, index, other)?;
            if exclusion_keys_conflict(&operators, &left, &right)? {
                // The pair is named in stored order — the row the build reached
                // first, then the one that could not join it — which is how
                // PostgreSQL words the same failure.
                return Err(exclusion_build_violation(index, &right, &left, ctx));
            }
        }
    }
    Ok(())
}

/// The 23P01 an index build raises when two stored rows cannot coexist.
///
/// The primary message names the constraint being built rather than a row
/// insertion, which is what tells `ALTER TABLE … ADD CONSTRAINT` apart from the
/// runtime [`exclusion_violation`]; the DETAIL is the same shape minus the word
/// "existing", because neither row is more established than the other.
pub(crate) fn exclusion_build_violation(
    index: &crabka_pgcatalog::Index,
    first: &[Datum],
    second: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> ExecError {
    let columns = index.columns.join(", ");
    let render = |values: &[Datum]| {
        values
            .iter()
            .map(|value| {
                String::from_utf8_lossy(&crabka_pgtypes::encoding::encode_text_in(
                    value,
                    ctx.output_style(),
                ))
                .into_owned()
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    ExecError::Remote(
        crabka_pgwire::error::PgError::error(
            "23P01",
            format!("could not create exclusion constraint \"{}\"", index.name),
        )
        .with_detail(format!(
            "Key ({columns})=({}) conflicts with key ({columns})=({}).",
            render(first),
            render(second)
        )),
    )
}

/// Whether `ALTER TABLE … ALTER COLUMN … TYPE` may rewrite `from` to `to`
/// without an explicit `USING`.
///
/// `PostgreSQL` coerces the stored value in *assignment* context. On top of the
/// ordinary assignment casts that admits every I/O-conversion cast whose target
/// is a string type. `int4 → text` needs no `USING`, and `text → int4` does.
/// It also admits the temporal narrowings `PostgreSQL` marks assignment-level.
pub(crate) fn alter_type_cast_allowed(from: ColumnType, to: ColumnType) -> bool {
    use ColumnType::{Date, Time, Timestamp, Timestamptz, Timetz};

    crabka_pgtypes::cast::assignment_cast_allowed(from, to)
        || to.is_string()
        || matches!(
            (from, to),
            (Timestamp | Timestamptz, Date | Time) | (Timetz, Time) | (Time, Timetz)
        )
}

/// Re-encode every index entry that covers `column` after its type changed.
///
/// Index keys are encoded from the column's type, so entries written under the
/// old one no longer match a probe built from the new one: an index scan would
/// silently miss live rows and a unique index would stop rejecting duplicates.
/// The rebuild also re-runs the uniqueness check, because a narrowing cast can
/// collapse two distinct keys onto one.
pub(crate) fn rebuild_indexes_on_column(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    column: &str,
    build: &IndexBuild<'_>,
) -> Result<(), ExecError> {
    let ctx = build.ctx();
    let mut affected = crabka_pgcatalog::list_table_indexes(kv, &state.table.name)?;
    // An index created earlier in this same statement is not in the catalog
    // yet, so listing cannot find it — but its staged entries are encoded under
    // the old type just the same.
    for index in &state.created_indexes {
        if !affected.iter().any(|listed| listed.name == index.name) {
            affected.push(index.clone());
        }
    }
    affected.retain(|index| {
        index
            .columns
            .iter()
            .any(|key| index_key_reads_column(&state.table, key, column))
            && !state.dropped_indexes.contains(&index.name)
    });
    if affected.is_empty() {
        return Ok(());
    }
    let rows = state.live_rows(kv, ctx)?;
    for index in &affected {
        for column in &index.columns {
            if let Some(column) = state.table.column_index(column) {
                validate_default_index_opclass(state.table.columns[column].ty, index.method)?;
            }
        }
        let prefix = crabka_pgkv::key::secondary_index_prefix(index.table_id, index.id);
        // Drop the entries this statement already staged before re-deriving
        // them, so the rebuild is not layered on top of stale keys.
        state.ops.retain(
            |op| !matches!(op, crabka_pgkv::WriteOp::Put { key, .. } if key.starts_with(&prefix)),
        );
        let mut ops: Vec<crabka_pgkv::WriteOp> = kv
            .scan_prefix(&prefix)?
            .into_iter()
            .map(|(key, _)| crabka_pgkv::WriteOp::Delete { key })
            .collect();
        ops.extend(local_index_backfill_ops_for_rows(
            kv,
            &rows,
            &state.table,
            index,
            build,
        )?);
        state.ops.extend(ops);
    }
    Ok(())
}

/// `PostgreSQL`'s `has_partition_attrs` guard, in the two spellings that reach
/// it: `DROP COLUMN` and `ALTER COLUMN … TYPE`. `verb` is the one word that
/// differs between the two messages.
///
/// Upstream refuses both because the key column's dependency would cascade the
/// whole table away. Crabka has a second reason for each, and they are the
/// harder ones. A drop *compacts* the column list, so a key left naming a
/// departed column resolves to nothing and the relation can no longer route a
/// row at all. A retype leaves every stored bound coerced to the old type,
/// where it no longer compares against the key value — which is the same
/// unroutable relation by a different road.
///
/// The caller runs this per relation, so a sub-partitioned descendant reached
/// by the recursion refuses on its own key. `ATExecDropColumn` and
/// `ATPrepAlterColumnType` check on each level for exactly that reason.
pub(crate) fn reject_partition_key_column(
    kv: &dyn Kv,
    table_name: &crabka_pgcatalog::RelationName,
    verb: &str,
    column: &str,
) -> Result<(), ExecError> {
    let Some(scheme) = crate::partition::scheme_of(kv, table_name)? else {
        return Ok(());
    };
    if !scheme.keys.iter().any(|key| key == column) {
        return Ok(());
    }
    Err(ExecError::InvalidTableDefinition(format!(
        "cannot {verb} column \"{column}\" because it is part of the partition key of relation \
         \"{}\"",
        table_name.name
    )))
}

/// Remove one column from the working schema and every stored row version, and
/// drop the indexes, `CHECK`s and foreign keys that depended on it. This is
/// `PostgreSQL`'s own `DROP COLUMN` dependency handling.
///
/// A foreign key *keyed on* the dropped column goes with it, exactly as its
/// index does. A foreign key that *references* the column is a different matter:
/// it hangs off the unique index proving the column a key, so the refusal comes
/// out of [`drop_index_by_name`] as that index is dropped.
pub(crate) fn drop_table_column(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    column: &str,
    cascade: bool,
) -> Result<(), ExecError> {
    let Some(index) = state.table.column_index(column) else {
        return Ok(());
    };
    let table_name = state.table.name.clone();
    reject_partition_key_column(kv, &table_name, "drop", column)?;
    // A policy that reads the column is a hard dependency: PostgreSQL refuses
    // the drop and takes the whole `pg_policy` row only under CASCADE. Asked
    // first, before anything is written, because it refuses the statement.
    let dependent_policies = policies_reading_column(kv, &state.table, column)?;
    if !dependent_policies.is_empty() {
        if !cascade {
            return Err(dependent_policy_refusal(
                &table_name,
                column,
                &dependent_policies,
            ));
        }
        for policy in &dependent_policies {
            state.ops.extend(crabka_pgcatalog::policy::drop_policy_ops(
                kv,
                state.table.id,
                &policy.name,
            )?);
        }
    }
    // A grant on the column dies with the column. Leaving it behind would hand
    // the grant to whatever column is added under that name next.
    state
        .ops
        .extend(crabka_pgcatalog::drop_column_privileges_ops(
            kv,
            &table_name,
            column,
        )?);
    // And so does its comment, for the same reason: PostgreSQL's
    // `deleteOneObject` calls `DeleteComments` with the column's `attnum`, so
    // the `pg_description` row goes with the column and the relation's own
    // comment stays. Leaving it behind would hand the comment to whatever
    // column is added under that name next.
    state.ops.push(crabka_pgcatalog::set_comment_op(
        "column",
        crabka_pgcatalog::CommentObject::Column(&table_name, column),
        None,
    ));
    state.ops.extend(drop_statistics_referencing_column_ops(
        kv,
        &state.table,
        column,
        index,
    )?);
    for trigger in crabka_pgcatalog::trigger::triggers_for_table(kv, state.table.id)? {
        if trigger_references_column(&trigger, &state.table, column) {
            state
                .ops
                .extend(crabka_pgcatalog::trigger::drop_trigger_ops(
                    trigger.table_id,
                    &trigger.name,
                ));
            for clone in crabka_pgcatalog::trigger::list_triggers(kv)?
                .into_iter()
                .filter(|candidate| candidate.parent_oid == trigger.oid)
            {
                state
                    .ops
                    .extend(crabka_pgcatalog::trigger::drop_trigger_ops(
                        clone.table_id,
                        &clone.name,
                    ));
            }
        }
    }
    for (_, _, _, _, _, row) in state.rows_mut(kv)? {
        if index < row.len() {
            row.remove(index);
        }
    }
    for foreign_key in state.current_foreign_keys(kv)? {
        if foreign_key.columns.iter().any(|name| name == column) {
            drop_foreign_key_constraint(kv, state, &foreign_key.name);
        }
    }
    for index_meta in crabka_pgcatalog::list_table_indexes(kv, &table_name)? {
        if index_meta
            .columns
            .iter()
            .any(|key| index_key_reads_column(&state.table, key, column))
            || index_meta.include.iter().any(|name| name == column)
        {
            drop_index_by_name(
                kv,
                state,
                &index_meta.name,
                &crate::error::DroppedObject::Index(index_meta.name.clone()),
                cascade,
            )?;
        }
    }
    state.table.columns.remove(index);
    let column_names: Vec<String> = state.table.columns.iter().map(|c| c.name.clone()).collect();
    state
        .table
        .checks
        .retain(|check| !check_references_column(&check.expr, column, &column_names));
    Ok(())
}

/// `DROP COLUMN` owns statistics definitions that name the removed attribute
/// directly or through an expression. Their derived payload shares the
/// definition record, so deleting that one record clears both catalogs.
fn drop_statistics_referencing_column_ops(
    kv: &dyn Kv,
    table: &Table,
    column: &str,
    index: usize,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    crabka_pgcatalog::statistics::list(kv)?
        .into_iter()
        .filter(|statistics| statistics_references_column(statistics, table, column, index))
        .map(|statistics| {
            crabka_pgcatalog::statistics::drop_ops(kv, &statistics.name).map_err(Into::into)
        })
        .collect::<Result<Vec<_>, ExecError>>()
        .map(|ops| ops.into_iter().flatten().collect())
}

fn clear_statistics_data_referencing_column_ops(
    kv: &dyn Kv,
    table: &Table,
    column: &str,
    index: usize,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    Ok(crabka_pgcatalog::statistics::list(kv)?
        .into_iter()
        .filter(|statistics| statistics_references_column(statistics, table, column, index))
        .map(|mut statistics| {
            statistics.data = None;
            crabka_pgcatalog::statistics::put_op(&statistics)
        })
        .collect())
}

fn statistics_references_column(
    statistics: &crabka_pgcatalog::statistics::Statistics,
    table: &Table,
    column: &str,
    index: usize,
) -> bool {
    let Ok(attnum) = i16::try_from(index + 1) else {
        return false;
    };
    let columns = table
        .columns
        .iter()
        .map(|candidate| candidate.name.clone())
        .collect::<Vec<_>>();
    statistics.table_id == table.id
        && (statistics.keys.contains(&attnum)
            || statistics
                .expressions
                .iter()
                .any(|expression| check_references_column(expression, column, &columns)))
}

fn triggers_referencing_column(
    kv: &dyn Kv,
    table: &crabka_pgcatalog::Table,
    column: &str,
) -> Result<Vec<String>, ExecError> {
    let mut triggers = crabka_pgcatalog::trigger::triggers_for_table(kv, table.id)?;
    triggers.sort_by_key(|trigger| trigger.oid);
    Ok(triggers
        .into_iter()
        .filter(|trigger| trigger_references_column(trigger, table, column))
        .map(|trigger| trigger.name)
        .collect())
}

fn trigger_references_column(
    trigger: &crabka_pgcatalog::trigger::Trigger,
    table: &crabka_pgcatalog::Table,
    column: &str,
) -> bool {
    trigger
        .events
        .update_columns
        .iter()
        .any(|name| name == column)
        || trigger.when.as_ref().is_some_and(|predicate| {
            check_references_column(
                predicate,
                column,
                &table
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>(),
            )
        })
}

/// Drop one index and its entries, refusing when a foreign key chose it as the
/// index proving its referenced columns unique.
///
/// `dropped` is how the *user's* statement named the object, which is what the
/// 2BP01's primary message quotes; every `DETAIL` line names the index, because
/// that is what the constraint actually depends on. `CASCADE` drops the
/// referencing constraints rather than the referencing relations.
pub(crate) fn drop_index_by_name(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    name: &str,
    dropped: &crate::error::DroppedObject,
    cascade: bool,
) -> Result<(), ExecError> {
    let (index, mut ops) =
        crabka_pgcatalog::drop_constraint_index_ops(kv, &state.table.name.sibling(name))?;
    let dependents = crate::fk::dependents_blocking_index_drop(kv, &index)?;
    let dependents: Vec<crate::error::DependentForeignKey> = dependents
        .into_iter()
        .filter(|dependent| {
            dependent.table != state.table.name
                || !state.dropped_foreign_keys.contains(&dependent.constraint)
        })
        .collect();
    if !dependents.is_empty() {
        if !cascade {
            return Err(ExecError::DependentForeignKeys(Box::new(
                crate::error::ForeignKeyDependents {
                    dropped: dropped.clone(),
                    dependents,
                },
            )));
        }
        for dependent in &dependents {
            let child = crabka_pgcatalog::get_table(kv, &dependent.table)?;
            let (_, drop_ops) =
                crabka_pgcatalog::drop_foreign_key_ops(kv, child.id, &dependent.constraint)?;
            ops.extend(drop_ops);
            state
                .dropped_foreign_keys
                .push(dependent.constraint.clone());
        }
    }
    for (key, _) in kv.scan_prefix(&crabka_pgkv::key::secondary_index_prefix(
        index.table_id,
        index.id,
    ))? {
        ops.push(crabka_pgkv::WriteOp::Delete { key });
    }
    state.ops.extend(ops);
    state.dropped_indexes.push(name.to_string());
    Ok(())
}

/// Rewrite everything that names the column being renamed: the table's stored
/// `CHECK` predicates, its secondary-index column lists, its comment key, and
/// every stored view whose text references it.
///
/// Views are stored as SQL text, so the rewrite is a token-level substitution
/// driven by catalog resolution rather than a blind search-and-replace. A
/// reference is rewritten only when the catalog proves it belongs to the
/// renamed relation:
///
/// * `q.<old>` where `q` is the renamed table's name or its alias in the view;
/// * a bare `<old>` when the renamed table is the *only* relation in the view's
///   `FROM` that has a column of that name.
///
/// Neither may be provable: the view references a relation the catalog cannot
/// resolve, or another referenced relation also has a column named `<old>` or
/// `<new>`. Then the whole `ALTER TABLE` fails with `0A000` naming the view, so
/// a rename can never silently change what a view returns.
pub(crate) fn rename_column_dependencies(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    old_name: &str,
    new_name: &str,
) -> Result<(), ExecError> {
    let table_name = state.table.name.clone();
    for check in &mut state.table.checks {
        check.expr = rewrite_identifier_tokens(&check.expr, old_name, new_name);
    }
    if let Some(foreign) = &mut state.table.foreign
        && let Some((name, _)) = foreign
            .column_options
            .iter_mut()
            .find(|(name, _)| name == old_name)
    {
        *name = new_name.to_string();
    }
    // A column grant follows its column. Leaving it under the old name would
    // strand the grant and hand it to the next column created with that name.
    let renamed: Vec<_> = crabka_pgcatalog::column_privileges_of(kv, &table_name)?
        .into_iter()
        .filter(|privilege| privilege.column == old_name)
        .collect();
    for privilege in renamed {
        let grantees = [privilege.grantee.clone()];
        let named = [privilege.privilege.clone()];
        state
            .ops
            .extend(crabka_pgcatalog::revoke_column_privileges_ops(
                kv,
                &table_name,
                std::slice::from_ref(&privilege.column),
                &grantees,
                &named,
            )?);
        state
            .ops
            .extend(crabka_pgcatalog::grant_column_privileges_ops(
                kv,
                &table_name,
                &[new_name.to_string()],
                &grantees,
                &named,
            )?);
    }
    // A partitioned parent names each key column, and routing resolves that
    // name against the live column list on every row. A key left holding the
    // old name would therefore stop resolving altogether, not merely misprint
    // in `pg_get_partkeydef` — so this rewrite is load-bearing, not cosmetic.
    // It runs per relation, so an intermediate parent reached by the recursion
    // is rewritten on its own pass.
    if let Some(mut scheme) = crate::partition::scheme_of(kv, &table_name)? {
        let mut touched = false;
        for key in &mut scheme.keys {
            if key == old_name {
                *key = new_name.to_string();
                touched = true;
            }
        }
        if touched {
            state
                .ops
                .extend(crate::partition::put_scheme_ops(&table_name, &scheme));
        }
    }
    for mut index in crabka_pgcatalog::list_table_indexes(kv, &table_name)? {
        if !index
            .columns
            .iter()
            .any(|key| index_key_reads_column(&state.table, key, old_name))
            && !index.include.iter().any(|column| column == old_name)
        {
            continue;
        }
        for key in &mut index.columns {
            if key == old_name {
                *key = new_name.to_string();
            } else if let Some(source) = crabka_pgcatalog::index_key_expression(key) {
                *key = crabka_pgcatalog::expression_index_key(&rewrite_identifier_tokens(
                    source, old_name, new_name,
                ));
            }
        }
        for column in &mut index.include {
            if column == old_name {
                *column = new_name.to_string();
            }
        }
        state.ops.extend(crabka_pgcatalog::put_index_ops(&index));
    }
    for mut trigger in crabka_pgcatalog::trigger::triggers_for_table(kv, state.table.id)? {
        let mut touched = false;
        for column in &mut trigger.events.update_columns {
            if column == old_name {
                *column = new_name.to_string();
                touched = true;
            }
        }
        if let Some(predicate) = &mut trigger.when {
            let rewritten = rewrite_identifier_tokens(predicate, old_name, new_name);
            touched |= rewritten != *predicate;
            *predicate = rewritten;
        }
        if touched {
            state
                .ops
                .extend(crabka_pgcatalog::trigger::put_trigger_ops(kv, &trigger)?);
        }
    }
    // Foreign keys store their columns as names, so both sides follow the
    // rename. A self-referencing constraint appears on both lists and is
    // rewritten once, with both column lists updated.
    let table_id = state.table.id;
    let mut foreign_keys = crabka_pgcatalog::list_table_foreign_keys(kv, table_id)?;
    for referencing in crabka_pgcatalog::list_referencing_foreign_keys(kv, table_id)? {
        if !foreign_keys
            .iter()
            .any(|seen| seen.table_id == referencing.table_id && seen.name == referencing.name)
        {
            foreign_keys.push(referencing);
        }
    }
    for mut foreign_key in foreign_keys {
        let rename = |columns: &mut Vec<String>| {
            let mut touched = false;
            for column in columns {
                if column == old_name {
                    *column = new_name.to_string();
                    touched = true;
                }
            }
            touched
        };
        let mut touched = false;
        if foreign_key.table_id == table_id {
            touched |= rename(&mut foreign_key.columns);
            touched |= rename(&mut foreign_key.set_columns);
        }
        if foreign_key.referenced_table_id == table_id {
            touched |= rename(&mut foreign_key.referenced_columns);
        }
        if touched {
            state
                .ops
                .extend(crabka_pgcatalog::put_foreign_key_ops(&foreign_key));
        }
    }
    let old_column = crabka_pgcatalog::CommentObject::Column(&table_name, old_name);
    if let Some(comment) = crabka_pgcatalog::get_comment(kv, "column", old_column)? {
        state
            .ops
            .push(crabka_pgcatalog::set_comment_op("column", old_column, None));
        state.ops.push(crabka_pgcatalog::set_comment_op(
            "column",
            crabka_pgcatalog::CommentObject::Column(&table_name, new_name),
            Some(&comment),
        ));
    }
    for mut view in crabka_pgcatalog::list_views(kv)? {
        let Some(rewritten) =
            rewrite_view_definition(kv, &view.definition, &table_name, old_name, new_name)?
        else {
            continue;
        };
        // The view's own output column names are unchanged: PostgreSQL keeps a
        // view's labels when a base column is renamed.
        view.definition = rewritten;
        state.ops.push(crabka_pgcatalog::put_view_op(&view));
    }
    Ok(())
}

/// The generated columns of `table` whose expression reads `column`.
///
/// `PostgreSQL` records a dependency from a generated column onto every column
/// its expression reads, so dropping or retyping one of those is refused.
pub(crate) fn generated_columns_reading(table: &Table, column: &str) -> Vec<String> {
    use crabka_pgparser::ast::Expr;

    let scope = Scope::single(table, &table.name.name);
    let target = table.column_index(column);
    table
        .columns
        .iter()
        .filter(|candidate| {
            let Some(source) = candidate.generation_expr() else {
                return false;
            };
            let Ok(expr) = crabka_pgparser::parser::parse_expression(source) else {
                return false;
            };
            let mut reads = false;
            crate::grouping::visit_expr(&expr, &mut |node| {
                if let Expr::Column {
                    table: qualifier,
                    name,
                } = node
                    && scope.resolve(qualifier.as_deref(), name).ok() == target
                    && target.is_some()
                {
                    reads = true;
                }
            });
            reads
        })
        .map(|candidate| candidate.name.clone())
        .collect()
}

pub(crate) fn index_key_reads_column(table: &Table, key: &str, column: &str) -> bool {
    match crabka_pgcatalog::index_key_expression(key) {
        Some(source) => expression_reads_column(table, source, column),
        None => key == column,
    }
}

/// Does the stored SQL source `expression`, read against `table`, reference
/// `table`'s `column`?
///
/// The question is answered by parsing and resolving, never by looking for the
/// name in the text. A column named `a` occurs inside the identifier `abc`,
/// inside the string `'a'` and inside the function name `avg`, and none of
/// those is a reference; `t.a` is one, and so is a bare `a` that resolves here.
///
/// A source that no longer parses answers `true`. The callers ask in order to
/// clean up after a dropped column, and treating an unreadable expression as
/// independent of the column would leave the dependency behind.
pub(crate) fn expression_reads_column(table: &Table, expression: &str, column: &str) -> bool {
    use crabka_pgparser::ast::Expr;

    let Ok(expr) = crabka_pgparser::parser::parse_expression(expression) else {
        return true;
    };
    let scope = Scope::single(table, &table.name.name);
    let target = table.column_index(column);
    let mut reads = false;
    crate::grouping::visit_expr(&expr, &mut |node| {
        if let Expr::Column {
            table: qualifier,
            name,
        } = node
            && scope.resolve(qualifier.as_deref(), name).ok() == target
            && target.is_some()
        {
            reads = true;
        }
    });
    reads
}

/// The row-security policies on `table` whose `USING` or `WITH CHECK` reads
/// `column`, in the order `pg_policy` stores them.
pub(crate) fn policies_reading_column(
    kv: &dyn Kv,
    table: &Table,
    column: &str,
) -> Result<Vec<crabka_pgcatalog::policy::Policy>, ExecError> {
    Ok(crabka_pgcatalog::policy::policies_for_table(kv, table.id)?
        .into_iter()
        .filter(|policy| {
            [policy.using.as_deref(), policy.with_check.as_deref()]
                .into_iter()
                .flatten()
                .any(|source| expression_reads_column(table, source, column))
        })
        .collect())
}

/// How a `DETAIL` line and a `NOTICE` line name a policy.
pub(crate) fn policy_dependency_line(
    table: &crabka_pgcatalog::RelationName,
    policy: &str,
) -> String {
    format!("policy {policy} on table {table}")
}

/// `PostgreSQL`'s 2BP01 for a `DROP COLUMN` that a policy still reads.
///
/// A policy's reference to a column is a `DEPENDENCY_NORMAL` `pg_depend` row
/// against `(pg_class, relid, attnum)`, so `performMultipleDeletions` under
/// `DROP_RESTRICT` refuses; the whole `pg_policy` row goes only under
/// `CASCADE`. See `ATExecDropColumn` and `reportDependentObjects`.
pub(crate) fn dependent_policy_refusal(
    table: &crabka_pgcatalog::RelationName,
    column: &str,
    dependents: &[crabka_pgcatalog::policy::Policy],
) -> ExecError {
    let depended_on = format!("column {column} of table {table}");
    let detail = dependents
        .iter()
        .map(|policy| {
            format!(
                "{} depends on {depended_on}",
                policy_dependency_line(table, &policy.name)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    ExecError::Remote(crabka_pgwire::error::PgError::error(
        "2BP01",
        format!(
            "cannot drop {depended_on} because other objects depend on it\nDETAIL:  \
             {detail}\nHINT:  Use DROP ... CASCADE to drop the dependent objects too."
        ),
    ))
}

/// Rewrite every stored view's references to a renamed relation.
///
/// `PostgreSQL` stores a view as a parsed rule over relation oids, so renaming
/// a table it reads is invisible to it; Crabka stores view *text*, so the
/// the substitution must happen. The rewrite touches only positions the token
/// walk can prove: a `FROM`/`JOIN` relation slot, and a `<table>.<column>`
/// qualifier when that item carries no alias. Any other occurrence of the name
/// is `0A000`, not a silent change of what the view returns.
pub(crate) fn rename_table_view_ops(
    kv: &dyn Kv,
    old_name: &crabka_pgcatalog::RelationName,
    new_name: &crabka_pgcatalog::RelationName,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let mut ops = Vec::new();
    for mut view in crabka_pgcatalog::list_views(kv)? {
        if !definition_mentions_identifier(&view.definition, &old_name.name) {
            continue;
        }
        let Some(rewritten) =
            rewrite_view_relation_name(&view.definition, &old_name.name, &new_name.name)
        else {
            return Err(
                crabka_pgcatalog::CatalogError::StoredViewDependency(old_name.to_string()).into(),
            );
        };
        view.definition = rewritten;
        ops.push(crabka_pgcatalog::put_view_op(&view));
    }
    Ok(ops)
}

/// The token indices at which a view definition names `relation` in a slot the
/// rewrite understands, or `None` when it names it anywhere else.
pub(crate) fn rewrite_view_relation_name(
    definition: &str,
    old_name: &str,
    new_name: &str,
) -> Option<String> {
    use crabka_pgparser::token::{Keyword, Token};

    let tokens = crabka_pgparser::lexer::lex(definition).ok()?;
    // A relation slot is the identifier directly after FROM/JOIN, or after a
    // comma continuing such a list. `(relation index, effective qualifier)`.
    let mut slots: Vec<(usize, String)> = Vec::new();
    let mut expect_relation = false;
    let mut in_from_list = false;
    for (index, (token, _)) in tokens.iter().enumerate() {
        match token {
            Token::Keyword(Keyword::From | Keyword::Join) => expect_relation = true,
            Token::Comma if in_from_list => expect_relation = true,
            Token::Ident(_) if expect_relation => {
                expect_relation = false;
                in_from_list = true;
                slots.push((index, view_from_item_qualifier(&tokens, index)));
            }
            Token::Keyword(Keyword::Where | Keyword::Group | Keyword::Order) => {
                expect_relation = false;
                in_from_list = false;
            }
            _ => expect_relation = false,
        }
    }
    // `t.col` may be rewritten only when the FROM item for `t` carries no alias
    // and no *other* item is named or aliased `t`.
    let bare = slots.iter().any(|(index, qualifier)| {
        qualifier == old_name && matches!(&tokens[*index].0, Token::Ident(w) if w == old_name)
    });
    let shadowed = slots.iter().any(|(index, qualifier)| {
        qualifier == old_name && !matches!(&tokens[*index].0, Token::Ident(w) if w == old_name)
    });
    let rewrite_qualifiers = bare && !shadowed;

    let mut targets: Vec<usize> = Vec::new();
    for (index, (token, _)) in tokens.iter().enumerate() {
        if !matches!(token, Token::Ident(word) if word == old_name) {
            continue;
        }
        let is_slot = slots.iter().any(|(slot, _)| *slot == index);
        let is_qualifier = matches!(tokens.get(index + 1).map(|t| &t.0), Some(Token::Dot));
        if is_slot || (is_qualifier && rewrite_qualifiers) {
            targets.push(index);
            continue;
        }
        return None;
    }
    substitute_identifier_tokens(definition, &tokens, &targets, old_name, new_name)
}

/// The name a `FROM` item at `index` is referenced by: its alias when one is
/// written, otherwise the relation's own name.
pub(crate) fn view_from_item_qualifier(
    tokens: &[(crabka_pgparser::token::Token, usize)],
    index: usize,
) -> String {
    use crabka_pgparser::token::{Keyword, Token};

    let relation = match &tokens[index].0 {
        Token::Ident(word) => word.clone(),
        _ => String::new(),
    };
    let mut next = index + 1;
    if matches!(
        tokens.get(next).map(|t| &t.0),
        Some(Token::Keyword(Keyword::As))
    ) {
        next += 1;
    }
    match tokens.get(next).map(|t| &t.0) {
        Some(Token::Ident(alias)) if !is_query_tail_keyword(alias) => alias.clone(),
        _ => relation,
    }
}

/// Substitute `old_name` with `new_name` at the given token indices, leaving all
/// other source text exactly as written, including other occurrences of the
/// same identifier.
///
/// `None` when a target is spelled as a quoted identifier: its source span is
/// longer than the token text, so the substitution cannot be made without
/// re-rendering the definition, and leaving it alone would silently break the
/// view.
pub(crate) fn substitute_identifier_tokens(
    source: &str,
    tokens: &[(crabka_pgparser::token::Token, usize)],
    targets: &[usize],
    old_name: &str,
    new_name: &str,
) -> Option<String> {
    let mut out = String::with_capacity(source.len());
    let mut cursor = 0usize;
    for index in targets {
        let offset = tokens[*index].1;
        if !source[offset..].starts_with(old_name) {
            return None;
        }
        out.push_str(&source[cursor..offset]);
        out.push_str(new_name);
        cursor = offset + old_name.len();
    }
    out.push_str(&source[cursor..]);
    Some(out)
}

pub(crate) fn definition_mentions_identifier(definition: &str, name: &str) -> bool {
    crabka_pgparser::lexer::lex(definition).is_ok_and(|tokens| {
        tokens.iter().any(|(token, _)| {
            matches!(token, crabka_pgparser::token::Token::Ident(word) if word == name)
        })
    })
}

/// The stored views that depend on `table`, or on one of its columns when
/// `column` is given. `PostgreSQL` tracks a view's dependency per column, so a
/// drop of a column no view reads is allowed.
/// Every relation one `DROP TABLE` statement will remove: each name it resolves,
/// plus the partitions that go with it. Sequence entries and (under `IF EXISTS`)
/// missing names contribute nothing.
/// [`crate::partition::is_partitioned`] for a relation named by the AST. The
/// DML dispatch tests it in a match guard, where the name must resolve first.
pub(crate) fn is_partitioned_ref(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    reference: &crabka_pgparser::ast::RelationRef,
) -> Result<bool, ExecError> {
    let name = resolve_relation(kv, resolution, reference, SchemaDisposition::Reference)?;
    crate::partition::is_partitioned(kv, &name)
}

/// Whether a write target has table-inheritance children to descend into.
///
/// This runs on every `UPDATE` and `DELETE`, so it stops at the first child key
/// rather than reading the child list: a relation with no direct children has no
/// descendants either, which is the answer nearly every write gets.
pub(crate) fn has_inheritance_children(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    reference: &crabka_pgparser::ast::RelationRef,
) -> Result<bool, ExecError> {
    let name = resolve_relation(kv, resolution, reference, SchemaDisposition::Reference)?;
    crate::inheritance::has_children(kv, &name)
}

/// The ops that clear the foreign keys blocking a `DROP TABLE`, or the 2BP01
/// that refuses it.
///
/// `CASCADE` drops the referencing *constraint*, not the referencing relation.
/// The child table survives, minus the key.
pub(crate) fn drop_blocking_foreign_keys(
    kv: &dyn Kv,
    table: &Table,
    dropping: &HashSet<crabka_pgcatalog::RelationName>,
    cascade: bool,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let dependents: Vec<crate::error::DependentForeignKey> =
        crate::fk::dependents_blocking_table_drop(kv, table)?
            .into_iter()
            .filter(|dependent| !dropping.contains(&dependent.table))
            .collect();
    if dependents.is_empty() {
        return Ok(Vec::new());
    }
    if !cascade {
        return Err(ExecError::DependentForeignKeys(Box::new(
            crate::error::ForeignKeyDependents {
                dropped: crate::error::DroppedObject::Table(table.name.to_string()),
                dependents,
            },
        )));
    }
    let mut ops = Vec::new();
    for dependent in &dependents {
        let child = crabka_pgcatalog::get_table(kv, &dependent.table)?;
        let (_, drop_ops) =
            crabka_pgcatalog::drop_foreign_key_ops(kv, child.id, &dependent.constraint)?;
        ops.extend(drop_ops);
    }
    Ok(ops)
}

pub(crate) fn dependent_view_names(
    kv: &dyn Kv,
    table: &crabka_pgcatalog::RelationName,
    column: Option<&str>,
) -> Result<Vec<crabka_pgcatalog::RelationName>, ExecError> {
    Ok(dependent_view_chain(kv, table, column)?
        .into_iter()
        .map(|(view, _)| view)
        .collect())
}

/// The dependency closure, each view paired with the relation it reads to reach
/// `table` — `table` itself for a direct dependent, an intermediate view for a
/// transitive one, which is the pair `PostgreSQL` names in its DETAIL.
///
/// The closure has to be transitive now that a view may read another view: a
/// `CASCADE` that removed only the direct dependents would leave the views
/// *those* fed pointing at nothing.
pub(crate) fn dependent_view_chain(
    kv: &dyn Kv,
    table: &crabka_pgcatalog::RelationName,
    column: Option<&str>,
) -> Result<
    Vec<(
        crabka_pgcatalog::RelationName,
        crabka_pgcatalog::RelationName,
    )>,
    ExecError,
> {
    // A materialized view depends on what its query reads exactly as a view
    // does — that is the whole of the dependency — so it is probed through the
    // same machinery, from a `View` synthesized out of its `Table` record. The
    // dependent's own kind is recovered from the catalog when a message names
    // it; the walk itself does not care which it is.
    let mut views: Vec<ViewProbe> = crabka_pgcatalog::list_views(kv)?
        .into_iter()
        .chain(
            crabka_pgcatalog::list_tables(kv)?
                .into_iter()
                .filter_map(materialized_as_view),
        )
        .map(ViewProbe::new)
        .collect();
    // A column restriction applies only to the direct step: once a view is a
    // dependent, everything reading *it* depends on it whole.
    let reads =
        |probe: &mut ViewProbe, target: &crabka_pgcatalog::RelationName, direct: bool| match column
            .filter(|_| direct)
        {
            Some(column) => probe.reads_column(kv, target, column),
            None => probe.reads_relation(target),
        };
    let mut found: Vec<(
        crabka_pgcatalog::RelationName,
        crabka_pgcatalog::RelationName,
    )> = Vec::new();
    for probe in &mut views {
        let name = probe.view.name.clone();
        if probe.can_reach_schema(&table.schema) && reads(probe, table, true) {
            found.push((name, table.clone()));
        }
    }
    // Depth-first from each direct dependent: everything reading a view is
    // reported directly after it, before the next sibling. That is the order
    // PostgreSQL's DETAIL lines carry -- `findDependentObjects` recurses -- so
    // three views where the third reads the first read out as first, third,
    // second rather than in discovery order. A view cannot read itself, so the
    // walk terminates.
    let mut frontier = 0;
    while frontier < found.len() {
        let via = found[frontier].0.clone();
        frontier += 1;
        let mut children = Vec::new();
        for probe in &mut views {
            let name = probe.view.name.clone();
            if name == via
                || name == *table
                || found.iter().any(|(found, _)| *found == name)
                || children.iter().any(|(child, _)| *child == name)
                || !probe.can_reach_schema(&via.schema)
                || !reads(probe, &via, false)
            {
                continue;
            }
            children.push((name, via.clone()));
        }
        for (offset, child) in children.into_iter().enumerate() {
            found.insert(frontier + offset, child);
        }
    }
    Ok(found)
}

/// One stored view, prepared for a walk that asks about several relations.
///
/// The definition is lexed once — every question starts by asking whether the
/// text even names the relation, which a set lookup answers — and parsed at
/// most once, only for a view that got past that filter. Without the memo a
/// transitive closure over a catalog of `n` views would re-parse each of them
/// once per dependent it finds.
pub(crate) struct ViewProbe {
    pub(crate) view: crabka_pgcatalog::View,
    pub(crate) tokens: Option<Vec<(crabka_pgparser::token::Token, usize)>>,
    pub(crate) identifiers: HashSet<String>,
    pub(crate) body: ParsedBody,
}

/// A stored definition's parse, computed the first time a question needs it.
pub(crate) enum ParsedBody {
    Unread,
    /// The definition is not one query this parser understands, which every
    /// caller over-approximates rather than guessing about.
    Unreadable,
    Body(Box<crabka_pgparser::ast::QueryExpr>),
}

impl ViewProbe {
    pub(crate) fn new(view: crabka_pgcatalog::View) -> Self {
        let tokens = crabka_pgparser::lexer::lex(&view.definition).ok();
        let identifiers = tokens
            .iter()
            .flatten()
            .filter_map(|(token, _)| match token {
                crabka_pgparser::token::Token::Ident(word) => Some(word.clone()),
                _ => None,
            })
            .collect();
        Self {
            view,
            tokens,
            identifiers,
            body: ParsedBody::Unread,
        }
    }

    /// Whether this view could read anything in `schema` at all.
    ///
    /// A definition is matched by its identifiers, not by resolved
    /// dependencies, so an unqualified `FROM orders` matches every `orders` in
    /// the database. That is harmless within one schema and wrong across
    /// schemas: a session's temporary `orders` would otherwise carry off a
    /// permanent view over a different table of the same name when the
    /// namespace is emptied. A view outside `schema` reaches into it only by
    /// naming it, so requiring the qualifier confines the match.
    pub(crate) fn can_reach_schema(&self, schema: &str) -> bool {
        self.view.name.schema == schema || self.identifiers.contains(schema)
    }

    /// The parsed body, or `None` for a definition this parser cannot read.
    pub(crate) fn body(&mut self) -> Option<&crabka_pgparser::ast::QueryExpr> {
        if matches!(self.body, ParsedBody::Unread) {
            self.body = match parse_view_body(&self.view.definition) {
                Some(body) => ParsedBody::Body(Box::new(body)),
                None => ParsedBody::Unreadable,
            };
        }
        match &self.body {
            ParsedBody::Body(body) => Some(body),
            ParsedBody::Unread | ParsedBody::Unreadable => None,
        }
    }

    /// Whether this view's body reads `target`.
    ///
    /// A definition that never writes the relation's name cannot read it,
    /// whatever its shape, so that check comes first and keeps the parse off
    /// the path for every view the drop does not concern.
    pub(crate) fn reads_relation(&mut self, target: &crabka_pgcatalog::RelationName) -> bool {
        if !self.identifiers.contains(&target.name) {
            return false;
        }
        let Some(body) = self.body() else {
            // Unparseable: the name is written somewhere, so it counts.
            return true;
        };
        crate::viewdeps::query_sources(body)
            .into_iter()
            .any(|source| {
                source.reference.name == target.name
                    && source
                        .reference
                        .schema
                        .as_deref()
                        .is_none_or(|schema| schema == target.schema)
            })
    }

    /// Whether this view's body reads `column` of `target`.
    ///
    /// The two coarse answers hold whatever shape the body has: a definition
    /// that never writes the column's name and has no `*` cannot read it, and
    /// one whose scopes cannot be flattened into a single `FROM` list might, so
    /// it counts. That order matters now that a body may open several scopes —
    /// a view over a derived table would otherwise pin every column of every
    /// table in the schema.
    pub(crate) fn reads_column(
        &mut self,
        kv: &dyn Kv,
        target: &crabka_pgcatalog::RelationName,
        column: &str,
    ) -> bool {
        use crabka_pgparser::token::Token;

        // A view that does not read the relation at all cannot read its column,
        // whatever else its body does. Without this gate the over-approximations
        // below answer for every view in the catalog: once view bodies could
        // contain joins and subqueries, a single `SELECT *` view made every
        // unrelated `ALTER TABLE … DROP COLUMN` report it as a dependent and
        // refuse the drop.
        if !self.reads_relation(target) {
            return false;
        }
        let Some(tokens) = self.tokens.clone() else {
            return true;
        };
        if tokens.iter().any(|(token, _)| *token == Token::Star) {
            return true;
        }
        if !self.identifiers.contains(column) {
            return false;
        }
        let Some(body) = self.body() else {
            return true;
        };
        let Some(bindings) = view_relation_bindings(kv, body, target) else {
            return true;
        };
        if bindings.qualifiers.is_empty() {
            return false;
        }
        tokens.iter().enumerate().any(|(index, (token, _))| {
            let Token::Ident(word) = token else {
                return false;
            };
            if word != column {
                return false;
            }
            if index >= 2
                && tokens[index - 1].0 == Token::Dot
                && matches!(&tokens[index - 2].0, Token::Ident(q) if bindings.qualifiers.contains(q))
            {
                return true;
            }
            let bare = index == 0 || tokens[index - 1].0 != Token::Dot;
            bare && !bindings.other_columns.iter().any(|other| other == column)
        })
    }
}

/// The 2BP01 that refuses a drop, with the `DETAIL` line `PostgreSQL` writes
/// per blocking object and the `CASCADE` hint that follows them.
pub(crate) fn dependent_objects_error(
    kv: &dyn Kv,
    message: &str,
    dependents: &[(
        crabka_pgcatalog::RelationName,
        crabka_pgcatalog::RelationName,
    )],
) -> ExecError {
    let detail = dependents
        .iter()
        .map(|(view, via)| {
            let kind = |name| relation_kind(kv, name).unwrap_or("table");
            format!("{} {view} depends on {} {via}", kind(view), kind(via))
        })
        .collect::<Vec<_>>()
        .join("\n");
    ExecError::Remote(
        crabka_pgwire::error::PgError::error("2BP01", message)
            .with_detail(detail)
            .with_hint("Use DROP ... CASCADE to drop the dependent objects too."),
    )
}

/// How a stored view's `FROM` list binds the relation being renamed: the
/// qualifiers under which it is visible (its own name plus every alias), and
/// the column names every *other* referenced relation contributes.
pub(crate) struct ViewRelationBindings {
    pub(crate) qualifiers: Vec<String>,
    pub(crate) other_columns: Vec<String>,
}

/// The stored view body of `definition`, or `None` when it is not one query
/// this parser understands.
pub(crate) fn parse_view_body(definition: &str) -> Option<crabka_pgparser::ast::QueryExpr> {
    let mut statements = crabka_pgparser::parse(definition).ok()?;
    if statements.len() != 1 {
        return None;
    }
    match statements.pop()? {
        Statement::Query(query) => Some(query),
        _ => None,
    }
}

/// The bindings for `table` inside `definition`. `None` means the definition
/// binds columns in a way this walk cannot flatten into one list, so no rewrite
/// may be attempted and no column dependency may be ruled out.
///
/// Only the flat shape answers `Some`: one `SELECT` whose `FROM` is a join tree
/// of plain relations the catalog can resolve. A derived table, a set
/// operation, a `WITH`, a set-returning function or a subquery anywhere in the
/// body opens a second scope, and a bare column reference in one scope says
/// nothing about the columns of the other — so those all answer `None` and the
/// caller over-approximates rather than guessing.
pub(crate) fn view_relation_bindings(
    kv: &dyn Kv,
    query: &crabka_pgparser::ast::QueryExpr,
    table: &crabka_pgcatalog::RelationName,
) -> Option<ViewRelationBindings> {
    if query.with.is_some()
        || !matches!(
            query.body,
            crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Select(_))
        )
    {
        return None;
    }
    let mut flat = true;
    let mut sources = Vec::new();
    crate::viewdeps::walk_query(query, &mut |node| match node {
        crate::viewdeps::Node::Relation(source) => {
            sources.push((source.reference.clone(), source.qualifier().to_string()));
        }
        crate::viewdeps::Node::Expr(expr) => flat &= query_children(expr).is_empty(),
        crate::viewdeps::Node::ComputedFrom | crate::viewdeps::Node::DataModifyingCte => {
            flat = false;
        }
    });
    if !flat {
        return None;
    }
    let mut qualifiers = Vec::new();
    let mut other_columns = Vec::new();
    for (reference, qualifier) in sources {
        // An unqualified reference is taken to name this relation, which is the
        // same over-approximation `view_can_reach_schema` already makes.
        let matches_table = reference.name == table.name
            && reference
                .schema
                .as_deref()
                .is_none_or(|schema| schema == table.schema);
        if matches_table {
            qualifiers.push(reference.name.clone());
            qualifiers.push(qualifier);
            continue;
        }
        let name = crabka_pgcatalog::RelationName::new(
            reference
                .schema
                .clone()
                .unwrap_or_else(|| table.schema.clone()),
            reference.name.clone(),
        );
        let columns = match crabka_pgcatalog::get_table(kv, &name) {
            Ok(other) => other.columns,
            Err(_) => crabka_pgcatalog::get_view(kv, &name).ok()?.columns,
        };
        other_columns.extend(columns.into_iter().map(|column| column.name));
    }
    qualifiers.sort();
    qualifiers.dedup();
    Some(ViewRelationBindings {
        qualifiers,
        other_columns,
    })
}

pub(crate) fn is_query_tail_keyword(word: &str) -> bool {
    matches!(
        word,
        "where" | "group" | "order" | "having" | "limit" | "offset" | "union" | "on" | "using"
    )
}

/// Rewrite one stored view definition, or `None` when it does not reference the
/// renamed column at all. Returns `0A000` when the rewrite cannot be proven safe.
pub(crate) fn rewrite_view_definition(
    kv: &dyn Kv,
    definition: &str,
    table: &crabka_pgcatalog::RelationName,
    old_name: &str,
    new_name: &str,
) -> Result<Option<String>, ExecError> {
    let tokens = match crabka_pgparser::lexer::lex(definition) {
        Ok(tokens) => tokens,
        Err(_) => return Ok(None),
    };
    let mentions_old = tokens.iter().any(|(token, _)| {
        matches!(token, crabka_pgparser::token::Token::Ident(word) if word == old_name)
    });
    if !mentions_old {
        return Ok(None);
    }
    let Some(bindings) = parse_view_body(definition)
        .as_ref()
        .and_then(|body| view_relation_bindings(kv, body, table))
    else {
        return Err(ExecError::Unsupported(format!(
            "cannot rename column \"{old_name}\" of relation \"{table}\": a stored view's \
             definition references a relation this catalog cannot resolve"
        )));
    };
    if bindings.qualifiers.is_empty() {
        return Ok(None);
    }
    if bindings
        .other_columns
        .iter()
        .any(|column| column == old_name || column == new_name)
    {
        return Err(ExecError::Unsupported(format!(
            "cannot rename column \"{old_name}\" of relation \"{table}\": a stored view joins \
             another relation with a column of the same name, so the reference is ambiguous"
        )));
    }
    Ok(Some(rewrite_identifier_tokens(
        definition, old_name, new_name,
    )))
}

/// Substitute every unquoted identifier token equal to `old_name` with
/// `new_name`, preserving all other source text verbatim.
pub(crate) fn rewrite_identifier_tokens(source: &str, old_name: &str, new_name: &str) -> String {
    let Ok(tokens) = crabka_pgparser::lexer::lex(source) else {
        return source.to_string();
    };
    let mut out = String::with_capacity(source.len());
    let mut cursor = 0usize;
    for (token, offset) in &tokens {
        let crabka_pgparser::token::Token::Ident(word) = token else {
            continue;
        };
        if word != old_name {
            continue;
        }
        // A quoted identifier's source span is longer than the token text; only
        // an unquoted spelling can be substituted safely.
        if !source[*offset..].starts_with(old_name) {
            continue;
        }
        out.push_str(&source[cursor..*offset]);
        out.push_str(new_name);
        cursor = offset + old_name.len();
    }
    out.push_str(&source[cursor..]);
    out
}

/// Move the metadata families that `pgexec` keys by relation *name* onto a new
/// name, so `ALTER TABLE … RENAME TO` is invisible to them.
///
/// The families `pgcatalog` owns move inside
/// [`crabka_pgcatalog::rename_table_ops`]; these three sit above it and it
/// cannot reach them. Leaving them behind is not a cosmetic loss. A renamed
/// partitioned parent stopped being partitioned at all — `relkind` fell to `r`,
/// its rows stopped being reachable through it, and its leaf's bound stopped
/// being enforced. A renamed inheritance parent lost every child, and the old
/// name it left behind was adopted, rows and all, by the next `CREATE TABLE`
/// that took it.
///
/// Two spellings change a relation's catalog name: `ALTER TABLE … RENAME TO`,
/// which `ALTER MATERIALIZED VIEW … RENAME TO` parses into, and
/// `ALTER SCHEMA … RENAME TO`, which moves every relation in a schema at once.
/// (`ALTER TABLE … SET SCHEMA` is still refused.) Both call this, so moving the
/// keys is sufficient and these need not be keyed by id instead.
pub(crate) fn rename_name_keyed_metadata_ops(
    kv: &dyn Kv,
    old_name: &crabka_pgcatalog::RelationName,
    new_name: &crabka_pgcatalog::RelationName,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let mut ops = crate::inheritance::rename_ops(kv, old_name, new_name)?;
    ops.extend(crate::partition::rename_ops(kv, old_name, new_name)?);
    ops.extend(crate::relstats::rename_ops(kv, old_name, new_name)?);
    ops.extend(crate::attrstats::rename_ops(kv, old_name, new_name)?);
    Ok(ops)
}

/// `ALTER SCHEMA … RENAME TO`.
///
/// [`crabka_pgcatalog::rename_schema_ops`] checks the rename and moves what the
/// schema itself owns. The relations are moved here, one at a time, each read
/// through a [`StagedKv`] over the batch so far. That is not an optimisation:
/// a link between two relations of the schema is stored at both ends, and a
/// second relation read from the base catalog would rebuild its end from the
/// state the first relation's move already replaced — a child's foreign key
/// would go on naming the schema its parent had just left.
///
/// Each relation also carries the families `pgexec` keys by relation name and
/// the catalog cannot reach, which are the ones
/// [`rename_name_keyed_metadata_ops`] moves for a table rename, plus its
/// triggers, whose key is id-based but whose record names its table.
pub(crate) fn rename_schema_ops(
    kv: &dyn Kv,
    name: &str,
    new_name: &str,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let mut ops = crabka_pgcatalog::rename_schema_ops(kv, name, new_name)?;
    reject_schema_qualified_definitions(kv, name)?;
    let relations = crabka_pgcatalog::schema_contents(kv, name)?;
    let staged = StagedKv::new(kv, &ops);
    for relation in &relations {
        let renamed = crabka_pgcatalog::RelationName::new(new_name, &relation.name);
        let mut moved = crabka_pgcatalog::move_relation_to_schema_ops(&staged, relation, &renamed)?;
        moved.extend(rename_name_keyed_metadata_ops(&staged, relation, &renamed)?);
        if let Ok(table) = crabka_pgcatalog::get_table(&staged, relation) {
            for mut trigger in crabka_pgcatalog::trigger::triggers_for_table(&staged, table.id)? {
                trigger.table = renamed.clone();
                moved.extend(crabka_pgcatalog::trigger::put_trigger_ops(
                    &staged, &trigger,
                )?);
            }
        }
        staged.stage(&moved);
        ops.extend(moved);
    }
    Ok(ops)
}

/// Refuse a schema rename that a stored definition spells the old schema into.
///
/// A view body resolves against the schema of the view that holds it, so an
/// unqualified reference follows its relation to the new schema on its own. A
/// written `oldschema.t` does not: the catalog keeps view and materialized-view
/// bodies as SQL text, and rewriting a qualifier there is the same problem that
/// makes [`rewrite_view_relation_name`] refuse the positions it cannot prove.
/// Refusing keeps the rename from quietly breaking a definition somewhere else
/// in the database.
pub(crate) fn reject_schema_qualified_definitions(
    kv: &dyn Kv,
    name: &str,
) -> Result<(), ExecError> {
    let views = crabka_pgcatalog::list_views(kv)?
        .into_iter()
        .map(|view| (view.name, view.definition));
    let materialized = crabka_pgcatalog::list_tables(kv)?
        .into_iter()
        .filter_map(|table| Some((table.name, table.materialized?.definition)));
    for (relation, definition) in views.chain(materialized) {
        if definition_mentions_identifier(&definition, name) {
            return Err(ExecError::Unsupported(format!(
                "cannot rename schema {name}: the definition of {relation} spells it out, and \
                 this catalog stores a definition as SQL text rather than as a dependency it \
                 could repoint"
            )));
        }
    }
    Ok(())
}

/// Move a relation's comments (and its columns') to a new relation name.
pub(crate) fn rename_relation_comment_ops(
    kv: &dyn Kv,
    old_name: &crabka_pgcatalog::RelationName,
    new_name: &crabka_pgcatalog::RelationName,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    use crabka_pgcatalog::CommentObject;

    let mut ops = Vec::new();
    if let Some(comment) =
        crabka_pgcatalog::get_comment(kv, "table", CommentObject::Relation(old_name))?
    {
        ops.push(crabka_pgcatalog::set_comment_op(
            "table",
            CommentObject::Relation(old_name),
            None,
        ));
        ops.push(crabka_pgcatalog::set_comment_op(
            "table",
            CommentObject::Relation(new_name),
            Some(&comment),
        ));
    }
    if let Ok(table) = crabka_pgcatalog::get_table(kv, old_name) {
        for column in &table.columns {
            let old_key = CommentObject::Column(old_name, &column.name);
            if let Some(comment) = crabka_pgcatalog::get_comment(kv, "column", old_key)? {
                ops.push(crabka_pgcatalog::set_comment_op("column", old_key, None));
                ops.push(crabka_pgcatalog::set_comment_op(
                    "column",
                    CommentObject::Column(new_name, &column.name),
                    Some(&comment),
                ));
            }
        }
    }
    Ok(ops)
}

/// `COMMENT ON <kind> <name> IS …`. The object must exist, with `PostgreSQL`'s
/// SQLSTATE for its kind when it does not.
/// Whether this table already has an index-backed constraint called `name`.
///
/// The catalog listing does not see an index this same statement created, and
/// still sees one it dropped, so the working state decides both.
pub(crate) fn constraint_index_named(
    kv: &dyn Kv,
    state: &AlterTableState,
    name: &str,
) -> Result<bool, ExecError> {
    if state.created_indexes.iter().any(|index| index.name == name) {
        return Ok(true);
    }
    Ok(crabka_pgcatalog::list_table_indexes(kv, &state.table.name)?
        .iter()
        .any(|index| {
            index.name == name
                && index.constraint.is_some()
                && !state.dropped_indexes.contains(&index.name)
        }))
}

/// The kind of relation `name` is, or `None` when no relation of that name
/// exists. Tables, views, materialized views, indexes, and sequences share one
/// namespace in `PostgreSQL`, so a name resolves to at most one of them.
///
/// The three stored kinds are all `Table` records here, so the word comes from
/// the record's relation-kind payload rather than from which catalog answered:
/// a foreign table and a materialized view both satisfy `get_table`, and
/// reporting either as `table` is what let `COMMENT ON TABLE` accept a foreign
/// table before this distinction existed.
///
/// A synthesised catalog relation is a relation with no record under any of
/// those keys, so it is asked for last — once every stored lookup has already
/// missed. That ordering is what keeps an ordinary relation's cost unchanged:
/// the extra question is answered from the name alone, and only a name no
/// stored key holds ever reaches it.
pub(crate) fn relation_kind(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Option<&'static str> {
    if let Ok(table) = crabka_pgcatalog::get_table(kv, name) {
        Some(stored_relation_kind(&table))
    } else if crabka_pgcatalog::get_view(kv, name).is_ok() {
        Some("view")
    } else if crabka_pgcatalog::get_index(kv, name).is_ok() {
        Some("index")
    } else if crabka_pgcatalog::get_sequence(kv, name).is_ok() {
        Some("sequence")
    } else {
        virtual_relation_kind(name)
    }
}

/// Which of the three kinds a stored relation is, in the word `PostgreSQL`
/// writes in a message about it.
pub(crate) fn stored_relation_kind(table: &crabka_pgcatalog::Table) -> &'static str {
    if table.foreign.is_some() {
        "foreign table"
    } else if table.materialized.is_some() {
        "materialized view"
    } else {
        "table"
    }
}

/// `DROP <kind> name` where `name` is a relation of some *other* kind.
///
/// `PostgreSQL`'s `DropErrorMsgWrongType` writes two things: the refusal names
/// the kind that was *asked for*, and the `HINT` names the command that would
/// have worked on the kind that is actually there. Both halves matter — the
/// message alone does not tell you what the relation is — and the relation is
/// named bare rather than schema-qualified, because the name in the message is
/// the one the statement wrote.
pub(crate) fn wrong_drop_kind_error(
    name: &crabka_pgcatalog::RelationName,
    requested: &str,
    actual: &str,
) -> ExecError {
    let article = |kind: &str| {
        if kind.starts_with(['a', 'e', 'i', 'o', 'u']) {
            "an"
        } else {
            "a"
        }
    };
    ExecError::Remote(
        crabka_pgwire::error::PgError::error(
            "42809",
            format!(
                "\"{}\" is not {} {requested}",
                name.name,
                article(requested)
            ),
        )
        .with_hint(format!(
            "Use DROP {} to remove {} {actual}.",
            actual.to_ascii_uppercase(),
            article(actual)
        )),
    )
}

/// `TRUNCATE` against a relation that is not a table.
///
/// `PostgreSQL`'s `truncate_check_rel` words this like the `DROP TABLE` refusal
/// — `"x" is not a table` — but emits no `HINT`, because there is no command it
/// could suggest: nothing truncates a materialized view.
pub(crate) fn wrong_relation_kind_write_error(name: &crabka_pgcatalog::RelationName) -> ExecError {
    ExecError::WrongObjectType(format!("\"{}\" is not a table", name.name))
}

/// The wrong-kind refusal a statement owes for `name`, or `None` when it owes
/// none.
///
/// `crabka_pgcatalog::get_table` answers `UndefinedTable` for every name a
/// view, sequence or index holds, because those live under other catalog keys.
/// A caller that only tries `get_table` therefore reports a relation of the
/// wrong *kind* as one that does not exist — 42P01 where `PostgreSQL` says
/// 42809 and names what the relation actually is. Threading that miss through
/// here recovers the kind and lets the caller word its own refusal.
///
/// `refusal` is asked about the kind that is really there and answers `None`
/// for a kind the statement *accepts*, which is how `LOCK TABLE` keeps working
/// on a view while refusing a sequence. `None` also comes back when no relation
/// of that name exists at all — then the caller's 42P01 was right all along.
pub(crate) fn wrong_kind(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
    refusal: impl FnOnce(&str) -> Option<ExecError>,
) -> Option<ExecError> {
    refusal(relation_kind(kv, name)?)
}

/// The plural noun `PostgreSQL`'s `errdetail_relkind_not_supported` writes for
/// a relation kind, as in `This operation is not supported for sequences.`
pub(crate) fn relkind_plural(kind: &str) -> &'static str {
    match kind {
        "view" => "views",
        "sequence" => "sequences",
        "index" => "indexes",
        "materialized view" => "materialized views",
        "foreign table" => "foreign tables",
        _ => "tables",
    }
}

/// A 42809 refusal carrying `PostgreSQL`'s relkind `DETAIL`.
///
/// A whole family of refusals — `ALTER action …`, `cannot open relation`,
/// `cannot lock relation`, `cannot create index on relation` — says only which
/// relation it would not touch, and leaves the reason to a `DETAIL` naming the
/// kind. The message is useless without it, so the two are built together.
///
/// [`ExecError::WrongObjectType`] is a bare string and cannot carry one, so
/// these go out as [`ExecError::Remote`], which `ExecError::into_pg` hands to
/// the wire untouched — the same route the `MERGE`-on-a-materialized-view
/// refusal already takes.
pub(crate) fn relkind_not_supported(message: String, kind: &str) -> ExecError {
    ExecError::Remote(
        crabka_pgwire::error::PgError::error("42809", message).with_detail(format!(
            "This operation is not supported for {}.",
            relkind_plural(kind)
        )),
    )
}

/// `TRUNCATE` against a relation of the wrong kind.
///
/// `PostgreSQL`'s `truncate_check_rel` refuses every kind it cannot empty with
/// the one wording, so a view, a sequence, an index and a materialized view all
/// get `"x" is not a table`.
///
/// Its system-catalog guard runs *after* that kind test, in that same function,
/// which is the whole reason the order here is observable: `TRUNCATE pg_class`
/// is a privilege refusal and `TRUNCATE pg_settings` a kind refusal, though
/// both name relations no `TRUNCATE` can empty.
pub(crate) fn truncate_wrong_kind(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Option<ExecError> {
    wrong_kind(kv, name, |kind| {
        (kind != "table" && kind != "foreign table").then(|| wrong_relation_kind_write_error(name))
    })
    .or_else(|| system_catalog_wrong_kind(name))
}

/// `CLUSTER` against a relation of the wrong kind.
///
/// `PostgreSQL` rejects a relation with no heap while the name is still being
/// opened. A materialized view has one and gets past this, to be refused later
/// for having no clustered index.
///
/// A synthesised catalog relation whose kind is `table` gets past it too, and
/// then lands on that same later refusal — `CLUSTER pg_class` is 42704, not the
/// privilege refusal the mutating statements earn, because reordering a heap
/// does not rewrite a catalog's definition.
pub(crate) fn cluster_wrong_kind(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Option<ExecError> {
    not_heap_bearing(kv, name)
        .or_else(|| is_virtual_relation(name).then(|| no_clustered_index(name)))
}

/// `"x" is not a table or materialized view`, for a relation that is neither.
///
/// The two statements that ask this — `CLUSTER` and `REINDEX TABLE` — want the
/// two kinds that carry a heap of their own, which is what an index hangs off
/// and what a reordering rewrites. Phrased as the kinds it accepts rather than
/// the kinds it refuses, because a foreign table shares the table catalog key
/// here: listing the refusals let one through, and `CLUSTER` on a foreign table
/// then reached the clustered-index lookup and reported that instead.
pub(crate) fn not_heap_bearing(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Option<ExecError> {
    wrong_kind(kv, name, |kind| {
        (kind != "table" && kind != "materialized view").then(|| {
            ExecError::WrongObjectType(format!(
                "\"{}\" is not a table or materialized view",
                name.name
            ))
        })
    })
}

/// `REINDEX TABLE` against a relation of the wrong kind.
///
/// The same two kinds `CLUSTER` accepts and the same wording, because both
/// statements want a relation with a heap under it. A partitioned table is
/// accepted too and is a `table` here, so it needs no arm; a partitioned
/// *index* is an `index` and is refused, which is the pair `create_index.sql`
/// writes back to back.
///
/// Where the two part company is what comes next: `CLUSTER` goes on to want a
/// clustered index, and `REINDEX` wants nothing more.
pub(crate) fn reindex_table_wrong_kind(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Option<ExecError> {
    not_heap_bearing(kv, name)
}

/// `REINDEX INDEX` against a relation of the wrong kind.
///
/// `PostgreSQL` words this one after the kind that was *asked for* rather than
/// after the kinds it would accept, and emits no `HINT` — unlike the `DROP`
/// family, which names the command that would have worked.
pub(crate) fn reindex_index_wrong_kind(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Option<ExecError> {
    wrong_kind(kv, name, |kind| {
        (kind != "index")
            .then(|| ExecError::WrongObjectType(format!("\"{}\" is not an index", name.name)))
    })
}

/// `REINDEX … CONCURRENTLY` against a system catalog.
///
/// This is the one refusal `REINDEX` owes a catalog. Unlike every other
/// statement that names one, `REINDEX` *rebuilds* rather than redefines, so
/// `allowSystemTableMods` never runs and `REINDEX TABLE pg_class` succeeds —
/// [`system_catalog_wrong_kind`]'s 42501 would be wrong here. What the catalog
/// cannot do is have its index swapped out while the catalog itself is what
/// records the swap.
///
/// Asked after the kind test, because a synthesised *view* takes the wrong-kind
/// refusal instead: `REINDEX TABLE CONCURRENTLY pg_settings` is 42809, not
/// this.
pub(crate) fn reindex_concurrently_system_catalog(
    name: &crabka_pgcatalog::RelationName,
) -> Option<ExecError> {
    is_system_catalog(name).then(reindex_concurrent_system_refusal)
}

/// `PostgreSQL`'s wording for a concurrent rebuild of a catalog index, which
/// `REINDEX SYSTEM CONCURRENTLY` reports for the whole database rather than for
/// a relation, so it names nothing.
pub(crate) fn reindex_concurrent_system_refusal() -> ExecError {
    ExecError::Remote(crabka_pgwire::error::PgError::error(
        "0A000",
        "cannot reindex system catalogs concurrently",
    ))
}

/// The options a `REINDEX` carries, read off the list as written.
pub(crate) struct ReindexOptions {
    /// Whether the rebuild was asked to be concurrent, by either spelling.
    pub(crate) concurrently: bool,
    /// The tablespace to move the rebuilt indexes into, which only has to
    /// exist here: nothing is rebuilt, so nothing moves.
    pub(crate) tablespace: Option<String>,
}

/// Read a `REINDEX` option list, refusing what `PostgreSQL`'s `ExecReindex`
/// refuses.
///
/// This runs before every other check, including the transaction-block guard:
/// `BEGIN; REINDEX (nosuchopt) TABLE CONCURRENTLY t` is the unrecognized
/// option, not the block.
///
/// # Errors
///
/// `42601` for an unknown option name, for a boolean option given a value that
/// is not one, and for `TABLESPACE` given no value at all.
pub(crate) fn reindex_options(
    stmt: &crabka_pgparser::ast::ReindexStmt,
) -> Result<ReindexOptions, ExecError> {
    let mut options = ReindexOptions {
        concurrently: stmt.concurrently,
        tablespace: None,
    };
    for (name, value) in &stmt.options {
        match name.as_str() {
            "verbose" => {
                utility_option_boolean(name, value.as_deref())?;
            }
            "concurrently" => {
                options.concurrently = utility_option_boolean(name, value.as_deref())?;
            }
            "tablespace" => {
                let Some(value) = value else {
                    return Err(ExecError::Syntax(format!("{name} requires a parameter")));
                };
                options.tablespace = Some(value.clone());
            }
            _ => {
                return Err(ExecError::Syntax(format!(
                    "unrecognized REINDEX option \"{name}\""
                )));
            }
        }
    }
    Ok(options)
}

/// A utility option written as a boolean, which `PostgreSQL` reads with
/// `defGetBoolean`: the bare name means true, and a value otherwise goes
/// through `parse_bool`, which takes any unambiguous prefix of the words it
/// knows as well as `1` and `0`.
///
/// # Errors
///
/// `42601 <option> requires a Boolean value`.
pub(crate) fn utility_option_boolean(name: &str, value: Option<&str>) -> Result<bool, ExecError> {
    let Some(value) = value else {
        return Ok(true);
    };
    match value.to_ascii_lowercase().as_str() {
        "on" | "t" | "tr" | "tru" | "true" | "y" | "ye" | "yes" | "1" => Ok(true),
        "of" | "off" | "f" | "fa" | "fal" | "fals" | "false" | "n" | "no" | "0" => Ok(false),
        _ => Err(ExecError::Syntax(format!(
            "{name} requires a Boolean value"
        ))),
    }
}

/// The tablespace a statement named, checked for existence alone.
///
/// Unlike [`resolve_relation_tablespace_oid`] this accepts `pg_global`, because
/// `REINDEX`'s refusal for it is per *index* — a table with no indexes takes
/// none — and there are no index files here to place anywhere.
///
/// # Errors
///
/// `42704` when no tablespace of that name exists.
pub(crate) fn require_tablespace(kv: &dyn Kv, name: &str) -> Result<(), ExecError> {
    match crabka_pgcatalog::tablespace_oid(kv, name) {
        Ok(_) => Ok(()),
        Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) => {
            Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                "42704",
                format!("tablespace \"{name}\" does not exist"),
            )))
        }
        Err(other) => Err(other.into()),
    }
}

/// `REINDEX DATABASE`/`REINDEX SYSTEM` naming a database that is not the open
/// one.
///
/// `PostgreSQL` compares the written name against `get_database_name` and
/// refuses anything else, including a database that does exist. The engine has
/// exactly one, so the comparison is against `open` — the session's own
/// database, the same name `current_database()` and `pg_database` answer with.
///
/// `open` is a parameter and not a constant on purpose. Against a constant the
/// rule inverted: `REINDEX DATABASE <the database you are in>` was refused
/// while `REINDEX DATABASE postgres` was accepted from every other database.
pub(crate) fn reindex_other_database(open: &str, name: Option<&str>) -> Option<ExecError> {
    name.filter(|name| *name != open).map(|_| {
        ExecError::Remote(crabka_pgwire::error::PgError::error(
            "0A000",
            "can only reindex the currently open database",
        ))
    })
}

/// `LOCK TABLE` against a relation of the wrong kind.
///
/// A view is lockable — `PostgreSQL` locks it and, recursively, what it reads —
/// so only the three kinds it refuses reach the refusal. A materialized view is
/// among them, which is why this cannot be phrased as "anything without a
/// heap".
pub(crate) fn lock_wrong_kind(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Option<ExecError> {
    wrong_kind(kv, name, |kind| {
        (kind == "sequence" || kind == "index" || kind == "materialized view")
            .then(|| relkind_not_supported(format!("cannot lock relation \"{}\"", name.name), kind))
    })
}

/// Reading or writing a relation that cannot be opened as one at all.
///
/// An index is the only kind that gets this: `PostgreSQL`'s `relation_open`
/// checks lock the relkind out before any statement-specific rule runs, so
/// `SELECT`, `INSERT`, `UPDATE`, `DELETE` and `COPY` all report the same thing
/// for an index and their own wordings for everything else.
///
/// Only the index key is read, not [`relation_kind`]'s four, because every
/// caller reaches this having already missed in `get_table` — so the one lookup
/// it adds is paid only by a statement that is failing anyway.
pub(crate) fn open_wrong_kind(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Option<ExecError> {
    crabka_pgcatalog::get_index(kv, name)
        .ok()
        .map(|_| relkind_not_supported(format!("cannot open relation \"{}\"", name.name), "index"))
}

/// `GRANT`/`REVOKE … ON <relation>` against a relation of the wrong kind.
///
/// Table privileges are the same privileges a view, a sequence and a
/// materialized view hold, so all three are granted on without complaint. An
/// index holds none — nothing can be granted or revoked on one — and
/// `PostgreSQL` says so in a wording that names what the relation *is* rather
/// than what it is not.
pub(crate) fn grant_wrong_kind(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Option<ExecError> {
    // Asked through `relation_kind` rather than the index key alone, because
    // unlike the other helpers this one runs with no `get_table` ahead of it,
    // and only the full lookup can say the name is an index and nothing else.
    // `GRANT` is a per-statement path, so the extra reads cost nothing that
    // matters.
    wrong_kind(kv, name, |kind| {
        (kind == "index")
            .then(|| ExecError::WrongObjectType(format!("\"{}\" is an index", name.name)))
    })
}

/// `CREATE INDEX ON name` where `name` is a relation that cannot carry one.
///
/// A materialized view can, which is why this cannot be phrased as "anything
/// that is not a table". An index gets [`open_wrong_kind`]'s wording instead,
/// because `PostgreSQL` rejects it while the relation is still being opened,
/// before `DefineIndex` has a relkind to complain about.
pub(crate) fn create_index_wrong_kind(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Option<ExecError> {
    wrong_kind(kv, name, |kind| match kind {
        "index" => Some(relkind_not_supported(
            format!("cannot open relation \"{}\"", name.name),
            kind,
        )),
        "view" | "sequence" | "foreign table" => Some(relkind_not_supported(
            format!("cannot create index on relation \"{}\"", name.name),
            kind,
        )),
        _ => None,
    })
}

/// `CREATE TABLE … INHERITS (name)` where `name` is a relation that cannot be
/// inherited from.
///
/// Only a table and a foreign table can be, and `PostgreSQL` words the refusal
/// after the relation name rather than after the command. An index is the
/// exception: it never gets that far, because `relation_open` locks its relkind
/// out first — the same split [`open_wrong_kind`] documents.
///
/// A materialized view is why this exists. It is stored under the table key
/// here, so `get_table` hands one back and the clause inherited its columns:
/// `CREATE TABLE c () INHERITS (mv)` built a child of a materialized view that
/// `PostgreSQL` refuses outright.
pub(crate) fn inherit_wrong_kind(name: &crabka_pgcatalog::RelationName, kind: &str) -> ExecError {
    if kind == "index" {
        return relkind_not_supported(format!("cannot open relation \"{}\"", name.name), kind);
    }
    ExecError::WrongObjectType(format!(
        "inherited relation \"{}\" is not a table or foreign table",
        name.name
    ))
}

/// Refuse `GRANT`/`REVOKE … ON <relation>` unless the relation is there and is
/// a kind that holds privileges.
///
/// Existence is asked here rather than inside the catalog's op builder because
/// a synthesised catalog relation holds no record under any stored key and
/// `PostgreSQL` still grants on it — `GRANT SELECT ON pg_proc` succeeds. Only
/// this side of the seam knows which names the engine synthesises.
///
/// # Errors
///
/// Returns 42P01 when no relation of that name exists, or 42809 when the
/// relation holds no privileges to grant.
pub(crate) fn require_grantable_relation(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Result<(), ExecError> {
    if let Some(error) = grant_wrong_kind(kv, name) {
        return Err(error);
    }
    if crabka_pgcatalog::relation_exists(kv, name)? || is_virtual_relation(name) {
        return Ok(());
    }
    Err(ExecError::Catalog(
        crabka_pgcatalog::CatalogError::UndefinedTable(name.to_string()),
    ))
}

/// `INSERT`/`UPDATE`/`DELETE` against a relation of the wrong kind.
///
/// A sequence has its own wording, and an index never gets as far as one —
/// [`open_wrong_kind`] refuses it while the relation is still being opened. A
/// view is writable when it is auto-updatable or carries an `INSTEAD OF`
/// trigger, so the view path owns that decision; a materialized view is refused
/// by the write pre-check that already names it.
///
/// Reads the two keys it can answer from rather than [`relation_kind`]'s four.
/// Its caller is on the pre-check every write statement runs and consults this
/// only once `get_table` has missed, so a write to a table pays nothing here.
pub(crate) fn write_wrong_kind(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Option<ExecError> {
    open_wrong_kind(kv, name).or_else(|| {
        crabka_pgcatalog::get_sequence(kv, name).ok().map(|_| {
            ExecError::WrongObjectType(format!("cannot change sequence \"{}\"", name.name))
        })
    })
}

/// The `DROP <kind>` refusal for `name`, or `None` when no relation of that
/// name exists at all — which is the caller's own 42P01, not a kind mismatch.
pub(crate) fn drop_kind_mismatch(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
    requested: &str,
) -> Option<ExecError> {
    let actual = relation_kind(kv, name)?;
    (actual != requested).then(|| wrong_drop_kind_error(name, requested, actual))
}

/// The materialized view `name` names, or `PostgreSQL`'s refusal for a relation
/// of any other kind.
///
/// `REFRESH MATERIALIZED VIEW` refuses a wrong relation kind in two different
/// wordings, and which one you get depends on how far `PostgreSQL` got:
///
/// * a relation with no heap at all — a view, a sequence, an index — is rejected
///   while the name is still being opened, as `42809 "x" is not a table or
///   materialized view`;
/// * a relation that *does* have a heap but is not a materialized view — an
///   ordinary or foreign table — gets past that and is rejected by the command
///   itself, as `0A000 "x" is not a materialized view`.
///
/// Neither carries a `HINT`, which is what separates them from
/// [`drop_kind_mismatch`]'s family.
pub(crate) fn require_materialized_view(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Result<crabka_pgcatalog::Table, ExecError> {
    let not_a_matview =
        || ExecError::Unsupported(format!("\"{}\" is not a materialized view", name.name));
    match crabka_pgcatalog::get_table(kv, name) {
        Ok(table) if table.materialized.is_some() => Ok(table),
        Ok(_) => Err(not_a_matview()),
        // Which of the two refusals a name earns follows from its *kind*, not
        // from which lookup answered: a synthesised catalog relation has no
        // record under any stored key, and one whose kind is `table` still has
        // to take the heap-bearing route the stored tables above take.
        Err(error @ crabka_pgcatalog::CatalogError::UndefinedTable(_)) => {
            match relation_kind(kv, name) {
                Some("table" | "foreign table") => Err(not_a_matview()),
                Some(_) => Err(ExecError::WrongObjectType(format!(
                    "\"{}\" is not a table or materialized view",
                    name.name
                ))),
                None => Err(error.into()),
            }
        }
        Err(error) => Err(error.into()),
    }
}

/// Refuse a read of a materialized view whose contents have never been
/// computed. Every other relation — and a populated materialized view — passes
/// through untouched.
pub(crate) fn require_populated(table: &crabka_pgcatalog::Table) -> Result<(), ExecError> {
    match &table.materialized {
        Some(matview) if !matview.populated => Err(ExecError::MaterializedViewNotPopulated(
            table.name.name.clone(),
        )),
        _ => Ok(()),
    }
}

/// `COMMENT ON <kind> <name>` names one relation kind and `PostgreSQL` enforces
/// it: a name that resolves to a relation of a *different* kind is 42809, and
/// only a name that resolves to nothing at all is the 42P01 relation lookup
/// failure.
pub(crate) fn require_relation_kind(
    kv: &dyn Kv,
    requested: &str,
    name: &crabka_pgcatalog::RelationName,
) -> Result<(), ExecError> {
    let Some(actual) = relation_kind(kv, name) else {
        return Err(ExecError::Catalog(
            crabka_pgcatalog::CatalogError::UndefinedTable(name.to_string()),
        ));
    };
    if actual == requested {
        return Ok(());
    }
    let article = if requested.starts_with(['a', 'e', 'i', 'o', 'u']) {
        "an"
    } else {
        "a"
    };
    // Named bare rather than schema-qualified, like every other wrong-kind
    // refusal: the name in the message is the one the statement wrote.
    Err(ExecError::WrongObjectType(format!(
        "\"{}\" is not {article} {requested}",
        name.name
    )))
}

pub(crate) fn comment_ops(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    object_kind: &str,
    object_name: &str,
    rule_table: Option<&crabka_pgparser::ast::RelationRef>,
    aggregate: Option<&crabka_pgparser::ast::AggregateSignature>,
    cast: Option<&(ColumnType, ColumnType)>,
    comment: Option<&str>,
    role: &str,
) -> Result<(QueryResult, Vec<crabka_pgkv::WriteOp>), ExecError> {
    use crabka_pgcatalog::CommentObject;

    if object_kind == "statistics" {
        let reference = match object_name.split_once('.') {
            Some((schema, name)) => crabka_pgparser::ast::RelationRef::qualified(schema, name),
            None => crabka_pgparser::ast::RelationRef::bare(object_name),
        };
        let name = resolve_relation(kv, resolution, &reference, SchemaDisposition::Utility)?;
        let object = crabka_pgcatalog::statistics::get(kv, &name)?
            .ok_or_else(|| crabka_pgcatalog::CatalogError::UndefinedObject(name.to_string()))?;
        crate::statistics_ddl::require_statistics_owner(kv, &object, role)?;
        let oid = object.oid.to_string();
        return Ok((
            command("COMMENT"),
            vec![crabka_pgcatalog::set_comment_op(
                object_kind,
                CommentObject::Named(&oid),
                comment,
            )],
        ));
    }

    if object_kind == "large object" {
        let oid = object_name
            .parse::<u32>()
            .map_err(|_| ExecError::Syntax("large object comments require an OID".into()))?;
        let _ = crabka_pgcatalog::largeobject::get_metadata(kv, oid)?;
        return Ok((
            command("COMMENT"),
            vec![crabka_pgcatalog::set_comment_op(
                object_kind,
                CommentObject::Named(object_name),
                comment,
            )],
        ));
    }

    if object_kind == "foreign data wrapper" {
        let _ = crabka_pgcatalog::get_fdw(kv, object_name)?;
        return Ok((
            command("COMMENT"),
            vec![crabka_pgcatalog::set_comment_op(
                object_kind,
                CommentObject::Named(object_name),
                comment,
            )],
        ));
    }

    if object_kind == "server" {
        let _ = crabka_pgcatalog::get_server(kv, object_name)?;
        return Ok((
            command("COMMENT"),
            vec![crabka_pgcatalog::set_comment_op(
                object_kind,
                CommentObject::Named(object_name),
                comment,
            )],
        ));
    }

    if object_kind == "aggregate" {
        let signature = aggregate.expect("parser records every aggregate comment signature");
        let Some(routine) = crate::useragg::resolve_signature(kv, signature)? else {
            return Err(crate::useragg::undefined_aggregate(format!(
                "aggregate {} does not exist",
                crate::useragg::spelled(signature)
            )));
        };
        return Ok((
            command("COMMENT"),
            vec![crabka_pgcatalog::set_comment_op(
                object_kind,
                CommentObject::Named(&routine.identity()),
                comment,
            )],
        ));
    }

    if object_kind == "cast" {
        let &(source, target) = cast.expect("parser records every cast comment signature");
        let cast =
            crabka_pgcatalog::get_user_cast(kv, source.oid(), target.oid())?.ok_or_else(|| {
                ExecError::UndefinedObject(format!(
                    "cast from type {} to type {} does not exist",
                    source.name(),
                    target.name()
                ))
            })?;
        return Ok((
            command("COMMENT"),
            vec![crabka_pgcatalog::set_comment_op(
                object_kind,
                CommentObject::Named(&cast.oid.to_string()),
                comment,
            )],
        ));
    }

    if object_kind == "access method" {
        let oid = crate::catalog_rel::access_method_oid(object_name)
            .map(|oid| u32::try_from(oid).expect("built-in access method oid is positive"))
            .map_or_else(
                || {
                    crabka_pgcatalog::list_access_methods(kv)?
                        .into_iter()
                        .find(|method| method.name == object_name)
                        .map(|method| method.oid)
                        .ok_or_else(|| {
                            ExecError::UndefinedObject(format!(
                                "access method \"{object_name}\" does not exist"
                            ))
                        })
                },
                Ok,
            )?;
        return Ok((
            command("COMMENT"),
            vec![crabka_pgcatalog::set_comment_op(
                object_kind,
                CommentObject::Named(&oid.to_string()),
                comment,
            )],
        ));
    }

    if matches!(object_kind, "type" | "domain") {
        let reference = match object_name.split_once('.') {
            Some((schema, name)) => crabka_pgparser::ast::RelationRef::qualified(schema, name),
            None => crabka_pgparser::ast::RelationRef::bare(object_name),
        };
        let name = resolve_relation(kv, resolution, &reference, SchemaDisposition::Reference)?;
        let Some(ty) = crabka_pgcatalog::get_user_type(kv, &name)? else {
            return Err(ExecError::UndefinedObject(format!(
                "type \"{name}\" does not exist"
            )));
        };
        if object_kind == "domain" && ty.domain().is_none() {
            return Err(ExecError::WrongObjectType(format!(
                "\"{name}\" is not a domain"
            )));
        }
        return Ok((
            command("COMMENT"),
            vec![crabka_pgcatalog::set_comment_op(
                object_kind,
                CommentObject::Named(&ty.oid.to_string()),
                comment,
            )],
        ));
    }

    if matches!(object_kind, "rule" | "trigger") {
        let table = rule_table.expect("parser records COMMENT ON RULE/TRIGGER's relation");
        let table = resolve_relation(kv, resolution, table, SchemaDisposition::Utility)?;
        let table = crate::trigger::relation_trigger_table(kv, &table)?;
        let oid = if object_kind == "rule" {
            crabka_pgcatalog::rule::get_rule(kv, table.id, object_name)?
                .ok_or_else(|| {
                    ExecError::UndefinedObject(format!(
                        "rule \"{object_name}\" for relation \"{}\" does not exist",
                        table.name
                    ))
                })?
                .oid
        } else {
            crabka_pgcatalog::trigger::get_trigger(kv, table.id, object_name)?
                .ok_or_else(|| {
                    ExecError::UndefinedObject(format!(
                        "trigger \"{object_name}\" for table \"{}\" does not exist",
                        table.name
                    ))
                })?
                .oid
        }
        .to_string();
        return Ok((
            command("COMMENT"),
            vec![crabka_pgcatalog::set_comment_op(
                object_kind,
                CommentObject::Named(&oid),
                comment,
            )],
        ));
    }

    // `Statement::Comment` flattens a column comment's target to
    // `relation.column`, so the relation half is recovered here and resolved
    // like any other written name.
    let (written, column) = match object_kind {
        "column" => {
            let (relation, column) = object_name.rsplit_once('.').ok_or_else(|| {
                ExecError::Syntax("column comments require a table-qualified name".into())
            })?;
            (relation, Some(column))
        }
        _ => (object_name, None),
    };
    let reference = match written.split_once('.') {
        Some((schema, name)) => crabka_pgparser::ast::RelationRef::qualified(schema, name),
        None => crabka_pgparser::ast::RelationRef::bare(written),
    };
    let relation = resolve_relation(kv, resolution, &reference, SchemaDisposition::Utility)?;
    let object = match object_kind {
        "table" | "view" | "materialized view" | "foreign table" | "index" | "sequence" => {
            require_relation_kind(kv, object_kind, &relation)?;
            CommentObject::Relation(&relation)
        }
        "column" => {
            let column = column.expect("a column comment always splits off a column");
            // A synthesised catalog relation has a column list without having a
            // record under the table key, and `PostgreSQL` comments on its
            // columns like any other's. Consulted only once `get_table` has
            // missed, so an ordinary relation pays nothing for it.
            let table = match crabka_pgcatalog::get_table(kv, &relation) {
                Ok(table) => table,
                Err(error) => virtual_relation_table(&relation).ok_or(error)?,
            };
            if table.column_index(column).is_none() {
                return Err(ExecError::UndefinedTableColumn {
                    column: column.to_string(),
                    table: relation.to_string(),
                });
            }
            CommentObject::Column(&relation, column)
        }
        other => {
            return Err(ExecError::Unsupported(format!(
                "COMMENT ON {other} is not supported"
            )));
        }
    };
    Ok((
        command("COMMENT"),
        vec![crabka_pgcatalog::set_comment_op(
            object_kind,
            object,
            comment,
        )],
    ))
}
