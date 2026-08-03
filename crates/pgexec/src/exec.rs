//! Per-statement execution.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt::Write as _,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use crabka_pgcatalog::{Column, ColumnDefault, Sequence, Table, TableId};
use crabka_pgkv::Kv;
use crabka_pgparser::ast::{
    ArraySubscript, Expr, FuncArgs, OrderItem, SelectItem, SelectStmt, Statement, UtilityStatement,
};
use crabka_pgtypes::{ColumnType, Datum};
use crabka_pgwire::engine::{Cell, FieldDescription, QueryResult};
use crabka_units::convert::ByteSizeExt as _;
use tracing::Instrument as _;
use zerocopy::{FromBytes, byteorder::big_endian::U64};

use crate::{
    error::ExecError,
    foreign::{ForeignScanner, ScanBounds},
    join::{
        PreparedJoinIndex, Relation, join_relations, join_relations_prepared, prepare_join_index,
    },
    relname::{SchemaDisposition, is_missing_schema, resolve_relation, resolve_relations},
    scanner::{
        JoinExecutionStrategy, JoinKind as ScannerJoinKind, JoinRangeRequest, JoinRow,
        JoinSnapshot, JoinTableInterval, PredicatePushdown, RowInterval, ScanRequest, ScannedRow,
    },
    scope::{ColumnBinding, Scope},
    timestamp_txn::{PrimaryTxnDecision, ReadTimestamp, TimestampTransactionId, TimestampWrite},
};

/// Work staged by a sharded-table timestamp DML statement.
pub struct TimestampWritePlan {
    /// SQL command result to return after a successful timestamp commit.
    pub result: QueryResult,
    /// Row/index intents prewritten and resolved by the timestamp participant.
    pub writes: Vec<TimestampWrite>,
    /// Extra durable ops folded into the timestamp commit decision batch.
    pub commit_ops: Vec<crabka_pgkv::WriteOp>,
}

/// SP40: the foreign-table read context threaded through the SELECT pipeline.
///
/// It carries the registered scanner (the `kafka_fdw` seam) and the current
/// user, which resolves the per-user `UserMapping`. One borrowed struct holds
/// both, so the already-wide read signatures gain a single argument and not
/// two. Paths that never reach a registered scanner (`describe`, the
/// schema-only build) use `ForeignCtx::none()`. A foreign `SELECT` with no
/// scanner registered returns `0A000` ("foreign tables require the `kafka`
/// feature").
#[derive(Clone, Copy)]
pub(crate) struct ForeignCtx<'a> {
    pub scanner: Option<&'a Arc<dyn ForeignScanner>>,
    pub current_user: &'a str,
    /// The session's name-resolution scope, so every DDL statement resolves an
    /// unqualified name against the same `search_path` a `SELECT` does.
    pub resolution: &'a crate::relname::ResolutionScope,
    /// The session's catalog handle, so a DDL-time expression can read the
    /// catalog. A `DEFAULT 'name'::regclass` has a relation to resolve. `None`
    /// outside a session, where such an expression keeps its 0A000.
    pub catalog: Option<&'a Arc<dyn Kv>>,
    /// Table ids the session claimed from the shared counter, drained from the
    /// back as this statement creates relations.
    ///
    /// A list rather than a single id because one statement can create many
    /// relations: `IMPORT FOREIGN SCHEMA` creates one per table the scanner
    /// discovers. `None` outside a session, where every id comes from the
    /// counter.
    pub reserved_table_ids: Option<&'a std::sync::Mutex<Vec<crabka_pgcatalog::TableId>>>,
    /// The xid of the open transaction, when this DDL runs inside one.
    ///
    /// A unique-index backfill must see the rows its OWN transaction has written
    /// but not yet committed: `BEGIN; CREATE TABLE t; INSERT …; CREATE UNIQUE
    /// INDEX ON t;` must back-validate against those rows, and the engine must
    /// reject a later insert of a duplicate. Without this the backfill scanned
    /// only committed rows, so the index was built empty and the duplicate was
    /// accepted and then committed. That left a table that violates its own
    /// unique index.
    pub own_xid: Option<u64>,
}

impl ForeignCtx<'_> {
    /// A context with no scanner and the conventional `"public"` user, for
    /// paths that never reach a registered scanner (schema-only describe).
    pub(crate) fn none() -> Self {
        Self {
            scanner: None,
            current_user: "public",
            resolution: crate::relname::ResolutionScope::default_scope(),
            catalog: None,
            reserved_table_ids: None,
            own_xid: None,
        }
    }

    /// The next reserved id, or the shared counter when there is no block or the
    /// block is spent.
    fn table_id(&self) -> crabka_pgcatalog::TableIdSource {
        self.reserved_table_ids
            .and_then(|reserved| reserved.lock().expect("table ids").pop())
            .map_or(
                crabka_pgcatalog::TableIdSource::Counter,
                crabka_pgcatalog::TableIdSource::Reserved,
            )
    }
}

/// Map a refused blocking acquire to its statement-level error (both 40P01).
fn lock_acquire_error(error: crate::lockmgr::AcquireError) -> ExecError {
    match error {
        crate::lockmgr::AcquireError::Deadlock => ExecError::Deadlock,
        crate::lockmgr::AcquireError::CapExpired => ExecError::LockWaitCapExpired,
    }
}

pub(crate) struct WriteContext<'a> {
    pub catalog_kv: &'a dyn Kv,
    pub kv: &'a dyn Kv,
    pub global: &'a dyn Kv,
    pub global_snapshot: &'a crabka_pgmvcc::visibility::Snapshot,
    pub procarray: &'a crate::procarray::ProcArray,
    pub lockmgr: &'a crate::lockmgr::RowLockManager,
    pub seq: &'a crate::seq::SequenceManager,
    pub snapshot: &'a crabka_pgmvcc::visibility::Snapshot,
    pub xid: u64,
    pub repeatable_read: bool,
    pub eval_ctx: &'a crate::clock::EvalCtx,
    /// `Some(garbage horizon)` iff this statement may opportunistically prune
    /// dead versions of the rows it writes. The session computes it once per
    /// statement. A prune happens only where a row's chain was re-read under
    /// its exclusive lock, which is UPDATE, DELETE, and `INSERT … ON CONFLICT
    /// DO UPDATE`. So a plain INSERT never prunes whatever this carries. `None`
    /// disables pruning entirely, for callers with no horizon to offer. The
    /// prune deletes ride this statement's own commit batch, so they replicate
    /// and replay like any other write.
    pub prune_horizon: Option<u64>,
    /// Bound on every lock wait this statement performs (`None` waits
    /// indefinitely under the engine-local deadlock detector). Set for
    /// sessions that can be enlisted in a cross-range transaction, whose
    /// deadlock cycles span engines and are invisible to any one engine's
    /// wait-for graph.
    pub lock_wait_cap: Option<std::time::Duration>,
    /// SP40: the foreign-table read context, forwarded so a query feeding a
    /// write (`INSERT … SELECT`, `UPDATE … FROM`, `MERGE … USING`) can read
    /// through the registered scanner.
    pub fctx: ForeignCtx<'a>,
    /// The ordinary-table scanner seam the same feeding queries read through.
    pub range_scanner: &'a dyn crate::scanner::RangeScanner,
    /// Memory available to blocking reads that feed this write.
    pub blocking_query_memory: crabka_units::ByteSize,
    /// The CTE scope the statement starts from (empty for a plain statement).
    pub ctes: &'a crate::cte::CteContext,
    /// The open transaction's deferred referential checks, which the
    /// end-of-statement drain promotes into and `COMMIT` drains.
    ///
    /// `None` in autocommit, where the statement *is* the transaction: nothing
    /// is promoted, because no later statement could repair a violation and the
    /// end-of-statement drain and a commit-time one would report the same thing
    /// at the same moment.
    pub deferred_fk: Option<&'a std::sync::Mutex<crate::fk::DeferredConstraints>>,
}

impl<'a> WriteContext<'a> {
    /// The read context a write's feeding query runs under: the write's own
    /// snapshot and xid, so it sees this transaction's earlier statements but
    /// not this statement's own (uncommitted, unwritten) rows.
    fn read_ctx<'b>(&'b self, ctes: &'b crate::cte::CteContext) -> crate::subquery::SubCtx<'b>
    where
        'a: 'b,
    {
        crate::subquery::SubCtx {
            catalog_kv: self.catalog_kv,
            kv: self.kv,
            global: self.global,
            gsnap: self.global_snapshot,
            snapshot: self.snapshot,
            own: Some(self.xid),
            ctes,
            eval_ctx: self.eval_ctx,
            fctx: self.fctx,
            range_scanner: self.range_scanner,
            blocking_query_memory: self.blocking_query_memory,
        }
    }
}

#[derive(Clone, Copy)]
struct MvccReadContext<'a> {
    kv: &'a dyn Kv,
    global: &'a dyn Kv,
    global_snapshot: &'a crabka_pgmvcc::visibility::Snapshot,
    snapshot: &'a crabka_pgmvcc::visibility::Snapshot,
    own: Option<u64>,
}

impl WriteContext<'_> {
    fn mvcc_read(&self) -> MvccReadContext<'_> {
        MvccReadContext {
            kv: self.kv,
            global: self.global,
            global_snapshot: self.global_snapshot,
            snapshot: self.snapshot,
            own: Some(self.xid),
        }
    }
}

struct MutationContext<'a> {
    kv: &'a dyn Kv,
    global: &'a dyn Kv,
    procarray: &'a crate::procarray::ProcArray,
    snapshot: &'a crabka_pgmvcc::visibility::Snapshot,
    xid: u64,
    repeatable_read: bool,
}

impl WriteContext<'_> {
    fn mutation(&self) -> MutationContext<'_> {
        MutationContext {
            kv: self.kv,
            global: self.global,
            procarray: self.procarray,
            snapshot: self.snapshot,
            xid: self.xid,
            repeatable_read: self.repeatable_read,
        }
    }

    /// [`WriteContext::mutation`] reading through the statement's pending write
    /// batch layered over the store.
    ///
    /// The one caller is a referential action's re-read of the row it is about
    /// to change. `PostgreSQL` runs the action as a query of its own, once the
    /// command's rows exist, so the version it operates on is the one the
    /// command itself last wrote, an image that is only staged here. A read of
    /// the store instead would stamp a version the command has already
    /// superseded and leave its replacement live.
    fn staged_mutation<'b>(&'b self, staged: &'b StagedKv<'b>) -> MutationContext<'b> {
        MutationContext {
            kv: staged,
            ..self.mutation()
        }
    }
}

/// Read a table's durable next-rowid (1 if unset). Single source of truth for
/// the sequence read.
pub(crate) fn read_seq_kv(kv: &dyn Kv, table: TableId) -> Result<u64, ExecError> {
    match kv.get(&crabka_pgkv::key::seq_key(table))? {
        Some(b) => {
            let (v, _) = U64::read_from_prefix(b.as_slice())
                .map_err(|_| crabka_pgkv::KvError::CorruptRow("sequence is not u64".into()))?;
            Ok(v.get())
        }
        None => Ok(1),
    }
}

/// DDL (CREATE/DROP TABLE) reads the catalog and builds its write batch WITHOUT
/// persisting it. The session routes the returned ops through the durable-write
/// seam, so DDL replicates too. The session holds the catalog lock across the
/// read and the commit, which serializes DDL globally. Non-DDL is unreachable
/// here, because `run_one` routes it, but this handles it defensively to keep
/// the match total. Validation (42P07 on duplicate, 42P01 on a missing drop) is
/// unchanged. Only the write destination moved.
pub(crate) fn execute_ddl(
    kv: &dyn Kv,
    stmt: &Statement,
    fctx: ForeignCtx,
) -> Result<(QueryResult, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let resolution = fctx.resolution;
    match stmt {
        Statement::Utility(UtilityStatement::TextSearch(ddl)) => {
            let (tag, ops) = crate::text_search_catalog::execute(kv, ddl)?;
            Ok((command(tag), ops))
        }
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
        Statement::CreateRoutine(routine) => crate::routine::create(kv, routine, fctx.current_user),
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
        } => crate::routine::alter(kv, *object, routine, action),
        // T5: user-defined types. Definition, lifecycle and catalog storage
        // live in `usertype`; only the DDL routing is here.
        Statement::CreateType { name, definition } => crate::usertype::create_type(
            kv,
            &resolve_relation(kv, resolution, name, SchemaDisposition::Utility)?.to_string(),
            definition,
        ),
        Statement::AlterType { name, action } => crate::usertype::alter_type(
            kv,
            &resolve_relation(kv, resolution, name, SchemaDisposition::Utility)?.to_string(),
            action,
        ),
        Statement::DropType {
            names,
            if_exists,
            cascade,
        } => crate::usertype::drop_types(
            kv,
            &resolve_relations(kv, resolution, names, SchemaDisposition::Utility)?
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            *if_exists,
            *cascade,
            false,
        ),
        Statement::CreateDomain {
            name,
            base,
            constraints,
        } => crate::usertype::create_domain(
            kv,
            &resolve_relation(kv, resolution, name, SchemaDisposition::Utility)?.to_string(),
            *base,
            constraints,
        ),
        Statement::AlterDomain { name, action } => crate::usertype::alter_domain(
            kv,
            &resolve_relation(kv, resolution, name, SchemaDisposition::Utility)?.to_string(),
            action,
        ),
        Statement::DropDomain {
            names,
            if_exists,
            cascade,
        } => crate::usertype::drop_types(
            kv,
            &resolve_relations(kv, resolution, names, SchemaDisposition::Utility)?
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
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
            like,
            inherits,
            on_commit,
            partition_by,
            partition_of,
            tablespace,
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
            let ddl_ctx = crate::clock::EvalCtx::for_ddl(resolution, fctx.catalog);
            let inheritance_parents = inherits
                .iter()
                .map(|parent| {
                    resolve_relation(kv, resolution, parent, SchemaDisposition::Reference)
                })
                .collect::<Result<Vec<_>, _>>()?;
            // A partition declares no columns of its own: it inherits the
            // parent's list, along with the parent's CHECK constraints, and may
            // only add qualifiers to what it inherits.
            let (cols, checks, serial_sequences, pending_indexes, pending_foreign_keys) =
                match partition_of {
                    Some(spec) => {
                        partition_definition(kv, name, spec, constraints, like, &ddl_ctx)?
                    }
                    None if inheritance_parents.is_empty() => {
                        create_table_definition(kv, name, columns, constraints, like, &ddl_ctx)?
                    }
                    None => inherited_table_definition(
                        kv,
                        name,
                        &inheritance_parents,
                        columns,
                        constraints,
                        like,
                        &ddl_ctx,
                    )?,
                };
            // `fk::resolve_foreign_key` refuses a sharded relation itself, but
            // `Table` carries no partition flag, so this is the only place that
            // knows a partitioned relation is being defined.
            if (partition_by.is_some() || partition_of.is_some())
                && let Some(pending) = pending_foreign_keys.first()
            {
                return Err(reject_partitioned_foreign_key(&pending.name));
            }
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
                crabka_pgcatalog::TableOptions { sharded: *sharded },
                sharding.as_ref(),
                checks.clone(),
                fctx.table_id(),
            )?;
            if let Some(tablespace) = tablespace {
                let oid = resolve_relation_tablespace_oid(kv, tablespace)?;
                ops.push(crabka_pgcatalog::set_relation_tablespace_op(name, oid));
            }
            let table = crabka_pgcatalog::Table {
                id,
                name: name.clone(),
                columns: cols,
                sharded: *sharded,
                sharding,
                foreign: None,
                checks,
            };
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
            for (name, is_sequence) in &targets {
                if *is_sequence {
                    tag = "DROP SEQUENCE";
                    match crabka_pgcatalog::drop_sequence_ops(kv, name) {
                        Ok(sequence_ops) => ops.extend(sequence_ops),
                        Err(crabka_pgcatalog::CatalogError::UndefinedSequence(_)) if *if_exists => {
                        }
                        Err(error) => return Err(error.into()),
                    }
                } else {
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
            Ok((command(tag), ops))
        }
        Statement::CreateSchema {
            name,
            authorization,
            if_not_exists,
            elements,
        } => {
            // `CREATE SCHEMA AUTHORIZATION role` names the schema after the role.
            let owner = authorization.as_deref().unwrap_or(fctx.current_user);
            let name = match name {
                Some(name) => name.clone(),
                None => owner.to_string(),
            };
            if let Some(role) = authorization
                && !crabka_pgcatalog::role_exists(kv, role)?
            {
                return Err(ExecError::UndefinedObject(format!("role \"{role}\"")));
            }
            if !elements.is_empty() {
                return Err(ExecError::Unsupported(
                    "CREATE SCHEMA with a schema-element list is not supported: DDL commits one \
                     catalog batch at a time here, so the schema and its contents could not be \
                     created atomically"
                        .into(),
                ));
            }
            // `IF NOT EXISTS` waives only the duplicate: an unacceptable name is
            // still unacceptable, so the reserved-prefix refusal has to come out
            // of `create_schema_ops` rather than be short-circuited before it.
            let ops = match crabka_pgcatalog::create_schema_ops(kv, &name, owner) {
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
                    crabka_pgcatalog::set_schema_owner_ops(kv, name, owner)?
                }
                AlterSchemaAction::RenameTo(_) => {
                    if !crabka_pgcatalog::schema_exists(kv, name)? {
                        return Err(ExecError::Catalog(
                            crabka_pgcatalog::CatalogError::UndefinedSchema(name.clone()),
                        ));
                    }
                    return Err(ExecError::Unsupported(
                        "ALTER SCHEMA … RENAME TO is not supported: a relation's catalog name \
                         carries its schema, so the rename would have to move every relation key \
                         in the schema"
                            .into(),
                    ));
                }
            };
            Ok((command("ALTER SCHEMA"), ops))
        }
        Statement::DropSchema {
            names,
            if_exists,
            cascade,
        } => {
            let mut ops = Vec::new();
            for name in names {
                if *if_exists && !crabka_pgcatalog::schema_exists(kv, name)? {
                    continue;
                }
                if *cascade {
                    ops.extend(drop_schema_contents_ops(kv, name)?);
                }
                ops.extend(crabka_pgcatalog::drop_schema_ops(kv, name, *cascade)?);
            }
            Ok((command("DROP SCHEMA"), ops))
        }
        Statement::AlterTable {
            table,
            if_exists,
            actions,
        } => match resolve_relation(kv, resolution, table, SchemaDisposition::Utility) {
            Ok(name) => alter_table_ops(
                kv,
                resolution,
                &name,
                *if_exists,
                actions,
                fctx.own_xid,
                fctx.catalog,
            ),
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
            comment,
        } => comment_ops(kv, resolution, object_kind, object_name, comment.as_deref()),
        Statement::CreateView {
            name,
            definition,
            query,
            or_replace,
            temporary,
            columns: aliases,
        } => {
            // The body is analysed before the view's own name is placed,
            // because what it reads decides where the view can go: a view over
            // a temporary relation is itself temporary whether or not `TEMP`
            // was written, so a qualifier naming an ordinary schema is refused.
            // `postgres:18.4` reports the two in that order.
            let sources = validate_view_definition(kv, resolution, query)?;
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
            let described = crate::query::describe_query_expr(kv, resolution, query)?;
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
            let ops = if *or_replace && crabka_pgcatalog::get_view(kv, name).is_ok() {
                let existing = crabka_pgcatalog::get_view(kv, name)?;
                check_view_columns_replaceable(&existing.columns, &columns, name)?;
                vec![crabka_pgcatalog::put_view_op(&crabka_pgcatalog::View {
                    name: name.clone(),
                    definition: definition.clone(),
                    columns,
                })]
            } else {
                let mut created = ensure_schema_ops(kv, &name.schema)?;
                created.extend(crabka_pgcatalog::create_view_ops(
                    kv,
                    name,
                    definition.clone(),
                    columns,
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
            let ops = match drop_view_with_triggers_ops(kv, name) {
                Ok(mut ops) => {
                    // A view may itself be read by other views. PostgreSQL
                    // refuses the drop unless CASCADE is written, and then drops
                    // the dependents too.
                    let dependents = dependent_view_names(kv, name, None)?;
                    if !dependents.is_empty() {
                        if !*cascade {
                            return Err(ExecError::DependentObjectsStillExist(format!(
                                "cannot drop view {name} because other objects depend on it"
                            )));
                        }
                        for view in &dependents {
                            ops.extend(drop_view_with_triggers_ops(kv, view)?);
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
                let ops = crabka_pgcatalog::create_sequence_ops(kv, name, sequence)?;
                return Ok((command("CREATE SEQUENCE"), ops));
            }
            let table = &resolve_relation(kv, resolution, table, SchemaDisposition::Utility)?;
            // An index name is never qualified: an index lands in its table's
            // schema, so only the sequence spelling above can carry one.
            let index = name
                .as_ref()
                .map(|name| resolve_relation(kv, resolution, name, SchemaDisposition::Utility))
                .transpose()?;
            let name = table.sibling(index_name_or_default(
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
            if !include.is_empty() {
                return Err(ExecError::Unsupported(
                    "CREATE INDEX … INCLUDE is not supported: index entries carry only key \
                     columns"
                        .into(),
                ));
            }
            if *if_not_exists && crabka_pgcatalog::get_index(kv, &name).is_ok() {
                return Ok((command("CREATE INDEX"), Vec::new()));
            }
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
            validate_index_expressions(&table_meta, keys, *unique, placement, index_method)?;
            validate_index_method(&table_meta, columns, *unique, placement, index_method)?;
            let (id, mut ops) = crabka_pgcatalog::create_index_with_method_ops(
                kv,
                &name.name,
                table,
                columns.clone(),
                *unique,
                placement,
                index_method,
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
                    unique: *unique,
                    placement,
                    method: index_method,
                    constraint: None,
                };
                ops.extend(local_index_backfill_ops(
                    kv,
                    &table_meta,
                    &index,
                    fctx.own_xid,
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
        Statement::AlterIndexTablespace { name, tablespace } => {
            let name = &resolve_relation(kv, resolution, name, SchemaDisposition::Utility)?;
            crabka_pgcatalog::get_index(kv, name)?;
            let oid = resolve_relation_tablespace_oid(kv, tablespace)?;
            Ok((
                command("ALTER INDEX"),
                vec![crabka_pgcatalog::set_relation_tablespace_op(name, oid)],
            ))
        }
        Statement::CreateRole {
            name,
            can_login,
            member_of,
        } => {
            let ops = crabka_pgcatalog::create_role_with_memberships_ops(
                kv,
                name,
                *can_login,
                member_of,
            )?;
            Ok((command("CREATE ROLE"), ops))
        }
        Statement::DropRole { name } => {
            let ops = crabka_pgcatalog::drop_role_ops(kv, name)?;
            Ok((command("DROP ROLE"), ops))
        }
        Statement::GrantTablePrivileges {
            privileges,
            table,
            grantees,
        } => {
            let ops = crabka_pgcatalog::grant_table_privileges_ops(
                kv,
                &resolve_relation(kv, resolution, table, SchemaDisposition::Utility)?,
                grantees,
                privileges,
            )?;
            Ok((command("GRANT"), ops))
        }
        Statement::GrantSchemaPrivileges {
            privileges,
            schemas,
            grantees,
        } => {
            let ops = crabka_pgcatalog::grant_schema_privileges_ops(
                kv, schemas, grantees, privileges,
            )?;
            Ok((command("GRANT"), ops))
        }
        Statement::RevokeTablePrivileges {
            privileges,
            table,
            grantees,
        } => {
            let ops = crabka_pgcatalog::revoke_table_privileges_ops(
                kv,
                &resolve_relation(kv, resolution, table, SchemaDisposition::Utility)?,
                grantees,
                privileges,
            )?;
            Ok((command("REVOKE"), ops))
        }
        Statement::RevokeSchemaPrivileges {
            privileges,
            schemas,
            grantees,
        } => {
            let ops = crabka_pgcatalog::revoke_schema_privileges_ops(
                kv, schemas, grantees, privileges,
            )?;
            Ok((command("REVOKE"), ops))
        }
        Statement::CreateFdw { name, options } => {
            let ops = crabka_pgcatalog::create_fdw_ops(kv, name, options.clone())?;
            Ok((command("CREATE FOREIGN DATA WRAPPER"), ops))
        }
        Statement::DropFdw {
            name,
            if_exists,
            cascade: _,
        } => {
            // No object can depend on this one in this engine, so CASCADE and
            // RESTRICT are indistinguishable; both are accepted.

            let ops = ignore_missing_ops(crabka_pgcatalog::drop_fdw_ops(kv, name), *if_exists)?;
            Ok((command("DROP FOREIGN DATA WRAPPER"), ops))
        }
        Statement::CreateServer {
            name,
            wrapper,
            options,
        } => {
            let ops = crabka_pgcatalog::create_server_ops(kv, name, wrapper, options.clone())?;
            Ok((command("CREATE SERVER"), ops))
        }
        Statement::DropServer {
            name,
            if_exists,
            cascade: _,
        } => {
            // No object can depend on this one in this engine, so CASCADE and
            // RESTRICT are indistinguishable; both are accepted.

            let ops = ignore_missing_ops(crabka_pgcatalog::drop_server_ops(kv, name), *if_exists)?;
            Ok((command("DROP SERVER"), ops))
        }
        Statement::CreateUserMapping {
            user,
            server,
            options,
        } => {
            // Normalize `FOR CURRENT_USER` and `FOR PUBLIC` to the catalog key
            // `"public"` — the same key that the scan path looks up via
            // `get_user_mapping(kv, fctx.current_user, …)` where
            // `current_user` is always `"public"` under trust-auth.
            // Per-named-user mappings await real SQL authentication (phase-2).
            let resolved_user = normalize_mapping_user(user);
            let ops = crabka_pgcatalog::create_user_mapping_ops(
                kv,
                resolved_user,
                server,
                options.clone(),
            )?;
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

            // Same normalization as CreateUserMapping so DROP matches CREATE.
            let resolved_user = normalize_mapping_user(user);
            let ops = ignore_missing_ops(
                crabka_pgcatalog::drop_user_mapping_ops(kv, resolved_user, server),
                *if_exists,
            )?;
            Ok((command("DROP USER MAPPING"), ops))
        }
        Statement::CreateForeignTable {
            name,
            columns,
            server,
            options,
        } => {
            let cols = columns
                .iter()
                .map(|c| Column::new(c.name.clone(), c.ty))
                .collect();
            let (_id, ops) = crabka_pgcatalog::create_foreign_table_ops(
                kv,
                &resolve_relation(kv, resolution, name, SchemaDisposition::Creation)?,
                cols,
                server,
                options.clone(),
                fctx.table_id(),
            )?;
            Ok((command("CREATE FOREIGN TABLE"), ops))
        }
        Statement::DropForeignTable {
            name,
            if_exists,
            cascade: _,
        } => {
            // No object can depend on this one in this engine, so CASCADE and
            // RESTRICT are indistinguishable; both are accepted.

            // A foreign table shares the ordinary table catalog key, so `drop_table`
            // removes it (catalog entry + sequence + any rows).
            let name = &resolve_relation(kv, resolution, name, SchemaDisposition::Utility)?;
            let ops = match crabka_pgcatalog::drop_table_ops(kv, name) {
                Ok(ops) => ops,
                Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) if *if_exists => Vec::new(),
                Err(e) => return Err(e.into()),
            };
            Ok((command("DROP FOREIGN TABLE"), ops))
        }
        // The catalog has no ALTER for foreign objects, and phase-1 querying does
        // not need one — surface a clear 0A000 rather than silently no-op'ing.
        Statement::AlterServer { .. } | Statement::AlterUserMapping { .. } => {
            Err(ExecError::CompatibilityRefusal(
                stmt.compatibility_refusal()
                    .expect("ALTER refusal metadata is centralized on the AST"),
            ))
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
        } => {
            // Resolve the server (42704 if undefined) and the current user's
            // optional mapping (no mapping → no credentials).
            let srv = crabka_pgcatalog::get_server(kv, server)?;
            let mapping = crabka_pgcatalog::get_user_mapping(kv, fctx.current_user, server).ok();
            // A scanner must be registered (the `kafka` feature is built in).
            let scanner = fctx.scanner.ok_or_else(|| {
                ExecError::Unsupported("foreign tables require the `kafka` feature".into())
            })?;
            let filter = crate::foreign::ImportFilter::from_selector(selector);
            let tables = scanner.import_schema(&srv, mapping.as_ref(), &filter)?;
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
                    crabka_pgcatalog::TableIdSource::Reserved(id),
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

/// PostgreSQL's `checkViewColumns`: a replacement query may APPEND output
/// columns but may not drop, rename, or retype any that already exist.
///
/// The order matters and is per column, not per rule: the count is checked
/// first, then each existing column in position order has its name checked and
/// then its type, and the FIRST offending column decides the error. A global
/// name pass followed by a global type pass would report the wrong column when
/// one column changes type and a later one changes name.
fn check_view_columns_replaceable(
    existing: &[Column],
    replacement: &[Column],
    view: &crabka_pgcatalog::RelationName,
) -> Result<(), ExecError> {
    if replacement.len() < existing.len() {
        return Err(ExecError::InvalidTableDefinition(
            "cannot drop columns from view".into(),
        ));
    }
    for (old, new) in existing.iter().zip(replacement) {
        if old.name != new.name {
            return Err(ExecError::InvalidTableDefinition(format!(
                "cannot change name of view column \"{}\" to \"{}\"",
                old.name, new.name
            )));
        }
        if old.ty != new.ty {
            return Err(ExecError::InvalidTableDefinition(format!(
                "cannot change data type of view column \"{}\" from {} to {}",
                old.name,
                old.ty.name(),
                new.ty.name()
            )));
        }
    }
    // An appended column may not collide with one of the existing names, which is
    // the same 42701 `check_for_column_name_collision` raises for a table.
    for appended in &replacement[existing.len()..] {
        if existing.iter().any(|old| old.name == appended.name) {
            return Err(ExecError::DuplicateColumn {
                column: appended.name.clone(),
                table: view.to_string(),
            });
        }
    }
    Ok(())
}

/// Check a view body against what this engine can store, and report the
/// relations it reads.
///
/// The caller needs those relations, not just the verdict: a view over a
/// temporary relation is itself temporary, so where the view lands is decided
/// by what its body names.
fn validate_view_definition(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    query: &crabka_pgparser::ast::QueryExpr,
) -> Result<Vec<crabka_pgcatalog::RelationName>, ExecError> {
    if query.with.is_some() {
        return Err(ExecError::Unsupported(
            "CREATE VIEW currently supports SELECT without WITH".into(),
        ));
    }
    if query.locking.is_some() {
        return Err(ExecError::Unsupported(
            "CREATE VIEW does not support locking SELECT".into(),
        ));
    }
    let crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Select(select)) =
        &query.body
    else {
        return Err(ExecError::Unsupported(
            "CREATE VIEW currently supports a single SELECT query".into(),
        ));
    };
    if select.from.len() > 1 {
        return Err(ExecError::Unsupported(
            "CREATE VIEW does not support joins or multiple FROM items".into(),
        ));
    }
    let mut sources = Vec::with_capacity(select.from.len());
    for table in &select.from {
        let crabka_pgparser::ast::TableExpr::Table { name, .. } = table else {
            return Err(ExecError::Unsupported(
                "CREATE VIEW does not support joins or derived tables".into(),
            ));
        };
        let name = resolve_relation(catalog_kv, resolution, name, SchemaDisposition::Reference)?;
        if crabka_pgcatalog::get_view(catalog_kv, &name).is_ok() {
            return Err(ExecError::Unsupported(
                "CREATE VIEW does not support references to other views".into(),
            ));
        }
        sources.push(name);
    }
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            validate_view_expr(expr)?;
        }
    }
    for expression in select
        .filter
        .iter()
        .chain(select.group_by.iter())
        .chain(select.having.iter())
        .chain(select.order_by.iter().map(|item| &item.expr))
        .chain(query.order_by.iter().map(|item| &item.expr))
    {
        validate_view_expr(expression)?;
    }
    Ok(sources)
}

fn validate_view_expr(expr: &Expr) -> Result<(), ExecError> {
    match expr {
        Expr::Param(_) => Err(ExecError::Unsupported(
            "CREATE VIEW does not support query parameters".into(),
        )),
        Expr::SqlJson(json) => json.children().into_iter().try_for_each(validate_view_expr),
        Expr::FieldSelect { base, .. } | Expr::FieldSelectAll(base) => validate_view_expr(base),
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::IsNull { expr, .. } => validate_view_expr(expr),
        Expr::Binary { left, right, .. } => {
            validate_view_expr(left)?;
            validate_view_expr(right)
        }
        Expr::Func(call) => match &call.args {
            FuncArgs::Star => Ok(()),
            FuncArgs::Exprs(args) => {
                for argument in args {
                    validate_view_expr(argument)?;
                }
                Ok(())
            }
        },
        Expr::InList { expr, list, .. } => {
            validate_view_expr(expr)?;
            for item in list {
                validate_view_expr(item)?;
            }
            Ok(())
        }
        // Array constructor / subscript / `= ANY(<array>)`: ordinary expression
        // nodes (no subquery, no parameter of their own), so the walk simply
        // recurses into their children.
        Expr::ArrayLiteral(elements) | Expr::Row(elements) => {
            for element in elements {
                validate_view_expr(element)?;
            }
            Ok(())
        }
        Expr::Subscript { base, index } => {
            validate_view_expr(base)?;
            validate_view_expr(index)
        }
        Expr::ArrayRef { base, subscripts } => {
            validate_view_expr(base)?;
            for bound in subscripts.iter().flat_map(ArraySubscript::bounds) {
                validate_view_expr(bound)?;
            }
            Ok(())
        }
        Expr::QuantifiedArray { expr, array, .. } => {
            validate_view_expr(expr)?;
            validate_view_expr(array)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_view_expr(expr)?;
            validate_view_expr(low)?;
            validate_view_expr(high)
        }
        Expr::Like { expr, pattern, .. } => {
            validate_view_expr(expr)?;
            validate_view_expr(pattern)
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            if let Some(operand) = operand {
                validate_view_expr(operand)?;
            }
            for (condition, result) in whens {
                validate_view_expr(condition)?;
                validate_view_expr(result)?;
            }
            if let Some(result) = else_result {
                validate_view_expr(result)?;
            }
            Ok(())
        }
        Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery { .. }
        | Expr::ArraySubquery(_)
        | Expr::Quantified { .. } => Err(ExecError::Unsupported(
            "CREATE VIEW does not support subqueries".into(),
        )),
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Default
        | Expr::Const { .. } => Ok(()),
    }
}

/// A `QueryResult::Command` with the given PostgreSQL completion tag.
fn command(tag: &str) -> QueryResult {
    QueryResult::Command { tag: tag.into() }
}

fn resolve_relation_tablespace_oid(kv: &dyn Kv, name: &str) -> Result<u32, ExecError> {
    if name == "pg_global" {
        return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
            "0A000",
            "only shared relations can be placed in pg_global tablespace",
        )));
    }
    crabka_pgcatalog::tablespace_oid(kv, name).map_err(|error| match error {
        crabka_pgcatalog::CatalogError::UndefinedObject(_) => {
            ExecError::Remote(crabka_pgwire::error::PgError::error(
                "42704",
                format!("tablespace \"{name}\" does not exist"),
            ))
        }
        other => other.into(),
    })
}

fn create_table_constraint_index(
    table_name: &crabka_pgcatalog::RelationName,
    columns: &[String],
    primary_key: bool,
) -> crabka_pgcatalog::NewIndex {
    let suffix = if primary_key { "pkey" } else { "key" };
    // The relation's own name, never its qualified spelling: `PostgreSQL` names
    // the index for `s1.t`'s primary key `t_pkey`, in schema `s1`.
    let table = &table_name.name;
    let name = if primary_key {
        format!("{table}_pkey")
    } else {
        format!("{table}_{}_{suffix}", columns.join("_"))
    };
    crabka_pgcatalog::NewIndex {
        name,
        columns: columns.to_vec(),
        unique: true,
        placement: crabka_pgcatalog::IndexPlacement::Local,
        method: crabka_pgcatalog::IndexMethod::Btree,
        constraint: Some(if primary_key {
            crabka_pgcatalog::IndexConstraint::PrimaryKey
        } else {
            crabka_pgcatalog::IndexConstraint::Unique
        }),
    }
}

/// The columns a `CREATE TABLE` marks NOT NULL because they are part of its
/// primary key, whether declared inline or as a table constraint.
fn create_table_primary_key_columns<'a>(
    columns: &'a [crabka_pgparser::ast::ColumnDef],
    constraints: &'a [crabka_pgparser::ast::TableConstraint],
) -> HashSet<&'a str> {
    let mut primary_key_columns = HashSet::new();
    for column in columns {
        if column.constraints.iter().any(|constraint| {
            matches!(
                constraint.kind,
                crabka_pgparser::ast::ColumnConstraintKind::PrimaryKey
            )
        }) {
            primary_key_columns.insert(column.name.as_str());
        }
    }
    for constraint in constraints {
        if let crabka_pgparser::ast::TableConstraintKind::PrimaryKey(columns) = &constraint.kind {
            primary_key_columns.extend(columns.iter().map(String::as_str));
        }
    }
    primary_key_columns
}

fn column_from_ast(
    table_name: &crabka_pgcatalog::RelationName,
    column: &crabka_pgparser::ast::ColumnDef,
    ctx: &crate::clock::EvalCtx,
    serial_sequences: &mut Vec<(crabka_pgcatalog::RelationName, Sequence)>,
    primary_key_columns: &HashSet<&str>,
) -> Result<Column, ExecError> {
    let mut catalog_column = Column::new(column.name.clone(), column.ty);
    if primary_key_columns.contains(column.name.as_str()) {
        catalog_column.not_null = true;
    }
    if column.serial.is_some() {
        let sequence_name = table_name.sibling(format!("{}_{}_seq", table_name.name, column.name));
        catalog_column.not_null = true;
        catalog_column.default = Some(ColumnDefault::NextVal(sequence_name.to_string()));
        serial_sequences.push((
            sequence_name,
            Sequence::new(1, 1, None, None, Some(1), false),
        ));
    }
    for constraint in &column.constraints {
        match &constraint.kind {
            crabka_pgparser::ast::ColumnConstraintKind::NotNull => catalog_column.not_null = true,
            crabka_pgparser::ast::ColumnConstraintKind::Null => catalog_column.not_null = false,
            crabka_pgparser::ast::ColumnConstraintKind::Default(expr) => {
                let value = crate::eval::eval(expr, &Scope::empty(), &[], ctx)?;
                let value = coerce(value, column.ty, ctx)?;
                ensure_default_can_be_persisted(&value)?;
                catalog_column.default = Some(ColumnDefault::Value(value));
            }
            crabka_pgparser::ast::ColumnConstraintKind::PrimaryKey => {
                catalog_column.not_null = true;
            }
            crabka_pgparser::ast::ColumnConstraintKind::Unique { nulls_not_distinct } => {
                if *nulls_not_distinct {
                    return Err(ExecError::Unsupported(
                        "UNIQUE NULLS NOT DISTINCT is not supported: unique indexes use \
                         PostgreSQL's default NULLS DISTINCT semantics"
                            .into(),
                    ));
                }
            }
            crabka_pgparser::ast::ColumnConstraintKind::Identity(spec) => {
                let sequence_name =
                    table_name.sibling(format!("{}_{}_seq", table_name.name, column.name));
                catalog_column.not_null = true;
                catalog_column.identity = Some(if spec.always {
                    crabka_pgcatalog::IdentityKind::Always
                } else {
                    crabka_pgcatalog::IdentityKind::ByDefault
                });
                catalog_column.default = Some(ColumnDefault::NextVal(sequence_name.to_string()));
                serial_sequences.push((sequence_name, sequence_from_options(&spec.options)));
            }
            crabka_pgparser::ast::ColumnConstraintKind::Generated(predicate) => {
                catalog_column.generated = Some(predicate.text.clone());
            }
            // A column-level CHECK or REFERENCES contributes a constraint, not a
            // column property; `create_table_definition` collects both.
            crabka_pgparser::ast::ColumnConstraintKind::Check(_)
            | crabka_pgparser::ast::ColumnConstraintKind::References(_) => {}
        }
    }
    Ok(catalog_column)
}

/// A `GENERATED … AS IDENTITY` sequence, from the parsed option list.
fn sequence_from_options(options: &crabka_pgparser::ast::SequenceOptions) -> Sequence {
    let increment = options.increment.unwrap_or(1);
    Sequence::new(
        options.start.unwrap_or(if increment > 0 { 1 } else { -1 }),
        increment,
        options.min,
        options.max,
        options.cache,
        options.cycle.unwrap_or(false),
    )
}

/// `PostgreSQL`'s default index name: `<table>_<key>_…_idx`, with `expr` for a
/// key that is not a bare column reference.
fn index_name_or_default(
    explicit: Option<&str>,
    table: &crabka_pgcatalog::RelationName,
    keys: &[crabka_pgparser::ast::IndexKey],
) -> String {
    if let Some(name) = explicit {
        return name.to_string();
    }
    let parts: Vec<&str> = keys
        .iter()
        .map(|key| key.column.as_deref().unwrap_or("expr"))
        .collect();
    format!("{}_{}_idx", table.name, parts.join("_"))
}

/// The durable key list an index can be built from. Expression source uses the
/// catalog's NUL-prefixed encoding, which cannot collide with a SQL identifier.
fn index_key_columns(
    keys: &[crabka_pgparser::ast::IndexKey],
    predicate: Option<&str>,
) -> Result<Vec<String>, ExecError> {
    if predicate.is_some() {
        return Err(ExecError::Unsupported(
            "partial indexes (CREATE INDEX … WHERE) are not supported: the scanner would treat \
             the index as covering every row"
                .into(),
        ));
    }
    keys.iter()
        .map(|key| {
            if key.descending || key.nulls_first == Some(true) {
                return Err(ExecError::Unsupported(
                    "DESC and NULLS FIRST index keys are not supported: index entries are \
                     stored in ascending, NULLS-LAST order"
                        .into(),
                ));
            }
            Ok(key.column.clone().unwrap_or_else(|| {
                crabka_pgcatalog::expression_index_key(&key.text)
            }))
        })
        .collect()
}

fn validate_index_expressions(
    table: &Table,
    keys: &[crabka_pgparser::ast::IndexKey],
    unique: bool,
    placement: crabka_pgcatalog::IndexPlacement,
    method: crabka_pgcatalog::IndexMethod,
) -> Result<(), ExecError> {
    use crabka_pgparser::ast::Expr;

    let expressions: Vec<&str> = keys
        .iter()
        .filter_map(|key| key.column.is_none().then_some(key.text.as_str()))
        .collect();
    if expressions.is_empty() {
        return Ok(());
    }
    // GiST/SP-GiST are exact-scan metadata today, so expression keys need no
    // physical entry evaluator. Other methods do maintain/probe stored keys.
    if unique
        || placement != crabka_pgcatalog::IndexPlacement::Local
        || !matches!(
            method,
            crabka_pgcatalog::IndexMethod::Gist | crabka_pgcatalog::IndexMethod::Spgist
        )
    {
        return Err(ExecError::Unsupported(
            "expression indexes currently require a non-unique local GiST or SP-GiST index"
                .into(),
        ));
    }
    let scope = Scope::single(table, &table.name.name);
    for source in expressions {
        let expr = crabka_pgparser::parser::parse_expression(source)?;
        crate::eval::infer_type(&expr, &scope)?;
        let mut invalid = false;
        crate::grouping::visit_expr(&expr, &mut |node| {
            invalid |= matches!(
                node,
                Expr::ScalarSubquery(_)
                    | Expr::Exists(_)
                    | Expr::InSubquery { .. }
                    | Expr::Quantified { .. }
            ) || matches!(node, Expr::Func(call) if !is_immutable_function(&call.name));
        });
        if invalid || crate::agg::contains_aggregate(&expr) {
            return Err(ExecError::InvalidObjectDefinition(
                "functions in index expression must be marked IMMUTABLE".into(),
            ));
        }
    }
    Ok(())
}

fn validate_index_method(
    table: &Table,
    columns: &[String],
    unique: bool,
    placement: crabka_pgcatalog::IndexPlacement,
    method: crabka_pgcatalog::IndexMethod,
) -> Result<(), ExecError> {
    if method == crabka_pgcatalog::IndexMethod::Btree {
        return Ok(());
    }
    if method != crabka_pgcatalog::IndexMethod::Gin {
        if unique {
            return Err(ExecError::Unsupported(format!(
                "access method {} does not support unique indexes",
                index_method_name(method)
            )));
        }
        return Ok(());
    }
    // ponytail: one stored tsvector column keeps maintenance on the existing
    // row path; add expression/multicolumn GIN only when queries require it.
    if unique {
        return Err(ExecError::Unsupported(
            "access method gin does not support unique indexes".into(),
        ));
    }
    if placement != crabka_pgcatalog::IndexPlacement::Local {
        return Err(ExecError::Unsupported(
            "global GIN indexes are not supported".into(),
        ));
    }
    let [column] = columns else {
        return Err(ExecError::Unsupported(
            "GIN indexes currently require exactly one tsvector column".into(),
        ));
    };
    let column = table
        .column_index(column)
        .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))?;
    if table.columns[column].ty != ColumnType::TsVector {
        return Err(ExecError::Unsupported(
            "GIN indexes currently support only tsvector columns".into(),
        ));
    }
    Ok(())
}

fn index_method_name(method: crabka_pgcatalog::IndexMethod) -> &'static str {
    match method {
        crabka_pgcatalog::IndexMethod::Btree => "btree",
        crabka_pgcatalog::IndexMethod::Hash => "hash",
        crabka_pgcatalog::IndexMethod::Gist => "gist",
        crabka_pgcatalog::IndexMethod::Gin => "gin",
        crabka_pgcatalog::IndexMethod::Spgist => "spgist",
    }
}

fn ensure_default_can_be_persisted(value: &Datum) -> Result<(), ExecError> {
    if matches!(
        value,
        Datum::Null
            | Datum::Bool(_)
            | Datum::Int4(_)
            | Datum::Int8(_)
            | Datum::Text(_)
            | Datum::Float8(_)
            | Datum::Numeric(_)
            | Datum::Jsonb(_)
            | Datum::TsVector(_)
            | Datum::TsQuery(_)
            // Stored as its bare oid, with the relation name re-derived on read.
            | Datum::Regclass(_)
            // An array carries its elements in the row encoding, so an element
            // of a type that cannot be a *column* default (a date, say) still
            // persists inside one.
            | Datum::Array(_)
    ) {
        return Ok(());
    }
    // The catalog's schema serializer has no encoding for these default values
    // (`crabka_pgcatalog::serde::write_default`), so they are refused at DDL
    // time rather than written and lost.
    Err(ExecError::Unsupported(
        "defaults for date/time, interval, bytea, composite and enum columns are not persisted yet"
            .into(),
    ))
}

/// Normalize the `user` spec from `CREATE / DROP USER MAPPING FOR <user>` to
/// the catalog key used by the scan path.
///
/// The parser emits `"current_user"` for `FOR CURRENT_USER` and `"public"` for
/// `FOR PUBLIC`.  Under trust-auth crabgresql has no authenticated SQL user, so
/// the scan path always looks up the mapping under `"public"`.  Both variants
/// must therefore resolve to the same catalog key so that a `FOR CURRENT_USER`
/// mapping is actually found at scan time.
fn normalize_mapping_user(user: &str) -> &str {
    if user.eq_ignore_ascii_case("current_user") || user.eq_ignore_ascii_case("public") {
        "public"
    } else {
        user
    }
}

/// Swallow a missing foreign-object error when `IF EXISTS` was given and return
/// an empty write batch, so DDL still flows through the committer seam.
fn ignore_missing_ops(
    r: Result<Vec<crabka_pgkv::WriteOp>, crabka_pgcatalog::CatalogError>,
    if_exists: bool,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    match r {
        Ok(ops) => Ok(ops),
        Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) if if_exists => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

fn sequence_from_encoded_options(encoded: &[String]) -> Result<Sequence, ExecError> {
    let mut start = None;
    let mut increment = None;
    let mut min = None;
    let mut max = None;
    let mut cache = None;
    let mut cycle = None;
    for option in encoded {
        let Some((key, value)) = option.split_once('=') else {
            return Err(ExecError::Syntax("invalid encoded sequence option".into()));
        };
        match key {
            "start" => start = Some(parse_sequence_i64(value)?),
            "increment" => increment = Some(parse_sequence_i64(value)?),
            "min" => min = Some(parse_sequence_i64(value)?),
            "max" => max = Some(parse_sequence_i64(value)?),
            "cache" => cache = Some(parse_sequence_i64(value)?),
            "cycle" => cycle = Some(value == "true"),
            _ => return Err(ExecError::Syntax("invalid encoded sequence option".into())),
        }
    }
    let increment = increment.unwrap_or(1);
    let start = start.unwrap_or(if increment > 0 { 1 } else { -1 });
    Ok(Sequence::new(
        start,
        increment,
        min,
        max,
        cache,
        cycle.unwrap_or(false),
    ))
}

fn parse_sequence_i64(value: &str) -> Result<i64, ExecError> {
    value
        .parse::<i64>()
        .map_err(|_| ExecError::Syntax("invalid encoded sequence option".into()))
}

/// Resolve INSERT target column indices: explicit `(cols...)` mapped to their
/// catalog positions (42703 on miss), or all columns in declared order.
fn resolve_targets(t: &Table, columns: &Option<Vec<String>>) -> Result<Vec<usize>, ExecError> {
    match columns {
        Some(cols) => cols
            .iter()
            .map(|c| {
                t.column_index(c)
                    .ok_or_else(|| ExecError::UndefinedColumn(c.clone()))
            })
            .collect::<Result<_, _>>(),
        None => Ok((0..t.columns.len()).collect()),
    }
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
fn resolve_insert_targets(
    t: &Table,
    columns: &Option<Vec<String>>,
    width: usize,
) -> Result<Vec<usize>, ExecError> {
    let mut target_idx = resolve_targets(t, columns)?;
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
fn unsupplied_defaults(
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

fn build_insert_row(
    table: &Table,
    target_idx: &[usize],
    row_exprs: &[Expr],
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Datum>, ExecError> {
    let mut row = unsupplied_defaults(table, target_idx, ctx)?;
    for (slot, expr) in target_idx.iter().zip(row_exprs.iter()) {
        let value = match expr {
            Expr::Default => default_value(&table.columns[*slot], ctx)?,
            Expr::StringLiteral(value) => {
                let target = table.columns[*slot].ty;
                if target == crabka_pgtypes::ColumnType::Bytea {
                    Datum::Bytea(crate::session::decode_bytea_text(value)?)
                } else {
                    // An unadorned literal resolves to the column's type, and that
                    // resolution is an assignment: `INSERT INTO t(v) VALUES ('abcd')`
                    // into a `varchar(3)` is 22001, where `'abcd'::varchar(3)` would
                    // have truncated.
                    crabka_pgtypes::cast::cast_assign(
                        &Datum::Text(value.clone()),
                        target,
                        &ctx.time_zone,
                    )?
                }
            }
            _ => {
                let value = crate::eval::eval(expr, &Scope::empty(), &[], ctx)?;
                coerce(value, table.columns[*slot].ty, ctx)?
            }
        };
        row[*slot] = value;
    }
    finish_written_row(table, &mut row, ctx)?;
    Ok(row)
}

fn build_copy_row(
    table: &Table,
    target_idx: &[usize],
    row_values: &[Option<String>],
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Datum>, ExecError> {
    let mut row = unsupplied_defaults(table, target_idx, ctx)?;
    for (slot, value) in target_idx.iter().zip(row_values.iter()) {
        row[*slot] = match value {
            Some(value) => crabka_pgtypes::cast::cast(
                &Datum::Text(value.clone()),
                table.columns[*slot].ty,
                &ctx.time_zone,
            )?,
            None => Datum::Null,
        };
    }
    finish_written_row(table, &mut row, ctx)?;
    Ok(row)
}

pub(crate) fn decode_copy_text(data: &[u8]) -> Result<Vec<Vec<Option<String>>>, ExecError> {
    let text = std::str::from_utf8(data)
        .map_err(|_| ExecError::Syntax("invalid byte sequence for encoding \"UTF8\"".into()))?;
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    let mut lines = text.split('\n').peekable();
    while let Some(raw_line) = lines.next() {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        // PostgreSQL's text-format end-of-data marker: clients on the old
        // PQputline/PQendcopy API (pgbench -i among them) send a final `\.`
        // line; it terminates the data and everything after it is ignored.
        if line == r"\." {
            break;
        }
        if raw_line.is_empty() && lines.peek().is_none() && text.ends_with('\n') {
            continue;
        }
        rows.push(decode_copy_text_line(line)?);
    }
    Ok(rows)
}

fn decode_copy_text_line(line: &str) -> Result<Vec<Option<String>>, ExecError> {
    line.split('\t')
        .map(|field| {
            if field == r"\N" {
                return Ok(None);
            }
            decode_copy_text_field(field).map(Some)
        })
        .collect()
}

fn decode_copy_text_field(field: &str) -> Result<String, ExecError> {
    let mut out = String::with_capacity(field.len());
    let mut chars = field.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let Some(escaped) = chars.next() else {
            return Err(ExecError::Syntax(
                "unterminated COPY escape sequence".into(),
            ));
        };
        out.push(match escaped {
            'b' => '\u{0008}',
            'f' => '\u{000c}',
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            'v' => '\u{000b}',
            '\\' => '\\',
            other => other,
        });
    }
    Ok(out)
}

/// The per-row work every write path shares once the target values are in
/// place.
///
/// Compute `GENERATED … STORED` columns, then enforce `NOT NULL`, then the
/// table's `CHECK` constraints. That is `PostgreSQL`'s order, and the reason a
/// generated column can satisfy a `CHECK` that references it.
pub(crate) fn finish_written_row(
    table: &Table,
    row: &mut [Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    apply_generated_columns(table, row, ctx)?;
    enforce_not_null(table, row)?;
    if table.checks.is_empty() {
        return Ok(());
    }
    let checks = compile_check_constraints(table)?;
    enforce_check_constraints(table, &checks, row, ctx)
}

fn enforce_not_null(table: &Table, row: &[Datum]) -> Result<(), ExecError> {
    for (column, value) in table.columns.iter().zip(row.iter()) {
        if column.not_null && value.is_null() {
            return Err(ExecError::NotNullViolation {
                column: column.name.clone(),
                table: table.name.to_string(),
            });
        }
    }
    Ok(())
}

fn default_value(column: &Column, ctx: &crate::clock::EvalCtx) -> Result<Datum, ExecError> {
    let Some(default) = &column.default else {
        return Ok(Datum::Null);
    };
    match default {
        // A stored `regclass` default holds only the oid, so the name it prints
        // is derived here — the same output-time resolution a scanned value
        // gets, which is what lets `RETURNING` print the relation's current
        // name rather than the bare number.
        ColumnDefault::Value(Datum::Regclass(value)) => match ctx.catalog() {
            Some(catalog) => regclass_by_oid(catalog, value.oid).map(Datum::Regclass),
            None => Ok(Datum::Regclass(value.clone())),
        },
        ColumnDefault::Value(value) => Ok(value.clone()),
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

/// The write path (INSERT/UPDATE/DELETE) with concurrent writers (SP6).
///
/// It builds the version write ops tagged with the transaction's `xid` and
/// returns them WITHOUT a write. The session assembles the final batch (clog
/// for autocommit) and writes once. INSERT allocates rowids with the
/// `SequenceManager`, which persists the sequence durably itself. UPDATE/DELETE
/// lock each candidate row exclusively with the `RowLockManager`. That lock
/// blocks until it is granted, or reports 40P01 on a deadlock. They then
/// re-check the row's current state under EvalPlanQual: a concurrent committed
/// change is a 40001 under REPEATABLE READ, or a re-find under READ COMMITTED.
/// Reads resolve with `satisfies_mvcc` and the txn's own xid
/// (read-your-writes).
pub(crate) async fn execute_write(
    write_ctx: &WriteContext<'_>,
    stmt: &Statement,
) -> Result<(QueryResult, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let span = execute_write_span(write_ctx, stmt);
    let triggers_before = crate::trigger::fired_count();
    let mut writes = StatementWrites::default();
    let written = execute_write_with_ctes(write_ctx, write_ctx.ctes, stmt, &mut writes)
        .instrument(span.clone())
        .await;
    let (outcome, ops) = match written {
        Ok(written) => written,
        Err(error) => {
            let rendered = error.clone().into_pg();
            crate::telemetry::record_error(&span, &rendered.code, &rendered.message);
            return Err(error);
        }
    };
    record_write_outcome(
        &span,
        &outcome,
        &ops,
        crate::trigger::fired_count().saturating_sub(triggers_before),
    );
    Ok((outcome.into_result(write_ctx.eval_ctx), ops))
}

/// Build the span covering one data-modifying statement's execution.
///
/// This is guarded, not built unconditionally. A resolution of the target
/// relation costs a name resolution and a catalog read, and a span macro's
/// field expressions evaluate whether or not the callsite is enabled.
fn execute_write_span(write_ctx: &WriteContext<'_>, stmt: &Statement) -> tracing::Span {
    if !tracing::enabled!(target: crate::telemetry::EXEC_TARGET, tracing::Level::DEBUG) {
        return tracing::Span::none();
    }
    let span = tracing::debug_span!(
        target: crate::telemetry::EXEC_TARGET,
        "pg.execute_write",
        otel.kind = "internal",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        db.response.status_code = tracing::field::Empty,
        "error.type" = tracing::field::Empty,
        db.collection.name = tracing::field::Empty,
        pg.table_id = tracing::field::Empty,
        pg.rows_affected = tracing::field::Empty,
        pg.write_ops = tracing::field::Empty,
        pg.index_ops = tracing::field::Empty,
        pg.fk_checks = tracing::field::Empty,
        pg.triggers_fired = tracing::field::Empty,
        pg.returning = tracing::field::Empty,
    );
    if let Some(relation) = crate::telemetry::statement_relation(stmt) {
        span.record("db.collection.name", relation.name.as_str());
        // A statement whose target does not resolve — a `CREATE TABLE AS`, a
        // name that is about to fail — records the name it wrote and no id,
        // rather than failing the span build.
        if let Ok(resolved) = resolve_relation(
            write_ctx.catalog_kv,
            write_ctx.fctx.resolution,
            relation,
            SchemaDisposition::Reference,
        ) && let Ok(table) = crabka_pgcatalog::get_table(write_ctx.catalog_kv, &resolved)
        {
            span.record("pg.table_id", crate::telemetry::integer(table.id));
        }
    }
    span
}

/// Fold a write's outcome onto its span.
///
/// `pg.fk_checks` is deliberately absent here: the referential drain records it
/// itself, onto whichever span encloses it, because only the drain knows how
/// many checks a cascade grew the batch to.
fn record_write_outcome(
    span: &tracing::Span,
    outcome: &WriteOutcome,
    ops: &[crabka_pgkv::WriteOp],
    triggers_fired: u64,
) {
    if span.is_disabled() {
        return;
    }
    if let Some(rows) = crate::telemetry::command_tag_row_count(&outcome.tag) {
        span.record("pg.rows_affected", crate::telemetry::integer(rows));
    }
    span.record("pg.write_ops", crate::telemetry::integer(ops.len()));
    span.record("pg.index_ops", crate::telemetry::integer(index_ops(ops)));
    span.record(
        "pg.triggers_fired",
        crate::telemetry::integer(triggers_fired),
    );
    span.record("pg.returning", outcome.returning.is_some());
}

/// How many of a write's ops maintain a secondary index rather than the heap.
///
/// The split an operator wants when a write is slower than its row count
/// explains: index maintenance is per index per row, so it is what a wide
/// index set costs.
fn index_ops(ops: &[crabka_pgkv::WriteOp]) -> usize {
    ops.iter()
        .filter(|op| {
            let key = match op {
                crabka_pgkv::WriteOp::Put { key, .. }
                | crabka_pgkv::WriteOp::ConditionalPut { key, .. }
                | crabka_pgkv::WriteOp::Delete { key } => key,
            };
            matches!(
                crabka_pgkv::key::classify_key(key),
                crabka_pgkv::key::KeyClass::SecondaryIndex { .. }
            )
        })
        .count()
}

/// The write state every part of one statement shares: the `WITH` list's
/// data-modifying items and the statement body.
///
/// `PostgreSQL` runs all of those parts as ONE command. They read one snapshot,
/// but they are not independent writers. Three rules only hold if this state is
/// per statement and not per part:
///
/// - a unique index is enforced across the whole command, so
///   `WITH i AS (INSERT INTO t VALUES (1)) INSERT INTO t VALUES (1)` is 23505;
/// - a row one part modified is never modified again by another (`UPDATE` and
///   `DELETE` skip it, `MERGE` and `ON CONFLICT DO UPDATE` raise 21000);
/// - a unique key a part freed is available to a later part, because
///   `PostgreSQL`'s uniqueness check ignores a tuple its own command superseded.
#[derive(Debug, Default)]
struct StatementWrites {
    /// Unique-index keys claimed by rows this statement staged. They live only
    /// in the pending op batch, which a KV probe cannot see.
    pending_unique_keys: HashSet<PendingUniqueKey>,
    /// Exclusion keys staged by this statement, which are not visible in KV
    /// until the statement's batch commits.
    pending_exclusion_keys:
        HashMap<crabka_pgcatalog::IndexId, Vec<(u64, Vec<Datum>)>>,
    /// `(index, rowid)` pairs whose key this statement freed — a deleted row, or
    /// an updated row whose indexed values changed. A row holds exactly one key
    /// per index, so the rowid identifies the freed key. The superseded version
    /// is still in the KV, so the probe still finds it and must discount it.
    released_unique_keys: HashSet<(crabka_pgcatalog::IndexId, u64)>,
    /// Every `(table, rowid)` this statement has already updated or deleted,
    /// whether by its own DML or by a referential action.
    row_claims: HashSet<(TableId, u64)>,
    /// Which `(table, rowid, constraint)` triples a referential action has
    /// already written. See [`StatementWrites::claim_row_for_action`].
    action_claims: HashSet<(TableId, u64, String)>,
    /// The referential checks this statement owes, appended by the write hooks
    /// and drained once, after the `WITH` list AND the body, because
    /// `PostgreSQL` treats the whole command as one trigger-firing unit.
    fk_checks: crate::fk::FkCheckQueue,
    /// The relations a `TRUNCATE` is emptying, empty for every other statement.
    ///
    /// `TRUNCATE` desugars to one unfiltered `DELETE` per relation, and those
    /// deletes must not fire `ON DELETE CASCADE`: `PostgreSQL`'s `TRUNCATE`
    /// refuses a child outside the set and `CASCADE` widens the *set* instead.
    /// Carried here so the desugared `DELETE` can suppress exactly the
    /// parent-side keys whose child is being truncated too.
    truncate_set: BTreeSet<TableId>,
}

impl StatementWrites {
    /// Claim a row for the command's own DML. `false` means anything else in
    /// this statement already modified it, which is what stops this part from
    /// modifying it a second time.
    ///
    /// `PostgreSQL`'s "a command modifies a given row at most once" rule is
    /// about the command's own `ModifyTable` nodes, and every one of them runs
    /// before the trigger queue does, so a referential action can never be what
    /// this refuses.
    fn claim_row(&mut self, table: TableId, rowid: u64) -> bool {
        self.row_claims.insert((table, rowid))
    }

    /// Claim a row for one constraint's referential action. `false` means *that
    /// constraint's* action has already written this row.
    ///
    /// A referential action is not one of the command's `ModifyTable` nodes: it
    /// runs as a separate query the trigger queue issues, so it reaches a row
    /// the command itself already modified, and so does a *second* constraint's
    /// action reach a row the first one has just rewritten. This is how one
    /// `DELETE` of a doubly-referenced parent key nulls both referencing
    /// columns and not one.
    ///
    /// This refuses one constraint that comes back around to a row its own
    /// action already wrote. The drain folds each action's ops into the view it
    /// reads, so a cascade cycle already terminates on the data exactly as
    /// `PostgreSQL`'s does. A deleted row reads as gone, and a re-keyed one no
    /// longer matches. This bounds the work at one write per
    /// `(row, constraint)` whatever the data does.
    fn claim_row_for_action(&mut self, table: TableId, rowid: u64, constraint: &str) -> bool {
        if !self
            .action_claims
            .insert((table, rowid, constraint.to_string()))
        {
            return false;
        }
        self.row_claims.insert((table, rowid));
        true
    }

    /// Has anything in this command already modified this row? The predicate
    /// `ON CONFLICT DO UPDATE` raises 21000 on, where *what* touched the row
    /// makes no difference. The upsert may not be the second thing to reach it.
    fn is_claimed(&self, table: TableId, rowid: u64) -> bool {
        self.row_claims.contains(&(table, rowid))
    }

    /// Does the probe's `holder` still hold the key it was found under, or did
    /// an earlier part of this statement free it?
    fn holder_still_holds(&self, index: crabka_pgcatalog::IndexId, rowid: u64) -> bool {
        !self.released_unique_keys.contains(&(index, rowid))
    }

    /// Record the unique keys `rowid` gives up: every one when the row is
    /// deleted (`next` is `None`), and only the ones whose indexed values change
    /// when it is updated.
    fn release_row_keys(
        &mut self,
        table: &Table,
        indexes: &[crabka_pgcatalog::Index],
        rowid: u64,
        old_row: &[Datum],
        next: Option<&[Datum]>,
    ) -> Result<(), ExecError> {
        for index in indexes.iter().filter(|index| index.unique) {
            let old_values = indexed_values(table, index, old_row)?;
            if let Some(next) = next
                && indexed_values(table, index, next)? == old_values
            {
                continue;
            }
            self.released_unique_keys.insert((index.id, rowid));
        }
        Ok(())
    }
}

impl<'a> WriteContext<'a> {
    /// The context the foreign-key drain probes and scans through.
    ///
    /// The row store it reads is `staged`, this statement's write batch layered
    /// over the real one. The drain's whole premise is that the statement's rows
    /// already exist, and they only reach the KV when the session commits the
    /// batch.
    fn fk_exec<'b>(&'b self, staged: &'b StagedKv<'b>) -> crate::fk::FkExecContext<'b>
    where
        'a: 'b,
    {
        crate::fk::FkExecContext {
            catalog_kv: self.catalog_kv,
            kv: staged,
            global: self.global,
            global_snapshot: self.global_snapshot,
            snapshot: self.snapshot,
            xid: self.xid,
            eval_ctx: self.eval_ctx,
        }
    }

    /// Move the transaction's deferred-check store out of its mutex for the
    /// duration of a drain, which needs it by `&mut` across `await` points. The
    /// lock is held only for the swap, never across an await; the store is put
    /// back by [`WriteContext::restore_deferred_fk`] on every exit path.
    fn take_deferred_fk(&self) -> Option<crate::fk::DeferredConstraints> {
        self.deferred_fk
            .map(|store| std::mem::take(&mut *store.lock().expect("deferred constraints mutex")))
    }

    fn restore_deferred_fk(&self, store: Option<crate::fk::DeferredConstraints>) {
        if let Some(slot) = self.deferred_fk
            && let Some(store) = store
        {
            *slot.lock().expect("deferred constraints mutex") = store;
        }
    }
}

/// Both sides of a foreign key name the same lock identity, the referenced
/// index's entry prefix. That is exactly what the uniqueness check already
/// locks, so this is a [`crate::lockmgr::LockKey::UniqueKey`] acquire in the
/// row-lock manager and no new lock mode exists.
///
/// The child side takes it SHARED, so many rows that reference one parent key
/// never convoy. The parent side takes it EXCLUSIVE, because it removes or
/// moves the key. Key locks and row locks share one wait-for graph, so the
/// engine still reports a cycle across both as 40P01, and it releases both
/// together at COMMIT/ROLLBACK.
impl crate::fk::FkKeyLocks for WriteContext<'_> {
    async fn lock_key(&self, key: Vec<u8>, mode: crate::fk::FkLockMode) -> Result<(), ExecError> {
        let mode = match mode {
            crate::fk::FkLockMode::Shared => crate::lockmgr::LockMode::Shared,
            crate::fk::FkLockMode::Exclusive => crate::lockmgr::LockMode::Exclusive,
        };
        self.lockmgr
            .acquire_key(
                crate::lockmgr::LockKey::UniqueKey(key),
                mode,
                self.xid,
                self.lock_wait_cap,
            )
            .await
            .map_err(lock_acquire_error)
    }
}

/// The write path a referential action re-enters, over the *outer statement's*
/// [`StatementWrites`].
///
/// `PostgreSQL` runs each referential action as its own query over the row's
/// current image, so neither an earlier `UPDATE` in the same command nor another
/// constraint's action exempts a child row from the action its parent's deletion
/// fires. Each action's ops are folded straight back into the staged view before
/// it returns, which is what makes "current image" true here: the next action to
/// reach the row reads what this one wrote, and a cascade cycle terminates
/// because the row it comes back to reads as deleted or off-key.
///
/// [`claim_row_for_action`] then only bounds the work, and refuses one
/// constraint a second write of the same row.
///
/// [`claim_row_for_action`]: StatementWrites::claim_row_for_action
struct StatementCascade<'a, 'w> {
    write_ctx: &'a WriteContext<'w>,
    writes: &'a mut StatementWrites,
    /// The view both this and the drain read through: the statement's pending
    /// batch over the store, grown by every op an action produces. An action
    /// re-reads its row here rather than in the store, so it changes the image
    /// the command, or an earlier action, last wrote.
    staged: &'a StagedKv<'a>,
    /// One index-set read per cascaded *relation*, not per cascaded row: a
    /// cascade walks a chain of relations and revisits each many times.
    indexes: HashMap<TableId, Vec<crabka_pgcatalog::Index>>,
}

impl crate::fk::FkCascade for StatementCascade<'_, '_> {
    fn begin_action(
        &mut self,
        table: &Table,
        delete: bool,
        updated: &[usize],
    ) -> Result<(), ExecError> {
        let event = if delete {
            crate::trigger::DmlEvent::Delete
        } else {
            crate::trigger::DmlEvent::Update
        };
        let columns = updated
            .iter()
            .filter_map(|index| table.columns.get(*index))
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        crate::trigger::fire_statement(
            self.write_ctx.catalog_kv,
            table,
            event,
            crabka_pgcatalog::trigger::TriggerTiming::Before,
            &columns,
            self.write_ctx.eval_ctx,
        )
    }

    fn end_action(
        &mut self,
        table: &Table,
        delete: bool,
        updated: &[usize],
    ) -> Result<(), ExecError> {
        let event = if delete {
            crate::trigger::DmlEvent::Delete
        } else {
            crate::trigger::DmlEvent::Update
        };
        let columns = updated
            .iter()
            .filter_map(|index| table.columns.get(*index))
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        crate::trigger::fire_statement(
            self.write_ctx.catalog_kv,
            table,
            event,
            crabka_pgcatalog::trigger::TriggerTiming::After,
            &columns,
            self.write_ctx.eval_ctx,
        )
    }

    async fn modify_row(
        &mut self,
        request: crate::fk::FkCascadeRequest<'_>,
    ) -> Result<(crate::fk::FkCascadeOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
        let crate::fk::FkCascadeRequest {
            table,
            rowid,
            change,
            constraint,
        } = request;
        // Split the borrows: the index cache and the statement's write
        // bookkeeping are both reached mutably from the same `&mut self`.
        let Self {
            write_ctx,
            writes,
            staged,
            indexes,
        } = self;
        let write_ctx = *write_ctx;
        let staged = *staged;
        let ctx = write_ctx.eval_ctx;
        let mut ops = Vec::new();
        let local_indexes = match indexes.entry(table.id) {
            std::collections::hash_map::Entry::Occupied(slot) => slot.into_mut(),
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(writable_local_indexes(write_ctx.catalog_kv, table)?)
            }
        };
        write_ctx
            .lockmgr
            .acquire(
                table.id,
                rowid,
                crate::lockmgr::LockMode::Exclusive,
                write_ctx.xid,
                write_ctx.lock_wait_cap,
            )
            .await
            .map_err(lock_acquire_error)?;
        let Some((cur_key_xid, cur_xmin, cur_row)) =
            eval_plan_qual(&write_ctx.staged_mutation(staged), table, rowid)?
        else {
            // Deleted by a concurrent committed transaction, or by this command's
            // own DML: nothing references the parent through this row any more.
            return Ok((crate::fk::FkCascadeOutcome::Skipped, ops));
        };
        // A row this constraint's own action already wrote. It runs before any
        // op is built, so a revisited row leaves the batch untouched as well as
        // unrecursed. A row the command's own DML — or another constraint's
        // action — modified passes, because the action is a command of its own.
        if !writes.claim_row_for_action(table.id, rowid, constraint) {
            return Ok((crate::fk::FkCascadeOutcome::Skipped, ops));
        }
        let (next, updated_columns) = match change {
            crate::fk::FkRowChange::Delete => (None, Vec::new()),
            crate::fk::FkRowChange::Assign(pairs) => {
                let mut next = cur_row.clone();
                let mut updated = Vec::new();
                for (ordinal, value) in pairs {
                    let ty = cascade_column(table, ordinal)?.ty;
                    next[ordinal] = coerce(value, ty, ctx)?;
                    updated.push(table.columns[ordinal].name.clone());
                }
                (Some(next), updated)
            }
            crate::fk::FkRowChange::AssignDefaults(ordinals) => {
                let mut next = cur_row.clone();
                let mut updated = Vec::new();
                for ordinal in ordinals {
                    next[ordinal] = default_value(cascade_column(table, ordinal)?, ctx)?;
                    updated.push(table.columns[ordinal].name.clone());
                }
                (Some(next), updated)
            }
        };
        let Some(next) = next else {
            if crate::trigger::fire_before_row(
                write_ctx.catalog_kv,
                table,
                crate::trigger::DmlEvent::Delete,
                &[],
                Some(&cur_row),
                None,
                ctx,
            )?
            .is_none()
            {
                return Ok((crate::fk::FkCascadeOutcome::Skipped, ops));
            }
            apply_locked_row_delete(
                write_ctx,
                table,
                local_indexes,
                &LockedRowDelete {
                    rowid,
                    cur_key_xid,
                    cur_xmin,
                    cur_row: &cur_row,
                },
                writes,
                &mut ops,
            )?;
            crate::trigger::fire_after_row(
                write_ctx.catalog_kv,
                table,
                crate::trigger::DmlEvent::Delete,
                &[],
                Some(&cur_row),
                None,
                ctx,
            )?;
            staged.stage(&ops);
            return Ok((crate::fk::FkCascadeOutcome::Applied { new_row: None }, ops));
        };
        let Some(next) = crate::trigger::fire_before_row(
            write_ctx.catalog_kv,
            table,
            crate::trigger::DmlEvent::Update,
            &updated_columns,
            Some(&cur_row),
            Some(next),
            ctx,
        )?
        else {
            return Ok((crate::fk::FkCascadeOutcome::Skipped, ops));
        };
        // The follow-on checks a cascaded update owes are computed by the drain
        // from the row it returns, so this write queues none of its own.
        let no_hooks = crate::fk::StatementFkContext::default();
        apply_locked_row_update(
            write_ctx,
            table,
            local_indexes,
            &no_hooks,
            &LockedRowUpdate {
                rowid,
                cur_key_xid,
                cur_xmin,
                cur_row: &cur_row,
                next: &next,
            },
            writes,
            &mut ops,
        )
        .await?;
        crate::trigger::fire_after_row(
            write_ctx.catalog_kv,
            table,
            crate::trigger::DmlEvent::Update,
            &updated_columns,
            Some(&cur_row),
            Some(&next),
            ctx,
        )?;
        staged.stage(&ops);
        Ok((
            crate::fk::FkCascadeOutcome::Applied {
                new_row: Some(next),
            },
            ops,
        ))
    }
}

/// Run the referential checks a statement queued, once its whole `WITH` list and
/// body have staged their rows.
///
/// `PostgreSQL` fires its `AFTER ROW` trigger queue once for the whole command,
/// which is why nothing here happens inline: a `NOT DEFERRABLE` self-referencing
/// `INSERT INTO t (id, boss) VALUES (1, 1)` succeeds because the row is in place
/// by the time the check runs. A referential action re-enters the write path
/// through [`StatementCascade`], which shares this statement's
/// [`StatementWrites`]. So a cascade that comes back around to a row *an
/// action* already changed stops and does not recurse, while a row the
/// statement's own DML modified is still the action's to change.
async fn drain_statement_fk_checks(
    write_ctx: &WriteContext<'_>,
    writes: &mut StatementWrites,
    staged: &[crabka_pgkv::WriteOp],
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    if writes.fk_checks.is_empty() {
        return Ok(Vec::new());
    }
    let mut queue = std::mem::take(&mut writes.fk_checks);
    let staged_kv = StagedKv::new(write_ctx.kv, staged);
    let exec = write_ctx.fk_exec(&staged_kv);
    let mut cascade = StatementCascade {
        write_ctx,
        writes,
        staged: &staged_kv,
        indexes: HashMap::new(),
    };
    let mut deferred = write_ctx.take_deferred_fk();
    let drained = crate::fk::drain_statement_checks(
        &exec,
        write_ctx,
        &mut cascade,
        &mut queue,
        deferred.as_mut(),
    )
    .await;
    write_ctx.restore_deferred_fk(deferred);
    drained
}

/// Run the checks a transaction deferred, at `COMMIT` or at
/// `SET CONSTRAINTS … IMMEDIATE`.
///
/// Every earlier statement's rows are in the KV under this transaction's xid by
/// now, so the drain reads storage directly (an empty staged batch) and finds
/// a re-supplied key. This is what makes `DELETE; INSERT; COMMIT`
/// succeed under a deferred `NO ACTION`. A referential action re-enters the
/// write path through the same [`StatementCascade`] the statement drain uses,
/// over write bookkeeping of its own: the statements whose rows these checks
/// describe are finished, so there is none to share.
pub(crate) async fn drain_deferred_fk_checks(
    write_ctx: &WriteContext<'_>,
    checks: Vec<crate::fk::PendingCheck>,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    if checks.is_empty() {
        return Ok(Vec::new());
    }
    let staged_kv = StagedKv::new(write_ctx.kv, &[]);
    let exec = write_ctx.fk_exec(&staged_kv);
    let mut writes = StatementWrites::default();
    let mut cascade = StatementCascade {
        write_ctx,
        writes: &mut writes,
        staged: &staged_kv,
        indexes: HashMap::new(),
    };
    crate::fk::drain_deferred_checks(&exec, write_ctx, &mut cascade, checks).await
}

/// The column a referential action names by ordinal. The ordinals come from the
/// same catalog relation the request carries, so a miss is catalog corruption.
fn cascade_column(table: &Table, ordinal: usize) -> Result<&Column, ExecError> {
    table
        .columns
        .get(ordinal)
        .ok_or_else(|| ExecError::UndefinedTableColumn {
            column: ordinal.to_string(),
            table: table.name.to_string(),
        })
}

/// What a data-modifying statement produced: the command tag, plus the relation
/// its `RETURNING` clause projected (absent when the statement had none).
pub(crate) struct WriteOutcome {
    tag: String,
    returning: Option<Relation>,
}

impl WriteOutcome {
    fn command(tag: String) -> Self {
        Self {
            tag,
            returning: None,
        }
    }

    fn into_result(self, ctx: &crate::clock::EvalCtx) -> QueryResult {
        match self.returning {
            None => QueryResult::Command { tag: self.tag },
            Some(rel) => {
                let fields = rel
                    .scope
                    .columns
                    .iter()
                    .map(|c| field(&c.name, c.ty))
                    .collect();
                rows_result_with_tag(fields, &rel.rows, ctx.output_style(), self.tag)
            }
        }
    }
}

/// Run the whole of one data-modifying statement: its `WITH` list, its body, and
/// then the referential checks all of those queued.
///
/// The drain is here rather than in each part because `PostgreSQL` treats the
/// `WITH`-list-plus-body as ONE command and fires its `AFTER ROW` trigger queue
/// once for it.
async fn execute_write_with_ctes(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let mut statement_triggers = Vec::new();
    if let Some(with) = statement_with_clause(stmt) {
        for cte in &with.ctes {
            if let crabka_pgparser::ast::CteBody::Dml(dml) = &cte.body {
                statement_triggers.extend(statement_trigger_targets(write_ctx, dml)?);
            }
        }
    }
    statement_triggers.extend(statement_trigger_targets(write_ctx, stmt)?);
    for (table, event, updated) in &statement_triggers {
        crate::trigger::fire_statement(
            write_ctx.catalog_kv,
            table,
            *event,
            crabka_pgcatalog::trigger::TriggerTiming::Before,
            updated,
            write_ctx.eval_ctx,
        )?;
    }
    let (outcome, mut ops) = execute_write_parts(write_ctx, ctes, stmt, writes).await?;
    let fk_ops = drain_statement_fk_checks(write_ctx, writes, &ops).await?;
    ops.extend(fk_ops);
    for (table, event, updated) in statement_triggers.iter().rev() {
        crate::trigger::fire_statement(
            write_ctx.catalog_kv,
            table,
            *event,
            crabka_pgcatalog::trigger::TriggerTiming::After,
            updated,
            write_ctx.eval_ctx,
        )?;
    }
    Ok((outcome, ops))
}

fn statement_trigger_targets(
    write_ctx: &WriteContext<'_>,
    stmt: &Statement,
) -> Result<Vec<(Table, crate::trigger::DmlEvent, Vec<String>)>, ExecError> {
    if let Statement::Truncate { names, cascade, .. } = stmt {
        let names = resolve_relations(
            write_ctx.catalog_kv,
            write_ctx.eval_ctx.resolution(),
            names,
            SchemaDisposition::Utility,
        )?;
        let tables = names
            .iter()
            .map(|name| crabka_pgcatalog::get_table(write_ctx.catalog_kv, name))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(
            crate::fk::expand_truncate_set(write_ctx.catalog_kv, &tables, *cascade)?
                .tables
                .into_iter()
                .map(|table| (table, crate::trigger::DmlEvent::Truncate, Vec::new()))
                .collect(),
        );
    }
    if let Statement::Insert {
        table,
        on_conflict:
            Some(crabka_pgparser::ast::OnConflict {
                action: crabka_pgparser::ast::OnConflictAction::DoUpdate { assignments, .. },
                ..
            }),
        ..
    } = stmt
    {
        let name = resolve_relation(
            write_ctx.catalog_kv,
            write_ctx.eval_ctx.resolution(),
            table,
            SchemaDisposition::Reference,
        )?;
        let table = crate::trigger::relation_trigger_table(write_ctx.catalog_kv, &name)?;
        return Ok(vec![
            (table.clone(), crate::trigger::DmlEvent::Insert, Vec::new()),
            (
                table,
                crate::trigger::DmlEvent::Update,
                assignments
                    .iter()
                    .map(|(column, _)| column.clone())
                    .collect(),
            ),
        ]);
    }
    if let Statement::Merge { table, clauses, .. } = stmt {
        let name = resolve_relation(
            write_ctx.catalog_kv,
            write_ctx.eval_ctx.resolution(),
            table,
            SchemaDisposition::Reference,
        )?;
        let table = crate::trigger::relation_trigger_table(write_ctx.catalog_kv, &name)?;
        let mut insert = false;
        let mut delete = false;
        let mut updated = Vec::new();
        for clause in clauses {
            match &clause.action {
                crabka_pgparser::ast::MergeAction::Insert { .. } => insert = true,
                crabka_pgparser::ast::MergeAction::Delete => delete = true,
                crabka_pgparser::ast::MergeAction::Update(assignments) => {
                    for column in assignments
                        .iter()
                        .flat_map(|assignment| assignment.targets.iter())
                    {
                        if !updated.contains(column) {
                            updated.push(column.clone());
                        }
                    }
                }
                crabka_pgparser::ast::MergeAction::DoNothing => {}
            }
        }
        let mut targets = Vec::new();
        if insert {
            targets.push((table.clone(), crate::trigger::DmlEvent::Insert, Vec::new()));
        }
        if !updated.is_empty() {
            targets.push((table.clone(), crate::trigger::DmlEvent::Update, updated));
        }
        if delete {
            targets.push((table, crate::trigger::DmlEvent::Delete, Vec::new()));
        }
        return Ok(targets);
    }
    let (reference, event, updated) = match stmt {
        Statement::Insert { table, .. } => (table, crate::trigger::DmlEvent::Insert, Vec::new()),
        Statement::Update {
            table, assignments, ..
        } => (
            table,
            crate::trigger::DmlEvent::Update,
            assignments
                .iter()
                .flat_map(|assignment| assignment.targets.iter().cloned())
                .collect(),
        ),
        Statement::Delete { table, .. } => (table, crate::trigger::DmlEvent::Delete, Vec::new()),
        _ => return Ok(Vec::new()),
    };
    let name = resolve_relation(
        write_ctx.catalog_kv,
        write_ctx.eval_ctx.resolution(),
        reference,
        SchemaDisposition::Reference,
    )?;
    let table = crate::trigger::relation_trigger_table(write_ctx.catalog_kv, &name)?;
    if crabka_pgcatalog::get_view(write_ctx.catalog_kv, &name).is_ok()
        && !crate::trigger::has_instead_row_trigger(
            write_ctx.catalog_kv,
            table.id,
            event,
            &updated,
        )?
    {
        return Ok(Vec::new());
    }
    Ok(vec![(table, event, updated)])
}

/// Evaluate the statement's `WITH` list, then the statement body against that
/// CTE scope.
///
/// The `WITH` list includes any data-modifying entries, which run exactly once
/// each whether or not the body references them. The referential checks they
/// queue are left for [`execute_write_with_ctes`] to drain once for all of
/// them.
///
/// Every entry sees the statement's own snapshot: a data-modifying CTE's rows
/// are staged as write ops and never written to the KV here, so neither a later
/// CTE nor the body can observe them, exactly as in `PostgreSQL`.
///
/// The parts run in `PostgreSQL`'s order, which is observable whenever two of
/// them touch the same row (whichever runs first is the one whose change
/// survives, because [`StatementWrites`] then holds the row against the other).
/// `PostgreSQL` runs a data-modifying item when something first demands its
/// rows, and runs the items nothing demands AFTER the main query, in reverse
/// list order, the order `ExecPostprocessPlan` walks `es_auxmodifytables`.
async fn execute_write_parts(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let Some(with) = statement_with_clause(stmt) else {
        return execute_write_body(write_ctx, ctes, stmt, writes).await;
    };
    let mut ops = Vec::new();
    let mut scope = ctes.child();
    // The body is stripped up front: the reference check below must see it
    // without the `WITH` list, whose own names would shadow the item.
    let body = statement_without_with(stmt);
    let mut deferred = Vec::new();
    for (index, cte) in with.ctes.iter().enumerate() {
        let rel = match &cte.body {
            crabka_pgparser::ast::CteBody::Query(_) => {
                let read = write_ctx.read_ctx(&scope);
                crate::cte::evaluate_cte_relation(&read, cte, with.recursive, &scope)?
            }
            crabka_pgparser::ast::CteBody::Dml(dml) => {
                if !cte_is_referenced(with, &body, index, &cte.name) {
                    // Nothing demands its rows, so it runs after the body.
                    deferred.push(dml);
                    continue;
                }
                let (outcome, cte_ops) =
                    Box::pin(execute_write_body(write_ctx, &scope, dml, writes)).await?;
                ops.extend(cte_ops);
                let Some(rel) = outcome.returning else {
                    // Only a *reference* to a data-modifying item without a
                    // RETURNING clause is refused, and this item is referenced.
                    return Err(ExecError::Unsupported(format!(
                        "WITH query \"{}\" does not have a RETURNING clause",
                        cte.name
                    )));
                };
                // `evaluate_cte_relation` applies the aliases for a query item.
                crate::cte::apply_cte_column_aliases(rel, &cte.name, &cte.columns)?
            }
        };
        scope.insert(cte.name.clone(), rel);
    }
    let (outcome, body_ops) =
        Box::pin(execute_write_body(write_ctx, &scope, &body, writes)).await?;
    ops.extend(body_ops);
    for dml in deferred.into_iter().rev() {
        let (_, cte_ops) = Box::pin(execute_write_body(write_ctx, &scope, dml, writes)).await?;
        ops.extend(cte_ops);
    }
    Ok((outcome, ops))
}

/// Whether anything after `WITH` item `index` names it: a later item, or the
/// statement body.
fn cte_is_referenced(
    with: &crabka_pgparser::ast::WithClause,
    stmt: &Statement,
    index: usize,
    name: &str,
) -> bool {
    with.ctes[index + 1..]
        .iter()
        .any(|later| match &later.body {
            crabka_pgparser::ast::CteBody::Query(query) => {
                crate::cte::query_references(query, name)
            }
            crabka_pgparser::ast::CteBody::Dml(dml) => statement_references_relation(dml, name),
        })
        || statement_references_relation(stmt, name)
}

/// Whether a statement's relation positions name `name`.
fn statement_references_relation(stmt: &Statement, name: &str) -> bool {
    use crabka_pgparser::ast::{InsertSource, MergeSource};
    match stmt {
        Statement::Query(query) => crate::cte::query_references(query, name),
        Statement::CreateTableAs { query, .. } => crate::cte::query_references(query, name),
        Statement::Insert { source, .. } => match source {
            InsertSource::Query(query) => crate::cte::query_references(query, name),
            InsertSource::Values(_) | InsertSource::DefaultValues => false,
        },
        Statement::Update { from, .. } => from.iter().any(|item| table_expr_references(item, name)),
        Statement::Delete { using, .. } => {
            using.iter().any(|item| table_expr_references(item, name))
        }
        Statement::Merge { source, .. } => match source {
            MergeSource::Table { name: source, .. } => source.name == *name,
            MergeSource::Query { query, .. } => crate::cte::query_references(query, name),
        },
        _ => false,
    }
}

fn table_expr_references(item: &crabka_pgparser::ast::TableExpr, name: &str) -> bool {
    use crabka_pgparser::ast::TableExpr;
    match item {
        TableExpr::Table { name: source, .. } => source.name == *name,
        TableExpr::Derived { subquery, .. } => crate::cte::query_references(subquery, name),
        TableExpr::Join { left, right, .. } => {
            table_expr_references(left, name) || table_expr_references(right, name)
        }
        _ => false,
    }
}

/// The `WITH` list attached to a statement, when it has one.
fn statement_with_clause(stmt: &Statement) -> Option<&crabka_pgparser::ast::WithClause> {
    match stmt {
        Statement::Query(q) => q.with.as_ref(),
        Statement::CreateTableAs { query, .. } => query.with.as_ref(),
        Statement::Insert { with, .. }
        | Statement::Update { with, .. }
        | Statement::Delete { with, .. }
        | Statement::Merge { with, .. } => with.as_ref(),
        _ => None,
    }
}

/// The same statement with its `WITH` list removed. The CTE relations are
/// already materialized into the scope the body executes against.
fn statement_without_with(stmt: &Statement) -> Statement {
    let mut stmt = stmt.clone();
    match &mut stmt {
        Statement::Query(q) => q.with = None,
        Statement::CreateTableAs { query, .. } => query.with = None,
        Statement::Insert { with, .. }
        | Statement::Update { with, .. }
        | Statement::Delete { with, .. }
        | Statement::Merge { with, .. } => *with = None,
        _ => {}
    }
    stmt
}

/// Fold every uncorrelated subquery in a data-modifying statement's expression
/// clauses to a constant, under this statement's snapshot and CTE scope.
///
/// The write path's evaluator executes no subqueries of its own, so this is what
/// lets `UPDATE … WHERE k IN (SELECT …)` and a `MERGE` condition over a CTE work
/// at all. Each one is evaluated once for the statement, which is `PostgreSQL`'s
/// behavior for an uncorrelated subquery.
fn resolve_write_subqueries(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
) -> Result<Statement, ExecError> {
    use crabka_pgparser::ast::{AssignmentValue, InsertSource, MergeAction};

    let read = write_ctx.read_ctx(ctes);
    let resolve = |expr: &Expr| crate::subquery::resolve_expr(&read, expr);
    let resolve_opt = |expr: &Option<Expr>| -> Result<Option<Expr>, ExecError> {
        expr.as_ref().map(&resolve).transpose()
    };
    let resolve_assignments =
        |assignments: &mut Vec<crabka_pgparser::ast::Assignment>| -> Result<(), ExecError> {
            for assignment in assignments {
                match &mut assignment.value {
                    AssignmentValue::Expr(expr) => *expr = resolve(expr)?,
                    AssignmentValue::Row(items) => {
                        for item in items {
                            *item = resolve(item)?;
                        }
                    }
                    AssignmentValue::Subquery(_) => {}
                }
            }
            Ok(())
        };

    let mut stmt = stmt.clone();
    match &mut stmt {
        Statement::Insert {
            source: InsertSource::Values(rows),
            ..
        } => {
            for row in rows {
                for value in row {
                    *value = resolve(value)?;
                }
            }
        }
        Statement::Update {
            assignments,
            filter,
            ..
        } => {
            resolve_assignments(assignments)?;
            *filter = resolve_opt(filter)?;
        }
        Statement::Delete { filter, .. } => *filter = resolve_opt(filter)?,
        Statement::Merge { on, clauses, .. } => {
            *on = resolve(on)?;
            for clause in clauses {
                clause.condition = resolve_opt(&clause.condition)?;
                match &mut clause.action {
                    MergeAction::Update(assignments) => resolve_assignments(assignments)?,
                    MergeAction::Insert {
                        values: Some(values),
                        ..
                    } => {
                        for value in values {
                            *value = resolve(value)?;
                        }
                    }
                    MergeAction::Insert { .. } | MergeAction::Delete | MergeAction::DoNothing => {}
                }
            }
        }
        _ => {}
    }
    Ok(stmt)
}

/// `UPDATE`/`DELETE` over a partitioned parent: the same statement runs against
/// each leaf in turn and the affected-row counts are summed.
///
/// Divergence from `PostgreSQL`: an `UPDATE` that moves a row out of its own
/// partition's bound is 23514 here (`new row for relation … violates partition
/// constraint`), where `PostgreSQL` deletes the row from its old partition and
/// re-inserts it into the new one. A refusal is the correctness-preserving
/// choice. The alternative stores a row in a partition whose bound it does not
/// satisfy, and every later read would answer that wrongly.
async fn partitioned_dml(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let resolution = write_ctx.eval_ctx.resolution();
    let (parent, verb) = match stmt {
        Statement::Update { table, .. } => (table, "UPDATE"),
        Statement::Delete { table, .. } => (table, "DELETE"),
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
        // than answered with mismatched columns.
        let leaf_table = crabka_pgcatalog::get_table(write_ctx.catalog_kv, &leaf)?;
        if column_mapping(&parent_table, &leaf_table)?
            .iter()
            .enumerate()
            .any(|(expected, actual)| expected != *actual)
        {
            return Err(ExecError::Unsupported(format!(
                "{verb} over a partitioned table is not supported when a partition declares its \
                 columns in a different order than its parent: partition \"{leaf}\" does"
            )));
        }
        let mut per_leaf = stmt.clone();
        match &mut per_leaf {
            Statement::Update { table, .. } | Statement::Delete { table, .. } => {
                *table = crabka_pgparser::ast::RelationRef::qualified(&leaf.schema, &leaf.name);
            }
            _ => unreachable!("the caller matched an UPDATE or a DELETE"),
        }
        let (outcome, leaf_ops) =
            Box::pin(execute_write_body(write_ctx, ctes, &per_leaf, writes)).await?;
        ops.extend(leaf_ops);
        // The per-leaf body already rendered its own count into the tag; the
        // parent's tag is their sum.
        affected += outcome
            .tag
            .rsplit(' ')
            .next()
            .and_then(|count| count.parse::<u64>().ok())
            .unwrap_or_default();
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

/// A row written straight into a leaf partition must still satisfy that leaf's
/// own bound, `PostgreSQL`'s implicit per-partition `CHECK`, reported as 23514.
fn check_partition_constraint(kv: &dyn Kv, table: &Table, row: &[Datum]) -> Result<(), ExecError> {
    let Some((parent, bound)) = crate::partition::parent_of(kv, &table.name)? else {
        return Ok(());
    };
    let parent_table = crabka_pgcatalog::get_table(kv, &parent)?;
    let Some(scheme) = crate::partition::scheme_of(kv, &parent)? else {
        return Ok(());
    };
    let ordinals = column_mapping(&parent_table, table)?;
    let parent_row = ordinals
        .iter()
        .map(|ordinal| row.get(*ordinal).cloned().unwrap_or(Datum::Null))
        .collect::<Vec<_>>();
    let siblings = crate::partition::partitions_of(kv, &parent)?;
    if crate::partition::satisfies(&scheme, &bound, &siblings, &parent_row)? {
        return Ok(());
    }
    Err(ExecError::PartitionConstraintViolation(
        table.name.to_string(),
    ))
}

/// `INSERT` into a partitioned parent: every proposed row is routed to the leaf
/// its partition key selects and written there.
///
/// The rows are built against the *parent's* column list, so defaults, coercion
/// and `NOT NULL` all come from the parent. They are then permuted into the
/// chosen leaf's own column order on the way out, so a leaf attached with its
/// columns in a different order still stores them correctly.
async fn partitioned_insert(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let resolution = write_ctx.eval_ctx.resolution();
    let Statement::Insert {
        table,
        columns,
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
    let (target_idx, rows) = insert_source_rows(write_ctx, ctes, &parent, columns, source)?;
    let mut ops: Vec<crabka_pgkv::WriteOp> = Vec::new();
    if rows.is_empty() {
        return Ok((WriteOutcome::command("INSERT 0 0".into()), ops));
    }
    let mut returned_rows = returning
        .as_ref()
        .map(|_| Vec::with_capacity(rows.len()))
        .unwrap_or_default();
    let mut inserted: u64 = 0;
    // Resolved once per leaf rather than once per row: a relation in no foreign
    // key must pay one boolean test per write, not a catalog read.
    let mut leaf_fk: HashMap<TableId, crate::fk::StatementFkContext> = HashMap::new();
    for row_exprs in &rows {
        let full = build_insert_row(&parent, &target_idx, row_exprs, ctx)?;
        let Some((leaf, leaf_row)) = route_row_to_leaf(catalog_kv, &parent, &full)? else {
            return Err(ExecError::NoPartitionForRow(parent.name.to_string()));
        };
        let Some(leaf_row) = crate::trigger::fire_before_row(
            catalog_kv,
            &leaf,
            crate::trigger::DmlEvent::Insert,
            &[],
            None,
            Some(leaf_row),
            ctx,
        )?
        else {
            continue;
        };
        check_partition_constraint(catalog_kv, &leaf, &leaf_row)?;
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
            returned_rows.push(ReturnedRow {
                new: Some(returned),
                old: None,
                source: Vec::new(),
                action: None,
            });
        }
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(leaf.id, rowid, write_ctx.xid),
            value: crabka_pgmvcc::version::encode_tuple(
                write_ctx.xid,
                crabka_pgmvcc::xid::INVALID_XID,
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
        &parent.name.to_string(),
        returning.as_ref(),
        None,
        false,
    )?;
    Ok((
        spec.outcome(format!("INSERT 0 {inserted}"), returned_rows, ctx)?,
        ops,
    ))
}

fn is_view_ref(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    reference: &crabka_pgparser::ast::RelationRef,
) -> Result<bool, ExecError> {
    let name = resolve_relation(kv, resolution, reference, SchemaDisposition::Reference)?;
    match crabka_pgcatalog::get_view(kv, &name) {
        Ok(_) => Ok(true),
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn execute_view_dml(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let reference = match stmt {
        Statement::Insert { table, .. }
        | Statement::Update { table, .. }
        | Statement::Delete { table, .. } => table,
        _ => unreachable!("view DML only accepts INSERT, UPDATE, or DELETE"),
    };
    let name = resolve_relation(
        write_ctx.catalog_kv,
        write_ctx.eval_ctx.resolution(),
        reference,
        SchemaDisposition::Reference,
    )?;
    let view = crate::trigger::relation_trigger_table(write_ctx.catalog_kv, &name)?;
    let ctx = write_ctx.eval_ctx;

    match stmt {
        Statement::Insert {
            columns,
            source,
            on_conflict,
            returning,
            ..
        } => {
            if on_conflict.is_some() {
                return Err(ExecError::Unsupported(
                    "INSERT ... ON CONFLICT is not supported on views".into(),
                ));
            }
            let (targets, rows) = insert_source_rows(write_ctx, ctes, &view, columns, source)?;
            let spec = ReturningSpec::new(&view, &view.name.name, returning.as_ref(), None, false)?;
            let mut returned = Vec::new();
            let mut count = 0_u64;
            for row in rows {
                let proposed = build_insert_row(&view, &targets, &row, ctx)?;
                let Some(result) = crate::trigger::fire_instead_row(
                    write_ctx.catalog_kv,
                    &view,
                    crate::trigger::DmlEvent::Insert,
                    &[],
                    None,
                    Some(proposed),
                    ctx,
                )?
                else {
                    continue;
                };
                count += 1;
                if returning.is_some() {
                    returned.push(ReturnedRow {
                        new: Some(result),
                        old: None,
                        source: Vec::new(),
                        action: None,
                    });
                }
            }
            Ok((
                spec.outcome(format!("INSERT 0 {count}"), returned, ctx)?,
                Vec::new(),
            ))
        }
        Statement::Update {
            alias,
            assignments,
            from,
            filter,
            returning,
            ..
        } => {
            let qualifier = table_qualifier(&view, alias);
            let read = write_ctx.read_ctx(ctes);
            let target_expr = crabka_pgparser::ast::TableExpr::Table {
                name: reference.clone(),
                only: true,
                alias: alias.clone(),
                columns: None,
                sample: None,
            };
            let target_rows =
                build_from(&read, std::slice::from_ref(&target_expr), None, None, None)?.rows;
            let source = DmlSource::build(write_ctx, ctes, &view, qualifier, from)?;
            let targets = resolve_assignments(write_ctx, ctes, &view, assignments)?;
            let spec = ReturningSpec::new(
                &view,
                qualifier,
                returning.as_ref(),
                Some(&source.scope),
                false,
            )?;
            let updated: Vec<String> = assignments
                .iter()
                .flat_map(|assignment| assignment.targets.iter().cloned())
                .collect();
            let mut returned = Vec::new();
            let mut count = 0_u64;
            for old in target_rows {
                let Some(joined) = source.first_match(filter.as_ref(), &old, ctx)? else {
                    continue;
                };
                let proposed = apply_assignments(&view, &targets, &source.scope, &joined, ctx)?;
                let Some(result) = crate::trigger::fire_instead_row(
                    write_ctx.catalog_kv,
                    &view,
                    crate::trigger::DmlEvent::Update,
                    &updated,
                    Some(&old),
                    Some(proposed),
                    ctx,
                )?
                else {
                    continue;
                };
                count += 1;
                if returning.is_some() {
                    returned.push(ReturnedRow::updated(
                        result,
                        old,
                        joined[view.columns.len()..].to_vec(),
                    ));
                }
            }
            Ok((
                spec.outcome(format!("UPDATE {count}"), returned, ctx)?,
                Vec::new(),
            ))
        }
        Statement::Delete {
            alias,
            using,
            filter,
            returning,
            ..
        } => {
            let qualifier = table_qualifier(&view, alias);
            let read = write_ctx.read_ctx(ctes);
            let target_expr = crabka_pgparser::ast::TableExpr::Table {
                name: reference.clone(),
                only: true,
                alias: alias.clone(),
                columns: None,
                sample: None,
            };
            let target_rows =
                build_from(&read, std::slice::from_ref(&target_expr), None, None, None)?.rows;
            let source = DmlSource::build(write_ctx, ctes, &view, qualifier, using)?;
            let spec = ReturningSpec::new(
                &view,
                qualifier,
                returning.as_ref(),
                Some(&source.scope),
                false,
            )?;
            let mut returned = Vec::new();
            let mut count = 0_u64;
            for old in target_rows {
                let Some(joined) = source.first_match(filter.as_ref(), &old, ctx)? else {
                    continue;
                };
                let Some(result) = crate::trigger::fire_instead_row(
                    write_ctx.catalog_kv,
                    &view,
                    crate::trigger::DmlEvent::Delete,
                    &[],
                    Some(&old),
                    None,
                    ctx,
                )?
                else {
                    continue;
                };
                count += 1;
                if returning.is_some() {
                    returned.push(ReturnedRow {
                        new: None,
                        old: Some(result),
                        source: joined[view.columns.len()..].to_vec(),
                        action: None,
                    });
                }
            }
            Ok((
                spec.outcome(format!("DELETE {count}"), returned, ctx)?,
                Vec::new(),
            ))
        }
        _ => unreachable!(),
    }
}

async fn execute_write_body(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let resolved = resolve_write_subqueries(write_ctx, ctes, stmt)?;
    let stmt = &resolved;
    let resolution = write_ctx.eval_ctx.resolution();
    let catalog_kv = write_ctx.catalog_kv;
    let kv = write_ctx.kv;
    let lockmgr = write_ctx.lockmgr;
    let seq = write_ctx.seq;
    let xid = write_ctx.xid;
    let ctx = write_ctx.eval_ctx;
    let mut ops: Vec<crabka_pgkv::WriteOp> = Vec::new();
    match stmt {
        // The read body of a statement whose `WITH` list modified data. The CTE
        // relations are already in `ctes`; the query itself runs read-only.
        Statement::Query(q) => {
            let read = write_ctx.read_ctx(ctes);
            let rel = crate::query::query_to_relation(&read, q)?;
            let tag = format!("SELECT {}", rel.rows.len());
            Ok((
                WriteOutcome {
                    tag,
                    returning: Some(rel),
                },
                ops,
            ))
        }
        Statement::Insert { table, .. }
        | Statement::Update { table, .. }
        | Statement::Delete { table, .. }
            if is_view_ref(catalog_kv, resolution, table)? =>
        {
            Box::pin(execute_view_dml(write_ctx, ctes, stmt)).await
        }
        Statement::Insert { table, .. } if is_partitioned_ref(catalog_kv, resolution, table)? => {
            partitioned_insert(write_ctx, ctes, stmt, writes).await
        }
        Statement::Update { table, .. } | Statement::Delete { table, .. }
            if is_partitioned_ref(catalog_kv, resolution, table)? =>
        {
            Box::pin(partitioned_dml(write_ctx, ctes, stmt, writes)).await
        }
        Statement::Merge { table, .. } if is_partitioned_ref(catalog_kv, resolution, table)? => {
            Err(ExecError::Unsupported(
                "MERGE into a partitioned table is not supported: a source row that matches no \
                 target row would have to be routed, and the matched/not-matched decision spans \
                 every partition at once"
                    .into(),
            ))
        }
        Statement::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
            ..
        } => {
            let table =
                &resolve_relation(catalog_kv, resolution, table, SchemaDisposition::Reference)?;
            let t = crabka_pgcatalog::get_table(catalog_kv, table)?;
            let (target_idx, rows) = insert_source_rows(write_ctx, ctes, &t, columns, source)?;
            let rows = &rows;
            if rows.is_empty() {
                return Ok((WriteOutcome::command("INSERT 0 0".into()), ops));
            }
            let local_indexes = writable_local_indexes(catalog_kv, &t)?;
            let fk_ctx = crate::fk::StatementFkContext::resolve(catalog_kv, &t)?;
            // Arbiter resolution is statement-level: a bad conflict target is an
            // error even when no row would have conflicted.
            let arbiters = match on_conflict {
                Some(on_conflict) => {
                    resolve_arbiter_indexes(&t, &local_indexes, &on_conflict.target)?
                }
                None => Vec::new(),
            };
            // The command tag's N: inserted rows plus rows updated by DO UPDATE.
            // Rows skipped by DO NOTHING or by a false DO UPDATE … WHERE do not
            // count. Without an ON CONFLICT clause this always ends at rows.len().
            let mut inserted_or_updated: u64 = 0;
            // Reserve a contiguous block of rowids atomically. In Durable mode the
            // SequenceManager persists the new next-rowid itself (seq_op is None).
            // In Replicated mode it returns the seq Put for us to fold into this
            // same commit batch (max-merged by the replicated state machine).
            // Rows that end up skipped or updated leave their reserved rowid
            // unused, exactly as PostgreSQL burns a sequence value per proposed row.
            let n_rows = rows.len() as u64;
            let (start, seq_op) = seq.alloc(kv, t.id, n_rows)?;
            if let Some(op) = seq_op {
                ops.push(op);
            }
            let mut returned_rows = returning
                .as_ref()
                .map(|_| Vec::with_capacity(rows.len()))
                .unwrap_or_default();
            for (rowid, row_exprs) in (start..).zip(rows.iter()) {
                // `insert_source_rows` already sized the target list to the
                // source's width, so every row fills exactly `target_idx`.
                // Defaults, coercion and NOT NULL apply to the proposed row even
                // when ON CONFLICT would go on to skip it — PostgreSQL raises
                // 23502 on a DO NOTHING row too.
                let full = build_insert_row(&t, &target_idx, row_exprs, ctx)?;
                let Some(full) = crate::trigger::fire_before_row(
                    catalog_kv,
                    &t,
                    crate::trigger::DmlEvent::Insert,
                    &[],
                    None,
                    Some(full),
                    ctx,
                )?
                else {
                    continue;
                };
                check_partition_constraint(catalog_kv, &t, &full)?;
                if let Some(on_conflict) = on_conflict {
                    let plan =
                        arbitrate_insert_row(write_ctx, &t, &arbiters, on_conflict, &full, writes)
                            .await?;
                    match plan {
                        InsertRowPlan::Insert => {}
                        // The arbiter key lock stays held to COMMIT/ROLLBACK, as
                        // it does for every other unique-key decision.
                        InsertRowPlan::Skip => continue,
                        // NB: the conflicting row's rowid, not this VALUES row's
                        // reserved one — that reservation goes unused.
                        InsertRowPlan::Update {
                            rowid: holder_rowid,
                            cur_key_xid,
                            cur_xmin,
                            cur_row,
                        } => {
                            let crabka_pgparser::ast::OnConflictAction::DoUpdate {
                                assignments,
                                filter,
                            } = &on_conflict.action
                            else {
                                unreachable!("only DO UPDATE plans a row update")
                            };
                            let updated = apply_insert_conflict_update(
                                write_ctx,
                                &t,
                                &local_indexes,
                                &fk_ctx,
                                &ConflictUpdate {
                                    assignments,
                                    filter: filter.as_ref(),
                                    rowid: holder_rowid,
                                    cur_key_xid,
                                    cur_xmin,
                                    cur_row: &cur_row,
                                    proposed: &full,
                                },
                                writes,
                                &mut ops,
                            )
                            .await?;
                            // A DO UPDATE … WHERE that is not true leaves the row
                            // neither inserted nor updated, with no RETURNING row.
                            let Some(next) = updated else { continue };
                            writes.claim_row(t.id, holder_rowid);
                            if returning.is_some() {
                                returned_rows.push(ReturnedRow::updated(next, cur_row, Vec::new()));
                            }
                            inserted_or_updated += 1;
                            continue;
                        }
                    }
                }
                // No conflict (or no ON CONFLICT clause): the plain insert path.
                // Re-locking the arbiter keys here is idempotent, and this also
                // enforces 23505 on the unique indexes that do NOT arbitrate —
                // PostgreSQL's ordering.
                enforce_unique_local_indexes(write_ctx, &t, &local_indexes, rowid, &full, writes)
                    .await?;
                // Append only, never probe: the check runs once the statement's
                // rows exist, which is what makes a self-referencing
                // `INSERT INTO t (id, boss) VALUES (1, 1)` succeed under a
                // NOT DEFERRABLE constraint, exactly as it does in PostgreSQL.
                if !fk_ctx.is_empty() {
                    writes.fk_checks.after_insert(&fk_ctx, rowid, &full)?;
                }
                if returning.is_some() {
                    returned_rows.push(ReturnedRow {
                        new: Some(full.clone()),
                        old: None,
                        source: Vec::new(),
                        action: None,
                    });
                }
                ops.push(crabka_pgkv::WriteOp::Put {
                    key: crabka_pgmvcc::version::version_key_xid(t.id, rowid, xid),
                    value: crabka_pgmvcc::version::encode_tuple(
                        xid,
                        crabka_pgmvcc::xid::INVALID_XID,
                        &full,
                    ),
                });
                ops.extend(local_index_entry_ops(&t, &local_indexes, rowid, &full)?);
                crate::trigger::fire_after_row(
                    catalog_kv,
                    &t,
                    crate::trigger::DmlEvent::Insert,
                    &[],
                    None,
                    Some(&full),
                    ctx,
                )?;
                inserted_or_updated += 1;
            }
            let tag = format!("INSERT 0 {inserted_or_updated}");
            let spec =
                ReturningSpec::new(&t, &t.name.to_string(), returning.as_ref(), None, false)?;
            Ok((spec.outcome(tag, returned_rows, ctx)?, ops))
        }
        Statement::Update {
            table,
            alias,
            assignments,
            from,
            filter,
            returning,
            ..
        } => {
            let table =
                &resolve_relation(catalog_kv, resolution, table, SchemaDisposition::Reference)?;
            let t = crabka_pgcatalog::get_table(catalog_kv, table)?;
            let local_indexes = writable_local_indexes(catalog_kv, &t)?;
            let fk_ctx = crate::fk::StatementFkContext::resolve(catalog_kv, &t)?;
            let qualifier = table_qualifier(&t, alias);
            let source = DmlSource::build(write_ctx, ctes, &t, qualifier, from)?;
            let targets = resolve_assignments(write_ctx, ctes, &t, assignments)?;
            let spec = ReturningSpec::new(
                &t,
                qualifier,
                returning.as_ref(),
                Some(&source.scope),
                false,
            )?;
            let mut n: u64 = 0;
            let mut returned_rows = returning.as_ref().map(|_| Vec::new()).unwrap_or_default();
            for (rowid, _xmin, scanned_row) in
                write_candidate_rows(write_ctx, &t, source.probe_filter(filter.as_ref()))?
            {
                // 1. Filter on the snapshot-visible row FIRST — do not lock rows
                //    that don't match the WHERE clause (avoids over-locking and
                //    restores row-level write concurrency for different rows).
                if source
                    .first_match(filter.as_ref(), &scanned_row, ctx)?
                    .is_none()
                {
                    continue;
                }
                // 2. Lock only matching candidates.
                lockmgr
                    .acquire(
                        t.id,
                        rowid,
                        crate::lockmgr::LockMode::Exclusive,
                        xid,
                        write_ctx.lock_wait_cap,
                    )
                    .await
                    .map_err(lock_acquire_error)?;
                // 3. EvalPlanQual: re-read this row under the lock and decide what to
                //    operate on (40001 under RR if changed since our snapshot).
                let Some((cur_key_xid, cur_xmin, cur_row)) =
                    eval_plan_qual(&write_ctx.mutation(), &t, rowid)?
                else {
                    continue; // deleted by a concurrent committed txn — skip
                };
                // 4. Re-check the filter on the (possibly re-found) current row —
                //    under READ COMMITTED the row may have changed since the scan.
                //    A joined UPDATE updates each target row once, using the first
                //    source row it matches (PostgreSQL leaves the choice
                //    unspecified when several match).
                let Some(joined) = source.first_match(filter.as_ref(), &cur_row, ctx)? else {
                    continue; // no longer matches the WHERE clause
                };
                // 5. PostgreSQL modifies a given row at most once per command, so
                //    a row another part of this statement already updated or
                //    deleted is left alone rather than updated again.
                if !writes.claim_row(t.id, rowid) {
                    continue;
                }
                let next = apply_assignments(&t, &targets, &source.scope, &joined, ctx)?;
                let updated_columns: Vec<String> = assignments
                    .iter()
                    .flat_map(|assignment| assignment.targets.iter().cloned())
                    .collect();
                let Some(next) = crate::trigger::fire_before_row(
                    catalog_kv,
                    &t,
                    crate::trigger::DmlEvent::Update,
                    &updated_columns,
                    Some(&cur_row),
                    Some(next),
                    ctx,
                )?
                else {
                    continue;
                };
                check_partition_constraint(catalog_kv, &t, &next)?;
                apply_locked_row_update(
                    write_ctx,
                    &t,
                    &local_indexes,
                    &fk_ctx,
                    &LockedRowUpdate {
                        rowid,
                        cur_key_xid,
                        cur_xmin,
                        cur_row: &cur_row,
                        next: &next,
                    },
                    writes,
                    &mut ops,
                )
                .await?;
                crate::trigger::fire_after_row(
                    catalog_kv,
                    &t,
                    crate::trigger::DmlEvent::Update,
                    &updated_columns,
                    Some(&cur_row),
                    Some(&next),
                    ctx,
                )?;
                if returning.is_some() {
                    returned_rows.push(ReturnedRow::updated(
                        next,
                        cur_row,
                        joined[t.columns.len()..].to_vec(),
                    ));
                }
                n += 1;
            }
            let tag = format!("UPDATE {n}");
            Ok((spec.outcome(tag, returned_rows, ctx)?, ops))
        }
        Statement::Delete {
            table,
            alias,
            using,
            filter,
            returning,
            ..
        } => {
            let table =
                &resolve_relation(catalog_kv, resolution, table, SchemaDisposition::Reference)?;
            let t = crabka_pgcatalog::get_table(catalog_kv, table)?;
            let local_indexes = writable_local_indexes(catalog_kv, &t)?;
            // A `TRUNCATE` desugars to one of these per relation; the truncate
            // set suppresses exactly the parent-side keys whose child is being
            // emptied in the same statement, so no referential action fires.
            let fk_ctx = crate::fk::StatementFkContext::resolve_for_truncate(
                catalog_kv,
                &t,
                &writes.truncate_set,
            )?;
            let is_truncate = writes.truncate_set.contains(&t.id);
            let qualifier = table_qualifier(&t, alias);
            let source = DmlSource::build(write_ctx, ctes, &t, qualifier, using)?;
            let spec = ReturningSpec::new(
                &t,
                qualifier,
                returning.as_ref(),
                Some(&source.scope),
                false,
            )?;
            let mut n: u64 = 0;
            let mut returned_rows = returning.as_ref().map(|_| Vec::new()).unwrap_or_default();
            for (rowid, _xmin, scanned_row) in
                write_candidate_rows(write_ctx, &t, source.probe_filter(filter.as_ref()))?
            {
                // 1. Filter on the snapshot-visible row FIRST — do not lock rows
                //    that don't match the WHERE clause.
                if source
                    .first_match(filter.as_ref(), &scanned_row, ctx)?
                    .is_none()
                {
                    continue;
                }
                // 2. Lock only matching candidates.
                lockmgr
                    .acquire(
                        t.id,
                        rowid,
                        crate::lockmgr::LockMode::Exclusive,
                        xid,
                        write_ctx.lock_wait_cap,
                    )
                    .await
                    .map_err(lock_acquire_error)?;
                // 3. EvalPlanQual: re-read this row under the lock.
                let Some((cur_key_xid, cur_xmin, cur_row)) =
                    eval_plan_qual(&write_ctx.mutation(), &t, rowid)?
                else {
                    continue; // already deleted by a concurrent committed txn
                };
                // 4. Re-check filter on the (possibly re-found) current row.
                let Some(joined) = source.first_match(filter.as_ref(), &cur_row, ctx)? else {
                    continue; // no longer matches the WHERE clause
                };
                // 5. A row another part of this statement already updated or
                //    deleted is left alone: PostgreSQL modifies a given row at
                //    most once per command, so it is neither deleted again nor
                //    RETURNed a second time.
                if !writes.claim_row(t.id, rowid) {
                    continue;
                }
                if !is_truncate
                    && crate::trigger::fire_before_row(
                        catalog_kv,
                        &t,
                        crate::trigger::DmlEvent::Delete,
                        &[],
                        Some(&cur_row),
                        None,
                        ctx,
                    )?
                    .is_none()
                {
                    continue;
                }
                // Append only: the parent-side probe needs the KV and the lock
                // manager, and the row's tombstone is only staged, so the check
                // waits for the end of the statement.
                if !fk_ctx.is_empty() {
                    writes.fk_checks.after_delete(&fk_ctx, rowid, &cur_row)?;
                }
                if returning.is_some() {
                    returned_rows.push(ReturnedRow {
                        new: None,
                        old: Some(cur_row.clone()),
                        source: joined[t.columns.len()..].to_vec(),
                        action: None,
                    });
                }
                apply_locked_row_delete(
                    write_ctx,
                    &t,
                    &local_indexes,
                    &LockedRowDelete {
                        rowid,
                        cur_key_xid,
                        cur_xmin,
                        cur_row: &cur_row,
                    },
                    writes,
                    &mut ops,
                )?;
                if !is_truncate {
                    crate::trigger::fire_after_row(
                        catalog_kv,
                        &t,
                        crate::trigger::DmlEvent::Delete,
                        &[],
                        Some(&cur_row),
                        None,
                        ctx,
                    )?;
                }
                n += 1;
            }
            let tag = format!("DELETE {n}");
            Ok((spec.outcome(tag, returned_rows, ctx)?, ops))
        }
        Statement::Merge { .. } => Box::pin(execute_merge(write_ctx, ctes, stmt, writes)).await,
        Statement::Truncate {
            names,
            restart_identity,
            cascade,
        } => {
            if *restart_identity {
                return Err(ExecError::Unsupported(
                    "TRUNCATE RESTART IDENTITY is not supported: SERIAL sequence ownership is not tracked".into(),
                ));
            }
            // Validate every name (and refuse sharded targets) before touching
            // any table: the statement is all-or-nothing across the list.
            let mut named = Vec::with_capacity(names.len());
            for name in
                resolve_relations(catalog_kv, resolution, names, SchemaDisposition::Utility)?
            {
                let t = crabka_pgcatalog::get_table(catalog_kv, &name)?;
                if table_uses_global_visibility(&t) {
                    return Err(ExecError::Unsupported(
                        "TRUNCATE on sharded tables is not supported".into(),
                    ));
                }
                named.push(t);
            }
            // `TRUNCATE` does not fire `ON DELETE CASCADE`: it refuses when a
            // relation outside the set references one inside it, and `CASCADE`
            // widens the *set* instead. Divergence: PostgreSQL also emits a
            // `NOTICE: truncate cascades to table "…"` per relation `CASCADE`
            // pulls in, which this engine has no NoticeResponse path for, so
            // `TruncateSet::cascaded` is computed and left unemitted.
            let set = crate::fk::expand_truncate_set(catalog_kv, &named, *cascade)?;
            // Carried on the statement's write state so each desugared DELETE
            // suppresses exactly the parent-side keys whose child is in the set
            // — by construction every one of them, so no action ever fires.
            writes.truncate_set = set.ids();
            // Desugar to an unfiltered DELETE per table: TRUNCATE shares the
            // MVCC write path (row locks, xmax stamping, rollback) rather
            // than clearing storage, so it is transactional like PostgreSQL's.
            for table in &set.tables {
                let delete = Statement::Delete {
                    table: crabka_pgparser::ast::RelationRef {
                        schema: Some(table.name.schema.clone()),
                        name: table.name.name.clone(),
                    },
                    alias: None,
                    filter: None,
                    using: Vec::new(),
                    returning: None,
                    with: None,
                };
                let (_, delete_ops) =
                    Box::pin(execute_write_body(write_ctx, ctes, &delete, writes)).await?;
                ops.extend(delete_ops);
            }
            Ok((WriteOutcome::command("TRUNCATE TABLE".into()), ops))
        }
        _ => Err(ExecError::Unsupported("not a write statement".into())),
    }
}

/// The name every expression in a DML statement resolves the target's columns
/// under: its alias when it has one, else the table name. `PostgreSQL` hides
/// the real name once an alias is given.
fn table_qualifier<'a>(table: &'a Table, alias: &'a Option<String>) -> &'a str {
    alias.as_deref().unwrap_or(&table.name.name)
}

/// The `FROM`/`USING` relation joined to a DML target, materialized once for the
/// whole statement. The plain (unjoined) form is the degenerate case with one
/// empty source row, so both share one code path.
struct DmlSource {
    /// Target columns first, then the source relation's columns.
    scope: Scope,
    rows: Vec<Vec<Datum>>,
    joined: bool,
}

impl DmlSource {
    fn build(
        write_ctx: &WriteContext<'_>,
        ctes: &crate::cte::CteContext,
        table: &Table,
        qualifier: &str,
        from: &[crabka_pgparser::ast::TableExpr],
    ) -> Result<Self, ExecError> {
        let mut scope = Scope::single(table, qualifier);
        if from.is_empty() {
            return Ok(Self {
                scope,
                rows: vec![Vec::new()],
                joined: false,
            });
        }
        let read = write_ctx.read_ctx(ctes);
        let rel = build_from(&read, from, None, None, None)?;
        scope.columns.extend(rel.scope.columns);
        Ok(Self {
            scope,
            rows: rel.rows,
            joined: true,
        })
    }

    /// The predicate the index-probe planner may use to narrow the target scan.
    /// A joined statement's `WHERE` mentions source columns the probe cannot
    /// resolve, so it falls back to a full scan.
    fn probe_filter<'f>(&self, filter: Option<&'f Expr>) -> Option<&'f Expr> {
        if self.joined { None } else { filter }
    }

    /// The first source row that satisfies `filter` for this target row, as the
    /// combined row expressions resolve against. `None` means the target row is
    /// not affected by the statement.
    fn first_match(
        &self,
        filter: Option<&Expr>,
        target_row: &[Datum],
        ctx: &crate::clock::EvalCtx,
    ) -> Result<Option<Vec<Datum>>, ExecError> {
        for source_row in &self.rows {
            let mut combined = target_row.to_vec();
            combined.extend_from_slice(source_row);
            if row_matches(filter, &self.scope, &combined, ctx)? {
                return Ok(Some(combined));
            }
        }
        Ok(None)
    }
}

/// One `SET` target after analysis: the column slot plus how its new value is
/// produced.
enum AssignedValue<'a> {
    /// Evaluated against the joined row, per affected row.
    Expr(&'a Expr),
    /// Already computed: a multi-column `= (SELECT …)`, which `PostgreSQL`
    /// evaluates once when the sub-select does not reference the target.
    Value(Datum),
    /// `SET j['a'][0] = e`: the write puts the new value *into* the column's
    /// current jsonb value at the subscripted path.
    Subscripted {
        subscripts: &'a [ArraySubscript],
        value: &'a Expr,
    },
}

/// Resolve every `SET` entry to a column slot and a value source, raising
/// `PostgreSQL`'s analysis errors up front: 42703 for an unknown column, 42701
/// for a column assigned twice, and 42601 for an arity mismatch on the
/// multi-column forms.
fn resolve_assignments<'a>(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    table: &Table,
    assignments: &'a [crabka_pgparser::ast::Assignment],
) -> Result<Vec<(usize, AssignedValue<'a>)>, ExecError> {
    use crabka_pgparser::ast::AssignmentValue;

    let mut out: Vec<(usize, AssignedValue<'a>)> = Vec::new();
    for assignment in assignments {
        let slots = assignment
            .targets
            .iter()
            .map(|column| {
                table
                    .column_index(column)
                    .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        match &assignment.value {
            AssignmentValue::Expr(expr) if !assignment.subscripts.is_empty() => {
                debug_assert_eq!(slots.len(), 1, "single-target assignment");
                out.push((
                    slots[0],
                    AssignedValue::Subscripted {
                        subscripts: &assignment.subscripts,
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
                for (slot, expr) in slots.iter().zip(items) {
                    out.push((*slot, AssignedValue::Expr(expr)));
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
                    out.push((*slot, AssignedValue::Value(value)));
                }
            }
        }
    }
    let mut seen = HashSet::new();
    for (slot, value) in &out {
        // Subscripted entries update the column in place rather than replacing
        // it, so `SET j['a'] = …, j['b'] = …` is legal in PostgreSQL and each
        // one sees the previous one's result.
        if matches!(value, AssignedValue::Subscripted { .. }) {
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

fn assignment_arity_error(targets: usize, values: usize) -> ExecError {
    ExecError::Syntax(format!(
        "number of columns ({targets}) does not match number of values ({values})"
    ))
}

/// Apply the resolved assignments to a copy of the target row.
fn apply_assignments(
    table: &Table,
    targets: &[(usize, AssignedValue<'_>)],
    scope: &Scope,
    joined_row: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Datum>, ExecError> {
    let mut next = joined_row[..table.columns.len()].to_vec();
    for (idx, value) in targets {
        let raw = match value {
            AssignedValue::Value(value) => value.clone(),
            AssignedValue::Expr(Expr::Default) => default_value(&table.columns[*idx], ctx)?,
            AssignedValue::Expr(expr) => crate::eval::eval(expr, scope, joined_row, ctx)?,
            // The subscripted form reads the column's *current* value, so a
            // second entry for the same column sees the first one's result.
            AssignedValue::Subscripted { subscripts, value } => {
                let args =
                    crate::eval::eval_assignment_subscripts(subscripts, scope, joined_row, ctx)?;
                let new_value = crate::eval::eval(value, scope, joined_row, ctx)?;
                // An array column writes into the array; anything else is the
                // jsonb path, which has no slice form.
                match table.columns[*idx].ty.array_element() {
                    Some(elem) => {
                        crate::array_fn::array_assign(&next[*idx], &args, &new_value, elem, ctx)?
                    }
                    None => {
                        let indexes = args
                            .iter()
                            .map(|arg| match arg {
                                crate::array_fn::SubscriptArg::Index(value) => Ok(value.clone()),
                                crate::array_fn::SubscriptArg::Slice { .. } => {
                                    Err(ExecError::TypeMismatch(
                                        "jsonb subscript does not support slices".into(),
                                    ))
                                }
                            })
                            .collect::<Result<Vec<_>, ExecError>>()?;
                        crate::json_fn::jsonb_subscript_assign(&next[*idx], &indexes, &new_value)?
                    }
                }
            }
        };
        next[*idx] = coerce(raw, table.columns[*idx].ty, ctx)?;
    }
    finish_written_row(table, &mut next, ctx)?;
    Ok(next)
}

/// One row a `RETURNING` clause will project: the post-image (absent for
/// `DELETE`), the pre-image (absent for a plain `INSERT`), and the joined
/// source columns the clause may also reference.
struct ReturnedRow {
    new: Option<Vec<Datum>>,
    old: Option<Vec<Datum>>,
    source: Vec<Datum>,
    /// What `merge_action()` reports for this row; `None` outside `MERGE`.
    action: Option<&'static str>,
}

impl ReturnedRow {
    fn updated(new: Vec<Datum>, old: Vec<Datum>, source: Vec<Datum>) -> Self {
        Self {
            new: Some(new),
            old: Some(old),
            source,
            action: None,
        }
    }
}

/// The prefix that makes an `OLD`/`NEW` image binding unreachable by a bare
/// column reference.
///
/// It cannot occur in any identifier the lexer produces, not even a quoted one,
/// which cannot contain a control character. So `RETURNING v` still resolves to
/// the one target column named `v`, as it does in `PostgreSQL`.
const IMAGE_BINDING_PREFIX: char = '\u{1}';

/// An analyzed `RETURNING` clause: the scope its expressions resolve against and
/// the projection rewritten so `OLD`/`NEW` references reach the image bindings.
struct ReturningSpec {
    scope: Scope,
    items: Vec<SelectItem>,
    /// Where the pre-image columns start in the combined row.
    old_offset: usize,
    /// Where the post-image columns start in the combined row.
    new_offset: usize,
    /// `MERGE` appends one `merge_action()` column after the image blocks.
    merge: bool,
    active: bool,
}

/// The name of the synthetic binding `merge_action()` is rewritten to. Like the
/// `OLD`/`NEW` image bindings it is unreachable by an ordinary column reference.
const MERGE_ACTION_BINDING: &str = "\u{1}merge_action";

impl ReturningSpec {
    fn new(
        table: &Table,
        qualifier: &str,
        returning: Option<&crabka_pgparser::ast::Returning>,
        source: Option<&Scope>,
        merge: bool,
    ) -> Result<Self, ExecError> {
        // `MERGE` lists its source relation before its target, so `RETURNING *`
        // expands source-first there and target-first everywhere else.
        let target_width = table.columns.len();
        let Some(returning) = returning else {
            return Ok(Self {
                scope: Scope::empty(),
                items: Vec::new(),
                old_offset: 0,
                new_offset: 0,
                merge,
                active: false,
            });
        };
        // The visible relations: the target, then any FROM/USING/MERGE source.
        let mut scope = match source {
            Some(source) => source.clone(),
            None => Scope::single(table, qualifier),
        };
        let visible_width = scope.width();
        // PostgreSQL 18's default `old`/`new` spellings are suppressed when a
        // relation of that name is already in scope.
        let taken = |name: &str| {
            scope
                .columns
                .iter()
                .any(|c| c.qualifier.as_deref() == Some(name))
        };
        // An explicit image alias is a relation name like any other: colliding
        // with a relation in scope, or with the other image's alias, is 42712.
        for alias in [&returning.old_alias, &returning.new_alias]
            .into_iter()
            .flatten()
        {
            if taken(alias) {
                return Err(ExecError::DuplicateAlias(alias.clone()));
            }
        }
        if let (Some(old), Some(new)) = (&returning.old_alias, &returning.new_alias)
            && old == new
        {
            return Err(ExecError::DuplicateAlias(old.clone()));
        }
        // An explicit alias also suppresses the OTHER image's default spelling,
        // so `RETURNING WITH (NEW AS old) old.v` reads the post-image.
        let old_alias = returning.old_alias.clone().or_else(|| {
            (!taken("old") && returning.new_alias.as_deref() != Some("old"))
                .then(|| "old".to_string())
        });
        let new_alias = returning.new_alias.clone().or_else(|| {
            (!taken("new") && returning.old_alias.as_deref() != Some("new"))
                .then(|| "new".to_string())
        });
        let old_offset = scope.width();
        scope.columns.extend(image_bindings(table, "old"));
        let new_offset = scope.width();
        scope.columns.extend(image_bindings(table, "new"));
        if merge {
            scope.columns.push(ColumnBinding {
                qualifier: None,
                name: MERGE_ACTION_BINDING.to_string(),
                ty: ColumnType::Text,
            });
        }
        let mut items = Vec::new();
        for item in &returning.items {
            match item {
                // `*` spans the visible relations only — never the image aliases.
                SelectItem::Wildcard => {
                    let order: Vec<usize> = if merge {
                        (target_width..visible_width)
                            .chain(0..target_width)
                            .collect()
                    } else {
                        (0..visible_width).collect()
                    };
                    items.extend(order.into_iter().map(|i| {
                        let c = &scope.columns[i];
                        SelectItem::Expr {
                            expr: Expr::Column {
                                table: c.qualifier.clone(),
                                name: c.name.clone(),
                            },
                            alias: Some(c.name.clone()),
                        }
                    }));
                }
                SelectItem::QualifiedWildcard(q) if Some(q) == old_alias.as_ref() => {
                    items.extend(image_wildcard(table, "old"));
                }
                SelectItem::QualifiedWildcard(q) if Some(q) == new_alias.as_ref() => {
                    items.extend(image_wildcard(table, "new"));
                }
                SelectItem::QualifiedWildcard(_) => items.push(item.clone()),
                SelectItem::Expr { expr, alias } => {
                    let rewritten = rewrite_image_refs(
                        expr,
                        &ImageAliases {
                            table,
                            old: old_alias.as_deref(),
                            new: new_alias.as_deref(),
                            merge,
                        },
                    );
                    // Rewriting hides the output name a bare `old.v` or
                    // `merge_action()` would otherwise derive, so it is pinned
                    // from the spelling the user wrote.
                    let alias = alias.clone().or_else(|| match expr {
                        Expr::Func(fc) if merge && fc.name == "merge_action" => {
                            Some("merge_action".to_string())
                        }
                        Expr::Column {
                            table: Some(q),
                            name,
                        } if Some(q.as_str()) == old_alias.as_deref()
                            || Some(q.as_str()) == new_alias.as_deref() =>
                        {
                            Some(name.clone())
                        }
                        _ => None,
                    });
                    items.push(SelectItem::Expr {
                        expr: rewritten,
                        alias,
                    });
                }
            }
        }
        Ok(Self {
            scope,
            items,
            old_offset,
            new_offset,
            merge,
            active: true,
        })
    }

    fn outcome(
        &self,
        tag: String,
        rows: Vec<ReturnedRow>,
        ctx: &crate::clock::EvalCtx,
    ) -> Result<WriteOutcome, ExecError> {
        if !self.active {
            return Ok(WriteOutcome::command(tag));
        }
        let width = self.new_offset - self.old_offset;
        let combined: Vec<Vec<Datum>> = rows
            .into_iter()
            .map(|row| {
                let nulls = vec![Datum::Null; width];
                // The visible target columns show the post-image, or the
                // pre-image for a DELETE, which is what PostgreSQL projects.
                let mut out = row
                    .new
                    .clone()
                    .or_else(|| row.old.clone())
                    .unwrap_or_else(|| nulls.clone());
                out.extend(row.source);
                out.extend(row.old.unwrap_or_else(|| nulls.clone()));
                out.extend(row.new.unwrap_or(nulls));
                if self.merge {
                    out.push(row.action.map_or(Datum::Null, |a| Datum::Text(a.into())));
                }
                out
            })
            .collect();
        let (fields, out_exprs, tys) = resolve_projection(&self.items, &self.scope)?;
        let projected = project_rows(&out_exprs, &self.scope, &combined, ctx)?;
        let scope = Scope {
            columns: fields
                .iter()
                .zip(&tys)
                .map(|(f, ty)| ColumnBinding {
                    qualifier: None,
                    name: f.name.clone(),
                    ty: *ty,
                })
                .collect(),
        };
        Ok(WriteOutcome {
            tag,
            returning: Some(Relation {
                scope,
                rows: projected,
            }),
        })
    }
}

fn image_binding_name(image: &str, column: &str) -> String {
    format!("{IMAGE_BINDING_PREFIX}{image}.{column}")
}

fn image_bindings(table: &Table, image: &str) -> Vec<ColumnBinding> {
    table
        .columns
        .iter()
        .map(|c| ColumnBinding {
            qualifier: None,
            name: image_binding_name(image, &c.name),
            ty: c.ty,
        })
        .collect()
}

fn image_wildcard(table: &Table, image: &str) -> Vec<SelectItem> {
    table
        .columns
        .iter()
        .map(|c| SelectItem::Expr {
            expr: Expr::Column {
                table: None,
                name: image_binding_name(image, &c.name),
            },
            alias: Some(c.name.clone()),
        })
        .collect()
}

/// Point every `old.col` / `new.col` reference at its image binding.
///
/// This leaves alone the nodes that cannot contain a row reference reachable
/// from `RETURNING`: literals, parameters, and the subquery forms, which have
/// their own scope.
struct ImageAliases<'a> {
    table: &'a Table,
    old: Option<&'a str>,
    new: Option<&'a str>,
    merge: bool,
}

fn rewrite_image_refs(expr: &Expr, aliases: &ImageAliases<'_>) -> Expr {
    let recurse = |e: &Expr| Box::new(rewrite_image_refs(e, aliases));
    let recurse_all = |items: &[Expr]| -> Vec<Expr> { items.iter().map(|e| *recurse(e)).collect() };
    match expr {
        // `merge_action()` is not an ordinary function: it reports which WHEN
        // clause produced the row, so it reads a per-row binding.
        Expr::Func(fc)
            if aliases.merge
                && fc.name == "merge_action"
                && matches!(&fc.args, crabka_pgparser::ast::FuncArgs::Exprs(a) if a.is_empty()) =>
        {
            Expr::Column {
                table: None,
                name: MERGE_ACTION_BINDING.to_string(),
            }
        }
        Expr::Column {
            table: Some(qualifier),
            name,
        } => {
            let image = if Some(qualifier.as_str()) == aliases.old {
                Some("old")
            } else if Some(qualifier.as_str()) == aliases.new {
                Some("new")
            } else {
                None
            };
            match image {
                // An image reference to a column the target does not have keeps
                // its readable spelling, so resolution reports 42703 against
                // `old.nope` rather than the internal binding name.
                Some(image) if aliases.table.column_index(name).is_some() => Expr::Column {
                    table: None,
                    name: image_binding_name(image, name),
                },
                Some(_) => Expr::Column {
                    table: None,
                    name: format!("{qualifier}.{name}"),
                },
                None => expr.clone(),
            }
        }
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: recurse(expr),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: recurse(left),
            right: recurse(right),
        },
        Expr::Func(fc) => Expr::Func(crabka_pgparser::ast::FuncCall {
            name: fc.name.clone(),
            distinct: fc.distinct,
            args: match &fc.args {
                crabka_pgparser::ast::FuncArgs::Star => crabka_pgparser::ast::FuncArgs::Star,
                crabka_pgparser::ast::FuncArgs::Exprs(args) => {
                    crabka_pgparser::ast::FuncArgs::Exprs(recurse_all(args))
                }
            },
            // The FILTER predicate is rewritten like an argument; dropping it
            // would turn a filtered aggregate into an unfiltered one.
            filter: fc.filter.as_deref().map(recurse),
        }),
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: recurse(expr),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: recurse(expr),
            list: recurse_all(list),
            negated: *negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: recurse(expr),
            low: recurse(low),
            high: recurse(high),
            negated: *negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            kind,
            escape,
        } => Expr::Like {
            expr: recurse(expr),
            pattern: recurse(pattern),
            negated: *negated,
            kind: *kind,
            escape: escape.as_ref().map(|e| recurse(e)),
        },
        Expr::Case {
            operand,
            whens,
            else_result,
        } => Expr::Case {
            operand: operand.as_ref().map(|e| recurse(e)),
            whens: whens
                .iter()
                .map(|(c, r)| (*recurse(c), *recurse(r)))
                .collect(),
            else_result: else_result.as_ref().map(|e| recurse(e)),
        },
        Expr::Cast { expr, ty } => Expr::Cast {
            expr: recurse(expr),
            ty: *ty,
        },
        Expr::ArrayLiteral(items) => Expr::ArrayLiteral(recurse_all(items)),
        Expr::Row(items) => Expr::Row(recurse_all(items)),
        Expr::Subscript { base, index } => Expr::Subscript {
            base: recurse(base),
            index: recurse(index),
        },
        other => other.clone(),
    }
}

/// `MERGE INTO target USING source ON cond WHEN …`.
///
/// The source relation and the target's visible rows are both materialized
/// against the statement snapshot, then joined on `ON`. Source rows drive the
/// `MATCHED` and `NOT MATCHED [BY TARGET]` clauses; a second pass over the
/// target rows no source row joined drives `NOT MATCHED BY SOURCE`. A target
/// row that two clauses would touch is `PostgreSQL`'s 21000.
async fn execute_merge(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    use crabka_pgparser::ast::{MergeAction, MergeMatchKind, MergeSource};

    let resolution = write_ctx.eval_ctx.resolution();
    let Statement::Merge {
        table,
        alias,
        source,
        on,
        clauses,
        returning,
        ..
    } = stmt
    else {
        return Err(ExecError::Unsupported("not a MERGE statement".into()));
    };
    let ctx = write_ctx.eval_ctx;
    let table = &resolve_relation(
        write_ctx.catalog_kv,
        resolution,
        table,
        SchemaDisposition::Reference,
    )?;
    let t = crabka_pgcatalog::get_table(write_ctx.catalog_kv, table)?;
    let local_indexes = writable_local_indexes(write_ctx.catalog_kv, &t)?;
    let fk_ctx = crate::fk::StatementFkContext::resolve(write_ctx.catalog_kv, &t)?;
    let qualifier = table_qualifier(&t, alias);
    let mut ops: Vec<crabka_pgkv::WriteOp> = Vec::new();

    let read = write_ctx.read_ctx(ctes);
    let source_rel = match source {
        MergeSource::Table { name, alias } => {
            let te = crabka_pgparser::ast::TableExpr::Table {
                name: name.clone(),
                only: false,
                alias: alias.clone(),
                columns: None,
                sample: None,
            };
            build_from(&read, std::slice::from_ref(&te), None, None, None)?
        }
        MergeSource::Query {
            query,
            alias,
            columns,
        } => {
            let rel = crate::query::query_to_relation(&read, query)?;
            crate::values::requalify_derived(rel, alias, columns)?
        }
    };
    let source_width = source_rel.scope.width();
    let mut scope = Scope::single(&t, qualifier);
    let target_width = scope.width();
    scope.columns.extend(source_rel.scope.columns.clone());
    let spec = ReturningSpec::new(&t, qualifier, returning.as_ref(), Some(&scope), true)?;

    let target_rows = write_candidate_rows(write_ctx, &t, None)?;
    let mut matched: HashSet<u64> = HashSet::new();
    let mut returned_rows = Vec::new();
    let mut n: u64 = 0;

    for source_row in &source_rel.rows {
        let mut any_match = false;
        for (rowid, _xmin, target_row) in &target_rows {
            let mut joined = target_row.clone();
            joined.extend_from_slice(source_row);
            if !row_matches(Some(on), &scope, &joined, ctx)? {
                continue;
            }
            any_match = true;
            matched.insert(*rowid);
            let Some(when) =
                pick_merge_clause(clauses, MergeMatchKind::Matched, &scope, &joined, ctx)?
            else {
                continue;
            };
            if matches!(when.action, MergeAction::DoNothing) {
                continue;
            }
            // A row any part of this statement already modified — an earlier
            // WHEN clause, or a data-modifying WITH item — is PostgreSQL's 21000.
            if !writes.claim_row(t.id, *rowid) {
                return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                    "21000",
                    "MERGE command cannot affect row a second time",
                )));
            }
            let applied = Box::pin(apply_merge_row_action(
                write_ctx,
                &MergeRowAction {
                    table: &t,
                    local_indexes: &local_indexes,
                    fk: &fk_ctx,
                    ctes,
                    scope: &scope,
                    rowid: *rowid,
                    joined: &joined,
                    action: &when.action,
                },
                writes,
                &mut ops,
            ))
            .await?;
            if let Some(row) = applied {
                n += 1;
                if spec.active {
                    returned_rows.push(row);
                }
            }
        }
        if any_match {
            continue;
        }
        let mut joined = vec![Datum::Null; target_width];
        joined.extend_from_slice(source_row);
        let Some(when) = pick_merge_clause(
            clauses,
            MergeMatchKind::NotMatchedByTarget,
            &scope,
            &joined,
            ctx,
        )?
        else {
            continue;
        };
        let MergeAction::Insert { columns, values } = &when.action else {
            continue; // DO NOTHING
        };
        let exprs: Vec<Expr> = values.clone().unwrap_or_default();
        // `INSERT DEFAULT VALUES` has no target list at all; otherwise a MERGE
        // insert action obeys the same arity rule as a plain INSERT, and reports
        // it with the same two messages.
        let target_idx = if values.is_none() {
            Vec::new()
        } else {
            resolve_insert_targets(&t, columns, exprs.len())?
        };
        // The VALUES may reference the source row, so they are evaluated against
        // the joined scope before the row is assembled.
        // A literal keeps its unresolved form so `build_insert_row` applies the
        // same `unknown`-literal typing a plain INSERT would; everything else is
        // folded against the joined row, which the source columns live in.
        let evaluated = exprs
            .iter()
            .zip(&target_idx)
            .map(|(expr, slot)| match expr {
                Expr::Default | Expr::StringLiteral(_) => Ok(expr.clone()),
                _ => crate::eval::eval(expr, &scope, &joined, ctx).map(|value| Expr::Const {
                    value,
                    ty: t.columns[*slot].ty,
                }),
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        let full = build_insert_row(&t, &target_idx, &evaluated, ctx)?;
        let Some(full) = crate::trigger::fire_before_row(
            write_ctx.catalog_kv,
            &t,
            crate::trigger::DmlEvent::Insert,
            &[],
            None,
            Some(full),
            ctx,
        )?
        else {
            continue;
        };
        let (rowid, seq_op) = write_ctx.seq.alloc(write_ctx.kv, t.id, 1)?;
        if let Some(op) = seq_op {
            ops.push(op);
        }
        enforce_unique_local_indexes(write_ctx, &t, &local_indexes, rowid, &full, writes).await?;
        if !fk_ctx.is_empty() {
            writes.fk_checks.after_insert(&fk_ctx, rowid, &full)?;
        }
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(t.id, rowid, write_ctx.xid),
            value: crabka_pgmvcc::version::encode_tuple(
                write_ctx.xid,
                crabka_pgmvcc::xid::INVALID_XID,
                &full,
            ),
        });
        ops.extend(local_index_entry_ops(&t, &local_indexes, rowid, &full)?);
        crate::trigger::fire_after_row(
            write_ctx.catalog_kv,
            &t,
            crate::trigger::DmlEvent::Insert,
            &[],
            None,
            Some(&full),
            ctx,
        )?;
        n += 1;
        if spec.active {
            returned_rows.push(ReturnedRow {
                new: Some(full),
                old: None,
                source: source_row.clone(),
                action: Some("INSERT"),
            });
        }
    }

    for (rowid, _xmin, target_row) in &target_rows {
        if matched.contains(rowid) {
            continue;
        }
        let mut joined = target_row.clone();
        joined.extend(std::iter::repeat_n(Datum::Null, source_width));
        let Some(when) = pick_merge_clause(
            clauses,
            MergeMatchKind::NotMatchedBySource,
            &scope,
            &joined,
            ctx,
        )?
        else {
            continue;
        };
        if matches!(when.action, MergeAction::DoNothing) {
            continue;
        }
        if !writes.claim_row(t.id, *rowid) {
            return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                "21000",
                "MERGE command cannot affect row a second time",
            )));
        }
        let applied = Box::pin(apply_merge_row_action(
            write_ctx,
            &MergeRowAction {
                table: &t,
                local_indexes: &local_indexes,
                fk: &fk_ctx,
                ctes,
                scope: &scope,
                rowid: *rowid,
                joined: &joined,
                action: &when.action,
            },
            writes,
            &mut ops,
        ))
        .await?;
        if let Some(row) = applied {
            n += 1;
            if spec.active {
                returned_rows.push(row);
            }
        }
    }

    Ok((spec.outcome(format!("MERGE {n}"), returned_rows, ctx)?, ops))
}

/// The first `WHEN` clause of `kind` whose `AND` condition holds for this row.
fn pick_merge_clause<'a>(
    clauses: &'a [crabka_pgparser::ast::MergeWhen],
    kind: crabka_pgparser::ast::MergeMatchKind,
    scope: &Scope,
    row: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<Option<&'a crabka_pgparser::ast::MergeWhen>, ExecError> {
    for clause in clauses.iter().filter(|c| c.kind == kind) {
        if row_matches(clause.condition.as_ref(), scope, row, ctx)? {
            return Ok(Some(clause));
        }
    }
    Ok(None)
}

struct MergeRowAction<'a> {
    table: &'a Table,
    local_indexes: &'a [crabka_pgcatalog::Index],
    fk: &'a crate::fk::StatementFkContext,
    ctes: &'a crate::cte::CteContext,
    scope: &'a Scope,
    rowid: u64,
    joined: &'a [Datum],
    action: &'a crabka_pgparser::ast::MergeAction,
}

/// Apply an `UPDATE`/`DELETE` merge action to one already-matched target row,
/// under the same lock + `EvalPlanQual` recheck the ordinary write path uses.
async fn apply_merge_row_action(
    write_ctx: &WriteContext<'_>,
    request: &MergeRowAction<'_>,
    writes: &mut StatementWrites,
    ops: &mut Vec<crabka_pgkv::WriteOp>,
) -> Result<Option<ReturnedRow>, ExecError> {
    use crabka_pgparser::ast::MergeAction;

    let t = request.table;
    let ctx = write_ctx.eval_ctx;
    write_ctx
        .lockmgr
        .acquire(
            t.id,
            request.rowid,
            crate::lockmgr::LockMode::Exclusive,
            write_ctx.xid,
            write_ctx.lock_wait_cap,
        )
        .await
        .map_err(lock_acquire_error)?;
    let Some((cur_key_xid, cur_xmin, cur_row)) =
        eval_plan_qual(&write_ctx.mutation(), t, request.rowid)?
    else {
        return Ok(None); // deleted by a concurrent committed transaction
    };
    let source = request.joined[t.columns.len()..].to_vec();
    match request.action {
        MergeAction::Update(assignments) => {
            let targets = resolve_assignments(write_ctx, request.ctes, t, assignments)?;
            let mut joined = cur_row.clone();
            joined.extend_from_slice(&source);
            let next = apply_assignments(t, &targets, request.scope, &joined, ctx)?;
            let updated_columns = assignments
                .iter()
                .flat_map(|assignment| assignment.targets.iter().cloned())
                .collect::<Vec<_>>();
            let Some(next) = crate::trigger::fire_before_row(
                write_ctx.catalog_kv,
                t,
                crate::trigger::DmlEvent::Update,
                &updated_columns,
                Some(&cur_row),
                Some(next),
                ctx,
            )?
            else {
                return Ok(None);
            };
            apply_locked_row_update(
                write_ctx,
                t,
                request.local_indexes,
                request.fk,
                &LockedRowUpdate {
                    rowid: request.rowid,
                    cur_key_xid,
                    cur_xmin,
                    cur_row: &cur_row,
                    next: &next,
                },
                writes,
                ops,
            )
            .await?;
            crate::trigger::fire_after_row(
                write_ctx.catalog_kv,
                t,
                crate::trigger::DmlEvent::Update,
                &updated_columns,
                Some(&cur_row),
                Some(&next),
                ctx,
            )?;
            Ok(Some(ReturnedRow {
                new: Some(next),
                old: Some(cur_row),
                source,
                action: Some("UPDATE"),
            }))
        }
        MergeAction::Delete => {
            if crate::trigger::fire_before_row(
                write_ctx.catalog_kv,
                t,
                crate::trigger::DmlEvent::Delete,
                &[],
                Some(&cur_row),
                None,
                ctx,
            )?
            .is_none()
            {
                return Ok(None);
            }
            // The deleted row's unique keys are free for a later part of this
            // statement, exactly as on the plain DELETE path.
            writes.release_row_keys(t, request.local_indexes, request.rowid, &cur_row, None)?;
            if !request.fk.is_empty() {
                writes
                    .fk_checks
                    .after_delete(request.fk, request.rowid, &cur_row)?;
            }
            if cur_xmin == write_ctx.xid {
                ops.push(crabka_pgkv::WriteOp::Put {
                    key: crabka_pgmvcc::version::version_key_xid(
                        t.id,
                        request.rowid,
                        write_ctx.xid,
                    ),
                    value: crabka_pgmvcc::version::encode_tuple(
                        write_ctx.xid,
                        write_ctx.xid,
                        &cur_row,
                    ),
                });
            } else {
                ops.push(crabka_pgkv::WriteOp::Put {
                    key: crabka_pgmvcc::version::version_key_xid(t.id, request.rowid, cur_key_xid),
                    value: crabka_pgmvcc::version::encode_tuple(cur_xmin, write_ctx.xid, &cur_row),
                });
            }
            crate::trigger::fire_after_row(
                write_ctx.catalog_kv,
                t,
                crate::trigger::DmlEvent::Delete,
                &[],
                Some(&cur_row),
                None,
                ctx,
            )?;
            Ok(Some(ReturnedRow {
                new: None,
                old: Some(cur_row),
                source,
                action: Some("DELETE"),
            }))
        }
        MergeAction::DoNothing | MergeAction::Insert { .. } => Ok(None),
    }
}

/// The rows an `INSERT` supplies, plus the target column slots they fill.
///
/// A feeding query is materialized before any row is written, so an
/// `INSERT … SELECT` that reads the target table sees the pre-insert snapshot
/// as `PostgreSQL` does.
fn insert_source_rows(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    table: &Table,
    columns: &Option<Vec<String>>,
    source: &crabka_pgparser::ast::InsertSource,
) -> Result<(Vec<usize>, Vec<Vec<Expr>>), ExecError> {
    use crabka_pgparser::ast::InsertSource;
    match source {
        InsertSource::Values(rows) => {
            // Rows of differing width are PostgreSQL's own 42601, raised before
            // the arity of the target list is even considered.
            let width = rows.first().map_or(0, Vec::len);
            if rows.iter().any(|row| row.len() != width) {
                return Err(ExecError::ValuesColumnCount);
            }
            Ok((resolve_insert_targets(table, columns, width)?, rows.clone()))
        }
        // Every column takes its default; an explicit column list is a syntax
        // error in PostgreSQL, so none can be present here.
        InsertSource::DefaultValues => Ok((Vec::new(), vec![Vec::new()])),
        InsertSource::Query(query) => {
            // Resolve the names first so an unknown column is 42703 before the
            // feeding query runs, as it is in PostgreSQL's parse analysis.
            resolve_targets(table, columns)?;
            let read = write_ctx.read_ctx(ctes);
            let Relation { scope, rows } = crate::query::query_to_relation(&read, query)?;
            let target_idx = resolve_insert_targets(table, columns, scope.width())?;
            let rows = rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .zip(&scope.columns)
                        .map(|(value, column)| Expr::Const {
                            value,
                            ty: column.ty,
                        })
                        .collect()
                })
                .collect();
            Ok((target_idx, rows))
        }
    }
}

pub(crate) async fn execute_copy_write(
    write_ctx: &WriteContext<'_>,
    copy: &crabka_pgparser::ast::CopyStmt,
    rows: &[Vec<Option<String>>],
) -> Result<(QueryResult, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let catalog_kv = write_ctx.catalog_kv;
    let kv = write_ctx.kv;
    let seq = write_ctx.seq;
    let snapshot_xid = write_ctx.xid;
    let ctx = write_ctx.eval_ctx;
    let resolution = ctx.resolution();
    let mut ops = Vec::new();
    let table = crabka_pgcatalog::get_table(
        catalog_kv,
        &resolve_relation(
            catalog_kv,
            resolution,
            &copy.table,
            SchemaDisposition::Utility,
        )?,
    )?;
    // Hoisted out of the row loop: the target's index set is the same for every
    // row, so reading it per row was a catalog round trip per row. A row that
    // routes to a partition leaf belongs to a different relation, so that case
    // — and only that case — reads again below.
    let parent_indexes = writable_local_indexes(catalog_kv, &table)?;
    // Hoisted for the same reason as the index set, and cached per routed leaf
    // below: one resolution per relation, never one per row.
    let parent_fk = crate::fk::StatementFkContext::resolve(catalog_kv, &table)?;
    let mut leaf_fk: HashMap<TableId, crate::fk::StatementFkContext> = HashMap::new();
    let mut writes = StatementWrites::default();
    let target_idx = resolve_targets(&table, &copy.columns)?;
    let n_rows = rows.len() as u64;
    crate::trigger::fire_statement(
        catalog_kv,
        &table,
        crate::trigger::DmlEvent::Insert,
        crabka_pgcatalog::trigger::TriggerTiming::Before,
        &[],
        ctx,
    )?;
    if n_rows == 0 {
        crate::trigger::fire_statement(
            catalog_kv,
            &table,
            crate::trigger::DmlEvent::Insert,
            crabka_pgcatalog::trigger::TriggerTiming::After,
            &[],
            ctx,
        )?;
        return Ok((command("COPY 0"), ops));
    }
    let (start, seq_op) = seq.alloc(kv, table.id, n_rows)?;
    if let Some(op) = seq_op {
        ops.push(op);
    }
    let partitioned = crate::partition::is_partitioned(catalog_kv, &table.name)?;
    let mut copied = 0_u64;
    for (rowid, row_values) in (start..).zip(rows.iter()) {
        if row_values.len() != target_idx.len() {
            return Err(ExecError::TypeMismatch(
                "COPY row has the wrong number of fields for the target columns".into(),
            ));
        }
        let full = build_copy_row(&table, &target_idx, row_values, ctx)?;
        // COPY into a partitioned parent routes each row exactly as INSERT
        // does; the reserved rowid block belongs to the parent, so a routed row
        // takes one from its own leaf instead.
        let (table, rowid, full, routed) = if partitioned {
            let Some((leaf, leaf_row)) = route_row_to_leaf(catalog_kv, &table, &full)? else {
                return Err(ExecError::NoPartitionForRow(table.name.to_string()));
            };
            let (leaf_rowid, seq_op) = seq.alloc(kv, leaf.id, 1)?;
            ops.extend(seq_op);
            (leaf, leaf_rowid, leaf_row, true)
        } else {
            check_partition_constraint(catalog_kv, &table, &full)?;
            (table.clone(), rowid, full, false)
        };
        let Some(full) = crate::trigger::fire_before_row(
            catalog_kv,
            &table,
            crate::trigger::DmlEvent::Insert,
            &[],
            None,
            Some(full),
            ctx,
        )?
        else {
            continue;
        };
        let routed_indexes = if routed {
            Some(writable_local_indexes(catalog_kv, &table)?)
        } else {
            None
        };
        let local_indexes = routed_indexes.as_deref().unwrap_or(&parent_indexes);
        let fk_ctx = if routed {
            match leaf_fk.entry(table.id) {
                std::collections::hash_map::Entry::Occupied(slot) => slot.into_mut(),
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(crate::fk::StatementFkContext::resolve(catalog_kv, &table)?)
                }
            }
        } else {
            &parent_fk
        };
        enforce_unique_local_indexes(write_ctx, &table, local_indexes, rowid, &full, &mut writes)
            .await?;
        if !fk_ctx.is_empty() {
            writes.fk_checks.after_insert(fk_ctx, rowid, &full)?;
        }
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(table.id, rowid, snapshot_xid),
            value: crabka_pgmvcc::version::encode_tuple(
                snapshot_xid,
                crabka_pgmvcc::xid::INVALID_XID,
                &full,
            ),
        });
        ops.extend(local_index_entry_ops(&table, local_indexes, rowid, &full)?);
        crate::trigger::fire_after_row(
            catalog_kv,
            &table,
            crate::trigger::DmlEvent::Insert,
            &[],
            None,
            Some(&full),
            ctx,
        )?;
        copied += 1;
    }
    // `COPY` is one command, so its referential checks fire once, after every
    // row is staged — the same timing an `INSERT` of the same rows would give.
    let fk_ops = drain_statement_fk_checks(write_ctx, &mut writes, &ops).await?;
    ops.extend(fk_ops);
    crate::trigger::fire_statement(
        catalog_kv,
        &table,
        crate::trigger::DmlEvent::Insert,
        crabka_pgcatalog::trigger::TriggerTiming::After,
        &[],
        ctx,
    )?;
    Ok((command(&format!("COPY {copied}")), ops))
}

pub(crate) fn execute_timestamp_copy_write(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    seq: &crate::seq::SequenceManager,
    copy: &crabka_pgparser::ast::CopyStmt,
    rows: &[Vec<Option<String>>],
    ctx: &crate::clock::EvalCtx,
) -> Result<TimestampWritePlan, ExecError> {
    let resolution = ctx.resolution();
    let table = crabka_pgcatalog::get_table(
        catalog_kv,
        &resolve_relation(
            catalog_kv,
            resolution,
            &copy.table,
            SchemaDisposition::Utility,
        )?,
    )?;
    if !table_uses_global_visibility(&table) {
        return Err(ExecError::Unsupported(
            "timestamp COPY requires a sharded table".into(),
        ));
    }
    let indexes = crabka_pgcatalog::list_table_indexes(catalog_kv, &table.name)?;
    if indexes.iter().any(|index| {
        index.placement == crabka_pgcatalog::IndexPlacement::Local
            || (index.placement == crabka_pgcatalog::IndexPlacement::Global && index.unique)
    }) {
        return Err(ExecError::Unsupported(
            "COPY index maintenance for sharded tables is not supported".into(),
        ));
    }
    let global_indexes = indexes
        .iter()
        .filter(|index| index.placement == crabka_pgcatalog::IndexPlacement::Global)
        .collect::<Vec<_>>();
    let target_idx = resolve_targets(&table, &copy.columns)?;
    let n_rows = rows.len() as u64;
    if n_rows == 0 {
        return Ok(TimestampWritePlan {
            result: command("COPY 0"),
            writes: Vec::new(),
            commit_ops: Vec::new(),
        });
    }
    let (start, seq_op) = seq.alloc(kv, table.id, n_rows)?;
    let mut writes = Vec::with_capacity(rows.len());
    for (rowid, row_values) in (start..).zip(rows.iter()) {
        if row_values.len() != target_idx.len() {
            return Err(ExecError::TypeMismatch(
                "COPY row has the wrong number of fields for the target columns".into(),
            ));
        }
        let row = build_copy_row(&table, &target_idx, row_values, ctx)?;
        writes.push(TimestampWrite {
            table_id: table.id,
            bucket: hash_bucket_for_row(&table, &row)?,
            rowid,
            global_index_intents: global_index_intents_for_row(
                &table,
                &global_indexes,
                rowid,
                &row,
            )?,
            row,
            delete: false,
        });
    }
    Ok(TimestampWritePlan {
        result: command(&format!("COPY {n_rows}")),
        writes,
        commit_ops: seq_op.into_iter().collect(),
    })
}

fn writable_local_indexes(
    catalog_kv: &dyn Kv,
    table: &Table,
) -> Result<Vec<crabka_pgcatalog::Index>, ExecError> {
    let indexes = crabka_pgcatalog::list_table_indexes(catalog_kv, &table.name)?;
    if indexes.is_empty() {
        return Ok(Vec::new());
    }
    let mut local_indexes = Vec::new();
    for index in indexes {
        if index.placement != crabka_pgcatalog::IndexPlacement::Local {
            if index.unique {
                return Err(ExecError::Unsupported(
                    "unique global indexes are not supported until global enforcement exists"
                        .into(),
                ));
            }
            continue;
        }
        reject_unwritable_local_index(table)?;
        local_indexes.push(index);
    }
    Ok(local_indexes)
}

/// Whether a DML statement must hold the engine's `unique_index_lock` SHARED
/// for its duration (until COMMIT/ROLLBACK in an explicit transaction).
///
/// Shared mode never blocks other DML. It only lets unique-index DDL (CREATE
/// UNIQUE INDEX backfill, CREATE TABLE with a unique constraint), which takes
/// the same lock EXCLUSIVELY, wait out in-flight writers and block new ones
/// while it scans. Same-key DML conflicts serialize through per-key locks in
/// the `RowLockManager` instead (see `enforce_unique_local_index`).
pub(crate) enum UniqueLocalSerialization {
    None,
    Shared,
}

pub(crate) fn write_requires_unique_local_serialization(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    stmt: &Statement,
) -> Result<UniqueLocalSerialization, ExecError> {
    let table_name = match stmt {
        Statement::Insert { table, .. }
        | Statement::Update { table, .. }
        | Statement::Delete { table, .. }
        | Statement::Merge { table, .. } => table,
        _ => return Ok(UniqueLocalSerialization::None),
    };
    let table_name = resolve_relation(
        catalog_kv,
        resolution,
        table_name,
        SchemaDisposition::Reference,
    )?;
    table_requires_unique_local_serialization(catalog_kv, &table_name)
}

pub(crate) fn copy_requires_unique_local_serialization(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    copy: &crabka_pgparser::ast::CopyStmt,
) -> Result<UniqueLocalSerialization, ExecError> {
    table_requires_unique_local_serialization(
        catalog_kv,
        &resolve_relation(
            catalog_kv,
            resolution,
            &copy.table,
            SchemaDisposition::Utility,
        )?,
    )
}

/// The op recording a temporary namespace, when it is not recorded already.
///
/// The engine creates a temporary namespace on behalf of a session that first
/// puts something in it, never a statement that names it. `CREATE SCHEMA`
/// refuses every `pg_`-prefixed name, as `PostgreSQL` does.
fn ensure_schema_ops(kv: &dyn Kv, schema: &str) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    if crabka_pgcatalog::schema_exists(kv, schema)? {
        return Ok(Vec::new());
    }
    Ok(vec![crabka_pgcatalog::create_temp_schema_op(schema)])
}

/// The batch that removes every relation `schema` holds, whatever kind each is,
/// together with everything outside the schema that depends on one of them:
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
/// # Errors
///
/// Returns undefined-relation, dependent-object, and storage/corruption errors
/// from the catalog KV seam.
fn drop_table_and_dependents_ops(
    kv: &dyn Kv,
    table: &Table,
    dropping: &HashSet<crabka_pgcatalog::RelationName>,
    cascade: bool,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let name = &table.name;
    let table_ops = crabka_pgcatalog::drop_table_ops(kv, name)?;
    let mut ops = Vec::new();
    let dependents: Vec<_> = dependent_view_names(kv, name, None)?
        .into_iter()
        .filter(|view| !dropping.contains(view))
        .collect();
    if !dependents.is_empty() {
        if !cascade {
            return Err(ExecError::DependentObjectsStillExist(format!(
                "cannot drop table {name} because other objects depend on it"
            )));
        }
        for view in &dependents {
            ops.extend(drop_view_with_triggers_ops(kv, view)?);
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
    }
    ops.extend(crate::partition::drop_metadata_ops(kv, name)?);
    ops.extend(crabka_pgcatalog::trigger::drop_triggers_for_table_ops(
        kv, table.id,
    )?);
    ops.extend(table_ops);
    Ok(ops)
}

fn drop_view_with_triggers_ops(
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
        // One foreign table per table the scanner discovers, which is only known
        // once the remote schema has been read.
        Statement::ImportForeignSchema { .. } => TableIdDemand::Unbounded,
        _ => TableIdDemand::None,
    }
}

pub(crate) fn ddl_requires_unique_local_serialization(stmt: &Statement) -> bool {
    match stmt {
        Statement::CreateIndex {
            unique: true,
            placement: crabka_pgparser::ast::IndexPlacement::Local,
            ..
        }
        // ADD PRIMARY KEY / ADD UNIQUE back-validates and backfills a local
        // unique index, so it must wait out in-flight writers exactly like
        // CREATE UNIQUE INDEX does.
        => true,
        Statement::AlterTable { actions, .. } => actions.iter().any(|action| {
            matches!(
                action,
                crabka_pgparser::ast::AlterTableAction::AddConstraint(
                    crabka_pgparser::ast::TableConstraint {
                        kind: crabka_pgparser::ast::TableConstraintKind::PrimaryKey(_)
                            | crabka_pgparser::ast::TableConstraintKind::Unique { .. },
                        ..
                    }
                )
            )
        }),
        Statement::CreateTable {
            columns,
            constraints,
            ..
        } => create_table_has_unique_constraint(columns, constraints),
        _ => false,
    }
}

pub(crate) fn inheritance_merge_notices(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    stmt: &Statement,
) -> Result<Vec<String>, ExecError> {
    let Statement::CreateTable { inherits, .. } = stmt else {
        return Ok(Vec::new());
    };
    let mut seen = std::collections::HashSet::new();
    let mut notices = Vec::new();
    for parent in inherits {
        let name = resolve_relation(kv, resolution, parent, SchemaDisposition::Reference)?;
        for column in crabka_pgcatalog::get_table(kv, &name)?.columns {
            if !seen.insert(column.name.clone()) && !notices.contains(&column.name) {
                notices.push(column.name);
            }
        }
    }
    Ok(notices)
}

fn create_table_has_unique_constraint(
    columns: &[crabka_pgparser::ast::ColumnDef],
    constraints: &[crabka_pgparser::ast::TableConstraint],
) -> bool {
    columns.iter().any(|column| {
        column.constraints.iter().any(|constraint| {
            matches!(
                constraint.kind,
                crabka_pgparser::ast::ColumnConstraintKind::PrimaryKey
                    | crabka_pgparser::ast::ColumnConstraintKind::Unique { .. }
            )
        })
    }) || constraints.iter().any(|constraint| {
        matches!(
            constraint.kind,
            crabka_pgparser::ast::TableConstraintKind::PrimaryKey(_)
                | crabka_pgparser::ast::TableConstraintKind::Unique { .. }
        )
    })
}

fn table_requires_unique_local_serialization(
    catalog_kv: &dyn Kv,
    table_name: &crabka_pgcatalog::RelationName,
) -> Result<UniqueLocalSerialization, ExecError> {
    let table = match crabka_pgcatalog::get_table(catalog_kv, table_name) {
        Ok(table) => table,
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_))
            if crabka_pgcatalog::get_view(catalog_kv, table_name).is_ok() =>
        {
            return Ok(UniqueLocalSerialization::None);
        }
        Err(error) => return Err(error.into()),
    };
    if table.sharded {
        return Ok(UniqueLocalSerialization::None);
    }
    let indexes = crabka_pgcatalog::list_table_indexes(catalog_kv, table_name)?;
    for index in indexes {
        if index.unique && index.placement != crabka_pgcatalog::IndexPlacement::Local {
            return Err(ExecError::Unsupported(
                "unique global indexes are not supported until global enforcement exists".into(),
            ));
        }
    }
    Ok(UniqueLocalSerialization::Shared)
}

fn reject_unwritable_local_index(table: &Table) -> Result<(), ExecError> {
    if table.sharded {
        return Err(ExecError::Unsupported(
            "local index maintenance for sharded timestamp writes is blocked on G-6".into(),
        ));
    }
    Ok(())
}

fn local_index_backfill_ops(
    kv: &dyn Kv,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    own_xid: Option<u64>,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let all_committed = all_committed_snapshot();
    // `own_xid` makes the open transaction's own uncommitted rows visible to the
    // back-validation; the all-committed snapshot alone does not, because the
    // scan still asks the commit log and this transaction is in progress there.
    let rows = scan_live(kv, kv, &all_committed, &all_committed, own_xid, table)?;
    local_index_backfill_ops_for_rows(&rows, table, index)
}

/// Backfill index entries for already-scanned live rows.
///
/// A UNIQUE index back-validates the existing data: a duplicate non-NULL key
/// fails the index *build* with 23505 before any op is committed. Rows with a
/// NULL key column are not indexed, which matches SQL NULL-distinct
/// semantics.
fn local_index_backfill_ops_for_rows(
    rows: &[(u64, u64, Vec<Datum>)],
    table: &Table,
    index: &crabka_pgcatalog::Index,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let mut seen = HashSet::new();
    let mut ops = Vec::with_capacity(rows.len());
    for (rowid, _xmin, row) in rows {
        for values in index_entries(table, index, row)? {
            if values.iter().any(Datum::is_null) {
                continue;
            }
            if index.unique && !seen.insert(values.clone()) {
                return Err(ExecError::UniqueIndexBuildViolation(index.name.clone()));
            }
            ops.push(crabka_pgkv::WriteOp::Put {
                key: crabka_pgkv::key::secondary_index_entry_key(
                    table.id, index.id, &values, *rowid,
                ),
                value: Vec::new(),
            });
        }
    }
    Ok(ops)
}

type PendingUniqueKey = (crabka_pgcatalog::IndexId, Vec<Datum>);

async fn enforce_unique_local_index_updates(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    indexes: &[crabka_pgcatalog::Index],
    rowid: u64,
    old_row: &[Datum],
    new_row: &[Datum],
    writes: &mut StatementWrites,
) -> Result<(), ExecError> {
    for index in indexes.iter().filter(|index| index.unique) {
        let old_values = indexed_values(table, index, old_row)?;
        let new_values = indexed_values(table, index, new_row)?;
        if old_values == new_values {
            // The indexed key is untouched: no probe, and — crucially for
            // write throughput — no key lock (a PK-preserving UPDATE takes
            // only its row lock).
            continue;
        }
        enforce_unique_local_index(write_ctx, table, index, rowid, new_values, writes).await?;
    }
    for index in indexes.iter().filter(|index| {
        matches!(
            index.constraint,
            Some(crabka_pgcatalog::IndexConstraint::Exclusion(_))
        )
    }) {
        let old_values = indexed_values(table, index, old_row)?;
        let new_values = indexed_values(table, index, new_row)?;
        if old_values != new_values {
            enforce_exclusion_constraint(write_ctx, table, index, rowid, new_values, writes)
                .await?;
        }
    }
    Ok(())
}

async fn enforce_unique_local_indexes(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    indexes: &[crabka_pgcatalog::Index],
    rowid: u64,
    row: &[Datum],
    writes: &mut StatementWrites,
) -> Result<(), ExecError> {
    for index in indexes.iter().filter(|index| index.unique) {
        let values = indexed_values(table, index, row)?;
        enforce_unique_local_index(write_ctx, table, index, rowid, values, writes).await?;
    }
    for index in indexes.iter().filter(|index| {
        matches!(
            index.constraint,
            Some(crabka_pgcatalog::IndexConstraint::Exclusion(_))
        )
    }) {
        let values = indexed_values(table, index, row)?;
        enforce_exclusion_constraint(write_ctx, table, index, rowid, values, writes).await?;
    }
    Ok(())
}

async fn enforce_exclusion_constraint(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    rowid: u64,
    values: Vec<Datum>,
    writes: &mut StatementWrites,
) -> Result<(), ExecError> {
    let Some(crabka_pgcatalog::IndexConstraint::Exclusion(operators)) = &index.constraint else {
        return Ok(());
    };
    if values.iter().any(Datum::is_null) {
        return Ok(());
    }
    // ponytail: This deliberately serializes per constraint. A spatial lock
    // structure can replace it when GiST indexes become physical rather than
    // catalog-only; correctness needs only this one coarse key today.
    write_ctx
        .lockmgr
        .acquire_key(
            crate::lockmgr::LockKey::UniqueKey(
                crabka_pgkv::key::secondary_index_entry_prefix(table.id, index.id, &[]),
            ),
            crate::lockmgr::LockMode::Exclusive,
            write_ctx.xid,
            write_ctx.lock_wait_cap,
        )
        .await
        .map_err(lock_acquire_error)?;

    let current_visibility = all_committed_snapshot();
    let rows = scan_live(
        write_ctx.kv,
        write_ctx.global,
        &current_visibility,
        &current_visibility,
        Some(write_ctx.xid),
        table,
    )?;
    for (holder_rowid, _xmin, holder_row) in rows {
        if holder_rowid == rowid || !writes.holder_still_holds(index.id, holder_rowid) {
            continue;
        }
        let holder = indexed_values(table, index, &holder_row)?;
        if exclusion_keys_conflict(operators, &values, &holder)? {
            return Err(exclusion_violation(write_ctx, table, index, &values, &holder));
        }
    }
    if let Some(pending) = writes.pending_exclusion_keys.get(&index.id) {
        for (holder_rowid, holder) in pending {
            if *holder_rowid != rowid && exclusion_keys_conflict(operators, &values, holder)? {
                return Err(exclusion_violation(write_ctx, table, index, &values, holder));
            }
        }
    }
    writes
        .pending_exclusion_keys
        .entry(index.id)
        .or_default()
        .push((rowid, values));
    Ok(())
}

fn exclusion_keys_conflict(
    operators: &[crabka_pgcatalog::ExclusionOperator],
    left: &[Datum],
    right: &[Datum],
) -> Result<bool, ExecError> {
    for ((operator, left), right) in operators.iter().zip(left).zip(right) {
        if left.is_null() || right.is_null() {
            return Ok(false);
        }
        let conflicts = match operator {
            crabka_pgcatalog::ExclusionOperator::Equal => {
                crabka_pgtypes::ops::compare(left, right)? == Some(std::cmp::Ordering::Equal)
            }
            crabka_pgcatalog::ExclusionOperator::Overlaps => match (left, right) {
                (Datum::Range(left), Datum::Range(right)) => {
                    crabka_pgtypes::range::overlaps(left, right)?
                }
                (Datum::Multirange(left), Datum::Multirange(right)) => {
                    crabka_pgtypes::multirange::overlaps(left, right)?
                }
                (Datum::Multirange(left), Datum::Range(right)) => {
                    crabka_pgtypes::multirange::overlaps_range(left, right)?
                }
                (Datum::Range(left), Datum::Multirange(right)) => {
                    crabka_pgtypes::multirange::overlaps_range(right, left)?
                }
                _ => return Err(ExecError::UndefinedFunction("operator &&".into())),
            },
        };
        if !conflicts {
            return Ok(false);
        }
    }
    Ok(true)
}

fn exclusion_violation(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    proposed: &[Datum],
    existing: &[Datum],
) -> ExecError {
    let columns = index.columns.join(", ");
    let render = |values: &[Datum]| {
        values
            .iter()
            .map(|value| {
                String::from_utf8_lossy(&crabka_pgtypes::encoding::encode_text_in(
                    value,
                    write_ctx.eval_ctx.output_style(),
                ))
                .into_owned()
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    ExecError::Remote(
        crabka_pgwire::error::PgError::error(
            "23P01",
            format!(
                "conflicting key value violates exclusion constraint \"{}\"",
                index.name
            ),
        )
        .with_detail(format!(
            "Key ({columns})=({}) conflicts with existing key ({columns})=({}).",
            render(proposed),
            render(existing)
        ))
        .with_schema(table.name.schema.clone())
        .with_table(table.name.name.clone())
        .with_constraint(index.name.clone()),
    )
}

async fn enforce_unique_local_index(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    rowid: u64,
    values: Vec<Datum>,
    writes: &mut StatementWrites,
) -> Result<(), ExecError> {
    if values.iter().any(Datum::is_null) {
        // SQL unique ignores NULLs: nothing to enforce, so no key lock either.
        return Ok(());
    }
    // The claim spans the whole statement, so a `WITH` item and the body cannot
    // both write the same key.
    let pending_key = (index.id, values.clone());
    if !writes.pending_unique_keys.insert(pending_key) {
        return Err(ExecError::UniqueViolation(index.name.clone()));
    }
    let holders = lock_and_probe_unique_key(write_ctx, table, index, &values).await?;
    // A holder whose key an earlier part of this statement freed is a version
    // this command has already superseded: PostgreSQL's uniqueness check does
    // not see it either.
    if holders
        .iter()
        .any(|holder| holder.rowid != rowid && writes.holder_still_holds(index.id, holder.rowid))
    {
        return Err(ExecError::UniqueViolation(index.name.clone()));
    }
    Ok(())
}

/// Take `values`' unique-key lock and return the rows that currently hold that
/// key.
///
/// This is the lock-and-probe half of unique enforcement, shared with
/// `ON CONFLICT` arbitration.
///
/// Serializes check-then-write PER KEY: takes this key's exclusive lock (in the
/// row-lock manager, so it shares the deadlock wait-for graph and is released
/// with the row locks at COMMIT/ROLLBACK) before probing. Without it, two
/// concurrent writers of the same key would both pass the probe, because
/// neither sees the other's uncommitted version, and both would commit. A
/// waiter that wakes here
/// after the holder's terminal outcome probes the then-current committed state:
/// a holder if it committed, none if it rolled back.
///
/// The probe reads exactly this key instead of scanning the whole table, under
/// the scan path's visibility (all-committed local + global snapshots plus our
/// own xid), and `lookup_local_index_equal` resolves each candidate rowid
/// through MVCC and re-checks its visible row's values. So dead entries left by
/// old versions or aborted writers never count.
///
/// `acquire_key` is idempotent for a holder xid, so a caller that already locked
/// this key (arbitration, before the insert path re-enforces uniqueness) can
/// call again without self-deadlocking.
async fn lock_and_probe_unique_key(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    values: &[Datum],
) -> Result<Vec<ScannedRow>, ExecError> {
    write_ctx
        .lockmgr
        .acquire_key(
            crate::lockmgr::LockKey::UniqueKey(crabka_pgkv::key::secondary_index_entry_prefix(
                table.id, index.id, values,
            )),
            crate::lockmgr::LockMode::Exclusive,
            write_ctx.xid,
            write_ctx.lock_wait_cap,
        )
        .await
        .map_err(lock_acquire_error)?;
    let mvcc = write_ctx.mvcc_read();
    let current_visibility = all_committed_snapshot();
    let probe = MvccReadContext {
        kv: mvcc.kv,
        global: mvcc.global,
        global_snapshot: &current_visibility,
        snapshot: &current_visibility,
        own: mvcc.own,
    };
    lookup_local_index_equal(&probe, table, index, values)
}

/// The unique local indexes that arbitrate an `ON CONFLICT` clause, resolved
/// once per statement (before the row loop) from the clause's conflict target.
///
/// - `Columns` matches every unique local index whose column SET equals the
///   inference set. PostgreSQL's inference is order-insensitive even though an
///   index's columns are ordered, so `ON CONFLICT (b, a)` arbitrates a
///   `UNIQUE (a, b)` index. No match is 42P10.
/// - `Columns` with an index predicate (`ON CONFLICT (c) WHERE …`) is refused
///   (0A000): partial indexes do not exist here, so nothing could ever match it.
/// - `OnConstraint` matches by index name, restricted to indexes that back a
///   constraint, because PostgreSQL rejects `ON CONSTRAINT` naming a plain
///   index.
///   No match is 42704.
/// - `None` (reachable only with `DO NOTHING`; the parser rejects a bare
///   `DO UPDATE`) arbitrates every unique local index. An empty result is legal:
///   a table with no unique index simply never conflicts.
///
/// Global unique indexes never reach here, because `writable_local_indexes`
/// refuses them for every write on the table.
fn resolve_arbiter_indexes(
    table: &Table,
    local_indexes: &[crabka_pgcatalog::Index],
    target: &crabka_pgparser::ast::OnConflictTarget,
) -> Result<Vec<crabka_pgcatalog::Index>, ExecError> {
    use crabka_pgparser::ast::OnConflictTarget;

    let unique = || local_indexes.iter().filter(|index| index.unique);
    match target {
        OnConflictTarget::None => Ok(unique().cloned().collect()),
        OnConflictTarget::Columns {
            index_predicate: Some(_),
            ..
        } => Err(ExecError::Unsupported(
            "ON CONFLICT inference predicates are not supported".into(),
        )),
        OnConflictTarget::Columns {
            columns,
            index_predicate: None,
        } => {
            for column in columns {
                if table.column_index(column).is_none() {
                    return Err(ExecError::UndefinedColumn(column.clone()));
                }
            }
            let wanted: BTreeSet<&str> = columns.iter().map(String::as_str).collect();
            let arbiters: Vec<_> = unique()
                .filter(|index| {
                    index
                        .columns
                        .iter()
                        .map(String::as_str)
                        .collect::<BTreeSet<_>>()
                        == wanted
                })
                .cloned()
                .collect();
            if arbiters.is_empty() {
                return Err(ExecError::OnConflictNoArbiter);
            }
            Ok(arbiters)
        }
        OnConflictTarget::OnConstraint(name) => local_indexes
            .iter()
            .find(|index| index.constraint.is_some() && index.name == *name)
            .map(|index| vec![index.clone()])
            .ok_or_else(|| ExecError::UndefinedConstraint {
                name: name.clone(),
                table: table.name.to_string(),
            }),
    }
}

/// What one `VALUES` row of an `INSERT … ON CONFLICT` should do, decided by
/// [`arbitrate_insert_row`].
enum InsertRowPlan {
    /// No arbiter conflicts: insert the proposed row through the normal path.
    Insert,
    /// `DO NOTHING` on a conflict: the row is skipped entirely. There are no
    /// ops, no RETURNING row, and it does not count towards the command tag.
    Skip,
    /// `DO UPDATE` on a conflict: the stored row to update, already locked and
    /// re-read under [`eval_plan_qual`].
    Update {
        rowid: u64,
        cur_key_xid: u64,
        cur_xmin: u64,
        cur_row: Vec<Datum>,
    },
}

/// Decide what an `INSERT … ON CONFLICT` does with one proposed row.
///
/// Probes the arbiter indexes in catalog order (`list_table_indexes` sorts by
/// name, so the choice of conflicting index is deterministic) and stops at the
/// first conflict. An arbiter whose key holds a NULL cannot conflict, because
/// SQL unique treats NULLs as distinct. That matches the enforcement path's own
/// short-circuit.
///
/// A key already claimed by an earlier row of THIS statement lives only in the
/// pending op batch, invisible to a KV probe, so it is checked separately:
/// `DO NOTHING` skips the row and `DO UPDATE` raises 21000. That reproduces
/// PostgreSQL's `INSERT … VALUES (1), (1)` semantics exactly.
///
/// Termination: the outer loop restarts only after adding a holder rowid to
/// `discarded` (its row vanished under the lock, or no longer carries the
/// arbiter key). Every probed key is held under an exclusive key lock for the
/// rest of the transaction, so the holder sets can only shrink and no new
/// holder can appear. `discarded` grows strictly on each restart, bounded by
/// the rows already in the table.
async fn arbitrate_insert_row(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    arbiters: &[crabka_pgcatalog::Index],
    on_conflict: &crabka_pgparser::ast::OnConflict,
    proposed: &[Datum],
    writes: &StatementWrites,
) -> Result<InsertRowPlan, ExecError> {
    use crabka_pgparser::ast::OnConflictAction;

    let do_update = matches!(on_conflict.action, OnConflictAction::DoUpdate { .. });
    let mut discarded: HashSet<u64> = HashSet::new();
    'arbitration: loop {
        for index in arbiters {
            let values = indexed_values(table, index, proposed)?;
            if values.iter().any(Datum::is_null) {
                continue;
            }
            if writes
                .pending_unique_keys
                .contains(&(index.id, values.clone()))
            {
                if do_update {
                    return Err(ExecError::OnConflictAffectsRowTwice);
                }
                return Ok(InsertRowPlan::Skip);
            }
            let holders = lock_and_probe_unique_key(write_ctx, table, index, &values).await?;
            // The proposed row has no version of its own yet, so every visible
            // holder is a genuine conflict.
            // A holder whose key an earlier part of this statement freed no
            // longer conflicts: that version has already been superseded.
            let Some(holder) = holders.into_iter().find(|holder| {
                !discarded.contains(&holder.rowid)
                    && writes.holder_still_holds(index.id, holder.rowid)
            }) else {
                continue;
            };
            if !do_update {
                return Ok(InsertRowPlan::Skip);
            }
            if writes.is_claimed(table.id, holder.rowid) {
                return Err(ExecError::OnConflictAffectsRowTwice);
            }
            // The probe deliberately reads all-committed visibility, so it finds
            // rows committed after our snapshot. `eval_plan_qual`'s own 40001
            // check keys off xmax stamps and would not catch a freshly inserted
            // row that our snapshot cannot see, so REPEATABLE READ needs this
            // explicit guard — without it the upsert would silently update a row
            // it cannot read.
            if write_ctx.repeatable_read
                && holder.xmin != write_ctx.xid
                && !snapshot_can_see(write_ctx.snapshot, holder.xmin)
            {
                return Err(ExecError::SerializationFailure);
            }
            // Key lock first, then row lock — the established order everywhere
            // on the write path, so upserts add no new deadlock shapes.
            write_ctx
                .lockmgr
                .acquire(
                    table.id,
                    holder.rowid,
                    crate::lockmgr::LockMode::Exclusive,
                    write_ctx.xid,
                    write_ctx.lock_wait_cap,
                )
                .await
                .map_err(lock_acquire_error)?;
            // READ COMMITTED: the probe reads all-committed visibility, so a
            // holder committed while we waited on the key lock is real but
            // invisible to our statement snapshot. `eval_plan_qual`'s own
            // read-committed refresh only fires on an `xmax` stamp, and a
            // concurrent INSERT leaves none — so re-read such a holder under a
            // fresh snapshot. Discarding it instead would fall through to the
            // insert path and raise 23505, breaking PostgreSQL's guarantee that
            // ON CONFLICT DO UPDATE yields an atomic insert-or-update outcome
            // even under high concurrency.
            let refreshed;
            let mutation = if !write_ctx.repeatable_read
                && holder.xmin != write_ctx.xid
                && !snapshot_can_see(write_ctx.snapshot, holder.xmin)
            {
                refreshed = write_ctx.procarray.snapshot();
                MutationContext {
                    snapshot: &refreshed,
                    ..write_ctx.mutation()
                }
            } else {
                write_ctx.mutation()
            };
            let Some((cur_key_xid, cur_xmin, cur_row)) =
                eval_plan_qual(&mutation, table, holder.rowid)?
            else {
                // Concurrently deleted: re-arbitrate without it.
                discarded.insert(holder.rowid);
                continue 'arbitration;
            };
            if indexed_values(table, index, &cur_row)? != values {
                // The row under the lock no longer carries the arbiter key.
                discarded.insert(holder.rowid);
                continue 'arbitration;
            }
            return Ok(InsertRowPlan::Update {
                rowid: holder.rowid,
                cur_key_xid,
                cur_xmin,
                cur_row,
            });
        }
        return Ok(InsertRowPlan::Insert);
    }
}

/// One locked row's in-place replacement: the version this write operates on
/// (`cur_key_xid`/`cur_xmin`/`cur_row`, as returned by [`eval_plan_qual`]) and
/// the post-image to store.
struct LockedRowUpdate<'a> {
    rowid: u64,
    cur_key_xid: u64,
    cur_xmin: u64,
    cur_row: &'a [Datum],
    next: &'a [Datum],
}

/// Stage the writes that replace a locked row with `next`.
///
/// Those are NOT NULL and unique enforcement, the referential checks the new
/// image owes, the MVCC version ops, index entries, and opportunistic chain
/// pruning. `UPDATE`, `MERGE`'s update action and `INSERT … ON CONFLICT DO
/// UPDATE` share this. Their stored-row mutation is identical once the row is
/// locked and the post-image computed, so all three reach the foreign-key hook
/// through this one site.
///
/// `fk` is the statement's resolved foreign-key context. A referential action
/// that re-enters here passes an empty one, because the drain derives the
/// follow-on checks a cascaded update owes from the row it hands back.
async fn apply_locked_row_update(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    local_indexes: &[crabka_pgcatalog::Index],
    fk: &crate::fk::StatementFkContext,
    update: &LockedRowUpdate<'_>,
    writes: &mut StatementWrites,
    ops: &mut Vec<crabka_pgkv::WriteOp>,
) -> Result<(), ExecError> {
    let LockedRowUpdate {
        rowid,
        cur_key_xid,
        cur_xmin,
        cur_row,
        next,
    } = *update;
    enforce_not_null(table, next)?;
    enforce_unique_local_index_updates(
        write_ctx,
        table,
        local_indexes,
        rowid,
        cur_row,
        next,
        writes,
    )
    .await?;
    // Append only — the probe needs the KV and the lock manager, and it must not
    // run until the statement's rows exist. A side whose key is unchanged queues
    // nothing, which is what keeps a non-key update of a hot parent row off the
    // key lock entirely.
    if !fk.is_empty() {
        writes.fk_checks.after_update(fk, rowid, cur_row, next)?;
    }
    // Whatever keys the superseded version held and this one does not are free
    // for a later part of the same statement to claim.
    writes.release_row_keys(table, local_indexes, rowid, cur_row, Some(next))?;
    let xid = write_ctx.xid;
    if cur_xmin == xid {
        // Updating my own uncommitted version: overwrite in place
        // (last-write-wins within the txn; no new tuple, xmax stays
        // invalid). PostgreSQL uses cmin/cmax here; we have no command
        // ids, so in-place replacement is the faithful observable result.
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(table.id, rowid, xid),
            value: crabka_pgmvcc::version::encode_tuple(xid, crabka_pgmvcc::xid::INVALID_XID, next),
        });
    } else {
        // Supersede a committed version: stamp its xmax, write a new
        // tuple. The stamp targets the version's PHYSICAL key
        // (`cur_key_xid`) — for a frozen tuple the header xmin
        // (`FROZEN_XID`) no longer names its key.
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(table.id, rowid, cur_key_xid),
            value: crabka_pgmvcc::version::encode_tuple(cur_xmin, xid, cur_row),
        });
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(table.id, rowid, xid),
            value: crabka_pgmvcc::version::encode_tuple(xid, crabka_pgmvcc::xid::INVALID_XID, next),
        });
    }
    ops.extend(local_index_entry_ops(table, local_indexes, rowid, next)?);
    // Opportunistic per-rowid chain pruning (local engines only):
    // we hold this row's exclusive lock and just re-read its chain,
    // so reclaim its dead versions in the same commit batch. The
    // versions this statement writes (`cur_xmin`, `xid`) are never
    // pruned, and `next`'s indexed values count as survivors.
    if let Some(horizon) = write_ctx.prune_horizon {
        ops.extend(
            prune_rowid_chain_ops(
                write_ctx.kv,
                table,
                local_indexes,
                &ChainPruneRequest {
                    rowid,
                    horizon,
                    keep_xids: &[cur_key_xid, xid],
                    new_row: Some(next),
                    freeze_below: None,
                },
            )?
            .ops,
        );
    }
    Ok(())
}

/// One locked row's tombstone: the version this delete operates on, exactly as
/// [`eval_plan_qual`] returned it.
struct LockedRowDelete<'a> {
    rowid: u64,
    cur_key_xid: u64,
    cur_xmin: u64,
    cur_row: &'a [Datum],
}

/// Stage the writes that delete a locked row.
///
/// Those are the unique keys it frees, the MVCC tombstone, and opportunistic
/// chain pruning. `DELETE` and a cascaded `ON DELETE CASCADE` share this. Their
/// stored-row mutation is identical once the row is locked and re-read.
///
/// Queues no referential check of its own: the caller knows whether this delete
/// is the statement's (which queues through [`crate::fk::FkCheckQueue`]) or a
/// referential action's (whose follow-on checks the drain derives itself).
fn apply_locked_row_delete(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    local_indexes: &[crabka_pgcatalog::Index],
    delete: &LockedRowDelete<'_>,
    writes: &mut StatementWrites,
    ops: &mut Vec<crabka_pgkv::WriteOp>,
) -> Result<(), ExecError> {
    let LockedRowDelete {
        rowid,
        cur_key_xid,
        cur_xmin,
        cur_row,
    } = *delete;
    let xid = write_ctx.xid;
    // The row's unique keys are free for a later part of this statement to
    // claim, even though its superseded version is still in the KV for the probe
    // to find.
    writes.release_row_keys(table, local_indexes, rowid, cur_row, None)?;
    if cur_xmin == xid {
        // Deleting my own uncommitted version: PostgreSQL stamps xmax=xid so it
        // is invisible to me. version_key is the same key; overwrite it with
        // xmax set.
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(table.id, rowid, xid),
            value: crabka_pgmvcc::version::encode_tuple(xid, xid, cur_row),
        });
    } else {
        // Set xmax = my xid on the matched version (keep its row bytes),
        // targeting its PHYSICAL key — see `apply_locked_row_update`.
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(table.id, rowid, cur_key_xid),
            value: crabka_pgmvcc::version::encode_tuple(cur_xmin, xid, cur_row),
        });
    }
    // Opportunistic per-rowid chain pruning (local engines only). The tombstoned
    // current version survives (its xmax is our in-progress xid), so its index
    // entries stay; an engine-level `vacuum` reclaims the chain once the delete
    // commits below a future horizon.
    if let Some(horizon) = write_ctx.prune_horizon {
        ops.extend(
            prune_rowid_chain_ops(
                write_ctx.kv,
                table,
                local_indexes,
                &ChainPruneRequest {
                    rowid,
                    horizon,
                    keep_xids: &[cur_key_xid, xid],
                    new_row: None,
                    freeze_below: None,
                },
            )?
            .ops,
        );
    }
    Ok(())
}

/// One `ON CONFLICT DO UPDATE` application: the clause's assignments and filter,
/// the locked stored row they run against, and the proposed row bound as
/// `excluded`.
struct ConflictUpdate<'a> {
    assignments: &'a [(String, Expr)],
    filter: Option<&'a Expr>,
    rowid: u64,
    cur_key_xid: u64,
    cur_xmin: u64,
    cur_row: &'a [Datum],
    proposed: &'a [Datum],
}

/// Run `DO UPDATE`'s filter and assignments against a locked conflicting row and
/// stage the resulting update. Returns the post-image (for RETURNING), or `None`
/// when the `WHERE` is not true. That row is then neither inserted nor updated
/// and produces no RETURNING row, though its row and key locks stay held, as
/// PostgreSQL's do.
///
/// Both the filter and the assignment right-hand sides evaluate against
/// [`Scope::insert_conflict`] over the stored row concatenated with the proposed
/// row, so `excluded.c` reads the proposed value and `t.c` the stored one. Every
/// column name appears under both qualifiers, which makes a bare reference
/// ambiguous (42702). That is PostgreSQL's behavior, where `DO UPDATE SET
/// v = v + 1` is an error and must be written `t.v` or `excluded.v`.
async fn apply_insert_conflict_update(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    local_indexes: &[crabka_pgcatalog::Index],
    fk: &crate::fk::StatementFkContext,
    update: &ConflictUpdate<'_>,
    writes: &mut StatementWrites,
    ops: &mut Vec<crabka_pgkv::WriteOp>,
) -> Result<Option<Vec<Datum>>, ExecError> {
    let ctx = write_ctx.eval_ctx;
    let scope = Scope::insert_conflict(table);
    let mut bindings = update.cur_row.to_vec();
    bindings.extend_from_slice(update.proposed);
    if !row_matches(update.filter, &scope, &bindings, ctx)? {
        return Ok(None);
    }
    let mut next = update.cur_row.to_vec();
    for (column, expr) in update.assignments {
        // Assignment targets are unqualified column names of the target table,
        // resolved exactly as the UPDATE arm resolves its own (42703 on miss).
        let idx = table
            .column_index(column)
            .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))?;
        let value = crate::eval::eval(expr, &scope, &bindings, ctx)?;
        next[idx] = coerce(value, table.columns[idx].ty, ctx)?;
    }
    let updated_columns = update
        .assignments
        .iter()
        .map(|(column, _)| column.clone())
        .collect::<Vec<_>>();
    let Some(next) = crate::trigger::fire_before_row(
        write_ctx.catalog_kv,
        table,
        crate::trigger::DmlEvent::Update,
        &updated_columns,
        Some(update.cur_row),
        Some(next),
        ctx,
    )?
    else {
        return Ok(None);
    };
    apply_locked_row_update(
        write_ctx,
        table,
        local_indexes,
        fk,
        &LockedRowUpdate {
            rowid: update.rowid,
            cur_key_xid: update.cur_key_xid,
            cur_xmin: update.cur_xmin,
            cur_row: update.cur_row,
            next: &next,
        },
        writes,
        ops,
    )
    .await?;
    crate::trigger::fire_after_row(
        write_ctx.catalog_kv,
        table,
        crate::trigger::DmlEvent::Update,
        &updated_columns,
        Some(update.cur_row),
        Some(&next),
        ctx,
    )?;
    Ok(Some(next))
}

fn all_committed_snapshot() -> crabka_pgmvcc::visibility::Snapshot {
    crabka_pgmvcc::visibility::Snapshot {
        xmin: 0,
        xmax: u64::MAX,
        xip: Vec::new(),
    }
}

fn local_index_entry_ops(
    table: &Table,
    indexes: &[crabka_pgcatalog::Index],
    rowid: u64,
    row: &[Datum],
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let mut ops = Vec::new();
    for index in indexes {
        for values in index_entries(table, index, row)? {
            ops.push(crabka_pgkv::WriteOp::Put {
                key: crabka_pgkv::key::secondary_index_entry_key(
                    table.id, index.id, &values, rowid,
                ),
                value: Vec::new(),
            });
        }
    }
    Ok(ops)
}

fn index_entries(
    table: &Table,
    index: &crabka_pgcatalog::Index,
    row: &[Datum],
) -> Result<Vec<Vec<Datum>>, ExecError> {
    if index.method == crabka_pgcatalog::IndexMethod::Btree {
        return indexed_values(table, index, row).map(|values| vec![values]);
    }
    if index.method != crabka_pgcatalog::IndexMethod::Gin {
        return Ok(Vec::new());
    }
    let column = table
        .column_index(&index.columns[0])
        .ok_or_else(|| ExecError::UndefinedColumn(index.columns[0].clone()))?;
    match &row[column] {
        Datum::Null => Ok(Vec::new()),
        Datum::TsVector(vector) => Ok(vector
            .0
            .iter()
            .map(|lexeme| vec![Datum::Text(lexeme.text.clone())])
            .collect()),
        got => Err(crate::func::type_error("tsvector", got)),
    }
}

fn indexed_values(
    table: &Table,
    index: &crabka_pgcatalog::Index,
    row: &[Datum],
) -> Result<Vec<Datum>, ExecError> {
    index
        .columns
        .iter()
        .map(|column| {
            let column_index = table
                .column_index(column)
                .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))?;
            Ok(row[column_index].clone())
        })
        .collect()
}

/// Result of pruning one rowid's version chain.
pub(crate) struct ChainPrune {
    /// `WriteOp`s reclaiming the chain: `Delete`s for dead version keys and
    /// their orphaned local secondary-index entries, plus (for `vacuum`)
    /// `Put`s freezing surviving sub-horizon tuple headers. Empty when
    /// nothing on the chain needs work.
    pub ops: Vec<crabka_pgkv::WriteOp>,
    /// Number of tuple versions deleted by `ops`.
    pub versions: u64,
    /// Number of secondary-index entries deleted by `ops`.
    pub index_entries: u64,
    /// Number of surviving tuple versions frozen by `ops`.
    pub frozen: u64,
}

/// Delete ops reclaiming `rowid`'s dead versions (and the local secondary-index
/// entries no surviving version still needs), judged at `horizon`.
///
/// A version is dead per [`crabka_pgmvcc::gc::version_is_dead`]: its creator
/// aborted, or a transaction that committed below `horizon` deleted/superseded
/// it. `horizon` must come from `checkpoint_garbage_horizon`, which caps it at
/// the oldest running writer xid, the lowest registered snapshot pin, and the
/// first non-terminal clog entry.
///
/// Snapshot safety: every snapshot consumer registers a `GcHorizon` pin at its
/// snapshot `xmin` for as long as the snapshot is in use (REPEATABLE READ
/// transactions pin at BEGIN until COMMIT/ROLLBACK; autocommit and READ
/// COMMITTED statements pin for the statement's duration), so a version some
/// live snapshot still sees, one whose committed deleter is in that
/// snapshot's `xip` or above its `xmax`, keeps `horizon` at or below the
/// deleter's xid and is never selected here. Un-pinned readers do not exist:
/// every statement path pins before taking its snapshot, and the pin value
/// (the ProcArray xmin at pin time) is monotonically `<=` any snapshot xmin
/// taken afterwards. `MemKv`/`FjallKv` scans additionally materialize their
/// results eagerly (`KvScan` is a `Vec`), so even a scan that raced an earlier
/// horizon computation observes an atomic before-or-after state of each prune
/// batch, never a partially deleted chain.
///
/// Lock interaction: callers must hold `rowid`'s exclusive row lock (UPDATE/
/// DELETE already do; `vacuum` takes it per row). Dead version KEYS can never
/// collide with a concurrent writer's puts. A writer only writes the newest
/// committed version's key (it stamps `xmax`) and its own new key, and neither
/// is ever dead. But the survivor computation for shared index entries must
/// not race a writer that re-adds the same indexed values for this rowid.
///
/// Engine kinds: sound everywhere, because the returned ops are folded into
/// the caller's own commit batch. On replicated engines they replicate
/// through the WAL and replay deterministically. Global 2PC writes are
/// self-protecting: an undecided enlisted xid reads as `Prepared` (which
/// [`crabka_pgmvcc::gc::version_is_dead`] never treats as dead), and global
/// xids sit numerically above every local horizon.
///
/// One rowid-chain prune request (see [`prune_rowid_chain_ops`]).
pub(crate) struct ChainPruneRequest<'a> {
    /// The row whose version chain is pruned.
    pub rowid: u64,
    /// The garbage horizon (from `checkpoint_garbage_horizon`).
    pub horizon: u64,
    /// Version-key xids this batch itself (re)writes. They are never deleted
    /// or frozen, whatever their current on-disk state.
    pub keep_xids: &'a [u64],
    /// The row this batch is writing, when any; its indexed values count as
    /// survivors so a shared index entry is never deleted out from under the
    /// incoming version.
    pub new_row: Option<&'a [Datum]>,
    /// When given (`vacuum` only), additionally rewrite every surviving
    /// version whose creator committed below this floor to `FROZEN_XID`
    /// (visible to every snapshot without a clog lookup). That is the
    /// precondition for a truncation of the clog below the horizon. The freeze
    /// is invisible to every snapshot: a registered snapshot's `xmin` is at or
    /// above the
    /// horizon, so a committed sub-horizon creator was already
    /// settled-and-committed for it.
    pub freeze_below: Option<u64>,
}

/// Rate-limited write-path reclamation telemetry accumulated between
/// emissions (process-wide; see [`log_prune_engagement`]).
#[derive(Default)]
struct PruneEngagementLog {
    /// When the previous line was emitted; `None` before the first.
    last_emitted: Option<std::time::Instant>,
    /// Row chains examined since the previous line.
    rows: u64,
    /// Dead versions selected for deletion since the previous line.
    pruned: u64,
}

static PRUNE_ENGAGEMENT: std::sync::LazyLock<std::sync::Mutex<PruneEngagementLog>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(PruneEngagementLog::default()));

/// Emit at most one `xid_chain_prune_engaged` debug line per second.
///
/// The line carries the current horizon and the chain/deletion counts
/// accumulated since the previous line. A live node that logs a low `horizon`
/// with growing `rows` and zero `pruned` shows the write path consults the
/// horizon but finds nothing dead. A non-zero `pruned` confirms end-to-end
/// reclamation.
fn log_prune_engagement(horizon: u64, pruned: u64) {
    const EMIT_EVERY: std::time::Duration = std::time::Duration::from_secs(1);
    let mut log = PRUNE_ENGAGEMENT.lock().expect("prune engagement log");
    log.rows += 1;
    log.pruned += pruned;
    let now = std::time::Instant::now();
    let due = log
        .last_emitted
        .is_none_or(|last| now.duration_since(last) >= EMIT_EVERY);
    if !due {
        return;
    }
    tracing::debug!(
        horizon,
        pruned = log.pruned,
        rows = log.rows,
        "xid_chain_prune_engaged"
    );
    log.last_emitted = Some(now);
    log.rows = 0;
    log.pruned = 0;
}

pub(crate) fn prune_rowid_chain_ops(
    kv: &dyn Kv,
    table: &Table,
    local_indexes: &[crabka_pgcatalog::Index],
    request: &ChainPruneRequest<'_>,
) -> Result<ChainPrune, ExecError> {
    let &ChainPruneRequest {
        rowid,
        horizon,
        keep_xids,
        new_row,
        freeze_below,
    } = request;
    let status = |xid| crabka_pgmvcc::clog::get(kv, xid);
    let mut dead: Vec<(Vec<u8>, Vec<Datum>)> = Vec::new();
    let mut surviving: Vec<Vec<Datum>> = Vec::new();
    let mut freeze: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for (key, value) in kv.scan_prefix(&crabka_pgkv::key::row_key(table.id, rowid))? {
        let (xmin, xmax, row) = crabka_pgmvcc::version::decode_tuple(&value)?;
        let key_xid = crabka_pgmvcc::version::xid_of_key(&key)?;
        if !keep_xids.contains(&key_xid)
            && crabka_pgmvcc::gc::version_is_dead(xmin, xmax, horizon, &status)?
        {
            dead.push((key, row));
            continue;
        }
        if let Some(floor) = freeze_below
            && !keep_xids.contains(&key_xid)
            && xmin != crabka_pgmvcc::xid::FROZEN_XID
            && xmin < floor
            && matches!(status(xmin)?, crabka_pgmvcc::clog::XidStatus::Committed)
        {
            freeze.push((key, value.clone()));
        }
        surviving.push(row);
    }
    log_prune_engagement(horizon, dead.len() as u64);
    if dead.is_empty() && freeze.is_empty() {
        return Ok(ChainPrune {
            ops: Vec::new(),
            versions: 0,
            index_entries: 0,
            frozen: 0,
        });
    }
    let mut ops: Vec<crabka_pgkv::WriteOp> = Vec::new();
    let frozen = freeze.len() as u64;
    for (key, value) in freeze {
        ops.push(crabka_pgkv::WriteOp::Put {
            key,
            value: crabka_pgmvcc::version::freeze_tuple_xmin(&value)?,
        });
    }
    let mut index_entries_pruned: u64 = 0;
    // An index entry key `(values, rowid)` is SHARED by every version of this
    // row carrying `values`: delete it only when no surviving version — nor
    // the row this batch is writing — still carries those values. Chains are
    // short (pruning keeps them O(1)), so linear survivor probes suffice.
    let mut removed: Vec<(crabka_pgcatalog::IndexId, Vec<Datum>)> = Vec::new();
    for index in local_indexes {
        let mut survivor_entries = Vec::new();
        for row in &surviving {
            survivor_entries.extend(index_entries(table, index, row)?);
        }
        if let Some(row) = new_row {
            survivor_entries.extend(index_entries(table, index, row)?);
        }
        for (_, row) in &dead {
            for values in index_entries(table, index, row)? {
                if survivor_entries.contains(&values)
                    || removed
                        .iter()
                        .any(|(id, prior)| *id == index.id && *prior == values)
                {
                    continue;
                }
                ops.push(crabka_pgkv::WriteOp::Delete {
                    key: crabka_pgkv::key::secondary_index_entry_key(
                        table.id, index.id, &values, rowid,
                    ),
                });
                removed.push((index.id, values));
                index_entries_pruned += 1;
            }
        }
    }
    let versions = dead.len() as u64;
    ops.extend(
        dead.into_iter()
            .map(|(key, _)| crabka_pgkv::WriteOp::Delete { key }),
    );
    Ok(ChainPrune {
        ops,
        versions,
        index_entries: index_entries_pruned,
        frozen,
    })
}

fn lookup_local_index_equal(
    mvcc: &MvccReadContext<'_>,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    values: &[Datum],
) -> Result<Vec<ScannedRow>, ExecError> {
    let prefix = crabka_pgkv::key::secondary_index_entry_prefix(table.id, index.id, values);
    let entries = mvcc.kv.scan_prefix(&prefix)?;
    let mut rowids = BTreeSet::new();
    for (key, _) in entries {
        rowids.insert(crabka_pgkv::key::secondary_index_rowid_of(
            table.id, index.id, &key,
        )?);
    }

    let mut exact = Vec::new();
    for candidate in visible_rows_for_rowids(mvcc, table, rowids)? {
        if indexed_values(table, index, &candidate.row)? == values {
            exact.push(candidate);
        }
    }
    Ok(exact)
}

fn lookup_local_gin(
    mvcc: &MvccReadContext<'_>,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    query: &crabka_pgtypes::TsQuery,
) -> Result<Option<Vec<ScannedRow>>, ExecError> {
    let Some(rowids) = gin_candidate_rowids(mvcc.kv, table, index, query)? else {
        return Ok(None);
    };
    let column = table
        .column_index(&index.columns[0])
        .ok_or_else(|| ExecError::UndefinedColumn(index.columns[0].clone()))?;
    Ok(Some(
        visible_rows_for_rowids(mvcc, table, rowids)?
            .into_iter()
            .filter(|candidate| {
                matches!(&candidate.row[column], Datum::TsVector(vector) if vector.matches(query))
            })
            .collect(),
    ))
}

fn gin_candidate_rowids(
    kv: &dyn Kv,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    query: &crabka_pgtypes::TsQuery,
) -> Result<Option<BTreeSet<u64>>, ExecError> {
    use crabka_pgtypes::TsQuery;

    match query {
        TsQuery::Empty => Ok(Some(BTreeSet::new())),
        TsQuery::Term(term) if term.prefix => Ok(None),
        TsQuery::Term(term) => {
            let prefix = crabka_pgkv::key::secondary_index_entry_prefix(
                table.id,
                index.id,
                &[Datum::Text(term.text.clone())],
            );
            let mut rowids = BTreeSet::new();
            for (key, _) in kv.scan_prefix(&prefix)? {
                rowids.insert(crabka_pgkv::key::secondary_index_rowid_of(
                    table.id, index.id, &key,
                )?);
            }
            Ok(Some(rowids))
        }
        TsQuery::Not(_) => Ok(None),
        TsQuery::And(left, right) | TsQuery::Phrase(left, right, _) => {
            let left = gin_candidate_rowids(kv, table, index, left)?;
            let right = gin_candidate_rowids(kv, table, index, right)?;
            Ok(match (left, right) {
                (Some(left), Some(right)) => Some(&left & &right),
                (Some(candidates), None) | (None, Some(candidates)) => Some(candidates),
                (None, None) => None,
            })
        }
        TsQuery::Or(left, right) => {
            let left = gin_candidate_rowids(kv, table, index, left)?;
            let right = gin_candidate_rowids(kv, table, index, right)?;
            Ok(match (left, right) {
                (Some(left), Some(right)) => Some(&left | &right),
                _ => None,
            })
        }
    }
}

fn visible_rows_for_rowids(
    mvcc: &MvccReadContext<'_>,
    table: &Table,
    rowids: BTreeSet<u64>,
) -> Result<Vec<ScannedRow>, ExecError> {
    let mut rows = Vec::new();
    for rowid in rowids {
        let row_prefix = crabka_pgkv::key::row_key(table.id, rowid);
        let versions = mvcc
            .kv
            .scan_prefix(&row_prefix)?
            .iter()
            .map(|(_, value)| {
                let (xmin, xmax, row) = crabka_pgmvcc::version::decode_tuple(value)?;
                Ok((xmin, xmax, row))
            })
            .collect::<Result<Vec<_>, crabka_pgkv::KvError>>()?;
        let Some((xmin, row)) = find_visible_one(
            mvcc.kv,
            mvcc.global,
            mvcc.global_snapshot,
            mvcc.snapshot,
            mvcc.own,
            &versions,
        )?
        else {
            continue;
        };
        rows.push(ScannedRow { rowid, xmin, row });
    }
    Ok(rows)
}

/// Choose the index probe for an UPDATE/DELETE filter: a top-level
/// `column = literal` conjunct matching a single-column local index. Returns
/// `None`, which means a full scan, for sharded tables, for filters outside the
/// pushdown subset, and when no index matches. It reuses the SELECT path's
/// extraction, so only exact-type literals on supported column types qualify.
fn choose_write_index_probe(
    catalog_kv: &dyn Kv,
    table: &Table,
    filter: Option<&Expr>,
) -> Result<Option<(crabka_pgcatalog::Index, Datum)>, ExecError> {
    if table.sharded {
        return Ok(None);
    }
    let predicate = crate::plan_dist::predicate_for_filter(table, filter);
    choose_local_index_equality(catalog_kv, table, &predicate)
}

/// Candidate `(rowid, xmin, row)` source for UPDATE/DELETE: probe a matching
/// local index instead of scanning the whole table when the filter pins an
/// indexed column to a literal, else fall back to `scan_live`. Both paths read
/// under the statement's snapshot/gsnap/own visibility and return rows sorted
/// by rowid; the caller still applies the FULL residual filter and the
/// under-lock EvalPlanQual re-check to every candidate, so the affected rows,
/// RETURNING output, and lock order are identical to the full scan.
fn write_candidate_rows(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    filter: Option<&Expr>,
) -> Result<Vec<(u64, u64, Vec<Datum>)>, ExecError> {
    if let Some((index, value)) = choose_write_index_probe(write_ctx.catalog_kv, table, filter)? {
        let rows = lookup_local_index_equal(&write_ctx.mvcc_read(), table, &index, &[value])?;
        return Ok(rows
            .into_iter()
            .map(|row| (row.rowid, row.xmin, row.row))
            .collect());
    }
    scan_live(
        write_ctx.kv,
        write_ctx.global,
        write_ctx.global_snapshot,
        write_ctx.snapshot,
        Some(write_ctx.xid),
        table,
    )
}

/// Build timestamp-transaction writes for sharded-table autocommit DML.
pub(crate) fn execute_timestamp_write(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    seq: &crate::seq::SequenceManager,
    stmt: &Statement,
    ctx: &crate::clock::EvalCtx,
) -> Result<TimestampWritePlan, ExecError> {
    let resolution = ctx.resolution();
    match stmt {
        Statement::Insert { returning, .. }
        | Statement::Update { returning, .. }
        | Statement::Delete { returning, .. }
            if returning.is_some() =>
        {
            return Err(ExecError::Unsupported(
                "RETURNING on sharded timestamp writes is not supported".into(),
            ));
        }
        _ => {}
    }
    // ON CONFLICT arbitration probes and locks a unique key on the local range;
    // a sharded table's unique keys live on other ranges, so the conflict can
    // neither be seen nor locked here. Refused permanently, like RETURNING.
    if let Statement::Insert {
        on_conflict: Some(_),
        ..
    } = stmt
    {
        return Err(ExecError::Unsupported(
            "ON CONFLICT on sharded timestamp writes is not supported".into(),
        ));
    }

    let table_name = match stmt {
        Statement::Insert { table, .. }
        | Statement::Update { table, .. }
        | Statement::Delete { table, .. } => table,
        _ => {
            return Err(ExecError::Unsupported(
                "not a timestamp write statement".into(),
            ));
        }
    };
    let table_name = &resolve_relation(
        catalog_kv,
        resolution,
        table_name,
        SchemaDisposition::Reference,
    )?;
    let table = crabka_pgcatalog::get_table(catalog_kv, table_name)?;
    if !table_uses_global_visibility(&table) {
        return Err(ExecError::Unsupported(
            "timestamp writes require a sharded table".into(),
        ));
    }
    let indexes = crabka_pgcatalog::list_table_indexes(catalog_kv, &table.name)?;
    let global_indexes: Vec<_> = indexes
        .iter()
        .filter(|index| index.placement == crabka_pgcatalog::IndexPlacement::Global)
        .collect();
    if global_indexes.iter().any(|index| index.unique) {
        return Err(ExecError::Unsupported(
            "unique global indexes are not supported until global enforcement exists".into(),
        ));
    }
    let local_indexes_present = indexes
        .iter()
        .any(|index| index.placement == crabka_pgcatalog::IndexPlacement::Local);
    if local_indexes_present {
        return Err(ExecError::Unsupported(
            "local index maintenance for sharded timestamp writes is blocked on G-6".into(),
        ));
    }

    match stmt {
        Statement::Insert {
            columns, source, ..
        } => {
            // A sharded write's rows must be known without a read: the feeding
            // query forms have no timestamp-domain plan yet.
            let crabka_pgparser::ast::InsertSource::Values(rows) = source else {
                return Err(ExecError::Unsupported(
                    "INSERT ... SELECT / DEFAULT VALUES on sharded tables is not supported".into(),
                ));
            };
            crate::trigger::fire_statement(
                catalog_kv,
                &table,
                crate::trigger::DmlEvent::Insert,
                crabka_pgcatalog::trigger::TriggerTiming::Before,
                &[],
                ctx,
            )?;
            let plan = execute_timestamp_insert(
                catalog_kv,
                kv,
                seq,
                &table,
                &global_indexes,
                columns,
                rows,
                ctx,
            )?;
            crate::trigger::fire_statement(
                catalog_kv,
                &table,
                crate::trigger::DmlEvent::Insert,
                crabka_pgcatalog::trigger::TriggerTiming::After,
                &[],
                ctx,
            )?;
            Ok(plan)
        }
        Statement::Update {
            assignments,
            from,
            filter,
            ..
        } => {
            if !from.is_empty() {
                return Err(ExecError::Unsupported(
                    "UPDATE ... FROM on sharded tables is not supported".into(),
                ));
            }
            let updated = assignments
                .iter()
                .flat_map(|assignment| assignment.targets.iter().cloned())
                .collect::<Vec<_>>();
            crate::trigger::fire_statement(
                catalog_kv,
                &table,
                crate::trigger::DmlEvent::Update,
                crabka_pgcatalog::trigger::TriggerTiming::Before,
                &updated,
                ctx,
            )?;
            let plan = execute_timestamp_update(
                catalog_kv,
                kv,
                &table,
                &global_indexes,
                assignments,
                filter.as_ref(),
                ctx,
            )?;
            crate::trigger::fire_statement(
                catalog_kv,
                &table,
                crate::trigger::DmlEvent::Update,
                crabka_pgcatalog::trigger::TriggerTiming::After,
                &updated,
                ctx,
            )?;
            Ok(plan)
        }
        Statement::Delete { using, filter, .. } => {
            if !using.is_empty() {
                return Err(ExecError::Unsupported(
                    "DELETE ... USING on sharded tables is not supported".into(),
                ));
            }
            crate::trigger::fire_statement(
                catalog_kv,
                &table,
                crate::trigger::DmlEvent::Delete,
                crabka_pgcatalog::trigger::TriggerTiming::Before,
                &[],
                ctx,
            )?;
            let plan = execute_timestamp_delete(
                catalog_kv,
                kv,
                &table,
                &global_indexes,
                filter.as_ref(),
                ctx,
            )?;
            crate::trigger::fire_statement(
                catalog_kv,
                &table,
                crate::trigger::DmlEvent::Delete,
                crabka_pgcatalog::trigger::TriggerTiming::After,
                &[],
                ctx,
            )?;
            Ok(plan)
        }
        _ => Err(ExecError::Unsupported(
            "this statement is not supported on sharded tables".into(),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_timestamp_insert(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    seq: &crate::seq::SequenceManager,
    table: &Table,
    global_indexes: &[&crabka_pgcatalog::Index],
    columns: &Option<Vec<String>>,
    rows: &[Vec<Expr>],
    ctx: &crate::clock::EvalCtx,
) -> Result<TimestampWritePlan, ExecError> {
    if rows.is_empty() {
        return Ok(TimestampWritePlan {
            result: command("INSERT 0 0"),
            writes: Vec::new(),
            commit_ops: Vec::new(),
        });
    }
    let width = rows.first().map_or(0, Vec::len);
    if rows.iter().any(|row| row.len() != width) {
        return Err(ExecError::ValuesColumnCount);
    }
    let target_idx = resolve_insert_targets(table, columns, width)?;
    let proposed_rows = rows.len() as u64;
    let (start, seq_op) = seq.alloc(kv, table.id, proposed_rows)?;
    let mut writes = Vec::with_capacity(rows.len());
    for (rowid, row_exprs) in (start..).zip(rows.iter()) {
        let full = build_insert_row(table, &target_idx, row_exprs, ctx)?;
        let Some(full) = crate::trigger::fire_before_row(
            catalog_kv,
            table,
            crate::trigger::DmlEvent::Insert,
            &[],
            None,
            Some(full),
            ctx,
        )?
        else {
            continue;
        };
        let bucket = hash_bucket_for_row(table, &full)?;
        crate::trigger::fire_after_row(
            catalog_kv,
            table,
            crate::trigger::DmlEvent::Insert,
            &[],
            None,
            Some(&full),
            ctx,
        )?;
        writes.push(TimestampWrite {
            table_id: table.id,
            bucket,
            rowid,
            global_index_intents: global_index_intents_for_row(
                table,
                global_indexes,
                rowid,
                &full,
            )?,
            row: full,
            delete: false,
        });
    }
    let n_rows = writes.len();
    Ok(TimestampWritePlan {
        result: command(&format!("INSERT 0 {n_rows}")),
        writes,
        commit_ops: seq_op.into_iter().collect(),
    })
}

fn execute_timestamp_update(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    table: &Table,
    global_indexes: &[&crabka_pgcatalog::Index],
    assignments: &[crabka_pgparser::ast::Assignment],
    filter: Option<&Expr>,
    ctx: &crate::clock::EvalCtx,
) -> Result<TimestampWritePlan, ExecError> {
    let scope = Scope::single(table, &table.name.name);
    // The sharded write path evaluates assignments without a read, so only the
    // single-expression form is available here.
    let targets = assignments
        .iter()
        .map(
            |assignment| match (&assignment.targets[..], &assignment.value) {
                ([column], crabka_pgparser::ast::AssignmentValue::Expr(expr)) => table
                    .column_index(column)
                    .map(|index| (index, expr))
                    .ok_or_else(|| ExecError::UndefinedColumn(column.clone())),
                _ => Err(ExecError::Unsupported(
                    "multi-column SET on sharded tables is not supported".into(),
                )),
            },
        )
        .collect::<Result<Vec<_>, _>>()?;
    let rows = scan_ts_live_interval(kv, kv, table, ReadTimestamp::MAX, None, RowInterval::ALL)?;
    let mut writes = Vec::new();
    for ScannedRow { rowid, row, .. } in rows {
        if !row_matches(filter, &scope, &row, ctx)? {
            continue;
        }
        let mut next = row.clone();
        for (index, expr) in &targets {
            let value = crate::eval::eval(expr, &scope, &row, ctx)?;
            next[*index] = coerce(value, table.columns[*index].ty, ctx)?;
        }
        finish_written_row(table, &mut next, ctx)?;
        let updated = assignments
            .iter()
            .flat_map(|assignment| assignment.targets.iter().cloned())
            .collect::<Vec<_>>();
        let Some(next) = crate::trigger::fire_before_row(
            catalog_kv,
            table,
            crate::trigger::DmlEvent::Update,
            &updated,
            Some(&row),
            Some(next),
            ctx,
        )?
        else {
            continue;
        };
        let old_bucket = hash_bucket_for_row(table, &row)?;
        let bucket = hash_bucket_for_row(table, &next)?;
        let global_index_intents =
            global_index_update_intents_for_row(table, global_indexes, rowid, &row, &next)?;
        if old_bucket != bucket {
            writes.push(TimestampWrite {
                table_id: table.id,
                bucket: old_bucket,
                rowid,
                global_index_intents: Vec::new(),
                row: row.clone(),
                delete: true,
            });
        }
        crate::trigger::fire_after_row(
            catalog_kv,
            table,
            crate::trigger::DmlEvent::Update,
            &updated,
            Some(&row),
            Some(&next),
            ctx,
        )?;
        writes.push(TimestampWrite {
            table_id: table.id,
            bucket,
            rowid,
            global_index_intents,
            row: next,
            delete: false,
        });
    }
    Ok(TimestampWritePlan {
        result: command(&format!(
            "UPDATE {}",
            writes.iter().filter(|write| !write.delete).count()
        )),
        writes,
        commit_ops: Vec::new(),
    })
}

fn execute_timestamp_delete(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    table: &Table,
    global_indexes: &[&crabka_pgcatalog::Index],
    filter: Option<&Expr>,
    ctx: &crate::clock::EvalCtx,
) -> Result<TimestampWritePlan, ExecError> {
    let scope = Scope::single(table, &table.name.name);
    let rows = scan_ts_live_interval(kv, kv, table, ReadTimestamp::MAX, None, RowInterval::ALL)?;
    let mut writes = Vec::new();
    for ScannedRow { rowid, row, .. } in rows {
        if !row_matches(filter, &scope, &row, ctx)? {
            continue;
        }
        if crate::trigger::fire_before_row(
            catalog_kv,
            table,
            crate::trigger::DmlEvent::Delete,
            &[],
            Some(&row),
            None,
            ctx,
        )?
        .is_none()
        {
            continue;
        }
        let global_index_intents =
            global_index_delete_intents_for_row(table, global_indexes, rowid, &row)?;
        let bucket = hash_bucket_for_row(table, &row)?;
        crate::trigger::fire_after_row(
            catalog_kv,
            table,
            crate::trigger::DmlEvent::Delete,
            &[],
            Some(&row),
            None,
            ctx,
        )?;
        writes.push(TimestampWrite {
            table_id: table.id,
            bucket,
            rowid,
            row,
            delete: true,
            global_index_intents,
        });
    }
    Ok(TimestampWritePlan {
        result: command(&format!("DELETE {}", writes.len())),
        writes,
        commit_ops: Vec::new(),
    })
}

fn hash_bucket_for_row(table: &Table, row: &[Datum]) -> Result<Option<u32>, ExecError> {
    let Some(crabka_pgcatalog::ShardingStrategy::Hash(hash)) = &table.sharding else {
        return Ok(None);
    };
    // A row's bucket is the hash of the one shard column, which is the arity
    // `SHARDED BY HASH` accepts. A wider catalog entry — attachable through the
    // catalog API, which does not gate arity — has no row encoding here: the
    // gateway derives a statement's route from every hash column's bytes, so a
    // row placed under the hash of the first column alone would sit in a range
    // that routing never visits. Refuse the write instead of misplacing it.
    let [column] = hash.columns.as_slice() else {
        return Err(ExecError::Unsupported(
            "hash sharding requires exactly one hash column".into(),
        ));
    };
    let index = table
        .column_index(column)
        .ok_or_else(|| ExecError::Unsupported("hash sharding catalog column mismatch".into()))?;
    let bytes = match &row[index] {
        Datum::Int4(value) => value.to_be_bytes().to_vec(),
        Datum::Int8(value) => value.to_be_bytes().to_vec(),
        Datum::Text(value) => value.as_bytes().to_vec(),
        Datum::Bytea(value) => value.clone(),
        // A `regclass` hashes on its oid: the name it renders is derived from
        // the catalog, so only the oid is stable enough to place a row.
        Datum::Regclass(value) => value.oid.to_be_bytes().to_vec(),
        Datum::Null => Vec::new(),
        _ => {
            return Err(ExecError::Unsupported(
                "hash shard key type is not supported".into(),
            ));
        }
    };
    crabka_pgkv::key::hash_bucket(&bytes, hash.buckets)
        .map(Some)
        .ok_or_else(|| ExecError::Unsupported("invalid hash sharding bucket count".into()))
}

fn global_index_intents_for_row(
    table: &Table,
    indexes: &[&crabka_pgcatalog::Index],
    rowid: u64,
    row: &[Datum],
) -> Result<Vec<crate::timestamp_txn::GlobalIndexIntent>, ExecError> {
    indexes
        .iter()
        .map(|index| {
            let indexed_values = index
                .columns
                .iter()
                .map(|column| {
                    let column_index = table
                        .column_index(column)
                        .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))?;
                    Ok(row[column_index].clone())
                })
                .collect::<Result<Vec<_>, ExecError>>()?;
            Ok(crate::timestamp_txn::GlobalIndexIntent {
                index_id: index.id,
                indexed_values,
                base_table_id: table.id,
                base_rowid: rowid,
                unique: index.unique,
                delete: false,
            })
        })
        .collect()
}

fn global_index_update_intents_for_row(
    table: &Table,
    indexes: &[&crabka_pgcatalog::Index],
    rowid: u64,
    old_row: &[Datum],
    new_row: &[Datum],
) -> Result<Vec<crate::timestamp_txn::GlobalIndexIntent>, ExecError> {
    let mut intents = Vec::new();
    for index in indexes {
        let old_values = indexed_values(table, index, old_row)?;
        let new_values = indexed_values(table, index, new_row)?;
        if old_values == new_values {
            continue;
        }
        intents.push(crate::timestamp_txn::GlobalIndexIntent {
            index_id: index.id,
            indexed_values: old_values,
            base_table_id: table.id,
            base_rowid: rowid,
            unique: index.unique,
            delete: true,
        });
        intents.push(crate::timestamp_txn::GlobalIndexIntent {
            index_id: index.id,
            indexed_values: new_values,
            base_table_id: table.id,
            base_rowid: rowid,
            unique: index.unique,
            delete: false,
        });
    }
    Ok(intents)
}

fn global_index_delete_intents_for_row(
    table: &Table,
    indexes: &[&crabka_pgcatalog::Index],
    rowid: u64,
    row: &[Datum],
) -> Result<Vec<crate::timestamp_txn::GlobalIndexIntent>, ExecError> {
    indexes
        .iter()
        .map(|index| {
            Ok(crate::timestamp_txn::GlobalIndexIntent {
                index_id: index.id,
                indexed_values: indexed_values(table, index, row)?,
                base_table_id: table.id,
                base_rowid: rowid,
                unique: index.unique,
                delete: true,
            })
        })
        .collect()
}

/// Was `xid` settled (committed or aborted) before `snapshot` was taken? True
/// iff `xid` was neither still running at, nor started after, the snapshot.
/// This mirrors the negation of `Snapshot::is_running`.
fn snapshot_can_see(snapshot: &crabka_pgmvcc::visibility::Snapshot, xid: u64) -> bool {
    xid < snapshot.xmax && !snapshot.xip.contains(&xid)
}

/// The global-aware clog resolver handed to `satisfies_mvcc`. Given this range's
/// local xid `Li`, reads this range's clog (`local`); a terminal status is
/// returned unchanged (today's single-range behavior). A `Prepared(Li -> g)`
/// marker is deref'd to range 0's global clog (`global`): if `g` is still
/// in-doubt as of the reader's global snapshot (`gsnap`) it reports `InProgress`
/// (the cross-range row is invisible until the global commit decision); once `g`
/// is settled relative to `gsnap`, range 0's global-clog status for `g` is the
/// answer. So both ranges' Prepared rows flip visible together at the single
/// `Committed(g)` instant.
///
/// For a single-range (non-GTM) engine the caller passes `global = local` and
/// `gsnap = NO_GLOBAL_SNAPSHOT()`; no `Prepared` tuple ever exists there, so the
/// `Prepared` arm is unreachable and behavior is byte-for-byte unchanged.
pub(crate) fn global_status<'a>(
    local: &'a dyn crabka_pgkv::Kv,
    global: &'a dyn crabka_pgkv::Kv,
    gsnap: &'a crabka_pgmvcc::visibility::Snapshot,
) -> impl Fn(u64) -> Result<crabka_pgmvcc::clog::XidStatus, crabka_pgkv::KvError> + 'a {
    use crabka_pgmvcc::clog::XidStatus;
    move |xid| match crabka_pgmvcc::clog::get(local, xid)? {
        XidStatus::Prepared(g) => {
            if g >= gsnap.xmax || gsnap.xip.binary_search(&g).is_ok() {
                Ok(XidStatus::InProgress) // global txn in-doubt as of my global snapshot
            } else {
                Ok(crabka_pgmvcc::clog::get(global, g)?) // settled: range 0's global decision
            }
        }
        other => Ok(other),
    }
}

/// Find the single version of `rowid` visible to `snap` (with own-xid
/// read-your-writes) among already-decoded `versions`. Mirrors `scan_live`'s
/// per-version `satisfies_mvcc` check, but over one rowid's versions.
///
/// Returns the greatest-xmin live version. The MVCC at-most-one-live invariant
/// means at most one version of a rowid is live under any one snapshot, so the
/// selection is unambiguous; choosing the max explicitly (rather than relying on
/// ascending scan order) makes it order-independent and is debug-asserted to see
/// at most one live version.
fn find_visible_one(
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &crabka_pgmvcc::visibility::Snapshot,
    snap: &crabka_pgmvcc::visibility::Snapshot,
    own: Option<u64>,
    versions: &[(u64, u64, Vec<crabka_pgtypes::Datum>)],
) -> Result<Option<(u64, Vec<crabka_pgtypes::Datum>)>, ExecError> {
    let mut visible: Option<(u64, Vec<crabka_pgtypes::Datum>)> = None;
    let mut live_count: usize = 0;
    for (xmin, xmax, row) in versions {
        if crabka_pgmvcc::visibility::satisfies_mvcc(
            *xmin,
            *xmax,
            snap,
            own,
            global_status(kv, global, gsnap),
        )? {
            live_count += 1;
            // Keep the greatest-xmin live version EXPLICITLY. The MVCC at-most-one-live
            // invariant means there is normally exactly one; selecting the max removes the
            // hidden dependence on ascending scan order, so a future scan-order change can
            // never silently return a stale shadow (e.g. an aborted re-attempt's
            // `Prepared(Li_old -> g)` tuple that resolves invisible anyway).
            // NB: `is_none_or`, NOT `map_or(true, …)` — the latter trips
            // `clippy::unnecessary_map_or` under the workspace's `-D warnings` gate.
            if visible.as_ref().is_none_or(|(cur, _)| *xmin > *cur) {
                visible = Some((*xmin, row.clone()));
            }
        }
    }
    debug_assert!(
        live_count <= 1,
        "find_visible_one: {live_count} live versions for one rowid under one snapshot \
         — MVCC at-most-one-live invariant violated"
    );
    Ok(visible)
}

/// One decoded version of a row chain, keyed by the xid suffix of its
/// PHYSICAL version key. The key xid normally equals the header `xmin`, but a
/// frozen tuple keeps its original key while its header reads `FROZEN_XID`.
/// Writers must stamp `xmax` on the physical key, never one reconstructed
/// from the header.
struct ChainVersion {
    key_xid: u64,
    xmin: u64,
    xmax: u64,
    row: Vec<crabka_pgtypes::Datum>,
}

/// The version of `rowid` a write should operate on, as
/// `(key_xid, xmin, row)`.
///
/// After locking the row, re-read its current versions. Returns the version to
/// operate on, or None to skip. Under REPEATABLE READ, a row changed by a txn
/// that committed after our snapshot is a serialization failure (40001). Under
/// READ COMMITTED, re-find the latest live version (a fresh snapshot).
fn eval_plan_qual(
    mutation: &MutationContext<'_>,
    table: &crabka_pgcatalog::Table,
    rowid: u64,
) -> Result<Option<(u64, u64, Vec<crabka_pgtypes::Datum>)>, ExecError> {
    let kv = mutation.kv;
    let global = mutation.global;
    let procarray = mutation.procarray;
    let snapshot = mutation.snapshot;
    let xid = mutation.xid;
    let repeatable_read = mutation.repeatable_read;
    // Re-scan just this rowid's versions from disk.
    let prefix = crabka_pgkv::key::row_key(table.id, rowid);
    let scanned = kv.scan_prefix(&prefix)?;
    let mut versions: Vec<ChainVersion> = Vec::with_capacity(scanned.len());
    for (k, v) in &scanned {
        let (xmn, xmx, row) = crabka_pgmvcc::version::decode_tuple(v)?;
        versions.push(ChainVersion {
            key_xid: crabka_pgmvcc::version::xid_of_key(k)?,
            xmin: xmn,
            xmax: xmx,
            row,
        });
    }
    // Resolve this row's `Prepared(Li -> g)` markers against a SETTLED global view
    // — range 0's global clog read directly — NOT the statement's pre-lock global
    // snapshot (`gsnap`). We hold this row's lock, and a cross-range participant
    // releases a row's lock only AFTER its global decision is durable
    // (commit_release/abort_release run post-decision). So every global txn `g`
    // with a `Prepared` marker on THIS row's versions has already settled in
    // range 0's global clog; a still-in-doubt `g` could not have left a marker
    // here (it would still hold this lock, so we could not have acquired it).
    // Reading the global clog directly under the lock is therefore exact — and is
    // the read-committed-under-lock analogue of how the LOCAL clog is read
    // directly. Using `gsnap` would be stale: a `g` that committed while we were
    // blocked on the lock still appears in-doubt in `gsnap.xip`, hiding its just-
    // committed supersede and losing the update across the 2PC boundary. A settled
    // Snapshot (xmin 0, xmax MAX, empty xip) drives `global_status`'s in-doubt gate
    // (`g >= xmax || xip.contains(g)`) always false, so it reads `clog::get` for g.
    // The LOCAL `snapshot`/`fresh` handling below is unchanged — it is about local
    // creation ordering and is already correct.
    let settled_global = crabka_pgmvcc::visibility::Snapshot {
        xmin: 0,
        xmax: u64::MAX,
        xip: Vec::new(),
    };
    // Is the row's latest committed version deleted/superseded by a transaction
    // NOT visible to our txn snapshot (committed AFTER it), other than ourselves?
    // The resolver derefs a Prepared(xmx -> g) deleter to range 0's global
    // decision so a cross-range supersede is detected exactly when it commits.
    let resolve = global_status(kv, global, &settled_global);
    let changed_since_snapshot = versions.iter().any(|version| {
        version.xmax != crabka_pgmvcc::xid::INVALID_XID
            && version.xmax != xid
            && matches!(
                resolve(version.xmax),
                Ok(crabka_pgmvcc::clog::XidStatus::Committed)
            )
            && !snapshot_can_see(snapshot, version.xmax)
    });
    if changed_since_snapshot {
        if repeatable_read {
            return Err(ExecError::SerializationFailure);
        }
        // READ COMMITTED: re-find the latest live version under a FRESH snapshot.
        let fresh = procarray.snapshot();
        return find_visible_one_keyed(kv, global, &settled_global, &fresh, Some(xid), &versions);
    }
    // No concurrent committed change: find the version visible to our snapshot.
    find_visible_one_keyed(kv, global, &settled_global, snapshot, Some(xid), &versions)
}

/// [`find_visible_one`] over [`ChainVersion`]s, additionally returning the
/// visible version's PHYSICAL key xid so callers stamp the key that actually
/// exists (a frozen tuple's header `xmin` no longer names its key).
fn find_visible_one_keyed(
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &crabka_pgmvcc::visibility::Snapshot,
    snap: &crabka_pgmvcc::visibility::Snapshot,
    own: Option<u64>,
    versions: &[ChainVersion],
) -> Result<Option<(u64, u64, Vec<crabka_pgtypes::Datum>)>, ExecError> {
    let mut visible: Option<(u64, u64, Vec<crabka_pgtypes::Datum>)> = None;
    let mut live_count: usize = 0;
    for version in versions {
        if crabka_pgmvcc::visibility::satisfies_mvcc(
            version.xmin,
            version.xmax,
            snap,
            own,
            global_status(kv, global, gsnap),
        )? {
            live_count += 1;
            // Keep the greatest live version — by header xmin, then by key xid
            // (frozen tuples all read xmin == FROZEN_XID; their key xids still
            // order them). See find_visible_one for the invariant discussion.
            if visible.as_ref().is_none_or(|(cur_key, cur_xmin, _)| {
                (version.xmin, version.key_xid) > (*cur_xmin, *cur_key)
            }) {
                visible = Some((version.key_xid, version.xmin, version.row.clone()));
            }
        }
    }
    debug_assert!(
        live_count <= 1,
        "find_visible_one_keyed: {live_count} live versions for one rowid under one snapshot \
         — MVCC at-most-one-live invariant violated"
    );
    Ok(visible)
}

/// Coerce an evaluated value into a target column type (assignment context).
///
/// `ctx` supplies the session zone for any temporal numeric conversion.
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
        return Ok(crabka_pgtypes::cast::cast(&value, target, &ctx.time_zone)?);
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
        (Datum::Text(s), ColumnType::Varchar(limit)) => Datum::Text(
            crabka_pgtypes::string::apply_varchar_typmod(&s, limit, Coercion::Assignment)?,
        ),
        (Datum::Text(s), ColumnType::Char(limit)) => Datum::Text(
            crabka_pgtypes::string::apply_char_typmod(&s, limit, Coercion::Assignment)?,
        ),
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
        (value @ (Datum::Text(_) | Datum::Jsonb(_)), ty @ ColumnType::Jsonb)
        | (value @ (Datum::Text(_) | Datum::Array(_)), ty @ ColumnType::Array(_)) => {
            // `cast_assign`, because this is a store: an over-long element of a
            // `varchar(n)[]` column is 22001, not a silent truncation.
            crabka_pgtypes::cast::cast_assign(&value, ty, &ctx.time_zone)?
        }
        (v, target) => {
            // Assignment-context implicit casts — PostgreSQL's pg_cast
            // castcontext 'i'/'a' pairs (crabka subset): notably
            // timestamptz ↔ timestamp and date → timestamp/timestamptz, all
            // rotated through the session time zone. Anything outside that
            // strict subset (e.g. int ↔ text) keeps erroring with 42804.
            if let Some(from) = v.column_type()
                && crabka_pgtypes::cast::assignment_cast_allowed(from, target)
            {
                return Ok(crabka_pgtypes::cast::cast(&v, target, &ctx.time_zone)?);
            }
            return Err(ExecError::TypeMismatch(format!(
                "column is of type {} but expression is of type {}",
                target.name(),
                v.column_type().map(|t| t.name()).unwrap_or("unknown"),
            )));
        }
    })
}

/// Scan a table's visible rows under `snapshot` (and the caller's own xid for
/// read-your-writes). Returns `(rowid, xmin, row)` for the one visible version
/// of each live row, sorted by rowid.
pub(crate) fn scan_live(
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &crabka_pgmvcc::visibility::Snapshot,
    snapshot: &crabka_pgmvcc::visibility::Snapshot,
    own: Option<u64>,
    table: &crabka_pgcatalog::Table,
) -> Result<Vec<(u64, u64, Vec<crabka_pgtypes::Datum>)>, ExecError> {
    scan_live_interval(kv, global, gsnap, snapshot, own, table, RowInterval::ALL).map(|rows| {
        rows.into_iter()
            .map(|row| (row.rowid, row.xmin, row.row))
            .collect()
    })
}

/// Scan visible rows within a rowid interval under `snapshot`, sorted by rowid.
pub(crate) fn scan_live_interval(
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &crabka_pgmvcc::visibility::Snapshot,
    snapshot: &crabka_pgmvcc::visibility::Snapshot,
    own: Option<u64>,
    table: &crabka_pgcatalog::Table,
    interval: RowInterval,
) -> Result<Vec<ScannedRow>, ExecError> {
    let scanned = scan_table_for_catalog_interval(kv, table, interval)?;
    let mut out: Vec<ScannedRow> = Vec::new();
    let mut i = 0;
    while i < scanned.len() {
        crate::session::check_query_canceled()?;
        let prefix = crabka_pgmvcc::version::row_prefix_of(&scanned[i].0)?.to_vec();
        let rowid = physical_rowid(table, &prefix)?;
        if !interval.contains(rowid) {
            while i < scanned.len()
                && crabka_pgmvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice()
            {
                i += 1;
            }
            continue;
        }
        let mut visible: Option<(u64, Vec<crabka_pgtypes::Datum>)> = None;
        let mut live_count: usize = 0;
        while i < scanned.len()
            && crabka_pgmvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice()
        {
            let (xmin, xmax, row) = crabka_pgmvcc::version::decode_tuple(&scanned[i].1)?;
            if crabka_pgmvcc::visibility::satisfies_mvcc(
                xmin,
                xmax,
                snapshot,
                own,
                global_status(kv, global, gsnap),
            )? {
                live_count += 1;
                // `is_none_or`, NOT `map_or(true, …)` — see find_visible_one above.
                if visible.as_ref().is_none_or(|(cur, _)| xmin > *cur) {
                    visible = Some((xmin, row));
                }
            }
            i += 1;
        }
        debug_assert!(
            live_count <= 1,
            "scan_live: {live_count} live versions for rowid {rowid} under one snapshot \
             — MVCC at-most-one-live invariant violated"
        );
        if let Some((xmin, row)) = visible {
            out.push(ScannedRow { rowid, xmin, row });
        }
    }
    out.sort_by_key(|row| row.rowid);
    Ok(out)
}

/// Scan timestamp-transaction versions within a rowid interval under `read_ts`.
pub(crate) fn scan_ts_live_interval(
    kv: &dyn Kv,
    primary_kv: &dyn Kv,
    table: &crabka_pgcatalog::Table,
    read_ts: ReadTimestamp,
    own_start_ts: Option<TimestampTransactionId>,
    interval: RowInterval,
) -> Result<Vec<ScannedRow>, ExecError> {
    let scanned = scan_table_for_catalog_interval(kv, table, interval)?;
    let mut out = Vec::new();
    let mut i = 0;
    while i < scanned.len() {
        crate::session::check_query_canceled()?;
        let prefix = crabka_pgmvcc::version::row_prefix_of(&scanned[i].0)?.to_vec();
        let rowid = physical_rowid(table, &prefix)?;
        let bucket = physical_bucket(table, &prefix)?;
        if !interval.contains(rowid) {
            while i < scanned.len()
                && crabka_pgmvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice()
            {
                i += 1;
            }
            continue;
        }

        let mut visible: Option<(u64, u64, Option<Vec<crabka_pgtypes::Datum>>)> = None;
        while i < scanned.len()
            && crabka_pgmvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice()
        {
            let version = crabka_pgmvcc::version::decode_ts_tuple(&scanned[i].1)?;
            let start_ts = TimestampTransactionId::new(version.start_ts).map_err(|error| {
                ExecError::Unsupported(format!("invalid timestamp intent start timestamp: {error}"))
            })?;
            // Corrupt or unreadable descriptor metadata is never a legacy version:
            // failing closed prevents an unverified participant write becoming visible.
            let descriptor =
                crate::timestamp_txn::read_timestamp_txn_descriptor(primary_kv, start_ts)?;
            let verified_distributed_intent = match descriptor.as_ref() {
                Some(descriptor) => crate::timestamp_txn::local_intent_matches_descriptor(
                    kv, descriptor, table.id, bucket, rowid,
                )?,
                None => false,
            };
            let descriptor_operation = descriptor.as_ref().is_some_and(|descriptor| {
                crate::timestamp_txn::local_terminal_operation_matches_descriptor(
                    descriptor, table.id, bucket, rowid,
                )
            });
            // Per-range local sequences reuse stamp values, so an unrelated
            // transaction's descriptor can sit at this version's start
            // timestamp. A descriptor that names this row neither as a
            // verified intent nor as a terminal operation belongs to such a
            // colliding transaction; treating it as authoritative would fence
            // a committed single-shard row invisible.
            let primary_decision = (verified_distributed_intent || descriptor_operation)
                .then(|| descriptor.as_ref().map(|descriptor| descriptor.decision))
                .flatten();
            let candidate = match (version.state, primary_decision, verified_distributed_intent) {
                (
                    crabka_pgmvcc::version::TsVersionState::Intent,
                    Some(PrimaryTxnDecision::Pending),
                    true,
                ) if own_start_ts == Some(start_ts) => Some((u64::MAX, Some(version.row))),
                // A range-0 commit decision makes every prewritten intent logically
                // visible, even if this particular participant has not yet completed
                // its idempotent physical resolution.
                (
                    crabka_pgmvcc::version::TsVersionState::Intent,
                    Some(PrimaryTxnDecision::Committed(commit_ts)),
                    true,
                ) if commit_ts.get() <= read_ts.get() => Some((commit_ts.get(), Some(version.row))),
                (
                    crabka_pgmvcc::version::TsVersionState::Committed { commit_ts },
                    Some(PrimaryTxnDecision::Committed(primary_commit_ts)),
                    _,
                ) if commit_ts == primary_commit_ts.get() && commit_ts <= read_ts.get() => {
                    descriptor_operation.then_some((commit_ts, Some(version.row)))
                }
                (
                    crabka_pgmvcc::version::TsVersionState::Deleted { commit_ts },
                    Some(PrimaryTxnDecision::Committed(primary_commit_ts)),
                    _,
                ) if commit_ts == primary_commit_ts.get() && commit_ts <= read_ts.get() => {
                    descriptor_operation.then_some((commit_ts, None))
                }
                // Legacy/single-range timestamp versions have no descriptor.
                (crabka_pgmvcc::version::TsVersionState::Committed { commit_ts }, None, _)
                    if commit_ts <= read_ts.get() =>
                {
                    Some((commit_ts, Some(version.row)))
                }
                (crabka_pgmvcc::version::TsVersionState::Deleted { commit_ts }, None, _)
                    if commit_ts <= read_ts.get() =>
                {
                    Some((commit_ts, None))
                }
                _ => None,
            };
            if let Some((commit_ts, row)) = candidate
                && visible
                    .as_ref()
                    .is_none_or(|(_, current_commit_ts, _)| commit_ts > *current_commit_ts)
            {
                visible = Some((version.start_ts, commit_ts, row));
            }
            i += 1;
        }
        if let Some((start_ts, _commit_ts, Some(row))) = visible {
            out.push(ScannedRow {
                rowid,
                xmin: start_ts,
                row,
            });
        }
    }
    out.sort_by_key(|row| (row.rowid, row.xmin));
    Ok(out)
}

fn physical_rowid(table: &Table, row_prefix: &[u8]) -> Result<u64, ExecError> {
    if matches!(
        table.sharding,
        Some(crabka_pgcatalog::ShardingStrategy::Hash(_))
    ) {
        return Ok(crabka_pgkv::key::bucket_rowid_of(table.id, row_prefix)?.1);
    }
    Ok(crabka_pgkv::key::rowid_of(table.id, row_prefix)?)
}

fn physical_bucket(table: &Table, row_prefix: &[u8]) -> Result<Option<u32>, ExecError> {
    if matches!(
        table.sharding,
        Some(crabka_pgcatalog::ShardingStrategy::Hash(_))
    ) {
        return Ok(Some(
            crabka_pgkv::key::bucket_rowid_of(table.id, row_prefix)?.0,
        ));
    }
    Ok(None)
}

fn scan_table_for_catalog_interval(
    kv: &dyn Kv,
    table: &Table,
    interval: RowInterval,
) -> Result<crabka_pgkv::KvScan, ExecError> {
    if matches!(
        table.sharding,
        Some(crabka_pgcatalog::ShardingStrategy::Hash(_))
    ) {
        return scan_table_interval(kv, table.id, RowInterval::ALL);
    }
    scan_table_interval(kv, table.id, interval)
}

pub(crate) fn scan_table_interval(
    kv: &dyn Kv,
    table_id: u32,
    interval: RowInterval,
) -> Result<crabka_pgkv::KvScan, ExecError> {
    let start = interval.start.map_or_else(
        || crabka_pgkv::key::table_prefix(table_id),
        |rowid| crabka_pgkv::key::row_key(table_id, rowid),
    );
    let end = interval.end.map_or_else(
        || {
            let mut end = crabka_pgkv::key::table_prefix(table_id);
            let last = end.last_mut().expect("table prefix is non-empty");
            *last = last.checked_add(1).expect("primary index has a successor");
            end
        },
        |rowid| crabka_pgkv::key::row_key(table_id, rowid),
    );
    Ok(kv.scan_range(&start, &end)?)
}

/// Evaluate an optional WHERE predicate against a row (NULL => false, like SELECT).
pub(crate) fn row_matches(
    filter: Option<&Expr>,
    scope: &Scope,
    row: &[crabka_pgtypes::Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<bool, ExecError> {
    match filter {
        None => Ok(true),
        Some(f) => match crate::eval::eval(f, scope, row, ctx)? {
            crabka_pgtypes::Datum::Bool(b) => Ok(b),
            crabka_pgtypes::Datum::Null => Ok(false),
            _ => Err(ExecError::TypeMismatch(
                "argument of WHERE must be type boolean".into(),
            )),
        },
    }
}

/// SP40 Task 14: extract per-partition offset bounds from a single-foreign-table
/// query's top-level `WHERE` for pushdown into the Kafka foreign scan.
///
/// Walks the top-level `AND` chain of the filter and, for every `_partition = N`
/// constraint, collects the `_offset` range comparisons scoped to that partition
/// into [`ScanBounds`]. This is a PURE OPTIMIZATION: anything not representable in
/// `ScanBounds` (a bare `_offset` with no `_partition =`, a `_timestamp`/`LIMIT`
/// constraint, an `OR`, a non-envelope predicate) is simply omitted here and
/// remains a residual `WHERE` filter applied locally after the scan. Callers MUST
/// keep evaluating the full `WHERE`; pushed bounds must never change results.
///
/// Conversions (the scan reads `[start, end)` per partition):
/// - `_offset >= a` → start `a`; `_offset > a` → start `a + 1` (inclusive lower).
/// - `_offset <= b` → end `b + 1`; `_offset < b` → end `b` (exclusive upper).
/// - `_offset BETWEEN a AND b` → start `a`, end `b + 1` (PG bounds are inclusive).
///
/// Only offset bounds anchored to a concrete `_partition = N` are emitted: under
/// this `ScanBounds` shape (`Vec<(partition, offset)>`) a partition-less offset
/// cannot target a partition, so it stays residual.
#[must_use]
pub(crate) fn extract_scan_bounds(filter: Option<&Expr>) -> ScanBounds {
    let mut bounds = ScanBounds::default();
    let Some(filter) = filter else {
        return bounds;
    };

    // Flatten the top-level AND chain into its conjuncts. An OR or any other
    // shape is left intact (and thus never matches a comparison below), so it
    // contributes nothing — it remains a residual filter.
    let mut conjuncts = Vec::new();
    collect_conjuncts(filter, &mut conjuncts);

    // Resolve the single `_partition = N` anchor, if exactly one is present. With
    // zero (or conflicting/multiple) partition equalities we cannot scope offsets
    // to a partition, so we push nothing and let WHERE do all the work.
    let mut partition: Option<i32> = None;
    for c in &conjuncts {
        if let Some(p) = match_partition_eq(c) {
            match partition {
                None => partition = Some(p),
                Some(prev) if prev == p => {}
                // Two different `_partition =` values → unsatisfiable as written;
                // don't try to push, let the residual WHERE return zero rows.
                Some(_) => return ScanBounds::default(),
            }
        }
    }
    let Some(partition) = partition else {
        return bounds;
    };

    // Tightest inclusive-start / exclusive-end across all offset conjuncts.
    let mut start: Option<i64> = None;
    let mut end: Option<i64> = None;
    let mut tighten_start = |v: i64| {
        start = Some(start.map_or(v, |cur: i64| cur.max(v)));
    };
    let mut tighten_end = |v: i64| {
        end = Some(end.map_or(v, |cur: i64| cur.min(v)));
    };

    for c in &conjuncts {
        match match_offset_bound(c) {
            Some(OffsetBound::StartIncl(v)) => tighten_start(v),
            Some(OffsetBound::EndExcl(v)) => tighten_end(v),
            Some(OffsetBound::Between { start: s, end: e }) => {
                tighten_start(s);
                tighten_end(e);
            }
            None => {}
        }
    }

    if let Some(s) = start {
        bounds.start_offsets.push((partition, s));
    }
    if let Some(e) = end {
        bounds.end_offsets.push((partition, e));
    }
    bounds
}

/// Flatten a top-level `AND` chain into its leaf conjuncts (depth-first). A node
/// that is not an `AND` is itself one conjunct.
fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::Binary {
        op: crabka_pgparser::ast::BinaryOp::And,
        left,
        right,
    } = expr
    {
        collect_conjuncts(left, out);
        collect_conjuncts(right, out);
    } else {
        out.push(expr);
    }
}

/// An envelope-column reference by bare name (`_partition`/`_offset`/…). Envelope
/// columns are unqualified in practice; a table-qualified `t._offset` also matches
/// on the bare name (the qualifier is the single foreign table in scope).
fn envelope_col(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column { name, .. } => Some(name.as_str()),
        _ => None,
    }
}

/// Parse an integer literal expression to `i64`. Only bare/negated integer
/// literals are recognized (offsets/partitions are integers); anything else
/// (params, casts, non-integers) is not pushable and returns `None`.
fn int_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::IntLiteral(s) => s.parse::<i64>().ok(),
        Expr::Unary {
            op: crabka_pgparser::ast::UnaryOp::Neg,
            expr,
        } => int_literal(expr).map(|v| -v),
        _ => None,
    }
}

/// Match `_partition = N` (either operand order) and return `N`.
fn match_partition_eq(expr: &Expr) -> Option<i32> {
    let Expr::Binary {
        op: crabka_pgparser::ast::BinaryOp::Eq,
        left,
        right,
    } = expr
    else {
        return None;
    };
    let v = if envelope_col(left) == Some("_partition") {
        int_literal(right)?
    } else if envelope_col(right) == Some("_partition") {
        int_literal(left)?
    } else {
        return None;
    };
    i32::try_from(v).ok()
}

/// An offset constraint normalized to the scan's `[start, end)` convention.
enum OffsetBound {
    /// Inclusive lower offset.
    StartIncl(i64),
    /// Exclusive upper offset.
    EndExcl(i64),
    /// `BETWEEN a AND b` → inclusive `start`, exclusive `end`.
    Between { start: i64, end: i64 },
}

/// Match an `_offset` comparison / BETWEEN and normalize it to an [`OffsetBound`].
/// Returns `None` for anything that is not an `_offset` range constraint. The
/// comparison is recognized with the column on either side (the operator is
/// mirrored when the column is on the right).
fn match_offset_bound(expr: &Expr) -> Option<OffsetBound> {
    use crabka_pgparser::ast::BinaryOp;
    match expr {
        Expr::Binary { op, left, right } => {
            // Normalize to `_offset <op> literal` by mirroring when reversed.
            let (op, lit) = if envelope_col(left) == Some("_offset") {
                (*op, int_literal(right)?)
            } else if envelope_col(right) == Some("_offset") {
                (mirror_op(*op)?, int_literal(left)?)
            } else {
                return None;
            };
            match op {
                BinaryOp::Ge => Some(OffsetBound::StartIncl(lit)),
                BinaryOp::Gt => Some(OffsetBound::StartIncl(lit + 1)),
                BinaryOp::Le => Some(OffsetBound::EndExcl(lit + 1)),
                BinaryOp::Lt => Some(OffsetBound::EndExcl(lit)),
                _ => None,
            }
        }
        Expr::Between {
            expr,
            low,
            high,
            negated: false,
        } if envelope_col(expr) == Some("_offset") => {
            let lo = int_literal(low)?;
            let hi = int_literal(high)?;
            Some(OffsetBound::Between {
                start: lo,
                end: hi + 1,
            })
        }
        _ => None,
    }
}

/// Mirror a comparison operator for the reversed-operand form (`5 < _offset`
/// means `_offset > 5`). Only the inequalities used for offset bounds are mapped.
fn mirror_op(op: crabka_pgparser::ast::BinaryOp) -> Option<crabka_pgparser::ast::BinaryOp> {
    use crabka_pgparser::ast::BinaryOp;
    match op {
        BinaryOp::Lt => Some(BinaryOp::Gt),
        BinaryOp::Le => Some(BinaryOp::Ge),
        BinaryOp::Gt => Some(BinaryOp::Lt),
        BinaryOp::Ge => Some(BinaryOp::Le),
        _ => None,
    }
}

/// SP40 Task 14: is the FROM clause exactly one foreign base table? Only then is
/// offset pushdown applicable. A join, a comma-FROM (cross join), or a derived
/// table all keep the full-scan path. A scanner must be registered (otherwise the
/// foreign read errors anyway) and the single table's catalog entry must have
/// `foreign` metadata. Non-foreign ordinary tables return `false` (unchanged).
fn is_single_foreign_table(
    catalog_kv: &dyn Kv,
    from: &[crabka_pgparser::ast::TableExpr],
    ctes: &crate::cte::CteContext,
    fctx: ForeignCtx,
) -> bool {
    if fctx.scanner.is_none() {
        return false;
    }
    let resolution = fctx.resolution;
    let [
        crabka_pgparser::ast::TableExpr::Table {
            name,
            columns: None,
            sample: None,
            ..
        },
    ] = from
    else {
        return false;
    };
    if name.schema.is_none() && ctes.lookup(&name.name).is_some() {
        return false;
    }
    resolve_relation(catalog_kv, resolution, name, SchemaDisposition::Reference).is_ok_and(|name| {
        crabka_pgcatalog::get_table(catalog_kv, &name).is_ok_and(|t| t.foreign.is_some())
    })
}

/// Build the relation for one FROM list (comma items folded as cross joins).
fn build_from(
    read_ctx: &crate::subquery::SubCtx<'_>,
    from: &[crabka_pgparser::ast::TableExpr],
    // SP40 Task 14: pushed-down offset bounds for the single-foreign-table case.
    // `Some` only when `from` is exactly one entry (set by `select_to_relation`);
    // joins/comma-FROM never see it and keep the full-scan + local-filter path.
    bounds: Option<&ScanBounds>,
    scan_plan: Option<&crate::plan_dist::DistributedScanPlan>,
    filter: Option<&Expr>,
) -> Result<Relation, ExecError> {
    let mut iter = from.iter();
    let first = iter
        .next()
        .ok_or_else(|| ExecError::Unsupported("build_from on empty FROM".into()))?;
    let mut acc = build_table_expr(read_ctx, first, bounds, scan_plan)?;
    for te in iter {
        // A comma-FROM (multiple tables) is a cross join — no single-table
        // pushdown applies, so subsequent items always scan in full.
        acc = append_from_item(
            read_ctx,
            acc,
            te,
            crabka_pgparser::ast::JoinKind::Cross,
            &crabka_pgparser::ast::JoinConstraint::None,
            filter,
        )?;
    }
    Ok(acc)
}

/// Join one more FROM item onto the accumulated relation.
///
/// An ordinary item materializes once and joins; a lateral one is rebuilt for
/// every accumulated row, with that row's values substituted for the outer
/// column references inside it.
fn append_from_item(
    read_ctx: &crate::subquery::SubCtx<'_>,
    acc: Relation,
    te: &crabka_pgparser::ast::TableExpr,
    kind: crabka_pgparser::ast::JoinKind,
    constraint: &crabka_pgparser::ast::JoinConstraint,
    filter: Option<&Expr>,
) -> Result<Relation, ExecError> {
    if !is_lateral_item(te, &acc.scope) {
        let mut acc = acc;
        let mut next = build_table_expr(read_ctx, te, None, None)?;
        if matches!(kind, crabka_pgparser::ast::JoinKind::Inner | crabka_pgparser::ast::JoinKind::Cross)
            && let Some(filter) = filter
        {
            push_local_where(&mut acc, &mut next, filter, read_ctx.eval_ctx)?;
        }
        let pushed_constraint = filter
            .filter(|filter| {
                matches!(kind, crabka_pgparser::ast::JoinKind::Cross)
                    && matches!(constraint, crabka_pgparser::ast::JoinConstraint::None)
                    && immutable_row_predicate(filter)
                    && {
                        let mut scope = acc.scope.clone();
                        scope.columns.extend(next.scope.columns.iter().cloned());
                        crate::eval::check_predicate_resolves(filter, &scope).is_ok()
                    }
            })
            .map(|filter| crabka_pgparser::ast::JoinConstraint::On(filter.clone()));
        return join_relations(
            acc,
            next,
            kind,
            pushed_constraint.as_ref().unwrap_or(constraint),
            read_ctx.eval_ctx,
            read_ctx.blocking_query_memory,
        );
    }
    lateral_join(read_ctx, acc, te, kind, constraint)
}

/// Apply immutable top-level WHERE conjuncts that bind to exactly one join side
/// before materializing an inner/cross product. The complete WHERE is evaluated
/// again after FROM, so this optimization cannot weaken filtering.
fn push_local_where(
    left: &mut Relation,
    right: &mut Relation,
    filter: &Expr,
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    let mut conjuncts = Vec::new();
    collect_conjuncts(filter, &mut conjuncts);
    for conjunct in conjuncts {
        if !immutable_row_predicate(conjunct) {
            continue;
        }
        let left_only = expr_references_scope(conjunct, &left.scope)
            && crate::eval::check_predicate_resolves(conjunct, &left.scope).is_ok();
        let right_only = expr_references_scope(conjunct, &right.scope)
            && crate::eval::check_predicate_resolves(conjunct, &right.scope).is_ok();
        match (left_only, right_only) {
            (true, false) => filter_relation(left, conjunct, ctx)?,
            (false, true) => filter_relation(right, conjunct, ctx)?,
            _ => {}
        }
    }
    Ok(())
}

fn immutable_row_predicate(expr: &Expr) -> bool {
    let mut immutable = !crate::agg::contains_aggregate(expr);
    crate::grouping::visit_expr(expr, &mut |node| {
        immutable &= !matches!(
            node,
            Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery { .. }
                | Expr::Quantified { .. }
                | Expr::ArraySubquery(_)
        ) && !matches!(node, Expr::Func(call) if !is_immutable_function(&call.name));
    });
    immutable
}

fn filter_relation(
    relation: &mut Relation,
    predicate: &Expr,
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    let rows = std::mem::take(&mut relation.rows);
    for row in rows {
        if row_matches(Some(predicate), &relation.scope, &row, ctx)? {
            relation.rows.push(row);
        }
    }
    Ok(())
}

/// Is this FROM item correlated with the columns already in scope?
///
/// `LATERAL` says so explicitly; `PostgreSQL` also makes a *function* item
/// lateral implicitly, so `FROM t, unnest(t.tags)` needs no keyword. A derived
/// table is never implicitly lateral. A reference to an earlier item without
/// `LATERAL` is an error there, and a reference left alone produces it.
fn is_lateral_item(te: &crabka_pgparser::ast::TableExpr, outer: &Scope) -> bool {
    use crabka_pgparser::ast::TableExpr;
    match te {
        TableExpr::Derived { lateral, .. } => *lateral,
        TableExpr::Function {
            lateral, functions, ..
        } => {
            *lateral
                || functions
                    .iter()
                    .flat_map(|call| call.args.iter())
                    .any(|arg| expr_references_scope(arg, outer))
        }
        TableExpr::Table { .. } | TableExpr::Join { .. } => false,
    }
}

/// Re-evaluate `te` once per accumulated row and concatenate the results.
///
/// Each iteration joins a one-row left relation against the specialized right
/// side, so `ON`/`USING`/`NATURAL` matching and LEFT-join NULL padding are the
/// ordinary join code. So `LEFT JOIN LATERAL` keeps an outer row whose lateral
/// side produced nothing, exactly as `PostgreSQL` does.
///
/// `RIGHT`/`FULL JOIN LATERAL` is only an error when the lateral item *does*
/// reference the other side: `PostgreSQL` accepts the keyword itself and runs
/// the join, because an item that reads nothing from the left needs no left row
/// to be evaluated against.
fn lateral_join(
    read_ctx: &crate::subquery::SubCtx<'_>,
    acc: Relation,
    te: &crabka_pgparser::ast::TableExpr,
    kind: crabka_pgparser::ast::JoinKind,
    constraint: &crabka_pgparser::ast::JoinConstraint,
) -> Result<Relation, ExecError> {
    use crabka_pgparser::ast::JoinKind;
    let ctx = read_ctx.eval_ctx;
    let mut binder =
        LateralBinder::new(read_ctx.catalog_kv, read_ctx.fctx.resolution, read_ctx.ctes);
    if matches!(kind, JoinKind::Right | JoinKind::Full) {
        let nulls = vec![Datum::Null; acc.scope.width()];
        let (specialized, referenced) = binder.bind(te, &acc.scope, &nulls);
        if let Some(relation) = referenced {
            // A lateral reference on the nullable side would have to be evaluated
            // for rows that do not exist yet, so PostgreSQL rejects the reference
            // rather than the join.
            return Err(ExecError::InvalidColumnReference(format!(
                "invalid reference to FROM-clause entry for table \"{relation}\""
            )));
        }
        // Nothing was correlated, so the item is an ordinary relation.
        let right = build_table_expr(read_ctx, &specialized, None, None)?;
        return join_relations(
            acc,
            right,
            kind,
            constraint,
            ctx,
            read_ctx.blocking_query_memory,
        );
    }
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    let mut scope: Option<Scope> = None;
    let mut bytes = 0usize;
    struct CachedRight {
        specialized: crabka_pgparser::ast::TableExpr,
        relation: Relation,
        index: PreparedJoinIndex,
    }
    let mut cache: Vec<CachedRight> = Vec::new();
    let mut cache_bytes = 0usize;
    for outer_row in &acc.rows {
        let (specialized, _) = binder.bind(te, &acc.scope, outer_row);
        let one = Relation {
            scope: acc.scope.clone(),
            rows: vec![outer_row.clone()],
        };
        let joined = if let Some(cached) = cache
            .iter()
            .find(|cached| cached.specialized == specialized)
        {
            join_relations_prepared(
                one,
                &cached.relation,
                kind,
                constraint,
                ctx,
                read_ctx.blocking_query_memory,
                &cached.index,
            )?
        } else {
            let right = build_table_expr(read_ctx, &specialized, None, None)?;
            let right_bytes = right
                .rows
                .iter()
                .map(|row| crate::scanner::datum_row_bytes(row))
                .sum::<usize>();
            // ponytail: cap memoization; a planner-level lateral flattening is
            // the upgrade if workloads need more than 64 stable variants.
            let can_cache = lateral_cacheable(te)
                && cache.len() < 64
                && !crate::scanner::exceeds_query_memory(
                    cache_bytes.saturating_add(right_bytes),
                    read_ctx.blocking_query_memory,
                );
            if can_cache {
                let index = prepare_join_index(&acc, &right, constraint)?;
                cache.push(CachedRight {
                    specialized,
                    relation: right,
                    index,
                });
                cache_bytes = cache_bytes.saturating_add(right_bytes);
                let cached = cache.last().expect("the cache entry was just pushed");
                join_relations_prepared(
                    one,
                    &cached.relation,
                    kind,
                    constraint,
                    ctx,
                    read_ctx.blocking_query_memory,
                    &cached.index,
                )?
            } else {
                join_relations(
                    one,
                    right,
                    kind,
                    constraint,
                    ctx,
                    read_ctx.blocking_query_memory,
                )?
            }
        };
        for row in &joined.rows {
            bytes = bytes.saturating_add(crate::scanner::datum_row_bytes(row));
        }
        if crate::scanner::exceeds_query_memory(bytes, read_ctx.blocking_query_memory) {
            return Err(crate::scanner::memory_budget_exceeded());
        }
        rows.extend(joined.rows);
        scope = Some(joined.scope);
    }
    // With no outer rows there is nothing to correlate against, but the output
    // still needs the lateral side's columns: build it once against a row of
    // NULLs, which yields its schema without depending on any outer value.
    let scope = match scope {
        Some(scope) => scope,
        None => {
            let nulls = vec![Datum::Null; acc.scope.width()];
            let (specialized, _) = binder.bind(te, &acc.scope, &nulls);
            let right = build_table_expr(read_ctx, &specialized, None, None)?;
            join_relations(
                Relation {
                    scope: acc.scope.clone(),
                    rows: Vec::new(),
                },
                right,
                kind,
                constraint,
                ctx,
                read_ctx.blocking_query_memory,
            )?
            .scope
        }
    };
    Ok(Relation { scope, rows })
}

fn lateral_cacheable(te: &crabka_pgparser::ast::TableExpr) -> bool {
    use crabka_pgparser::ast::{DistinctClause, QueryBody, SetExpr, TableExpr};
    let TableExpr::Derived {
        subquery,
        lateral: true,
        ..
    } = te
    else {
        return false;
    };
    let SetExpr::Query(QueryBody::Select(select)) = &subquery.body else {
        return false;
    };
    subquery.with.is_none()
        && subquery.order_by.is_empty()
        && subquery.limit.is_none()
        && subquery
            .offset
            .as_ref()
            .is_none_or(|offset| matches!(offset, Expr::IntLiteral(value) if value == "0"))
        && subquery.locking.is_none()
        && select.from.len() == 1
        && matches!(&select.from[0], TableExpr::Table { .. })
        && matches!(select.distinct, DistinctClause::All)
        && select.group_by.is_empty()
        && select.grouping.is_none()
        && select.having.is_none()
        && select.windows.is_empty()
        && select.window_calls.is_empty()
        && select.order_by.is_empty()
        && select.limit.is_none()
        && select.offset.is_none()
        && select.locking.is_none()
        && select.projection.iter().all(|item| match item {
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => true,
            SelectItem::Expr { expr, .. } => lateral_cacheable_expr(expr),
        })
        && select.filter.as_ref().is_none_or(lateral_cacheable_expr)
}

fn lateral_cacheable_expr(expr: &Expr) -> bool {
    match expr {
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Const { .. } => true,
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::Collate { expr, .. } => {
            lateral_cacheable_expr(expr)
        }
        Expr::Binary { left, right, .. } => {
            lateral_cacheable_expr(left) && lateral_cacheable_expr(right)
        }
        _ => false,
    }
}

/// Does `expr` name a column that resolves in `scope`?
///
/// Only a reference that actually binds counts, so an unqualified name that is
/// ambiguous or missing is left for ordinary resolution to report.
fn expr_references_scope(expr: &Expr, scope: &Scope) -> bool {
    if let Expr::Column { table, name } = expr {
        return scope.resolve(table.as_deref(), name).is_ok();
    }
    // A subquery argument is over-approximated: a shadowed reference inside one
    // would still count. That only ever adds the per-row rebuild, which produces
    // the same rows as materializing once.
    expr_children(expr)
        .into_iter()
        .any(|child| expr_references_scope(child, scope))
}

/// The immediate sub-expressions of `expr`, including those reached through a
/// subquery. The walk visits those by way of the subquery's own clauses.
fn expr_children(expr: &Expr) -> Vec<&Expr> {
    let mut owned: Vec<&Expr> = Vec::new();
    match expr {
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Quantified { expr, .. }
        | Expr::InSubquery { expr, .. } => owned.push(expr),
        Expr::Binary { left, right, .. } => owned.extend([left.as_ref(), right.as_ref()]),
        Expr::Func(call) => {
            if let FuncArgs::Exprs(args) = &call.args {
                owned.extend(args);
            }
        }
        Expr::InList { expr, list, .. } => {
            owned.push(expr);
            owned.extend(list);
        }
        Expr::Between {
            expr, low, high, ..
        } => owned.extend([expr.as_ref(), low.as_ref(), high.as_ref()]),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            owned.extend([expr.as_ref(), pattern.as_ref()]);
            owned.extend(escape.iter().map(std::convert::AsRef::as_ref));
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            owned.extend(operand.iter().map(std::convert::AsRef::as_ref));
            owned.extend(whens.iter().flat_map(|(when, then)| [when, then]));
            owned.extend(else_result.iter().map(std::convert::AsRef::as_ref));
        }
        Expr::QuantifiedArray { expr, array, .. } => {
            owned.extend([expr.as_ref(), array.as_ref()]);
        }
        Expr::ArrayLiteral(items) | Expr::Row(items) => owned.extend(items),
        Expr::Subscript { base, index } => owned.extend([base.as_ref(), index.as_ref()]),
        Expr::ArrayRef { base, subscripts } => {
            owned.push(base.as_ref());
            owned.extend(subscripts.iter().flat_map(ArraySubscript::bounds));
        }
        Expr::ArraySubquery(_) => {}
        Expr::FieldSelect { base, .. } | Expr::FieldSelectAll(base) => owned.push(base.as_ref()),
        Expr::SqlJson(json) => owned.extend(json.children()),
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Default
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::Const { .. } => {}
    }
    owned
}

/// The mutable counterpart of [`expr_children`].
fn expr_children_mut(expr: &mut Expr) -> Vec<&mut Expr> {
    let mut owned: Vec<&mut Expr> = Vec::new();
    match expr {
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Quantified { expr, .. }
        | Expr::InSubquery { expr, .. } => owned.push(expr),
        Expr::Binary { left, right, .. } => owned.extend([left.as_mut(), right.as_mut()]),
        Expr::Func(call) => {
            if let FuncArgs::Exprs(args) = &mut call.args {
                owned.extend(args);
            }
        }
        Expr::InList { expr, list, .. } => {
            owned.push(expr);
            owned.extend(list);
        }
        Expr::Between {
            expr, low, high, ..
        } => owned.extend([expr.as_mut(), low.as_mut(), high.as_mut()]),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            owned.extend([expr.as_mut(), pattern.as_mut()]);
            owned.extend(escape.iter_mut().map(std::convert::AsMut::as_mut));
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            owned.extend(operand.iter_mut().map(std::convert::AsMut::as_mut));
            owned.extend(whens.iter_mut().flat_map(|(when, then)| [when, then]));
            owned.extend(else_result.iter_mut().map(std::convert::AsMut::as_mut));
        }
        Expr::QuantifiedArray { expr, array, .. } => {
            owned.extend([expr.as_mut(), array.as_mut()]);
        }
        Expr::ArrayLiteral(items) | Expr::Row(items) => owned.extend(items),
        Expr::Subscript { base, index } => owned.extend([base.as_mut(), index.as_mut()]),
        Expr::ArrayRef { base, subscripts } => {
            owned.push(base.as_mut());
            owned.extend(subscripts.iter_mut().flat_map(ArraySubscript::bounds_mut));
        }
        Expr::ArraySubquery(_) => {}
        Expr::FieldSelect { base, .. } | Expr::FieldSelectAll(base) => owned.push(base.as_mut()),
        Expr::SqlJson(json) => owned.extend(json.children_mut()),
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Default
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::Const { .. } => {}
    }
    owned
}

/// The query expressions directly under `expr` (its subqueries).
fn query_children_mut(expr: &mut Expr) -> Vec<&mut crabka_pgparser::ast::QueryExpr> {
    match expr {
        Expr::ScalarSubquery(query) | Expr::Exists(query) => vec![query],
        Expr::InSubquery { subquery, .. } | Expr::Quantified { subquery, .. } => vec![subquery],
        _ => Vec::new(),
    }
}

/// The read handles a bind pass needs to describe an inner FROM clause, plus the
/// per-lateral-join cache of those descriptions.
///
/// The FROM structure of a lateral item is identical for every outer row, so the
/// blocks are described once (in walk order) and reused for the remaining rows.
struct LateralBinder<'a> {
    catalog_kv: &'a dyn Kv,
    resolution: &'a crate::relname::ResolutionScope,
    ctes: &'a crate::cte::CteContext,
    /// The column names each query block's FROM provides, in walk order.
    /// `None` for a block whose FROM could not be described.
    described: Vec<Option<Vec<String>>>,
}

impl<'a> LateralBinder<'a> {
    fn new(
        catalog_kv: &'a dyn Kv,
        resolution: &'a crate::relname::ResolutionScope,
        ctes: &'a crate::cte::CteContext,
    ) -> Self {
        Self {
            catalog_kv,
            resolution,
            ctes,
            described: Vec::new(),
        }
    }

    /// Replace every reference to an outer column inside `te` with that column's
    /// value from `row`, yielding a FROM item that no longer depends on the outer
    /// relation and can be built by the ordinary path. Also reports the outer
    /// relation the item referenced, which names the `42P10` a `RIGHT`/`FULL`
    /// join raises for a lateral reference it cannot evaluate.
    ///
    /// A reference is substituted only when it cannot bind to a FROM item
    /// *inside* `te`: a qualifier re-introduced there shadows the outer one, and
    /// an unqualified name is substituted only when no enclosing FROM supplies a
    /// column of that name. `PostgreSQL` resolves the inner query level first
    /// and falls back to the lateral scope.
    fn bind(
        &mut self,
        te: &crabka_pgparser::ast::TableExpr,
        outer: &Scope,
        row: &[Datum],
    ) -> (crabka_pgparser::ast::TableExpr, Option<String>) {
        let mut bound = te.clone();
        let mut pass = BindPass {
            binder: self,
            outer,
            row,
            visited: 0,
            referenced: None,
        };
        let shadow = Shadow::default();
        pass.table_expr(&mut bound, &shadow);
        let referenced = pass.referenced;
        (bound, referenced)
    }
}

/// One walk of a lateral item, substituting `row`'s values into it.
struct BindPass<'a, 'b> {
    binder: &'a mut LateralBinder<'b>,
    outer: &'a Scope,
    row: &'a [Datum],
    /// How many query blocks this pass has described so far, which indexes the
    /// binder's cache.
    visited: usize,
    /// The outer relation whose column was first substituted, if any.
    referenced: Option<String>,
}

/// The FROM-item names visible at the point being rewritten, which take
/// precedence over the outer relation's.
#[derive(Debug, Clone)]
struct Shadow {
    qualifiers: Vec<String>,
    /// The unqualified column names the enclosing FROM clauses supply. `None`
    /// once some enclosing FROM could not be described, which leaves every
    /// unqualified name to inner resolution rather than guessing.
    columns: Option<Vec<String>>,
}

impl Default for Shadow {
    /// At the top of a lateral item nothing is in scope yet, so no name is
    /// shadowed. That is different from "we do not know what is in scope".
    fn default() -> Self {
        Self {
            qualifiers: Vec::new(),
            columns: Some(Vec::new()),
        }
    }
}

impl BindPass<'_, '_> {
    /// The shadow in force inside a query block whose FROM list is `from`.
    fn extended(&mut self, shadow: &Shadow, from: &[crabka_pgparser::ast::TableExpr]) -> Shadow {
        let mut next = shadow.clone();
        collect_qualifiers(from, &mut next.qualifiers);
        match self.describe(from) {
            Some(names) => {
                if let Some(columns) = &mut next.columns {
                    columns.extend(names);
                }
            }
            None => next.columns = None,
        }
        next
    }

    /// The column names `from` supplies, cached across outer rows.
    fn describe(&mut self, from: &[crabka_pgparser::ast::TableExpr]) -> Option<Vec<String>> {
        let index = self.visited;
        self.visited += 1;
        if index >= self.binder.described.len() {
            let names = if from.is_empty() {
                Some(Vec::new())
            } else {
                build_from_schema_with_ctes(
                    self.binder.catalog_kv,
                    self.binder.resolution,
                    from,
                    self.binder.ctes,
                )
                .ok()
                .map(|relation| {
                    relation
                        .scope
                        .columns
                        .into_iter()
                        .map(|column| column.name)
                        .collect()
                })
            };
            self.binder.described.push(names);
        }
        self.binder.described[index].clone()
    }

    fn table_expr(&mut self, te: &mut crabka_pgparser::ast::TableExpr, shadow: &Shadow) {
        use crabka_pgparser::ast::TableExpr;
        match te {
            TableExpr::Table { .. } => {}
            TableExpr::Derived { subquery, .. } => self.query(subquery, shadow),
            TableExpr::Function { functions, .. } => {
                for call in functions {
                    for arg in &mut call.args {
                        self.expr(arg, shadow);
                    }
                }
            }
            TableExpr::Join {
                left,
                right,
                constraint,
                ..
            } => {
                let inner = self.extended(shadow, std::slice::from_ref(left));
                let inner = self.extended(&inner, std::slice::from_ref(right));
                self.table_expr(left, shadow);
                self.table_expr(right, shadow);
                if let crabka_pgparser::ast::JoinConstraint::On(expr) = constraint {
                    self.expr(expr, &inner);
                }
            }
        }
    }

    fn query(&mut self, query: &mut crabka_pgparser::ast::QueryExpr, shadow: &Shadow) {
        // A CTE inside a lateral item may reference the outer row too, so the
        // WITH list is part of the walk.
        if let Some(with) = &mut query.with {
            for cte in &mut with.ctes {
                match &mut cte.body {
                    crabka_pgparser::ast::CteBody::Query(body) => self.query(body, shadow),
                    // A data-modifying CTE is not a lateral read path; leaving it
                    // alone keeps the reference to be reported by name resolution.
                    crabka_pgparser::ast::CteBody::Dml(_) => {}
                }
            }
        }
        self.set_expr(&mut query.body, shadow);
        for item in &mut query.order_by {
            self.expr(&mut item.expr, shadow);
        }
        for expr in query.limit.iter_mut().chain(query.offset.iter_mut()) {
            self.expr(expr, shadow);
        }
    }

    fn set_expr(&mut self, body: &mut crabka_pgparser::ast::SetExpr, shadow: &Shadow) {
        use crabka_pgparser::ast::{QueryBody, SetExpr};
        match body {
            SetExpr::Query(QueryBody::Select(select)) => {
                let inner = self.extended(shadow, &select.from);
                for item in &mut select.from {
                    self.table_expr(item, shadow);
                }
                for expr in select_exprs_mut(select) {
                    self.expr(expr, &inner);
                }
            }
            SetExpr::Query(QueryBody::Values(values)) => {
                for value_row in &mut values.rows {
                    for expr in value_row {
                        self.expr(expr, shadow);
                    }
                }
            }
            SetExpr::Query(QueryBody::Nested(nested)) => self.query(nested, shadow),
            SetExpr::SetOp { left, right, .. } => {
                self.set_expr(left, shadow);
                self.set_expr(right, shadow);
            }
        }
    }

    fn expr(&mut self, expr: &mut Expr, shadow: &Shadow) {
        if let Expr::Column { table, name } = expr {
            let bindable = match table {
                Some(qualifier) => !shadow
                    .qualifiers
                    .iter()
                    .any(|q| q.eq_ignore_ascii_case(qualifier)),
                None => shadow
                    .columns
                    .as_ref()
                    .is_some_and(|columns| !columns.iter().any(|column| column == name)),
            };
            if bindable && let Ok(index) = self.outer.resolve(table.as_deref(), name) {
                if self.referenced.is_none() {
                    self.referenced = self.outer.columns[index].qualifier.clone();
                }
                *expr = Expr::Const {
                    value: self.row[index].clone(),
                    ty: self.outer.ty_at(index),
                };
            }
            return;
        }
        for child in expr_children_mut(expr) {
            self.expr(child, shadow);
        }
        for query in query_children_mut(expr) {
            self.query(query, shadow);
        }
    }
}

/// Every qualifier a FROM list introduces (alias if present, else the relation
/// or function name).
fn collect_qualifiers(from: &[crabka_pgparser::ast::TableExpr], out: &mut Vec<String>) {
    use crabka_pgparser::ast::TableExpr;
    for item in from {
        match item {
            TableExpr::Table { name, alias, .. } => {
                out.push(alias.clone().unwrap_or_else(|| name.to_string()));
            }
            TableExpr::Derived { alias, .. } => out.push(alias.clone()),
            TableExpr::Function {
                alias, functions, ..
            } => out.push(alias.clone().unwrap_or_else(|| {
                functions
                    .first()
                    .map(|call| call.name.to_ascii_lowercase())
                    .unwrap_or_default()
            })),
            TableExpr::Join { left, right, .. } => {
                collect_qualifiers(std::slice::from_ref(left), out);
                collect_qualifiers(std::slice::from_ref(right), out);
            }
        }
    }
}

/// Every expression a SELECT evaluates against its own FROM scope.
fn select_exprs_mut(select: &mut SelectStmt) -> Vec<&mut Expr> {
    let mut out: Vec<&mut Expr> = Vec::new();
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            out.push(expr);
        }
    }
    out.extend(select.filter.iter_mut());
    out.extend(select.group_by.iter_mut());
    out.extend(select.having.iter_mut());
    out.extend(select.order_by.iter_mut().map(|item| &mut item.expr));
    if let crabka_pgparser::ast::DistinctClause::On(on) = &mut select.distinct {
        out.extend(on.iter_mut());
    }
    out
}

/// Read a partitioned parent as the append of its leaf partitions.
///
/// Each leaf is scanned through the ordinary base-table path and its rows are
/// permuted into the parent's column order. A leaf attached by `ATTACH
/// PARTITION` may declare the same columns in a different order, and
/// `PostgreSQL` maps them by name.
fn partitioned_scan(
    read_ctx: &crate::subquery::SubCtx<'_>,
    parent: &Table,
    qualifier: &str,
) -> Result<Relation, ExecError> {
    let scope = Scope::single(parent, qualifier);
    let mut rows = Vec::new();
    for leaf in crate::partition::leaves_of(read_ctx.catalog_kv, &parent.name)? {
        let leaf_table = crabka_pgcatalog::get_table(read_ctx.catalog_kv, &leaf)?;
        let ordinals = column_mapping(parent, &leaf_table)?;
        let relation = build_table_expr(
            read_ctx,
            &crabka_pgparser::ast::TableExpr::Table {
                name: crabka_pgparser::ast::RelationRef::qualified(&leaf.schema, &leaf.name),
                only: true,
                alias: None,
                columns: None,
                sample: None,
            },
            None,
            None,
        )?;
        rows.extend(relation.rows.into_iter().map(|row| {
            ordinals
                .iter()
                .map(|ordinal| row.get(*ordinal).cloned().unwrap_or(Datum::Null))
                .collect::<Vec<_>>()
        }));
    }
    Ok(Relation { scope, rows })
}

/// Read a table and all inheritance descendants as rows shaped like the parent.
fn inherited_scan(
    read_ctx: &crate::subquery::SubCtx<'_>,
    parent: &Table,
    qualifier: &str,
) -> Result<Relation, ExecError> {
    let scope = Scope::single(parent, qualifier);
    let mut relations = vec![parent.name.clone()];
    relations.extend(crate::inheritance::descendants(
        read_ctx.catalog_kv,
        &parent.name,
    )?);
    let mut rows = Vec::new();
    for relation_name in relations {
        let table = crabka_pgcatalog::get_table(read_ctx.catalog_kv, &relation_name)?;
        let ordinals = column_mapping(parent, &table)?;
        let relation = build_table_expr(
            read_ctx,
            &crabka_pgparser::ast::TableExpr::Table {
                name: crabka_pgparser::ast::RelationRef::qualified(
                    &relation_name.schema,
                    &relation_name.name,
                ),
                only: true,
                alias: None,
                columns: None,
                sample: None,
            },
            None,
            None,
        )?;
        rows.extend(relation.rows.into_iter().map(|row| {
            ordinals
                .iter()
                .map(|ordinal| row.get(*ordinal).cloned().unwrap_or(Datum::Null))
                .collect::<Vec<_>>()
        }));
    }
    Ok(Relation { scope, rows })
}

/// For each of `target`'s columns, the ordinal of the same-named column in
/// `source`, the permutation that rewrites a `source`-shaped row into a
/// `target`-shaped one. A partition and its parent always declare the same
/// column names, but `ATTACH PARTITION` maps them by name, not by position.
pub(crate) fn column_mapping(target: &Table, source: &Table) -> Result<Vec<usize>, ExecError> {
    target
        .columns
        .iter()
        .map(|column| {
            source
                .column_index(&column.name)
                .ok_or_else(|| ExecError::ChildMissingColumn(column.name.clone()))
        })
        .collect()
}

/// The leaf partition a row of `parent`'s shape belongs to, together with the
/// row permuted into that leaf's own column order.
///
/// `None` means no partition accepts the row, which is `PostgreSQL`'s 23514.
fn route_row_to_leaf(
    kv: &dyn Kv,
    parent: &Table,
    row: &[Datum],
) -> Result<Option<(Table, Vec<Datum>)>, ExecError> {
    let Some(scheme) = crate::partition::scheme_of(kv, &parent.name)? else {
        return Ok(Some((parent.clone(), row.to_vec())));
    };
    let partitions = crate::partition::partitions_of(kv, &parent.name)?;
    let Some(chosen) = crate::partition::route(&scheme, &partitions, row)? else {
        return Ok(None);
    };
    let child = crabka_pgcatalog::get_table(kv, &chosen.name)?;
    let ordinals = column_mapping(&child, parent)?;
    let child_row = ordinals
        .iter()
        .map(|ordinal| row.get(*ordinal).cloned().unwrap_or(Datum::Null))
        .collect::<Vec<_>>();
    route_row_to_leaf(kv, &child, &child_row)
}

fn build_table_expr(
    read_ctx: &crate::subquery::SubCtx<'_>,
    te: &crabka_pgparser::ast::TableExpr,
    // SP40 Task 14: pushed-down offset bounds, `Some` only for a single foreign
    // base table. Applied verbatim to the foreign scan; `None` ⇒ full scan.
    bounds: Option<&ScanBounds>,
    scan_plan: Option<&crate::plan_dist::DistributedScanPlan>,
) -> Result<Relation, ExecError> {
    let catalog_kv = read_ctx.catalog_kv;
    let resolution = read_ctx.fctx.resolution;
    let kv = read_ctx.kv;
    let global = read_ctx.global;
    let gsnap = read_ctx.gsnap;
    let snapshot = read_ctx.snapshot;
    let own = read_ctx.own;
    let ctes = read_ctx.ctes;
    let ctx = read_ctx.eval_ctx;
    let fctx = read_ctx.fctx;
    let range_scanner = read_ctx.range_scanner;
    use crabka_pgparser::ast::TableExpr;
    match te {
        TableExpr::Table {
            name,
            only,
            alias,
            columns,
            sample,
        } => {
            // A base-table alias may rename the leading columns (`t AS q(x, y)`),
            // exactly like a derived table's. The rename applies to whatever the
            // name resolves to — a CTE, a view, a catalog relation, or a stored
            // table — so it wraps the ordinary build rather than duplicating it.
            if let Some(names) = columns {
                let base = build_table_expr(
                    read_ctx,
                    &TableExpr::Table {
                        name: name.clone(),
                        only: *only,
                        alias: alias.clone(),
                        columns: None,
                        sample: sample.clone(),
                    },
                    bounds,
                    scan_plan,
                )?;
                let qualifier = alias.clone().unwrap_or_else(|| name.to_string());
                return crate::values::requalify_derived(base, &qualifier, &Some(names.clone()));
            }
            if let Some(sample) = sample {
                let base = build_table_expr(
                    read_ctx,
                    &TableExpr::Table {
                        name: name.clone(),
                        only: *only,
                        alias: alias.clone(),
                        columns: columns.clone(),
                        sample: None,
                    },
                    bounds,
                    scan_plan,
                )?;
                return apply_tablesample(base, sample, ctx);
            }
            // A CTE is never schema-qualified, so `public.t` names the stored
            // relation even where a CTE `t` is in scope, as PostgreSQL does.
            if name.schema.is_none()
                && let Some(rel) = ctes.lookup(&name.name)
            {
                let qualifier = alias.as_deref().unwrap_or(&name.name);
                return Ok(crate::cte::requalify_cte(rel, qualifier));
            }
            if name.schema.is_none()
                && let Some(runtime) = &ctx.transition_relations
                && let Some(transition) = runtime
                    .lock()
                    .expect("transition relation mutex")
                    .get(&name.name)
                    .cloned()
            {
                let qualifier = alias.as_deref().unwrap_or(&name.name);
                return Ok(Relation {
                    scope: Scope {
                        columns: transition
                            .columns
                            .into_iter()
                            .map(|(name, ty)| ColumnBinding {
                                qualifier: Some(qualifier.to_string()),
                                name,
                                ty,
                            })
                            .collect(),
                    },
                    rows: transition.rows,
                });
            }
            let name =
                &resolve_relation(catalog_kv, resolution, name, SchemaDisposition::Reference)?;
            if let Some(rel) = virtual_catalog_relation(catalog_kv, name, alias.as_deref(), ctx)? {
                return Ok(rel);
            }
            match crabka_pgcatalog::get_view(catalog_kv, name) {
                Ok(view) => {
                    let statement = crabka_pgparser::parse(&view.definition)?;
                    let [Statement::Query(query)] = statement.as_slice() else {
                        return Err(ExecError::Unsupported(
                            "stored view definition is not a query".into(),
                        ));
                    };
                    let relation = crate::query::query_to_relation(read_ctx, query)?;
                    let qualifier = alias.as_deref().unwrap_or(&view.name.name);
                    return requalify_view_relation(relation, &view, qualifier);
                }
                Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => {}
                Err(error) => return Err(error.into()),
            }
            let t = crabka_pgcatalog::get_table(catalog_kv, name)?;
            let qualifier = alias.as_deref().unwrap_or(&t.name.name);
            if !*only && !crate::inheritance::children_of(catalog_kv, &t.name)?.is_empty() {
                return inherited_scan(read_ctx, &t, qualifier);
            }
            // A partitioned parent owns no rows: reading it is an append over
            // its leaves. Doing this before every other scan path is what keeps
            // a partitioned relation from silently answering empty.
            if crate::partition::is_partitioned(catalog_kv, &t.name)? {
                return partitioned_scan(read_ctx, &t, qualifier);
            }
            // SP40: a foreign table reads through the registered scanner, not the
            // local MVCC version store. `build_from` materializes BEFORE WHERE, so
            // this scan runs even for `WHERE false` — there is no skip path.
            if let Some(meta) = &t.foreign {
                let scanner = fctx.scanner.ok_or_else(|| {
                    ExecError::Unsupported("foreign tables require the `kafka` feature".into())
                })?;
                let server = crabka_pgcatalog::get_server(catalog_kv, &meta.server)?;
                // A per-user mapping is optional: fall back to no credentials when
                // the current user has none registered for this server.
                let mapping =
                    crabka_pgcatalog::get_user_mapping(catalog_kv, fctx.current_user, &meta.server)
                        .ok();
                // SP40 Task 14: pass the pushed-down slice when present (single
                // foreign table). The residual WHERE still re-filters locally, so
                // results are identical whether or not the scan honors `bounds`.
                let default_bounds = ScanBounds::default();
                let scan_bounds = bounds.unwrap_or(&default_bounds);
                let mut rows = scanner.scan(&t, &server, mapping.as_ref(), scan_bounds, ctx)?;
                resolve_scanned_regclass(catalog_kv, &t, &mut rows)?;
                let scope = Scope::single(&t, qualifier);
                return Ok(Relation { scope, rows });
            }
            let scope = Scope::single(&t, qualifier);
            let default_scan_plan = crate::plan_dist::DistributedScanPlan::default();
            let distributed_plan = scan_plan.unwrap_or(&default_scan_plan);
            if let Some(rows) = try_scan_with_local_index(read_ctx, &t, distributed_plan)? {
                let mut rows: Vec<Vec<Datum>> =
                    rows.into_iter().map(|scanned| scanned.row).collect();
                resolve_scanned_regclass(catalog_kv, &t, &mut rows)?;
                return Ok(Relation { scope, rows });
            }
            let scan_request = ScanRequest {
                local: kv,
                global,
                global_snapshot: gsnap,
                snapshot,
                own_xid: own,
                read_ts: None,
                own_start_ts: None,
                table: &t,
                interval: RowInterval::ALL,
                predicate: distributed_plan.predicate.clone(),
                projection: distributed_plan.projection.clone(),
                partial_aggregate: distributed_plan.partial_aggregate.clone(),
                top_k: distributed_plan.top_k.clone(),
            };
            let rows = match crate::scanner::collect_cursor_bounded(
                range_scanner,
                scan_request,
                read_ctx.blocking_query_memory,
            ) {
                Ok(rows) => rows,
                Err(error) if should_retry_without_scan_pushdown(&error, distributed_plan) => {
                    crate::scanner::collect_cursor_bounded(
                        range_scanner,
                        ScanRequest {
                            local: kv,
                            global,
                            global_snapshot: gsnap,
                            snapshot,
                            own_xid: own,
                            read_ts: None,
                            own_start_ts: None,
                            table: &t,
                            interval: RowInterval::ALL,
                            predicate: PredicatePushdown::FullScan,
                            projection: crate::ProjectionPushdown::All,
                            partial_aggregate: None,
                            top_k: None,
                        },
                        read_ctx.blocking_query_memory,
                    )?
                }
                Err(error) => return Err(error),
            }
            .into_iter()
            .map(|scanned| scanned.row)
            .collect();
            let mut rows: Vec<Vec<Datum>> = rows;
            resolve_scanned_regclass(catalog_kv, &t, &mut rows)?;
            Ok(Relation { scope, rows })
        }
        TableExpr::Join {
            left,
            right,
            kind,
            constraint,
        } => {
            if let Some(relation) =
                try_distributed_inner_equi_join(read_ctx, left, right, *kind, constraint)?
            {
                return Ok(relation);
            }
            // A join is never a single foreign table: each side scans in full and
            // the join predicate / residual WHERE filters locally.
            let l = build_table_expr(read_ctx, left, None, None)?;
            // A lateral right side sees the left side's columns, so it is rebuilt
            // per left row instead of materialized once.
            append_from_item(read_ctx, l, right, *kind, constraint, None)
        }
        TableExpr::Derived {
            subquery,
            alias,
            columns,
            ..
        } => {
            let inner = crate::query::query_to_relation_with_ctes(read_ctx, subquery)?;
            crate::values::requalify_derived(inner, alias, columns)
        }
        // P2: a user-defined SQL function in FROM position is a parameterized
        // derived table — its body runs under the caller's own read context.
        // Built-in set-returning functions stay with the `srf` registry.
        TableExpr::Function {
            functions,
            with_ordinality,
            alias,
            column_aliases,
            ..
        } if crate::routine::expands_as_table(read_ctx.catalog_kv, functions) => {
            if let Some((columns, rows)) =
                crate::routine::eval_plpgsql_table_function(&functions[0], ctx)?
            {
                return crate::srf::user_function_relation(
                    &functions[0].name,
                    columns,
                    rows,
                    *with_ordinality,
                    alias.as_deref(),
                    column_aliases,
                );
            }
            if *with_ordinality {
                return Err(ExecError::Unsupported(
                    "WITH ORDINALITY over a user-defined function is not supported".into(),
                ));
            }
            let (query, names) =
                crate::routine::table_function_expansion(read_ctx.catalog_kv, &functions[0])?;
            let inner = crate::query::query_to_relation_with_ctes(read_ctx, &query)?;
            let columns = column_aliases.clone().or(Some(names));
            crate::values::requalify_derived(
                inner,
                alias.as_deref().unwrap_or(&functions[0].name),
                &columns,
            )
        }
        TableExpr::Function {
            functions,
            with_ordinality,
            alias,
            column_aliases,
            ..
        } => crate::srf::from_item(
            functions,
            *with_ordinality,
            alias.as_deref(),
            column_aliases,
            ctx,
        ),
    }
}

/// Apply a `TABLESAMPLE` clause to an already-materialized base-table relation.
///
/// `PostgreSQL` samples physical pages (`SYSTEM`) or individual rows
/// (`BERNOULLI`); crabka has no page layout to sample, so both methods draw rows
/// independently at the given probability. The percentage checks, the `42704`
/// for an unknown method, and the deterministic 0% / 100% ends match
/// `PostgreSQL` exactly; which rows a partial sample returns does not.
fn apply_tablesample(
    relation: Relation,
    sample: &crabka_pgparser::ast::TableSample,
    ctx: &crate::clock::EvalCtx,
) -> Result<Relation, ExecError> {
    if !matches!(sample.method.as_str(), "system" | "bernoulli") {
        return Err(ExecError::FunctionError {
            sqlstate: "42704",
            message: format!("tablesample method {} does not exist", sample.method),
        });
    }
    let percent = crate::eval::eval(&sample.percent, &Scope::empty(), &[], ctx)?;
    if percent.is_null() {
        return Err(ExecError::FunctionError {
            sqlstate: "2202H",
            message: "TABLESAMPLE parameter cannot be null".into(),
        });
    }
    let Datum::Float8(percent) =
        crabka_pgtypes::cast::cast(&percent, ColumnType::Float8, &ctx.time_zone)?
    else {
        return Err(ExecError::TypeMismatch(
            "TABLESAMPLE percentage must be numeric".into(),
        ));
    };
    if !(0.0..=100.0).contains(&percent) {
        return Err(ExecError::FunctionError {
            sqlstate: "2202H",
            message: "sample percentage must be between 0 and 100".into(),
        });
    }
    let seed = match &sample.repeatable {
        Some(expr) => {
            let value = crate::eval::eval(expr, &Scope::empty(), &[], ctx)?;
            if value.is_null() {
                // A null seed is invalid_tablesample_repeat, NOT the
                // invalid_tablesample_argument a null/out-of-range percentage is.
                return Err(ExecError::FunctionError {
                    sqlstate: "2202G",
                    message: "TABLESAMPLE REPEATABLE parameter cannot be null".into(),
                });
            }
            let Datum::Float8(seed) =
                crabka_pgtypes::cast::cast(&value, ColumnType::Float8, &ctx.time_zone)?
            else {
                return Err(ExecError::TypeMismatch(
                    "TABLESAMPLE seed must be numeric".into(),
                ));
            };
            seed.to_bits()
        }
        None => 0x9E37_79B9_7F4A_7C15,
    };
    let Relation { scope, rows } = relation;
    // A xorshift over (seed, row ordinal): repeatable across runs for a given
    // seed, and independent of how the rows were physically stored.
    let threshold = percent / 100.0;
    let sampled = rows
        .into_iter()
        .enumerate()
        .filter(|(index, _)| {
            let ordinal = u64::try_from(*index).unwrap_or(u64::MAX);
            let mut state = seed ^ ordinal.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            // The top 32 bits convert to f64 exactly, so the draw needs no
            // lossy cast.
            let bucket = u32::try_from(state >> 32).unwrap_or(u32::MAX);
            f64::from(bucket) / f64::from(u32::MAX) < threshold
        })
        .map(|(_, row)| row)
        .collect();
    Ok(Relation {
        scope,
        rows: sampled,
    })
}

fn try_distributed_inner_equi_join(
    read_ctx: &crate::subquery::SubCtx<'_>,
    left_expr: &crabka_pgparser::ast::TableExpr,
    right_expr: &crabka_pgparser::ast::TableExpr,
    kind: crabka_pgparser::ast::JoinKind,
    constraint: &crabka_pgparser::ast::JoinConstraint,
) -> Result<Option<Relation>, ExecError> {
    let catalog_kv = read_ctx.catalog_kv;
    let resolution = read_ctx.fctx.resolution;
    let gsnap = read_ctx.gsnap;
    let snapshot = read_ctx.snapshot;
    let own = read_ctx.own;
    let ctes = read_ctx.ctes;
    let range_scanner = read_ctx.range_scanner;
    use crabka_pgparser::ast::{BinaryOp, Expr, JoinConstraint, JoinKind, TableExpr};

    if kind != JoinKind::Inner {
        return Ok(None);
    }
    let (
        TableExpr::Table {
            name: left_name,
            alias: left_alias,
            columns: None,
            sample: None,
            ..
        },
        TableExpr::Table {
            name: right_name,
            alias: right_alias,
            columns: None,
            sample: None,
            ..
        },
    ) = (left_expr, right_expr)
    else {
        return Ok(None);
    };
    if (left_name.schema.is_none() && ctes.lookup(&left_name.name).is_some())
        || (right_name.schema.is_none() && ctes.lookup(&right_name.name).is_some())
    {
        return Ok(None);
    }
    let left_name = &resolve_relation(
        catalog_kv,
        resolution,
        left_name,
        SchemaDisposition::Reference,
    )?;
    let right_name = &resolve_relation(
        catalog_kv,
        resolution,
        right_name,
        SchemaDisposition::Reference,
    )?;
    let JoinConstraint::On(Expr::Binary {
        op: BinaryOp::Eq,
        left: key_left,
        right: key_right,
    }) = constraint
    else {
        return Ok(None);
    };
    let table =
        |name: &crabka_pgcatalog::RelationName| match crabka_pgcatalog::get_table(catalog_kv, name)
        {
            Ok(table) => Ok(Some(table)),
            Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => Ok(None),
            Err(error) => Err(ExecError::from(error)),
        };
    let Some(left_table) = table(left_name)? else {
        return Ok(None);
    };
    let Some(right_table) = table(right_name)? else {
        return Ok(None);
    };
    if !left_table.sharded
        || !right_table.sharded
        || left_table.foreign.is_some()
        || right_table.foreign.is_some()
    {
        return Ok(None);
    }
    let left_qualifier = left_alias.as_deref().unwrap_or(&left_table.name.name);
    let right_qualifier = right_alias.as_deref().unwrap_or(&right_table.name.name);
    fn qualified_key(expr: &Expr) -> Option<(&str, &str)> {
        let Expr::Column {
            table: Some(table),
            name,
        } = expr
        else {
            return None;
        };
        Some((table.as_str(), name.as_str()))
    }
    let (Some((first_table, first_column)), Some((second_table, second_column))) =
        (qualified_key(key_left), qualified_key(key_right))
    else {
        return Ok(None);
    };
    let (left_column, right_column) =
        if first_table == left_qualifier && second_table == right_qualifier {
            (first_column, second_column)
        } else if first_table == right_qualifier && second_table == left_qualifier {
            (second_column, first_column)
        } else {
            return Ok(None);
        };
    let Some(left_key) = left_table
        .columns
        .iter()
        .position(|column| column.name == left_column)
    else {
        return Ok(None);
    };
    let Some(right_key) = right_table
        .columns
        .iter()
        .position(|column| column.name == right_column)
    else {
        return Ok(None);
    };
    let planned =
        range_scanner.join_strategy_for_keys(&left_table, &right_table, &[left_key], &[right_key]);
    let strategy = match planned {
        crate::plan_dist::JoinStrategy::Broadcast { small_table_id }
            if small_table_id == u64::from(left_table.id) =>
        {
            JoinExecutionStrategy::BroadcastLeft
        }
        crate::plan_dist::JoinStrategy::Broadcast { small_table_id }
            if small_table_id == u64::from(right_table.id) =>
        {
            JoinExecutionStrategy::BroadcastRight
        }
        crate::plan_dist::JoinStrategy::Broadcast { .. } => JoinExecutionStrategy::Gather,
        crate::plan_dist::JoinStrategy::CoPartitioned
            if hash_sharding_matches_join_keys(
                &left_table,
                &right_table,
                left_column,
                right_column,
            ) =>
        {
            JoinExecutionStrategy::CoPartitioned
        }
        crate::plan_dist::JoinStrategy::CoPartitioned => JoinExecutionStrategy::Gather,
        crate::plan_dist::JoinStrategy::Gather => JoinExecutionStrategy::Gather,
    };
    // Folded onto the enclosing read span rather than carried on one of this
    // join's own: the strategy is a property of how the statement ran, and a
    // span that declares no such field ignores this.
    tracing::Span::current().record("pg.join_strategy", strategy.as_str());
    let join_snapshot = |source: &crabka_pgmvcc::visibility::Snapshot| JoinSnapshot {
        xmin: source.xmin,
        xmax: source.xmax,
        xip: source.xip.clone(),
    };
    let request = JoinRangeRequest {
        local_snapshot: join_snapshot(snapshot),
        global_snapshot: join_snapshot(gsnap),
        read_ts: 1,
        own_xid: own,
        own_start_ts: None,
        kind: ScannerJoinKind::Inner,
        left_keys: vec![left_key],
        right_keys: vec![right_key],
        strategy,
        left: JoinTableInterval {
            table_id: u64::from(left_table.id),
            table_name: left_table.name.to_string(),
            interval: RowInterval::ALL,
        },
        right: JoinTableInterval {
            table_id: u64::from(right_table.id),
            table_name: right_table.name.to_string(),
            interval: RowInterval::ALL,
        },
        broadcast_rows: matches!(
            strategy,
            JoinExecutionStrategy::BroadcastLeft | JoinExecutionStrategy::BroadcastRight
        )
        .then(Vec::new),
        left_filter: PredicatePushdown::FullScan,
        right_filter: PredicatePushdown::FullScan,
        projection: Vec::new(),
    };
    let result = match range_scanner.join(request) {
        Ok(result) => result,
        Err(ExecError::Unsupported(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    result
        .validate()
        .map_err(|error| ExecError::Unsupported(error.to_string()))?;
    let rows = result
        .rows
        .into_iter()
        .map(|JoinRow { tuple }| {
            crabka_pgmvcc::version::decode_tuple(&tuple)
                .map(|(_, _, row)| row)
                .map_err(ExecError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    // The join result is the left table's columns followed by the right's, so
    // the right table's `regclass` columns sit past the left's width.
    let mut rows = rows;
    let mut regclass_columns = regclass_column_indexes(&left_table, 0);
    regclass_columns.extend(regclass_column_indexes(
        &right_table,
        left_table.columns.len(),
    ));
    resolve_regclass_at(read_ctx.catalog_kv, &regclass_columns, &mut rows)?;
    let mut scope = Scope::single(&left_table, left_qualifier);
    scope
        .columns
        .extend(Scope::single(&right_table, right_qualifier).columns);
    Ok(Some(Relation { scope, rows }))
}

fn hash_sharding_matches_join_keys(
    left: &Table,
    right: &Table,
    left_column: &str,
    right_column: &str,
) -> bool {
    use crabka_pgcatalog::ShardingStrategy;

    let (Some(ShardingStrategy::Hash(left_hash)), Some(ShardingStrategy::Hash(right_hash))) =
        (&left.sharding, &right.sharding)
    else {
        return false;
    };
    left_hash.columns.as_slice() == [left_column] && right_hash.columns.as_slice() == [right_column]
}

fn try_scan_with_local_index(
    read_ctx: &crate::subquery::SubCtx<'_>,
    table: &Table,
    plan: &crate::plan_dist::DistributedScanPlan,
) -> Result<Option<Vec<ScannedRow>>, ExecError> {
    if table.sharded || plan.partial_aggregate.is_some() {
        return Ok(None);
    }
    if let Some(predicate) = &plan.text_search
        && let Some(index) = choose_local_gin_index(read_ctx.catalog_kv, table, predicate.column)?
        && let Some(rows) = lookup_local_gin(
            &MvccReadContext {
                kv: read_ctx.kv,
                global: read_ctx.global,
                global_snapshot: read_ctx.gsnap,
                snapshot: read_ctx.snapshot,
                own: read_ctx.own,
            },
            table,
            &index,
            &predicate.query,
        )?
    {
        return Ok(Some(rows));
    }
    let Some((index, value)) =
        choose_local_index_equality(read_ctx.catalog_kv, table, &plan.predicate)?
    else {
        return Ok(None);
    };
    let rows = lookup_local_index_equal(
        &MvccReadContext {
            kv: read_ctx.kv,
            global: read_ctx.global,
            global_snapshot: read_ctx.gsnap,
            snapshot: read_ctx.snapshot,
            own: read_ctx.own,
        },
        table,
        &index,
        &[value],
    )?;
    crate::scanner::apply_executable_scan_pushdown(
        rows,
        &plan.predicate,
        &plan.projection,
        None,
        plan.top_k.as_ref(),
    )
    .map(Some)
}

fn choose_local_gin_index(
    catalog_kv: &dyn Kv,
    table: &Table,
    column: usize,
) -> Result<Option<crabka_pgcatalog::Index>, ExecError> {
    Ok(
        crabka_pgcatalog::list_table_indexes(catalog_kv, &table.name)?
            .into_iter()
            .find(|index| {
                index.placement == crabka_pgcatalog::IndexPlacement::Local
                    && index.method == crabka_pgcatalog::IndexMethod::Gin
                    && index.columns.len() == 1
                    && table.column_index(&index.columns[0]) == Some(column)
            }),
    )
}

fn choose_local_index_equality(
    catalog_kv: &dyn Kv,
    table: &Table,
    predicate: &PredicatePushdown,
) -> Result<Option<(crabka_pgcatalog::Index, Datum)>, ExecError> {
    let PredicatePushdown::Conjunctive(predicates) = predicate else {
        return Ok(None);
    };
    let indexes = crabka_pgcatalog::list_table_indexes(catalog_kv, &table.name)?;
    for predicate in predicates
        .iter()
        .filter(|predicate| predicate.op == crate::PredicateOp::Eq && !predicate.value.is_null())
    {
        let Some(index) = indexes.iter().find(|index| {
            index.placement == crabka_pgcatalog::IndexPlacement::Local
                && index.method == crabka_pgcatalog::IndexMethod::Btree
                && index.columns.len() == 1
                && table.column_index(&index.columns[0]) == Some(predicate.column)
        }) else {
            continue;
        };
        return Ok(Some((index.clone(), predicate.value.clone())));
    }
    Ok(None)
}

fn should_retry_without_scan_pushdown(
    error: &ExecError,
    plan: &crate::plan_dist::DistributedScanPlan,
) -> bool {
    if plan.partial_aggregate.is_some() || plan.top_k.is_some() {
        return false;
    }
    if *plan == crate::plan_dist::DistributedScanPlan::default() {
        return false;
    }
    let message = error.clone().into_pg().message;
    message.contains("pushdown") || message.contains("full scans")
}

fn try_execute_partial_aggregate_pushdown(
    read_ctx: &crate::subquery::SubCtx<'_>,
    s: &SelectStmt,
) -> Result<Option<Relation>, ExecError> {
    if !is_plain_partial_aggregate_select(s) {
        return Ok(None);
    }
    let Some((table, qualifier)) = single_sharded_base_table(
        read_ctx.catalog_kv,
        read_ctx.fctx.resolution,
        s,
        read_ctx.ctes,
    )?
    else {
        return Ok(None);
    };
    let spec = if s.group_by.is_empty() {
        crate::plan_dist::plan_scan(&table, s.filter.as_ref(), &s.projection).partial_aggregate
    } else {
        crate::plan_dist::grouped_partial_aggregate_for_select(&table, &s.projection, &s.group_by)
    };
    let Some(spec) = spec else {
        return Ok(None);
    };
    let predicate = match crate::plan_dist::strict_predicate_for_filter(&table, s.filter.as_ref()) {
        Ok(predicate) => predicate,
        Err(_) => return Ok(None),
    };
    let scope = Scope::single(&table, &qualifier);
    let (fields, _out_exprs, tys) = resolve_projection(&s.projection, &scope)?;
    let rows = read_ctx.range_scanner.scan(ScanRequest {
        local: read_ctx.kv,
        global: read_ctx.global,
        global_snapshot: read_ctx.gsnap,
        snapshot: read_ctx.snapshot,
        own_xid: read_ctx.own,
        read_ts: None,
        own_start_ts: None,
        table: &table,
        interval: RowInterval::ALL,
        predicate,
        projection: crate::ProjectionPushdown::All,
        partial_aggregate: Some(spec.clone()),
        top_k: None,
    })?;
    let rows = crate::scanner::finalize_partial_aggregate_rows(rows, &spec)?;
    let out_scope = Scope {
        columns: fields
            .iter()
            .zip(&tys)
            .map(|(field, ty)| ColumnBinding {
                qualifier: None,
                name: field.name.clone(),
                ty: *ty,
            })
            .collect(),
    };
    Ok(Some(Relation {
        scope: out_scope,
        rows: rows.into_iter().map(|row| row.row).collect(),
    }))
}

/// Stream a supported local aggregate through per-page partial-aggregate
/// folding, instead of a fold over every visible row after it is materialized.
///
/// Fires on exactly one ordinary non-sharded base table when every projection
/// item decomposes into scalar expressions over pushdown-model aggregate calls
/// (`CAST(count(*) AS BIGINT)`, `COALESCE(sum(x), 0)`, `sum(a) / count(*)`, …,
/// or the narrow grouped shape), with a WHERE that parses into the strict
/// pushdown predicate subset. Everything else keeps the materializing scan and
/// its whole-result memory budget: `DISTINCT`, `HAVING`, bare ungrouped
/// columns, aggregates over non-column arguments, whole-row reads, and
/// non-pushdown filters.
fn try_execute_local_streaming_aggregate(
    read_ctx: &crate::subquery::SubCtx<'_>,
    s: &SelectStmt,
) -> Result<Option<Relation>, ExecError> {
    if !is_streamable_aggregate_select(s) {
        return Ok(None);
    }
    let Some((table, qualifier)) = single_local_base_table(
        read_ctx.catalog_kv,
        read_ctx.fctx.resolution,
        s,
        read_ctx.ctes,
    )?
    else {
        return Ok(None);
    };
    let Some(plan) = local_streaming_aggregate_plan(&table, s) else {
        return Ok(None);
    };
    let Ok(predicate) = crate::plan_dist::strict_predicate_for_filter(&table, s.filter.as_ref())
    else {
        return Ok(None);
    };
    // An equality probe over a local index reads less than any table scan:
    // keep that existing path (and its materializing budget) for those filters.
    if choose_local_index_equality(read_ctx.catalog_kv, &table, &predicate)?.is_some() {
        return Ok(None);
    }
    let scope = Scope::single(&table, &qualifier);
    let (fields, out_exprs, tys) = resolve_projection(&s.projection, &scope)?;
    let states = crate::scanner::collect_partial_aggregates_bounded(
        read_ctx.range_scanner,
        ScanRequest {
            local: read_ctx.kv,
            global: read_ctx.global,
            global_snapshot: read_ctx.gsnap,
            snapshot: read_ctx.snapshot,
            own_xid: read_ctx.own,
            read_ts: None,
            own_start_ts: None,
            table: &table,
            interval: RowInterval::ALL,
            predicate,
            projection: crate::ProjectionPushdown::All,
            partial_aggregate: None,
            top_k: None,
        },
        plan.specs(),
        crate::scanner::BLOCKING_QUERY_MEMORY,
    )?;
    let rows = match &plan {
        StreamingAggregatePlan::Scalar { calls, specs } => {
            let values = finalize_scalar_streaming_aggregates(states, specs)?;
            vec![crate::agg::eval_over_aggregate_values(
                &out_exprs,
                &scope,
                calls,
                &values,
                read_ctx.eval_ctx,
            )?]
        }
        StreamingAggregatePlan::Grouped(spec) => {
            let Ok([state]) = <[Vec<ScannedRow>; 1]>::try_from(states) else {
                return Err(ExecError::Unsupported(
                    "grouped partial aggregate streaming expects exactly one spec".into(),
                ));
            };
            crate::scanner::finalize_partial_aggregate_rows(state, spec)?
                .into_iter()
                .map(|row| row.row)
                .collect()
        }
    };
    let out_scope = Scope {
        columns: fields
            .iter()
            .zip(&tys)
            .map(|(field, ty)| ColumnBinding {
                qualifier: None,
                name: field.name.clone(),
                ty: *ty,
            })
            .collect(),
    };
    Ok(Some(Relation {
        scope: out_scope,
        rows,
    }))
}

/// How the local streaming path computes a supported aggregate SELECT.
enum StreamingAggregatePlan {
    /// No GROUP BY: stream one partial spec per distinct aggregate call, then
    /// evaluate each projection expression over the finalized values.
    Scalar {
        /// Deduped aggregate calls, aligned index-for-index with `specs` (and
        /// with the finalized values fed to the outer-expression evaluation).
        calls: Vec<crabka_pgparser::ast::FuncCall>,
        specs: Vec<crate::PartialAggregateSpec>,
    },
    /// The narrow grouped shape: one spec whose finalized rows ARE the output
    /// rows. Those rows are the group key columns, then the aggregate, ordered
    /// by key.
    Grouped(crate::PartialAggregateSpec),
}

impl StreamingAggregatePlan {
    fn specs(&self) -> &[crate::PartialAggregateSpec] {
        match self {
            Self::Scalar { specs, .. } => specs,
            Self::Grouped(spec) => std::slice::from_ref(spec),
        }
    }
}

/// Decompose the SELECT into a streaming plan: the single grouped spec for the
/// grouped shape; with no GROUP BY, the deduped aggregate calls (each inside
/// the pushdown model) with everything around them scalar expressions to
/// evaluate over the finalized values. `None` when any part falls outside the
/// model, and the caller then keeps the materializing scan.
fn local_streaming_aggregate_plan(table: &Table, s: &SelectStmt) -> Option<StreamingAggregatePlan> {
    if !s.group_by.is_empty() {
        return crate::plan_dist::grouped_partial_aggregate_for_select(
            table,
            &s.projection,
            &s.group_by,
        )
        .map(StreamingAggregatePlan::Grouped);
    }
    let mut calls = Vec::new();
    for item in &s.projection {
        let SelectItem::Expr { expr, .. } = item else {
            return None;
        };
        if !crate::agg::collect_streamable_aggregate_calls(expr, &mut calls) {
            return None;
        }
    }
    // No aggregate anywhere means this is not an aggregate query at all (one
    // output row per table row) — never a streaming-fold candidate.
    if calls.is_empty() {
        return None;
    }
    let specs = calls
        .iter()
        .map(|call| crate::plan_dist::partial_aggregate_for_call(table, call))
        .collect::<Option<Vec<_>>>()?;
    Some(StreamingAggregatePlan::Scalar { calls, specs })
}

/// Finalize each scalar spec's streamed partial state into the aggregate's
/// SQL-visible value, in spec order.
fn finalize_scalar_streaming_aggregates(
    states: Vec<Vec<ScannedRow>>,
    specs: &[crate::PartialAggregateSpec],
) -> Result<Vec<Datum>, ExecError> {
    states
        .into_iter()
        .zip(specs)
        .map(|(state, spec)| {
            let finalized = crate::scanner::finalize_partial_aggregate_rows(state, spec)?;
            let [ScannedRow { row, .. }] = finalized.as_slice() else {
                return Err(invalid_scalar_aggregate_shape());
            };
            let [value] = row.as_slice() else {
                return Err(invalid_scalar_aggregate_shape());
            };
            Ok(value.clone())
        })
        .collect()
}

fn invalid_scalar_aggregate_shape() -> ExecError {
    ExecError::Unsupported("scalar partial aggregate produced an invalid merged shape".into())
}

/// Match a FROM that is exactly one ordinary local base table.
///
/// CTE, view, virtual-catalog, sharded, and foreign relations all resolve
/// through their own scan paths, so they deliberately return `None` here.
fn single_local_base_table(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    s: &SelectStmt,
    ctes: &crate::cte::CteContext,
) -> Result<Option<(Table, String)>, ExecError> {
    let [
        crabka_pgparser::ast::TableExpr::Table {
            name,
            alias,
            columns: None,
            sample: None,
            ..
        },
    ] = s.from.as_slice()
    else {
        return Ok(None);
    };
    if name.schema.is_none() && ctes.lookup(&name.name).is_some() {
        return Ok(None);
    }
    let name = &resolve_relation(catalog_kv, resolution, name, SchemaDisposition::Reference)?;
    if is_virtual_relation(name) {
        return Ok(None);
    }
    match crabka_pgcatalog::get_view(catalog_kv, name) {
        Ok(_) => return Ok(None),
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => {}
        Err(error) => return Err(error.into()),
    }
    let table = match crabka_pgcatalog::get_table(catalog_kv, name) {
        Ok(table) => table,
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    // A partitioned parent owns no rows of its own: the streaming fold would
    // read its empty row space and answer for the whole hierarchy.
    if table.sharded
        || table.foreign.is_some()
        || crate::partition::is_partitioned(catalog_kv, name)?
    {
        return Ok(None);
    }
    let qualifier = alias.clone().unwrap_or_else(|| table.name.name.clone());
    Ok(Some((table, qualifier)))
}

/// Match a FROM that is exactly one sharded base table.
///
/// CTE, view, virtual-catalog, local, and foreign relations all resolve
/// through their own scan paths, so they deliberately return `None` here, and
/// so does a relation that does not exist at all, whose undefined-table
/// error surfaces from the materializing path instead.
fn single_sharded_base_table(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    s: &SelectStmt,
    ctes: &crate::cte::CteContext,
) -> Result<Option<(Table, String)>, ExecError> {
    let [
        crabka_pgparser::ast::TableExpr::Table {
            name,
            alias,
            columns: None,
            sample: None,
            ..
        },
    ] = s.from.as_slice()
    else {
        return Ok(None);
    };
    if name.schema.is_none() && ctes.lookup(&name.name).is_some() {
        return Ok(None);
    }
    let name = &resolve_relation(catalog_kv, resolution, name, SchemaDisposition::Reference)?;
    if is_virtual_relation(name) {
        return Ok(None);
    }
    match crabka_pgcatalog::get_view(catalog_kv, name) {
        Ok(_) => return Ok(None),
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => {}
        Err(error) => return Err(error.into()),
    }
    let table = match crabka_pgcatalog::get_table(catalog_kv, name) {
        Ok(table) => table,
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !table.sharded || table.foreign.is_some() {
        return Ok(None);
    }
    let qualifier = alias.clone().unwrap_or_else(|| table.name.name.clone());
    Ok(Some((table, qualifier)))
}

fn is_plain_partial_aggregate_select(s: &SelectStmt) -> bool {
    if !select_modifiers_allow_partial_aggregate(s) {
        return false;
    }
    let Some(SelectItem::Expr {
        expr: Expr::Func(call),
        ..
    }) = s.projection.last()
    else {
        return false;
    };
    if call.distinct {
        return false;
    }
    matches!(call.name.as_str(), "count" | "sum" | "avg" | "min" | "max")
        && matches!(&call.args, FuncArgs::Star | FuncArgs::Exprs(_))
}

/// Cheap AST pre-filter for the local streaming path: the modifier shape the
/// partial-aggregate model supports, every projection item an expression, and
/// an aggregate call somewhere in the projection. The per-item decomposition in
/// `local_streaming_aggregate_plan` does the precise streamability check.
fn is_streamable_aggregate_select(s: &SelectStmt) -> bool {
    select_modifiers_allow_partial_aggregate(s)
        && s.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => crate::agg::contains_aggregate(expr),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
        })
}

/// No `DISTINCT` / `HAVING` / `LIMIT` / `OFFSET`, and an ORDER BY only as the
/// exact ascending echo of the GROUP BY key (the order the grouped partial
/// fold already produces).
fn select_modifiers_allow_partial_aggregate(s: &SelectStmt) -> bool {
    // A window query needs every row the WHERE kept, unaggregated: the window
    // node runs above the grouping, so no partial-aggregate scan may replace it.
    if crate::window::has_window_calls(s) {
        return false;
    }
    // A grouping-set clause folds each row into several groups; the partial
    // aggregate model has one group key per row, so it cannot express it.
    if s.grouping.is_some() {
        return false;
    }
    if s.distinct.dedups() || s.having.is_some() || s.limit.is_some() || s.offset.is_some() {
        return false;
    }
    s.order_by.is_empty()
        || (s.order_by.len() == s.group_by.len()
            && s.order_by
                .iter()
                .zip(&s.group_by)
                .all(|(order, group)| order.asc && order.expr == *group))
}

fn top_k_pushdown_for_select(
    table: &Table,
    qualifier: &str,
    s: &SelectStmt,
) -> Result<Option<crate::TopKSpec>, ExecError> {
    // A select-list set-returning function expands each source row into many, so
    // a LIMIT pushed onto the SOURCE scan would cut rows the expansion still owes.
    if !table.sharded
        || !is_top_k_candidate(s)
        || crate::srf::projection_contains_srf(&s.projection)
    {
        return Ok(None);
    }
    if crate::plan_dist::strict_predicate_for_filter(table, s.filter.as_ref()).is_err() {
        return Ok(None);
    }
    let scope = Scope::single(table, qualifier);
    let (fields, out_exprs, _tys) = resolve_projection(&s.projection, &scope)?;
    let mut order_by = Vec::with_capacity(s.order_by.len());
    for order_item in &s.order_by {
        let order_key = resolve_select_order_key(order_item, &scope, &fields, &out_exprs, false)?;
        let Some(column) = top_k_column_index(&scope, &out_exprs, &order_key)? else {
            return Ok(None);
        };
        let order_column = &table.columns[column];
        if !order_column.not_null || !is_top_k_column_type_supported(order_column.ty) {
            return Ok(None);
        }
        order_by.push(crate::TopKColumn {
            column,
            asc: order_item.asc,
        });
    }
    let limit = u64::try_from(constant_limit(s).expect("candidate has a positive limit"))
        .map_err(|_| ExecError::Unsupported("top-k LIMIT is outside u64 range".into()))?;
    Ok(Some(crate::TopKSpec { order_by, limit }))
}

fn is_top_k_candidate(s: &SelectStmt) -> bool {
    // A window function sees the rows before LIMIT, so a top-k scan that stops
    // early would change its result.
    if crate::window::has_window_calls(s) {
        return false;
    }
    if s.distinct.dedups()
        || !s.group_by.is_empty()
        || s.having.is_some()
        || s.offset.is_some()
        || s.with_ties
        || crate::agg::is_aggregate_query(s)
        || s.order_by.is_empty()
    {
        return false;
    }
    constant_limit(s).is_some_and(|limit| limit > 0)
}

/// The `LIMIT` as a plain integer constant, or `None` when it is absent or is an
/// expression. Scan pushdown runs before evaluation, so only a literal count can
/// bound the scan; anything else keeps the full-scan path and is applied to the
/// materialized rows.
fn constant_limit(s: &SelectStmt) -> Option<i64> {
    match s.limit.as_ref()? {
        Expr::IntLiteral(text) => text.parse().ok(),
        _ => None,
    }
}

fn top_k_column_index(
    scope: &Scope,
    out_exprs: &[Expr],
    order_key: &SelectOrderKey,
) -> Result<Option<usize>, ExecError> {
    let expr = match order_key {
        SelectOrderKey::Output(index) => &out_exprs[*index],
        SelectOrderKey::SourceExpr(expr) => expr,
    };
    let Expr::Column {
        table: qualifier,
        name,
    } = expr
    else {
        return Ok(None);
    };
    let column = scope.resolve(qualifier.as_deref(), name)?;
    Ok(Some(column))
}

fn is_top_k_column_type_supported(ty: ColumnType) -> bool {
    matches!(ty, ColumnType::Int4 | ColumnType::Int8 | ColumnType::Text)
}

pub(crate) fn table_uses_global_visibility(table: &Table) -> bool {
    table.sharded
}

/// Refuse `SHARDED BY HASH (col)` on a column whose values the shard-key hasher
/// cannot turn into bytes, at CREATE TABLE rather than at every INSERT.
///
/// A missing hash column is *not* reported here, because the catalog's own
/// validation raises the undefined-column error for that.
fn ensure_hash_shard_key_types_are_supported(
    columns: &[Column],
    sharding: Option<&crabka_pgcatalog::ShardingStrategy>,
) -> Result<(), ExecError> {
    let Some(crabka_pgcatalog::ShardingStrategy::Hash(hash)) = sharding else {
        return Ok(());
    };
    for column in hash
        .columns
        .iter()
        .filter_map(|name| columns.iter().find(|column| column.name == *name))
    {
        if !hash_shard_key_type_is_supported(column.ty) {
            return Err(ExecError::Unsupported(format!(
                "hash shard key column \"{}\" of type {} is not supported",
                column.name,
                column.ty.name()
            )));
        }
    }
    Ok(())
}

/// The column types [`hash_bucket_for_row`] can hash: those stored as an
/// `Int4`, `Int8`, `Text`, or `Bytea` datum. Everything else would fail on the
/// write path, so a table is never created with such a key: `boolean`,
/// `double precision`, `numeric`, the date/time types, `jsonb`, and arrays.
fn hash_shard_key_type_is_supported(ty: ColumnType) -> bool {
    matches!(
        ty,
        ColumnType::Int4
            | ColumnType::Int8
            | ColumnType::Text
            | ColumnType::Varchar(_)
            | ColumnType::Char(_)
            | ColumnType::Bytea
            | ColumnType::Uuid
            | ColumnType::Regclass
    )
}

fn hash_sharding_from_ast(
    sharding: &crabka_pgparser::ast::ShardingSpec,
) -> Result<crabka_pgcatalog::ShardingStrategy, ExecError> {
    match sharding {
        crabka_pgparser::ast::ShardingSpec::Hash(hash) => {
            // Redundant for SQL input: the grammar refuses a `SHARDED BY HASH`
            // list of any length but one outright (42601), so this never fires
            // for a parsed statement. It is the gate for the callers that build
            // the AST directly, and it matches the arity the row encoder in
            // [`hash_bucket_for_row`] has an encoding for — one column, the
            // only arity that agrees with the route the gateway computes.
            if hash.columns.len() != 1 {
                return Err(ExecError::Unsupported(
                    "hash sharding requires exactly one column".into(),
                ));
            }
            if hash.buckets == 0 || !hash.buckets.is_power_of_two() {
                return Err(ExecError::Unsupported(
                    "hash sharding bucket count must be a power of two".into(),
                ));
            }
            Ok(crabka_pgcatalog::ShardingStrategy::Hash(
                crabka_pgcatalog::HashSharding {
                    columns: hash.columns.clone(),
                    buckets: hash.buckets,
                    co_location_group: hash.co_location_group.clone(),
                },
            ))
        }
    }
}

/// Run a SELECT to a `Relation` with an already-evaluated CTE scope. `WITH`
/// belongs to `QueryExpr`; this function handles the SELECT body under that scope.
pub(crate) fn select_to_relation_with_ctes(
    read_ctx: &crate::subquery::SubCtx<'_>,
    s: &SelectStmt,
) -> Result<Relation, ExecError> {
    let catalog_kv = read_ctx.catalog_kv;
    let resolution = read_ctx.fctx.resolution;
    let ctes = read_ctx.ctes;
    let ctx = read_ctx.eval_ctx;
    let fctx = read_ctx.fctx;
    reject_nested_relation_locking(s)?;

    // SP34: resolve this (sub)query's uncorrelated subquery expressions to constants
    // first, under the same snapshot handles. Nested subqueries recurse here.
    let resolved = crate::subquery::resolve_in_select(read_ctx, s)?;
    let s = &resolved;
    crate::window::reject_misplaced_calls(s)?;
    crate::grouping::reject_misplaced_calls(s)?;
    let relation = if s.from.is_empty() {
        reject_from_less_wildcard(&s.projection)?;
        Relation {
            scope: Scope::empty(),
            rows: vec![vec![]],
        }
    } else {
        // SP40 Task 14: when the FROM is EXACTLY one foreign base table, extract
        // `_partition`/`_offset` bounds from the WHERE and push them into the
        // scan. The WHERE is still applied below, so this only ever reads less —
        // it never changes the result set.
        let pushed = if is_single_foreign_table(catalog_kv, &s.from, ctes, fctx) {
            Some(extract_scan_bounds(s.filter.as_ref()))
        } else {
            None
        };
        if let Some(relation) = try_execute_partial_aggregate_pushdown(read_ctx, s)? {
            return Ok(relation);
        }
        if let Some(relation) = try_execute_local_streaming_aggregate(read_ctx, s)? {
            return Ok(relation);
        }
        let scan_plan = match s.from.as_slice() {
            [
                crabka_pgparser::ast::TableExpr::Table {
                    name,
                    alias,
                    columns: None,
                    sample: None,
                    ..
                },
            ] if (name.schema.is_none() && ctes.lookup(&name.name).is_none())
                && scan_plan_table(catalog_kv, resolution, name)?.is_some() =>
            {
                let table = scan_plan_table(catalog_kv, resolution, name)?
                    .expect("the guard just resolved this relation");
                let mut plan =
                    crate::plan_dist::plan_scan(&table, s.filter.as_ref(), &s.projection);
                plan.projection = crate::ProjectionPushdown::All;
                plan.partial_aggregate = None;
                let qualifier = alias.as_deref().unwrap_or(&table.name.name);
                plan.top_k = top_k_pushdown_for_select(&table, qualifier, s)?;
                Some(plan)
            }
            _ => None,
        };
        build_from(
            read_ctx,
            &s.from,
            pushed.as_ref(),
            scan_plan.as_ref(),
            s.filter.as_ref(),
        )?
    };
    let mut kept = Vec::new();
    for row in &relation.rows {
        if row_matches(s.filter.as_ref(), &relation.scope, row, ctx)? {
            kept.push(row.clone());
        }
    }
    // Window functions run above WHERE/GROUP BY/HAVING and below DISTINCT/ORDER
    // BY/LIMIT, so they own the whole projection shape for the queries that use
    // them (including the grouped ones).
    let (fields, out_exprs, tys) = if crate::window::has_window_calls(s) {
        let (fields, tys, rows) = crate::window::execute(s, &relation.scope, kept, ctx)?;
        return Ok(Relation {
            scope: projected_scope(&fields, &tys),
            rows,
        });
    } else {
        resolve_projection(&s.projection, &relation.scope)?
    };
    let out_scope = projected_scope(&fields, &tys);
    let rows = if crate::grouping::is_grouping_query(s) {
        crate::srf::reject_in_aggregate(&out_exprs)?;
        crate::grouping::aggregate_rows(s, &relation.scope, kept, ctx)?
    } else {
        project_rows_ordered(s, &relation.scope, &fields, &out_exprs, kept, ctx)?
    };
    Ok(Relation {
        scope: out_scope,
        rows,
    })
}

/// A FROM item with any lateral reference to `outer` replaced by a NULL of that
/// column's type, so a schema-only describe can resolve it. Non-lateral items
/// pass through untouched.
fn lateral_schema_item(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    ctes: &crate::cte::CteContext,
    te: &crabka_pgparser::ast::TableExpr,
    outer: &Scope,
) -> crabka_pgparser::ast::TableExpr {
    if !is_lateral_item(te, outer) {
        return te.clone();
    }
    let nulls = vec![Datum::Null; outer.width()];
    LateralBinder::new(catalog_kv, resolution, ctes)
        .bind(te, outer, &nulls)
        .0
}

pub(crate) fn build_from_schema_with_ctes(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    from: &[crabka_pgparser::ast::TableExpr],
    ctes: &crate::cte::CteContext,
) -> Result<Relation, ExecError> {
    build_from_schema_with_ctes_and_context(catalog_kv, resolution, from, ctes, None)
}

pub(crate) fn build_from_schema_with_ctes_and_context(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    from: &[crabka_pgparser::ast::TableExpr],
    ctes: &crate::cte::CteContext,
    ctx: Option<&crate::clock::EvalCtx>,
) -> Result<Relation, ExecError> {
    let mut iter = from.iter();
    let first = iter
        .next()
        .ok_or_else(|| ExecError::Unsupported("build_from_schema on empty FROM".into()))?;
    let mut acc = build_table_expr_schema_with_ctes(catalog_kv, resolution, first, ctes, ctx)?;
    for te in iter {
        // A lateral item references the accumulated columns, which no schema
        // description of it on its own can resolve. Substituting NULLs of the
        // right types leaves an item the ordinary describe understands and whose
        // output columns are unchanged.
        let te = &lateral_schema_item(catalog_kv, resolution, ctes, te, &acc.scope);
        let next = build_table_expr_schema_with_ctes(catalog_kv, resolution, te, ctes, ctx)?;
        // Schema-only: no rows, so no ON predicate is ever evaluated — a default
        // (UTC/epoch) eval context is correct here.
        acc = join_relations(
            acc,
            next,
            crabka_pgparser::ast::JoinKind::Cross,
            &crabka_pgparser::ast::JoinConstraint::None,
            &crate::clock::EvalCtx::test_default(),
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )?;
    }
    Ok(acc)
}

fn build_table_expr_schema_with_ctes(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    te: &crabka_pgparser::ast::TableExpr,
    ctes: &crate::cte::CteContext,
    ctx: Option<&crate::clock::EvalCtx>,
) -> Result<Relation, ExecError> {
    use crabka_pgparser::ast::TableExpr;
    match te {
        TableExpr::Table {
            name,
            alias,
            columns,
            ..
        } => {
            if let Some(names) = columns {
                let base = build_table_expr_schema_with_ctes(
                    catalog_kv,
                    resolution,
                    &TableExpr::Table {
                        name: name.clone(),
                        only: false,
                        alias: alias.clone(),
                        columns: None,
                        sample: None,
                    },
                    ctes,
                    ctx,
                )?;
                let qualifier = alias.clone().unwrap_or_else(|| name.to_string());
                return crate::values::requalify_derived(base, &qualifier, &Some(names.clone()));
            }
            if name.schema.is_none()
                && let Some(rel) = ctes.lookup(&name.name)
            {
                let qualifier = alias.as_deref().unwrap_or(&name.name);
                let mut rel = crate::cte::requalify_cte(rel, qualifier);
                rel.rows.clear();
                return Ok(rel);
            }
            if name.schema.is_none()
                && let Some(runtime) = ctx.and_then(|ctx| ctx.transition_relations.as_ref())
                && let Some(transition) = runtime
                    .lock()
                    .expect("transition relation mutex")
                    .get(&name.name)
                    .cloned()
            {
                let qualifier = alias.as_deref().unwrap_or(&name.name);
                return Ok(Relation {
                    scope: Scope {
                        columns: transition
                            .columns
                            .into_iter()
                            .map(|(name, ty)| ColumnBinding {
                                qualifier: Some(qualifier.to_string()),
                                name,
                                ty,
                            })
                            .collect(),
                    },
                    rows: Vec::new(),
                });
            }
            let name =
                &resolve_relation(catalog_kv, resolution, name, SchemaDisposition::Reference)?;
            if let Some(rel) = virtual_catalog_relation_schema(name, alias.as_deref()) {
                return Ok(rel);
            }
            match crabka_pgcatalog::get_view(catalog_kv, name) {
                Ok(view) => {
                    let qualifier = alias.as_deref().unwrap_or(&view.name.name);
                    return Ok(Relation {
                        scope: Scope {
                            columns: view
                                .columns
                                .iter()
                                .map(|column| ColumnBinding {
                                    qualifier: Some(qualifier.to_string()),
                                    name: column.name.clone(),
                                    ty: column.ty,
                                })
                                .collect(),
                        },
                        rows: Vec::new(),
                    });
                }
                Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => {}
                Err(error) => return Err(error.into()),
            }
            let t = crabka_pgcatalog::get_table(catalog_kv, name)?;
            let qualifier = alias.as_deref().unwrap_or(&t.name.name);
            Ok(Relation {
                scope: Scope::single(&t, qualifier),
                rows: Vec::new(),
            })
        }
        TableExpr::Join {
            left,
            right,
            kind,
            constraint,
        } => {
            let l = build_table_expr_schema_with_ctes(catalog_kv, resolution, left, ctes, ctx)?;
            let right = &lateral_schema_item(catalog_kv, resolution, ctes, right, &l.scope);
            let r = build_table_expr_schema_with_ctes(catalog_kv, resolution, right, ctes, ctx)?;
            // Schema-only: no rows, so no ON predicate is ever evaluated.
            join_relations(
                l,
                r,
                *kind,
                constraint,
                &crate::clock::EvalCtx::test_default(),
                crate::scanner::BLOCKING_QUERY_MEMORY,
            )
        }
        TableExpr::Derived {
            subquery,
            alias,
            columns,
            ..
        } => {
            let fields = crate::query::describe_query_expr_with_ctes(
                catalog_kv, resolution, subquery, ctes,
            )?;
            let bindings = fields
                .iter()
                .map(|f| {
                    Ok(ColumnBinding {
                        qualifier: None,
                        name: f.name.clone(),
                        ty: column_type_from_oid(f.type_oid)?,
                    })
                })
                .collect::<Result<_, ExecError>>()?;
            let inner = Relation {
                scope: Scope { columns: bindings },
                rows: Vec::new(),
            };
            crate::values::requalify_derived(inner, alias, columns)
        }
        TableExpr::Function {
            functions,
            with_ordinality,
            alias,
            column_aliases,
            ..
        } => {
            if functions.len() == 1
                && let Some((_routine, columns)) =
                    crate::routine::plpgsql_table_function_schema(catalog_kv, &functions[0])?
            {
                return crate::srf::user_function_relation(
                    &functions[0].name,
                    columns,
                    Vec::new(),
                    *with_ordinality,
                    alias.as_deref(),
                    column_aliases,
                );
            }
            crate::srf::from_item_schema(
                functions,
                *with_ordinality,
                alias.as_deref(),
                column_aliases,
            )
        }
    }
}

fn requalify_view_relation(
    mut relation: Relation,
    view: &crabka_pgcatalog::View,
    qualifier: &str,
) -> Result<Relation, ExecError> {
    if relation.scope.width() != view.columns.len() {
        return Err(ExecError::Unsupported(
            "stored view definition does not match its catalog schema".into(),
        ));
    }
    for (binding, column) in relation.scope.columns.iter_mut().zip(&view.columns) {
        binding.qualifier = Some(qualifier.to_string());
        binding.name.clone_from(&column.name);
        binding.ty = column.ty;
    }
    Ok(relation)
}

pub(crate) const PG_CATALOG_NAMESPACE_OID: i32 = 11;
pub(crate) const INFORMATION_SCHEMA_NAMESPACE_OID: i32 = 13_370;
pub(crate) const PUBLIC_NAMESPACE_OID: i32 = 2200;
pub(crate) const CURRENT_DATABASE: &str = "postgres";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BuiltinTypeRow {
    oid: i32,
    name: &'static str,
    len: i16,
    category: &'static str,
    /// `pg_type.typelem`: the element type of an array type, 0 for a scalar.
    elem: i32,
    /// `pg_type.typarray`: the array type over a scalar, 0 for an array type
    /// and for the scalars crabka has no array type for (`varchar`, `char(n)`,
    /// `regclass`). [`crabka_pgtypes::ElemType::from_column_type`] refuses
    /// those, so a pointer at an absent row would be worse than a report of
    /// none.
    array: i32,
}

fn virtual_catalog_relation(
    catalog_kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
    alias: Option<&str>,
    ctx: &crate::clock::EvalCtx,
) -> Result<Option<Relation>, ExecError> {
    let Some(table) = virtual_table(&virtual_lookup_key(name)) else {
        return Ok(None);
    };
    let rows = virtual_catalog_rows(catalog_kv, table, ctx)?;
    Ok(Some(Relation {
        scope: Scope::single(&virtual_catalog_table(table), alias.unwrap_or(&name.name)),
        rows,
    }))
}

fn virtual_catalog_relation_schema(
    name: &crabka_pgcatalog::RelationName,
    alias: Option<&str>,
) -> Option<Relation> {
    let key = virtual_lookup_key(name);
    let table = virtual_table(&key)?;
    Some(Relation {
        scope: Scope::single(&virtual_catalog_table(table), alias.unwrap_or(&name.name)),
        rows: Vec::new(),
    })
}

/// The ordinary local relation a single-item `FROM` names, or `None` when it is
/// anything a scan plan cannot be built over (a foreign table, or no relation
/// at all).
fn scan_plan_table(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    reference: &crabka_pgparser::ast::RelationRef,
) -> Result<Option<Table>, ExecError> {
    let name = resolve_relation(
        catalog_kv,
        resolution,
        reference,
        SchemaDisposition::Reference,
    )?;
    Ok(crabka_pgcatalog::get_table(catalog_kv, &name)
        .ok()
        .filter(|table| table.foreign.is_none()))
}

/// The key the virtual catalog relations are held under: a bare name for
/// anything in `pg_catalog`, and `schema.name` elsewhere. This is how
/// `information_schema.tables` has always been spelled here.
fn virtual_lookup_key(name: &crabka_pgcatalog::RelationName) -> String {
    if name.schema == crate::search_path::PG_CATALOG {
        name.name.clone()
    } else {
        format!("{}.{}", name.schema, name.name)
    }
}

/// True when `name` denotes a relation the engine synthesises rather than
/// stores. The resolver consults this alongside the catalog, so an unqualified
/// `pg_class` finds the catalog relation through the implicit `pg_catalog`
/// entry exactly as `PostgreSQL` does. This includes the case where a user
/// relation of the same name exists in `public`, which the oracle confirms does
/// not shadow it.
pub(crate) fn is_virtual_relation(name: &crabka_pgcatalog::RelationName) -> bool {
    virtual_table(&virtual_lookup_key(name)).is_some()
}

fn virtual_table(name: &str) -> Option<&'static str> {
    match name.strip_prefix("pg_catalog.").unwrap_or(name) {
        "pg_namespace" => Some("pg_namespace"),
        "pg_class" => Some("pg_class"),
        "pg_attribute" => Some("pg_attribute"),
        "pg_type" => Some("pg_type"),
        "pg_ts_config" => Some("pg_ts_config"),
        "pg_ts_dict" => Some("pg_ts_dict"),
        "pg_range" => Some("pg_range"),
        "pg_index" => Some("pg_index"),
        "pg_settings" => Some("pg_settings"),
        "pg_prepared_statements" => Some("pg_prepared_statements"),
        "pg_roles" => Some("pg_roles"),
        "pg_user" => Some("pg_user"),
        _ => None,
    }
    .or_else(|| match name.strip_prefix("information_schema.")? {
        "schemata" => Some("information_schema.schemata"),
        "tables" => Some("information_schema.tables"),
        "columns" => Some("information_schema.columns"),
        "triggers" => Some("information_schema.triggers"),
        "triggered_update_columns" => Some("information_schema.triggered_update_columns"),
        _ => None,
    })
    // F-2 owns the rest of the `psql`/ORM introspection surface.
    .or_else(|| crate::catalog_rel::catalog_relation(name))
}

fn virtual_catalog_table(name: &str) -> Table {
    Table {
        id: virtual_relation_oid(name) as u32,
        name: crabka_pgcatalog::RelationName::new(
            virtual_relation_schema(name),
            virtual_relation_name(name),
        ),
        columns: virtual_catalog_columns(name),
        sharded: false,
        sharding: None,
        foreign: None,
        checks: Vec::new(),
    }
}

fn virtual_catalog_columns(name: &str) -> Vec<Column> {
    use ColumnType::{Bool, Int2, Int4, Int8, Text, Timestamptz};
    match name {
        "pg_namespace" => cols(&[
            ("oid", Int4),
            ("nspname", Text),
            // `\dn` reads `nspowner` through `pg_get_userbyid`.
            ("nspowner", Int4),
            ("nspacl", ColumnType::Array(crabka_pgtypes::ElemType::Text)),
        ]),
        // PostgreSQL 18.4's column set, in catalog order: `psql`'s `\d` reads
        // relpersistence/relreplident/relchecks/relhasrules/relhastriggers/
        // relrowsecurity/relispartition/reloftype/reltablespace/relam by name,
        // and ORMs positionally `SELECT *`.
        "pg_class" => pg_class_columns(),
        "pg_attribute" => pg_attribute_columns(),
        "pg_type" => cols(&[
            ("oid", Int4),
            ("typname", Text),
            ("typlen", Int4),
            ("typcategory", Text),
            ("typnamespace", Int4),
            ("typrelid", Int4),
            // `typtype` is `"char"` (OID 18) in PostgreSQL; Text is the
            // closest synthesized type, same trade-off as `typcategory`.
            ("typtype", Text),
            // `typdelim` is `"char"` in PostgreSQL too. It is the element
            // separator of the array text literal — ',' for every type crabka
            // exposes — and drivers read it when parsing array output.
            ("typdelim", Text),
            // `typelem`/`typarray` are the two halves of the scalar ↔ array
            // link a driver walks to recognize an array OID and to find the
            // array type of a scalar.
            ("typelem", Int4),
            ("typarray", Int4),
            ("typbasetype", Int4),
        ]),
        "pg_ts_config" => cols(&[
            ("oid", Int4),
            ("cfgname", Text),
            ("cfgnamespace", Int4),
            ("cfgowner", Int4),
            ("cfgparser", Int4),
        ]),
        "pg_ts_dict" => cols(&[
            ("oid", Int4),
            ("dictname", Text),
            ("dictnamespace", Int4),
            ("dictowner", Int4),
            ("dicttemplate", Int4),
            ("dictinitoption", Text),
        ]),
        // PostgreSQL 18 column set, in catalog order. The oid-valued columns
        // use Int4 like every other synthesized catalog oid; the two regproc
        // columns use Text (regproc renders as a function name).
        "pg_range" => cols(&[
            ("rngtypid", Int4),
            ("rngsubtype", Int4),
            ("rngmultitypid", Int4),
            ("rngcollation", Int4),
            ("rngsubopc", Int4),
            ("rngcanonical", Text),
            ("rngsubdiff", Text),
        ]),
        // PostgreSQL 18.4's column set, in catalog order. `\d <table>` reads
        // indisclustered/indisvalid/indisreplident by name and joins
        // `indexrelid`/`indrelid` against `pg_class`.
        "pg_index" => cols(&[
            ("indexrelid", Int4),
            ("indrelid", Int4),
            ("indnatts", Int2),
            ("indnkeyatts", Int2),
            ("indisunique", Bool),
            ("indnullsnotdistinct", Bool),
            ("indisprimary", Bool),
            ("indisexclusion", Bool),
            ("indimmediate", Bool),
            ("indisclustered", Bool),
            ("indisvalid", Bool),
            ("indcheckxmin", Bool),
            ("indisready", Bool),
            ("indislive", Bool),
            ("indisreplident", Bool),
            ("indkey", Text),
            ("indcollation", Text),
            ("indclass", Text),
            ("indoption", Text),
            ("indexprs", Text),
            ("indpred", Text),
        ]),
        "pg_prepared_statements" => cols(&[
            ("name", Text),
            ("statement", Text),
            ("prepare_time", Timestamptz),
            ("parameter_types", Text),
            ("result_types", Text),
            ("from_sql", Bool),
            ("generic_plans", Int8),
            ("custom_plans", Int8),
        ]),
        "pg_settings" => cols(&[
            ("name", Text),
            ("setting", Text),
            ("unit", Text),
            ("category", Text),
            ("short_desc", Text),
            ("context", Text),
            ("vartype", Text),
            ("source", Text),
            ("min_val", Text),
            ("max_val", Text),
            ("enumvals", Text),
            ("boot_val", Text),
            ("reset_val", Text),
            ("pending_restart", Bool),
        ]),
        // PostgreSQL 18.4's column set, in catalog order — `\du` projects
        // rolinherit/rolconnlimit/rolvaliduntil positionally after the flags.
        "pg_roles" => cols(&[
            ("rolname", Text),
            ("rolsuper", Bool),
            ("rolinherit", Bool),
            ("rolcreaterole", Bool),
            ("rolcreatedb", Bool),
            ("rolcanlogin", Bool),
            ("rolreplication", Bool),
            ("rolconnlimit", Int4),
            ("rolpassword", Text),
            ("rolvaliduntil", Timestamptz),
            ("rolbypassrls", Bool),
            (
                "rolconfig",
                ColumnType::Array(crabka_pgtypes::ElemType::Text),
            ),
            ("oid", Int4),
        ]),
        "pg_user" => cols(&[("usename", Text), ("usesuper", Bool), ("usecreatedb", Bool)]),
        // The full standard projection, in PostgreSQL 18.4's column order. The
        // three `default_character_set_*` columns and `sql_path` are NULL in
        // PostgreSQL too — the standard defines them, PostgreSQL fills none.
        "information_schema.schemata" => cols(&[
            ("catalog_name", Text),
            ("schema_name", Text),
            ("schema_owner", Text),
            ("default_character_set_catalog", Text),
            ("default_character_set_schema", Text),
            ("default_character_set_name", Text),
            ("sql_path", Text),
        ]),
        "information_schema.tables" => cols(&[
            ("table_catalog", Text),
            ("table_schema", Text),
            ("table_name", Text),
            ("table_type", Text),
        ]),
        "information_schema.columns" => cols(&[
            ("table_schema", Text),
            ("table_name", Text),
            ("column_name", Text),
            ("ordinal_position", Int4),
            ("data_type", Text),
            ("is_nullable", Text),
            ("column_default", Text),
        ]),
        "information_schema.triggers" => cols(&[
            ("trigger_catalog", Text),
            ("trigger_schema", Text),
            ("trigger_name", Text),
            ("event_manipulation", Text),
            ("event_object_catalog", Text),
            ("event_object_schema", Text),
            ("event_object_table", Text),
            ("action_order", Int4),
            ("action_condition", Text),
            ("action_statement", Text),
            ("action_orientation", Text),
            ("action_timing", Text),
            ("action_reference_old_table", Text),
            ("action_reference_new_table", Text),
            ("action_reference_old_row", Text),
            ("action_reference_new_row", Text),
            ("created", Timestamptz),
        ]),
        "information_schema.triggered_update_columns" => cols(&[
            ("trigger_catalog", Text),
            ("trigger_schema", Text),
            ("trigger_name", Text),
            ("event_object_catalog", Text),
            ("event_object_schema", Text),
            ("event_object_table", Text),
            ("event_object_column", Text),
        ]),
        _ => crate::catalog_rel::columns(name),
    }
}

fn cols(defs: &[(&str, ColumnType)]) -> Vec<Column> {
    defs.iter()
        .map(|(name, ty)| Column::new(*name, *ty))
        .collect()
}

fn pg_class_columns() -> Vec<Column> {
    use ColumnType::{Array, Bool, Float4, Int2, Int4, Int8, Text};
    cols(&[
        ("oid", Int4),
        ("relname", Text),
        ("relnamespace", Int4),
        ("reltype", Int4),
        ("reloftype", Int4),
        ("relowner", Int4),
        ("relam", Int4),
        ("relfilenode", Int4),
        ("reltablespace", Int4),
        ("relpages", Int4),
        ("reltuples", Float4),
        ("relallvisible", Int4),
        ("relallfrozen", Int4),
        ("reltoastrelid", Int4),
        ("relhasindex", Bool),
        ("relisshared", Bool),
        ("relpersistence", Text),
        ("relkind", Text),
        ("relnatts", Int2),
        ("relchecks", Int2),
        ("relhasrules", Bool),
        ("relhastriggers", Bool),
        ("relhassubclass", Bool),
        ("relrowsecurity", Bool),
        ("relforcerowsecurity", Bool),
        ("relispopulated", Bool),
        ("relreplident", Text),
        ("relispartition", Bool),
        ("relrewrite", Int4),
        ("relfrozenxid", Int8),
        ("relminmxid", Int8),
        ("relacl", Array(crabka_pgtypes::ElemType::Text)),
        ("reloptions", Array(crabka_pgtypes::ElemType::Text)),
        ("relpartbound", Text),
    ])
}

fn pg_attribute_columns() -> Vec<Column> {
    use ColumnType::{Array, Bool, Int2, Int4, Text};
    cols(&[
        ("attrelid", Int4),
        ("attname", Text),
        ("atttypid", Int4),
        ("attlen", Int2),
        ("attnum", Int2),
        ("atttypmod", Int4),
        ("attndims", Int2),
        ("attbyval", Bool),
        ("attalign", Text),
        ("attstorage", Text),
        ("attcompression", Text),
        ("attnotnull", Bool),
        ("atthasdef", Bool),
        ("atthasmissing", Bool),
        ("attidentity", Text),
        ("attgenerated", Text),
        ("attisdropped", Bool),
        ("attislocal", Bool),
        ("attinhcount", Int2),
        ("attcollation", Int4),
        ("attstattarget", Int2),
        ("attacl", Array(crabka_pgtypes::ElemType::Text)),
        ("attoptions", Array(crabka_pgtypes::ElemType::Text)),
        ("attfdwoptions", Array(crabka_pgtypes::ElemType::Text)),
        ("attmissingval", Array(crabka_pgtypes::ElemType::Text)),
    ])
}

fn virtual_catalog_rows(
    catalog_kv: &dyn Kv,
    name: &str,
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    match name {
        "pg_namespace" => pg_namespace_rows(catalog_kv),
        "pg_class" => pg_class_rows(catalog_kv),
        "pg_attribute" => pg_attribute_rows(catalog_kv),
        "pg_type" => Ok(pg_type_rows()),
        "pg_ts_config" => text_search_catalog_rows(
            catalog_kv,
            crabka_pgparser::ast::TextSearchObjectKind::Configuration,
        ),
        "pg_ts_dict" => text_search_catalog_rows(
            catalog_kv,
            crabka_pgparser::ast::TextSearchObjectKind::Dictionary,
        ),
        // Zero rows: no built-in type in the exposed scalar slice is a range
        // type. Drivers still LEFT JOIN it in their typeinfo queries.
        "pg_range" => Ok(Vec::new()),
        "pg_index" => pg_index_rows(catalog_kv),
        "pg_settings" => pg_settings_rows(),
        "pg_prepared_statements" => pg_prepared_statement_rows(),
        "pg_roles" => pg_roles_rows(catalog_kv),
        "pg_user" => pg_user_rows(catalog_kv),
        "information_schema.schemata" => information_schema_schemata_rows(catalog_kv),
        "information_schema.tables" => information_schema_tables_rows(catalog_kv, ctx.backend_pid),
        "information_schema.columns" => {
            information_schema_columns_rows(catalog_kv, ctx.backend_pid)
        }
        "information_schema.triggers" => information_schema_trigger_rows(catalog_kv),
        "information_schema.triggered_update_columns" => {
            information_schema_triggered_update_column_rows(catalog_kv)
        }
        "pg_inherits" => pg_inherits_rows(catalog_kv),
        "pg_partitioned_table" => pg_partitioned_table_rows(catalog_kv),
        _ => crate::catalog_rel::rows(catalog_kv, name, ctx.backend_pid),
    }
}

/// `pg_inherits`: one row per partition, naming its direct parent.
///
/// A partition is always its parent's only inheritance step, so `inhseqno` is
/// 1 and `inhdetachpending` false. The concurrent-detach flag has no state to
/// report here, because detach is a single catalog batch.
fn pg_inherits_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(catalog_kv)? {
        let mut parents = crate::inheritance::parents_of(catalog_kv, &table.name)?;
        if let Some((parent, _)) = crate::partition::parent_of(catalog_kv, &table.name)? {
            parents.push(parent);
        }
        for (index, parent) in parents.into_iter().enumerate() {
            let parent = crabka_pgcatalog::get_table(catalog_kv, &parent)?;
            rows.push(vec![
                int(oid_i32(table.id)?),
                int(oid_i32(parent.id)?),
                int(i32::try_from(index + 1).unwrap_or(i32::MAX)),
                Datum::Bool(false),
            ]);
        }
    }
    Ok(rows)
}

/// `pg_partitioned_table`: one row per partitioned parent.
fn pg_partitioned_table_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(catalog_kv)? {
        let Some(scheme) = crate::partition::scheme_of(catalog_kv, &table.name)? else {
            continue;
        };
        let natts = i16::try_from(scheme.keys.len())
            .map_err(|_| ExecError::Unsupported("partnatts exceeds int2 range".into()))?;
        // `partattrs` is an int2vector, printed as a space-separated list of
        // one-based attribute numbers.
        let attrs = scheme
            .keys
            .iter()
            .map(|key| (key.ordinal + 1).to_string())
            .collect::<Vec<_>>()
            .join(" ");
        rows.push(vec![
            int(oid_i32(table.id)?),
            text(scheme.strategy.code()),
            Datum::Int2(natts),
            int(0),
            text(&attrs),
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ]);
    }
    Ok(rows)
}

/// `pg_namespace`: one row per schema the catalog holds. This adds nothing, so
/// a schema appears exactly once and a dropped one not at all.
fn pg_namespace_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crabka_pgcatalog::list_schemas(catalog_kv)?
        .into_iter()
        .map(|schema| {
            vec![
                int(crate::catalog_rel::namespace_oid(&schema.name)),
                text(&schema.name),
                int(schema_owner_oid(&schema.owner)),
                Datum::Null,
            ]
        })
        .collect())
}

/// The `pg_authid.oid` a schema's owner projects as. `public` belongs to the
/// implicit `pg_database_owner` role; every other schema projects the bootstrap
/// superuser, because trust auth makes every session that user and crabka has
/// no ownership model to distinguish them by.
fn schema_owner_oid(owner: &str) -> i32 {
    if owner == crabka_pgcatalog::PUBLIC_SCHEMA_OWNER {
        crate::catalog_fn::DATABASE_OWNER_ROLE_OID
    } else {
        crate::catalog_fn::BOOTSTRAP_ROLE_OID
    }
}

/// Every relation crabka has, in the `relkind` PostgreSQL would report: user
/// tables `r`, foreign tables `f`, views `v`, sequences `S`, indexes `i`, and
/// the virtual catalog relations `v`. `psql`'s `\dt`/`\dv`/`\di`/`\ds` differ
/// only in the `relkind` they filter on, so all four need this one list.
fn pg_class_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let triggered_relation_ids = crabka_pgcatalog::trigger::list_triggers(catalog_kv)?
        .into_iter()
        .map(|trigger| trigger.table_id)
        .collect::<std::collections::HashSet<_>>();
    let indexes = crabka_pgcatalog::list_indexes(catalog_kv)?;
    let indexed_table_ids = indexes
        .iter()
        .map(|index| index.table_id)
        .collect::<std::collections::BTreeSet<_>>();
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(catalog_kv)? {
        let partitioned = crate::partition::is_partitioned(catalog_kv, &table.name)?;
        let relkind = match (table.foreign.is_some(), partitioned) {
            (true, _) => "f",
            (false, true) => "p",
            (false, false) => "r",
        };
        let mut row = PgClassRow::new(
            oid_i32(table.id)?,
            &table.name.name,
            relkind,
            crate::catalog_rel::namespace_oid(&table.name.schema),
        );
        row.relnatts = table.columns.len();
        row.relchecks = table.checks.len();
        row.relhasindex = indexed_table_ids.contains(&table.id);
        row.relhastriggers = triggered_relation_ids.contains(&table.id);
        row.relam = crate::catalog_rel::BTREE_AM_OID;
        row.relispartition = crate::partition::parent_of(catalog_kv, &table.name)?.is_some();
        row.relpersistence = crabka_pgcatalog::relpersistence_of(&table.name.schema);
        row.reltablespace = crabka_pgcatalog::relation_tablespace_oid(catalog_kv, &table.name)?;
        rows.push(row.build()?);
    }
    for view in crabka_pgcatalog::list_views(catalog_kv)? {
        let oid = crate::catalog_rel::view_oids(catalog_kv)?
            .get(&view.name)
            .copied()
            .unwrap_or(0);
        let mut row = PgClassRow::new(
            oid,
            &view.name.name,
            "v",
            crate::catalog_rel::namespace_oid(&view.name.schema),
        );
        row.relnatts = view.columns.len();
        row.relhasrules = true;
        row.relhastriggers =
            u32::try_from(oid).is_ok_and(|oid| triggered_relation_ids.contains(&oid));
        row.relpersistence = crabka_pgcatalog::relpersistence_of(&view.name.schema);
        rows.push(row.build()?);
    }
    for (name, _) in crabka_pgcatalog::list_sequences(catalog_kv)? {
        let oid = crate::catalog_rel::sequence_oids(catalog_kv)?
            .get(&name)
            .copied()
            .unwrap_or(0);
        let mut row = PgClassRow::new(
            oid,
            &name.name,
            "S",
            crate::catalog_rel::namespace_oid(&name.schema),
        );
        row.relpersistence = crabka_pgcatalog::relpersistence_of(&name.schema);
        rows.push(row.build()?);
    }
    for virtual_table in virtual_table_names() {
        let table = virtual_catalog_table(virtual_table);
        let mut row = PgClassRow::new(
            virtual_relation_oid(virtual_table),
            &table.name.name,
            "v",
            virtual_relation_namespace_oid(virtual_table),
        );
        row.relnatts = table.columns.len();
        rows.push(row.build()?);
    }
    for index in indexes {
        // An index lives in the schema of the table it indexes, which is also
        // what makes a temporary table's index temporary.
        let mut row = PgClassRow::new(
            catalog_index_oid(index.id)?,
            &index.name,
            "i",
            crate::catalog_rel::namespace_oid(&index.table.schema),
        );
        row.relnatts = index.columns.len();
        row.relam = match index.method {
            crabka_pgcatalog::IndexMethod::Btree => crate::catalog_rel::BTREE_AM_OID,
            crabka_pgcatalog::IndexMethod::Hash => crate::catalog_rel::HASH_AM_OID,
            crabka_pgcatalog::IndexMethod::Gist => crate::catalog_rel::GIST_AM_OID,
            crabka_pgcatalog::IndexMethod::Gin => crate::catalog_rel::GIN_AM_OID,
            crabka_pgcatalog::IndexMethod::Spgist => crate::catalog_rel::SPGIST_AM_OID,
        };
        row.relpersistence = crabka_pgcatalog::relpersistence_of(&index.table.schema);
        row.reltablespace =
            crabka_pgcatalog::relation_tablespace_oid(catalog_kv, &index.qualified_name())?;
        rows.push(row.build()?);
    }
    Ok(rows)
}

/// The handful of `pg_class` fields that actually vary between crabka's
/// relation kinds. Everything else in the row is the same constant for all of
/// them, and [`PgClassRow::build`] writes it.
#[allow(clippy::struct_excessive_bools)]
struct PgClassRow<'a> {
    oid: i32,
    relname: &'a str,
    relkind: &'a str,
    relnamespace: i32,
    relnatts: usize,
    relchecks: usize,
    relhasindex: bool,
    relhasrules: bool,
    relhastriggers: bool,
    relam: i32,
    relispartition: bool,
    reltablespace: u32,
    /// `p` for an ordinary relation, `t` for one in a session's temporary
    /// namespace. That is where every temporary relation is, so the schema is
    /// the whole fact and nothing stores it twice.
    relpersistence: char,
}

impl<'a> PgClassRow<'a> {
    fn new(oid: i32, relname: &'a str, relkind: &'a str, relnamespace: i32) -> Self {
        Self {
            oid,
            relname,
            relkind,
            relnamespace,
            relnatts: 0,
            relchecks: 0,
            relhasindex: false,
            relhasrules: false,
            relhastriggers: false,
            relam: 0,
            relispartition: false,
            reltablespace: 0,
            relpersistence: 'p',
        }
    }

    fn build(self) -> Result<Vec<Datum>, ExecError> {
        let natts = i16::try_from(self.relnatts)
            .map_err(|_| ExecError::Unsupported("relnatts exceeds int2 range".into()))?;
        let checks = i16::try_from(self.relchecks)
            .map_err(|_| ExecError::Unsupported("relchecks exceeds int2 range".into()))?;
        Ok(vec![
            int(self.oid),
            text(self.relname),
            int(self.relnamespace),
            int(0),
            int(0),
            int(crate::catalog_fn::BOOTSTRAP_ROLE_OID),
            int(self.relam),
            int(self.oid),
            int(i32::try_from(self.reltablespace)
                .map_err(|_| ExecError::Unsupported("tablespace oid exceeds int4".into()))?),
            int(0),
            Datum::Float4(-1.0),
            int(0),
            int(0),
            int(0),
            Datum::Bool(self.relhasindex),
            Datum::Bool(false),
            // Every crabka relation is populated and replica-identity
            // "default"; its persistence follows the schema holding it.
            text(&self.relpersistence.to_string()),
            text(self.relkind),
            Datum::Int2(natts),
            Datum::Int2(checks),
            Datum::Bool(self.relhasrules),
            Datum::Bool(self.relhastriggers),
            Datum::Bool(false),
            Datum::Bool(false),
            Datum::Bool(false),
            Datum::Bool(true),
            text("d"),
            Datum::Bool(self.relispartition),
            int(0),
            Datum::Int8(0),
            Datum::Int8(0),
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ])
    }
}

fn pg_attribute_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(catalog_kv)? {
        rows.extend(attribute_rows_for_table(oid_i32(table.id)?, &table)?);
    }
    for virtual_table in virtual_table_names() {
        let table = virtual_catalog_table(virtual_table);
        rows.extend(attribute_rows_for_table(
            virtual_relation_oid(virtual_table),
            &table,
        )?);
    }
    // A composite type's attributes hang off the relation its `pg_type.typrelid`
    // points at, which is how `\d <type>` and the driver introspection queries
    // reach them.
    for ty in crabka_pgtypes::usertype::all() {
        let Some(fields) = ty.fields() else { continue };
        let relid = i32::try_from(crabka_pgtypes::usertype::composite_relation_oid(ty.oid))
            .map_err(|_| ExecError::Unsupported("composite relation oid exceeds int4".into()))?;
        let table = crabka_pgcatalog::Table {
            id: 0,
            name: crabka_pgcatalog::RelationName::new(
                crate::search_path::PG_CATALOG,
                ty.name.clone(),
            ),
            columns: fields
                .iter()
                .map(|field| crabka_pgcatalog::Column::new(field.name.clone(), field.ty))
                .collect(),
            sharded: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        };
        rows.extend(attribute_rows_for_table(relid, &table)?);
    }
    Ok(rows)
}

/// The standard's view of the same schemas `pg_namespace` lists, so a schema
/// created here appears and a dropped `public` disappears. PostgreSQL builds
/// this view by joining `pg_namespace.nspowner` to `pg_authid`, so
/// `schema_owner` is exactly what `pg_get_userbyid(nspowner)` answers. The
/// character-set columns and `sql_path` are NULL there too.
fn information_schema_schemata_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crabka_pgcatalog::list_schemas(catalog_kv)?
        .into_iter()
        .map(|schema| {
            vec![
                text(CURRENT_DATABASE),
                text(&schema.name),
                text(schema_owner_name(&schema.owner)),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ]
        })
        .collect())
}

/// The role name behind [`schema_owner_oid`], so the two schema projections
/// cannot disagree about who owns a schema.
fn schema_owner_name(owner: &str) -> &'static str {
    if owner == crabka_pgcatalog::PUBLIC_SCHEMA_OWNER {
        crabka_pgcatalog::PUBLIC_SCHEMA_OWNER
    } else {
        crate::catalog_fn::OBJECT_OWNER
    }
}

/// True when `schema` is a temporary namespace belonging to some *other*
/// session. This is `PostgreSQL`'s `pg_is_other_temp_schema`, which its
/// `information_schema` views filter relations on.
///
/// `pg_class`, `pg_namespace` and `information_schema.schemata` do not filter:
/// on `postgres:18.4` another session's temporary relation is visible in
/// `pg_class` and its namespace in all three. Only the standard's relation
/// views hide it.
fn is_other_temp_schema(schema: &str, backend_id: i32) -> bool {
    crabka_pgcatalog::is_temp_schema(schema)
        && schema != crabka_pgcatalog::temp_schema_name(backend_id)
}

/// Every relation the SQL standard calls a table: base tables, foreign tables,
/// and (F-2) views, which `table_type = 'VIEW'` distinguishes.
fn information_schema_tables_rows(
    catalog_kv: &dyn Kv,
    backend_id: i32,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = crabka_pgcatalog::list_tables(catalog_kv)?
        .into_iter()
        .filter(|table| !is_other_temp_schema(&table.name.schema, backend_id))
        .map(|table| {
            information_schema_table_row(
                &table.name,
                if table.foreign.is_some() {
                    "FOREIGN"
                } else {
                    "BASE TABLE"
                },
            )
        })
        .collect::<Vec<_>>();
    rows.extend(
        crabka_pgcatalog::list_views(catalog_kv)?
            .into_iter()
            .filter(|view| !is_other_temp_schema(&view.name.schema, backend_id))
            .map(|view| information_schema_table_row(&view.name, "VIEW")),
    );
    Ok(rows)
}

fn information_schema_table_row(
    name: &crabka_pgcatalog::RelationName,
    table_type: &str,
) -> Vec<Datum> {
    vec![
        text(CURRENT_DATABASE),
        text(&name.schema),
        text(&name.name),
        text(table_type),
    ]
}

fn information_schema_columns_rows(
    catalog_kv: &dyn Kv,
    backend_id: i32,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(catalog_kv)? {
        if is_other_temp_schema(&table.name.schema, backend_id) {
            continue;
        }
        for (idx, column) in table.columns.iter().enumerate() {
            rows.push(vec![
                text(&table.name.schema),
                text(&table.name.name),
                text(&column.name),
                int(usize_i32(idx + 1)?),
                // PostgreSQL reports the literal string `ARRAY` here for every
                // array column (the element type lives in `udt_name`, which
                // this synthesized view does not expose).
                text(match column.ty {
                    ColumnType::Array(_) => "ARRAY",
                    ty => ty.name(),
                }),
                text(if column.not_null { "NO" } else { "YES" }),
                column_default_datum(catalog_kv, column),
            ]);
        }
    }
    Ok(rows)
}

fn information_schema_trigger_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    use crabka_pgcatalog::trigger::{TriggerLevel, TriggerTiming};
    let triggers = crabka_pgcatalog::trigger::list_triggers(catalog_kv)?;
    let mut rows = Vec::new();
    for trigger in triggers.iter().filter(|trigger| !trigger.is_internal) {
        let function = crate::routine::routine_by_oid(
            catalog_kv,
            i32::try_from(trigger.function_oid).unwrap_or(0),
        )?
        .map_or_else(|| trigger.function.clone(), |routine| routine.name);
        let events = [
            (trigger.events.insert, "INSERT"),
            (trigger.events.update, "UPDATE"),
            (trigger.events.delete, "DELETE"),
            (trigger.events.truncate, "TRUNCATE"),
        ];
        for (_, event) in events.into_iter().filter(|(enabled, _)| *enabled) {
            let action_order = triggers
                .iter()
                .filter(|candidate| {
                    candidate.table_id == trigger.table_id
                        && candidate.timing == trigger.timing
                        && candidate.level == trigger.level
                        && candidate.name <= trigger.name
                        && match event {
                            "INSERT" => candidate.events.insert,
                            "UPDATE" => candidate.events.update,
                            "DELETE" => candidate.events.delete,
                            _ => candidate.events.truncate,
                        }
                })
                .count();
            let arguments = trigger
                .arguments
                .iter()
                .map(|argument| format!("'{}'", argument.replace(char::from(39), "''")))
                .collect::<Vec<_>>()
                .join(", ");
            rows.push(vec![
                text(CURRENT_DATABASE),
                text(&trigger.table.schema),
                text(&trigger.name),
                text(event),
                text(CURRENT_DATABASE),
                text(&trigger.table.schema),
                text(&trigger.table.name),
                Datum::Int4(i32::try_from(action_order).unwrap_or(i32::MAX)),
                trigger
                    .when
                    .as_ref()
                    .map_or(Datum::Null, |value| text(value)),
                text(&format!("EXECUTE FUNCTION {function}({arguments})")),
                text(match trigger.level {
                    TriggerLevel::Row => "ROW",
                    TriggerLevel::Statement => "STATEMENT",
                }),
                text(match trigger.timing {
                    TriggerTiming::Before => "BEFORE",
                    TriggerTiming::After => "AFTER",
                    TriggerTiming::InsteadOf => "INSTEAD OF",
                }),
                trigger
                    .old_transition
                    .as_ref()
                    .map_or(Datum::Null, |value| text(value)),
                trigger
                    .new_transition
                    .as_ref()
                    .map_or(Datum::Null, |value| text(value)),
                if trigger.level == TriggerLevel::Row {
                    text("OLD")
                } else {
                    Datum::Null
                },
                if trigger.level == TriggerLevel::Row {
                    text("NEW")
                } else {
                    Datum::Null
                },
                Datum::Null,
            ]);
        }
    }
    Ok(rows)
}

fn information_schema_triggered_update_column_rows(
    catalog_kv: &dyn Kv,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for trigger in crabka_pgcatalog::trigger::list_triggers(catalog_kv)?
        .into_iter()
        .filter(|trigger| !trigger.is_internal && trigger.events.update)
    {
        for column in &trigger.events.update_columns {
            rows.push(vec![
                text(CURRENT_DATABASE),
                text(&trigger.table.schema),
                text(&trigger.name),
                text(CURRENT_DATABASE),
                text(&trigger.table.schema),
                text(&trigger.table.name),
                text(column),
            ]);
        }
    }
    Ok(rows)
}

fn column_default_datum(catalog_kv: &dyn Kv, column: &Column) -> Datum {
    let Some(default) = &column.default else {
        return Datum::Null;
    };
    text(&format_column_default(catalog_kv, default, column.ty))
}

fn format_column_default(catalog_kv: &dyn Kv, default: &ColumnDefault, ty: ColumnType) -> String {
    match default {
        ColumnDefault::NextVal(sequence) => {
            format!("nextval('{}'::regclass)", escape_sql_string(sequence))
        }
        // Only the oid is stored, so the name is read from the catalog now —
        // the same output-time resolution `pg_get_expr` performs.
        ColumnDefault::Value(Datum::Regclass(value)) => {
            let resolved = regclass_by_oid(catalog_kv, value.oid)
                .unwrap_or_else(|_| crabka_pgtypes::RegclassValue::unresolved(value.oid));
            format!("'{}'::{}", escape_sql_string(&resolved.name), ty.name())
        }
        ColumnDefault::Value(value) => format_default_value(value, ty),
    }
}

fn format_default_value(value: &Datum, ty: ColumnType) -> String {
    match value {
        Datum::Null => "NULL".to_string(),
        Datum::Bool(true) => "true".to_string(),
        Datum::Bool(false) => "false".to_string(),
        Datum::Int2(value) => value.to_string(),
        Datum::Int4(value) => value.to_string(),
        Datum::Int8(value) => value.to_string(),
        // Both float widths render through their own output function so a
        // `real` default reads back as PostgreSQL spells it (`1e+06`, not
        // `1000000`).
        Datum::Float4(_) | Datum::Float8(_) => String::from_utf8(
            crabka_pgtypes::encoding::encode_text(value, &jiff::tz::TimeZone::UTC),
        )
        .expect("a Datum's text encoding is always valid UTF-8"),
        Datum::Numeric(value) => value.to_string(),
        Datum::Text(value) => {
            let mut out = String::new();
            let _ = write!(out, "'{}'::{}", escape_sql_string(value), ty.name());
            out
        }
        // A jsonb/array default renders like PostgreSQL's `pg_get_expr` output:
        // the value's own text, quoted and cast to the column type.
        Datum::Jsonb(_)
        | Datum::Array(_)
        | Datum::OidVector(_)
        | Datum::Range(_)
        | Datum::Multirange(_)
        | Datum::TsVector(_)
        | Datum::TsQuery(_) => {
            match zone_independent_text(value) {
                Some(literal) => {
                    let mut out = String::new();
                    let _ = write!(out, "'{}'::{}", escape_sql_string(&literal), ty.name());
                    out
                }
                None => "<unsupported>".to_string(),
            }
        }
        Datum::Date(_)
        | Datum::Point(_)
        | Datum::Path(_)
        | Datum::Time(_)
        | Datum::Timetz(_)
        | Datum::Timestamp(_)
        | Datum::Timestamptz(_)
        | Datum::Interval(_)
        | Datum::Record(_)
        | Datum::Enum(_)
        // A `regclass` default is rendered by `format_column_default`, which has
        // the catalog handle its name needs.
        | Datum::Regclass(_)
        | Datum::Bytea(_) => "<unsupported>".to_string(),
    }
}

/// The output text of a value whose rendering does not depend on the session
/// time zone, for the catalog's default-expression rendering (which has no
/// session context). `None` for a `timestamptz` array element, the one case a
/// jsonb/array value can be zone-dependent.
fn zone_independent_text(value: &Datum) -> Option<String> {
    fn zone_dependent(value: &Datum) -> bool {
        match value {
            Datum::Timestamptz(_) => true,
            Datum::Array(array) => array.elems.iter().any(zone_dependent),
            Datum::Range(range) => range
                .lower
                .iter()
                .chain(&range.upper)
                .any(|bound| zone_dependent(bound)),
            _ => false,
        }
    }
    if zone_dependent(value) {
        return None;
    }
    String::from_utf8(crabka_pgtypes::encoding::encode_text(
        value,
        &jiff::tz::TimeZone::UTC,
    ))
    .ok()
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

/// PostgreSQL 18.4's `pg_attribute` row per column. `attidentity` and
/// `attgenerated` carry the empty string for an ordinary column, which is what
/// PostgreSQL stores and what `\d`'s "Generated"/"Identity" columns test.
fn attribute_rows_for_table(relid: i32, table: &Table) -> Result<Vec<Vec<Datum>>, ExecError> {
    table
        .columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let attnum = i16::try_from(idx + 1)
                .map_err(|_| ExecError::Unsupported("attnum exceeds int2 range".into()))?;
            let identity = match column.identity {
                Some(crabka_pgcatalog::IdentityKind::Always) => "a",
                Some(crabka_pgcatalog::IdentityKind::ByDefault) => "d",
                None => "",
            };
            Ok(vec![
                int(relid),
                text(&column.name),
                int(oid_i32(column.ty.oid())?),
                Datum::Int2(column.ty.type_size()),
                Datum::Int2(attnum),
                int(catalog_typmod(column.ty)),
                Datum::Int2(i16::from(matches!(column.ty, ColumnType::Array(_)))),
                Datum::Bool(column.ty.type_size() > 0),
                text("i"),
                text("x"),
                text(""),
                Datum::Bool(column.not_null),
                Datum::Bool(column.default.is_some()),
                Datum::Bool(false),
                text(identity),
                text(if column.generated.is_some() { "s" } else { "" }),
                Datum::Bool(false),
                Datum::Bool(true),
                Datum::Int2(0),
                int(text_collation_oid(column.ty)),
                Datum::Int2(-1),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ])
        })
        .collect()
}

/// `pg_attribute.atttypmod`. [`ColumnType::typmod`] covers the string types;
/// `numeric(p, s)` needs PostgreSQL's packed `((p << 16) | s) + 4` too, because
/// `format_type(atttypid, atttypmod)` reconstructs `numeric(10,2)` from exactly
/// that word. That is how `\d` and every ORM print a column's type.
fn catalog_typmod(ty: ColumnType) -> i32 {
    match ty {
        ColumnType::Numeric(Some(typmod)) => {
            (i32::from(typmod.precision) << 16 | i32::from(typmod.scale)) + 4
        }
        other => other.typmod(),
    }
}

/// `attcollation`: the database default collation for a collatable type, 0 for
/// everything else, the exact test `\d`'s collation column makes.
fn text_collation_oid(ty: ColumnType) -> i32 {
    if matches!(
        ty,
        ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Char(_)
    ) {
        crate::catalog_rel::DEFAULT_COLLATION_OID
    } else {
        0
    }
}

fn pg_type_rows() -> Vec<Vec<Datum>> {
    let mut rows: Vec<Vec<Datum>> = builtin_type_rows()
        .iter()
        .map(|ty| {
            vec![
                int(ty.oid),
                text(ty.name),
                int(i32::from(ty.len)),
                text(ty.category),
                int(PG_CATALOG_NAMESPACE_OID),
                int(0),
                // Every exposed built-in — scalar or array — is a base type
                // ('b') with no domain base type, matching PostgreSQL 18's
                // pg_type for these OIDs. Only `box` uses a typdelim other
                // than ',', and crabka has no geometric types.
                text(if ty.name.ends_with("multirange") {
                    "m"
                } else if ty.category == "R" {
                    "r"
                } else {
                    "b"
                }),
                text(","),
                int(ty.elem),
                int(ty.array),
                int(0),
            ]
        })
        .collect();
    rows.extend(user_type_rows());
    rows
}

fn text_search_catalog_rows(
    kv: &dyn Kv,
    kind: crabka_pgparser::ast::TextSearchObjectKind,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crate::text_search_catalog::catalog_rows(kv, kind)?
        .into_iter()
        .map(|(name, base)| {
            let mut hash = 2_166_136_261u32;
            for byte in name.bytes() {
                hash = (hash ^ u32::from(byte)).wrapping_mul(16_777_619);
            }
            let oid = i32::try_from(60_000 + hash % 1_000_000).expect("bounded oid");
            match kind {
                crabka_pgparser::ast::TextSearchObjectKind::Configuration => vec![
                    Datum::Int4(oid),
                    Datum::Text(name),
                    Datum::Int4(PG_CATALOG_NAMESPACE_OID),
                    Datum::Int4(10),
                    Datum::Int4(3722),
                ],
                crabka_pgparser::ast::TextSearchObjectKind::Dictionary => vec![
                    Datum::Int4(oid),
                    Datum::Text(name),
                    Datum::Int4(PG_CATALOG_NAMESPACE_OID),
                    Datum::Int4(10),
                    Datum::Int4(3727),
                    if base.is_empty() {
                        Datum::Null
                    } else {
                        Datum::Text(base)
                    },
                ],
            }
        })
        .collect())
}

/// The `pg_type` rows of the `CREATE TYPE`/`CREATE DOMAIN` types.
///
/// `typrelid` of a composite is the derived `pg_class` oid its attributes hang
/// off (`pg_attribute` uses the same derivation), and `typbasetype` of a domain
/// is the base type's oid. Those are the two columns `\d` and every driver's
/// type introspection walk.
fn user_type_rows() -> Vec<Vec<Datum>> {
    use crabka_pgtypes::usertype;
    usertype::all()
        .into_iter()
        .map(|ty| {
            let column_type = ty.column_type();
            let (typrelid, typbasetype, category) = match &ty.body {
                usertype::UserTypeBody::Composite(_) => (
                    i32::try_from(usertype::composite_relation_oid(ty.oid)).unwrap_or(0),
                    0,
                    "C",
                ),
                usertype::UserTypeBody::Enum(_) => (0, 0, "E"),
                usertype::UserTypeBody::Range(_) => (0, 0, "R"),
                usertype::UserTypeBody::Domain(domain) => (
                    0,
                    i32::try_from(domain.base.oid()).unwrap_or(0),
                    builtin_type_category(domain.base),
                ),
            };
            vec![
                int(i32::try_from(ty.oid).unwrap_or(0)),
                text(&ty.name),
                int(i32::from(column_type.type_size())),
                text(category),
                int(PUBLIC_NAMESPACE_OID),
                int(typrelid),
                text(ty.typtype()),
                text(","),
                int(0),
                int(0),
                int(typbasetype),
            ]
        })
        .collect()
}

/// The `pg_type.typcategory` of a built-in type, for the domain rows that
/// inherit their base type's category.
fn builtin_type_category(base: crabka_pgtypes::ColumnType) -> &'static str {
    builtin_type_rows()
        .iter()
        .find(|row| u32::try_from(row.oid) == Ok(base.oid()))
        .map_or("U", |row| row.category)
}

fn pg_index_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    crabka_pgcatalog::list_indexes(catalog_kv)?
        .into_iter()
        .map(|index| {
            let table = crabka_pgcatalog::get_table(catalog_kv, &index.table)?;
            let indkey = index
                .columns
                .iter()
                .map(|column| {
                    table
                        .column_index(column)
                        .map(|idx| (idx + 1).to_string())
                        .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))
                })
                .collect::<Result<Vec<_>, _>>()?
                .join(" ");
            let natts = i16::try_from(index.columns.len())
                .map_err(|_| ExecError::Unsupported("indnatts exceeds int2 range".into()))?;
            Ok(vec![
                int(catalog_index_oid(index.id)?),
                int(oid_i32(index.table_id)?),
                Datum::Int2(natts),
                Datum::Int2(natts),
                Datum::Bool(index.unique),
                Datum::Bool(false),
                // The catalog knows which index backs the primary key; ORMs
                // introspecting for upserts key off exactly this column.
                Datum::Bool(
                    index.constraint == Some(crabka_pgcatalog::IndexConstraint::PrimaryKey),
                ),
                Datum::Bool(false),
                Datum::Bool(true),
                Datum::Bool(false),
                // Every crabka index is valid, ready and live the moment it is
                // in the catalog: there is no concurrent-build state.
                Datum::Bool(true),
                Datum::Bool(false),
                Datum::Bool(true),
                Datum::Bool(true),
                Datum::Bool(false),
                text(&indkey),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ])
        })
        .collect()
}

fn pg_settings_rows() -> Result<Vec<Vec<Datum>>, ExecError> {
    crate::session::guc_settings_runtime()?
        .into_iter()
        .map(|setting| {
            let optional = |value: Option<&String>| value.map_or(Datum::Null, |value| text(value));
            Ok(vec![
                text(&setting.name),
                text(&setting.value),
                optional(setting.unit.as_ref()),
                text("Client Connection Defaults / Statement Behavior"),
                text("Crabka session parameter"),
                text(&setting.context),
                text(&setting.vartype),
                text("session"),
                optional(setting.min_val.as_ref()),
                optional(setting.max_val.as_ref()),
                optional(setting.enumvals.as_ref()),
                text(&setting.boot_val),
                text(&setting.reset_val),
                Datum::Bool(false),
            ])
        })
        .collect()
}

/// S2: `pg_catalog.pg_prepared_statements` over the session's prepared
/// statements. `parameter_types`/`result_types` are rendered as `PostgreSQL`
/// renders a `regtype[]` literal.
fn pg_prepared_statement_rows() -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crate::session::prepared_statement_runtime()?
        .into_iter()
        .map(|prepared| {
            vec![
                text(&prepared.name),
                text(&prepared.statement),
                Datum::Null,
                text(&prepared.parameter_types),
                text(&prepared.result_types),
                Datum::Bool(prepared.from_sql),
                Datum::Int8(0),
                Datum::Int8(1),
            ]
        })
        .collect())
}

fn pg_roles_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let oids = crate::catalog_rel::role_oids(catalog_kv)?;
    Ok(crabka_pgcatalog::list_roles(catalog_kv)?
        .into_iter()
        .map(|role| {
            let superuser = role.name == crate::catalog_fn::OBJECT_OWNER;
            vec![
                text(&role.name),
                Datum::Bool(superuser),
                Datum::Bool(true),
                Datum::Bool(superuser),
                Datum::Bool(superuser),
                Datum::Bool(role.can_login),
                Datum::Bool(false),
                int(-1),
                // PostgreSQL blanks the password in `pg_roles` (only
                // `pg_authid` holds it, and only a superuser may read that).
                text("********"),
                Datum::Null,
                Datum::Bool(superuser),
                Datum::Null,
                int(oids.get(&role.name).copied().unwrap_or(0)),
            ]
        })
        .collect())
}

fn pg_user_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crabka_pgcatalog::list_roles(catalog_kv)?
        .into_iter()
        .filter(|role| role.can_login)
        .map(|role| vec![text(&role.name), Datum::Bool(false), Datum::Bool(false)])
        .collect())
}

/// Every virtual relation, `exec`'s starter set followed by the F-2
/// introspection surface. `pg_class`/`pg_attribute` describe themselves through
/// this list, so a relation missing from it is invisible to `\d`.
pub(crate) fn virtual_table_names() -> &'static [&'static str] {
    static NAMES: std::sync::LazyLock<Vec<&'static str>> = std::sync::LazyLock::new(|| {
        let mut names = vec![
            "pg_namespace",
            "pg_class",
            "pg_attribute",
            "pg_type",
            "pg_ts_config",
            "pg_ts_dict",
            "pg_range",
            "pg_index",
            "pg_settings",
            "pg_roles",
            "pg_user",
            "information_schema.schemata",
            "information_schema.tables",
            "information_schema.columns",
            "information_schema.triggers",
            "information_schema.triggered_update_columns",
        ];
        names.extend_from_slice(crate::catalog_rel::relation_names());
        names
    });
    &NAMES
}

fn virtual_relation_name(name: &str) -> &str {
    name.rsplit_once('.').map_or(name, |(_, relation)| relation)
}

/// The schema a synthesised catalog relation lives in: `information_schema`
/// for the SQL-standard views, `pg_catalog` for everything else.
fn virtual_relation_schema(name: &str) -> &'static str {
    if name.starts_with("information_schema.") {
        "information_schema"
    } else {
        crate::search_path::PG_CATALOG
    }
}

fn virtual_relation_namespace_oid(name: &str) -> i32 {
    if name.starts_with("information_schema.") {
        INFORMATION_SCHEMA_NAMESPACE_OID
    } else {
        PG_CATALOG_NAMESPACE_OID
    }
}

/// Resolve a `regclass` relation name to its `pg_class` oid: virtual catalog
/// relations use their fixed oids; user tables use their catalog table id (the
/// same value `pg_class_rows` reports). An optional `pg_catalog.` / `public.`
/// qualifier is accepted like PostgreSQL's search path would.
///
/// # Errors
///
/// Propagates the catalog's undefined-table error (42P01) for an unknown
/// relation name, matching PostgreSQL's `relation "..." does not exist`.
pub(crate) fn resolve_regclass(
    catalog_kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    name: &str,
) -> Result<i32, ExecError> {
    crate::catalog_fn::resolve_relation_in_scope(catalog_kv, scope, name)
}

/// The `regclass` value for a relation oid: the oid paired with the name
/// `regclassout` prints for it. An oid no relation has is not an error in
/// PostgreSQL. It keeps the fallback rendering, `-` for `InvalidOid` and the
/// bare number otherwise, which [`RegclassValue::unresolved`] supplies.
pub(crate) fn regclass_by_oid(
    catalog_kv: &dyn Kv,
    oid: i32,
) -> Result<crabka_pgtypes::RegclassValue, ExecError> {
    Ok(
        crate::catalog_fn::relation_name_by_oid(catalog_kv, oid)?.map_or_else(
            || crabka_pgtypes::RegclassValue::unresolved(oid),
            |name| crabka_pgtypes::RegclassValue::resolved(oid, &name),
        ),
    )
}

/// Whether a column of this type holds a `regclass` value: the type itself, or
/// a domain over it, whose values *are* the base type's values.
fn holds_regclass(ty: ColumnType) -> bool {
    match ty {
        ColumnType::Regclass => true,
        ColumnType::Domain(domain) => holds_regclass(*domain.base),
        _ => false,
    }
}

/// Re-attach the relation name to every `regclass` a scan just decoded.
///
/// The row encoding stores a `regclass` as its bare oid, which is all
/// PostgreSQL keeps on disk too, so a decoded value arrives as a
/// `Datum::Int4`. PostgreSQL
/// consults the catalog in `regclassout`; crabka cannot, because the text
/// encoder and the `→ text` cast both live in a crate with no catalog handle.
/// The scan is the last point where the catalog *is* in scope, so the name is
/// attached here and travels with the value, exactly as the `::regclass` cast
/// arranges for a value that never touched storage.
///
/// Resolving from the catalog on the way out rather than storing the name is
/// what makes an already-stored value follow a `RENAME` and fall back to the
/// bare oid once its relation is dropped, which is what PostgreSQL does.
///
/// [`crate::catalog_fn::relation_name_by_oid`] walks the whole catalog, so the
/// lookup is memoized across the scan: a column holding one repeated oid costs
/// one lookup, not one per row. A table with no `regclass` column returns before
/// touching a row.
fn resolve_scanned_regclass(
    catalog_kv: &dyn Kv,
    table: &crabka_pgcatalog::Table,
    rows: &mut [Vec<Datum>],
) -> Result<(), ExecError> {
    resolve_regclass_at(catalog_kv, &regclass_column_indexes(table, 0), rows)
}

/// The positions of `table`'s `regclass`-valued columns within a scanned row
/// whose first column sits at `offset`. That offset is non-zero for a join
/// result, which concatenates one table's columns after another's.
fn regclass_column_indexes(table: &crabka_pgcatalog::Table, offset: usize) -> Vec<usize> {
    table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| holds_regclass(column.ty))
        .map(|(index, _)| index + offset)
        .collect()
}

/// The shared body of [`resolve_scanned_regclass`], over already-located
/// columns.
fn resolve_regclass_at(
    catalog_kv: &dyn Kv,
    columns: &[usize],
    rows: &mut [Vec<Datum>],
) -> Result<(), ExecError> {
    if columns.is_empty() {
        return Ok(());
    }
    let mut resolved: HashMap<i32, crabka_pgtypes::RegclassValue> = HashMap::new();
    for row in rows {
        for &index in columns {
            // A projection that dropped the column, or a NULL, leaves nothing to
            // resolve; a value already carrying its name is left alone.
            let Some(Datum::Int4(oid)) = row.get(index) else {
                continue;
            };
            let oid = *oid;
            let value = match resolved.entry(oid) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.get().clone(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(regclass_by_oid(catalog_kv, oid)?).clone()
                }
            };
            row[index] = Datum::Regclass(value);
        }
    }
    Ok(())
}

/// PostgreSQL's `regclassin`: an all-digit string is an oid, `-` is
/// `InvalidOid`, and anything else is a relation name the catalog resolves
/// (42P01 when it has none).
pub(crate) fn regclass_from_text(
    catalog_kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    text: &str,
) -> Result<Datum, ExecError> {
    let trimmed = text.trim();
    let oid = if trimmed == "-" {
        0
    } else {
        match trimmed.parse::<i32>() {
            Ok(oid) => oid,
            Err(_) => resolve_regclass(catalog_kv, scope, text)?,
        }
    };
    regclass_by_oid(catalog_kv, oid).map(Datum::Regclass)
}

/// The catalog-aware half of a `… :: regclass` cast. `None` for an operand the
/// catalog adds nothing to (NULL, an out-of-range `int8`), which then takes the
/// pure cast in [`crabka_pgtypes::cast`] and its error reporting.
pub(crate) fn regclass_cast(
    catalog_kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    value: &Datum,
) -> Result<Option<Datum>, ExecError> {
    let oid = match value {
        Datum::Text(text) => return regclass_from_text(catalog_kv, scope, text).map(Some),
        Datum::Int4(oid) => *oid,
        Datum::Int8(oid) => match i32::try_from(*oid) {
            Ok(oid) => oid,
            Err(_) => return Ok(None),
        },
        Datum::Regclass(value) => value.oid,
        _ => return Ok(None),
    };
    regclass_by_oid(catalog_kv, oid)
        .map(Datum::Regclass)
        .map(Some)
}

/// PostgreSQL `regtypein`/`regtypeout` using the existing built-in signature
/// map and user-type registry.
pub(crate) fn regtype_cast(value: &Datum) -> Result<Option<Datum>, ExecError> {
    let oid = match value {
        Datum::Text(text) => {
            let trimmed = text.trim();
            match trimmed.parse::<i32>() {
                Ok(oid) => oid,
                Err(_) => {
                    let name = trimmed
                        .strip_prefix("pg_catalog.")
                        .unwrap_or(trimmed)
                        .trim_matches('"')
                        .to_ascii_lowercase();
                    regtype_oid(&name)
                        .or_else(|| {
                            crabka_pgtypes::usertype::lookup(&name)
                                .and_then(|ty| i32::try_from(ty.oid).ok())
                        })
                        .ok_or_else(|| {
                            ExecError::UndefinedObject(format!("type \"{name}\" does not exist"))
                        })?
                }
            }
        }
        Datum::Int4(oid) => *oid,
        Datum::Int8(oid) => match i32::try_from(*oid) {
            Ok(oid) => oid,
            Err(_) => return Ok(None),
        },
        Datum::Regclass(value) => value.oid,
        _ => return Ok(None),
    };
    let name = regtype_name(oid);
    Ok(Some(Datum::Regclass(crabka_pgtypes::RegclassValue {
        oid,
        name: name.into(),
    })))
}

fn regtype_oid(name: &str) -> Option<i32> {
    crate::routine::TYPE_OIDS
        .iter()
        .find(|(candidate, _)| *candidate == name)
        .map(|(_, oid)| *oid)
}

fn regtype_name(oid: i32) -> String {
    crabka_pgtypes::usertype::lookup_oid(u32::try_from(oid).unwrap_or(0))
        .map(|ty| ty.name)
        .unwrap_or_else(|| {
            let formatted = crate::func::format_type(i64::from(oid), -1);
            if formatted == "-" {
                crate::routine::TYPE_OIDS
                    .iter()
                    .find(|(_, candidate_oid)| *candidate_oid == oid)
                    .map_or_else(|| oid.to_string(), |(name, _)| (*name).to_string())
            } else {
                formatted
            }
        })
}

/// PostgreSQL `regprocedurein`/`regprocedureout`, resolved from the same rows
/// exposed through `pg_proc`.
pub(crate) fn regprocedure_cast(
    catalog_kv: &dyn Kv,
    value: &Datum,
) -> Result<Option<Datum>, ExecError> {
    let rows = crate::routine::pg_proc_rows(catalog_kv)?;
    let oid = match value {
        Datum::Text(text) => match text.trim().parse::<i32>() {
            Ok(oid) => oid,
            Err(_) => {
                let written = text.trim();
                let Some((name, args)) = written.strip_suffix(')').and_then(|s| s.split_once('('))
                else {
                    return Err(ExecError::UndefinedFunction(format!(
                        "function \"{written}\" does not exist"
                    )));
                };
                let name = name
                    .rsplit_once('.')
                    .map_or(name, |(_, bare)| bare)
                    .trim_matches('"')
                    .to_ascii_lowercase();
                let arg_oids = if args.trim().is_empty() {
                    Vec::new()
                } else {
                    args.split(',')
                        .map(|arg| {
                            let arg = arg.trim().trim_matches('"').to_ascii_lowercase();
                            regtype_oid(&arg).ok_or_else(|| {
                                ExecError::UndefinedObject(format!("type \"{arg}\" does not exist"))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?
                };
                rows.iter()
                    .find(|row| {
                        row.get(1) == Some(&Datum::Text(name.clone()))
                            && matches!(
                                row.get(19),
                                Some(Datum::OidVector(arguments))
                                    if arguments.elems
                                        == arg_oids.iter().copied().map(Datum::Int4).collect::<Vec<_>>()
                            )
                    })
                    .and_then(|row| match row.first() {
                        Some(Datum::Int4(oid)) => Some(*oid),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        ExecError::UndefinedFunction(format!(
                            "function \"{written}\" does not exist"
                        ))
                    })?
            }
        },
        Datum::Int4(oid) => *oid,
        Datum::Int8(oid) => match i32::try_from(*oid) {
            Ok(oid) => oid,
            Err(_) => return Ok(None),
        },
        Datum::Regclass(value) => value.oid,
        _ => return Ok(None),
    };
    let name = rows
        .iter()
        .find(|row| row.first() == Some(&Datum::Int4(oid)))
        .and_then(|row| {
            let Datum::Text(name) = row.get(1)? else {
                return None;
            };
            let Datum::OidVector(arguments) = row.get(19)? else {
                return None;
            };
            Some(format!(
                "{}({})",
                name,
                arguments
                    .elems
                    .iter()
                    .filter_map(|arg| match arg {
                        Datum::Int4(oid) => Some(regtype_name(*oid)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            ))
        })
        .unwrap_or_else(|| oid.to_string());
    Ok(Some(Datum::Regclass(crabka_pgtypes::RegclassValue {
        oid,
        name: name.into(),
    })))
}

/// The base-table half of [`resolve_regclass`]: virtual catalog relations and
/// ordinary/foreign tables. [`crate::catalog_fn`] layers views, sequences and
/// indexes, the other three `pg_class` kinds, on top.
///
/// # Errors
///
/// Propagates the catalog's undefined-table error (42P01).
pub(crate) fn resolve_base_relation(
    catalog_kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
) -> Result<i32, ExecError> {
    let key = virtual_lookup_key(name);
    if virtual_table_names().contains(&key.as_str()) {
        return Ok(virtual_relation_oid(&key));
    }
    let table = crabka_pgcatalog::get_table(catalog_kv, name)?;
    oid_i32(table.id)
}

pub(crate) fn virtual_relation_oid(name: &str) -> i32 {
    match name {
        "pg_namespace" => 2615,
        "pg_class" => 1259,
        "pg_attribute" => 1249,
        "pg_type" => 1247,
        "pg_ts_config" => 3602,
        "pg_ts_dict" => 3600,
        "pg_range" => 3541,
        "pg_index" => 2610,
        "pg_settings" => 100_001,
        "pg_prepared_statements" => 100_003,
        "pg_roles" => 1261,
        "pg_user" => 100_002,
        "information_schema.schemata" => 100_010,
        "information_schema.tables" => 100_011,
        "information_schema.columns" => 100_012,
        "information_schema.triggers" => 100_013,
        "information_schema.triggered_update_columns" => 100_014,
        _ => crate::catalog_rel::relation_oid(name),
    }
}

/// The scalar built-in types crabka exposes. `array` is 0 for the three whose
/// array type crabka does not implement; every other one gets a generated
/// array row from [`builtin_type_rows`].
fn scalar_type_rows() -> &'static [BuiltinTypeRow] {
    &[
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::OIDVECTOR as i32,
            name: "oidvector",
            len: -1,
            category: "A",
            elem: crabka_pgtypes::oids::OID as i32,
            array: 1013,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::REGPROCEDURE as i32,
            name: "regprocedure",
            len: 4,
            category: "N",
            elem: 0,
            array: 2207,
        },
        BuiltinTypeRow {
            oid: 2205,
            name: "regclass",
            len: 4,
            category: "N",
            elem: 0,
            array: 0,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::REGTYPE as i32,
            name: "regtype",
            len: 4,
            category: "N",
            elem: 0,
            array: 2211,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::BOOL as i32,
            name: "bool",
            len: 1,
            category: "B",
            elem: 0,
            array: crabka_pgtypes::oids::BOOLARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::BYTEA as i32,
            name: "bytea",
            len: -1,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::BYTEAARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INT2 as i32,
            name: "int2",
            len: 2,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::INT2ARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INT8 as i32,
            name: "int8",
            len: 8,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::INT8ARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INT4 as i32,
            name: "int4",
            len: 4,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::INT4ARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TEXT as i32,
            name: "text",
            len: -1,
            category: "S",
            elem: 0,
            array: crabka_pgtypes::oids::TEXTARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::BPCHAR as i32,
            name: "bpchar",
            len: -1,
            category: "S",
            elem: 0,
            array: 0,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::VARCHAR as i32,
            name: "varchar",
            len: -1,
            category: "S",
            elem: 0,
            array: 0,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::FLOAT4 as i32,
            name: "float4",
            len: 4,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::FLOAT4ARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::FLOAT8 as i32,
            name: "float8",
            len: 8,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::FLOAT8ARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::POINT as i32,
            name: "point",
            len: 16,
            category: "G",
            elem: 0,
            array: 0,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::PATH as i32,
            name: "path",
            len: -1,
            category: "G",
            elem: 0,
            array: 0,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::NUMERIC as i32,
            name: "numeric",
            len: -1,
            category: "N",
            elem: 0,
            array: crabka_pgtypes::oids::NUMERICARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::DATE as i32,
            name: "date",
            len: 4,
            category: "D",
            elem: 0,
            array: crabka_pgtypes::oids::DATEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TIME as i32,
            name: "time",
            len: 8,
            category: "D",
            elem: 0,
            array: crabka_pgtypes::oids::TIMEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TIMESTAMP as i32,
            name: "timestamp",
            len: 8,
            category: "D",
            elem: 0,
            array: crabka_pgtypes::oids::TIMESTAMPARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TIMESTAMPTZ as i32,
            name: "timestamptz",
            len: 8,
            category: "D",
            elem: 0,
            array: crabka_pgtypes::oids::TIMESTAMPTZARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INTERVAL as i32,
            name: "interval",
            len: 16,
            category: "T",
            elem: 0,
            array: crabka_pgtypes::oids::INTERVALARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::UUID as i32,
            name: "uuid",
            len: 16,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::UUIDARRAY as i32,
        },
        // `json` is an input alias for `jsonb` (crabka never reports OID 114),
        // but PostgreSQL's own row is what a driver's typeinfo query finds when
        // an application asks about the `json` type by name or oid.
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::JSON as i32,
            name: "json",
            len: -1,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::JSONARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::JSONB as i32,
            name: "jsonb",
            len: -1,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::JSONBARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TSVECTOR as i32,
            name: "tsvector",
            len: -1,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::TSVECTORARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TSQUERY as i32,
            name: "tsquery",
            len: -1,
            category: "U",
            elem: 0,
            array: crabka_pgtypes::oids::TSQUERYARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INT4RANGE as i32,
            name: "int4range",
            len: -1,
            category: "R",
            elem: 0,
            array: crabka_pgtypes::oids::INT4RANGEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::NUMRANGE as i32,
            name: "numrange",
            len: -1,
            category: "R",
            elem: 0,
            array: crabka_pgtypes::oids::NUMRANGEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TSRANGE as i32,
            name: "tsrange",
            len: -1,
            category: "R",
            elem: 0,
            array: crabka_pgtypes::oids::TSRANGEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TSTZRANGE as i32,
            name: "tstzrange",
            len: -1,
            category: "R",
            elem: 0,
            array: crabka_pgtypes::oids::TSTZRANGEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::DATERANGE as i32,
            name: "daterange",
            len: -1,
            category: "R",
            elem: 0,
            array: crabka_pgtypes::oids::DATERANGEARRAY as i32,
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INT8RANGE as i32,
            name: "int8range",
            len: -1,
            category: "R",
            elem: 0,
            array: crabka_pgtypes::oids::INT8RANGEARRAY as i32,
        },
    ]
}

/// The `pg_type.typname` of an element type's array type, which is
/// PostgreSQL's leading underscore over the element's own `typname`.
fn array_typname(elem: crabka_pgtypes::ElemType) -> &'static str {
    use crabka_pgtypes::ElemType;
    match elem {
        ElemType::Bool => "_bool",
        ElemType::Int4 => "_int4",
        ElemType::Int8 => "_int8",
        ElemType::Text => "_text",
        ElemType::Float8 => "_float8",
        ElemType::Numeric => "_numeric",
        ElemType::Date => "_date",
        ElemType::Time => "_time",
        ElemType::Timestamp => "_timestamp",
        ElemType::Timestamptz => "_timestamptz",
        ElemType::Interval => "_interval",
        ElemType::Bytea => "_bytea",
        ElemType::Uuid => "_uuid",
        ElemType::Jsonb => "_jsonb",
        ElemType::Int2 => "_int2",
        ElemType::Float4 => "_float4",
        ElemType::Regtype => "_regtype",
        ElemType::Varchar(_) => "_varchar",
        ElemType::Char(_) => "_bpchar",
        ElemType::Range(range) => match range.oid {
            crabka_pgtypes::oids::INT4RANGE => "_int4range",
            crabka_pgtypes::oids::NUMRANGE => "_numrange",
            crabka_pgtypes::oids::TSRANGE => "_tsrange",
            crabka_pgtypes::oids::TSTZRANGE => "_tstzrange",
            crabka_pgtypes::oids::DATERANGE => "_daterange",
            crabka_pgtypes::oids::INT8RANGE => "_int8range",
            _ => "_range",
        },
        ElemType::Multirange(multirange) => match multirange.oid {
            crabka_pgtypes::oids::INT4MULTIRANGE => "_int4multirange",
            crabka_pgtypes::oids::NUMMULTIRANGE => "_nummultirange",
            crabka_pgtypes::oids::TSMULTIRANGE => "_tsmultirange",
            crabka_pgtypes::oids::TSTZMULTIRANGE => "_tstzmultirange",
            crabka_pgtypes::oids::DATEMULTIRANGE => "_datemultirange",
            crabka_pgtypes::oids::INT8MULTIRANGE => "_int8multirange",
            _ => "_multirange",
        },
    }
}

/// The scalar rows plus one array row per supported element type (and `_json`,
/// the array of the `json` input alias). Array types are base types like their
/// elements (`typtype` 'b'), in category 'A', variable length, and they carry
/// the element's oid in `typelem`.
fn builtin_type_rows() -> &'static [BuiltinTypeRow] {
    static ROWS: std::sync::LazyLock<Vec<BuiltinTypeRow>> = std::sync::LazyLock::new(|| {
        let mut rows = scalar_type_rows().to_vec();
        rows.extend([
            BuiltinTypeRow {
                oid: 1013,
                name: "_oidvector",
                len: -1,
                category: "A",
                elem: crabka_pgtypes::oids::OIDVECTOR as i32,
                array: 0,
            },
            BuiltinTypeRow {
                oid: 2207,
                name: "_regprocedure",
                len: -1,
                category: "A",
                elem: crabka_pgtypes::oids::REGPROCEDURE as i32,
                array: 0,
            },
        ]);
        for (oid, name, array) in [
            (
                crabka_pgtypes::oids::INT4MULTIRANGE,
                "int4multirange",
                crabka_pgtypes::oids::INT4MULTIRANGEARRAY,
            ),
            (
                crabka_pgtypes::oids::NUMMULTIRANGE,
                "nummultirange",
                crabka_pgtypes::oids::NUMMULTIRANGEARRAY,
            ),
            (
                crabka_pgtypes::oids::TSMULTIRANGE,
                "tsmultirange",
                crabka_pgtypes::oids::TSMULTIRANGEARRAY,
            ),
            (
                crabka_pgtypes::oids::TSTZMULTIRANGE,
                "tstzmultirange",
                crabka_pgtypes::oids::TSTZMULTIRANGEARRAY,
            ),
            (
                crabka_pgtypes::oids::DATEMULTIRANGE,
                "datemultirange",
                crabka_pgtypes::oids::DATEMULTIRANGEARRAY,
            ),
            (
                crabka_pgtypes::oids::INT8MULTIRANGE,
                "int8multirange",
                crabka_pgtypes::oids::INT8MULTIRANGEARRAY,
            ),
        ] {
            rows.push(BuiltinTypeRow {
                oid: oid as i32,
                name,
                len: -1,
                category: "R",
                elem: 0,
                array: array as i32,
            });
        }
        rows.push(BuiltinTypeRow {
            oid: crabka_pgtypes::oids::JSONARRAY as i32,
            name: "_json",
            len: -1,
            category: "A",
            elem: crabka_pgtypes::oids::JSON as i32,
            array: 0,
        });
        rows.push(BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TSVECTORARRAY as i32,
            name: "_tsvector",
            len: -1,
            category: "A",
            elem: crabka_pgtypes::oids::TSVECTOR as i32,
            array: 0,
        });
        rows.push(BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TSQUERYARRAY as i32,
            name: "_tsquery",
            len: -1,
            category: "A",
            elem: crabka_pgtypes::oids::TSQUERY as i32,
            array: 0,
        });
        for (oid, name, elem) in [
            (
                crabka_pgtypes::oids::INT4RANGEARRAY,
                "_int4range",
                crabka_pgtypes::oids::INT4RANGE,
            ),
            (
                crabka_pgtypes::oids::NUMRANGEARRAY,
                "_numrange",
                crabka_pgtypes::oids::NUMRANGE,
            ),
            (
                crabka_pgtypes::oids::TSRANGEARRAY,
                "_tsrange",
                crabka_pgtypes::oids::TSRANGE,
            ),
            (
                crabka_pgtypes::oids::TSTZRANGEARRAY,
                "_tstzrange",
                crabka_pgtypes::oids::TSTZRANGE,
            ),
            (
                crabka_pgtypes::oids::DATERANGEARRAY,
                "_daterange",
                crabka_pgtypes::oids::DATERANGE,
            ),
            (
                crabka_pgtypes::oids::INT8RANGEARRAY,
                "_int8range",
                crabka_pgtypes::oids::INT8RANGE,
            ),
        ] {
            rows.push(BuiltinTypeRow {
                oid: oid as i32,
                name,
                len: -1,
                category: "A",
                elem: elem as i32,
                array: 0,
            });
        }
        for (oid, name, elem) in [
            (
                crabka_pgtypes::oids::INT4MULTIRANGEARRAY,
                "_int4multirange",
                crabka_pgtypes::oids::INT4MULTIRANGE,
            ),
            (
                crabka_pgtypes::oids::NUMMULTIRANGEARRAY,
                "_nummultirange",
                crabka_pgtypes::oids::NUMMULTIRANGE,
            ),
            (
                crabka_pgtypes::oids::TSMULTIRANGEARRAY,
                "_tsmultirange",
                crabka_pgtypes::oids::TSMULTIRANGE,
            ),
            (
                crabka_pgtypes::oids::TSTZMULTIRANGEARRAY,
                "_tstzmultirange",
                crabka_pgtypes::oids::TSTZMULTIRANGE,
            ),
            (
                crabka_pgtypes::oids::DATEMULTIRANGEARRAY,
                "_datemultirange",
                crabka_pgtypes::oids::DATEMULTIRANGE,
            ),
            (
                crabka_pgtypes::oids::INT8MULTIRANGEARRAY,
                "_int8multirange",
                crabka_pgtypes::oids::INT8MULTIRANGE,
            ),
        ] {
            rows.push(BuiltinTypeRow {
                oid: oid as i32,
                name,
                len: -1,
                category: "A",
                elem: elem as i32,
                array: 0,
            });
        }
        rows.extend(crabka_pgtypes::ElemType::ALL.map(|elem| BuiltinTypeRow {
            oid: i32::try_from(elem.array_oid()).expect("array oid fits in int4"),
            name: array_typname(elem),
            len: -1,
            category: "A",
            elem: i32::try_from(elem.oid()).expect("element oid fits in int4"),
            array: 0,
        }));
        rows
    });
    &ROWS
}

fn oid_i32(oid: u32) -> Result<i32, ExecError> {
    i32::try_from(oid).map_err(|_| ExecError::Unsupported("oid exceeds int4 range".into()))
}

fn catalog_index_oid(index_id: u32) -> Result<i32, ExecError> {
    let oid = 50_000u32
        .checked_add(index_id)
        .ok_or_else(|| ExecError::Unsupported("index oid exceeds int4 range".into()))?;
    oid_i32(oid)
}

fn usize_i32(value: usize) -> Result<i32, ExecError> {
    i32::try_from(value)
        .map_err(|_| ExecError::Unsupported("catalog value exceeds int4 range".into()))
}

fn int(value: i32) -> Datum {
    Datum::Int4(value)
}

fn text(value: &str) -> Datum {
    Datum::Text(value.to_string())
}

pub(crate) fn reject_nested_relation_locking(s: &SelectStmt) -> Result<(), ExecError> {
    if s.locking.is_some() {
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE is not supported in CTEs or derived tables".into(),
        ));
    }
    Ok(())
}

/// `PostgreSQL`'s `CheckSelectLocking`: a locking read may not be combined with
/// any clause that turns rows into aggregates or a computed set, because there
/// would be no base-table row left to lock.
pub(crate) fn check_select_locking(
    s: &SelectStmt,
    strength: crabka_pgparser::ast::RowLockStrength,
) -> Result<(), ExecError> {
    let refuse = |what: &str| {
        Err(ExecError::Unsupported(format!(
            "{} is not allowed with {what}",
            strength.as_sql()
        )))
    };
    if s.distinct.dedups() {
        return refuse("DISTINCT clause");
    }
    if !s.group_by.is_empty() {
        return refuse("GROUP BY clause");
    }
    if s.having.is_some() {
        return refuse("HAVING clause");
    }
    if crate::agg::is_aggregate_query(s) {
        return refuse("aggregate functions");
    }
    // A window result is not a row of any table, so there is nothing for the
    // lock to name. PostgreSQL checks this after the aggregate test, so a
    // grouped window query still reports its GROUP BY clause first.
    if crate::window::has_window_calls(s) {
        return refuse("window functions");
    }
    Ok(())
}

/// The row-lock mode a strength maps onto.
///
/// Divergence: crabka's lock table has two modes, so `FOR NO KEY UPDATE` folds
/// onto the exclusive mode and `FOR KEY SHARE` onto the shared one. Every pair
/// `PostgreSQL` lets proceed concurrently still does, except that `FOR KEY
/// SHARE` blocks against `FOR NO KEY UPDATE` here where `PostgreSQL` lets both
/// through.
pub(crate) fn lock_mode_for(
    strength: crabka_pgparser::ast::RowLockStrength,
) -> crate::lockmgr::LockMode {
    use crabka_pgparser::ast::RowLockStrength;
    match strength {
        RowLockStrength::ForUpdate | RowLockStrength::ForNoKeyUpdate => {
            crate::lockmgr::LockMode::Exclusive
        }
        RowLockStrength::ForShare | RowLockStrength::ForKeyShare => {
            crate::lockmgr::LockMode::Shared
        }
    }
}

pub(crate) fn execute_read(
    read_ctx: &crate::subquery::SubCtx<'_>,
    stmt: &Statement,
) -> Result<QueryResult, ExecError> {
    let span = exec_read_span(read_ctx);
    let _guard = span.enter();
    crate::session::check_query_canceled()?;
    let Statement::Query(q) = stmt else {
        return Err(ExecError::Unsupported("not a query statement".into()));
    };
    let rel = crate::query::query_to_relation(read_ctx, q)?;
    crate::session::check_query_canceled()?;
    let result = crate::query::relation_to_rows_result(rel, read_ctx.eval_ctx);
    if let QueryResult::Rows { rows, .. } = &result {
        span.record("pg.rows_out", crate::telemetry::integer(rows.len()));
    }
    Ok(result)
}

/// Build the span covering a read statement's execution inside the executor.
///
/// The scans, joins and locks the read performs attach to this, so it is the
/// level at which "the query itself was slow" separates from "getting a read
/// timestamp was slow". `pg.join_strategy` stays empty unless the planner
/// actually chose a distributed join. See [`try_distributed_inner_equi_join`].
fn exec_read_span(read_ctx: &crate::subquery::SubCtx<'_>) -> tracing::Span {
    tracing::debug_span!(
        target: crate::telemetry::EXEC_TARGET,
        "gres.exec_read",
        otel.kind = "internal",
        pg.rows_out = tracing::field::Empty,
        pg.blocking_query_memory_bytes =
            crate::telemetry::integer(read_ctx.blocking_query_memory.bytes_usize()),
        pg.join_strategy = tracing::field::Empty,
    )
}

/// Run a locking SELECT's body without any locks.
///
/// This is the case where its FROM names no base table (a FROM-less SELECT, a
/// set-returning function, a derived table), which `PostgreSQL` executes as an
/// ordinary read.
fn execute_read_body(
    read_ctx: &crate::subquery::SubCtx<'_>,
    s: &SelectStmt,
) -> Result<QueryResult, ExecError> {
    let mut unlocked = s.clone();
    unlocked.locking = None;
    let relation = select_to_relation_with_ctes(read_ctx, &unlocked)?;
    Ok(crate::query::relation_to_rows_result(
        relation,
        read_ctx.eval_ctx,
    ))
}

/// Locking SELECT (FOR UPDATE / FOR SHARE). Takes a row lock on each visible
/// row before rechecking it via EvalPlanQual (same semantics as UPDATE/DELETE).
/// The snapshot and xid must already be established by the caller.
pub(crate) async fn execute_read_locking(
    read_ctx: &crate::subquery::SubCtx<'_>,
    procarray: &crate::procarray::ProcArray,
    lockmgr: &crate::lockmgr::RowLockManager,
    repeatable_read: bool,
    lock_wait_cap: Option<std::time::Duration>,
    s: &SelectStmt,
) -> Result<QueryResult, ExecError> {
    let catalog_kv = read_ctx.catalog_kv;
    let resolution = read_ctx.fctx.resolution;
    let kv = read_ctx.kv;
    let global = read_ctx.global;
    let gsnap = read_ctx.gsnap;
    let snapshot = read_ctx.snapshot;
    let xid = read_ctx.own.ok_or_else(|| {
        ExecError::Unsupported("locking SELECT requires a transaction xid".into())
    })?;
    let ctx = read_ctx.eval_ctx;
    let locking = s
        .locking
        .clone()
        .ok_or_else(|| ExecError::Unsupported("locking SELECT has no locking clause".into()))?;
    // Ahead of subquery resolution, which evaluates the statement's expressions:
    // PostgreSQL refuses these shapes during parse analysis, so a query it will
    // not run must not be part-run to report it.
    check_select_locking(s, locking.strength)?;
    // SP34: resolve uncorrelated subqueries (e.g. in the WHERE of a FOR UPDATE) to
    // constants first, under this statement's snapshot handles.
    let resolved = crate::subquery::resolve_in_select(read_ctx, s)?;
    let s = &resolved;
    let mode = lock_mode_for(locking.strength);
    // FOR UPDATE/SHARE names base-table rows. A FROM with none — a FROM-less
    // SELECT, a set-returning function, a derived table — has nothing to lock,
    // and PostgreSQL simply runs the query.
    // `OF <rel>` restricts locking to the relations it names; one that is not in
    // the FROM clause at all is PostgreSQL's 42P01.
    let mut qualifiers = Vec::new();
    collect_qualifiers(&s.from, &mut qualifiers);
    for named in &locking.of {
        if !qualifiers
            .iter()
            .any(|qualifier| qualifier.eq_ignore_ascii_case(named))
        {
            return Err(ExecError::MissingFromEntry(named.clone()));
        }
    }
    let t = match s.from.as_slice() {
        [
            crabka_pgparser::ast::TableExpr::Table {
                name,
                alias,
                columns: None,
                sample: None,
                ..
            },
        ] if name.schema.is_none() && read_ctx.ctes.lookup(&name.name).is_none() => {
            let table = crabka_pgcatalog::get_table(
                catalog_kv,
                &resolve_relation(catalog_kv, resolution, name, SchemaDisposition::Reference)?,
            )?;
            let qualifier = alias.clone().unwrap_or_else(|| table.name.name.clone());
            if locking.of.is_empty()
                || locking
                    .of
                    .iter()
                    .any(|named| named.eq_ignore_ascii_case(&qualifier))
            {
                table
            } else {
                // The clause names other relations only, so this one is read
                // without locking.
                return execute_read_body(read_ctx, s);
            }
        }
        // A FROM with nothing lockable — no FROM at all, a set-returning
        // function, a derived table — just runs the query, as in PostgreSQL.
        [] => return execute_read_body(read_ctx, s),
        [item] if !matches!(item, crabka_pgparser::ast::TableExpr::Table { .. }) => {
            return execute_read_body(read_ctx, s);
        }
        _ => {
            return Err(ExecError::Unsupported(format!(
                "{} with a join is not supported",
                locking.strength.as_sql()
            )));
        }
    };
    if crate::partition::is_partitioned(read_ctx.catalog_kv, &t.name)? {
        return Err(ExecError::Unsupported(format!(
            "{} on a partitioned table is not supported: the lock would have to be taken on \
             every partition that contributes rows",
            locking.strength.as_sql()
        )));
    }
    let scope = Scope::single(&t, &t.name.name);

    // Scan visible rows, then lock and EvalPlanQual-recheck each one.
    let mut kept: Vec<Vec<Datum>> = Vec::new();
    for ScannedRow {
        rowid,
        row: scanned_row,
        ..
    } in read_ctx.range_scanner.scan(ScanRequest {
        local: kv,
        global,
        global_snapshot: gsnap,
        snapshot,
        own_xid: Some(xid),
        read_ts: None,
        own_start_ts: None,
        table: &t,
        interval: RowInterval::ALL,
        predicate: PredicatePushdown::FullScan,
        projection: crate::ProjectionPushdown::All,
        partial_aggregate: None,
        top_k: None,
    })? {
        // 1. Filter on the snapshot-visible row FIRST — only lock rows that
        //    match the WHERE clause (a FOR UPDATE/SHARE with no WHERE still
        //    locks all rows because row_matches(None, ..) returns true).
        if !row_matches(s.filter.as_ref(), &scope, &scanned_row, ctx)? {
            continue;
        }

        // 2. Lock only matching candidates (40P01 on deadlock or expired cap).
        //    NOWAIT and SKIP LOCKED both take the non-blocking path and differ
        //    only in what a conflict means: an error, or a row that is skipped.
        match locking.wait {
            crabka_pgparser::ast::LockWaitPolicy::Wait => {
                lockmgr
                    .acquire(t.id, rowid, mode, xid, lock_wait_cap)
                    .await
                    .map_err(lock_acquire_error)?;
            }
            policy => {
                if let crate::lockmgr::Acquire::Conflict(_) =
                    lockmgr.try_acquire(t.id, rowid, mode, xid)
                {
                    if policy == crabka_pgparser::ast::LockWaitPolicy::SkipLocked {
                        continue;
                    }
                    return Err(ExecError::FunctionError {
                        sqlstate: "55P03",
                        message: format!("could not obtain lock on row in relation \"{}\"", t.name),
                    });
                }
            }
        }

        // 3. EvalPlanQual: re-read the row under the lock (40001 under RR if
        //    changed since our snapshot; RC re-finds the latest live version).
        let Some((_cur_key_xid, _cur_xmin, cur_row)) = eval_plan_qual(
            &MutationContext {
                kv,
                global,
                procarray,
                snapshot,
                xid,
                repeatable_read,
            },
            &t,
            rowid,
        )?
        else {
            continue; // deleted by a concurrent committed txn — skip
        };

        // 4. Re-apply the WHERE filter against the (possibly newer) row.
        if !row_matches(s.filter.as_ref(), &scope, &cur_row, ctx)? {
            continue; // no longer matches
        }
        kept.push(cur_row);
    }

    resolve_scanned_regclass(read_ctx.catalog_kv, &t, &mut kept)?;
    project_order_limit(s, &scope, kept, ctx)
}

/// Apply DISTINCT / ORDER BY / OFFSET / LIMIT and projection, returning the
/// projected output Datum rows. Shared by the top-level row path and derived
/// tables. `ctx` carries the session zone + transaction/statement clock used by
/// temporal eval.
pub(crate) fn project_rows_ordered(
    s: &SelectStmt,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    kept: Vec<Vec<Datum>>,
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    // A select list with a set-returning function expands rows BELOW DISTINCT,
    // ORDER BY and LIMIT (PostgreSQL's ProjectSet), so it owns the whole
    // sort/dedup/limit shape rather than sharing the one-row-in-one-row-out one.
    if crate::srf::exprs_contain_srf(out_exprs) || crate::srf::order_by_contains_srf(&s.order_by) {
        return crate::srf::project_rows_ordered(s, scope, fields, out_exprs, kept, ctx);
    }
    let window = RowWindow {
        offset: eval_row_count(s.offset.as_ref(), RowCountClause::Offset, ctx)?,
        limit: eval_row_count(s.limit.as_ref(), RowCountClause::Limit, ctx)?,
        with_ties: s.with_ties,
    };
    // Only plain DISTINCT restricts ORDER BY to the select-list output; DISTINCT
    // ON sorts the source rows, so its keys may name source-only columns.
    let require_output = matches!(s.distinct, crabka_pgparser::ast::DistinctClause::Distinct);
    let order_keys =
        resolve_select_order_keys(&s.order_by, scope, fields, out_exprs, require_output)?;

    // SP39: SELECT DISTINCT projects FIRST, dedups output rows, then ORDER BY
    // sorts the deduped output. PostgreSQL requires every sort key to refer to
    // the select-list output (ordinal, alias/name, or the exact select expression).
    if matches!(s.distinct, crabka_pgparser::ast::DistinctClause::Distinct) {
        let mut projected = project_rows(out_exprs, scope, &kept, ctx)?;
        ensure_blocking_rows_fit(&projected)?;
        let mut seen: std::collections::HashSet<Vec<Datum>> = std::collections::HashSet::new();
        projected.retain(|r| seen.insert(r.clone()));
        let keyed: Vec<(Vec<Datum>, Vec<Datum>)> = projected
            .into_iter()
            .map(|r| {
                let keys = order_keys
                    .iter()
                    .map(|k| match k {
                        SelectOrderKey::Output(i) => r[*i].clone(),
                        SelectOrderKey::SourceExpr(_) => {
                            unreachable!("DISTINCT order keys are output-only")
                        }
                    })
                    .collect();
                (keys, r)
            })
            .collect();
        let mut keyed = keyed;
        if !s.order_by.is_empty() {
            keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, &s.order_by));
        }
        return Ok(apply_row_window(keyed, window, &s.order_by));
    }

    // Non-DISTINCT keeps the existing source-row ordering shape so non-projected
    // source expressions still work, but output ordinals/labels evaluate the
    // corresponding projection expression for each source row.
    let Some(plan) = distinct_on_plan(s, scope, fields, out_exprs, &order_keys)? else {
        let mut keyed = key_source_rows(&order_keys, out_exprs, scope, kept, ctx)?;
        if !order_keys.is_empty() {
            keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, &s.order_by));
        }
        let kept = apply_row_window(keyed, window, &s.order_by);
        return project_rows(out_exprs, scope, &kept, ctx);
    };

    // DISTINCT ON dedups a stream sorted by `plan.sort`, which is not always the
    // query's own ORDER BY — the sort decides which row of each group survives,
    // ORDER BY only decides how the survivors come out.
    let dedup_keys: Vec<SelectOrderKey> = plan
        .sort
        .iter()
        .map(|item| SelectOrderKey::SourceExpr(item.expr.clone()))
        .collect();
    let mut keyed = key_source_rows(&dedup_keys, out_exprs, scope, kept, ctx)?;
    if !dedup_keys.is_empty() {
        // A stable sort is load-bearing for DISTINCT ON without an ORDER BY:
        // PostgreSQL keeps the first row of each key group in input order.
        keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, &plan.sort));
    }
    let survivors = keep_first_per_distinct_on_group(keyed, &plan.group, scope, ctx)?;
    // Re-key on the query's ORDER BY and sort the survivors into it, the way
    // PostgreSQL puts a Sort above the Unique when the two differ. The sort is
    // stable, so it is a no-op when the dedup ordering already satisfies it.
    let rows = survivors.into_iter().map(|(_, row)| row).collect();
    let mut keyed = key_source_rows(&order_keys, out_exprs, scope, rows, ctx)?;
    if !order_keys.is_empty() {
        keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, &s.order_by));
    }
    let kept = apply_row_window(keyed, window, &s.order_by);
    project_rows(out_exprs, scope, &kept, ctx)
}

/// Pair each source row with the values of `keys`, under the blocking-query
/// memory budget. With no keys the rows pass through unmeasured. Nothing is
/// sorted, so nothing extra is held.
fn key_source_rows(
    keys: &[SelectOrderKey],
    out_exprs: &[Expr],
    scope: &Scope,
    rows: Vec<Vec<Datum>>,
    ctx: &crate::clock::EvalCtx,
) -> Result<KeyedRows, ExecError> {
    if keys.is_empty() {
        return Ok(rows.into_iter().map(|row| (Vec::new(), row)).collect());
    }
    let mut keyed: KeyedRows = Vec::with_capacity(rows.len());
    let mut keyed_bytes = 0usize;
    for row in rows {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(match key {
                SelectOrderKey::Output(i) => crate::eval::eval(&out_exprs[*i], scope, &row, ctx)?,
                SelectOrderKey::SourceExpr(expr) => crate::eval::eval(expr, scope, &row, ctx)?,
            });
        }
        let bytes = crate::scanner::datum_row_bytes(&values)
            .saturating_add(crate::scanner::datum_row_bytes(&row));
        if crate::scanner::exceeds_query_memory(
            keyed_bytes.saturating_add(bytes),
            crate::scanner::BLOCKING_QUERY_MEMORY,
        ) {
            return Err(crate::scanner::memory_budget_exceeded());
        }
        keyed_bytes += bytes;
        keyed.push((values, row));
    }
    Ok(keyed)
}

/// How a `DISTINCT ON` query dedups and sorts.
pub(crate) struct DistinctOnPlan {
    /// The expressions consecutive rows are grouped by, each already resolved
    /// through the SQL92 rules (`DISTINCT ON (1)` names a select-list column).
    pub(crate) group: Vec<Expr>,
    /// The order the rows must be in before that grouping, which is what decides
    /// which row of each group survives.
    pub(crate) sort: Vec<crabka_pgparser::ast::OrderItem>,
}

/// Resolve `DISTINCT ON` against the query's `ORDER BY`, or `None` when the
/// query has no `DISTINCT ON` at all.
///
/// `PostgreSQL`'s compatibility rule (`transformDistinctOnClause`) is
/// **one-directional**, and it is not a set-match. It walks the ORDER BY keys
/// adopting each one that is also a `DISTINCT ON` expression; `42P10` fires
/// only once an ORDER BY key has been *skipped*, and then in two places: for a
/// later ORDER BY key that is in the `ON` list, and for any `ON` expression the
/// ORDER BY never adopted. So `DISTINCT ON (a, b) … ORDER BY a` is valid (`b` is
/// appended with default `ASC NULLS LAST` semantics), while
/// `DISTINCT ON (a, b) … ORDER BY a, c` is not: `c` is skipped and `b` still
/// needs appending.
///
/// When the resulting dedup sort is shorter than the ORDER BY, `PostgreSQL`
/// sorts by the whole ORDER BY instead (`create_distinct_paths`); that ordering
/// still satisfies the grouping, and its trailing keys are what pick the
/// surviving row of each group.
pub(crate) fn distinct_on_plan(
    s: &SelectStmt,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    order_keys: &[SelectOrderKey],
) -> Result<Option<DistinctOnPlan>, ExecError> {
    let Some(on) = s.distinct.on_exprs() else {
        return Ok(None);
    };
    let group = on
        .iter()
        .map(|expr| resolve_sql92_expr(expr, scope, fields, out_exprs, SQL92_DISTINCT_ON))
        .collect::<Result<Vec<_>, ExecError>>()?;
    let ordered: Vec<(&Expr, &crabka_pgparser::ast::OrderItem)> = order_keys
        .iter()
        .zip(&s.order_by)
        .map(|(key, item)| match key {
            SelectOrderKey::Output(i) => (&out_exprs[*i], item),
            SelectOrderKey::SourceExpr(expr) => (expr, item),
        })
        .collect();

    let mut sort: Vec<crabka_pgparser::ast::OrderItem> = Vec::new();
    let mut skipped = false;
    for (expr, item) in &ordered {
        if !group
            .iter()
            .any(|key| order_output_exprs_equivalent(scope, key, expr))
        {
            skipped = true;
            continue;
        }
        if skipped {
            return Err(ExecError::InvalidColumnReference(
                "SELECT DISTINCT ON expressions must match initial ORDER BY expressions".into(),
            ));
        }
        sort.push(crabka_pgparser::ast::OrderItem {
            expr: (*expr).clone(),
            asc: item.asc,
            nulls_first: item.nulls_first,
        });
    }
    for key in &group {
        if sort
            .iter()
            .any(|item| order_output_exprs_equivalent(scope, &item.expr, key))
        {
            continue;
        }
        // An ON expression the ORDER BY never adopted has to be appended to the
        // dedup sort — which is only sound while the adopted keys are still the
        // ORDER BY's own leading keys. Once an ORDER BY key has been skipped
        // they are not, so PostgreSQL rejects the query here.
        if skipped {
            return Err(ExecError::InvalidColumnReference(
                "SELECT DISTINCT ON expressions must match initial ORDER BY expressions".into(),
            ));
        }
        sort.push(crabka_pgparser::ast::OrderItem {
            expr: key.clone(),
            asc: true,
            nulls_first: false,
        });
    }
    if sort.len() < ordered.len() {
        sort = ordered
            .iter()
            .map(|(expr, item)| crabka_pgparser::ast::OrderItem {
                expr: (*expr).clone(),
                asc: item.asc,
                nulls_first: item.nulls_first,
            })
            .collect();
    }
    Ok(Some(DistinctOnPlan { group, sort }))
}

/// The clause name `DISTINCT ON` position errors carry.
const SQL92_DISTINCT_ON: crate::sql92::Sql92Clause = crate::sql92::Sql92Clause::DistinctOn;

/// Resolve one `DISTINCT ON` expression through `PostgreSQL`'s SQL92 rules to
/// the expression it stands for: an integer constant is a select-list position,
/// a bare name matching an output label is that column, and anything else is
/// itself.
fn resolve_sql92_expr(
    expr: &Expr,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    clause: crate::sql92::Sql92Clause,
) -> Result<Expr, ExecError> {
    if let Some(index) = crate::sql92::output_position(expr, fields.len(), clause)? {
        return Ok(out_exprs[index].clone());
    }
    if let Expr::Column { table: None, name } = expr
        && let Some(index) = output_label_index(scope, fields, out_exprs, name)?
    {
        return Ok(out_exprs[index].clone());
    }
    Ok(expr.clone())
}

/// Rows paired with the ORDER BY key vector they sort on.
type KeyedRows = Vec<(Vec<Datum>, Vec<Datum>)>;

/// Keep the first row of each `DISTINCT ON` key group. The rows are already in
/// the order that decides which row wins, so this is a single pass over
/// consecutive-equal groups, the shape `PostgreSQL`'s `Unique` node has.
fn keep_first_per_distinct_on_group(
    keyed: KeyedRows,
    on: &[Expr],
    scope: &Scope,
    ctx: &crate::clock::EvalCtx,
) -> Result<KeyedRows, ExecError> {
    let mut out: KeyedRows = Vec::new();
    let mut previous: Option<Vec<Datum>> = None;
    for (keys, row) in keyed {
        let group = on
            .iter()
            .map(|expr| crate::eval::eval(expr, scope, &row, ctx))
            .collect::<Result<Vec<_>, _>>()?;
        if previous.as_ref() == Some(&group) {
            continue;
        }
        previous = Some(group);
        out.push((keys, row));
    }
    Ok(out)
}

fn ensure_blocking_rows_fit(rows: &[Vec<Datum>]) -> Result<(), ExecError> {
    let bytes = rows.iter().fold(0usize, |bytes, row| {
        bytes.saturating_add(crate::scanner::datum_row_bytes(row))
    });
    if crate::scanner::exceeds_query_memory(bytes, crate::scanner::BLOCKING_QUERY_MEMORY) {
        return Err(crate::scanner::memory_budget_exceeded());
    }
    Ok(())
}

/// Apply ORDER BY, LIMIT, and projection to a set of already-filtered source
/// rows, producing the final `QueryResult::Rows`. Used by both `execute_read`
/// and `execute_read_locking` to avoid duplication.
///
/// `ctx` carries the session zone (forwarded to `rows_result` for `Timestamptz`
/// text rendering) and the transaction/statement clock used by temporal eval.
fn project_order_limit(
    s: &SelectStmt,
    scope: &Scope,
    kept: Vec<Vec<Datum>>,
    ctx: &crate::clock::EvalCtx,
) -> Result<QueryResult, ExecError> {
    let (fields, out_exprs, _tys) = resolve_projection(&s.projection, scope)?;
    let rows = project_rows_ordered(s, scope, &fields, &out_exprs, kept, ctx)?;
    Ok(rows_result(fields, &rows, ctx.output_style()))
}

/// Evaluate the projection expressions for each source row, yielding output
/// Datum rows (one `Datum` per output column).
pub(crate) fn project_rows(
    out_exprs: &[Expr],
    scope: &Scope,
    rows: &[Vec<Datum>],
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut cells = Vec::with_capacity(out_exprs.len());
        for e in out_exprs {
            cells.push(crate::eval::eval(e, scope, row, ctx)?);
        }
        out.push(cells);
    }
    Ok(out)
}

/// Encode projected Datum rows into a `QueryResult::Rows` (text + binary cells).
///
/// `tz` is the session time zone (`EvalCtx::time_zone`) used for `Timestamptz`
/// text rendering. Task 9 threads it from the per-statement `EvalCtx`; a
/// UTC/epoch context reproduces prior behavior until the session builds it.
pub(crate) fn rows_result(
    fields: Vec<FieldDescription>,
    projected: &[Vec<Datum>],
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> QueryResult {
    rows_result_with_tag(
        fields,
        projected,
        style,
        format!("SELECT {}", projected.len()),
    )
}

pub(crate) fn rows_result_with_tag(
    fields: Vec<FieldDescription>,
    projected: &[Vec<Datum>],
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
    tag: String,
) -> QueryResult {
    let rows: Vec<Vec<Option<Cell>>> = projected
        .iter()
        .map(|r| r.iter().map(|d| datum_to_cell(d, style)).collect())
        .collect();
    QueryResult::Rows { fields, rows, tag }
}

/// One resolved ORDER BY key for a plain SELECT. SQL92-style output references
/// (`ORDER BY 1`, `ORDER BY alias`) are represented as output indices; all other
/// expressions are evaluated against the source/group scope.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SelectOrderKey {
    Output(usize),
    SourceExpr(Expr),
}

/// Resolve SELECT ORDER BY items using PostgreSQL's SQL92 rules:
/// integer constant -> output ordinal, bare output label -> output column, and
/// everything else -> source expression unless `require_output` is true.
pub(crate) fn resolve_select_order_keys(
    order_by: &[OrderItem],
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    require_output: bool,
) -> Result<Vec<SelectOrderKey>, ExecError> {
    order_by
        .iter()
        .map(|item| resolve_select_order_key(item, scope, fields, out_exprs, require_output))
        .collect()
}

fn resolve_select_order_key(
    item: &OrderItem,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    require_output: bool,
) -> Result<SelectOrderKey, ExecError> {
    if let Some(index) =
        crate::sql92::output_position(&item.expr, fields.len(), crate::sql92::Sql92Clause::OrderBy)?
    {
        return Ok(SelectOrderKey::Output(index));
    }

    if let Expr::Column { table: None, name } = &item.expr
        && let Some(i) = output_label_index(scope, fields, out_exprs, name)?
    {
        return Ok(SelectOrderKey::Output(i));
    }

    if require_output {
        if let Some(i) = out_exprs
            .iter()
            .position(|e| order_output_exprs_equivalent(scope, e, &item.expr))
        {
            return Ok(SelectOrderKey::Output(i));
        }
        if let Expr::Column {
            table: Some(table),
            name,
        } = &item.expr
        {
            scope.resolve(Some(table), name)?;
        }
        return Err(ExecError::InvalidColumnReference(
            "for SELECT DISTINCT, ORDER BY expressions must appear in select list".into(),
        ));
    }

    Ok(SelectOrderKey::SourceExpr(item.expr.clone()))
}

fn output_label_index(
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    name: &str,
) -> Result<Option<usize>, ExecError> {
    let mut found = None;
    for (i, f) in fields.iter().enumerate() {
        if f.name == name {
            if let Some(prev) = found {
                if !order_output_exprs_equivalent(scope, &out_exprs[prev], &out_exprs[i]) {
                    return Err(ExecError::AmbiguousOrderBy(name.to_string()));
                }
            } else {
                found = Some(i);
            }
        }
    }
    Ok(found)
}

fn order_output_exprs_equivalent(scope: &Scope, a: &Expr, b: &Expr) -> bool {
    if a == b {
        return true;
    }
    match (a, b) {
        (
            Expr::Column {
                table: table_a,
                name: name_a,
            },
            Expr::Column {
                table: table_b,
                name: name_b,
            },
        ) => {
            let left = scope.resolve(table_a.as_deref(), name_a);
            let right = scope.resolve(table_b.as_deref(), name_b);
            matches!((left, right), (Ok(left), Ok(right)) if left == right)
        }
        _ => false,
    }
}

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
        Datum::Text(_) => matches!(expr, Expr::StringLiteral(_)),
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

/// Expand the projection list into output FieldDescriptions, the expressions
/// that produce each column, and each column's `ColumnType` (the third element
/// lets `select_to_relation` build a derived table's output scope without
/// re-inferring types).
type ResolvedProjection = (Vec<FieldDescription>, Vec<Expr>, Vec<ColumnType>);

/// `SELECT *` needs a relation to expand over, and `PostgreSQL` rejects it at
/// parse analysis when the query names none.
///
/// That is *not* the same as a `FROM` naming a relation with no columns left:
/// `ALTER TABLE … DROP COLUMN` down to zero columns leaves a legal relation, and
/// `SELECT *` over it yields rows of no columns rather than an error.
pub(crate) fn reject_from_less_wildcard(items: &[SelectItem]) -> Result<(), ExecError> {
    if items
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard))
    {
        return Err(ExecError::Syntax(
            "SELECT * with no tables specified is not valid".into(),
        ));
    }
    Ok(())
}

pub(crate) fn resolve_projection(
    items: &[SelectItem],
    scope: &Scope,
) -> Result<ResolvedProjection, ExecError> {
    // SP33: expand each item in turn so `*` spans every FROM table and `a.*`
    // expands one qualifier. Each `*`-expanded column carries its qualifier so a
    // multi-table `*` re-resolves unambiguously via `scope.resolve`.
    let mut fields = Vec::new();
    let mut exprs = Vec::new();
    let mut tys = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard => {
                // An empty scope here is a relation with no columns left, which
                // `*` legitimately expands to nothing. A query with no FROM at
                // all is rejected before it gets this far.
                //
                // The synthetic window-result and grouping-set bindings are not
                // part of the relation, so `*` never expands to them.
                for (index, c) in scope.columns.iter().enumerate().filter(|(_, c)| {
                    !is_window_binding(c) && !crate::grouping::is_hidden_binding(c)
                }) {
                    fields.push(field(&c.name, c.ty));
                    exprs.push(wildcard_reference(scope, index, c));
                    tys.push(c.ty);
                }
            }
            SelectItem::QualifiedWildcard(q) => {
                let cols: Vec<_> = scope
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.qualifier.as_deref() == Some(q))
                    .collect();
                if cols.is_empty() {
                    return Err(ExecError::MissingFromEntry(q.clone()));
                }
                for (index, c) in cols {
                    fields.push(field(&c.name, c.ty));
                    exprs.push(wildcard_reference(scope, index, c));
                    tys.push(c.ty);
                }
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| derived_name(expr));
                // A set-returning function in the select list contributes its
                // single output column's type; everything else infers normally.
                let ty = crate::srf::projection_type(expr, scope)?;
                fields.push(field(&name, ty));
                exprs.push(expr.clone());
                tys.push(ty);
            }
        }
    }
    Ok((fields, exprs, tys))
}

/// How a `*` expansion refers to the scope column at `index`.
///
/// By name where that name resolves back to this very column, and positionally
/// where it does not. A relation whose column names repeat, such as
/// `ROWS FROM (f(), f())` or a multi-argument `unnest`, would otherwise expand
/// `*` into
/// references `PostgreSQL` itself would call ambiguous, even though `SELECT *`
/// is valid there and only a bare reference to the repeated name is `42702`.
fn wildcard_reference(scope: &Scope, index: usize, column: &ColumnBinding) -> Expr {
    if scope.resolve(column.qualifier.as_deref(), &column.name) == Ok(index) {
        return Expr::Column {
            table: column.qualifier.clone(),
            name: column.name.clone(),
        };
    }
    Expr::Column {
        table: Some(crate::scope::POSITION_QUALIFIER.to_string()),
        name: index.to_string(),
    }
}

/// The output scope of a projected relation: the field names with their types
/// and no base-table qualifier.
fn projected_scope(fields: &[FieldDescription], tys: &[ColumnType]) -> Scope {
    Scope {
        columns: fields
            .iter()
            .zip(tys)
            .map(|(f, ty)| ColumnBinding {
                qualifier: None, // a projected result has no base-table qualifier
                name: f.name.clone(),
                ty: *ty,
            })
            .collect(),
    }
}

/// Is this binding one of the synthetic columns a window call's result occupies?
fn is_window_binding(c: &ColumnBinding) -> bool {
    c.qualifier.as_deref() == Some(crabka_pgparser::ast::WINDOW_QUALIFIER)
}

pub(crate) fn derived_name(expr: &Expr) -> String {
    match expr {
        // A window placeholder carries the label PostgreSQL gives an unaliased
        // window call: the function's own name.
        Expr::Column { name, .. } => crabka_pgparser::ast::window_binding_parts(name)
            .map_or_else(|| name.clone(), |(_, label)| label.to_string()),
        // PostgreSQL names an aggregate output column after the function.
        Expr::Func(fc) => fc.name.clone(),
        // A SQL/JSON expression is labelled after its construct (`json_object`,
        // `json_value`, …); `IS JSON` is a predicate and stays `?column?`.
        Expr::SqlJson(json) => json.output_label().to_string(),
        // PostgreSQL's `FigureColname` looks THROUGH a cast and a COLLATE (and
        // through the parentheses the parser has already discarded), so
        // `b::numeric`, `count(*)::bigint`, `b COLLATE "C"` and `(b)` are labelled
        // `b`, `count`, `b`, `b`. When the inner expression supplies no name of
        // its own, a CAST falls back to the TYPE's name — `1::bigint` is `bigint`,
        // not `?column?` — while a COLLATE has no such fallback.
        Expr::Cast { expr, ty } => match derived_name(expr) {
            unnamed if unnamed == "?column?" => ty.name().to_string(),
            named => named,
        },
        Expr::Collate { expr, .. } => derived_name(expr),
        _ => "?column?".to_string(),
    }
}

pub(crate) fn field(name: &str, ty: ColumnType) -> FieldDescription {
    FieldDescription {
        name: name.to_string(),
        table_oid: 0,
        column_id: 0,
        type_oid: ty.oid(),
        type_size: ty.type_size(),
        type_modifier: ty.typmod(),
        format: 0,
    }
}

pub(crate) fn column_type_from_oid(oid: u32) -> Result<ColumnType, ExecError> {
    Ok(match oid {
        crabka_pgtypes::oids::BOOL => ColumnType::Bool,
        crabka_pgtypes::oids::INT2 => ColumnType::Int2,
        crabka_pgtypes::oids::INT4 => ColumnType::Int4,
        crabka_pgtypes::oids::OIDVECTOR => ColumnType::OidVector,
        crabka_pgtypes::oids::REGTYPE => ColumnType::Regtype,
        crabka_pgtypes::oids::REGPROCEDURE => ColumnType::Regprocedure,
        crabka_pgtypes::oids::INT8 => ColumnType::Int8,
        crabka_pgtypes::oids::TEXT => ColumnType::Text,
        crabka_pgtypes::oids::VARCHAR => ColumnType::Varchar(None),
        crabka_pgtypes::oids::BPCHAR => ColumnType::Char(None),
        crabka_pgtypes::oids::FLOAT4 => ColumnType::Float4,
        crabka_pgtypes::oids::FLOAT8 => ColumnType::Float8,
        crabka_pgtypes::oids::POINT => ColumnType::Point,
        crabka_pgtypes::oids::PATH => ColumnType::Path,
        crabka_pgtypes::oids::NUMERIC => ColumnType::Numeric(None),
        crabka_pgtypes::oids::DATE => ColumnType::Date,
        crabka_pgtypes::oids::TIME => ColumnType::Time,
        crabka_pgtypes::oids::TIMESTAMP => ColumnType::Timestamp,
        crabka_pgtypes::oids::TIMESTAMPTZ => ColumnType::Timestamptz,
        crabka_pgtypes::oids::INTERVAL => ColumnType::Interval,
        crabka_pgtypes::oids::UUID => ColumnType::Uuid,
        // `json` is an input alias for `jsonb`; both name the same column type.
        crabka_pgtypes::oids::JSON | crabka_pgtypes::oids::JSONB => ColumnType::Jsonb,
        // Every array oid crabka has an element type for, `_json` included.
        _ => match crabka_pgtypes::ElemType::from_array_oid(oid) {
            Some(elem) => ColumnType::Array(elem),
            None => {
                return Err(ExecError::Unsupported(format!(
                    "unknown query field type oid {oid}"
                )));
            }
        },
    })
}

pub(crate) fn datum_to_cell(
    d: &Datum,
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> Option<Cell> {
    if d.is_null() {
        return None;
    }
    Some(Cell {
        text: Bytes::from(crabka_pgtypes::encoding::encode_text_in(d, style)),
        binary: Bytes::from(crabka_pgtypes::encoding::encode_binary(d)),
    })
}

/// Compare two order-key vectors per the SELECT's ASC/DESC flags, with PG's
/// default null placement (NULLS LAST for ASC, NULLS FIRST for DESC).
pub(crate) fn order_cmp(
    a: &[Datum],
    b: &[Datum],
    order_by: &[crabka_pgparser::ast::OrderItem],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (i, item) in order_by.iter().enumerate() {
        let (x, y) = (&a[i], &b[i]);
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            // The parser has already resolved PostgreSQL's defaults into
            // `nulls_first` (NULLS LAST for ASC, NULLS FIRST for DESC), and null
            // placement is independent of the ASC/DESC of the non-null values.
            (true, false) => {
                if item.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if item.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                // SLICE INVARIANT: each ORDER BY key position is type-homogeneous
                // (one column = one declared type; one expression = one static
                // type), so ops::compare never errors here. The Equal fallback is
                // defensive — when CAST / heterogeneous keys arrive in a later SP,
                // this must become a real error path or the sort loses total order.
                let base = crabka_pgtypes::ops::compare(x, y)
                    .ok()
                    .flatten()
                    .unwrap_or(Ordering::Equal);
                if item.asc { base } else { base.reverse() }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

// `describe` only resolves the SELECT's row description from the catalog (no
// rows are scanned), so the data store `_kv` is unused here. It is kept in the
// signature for uniformity with the other three executor entry points (all take
// `catalog_kv, kv, …`) so the session's call sites stay consistent.
pub(crate) fn describe(
    catalog_kv: &dyn Kv,
    _kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    sql: &str,
) -> Result<Vec<crabka_pgwire::engine::FieldDescription>, ExecError> {
    let statements = crabka_pgparser::parse(sql)?;
    let Some(statement) = statements.first() else {
        return Ok(Vec::new());
    };
    describe_statement(catalog_kv, resolution, statement)
}

pub(crate) fn describe_statement(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    statement: &Statement,
) -> Result<Vec<crabka_pgwire::engine::FieldDescription>, ExecError> {
    match statement {
        Statement::Query(q) => crate::query::describe_query_expr(catalog_kv, resolution, q),
        Statement::Insert {
            table, returning, ..
        }
        | Statement::Update {
            table, returning, ..
        }
        | Statement::Delete {
            table, returning, ..
        } => describe_returning(
            catalog_kv,
            &resolve_relation(catalog_kv, resolution, table, SchemaDisposition::Reference)?,
            returning.as_ref(),
            false,
        ),
        Statement::Merge {
            table, returning, ..
        } => describe_returning(
            catalog_kv,
            &resolve_relation(catalog_kv, resolution, table, SchemaDisposition::Reference)?,
            returning.as_ref(),
            true,
        ),
        _ => Ok(Vec::new()),
    }
}

pub(crate) fn describe_returning(
    catalog_kv: &dyn Kv,
    table: &crabka_pgcatalog::RelationName,
    returning: Option<&crabka_pgparser::ast::Returning>,
    merge: bool,
) -> Result<Vec<FieldDescription>, ExecError> {
    // The target is resolved even with no RETURNING clause, because analysing a
    // DML statement must reject a missing table (42P01) whether or not the
    // statement would have returned rows.
    let table = crabka_pgcatalog::get_table(catalog_kv, table)?;
    let Some(returning) = returning else {
        return Ok(Vec::new());
    };

    // Describe resolves against the target alone: OLD/NEW image columns mirror
    // its types, and a joined FROM/USING adds columns the caller must qualify.
    let spec = ReturningSpec::new(
        &table,
        &table.name.to_string(),
        Some(returning),
        None,
        merge,
    )?;
    let (fields, _exprs, _tys) = resolve_projection(&spec.items, &spec.scope)?;
    Ok(fields)
}

// ── D1/D4/D7: CREATE TABLE breadth, CHECK constraints, ALTER TABLE ───────────

/// A resolved `CREATE TABLE` definition: catalog columns, `CHECK` constraints,
/// the sequences its SERIAL/identity columns need, its constraint-backed
/// indexes, and the `FOREIGN KEY` clauses it collected (named, but not yet
/// resolved; see [`PendingForeignKey`]).
type TableDefinition = (
    Vec<Column>,
    Vec<crabka_pgcatalog::CheckConstraint>,
    Vec<(crabka_pgcatalog::RelationName, Sequence)>,
    Vec<crabka_pgcatalog::NewIndex>,
    Vec<PendingForeignKey>,
);

/// Build an inherited table by prepending each distinct parent column and
/// carrying inherited checks into the child's own catalog schema.
fn inherited_table_definition(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
    parents: &[crabka_pgcatalog::RelationName],
    columns: &[crabka_pgparser::ast::ColumnDef],
    constraints: &[crabka_pgparser::ast::TableConstraint],
    like: &[crabka_pgparser::ast::LikeClause],
    ctx: &crate::clock::EvalCtx,
) -> Result<TableDefinition, ExecError> {
    let (local_columns, mut checks, sequences, indexes, foreign_keys) =
        create_table_definition(kv, name, columns, constraints, like, ctx)?;
    let mut merged = Vec::<Column>::new();
    let mut inherited_checks = Vec::new();
    for parent_name in parents {
        let parent = crabka_pgcatalog::get_table(kv, parent_name)?;
        for column in parent.columns {
            if let Some(existing) = merged.iter_mut().find(|item| item.name == column.name) {
                if existing.ty != column.ty {
                    return Err(ExecError::InvalidTableDefinition(format!(
                        "inherited column \"{}\" has a type conflict",
                        column.name
                    )));
                }
                existing.not_null |= column.not_null;
                if existing.default.is_none() {
                    existing.default = column.default;
                }
            } else {
                merged.push(column);
            }
        }
        inherited_checks.extend(parent.checks);
    }
    for column in local_columns {
        if merged.iter().any(|item| item.name == column.name) {
            return Err(ExecError::InvalidTableDefinition(format!(
                "column \"{}\" specified more than once",
                column.name
            )));
        }
        merged.push(column);
    }
    inherited_checks.append(&mut checks);
    Ok((merged, inherited_checks, sequences, indexes, foreign_keys))
}

/// Build the definition of a `CREATE TABLE … PARTITION OF parent`.
///
/// A partition declares no columns: it takes the parent's list and the parent's
/// `CHECK` constraints, and the written element list may only add qualifiers to
/// what it inherits.
fn partition_definition(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
    spec: &crabka_pgparser::ast::PartitionOf,
    constraints: &[crabka_pgparser::ast::TableConstraint],
    like: &[crabka_pgparser::ast::LikeClause],
    ctx: &crate::clock::EvalCtx,
) -> Result<TableDefinition, ExecError> {
    let resolution = ctx.resolution();
    let parent_name = &resolve_relation(kv, resolution, &spec.parent, SchemaDisposition::Utility)?;
    let parent = crabka_pgcatalog::get_table(kv, parent_name)?;
    if crate::partition::scheme_of(kv, parent_name)?.is_none() {
        return Err(ExecError::NotPartitioned(parent_name.to_string()));
    }
    let (_, extra_checks, sequences, indexes, foreign_keys) =
        create_table_definition(kv, name, &[], constraints, like, ctx)?;
    let mut columns = parent.columns.clone();
    for (column, qualifiers) in &spec.column_options {
        let target = columns
            .iter_mut()
            .find(|candidate| candidate.name == *column)
            .ok_or_else(|| ExecError::UndefinedTableColumn {
                column: column.clone(),
                table: name.to_string(),
            })?;
        for qualifier in qualifiers {
            match &qualifier.kind {
                crabka_pgparser::ast::ColumnConstraintKind::NotNull => target.not_null = true,
                crabka_pgparser::ast::ColumnConstraintKind::Null => target.not_null = false,
                crabka_pgparser::ast::ColumnConstraintKind::Default(expr) => {
                    let value = crate::eval::eval(expr, &Scope::empty(), &[], ctx)?;
                    target.default = Some(ColumnDefault::Value(coerce(value, target.ty, ctx)?));
                }
                other => {
                    return Err(ExecError::Unsupported(format!(
                        "{other:?} on a partition's inherited column is not supported"
                    )));
                }
            }
        }
    }
    let mut checks = parent.checks.clone();
    checks.extend(extra_checks);
    Ok((columns, checks, sequences, indexes, foreign_keys))
}

/// Resolve a written `PARTITION BY` clause into the stored partition key.
fn partition_scheme_from_ast(
    spec: &crabka_pgparser::ast::PartitionBy,
    columns: &[Column],
    indexes: &[crabka_pgcatalog::NewIndex],
) -> Result<crate::partition::Scheme, ExecError> {
    use crate::partition::Strategy;
    let strategy = match spec.strategy.as_str() {
        "range" => Strategy::Range,
        "list" => Strategy::List,
        "hash" => Strategy::Hash,
        other => {
            return Err(ExecError::UnrecognizedPartitionStrategy(other.to_string()));
        }
    };
    let keys = crate::partition::key_columns(strategy, &spec.keys, columns)?;
    // PostgreSQL cannot enforce a unique constraint across partitions unless
    // every partition-key column is part of the key, because two partitions
    // never see each other's rows.
    for index in indexes {
        if let Some(missing) = keys.iter().find(|key| !index.columns.contains(&key.name)) {
            let kind = match index.constraint {
                Some(crabka_pgcatalog::IndexConstraint::PrimaryKey) => "PRIMARY KEY",
                _ => "UNIQUE",
            };
            return Err(ExecError::Unsupported(format!(
                "unique constraint on partitioned table must include all partitioning columns: \
                 the {kind} constraint lacks column \"{}\" which is part of the partition key",
                missing.name
            )));
        }
    }
    Ok(crate::partition::Scheme { strategy, keys })
}

/// Validate a written partition bound against its parent and resolve it into
/// the stored form, returning `(parent, bound)`.
fn partition_attachment(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
    spec: &crabka_pgparser::ast::PartitionOf,
    columns: &[Column],
    ctx: &crate::clock::EvalCtx,
) -> Result<(crabka_pgcatalog::RelationName, crate::partition::Bound), ExecError> {
    let resolution = ctx.resolution();
    let parent_name = resolve_relation(kv, resolution, &spec.parent, SchemaDisposition::Utility)?;
    let scheme = crate::partition::scheme_of(kv, &parent_name)?
        .ok_or_else(|| ExecError::NotPartitioned(parent_name.to_string()))?;
    let bound = resolve_partition_bound(&spec.bound, &scheme, columns, ctx)?;
    let siblings = crate::partition::partitions_of(kv, &parent_name)?;
    crate::partition::check_bound_shape(scheme.strategy, &bound)?;
    crate::partition::check_hash_bound(&bound)?;
    crate::partition::check_range_not_empty(name, &bound)?;
    crate::partition::check_no_overlap(scheme.strategy, name, &bound, &siblings)?;
    Ok((parent_name, bound))
}

/// Evaluate a written bound's constant expressions and coerce each to the type
/// of the partition-key column it bounds.
fn resolve_partition_bound(
    bound: &crabka_pgparser::ast::PartitionBound,
    scheme: &crate::partition::Scheme,
    columns: &[Column],
    ctx: &crate::clock::EvalCtx,
) -> Result<crate::partition::Bound, ExecError> {
    use crabka_pgparser::ast::{PartitionBound as Written, RangeBoundValue};

    use crate::partition::{Bound, RangeDatum};

    let key_type = |index: usize| -> Result<crabka_pgtypes::ColumnType, ExecError> {
        let key = scheme.keys.get(index).ok_or_else(|| {
            ExecError::InvalidTableDefinition(format!(
                "{} must specify exactly one value per partitioning column",
                if matches!(bound, Written::List(_)) {
                    "IN"
                } else {
                    "FROM"
                }
            ))
        })?;
        crate::partition::key_column_type(columns, key)
    };
    let value = |expr: &Expr, index: usize| -> Result<Datum, ExecError> {
        check_partition_bound_expr(expr)?;
        let evaluated = crate::eval::eval(expr, &Scope::empty(), &[], ctx)?;
        coerce(evaluated, key_type(index)?, ctx)
    };

    match bound {
        Written::Default => Ok(Bound::Default),
        Written::List(values) => values
            .iter()
            .map(|expr| value(expr, 0))
            .collect::<Result<Vec<_>, _>>()
            .map(Bound::List),
        Written::Range { from, to } => {
            let side = |written: &[RangeBoundValue]| -> Result<Vec<RangeDatum>, ExecError> {
                if written.len() != scheme.keys.len() {
                    return Err(ExecError::InvalidTableDefinition(
                        "FROM must specify exactly one value per partitioning column".into(),
                    ));
                }
                written
                    .iter()
                    .enumerate()
                    .map(|(index, written)| match written {
                        RangeBoundValue::MinValue => Ok(RangeDatum::MinValue),
                        RangeBoundValue::MaxValue => Ok(RangeDatum::MaxValue),
                        RangeBoundValue::Value(expr) => value(expr, index).map(RangeDatum::Value),
                    })
                    .collect()
            };
            Ok(Bound::Range {
                from: side(from)?,
                to: side(to)?,
            })
        }
        Written::Hash { modulus, remainder } => Ok(Bound::Hash {
            modulus: *modulus,
            remainder: *remainder,
        }),
    }
}

/// `PostgreSQL` allows only an immutable constant expression in a partition
/// bound, and reports each disallowed construct with its own SQLSTATE.
fn check_partition_bound_expr(expr: &Expr) -> Result<(), ExecError> {
    match expr {
        Expr::Column { .. } => {
            return Err(ExecError::Unsupported(
                "cannot use column reference in partition bound expression".into(),
            ));
        }
        Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery { .. } => {
            return Err(ExecError::Unsupported(
                "cannot use subquery in partition bound".into(),
            ));
        }
        Expr::Func(call) if crate::agg::is_aggregate_name(&call.name) => {
            return Err(ExecError::Grouping(
                "aggregate functions are not allowed in partition bound".into(),
            ));
        }
        _ => {}
    }
    for child in expr_children(expr) {
        check_partition_bound_expr(child)?;
    }
    Ok(())
}

/// Build the catalog column list and `CHECK` list for a `CREATE TABLE`,
/// expanding any `LIKE` clauses first (they contribute columns ahead of the
/// explicitly written ones, in clause order, exactly like `PostgreSQL`).
fn create_table_definition(
    kv: &dyn Kv,
    name: &crabka_pgcatalog::RelationName,
    columns: &[crabka_pgparser::ast::ColumnDef],
    constraints: &[crabka_pgparser::ast::TableConstraint],
    like: &[crabka_pgparser::ast::LikeClause],
    ctx: &crate::clock::EvalCtx,
) -> Result<TableDefinition, ExecError> {
    let resolution = ctx.resolution();
    let mut cols: Vec<Column> = Vec::new();
    let mut checks: Vec<crabka_pgcatalog::CheckConstraint> = Vec::new();
    let mut sequences: Vec<(crabka_pgcatalog::RelationName, Sequence)> = Vec::new();
    let mut indexes: Vec<crabka_pgcatalog::NewIndex> = Vec::new();
    let mut foreign_keys: Vec<PendingForeignKey> = Vec::new();

    for clause in like {
        let source_name =
            &resolve_relation(kv, resolution, &clause.source, SchemaDisposition::Utility)?;
        let source = crabka_pgcatalog::get_table(kv, source_name)?;
        for column in &source.columns {
            let mut copied = column.clone();
            // NOT NULL always rides along; DEFAULT and IDENTITY only when asked.
            if !clause.includes(crabka_pgparser::ast::LikeOption::Defaults)
                && copied.identity.is_none()
            {
                copied.default = None;
            }
            if !clause.includes(crabka_pgparser::ast::LikeOption::Identity)
                && copied.identity.is_some()
            {
                copied.identity = None;
                copied.default = None;
            }
            cols.push(copied);
        }
        if clause.includes(crabka_pgparser::ast::LikeOption::Constraints) {
            for check in &source.checks {
                let taken: Vec<&str> = checks.iter().map(|c| c.name.as_str()).collect();
                let name = unique_constraint_name(&taken, &check.name);
                checks.push(crabka_pgcatalog::CheckConstraint {
                    name,
                    expr: check.expr.clone(),
                    validated: check.validated,
                });
            }
        }
        if clause.includes(crabka_pgparser::ast::LikeOption::Indexes) {
            for index in crabka_pgcatalog::list_table_indexes(kv, source_name)? {
                let Some(constraint) = index.constraint else {
                    continue;
                };
                indexes.push(crabka_pgcatalog::NewIndex {
                    name: constraint_index_name(
                        name,
                        &index.columns,
                        constraint == crabka_pgcatalog::IndexConstraint::PrimaryKey,
                    ),
                    columns: index.columns.clone(),
                    unique: true,
                    placement: crabka_pgcatalog::IndexPlacement::Local,
                    method: crabka_pgcatalog::IndexMethod::Btree,
                    constraint: Some(constraint),
                });
            }
        }
    }

    let primary_key_columns = create_table_primary_key_columns(columns, constraints);
    for column in columns {
        cols.push(column_from_ast(
            name,
            column,
            ctx,
            &mut sequences,
            &primary_key_columns,
        )?);
    }
    let column_names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
    for column in columns {
        for constraint in &column.constraints {
            match &constraint.kind {
                crabka_pgparser::ast::ColumnConstraintKind::Check(predicate) => {
                    let taken = non_check_constraint_names(&indexes, &foreign_keys);
                    push_table_check(
                        &mut checks,
                        name,
                        constraint.name.as_deref(),
                        &predicate.text,
                        &column_names,
                        &taken,
                    )?;
                }
                crabka_pgparser::ast::ColumnConstraintKind::PrimaryKey => {
                    indexes.push(named_constraint_index(
                        constraint.name.as_deref(),
                        name,
                        std::slice::from_ref(&column.name),
                        true,
                    ));
                }
                crabka_pgparser::ast::ColumnConstraintKind::Unique { .. } => {
                    indexes.push(named_constraint_index(
                        constraint.name.as_deref(),
                        name,
                        std::slice::from_ref(&column.name),
                        false,
                    ));
                }
                // A column-level REFERENCES is a one-column FOREIGN KEY, named
                // and resolved exactly as the table-level spelling is.
                crabka_pgparser::ast::ColumnConstraintKind::References(reference) => {
                    push_pending_foreign_key(
                        &mut foreign_keys,
                        &checks,
                        &indexes,
                        name,
                        &AddForeignKey {
                            name: constraint.name.as_deref(),
                            columns: std::slice::from_ref(&column.name),
                            reference,
                            attributes: constraint.attributes,
                        },
                    )?;
                }
                _ => {}
            }
        }
    }
    for constraint in constraints {
        match &constraint.kind {
            crabka_pgparser::ast::TableConstraintKind::Check(predicate) => {
                let taken = non_check_constraint_names(&indexes, &foreign_keys);
                push_table_check(
                    &mut checks,
                    name,
                    constraint.name.as_deref(),
                    &predicate.text,
                    &column_names,
                    &taken,
                )?;
            }
            crabka_pgparser::ast::TableConstraintKind::PrimaryKey(key) => {
                indexes.push(named_constraint_index(
                    constraint.name.as_deref(),
                    name,
                    key,
                    true,
                ));
            }
            crabka_pgparser::ast::TableConstraintKind::Unique { columns: key, .. } => {
                indexes.push(named_constraint_index(
                    constraint.name.as_deref(),
                    name,
                    key,
                    false,
                ));
            }
            crabka_pgparser::ast::TableConstraintKind::ForeignKey {
                columns: key,
                references,
            } => {
                push_pending_foreign_key(
                    &mut foreign_keys,
                    &checks,
                    &indexes,
                    name,
                    &AddForeignKey {
                        name: constraint.name.as_deref(),
                        columns: key,
                        reference: references,
                        attributes: constraint.attributes,
                    },
                )?;
            }
            crabka_pgparser::ast::TableConstraintKind::Exclude { method, elements } => {
                indexes.push(exclusion_constraint_index(
                    constraint.name.as_deref(),
                    name,
                    &cols,
                    method,
                    elements,
                )?);
            }
        }
    }
    // Resolve every CHECK against the finished column list up front, so an
    // unknown column is a 42703 at DDL time rather than at the first INSERT.
    let table_for_validation = Table {
        id: 0,
        name: name.clone(),
        columns: cols.clone(),
        sharded: false,
        sharding: None,
        foreign: None,
        checks: checks.clone(),
    };
    for check in &checks {
        validate_check_predicate(&table_for_validation, &check.expr)?;
    }
    validate_generation_expressions(&table_for_validation)?;
    // PostgreSQL keeps one constraint namespace per relation, so a name shared
    // by constraints of *different* kinds is 42710. Two index-backed
    // constraints collide on the index name first, which the catalog reports as
    // 42P07 instead.
    for check in &checks {
        if indexes.iter().any(|index| index.name == check.name)
            || foreign_keys.iter().any(|fk| fk.name == check.name)
        {
            return Err(ExecError::DuplicateObject(format!(
                "constraint \"{}\" for relation \"{name}\" already exists",
                check.name
            )));
        }
    }
    Ok((cols, checks, sequences, indexes, foreign_keys))
}

/// One `FOREIGN KEY` clause a `CREATE TABLE` collected, with its name already
/// drawn from the relation's constraint namespace.
///
/// Resolution waits until the relation's own id and its indexes' ids exist,
/// because `CREATE TABLE t (… REFERENCES t …)` has to resolve against them and
/// no catalog read can find them yet.
struct PendingForeignKey {
    name: String,
    columns: Vec<String>,
    reference: crabka_pgparser::ast::ForeignKeyRef,
    attributes: crabka_pgparser::ast::ConstraintAttributes,
}

/// The constraint names a `CREATE TABLE` has assigned to things that are not
/// `CHECK`s. There is one namespace per relation, so a `CHECK` must step
/// around them.
fn non_check_constraint_names<'a>(
    indexes: &'a [crabka_pgcatalog::NewIndex],
    foreign_keys: &'a [PendingForeignKey],
) -> Vec<&'a str> {
    indexes
        .iter()
        .map(|index| index.name.as_str())
        .chain(foreign_keys.iter().map(|fk| fk.name.as_str()))
        .collect()
}

/// Collect one `FOREIGN KEY` clause, assigning the name `PostgreSQL` would:
/// the explicit `CONSTRAINT <name>` when written (42710 if the relation already
/// uses it for a constraint of any kind), else `<table>_<col>…_fkey` with the
/// lowest free numeric suffix.
fn push_pending_foreign_key(
    foreign_keys: &mut Vec<PendingForeignKey>,
    checks: &[crabka_pgcatalog::CheckConstraint],
    indexes: &[crabka_pgcatalog::NewIndex],
    table_name: &crabka_pgcatalog::RelationName,
    request: &AddForeignKey<'_>,
) -> Result<(), ExecError> {
    let taken: Vec<&str> = checks
        .iter()
        .map(|check| check.name.as_str())
        .chain(indexes.iter().map(|index| index.name.as_str()))
        .chain(foreign_keys.iter().map(|fk| fk.name.as_str()))
        .collect();
    let name = match request.name {
        Some(name) => {
            if taken.contains(&name) {
                return Err(ExecError::DuplicateObject(format!(
                    "constraint \"{name}\" for relation \"{table_name}\" already exists"
                )));
            }
            name.to_string()
        }
        None => unique_constraint_name(
            &taken,
            &crate::fk::default_foreign_key_name(table_name, request.columns),
        ),
    };
    foreign_keys.push(PendingForeignKey {
        name,
        columns: request.columns.to_vec(),
        reference: request.reference.clone(),
        attributes: request.attributes,
    });
    Ok(())
}

/// Append one `CREATE TABLE` `CHECK`, applying `PostgreSQL`'s naming rules: an
/// explicit `CONSTRAINT <name>` that collides with a name already assigned in
/// this same statement is 42710, while a generated name takes the lowest free
/// numeric suffix.
fn push_table_check(
    checks: &mut Vec<crabka_pgcatalog::CheckConstraint>,
    table_name: &crabka_pgcatalog::RelationName,
    explicit: Option<&str>,
    predicate: &str,
    column_names: &[String],
    other_names: &[&str],
) -> Result<(), ExecError> {
    let name = match explicit {
        Some(name) => {
            if checks.iter().any(|check| check.name == name) {
                return Err(ExecError::DuplicateObject(format!(
                    "check constraint \"{name}\" already exists"
                )));
            }
            if other_names.contains(&name) {
                return Err(ExecError::DuplicateObject(format!(
                    "constraint \"{name}\" for relation \"{table_name}\" already exists"
                )));
            }
            name.to_string()
        }
        None => {
            let mut taken: Vec<&str> = checks.iter().map(|check| check.name.as_str()).collect();
            taken.extend_from_slice(other_names);
            unique_constraint_name(
                &taken,
                &default_check_name(table_name, predicate, column_names),
            )
        }
    };
    checks.push(crabka_pgcatalog::CheckConstraint {
        name,
        expr: predicate.to_string(),
        validated: true,
    });
    Ok(())
}

/// Reject a `GENERATED ALWAYS AS (…) STORED` expression `PostgreSQL` refuses at
/// DDL time. A generation expression may read only plain stored columns of the
/// same row: another generated column is 42P17 (`PostgreSQL` has no ordering
/// guarantee that would make it well-defined), and a subquery or aggregate is
/// 0A000 / 42803.
fn validate_generation_expressions(table: &Table) -> Result<(), ExecError> {
    use crabka_pgparser::ast::Expr;

    let scope = Scope::single(table, &table.name.name);
    for column in &table.columns {
        let Some(source) = &column.generated else {
            continue;
        };
        let expr = crabka_pgparser::parser::parse_expression(source)?;
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
                    "cannot use subquery in column generation expression".into(),
                )),
                Expr::Column {
                    table: qualifier,
                    name,
                } => match scope.resolve(qualifier.as_deref(), name) {
                    Err(_) => Some(ExecError::UndefinedColumn(name.clone())),
                    Ok(index) if table.columns[index].generated.is_some() => {
                        Some(ExecError::InvalidObjectDefinition(format!(
                            "cannot use generated column \"{name}\" in column generation expression"
                        )))
                    }
                    Ok(_) => None,
                },
                // PostgreSQL stores a generated column's value, so the
                // expression has to be IMMUTABLE: anything reading the clock,
                // the session, a sequence, or a random source would make the
                // stored value disagree with its own expression on the next
                // write.
                Expr::Func(call) if !is_immutable_function(&call.name) => {
                    Some(ExecError::InvalidObjectDefinition(
                        "generation expression is not immutable".into(),
                    ))
                }
                _ => None,
            };
        });
        if let Some(error) = rejection {
            return Err(error);
        }
        if crate::agg::contains_aggregate(&expr) {
            return Err(ExecError::Grouping(
                "aggregate functions are not allowed in column generation expressions".into(),
            ));
        }
    }
    Ok(())
}

/// Whether a built-in function name is `IMMUTABLE` in `PostgreSQL`'s sense.
///
/// Only the non-immutable built-ins crabka implements are listed: everything
/// else it can evaluate is a pure function of its arguments, so an unknown name
/// is immutable by default and the call is rejected elsewhere if it does not
/// exist at all.
fn is_immutable_function(name: &str) -> bool {
    !matches!(
        name,
        // The clock family — VOLATILE (`clock_timestamp`) or STABLE (the rest).
        "now"
            | "current_timestamp"
            | "transaction_timestamp"
            | "statement_timestamp"
            | "clock_timestamp"
            | "current_date"
            | "current_time"
            | "localtime"
            | "localtimestamp"
            | "timeofday"
            // Random sources.
            | "random"
            | "random_normal"
            | "gen_random_uuid"
            | "uuid_generate_v4"
            // Sequence access reads, and may advance, session state.
            | "nextval"
            | "currval"
            | "lastval"
            | "setval"
            // Session, database, and installation identity.
            | "current_user"
            | "session_user"
            | "user"
            | "current_role"
            | "current_catalog"
            | "current_database"
            | "current_schema"
            | "current_schemas"
            | "current_setting"
            | "set_config"
            | "version"
            | "pg_backend_pid"
            | "pg_postmaster_start_time"
            | "txid_current"
            | "pg_current_xact_id"
            | "inet_client_addr"
            | "inet_server_addr"
    )
}

/// Compute a newly added `GENERATED ALWAYS AS (…) STORED` column's value for
/// every stored row version.
///
/// `PostgreSQL` rewrites the table when `ALTER TABLE … ADD COLUMN` carries a
/// generation expression, so the rows that already exist hold the computed
/// value rather than NULL. As in the `SET DATA TYPE` rewrite, a version no
/// snapshot can reach again must not be able to fail the statement.
fn backfill_generated_column(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    index: usize,
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    let Some(source) = state.table.columns[index].generated.clone() else {
        return Ok(());
    };
    let ty = state.table.columns[index].ty;
    let expr = crabka_pgparser::parser::parse_expression(&source)?;
    let table_name = state.table.name.clone();
    let scope = Scope::single(&state.table, &table_name.name);
    let computed = state
        .rows_mut(kv)?
        .iter()
        .map(|(_, xmin, xmax, row)| {
            let value = crate::eval::eval(&expr, &scope, row, ctx).and_then(|v| coerce(v, ty, ctx));
            match value {
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
    for ((_, _, _, row), value) in state.rows_mut(kv)?.iter_mut().zip(computed) {
        if index < row.len() {
            row[index] = value;
        }
    }
    Ok(())
}

/// A foreign key on a partitioned relation, or on one of its partitions, is
/// refused for this wave.
///
/// [`crate::fk::resolve_foreign_key`] refuses a *sharded* relation itself, but
/// [`Table`] carries no partition flag, because the partition scheme lives in
/// its own metadata. So only the DDL caller can raise this.
fn reject_partitioned_foreign_key(constraint: &str) -> ExecError {
    ExecError::Unsupported(format!(
        "foreign key constraint \"{constraint}\" on a partitioned table is not supported"
    ))
}

/// The [`crabka_pgcatalog::Index`] records an index batch allocated, read back
/// out of the batch itself.
///
/// A `CREATE TABLE` whose `FOREIGN KEY` references the relation being created
/// has to name the unique index proving its referenced columns are a key, and
/// that index exists only as a staged write until the statement commits. This
/// is where a caller can observe its allocated id. Values that are not index
/// records
/// (the next-id counter) simply fail to decode, and only the names this batch
/// asked for are kept.
fn staged_indexes_of(
    ops: &[crabka_pgkv::WriteOp],
    pending: &[crabka_pgcatalog::NewIndex],
) -> Vec<crabka_pgcatalog::Index> {
    let mut staged: Vec<crabka_pgcatalog::Index> = Vec::with_capacity(pending.len());
    for op in ops {
        let crabka_pgkv::WriteOp::Put { value, .. } = op else {
            continue;
        };
        let Ok(index) = crabka_pgcatalog::serde::deserialize_index(value) else {
            continue;
        };
        if pending.iter().any(|new| new.name == index.name)
            && !staged.iter().any(|seen| seen.name == index.name)
        {
            staged.push(index);
        }
    }
    staged
}

fn named_constraint_index(
    explicit: Option<&str>,
    table_name: &crabka_pgcatalog::RelationName,
    columns: &[String],
    primary_key: bool,
) -> crabka_pgcatalog::NewIndex {
    let mut index = create_table_constraint_index(table_name, columns, primary_key);
    if let Some(name) = explicit {
        index.name = name.to_string();
    }
    index
}

fn exclusion_constraint_index(
    explicit: Option<&str>,
    table_name: &crabka_pgcatalog::RelationName,
    table_columns: &[Column],
    method: &str,
    elements: &[crabka_pgparser::ast::ExclusionElement],
) -> Result<crabka_pgcatalog::NewIndex, ExecError> {
    if !method.eq_ignore_ascii_case("gist") {
        return Err(ExecError::Unsupported(format!(
            "exclusion constraints using access method \"{method}\" are not supported"
        )));
    }
    let mut columns = Vec::with_capacity(elements.len());
    let mut operators = Vec::with_capacity(elements.len());
    for element in elements {
        if !table_columns
            .iter()
            .any(|column| column.name == element.column)
        {
            return Err(ExecError::UndefinedColumn(element.column.clone()));
        }
        columns.push(element.column.clone());
        operators.push(match element.operator {
            crabka_pgparser::ast::BinaryOp::Eq => {
                crabka_pgcatalog::ExclusionOperator::Equal
            }
            crabka_pgparser::ast::BinaryOp::Overlaps => {
                crabka_pgcatalog::ExclusionOperator::Overlaps
            }
            _ => unreachable!("parser accepts only exclusion operators the executor supports"),
        });
    }
    let name = explicit.map_or_else(
        || format!("{}_{}_excl", table_name.name, columns.join("_")),
        str::to_string,
    );
    Ok(crabka_pgcatalog::NewIndex {
        name,
        columns,
        unique: false,
        placement: crabka_pgcatalog::IndexPlacement::Local,
        method: crabka_pgcatalog::IndexMethod::Gist,
        constraint: Some(crabka_pgcatalog::IndexConstraint::Exclusion(operators)),
    })
}

/// `PostgreSQL`'s default index name for a `PRIMARY KEY`/`UNIQUE` constraint.
///
/// It is built from the relation's own name, not its qualified spelling: the
/// index for `s1.t`'s primary key is `t_pkey`, sitting in `s1` beside the
/// table.
fn constraint_index_name(
    table_name: &crabka_pgcatalog::RelationName,
    columns: &[String],
    primary_key: bool,
) -> String {
    if primary_key {
        format!("{}_pkey", table_name.name)
    } else {
        format!("{}_{}_key", table_name.name, columns.join("_"))
    }
}

/// `PostgreSQL`'s default `CHECK` constraint name: `<table>_<column>_check` when
/// the predicate references exactly one of the table's columns, `<table>_check`
/// otherwise. The referenced set is taken from the predicate's identifier
/// tokens, so function names and literals never contribute.
fn default_check_name(
    table_name: &crabka_pgcatalog::RelationName,
    predicate: &str,
    columns: &[String],
) -> String {
    let mut referenced: Vec<&String> = Vec::new();
    if let Ok(tokens) = crabka_pgparser::lexer::lex(predicate) {
        for (token, _) in &tokens {
            let crabka_pgparser::token::Token::Ident(word) = token else {
                continue;
            };
            if let Some(column) = columns.iter().find(|name| *name == word)
                && !referenced.contains(&column)
            {
                referenced.push(column);
            }
        }
    }
    match referenced.as_slice() {
        [only] => format!("{}_{only}_check", table_name.name),
        _ => format!("{}_check", table_name.name),
    }
}

/// `PostgreSQL` disambiguates a colliding default constraint name by appending
/// the lowest free positive integer.
///
/// One relation has ONE constraint namespace, shared by `CHECK`s, index-backed
/// constraints and foreign keys alike, so `taken` is every constraint name of
/// every kind the relation already carries.
fn unique_constraint_name(taken: &[&str], base: &str) -> String {
    if !taken.contains(&base) {
        return base.to_string();
    }
    let mut suffix = 1u32;
    loop {
        let candidate = format!("{base}{suffix}");
        if !taken.iter().any(|name| *name == candidate) {
            return candidate;
        }
        suffix += 1;
    }
}

/// A table's `CHECK` predicates, re-parsed from their stored source text and
/// resolved against the table's columns.
pub(crate) struct CompiledCheck {
    pub(crate) name: String,
    pub(crate) expr: crabka_pgparser::ast::Expr,
}

/// Re-parse every stored `CHECK` predicate and verify it resolves against the
/// current column list. An unknown column surfaces as 42703 here.
pub(crate) fn compile_check_constraints(table: &Table) -> Result<Vec<CompiledCheck>, ExecError> {
    let scope = Scope::single(table, &table.name.name);
    table
        .checks
        .iter()
        .map(|check| {
            let expr = crabka_pgparser::parser::parse_expression(&check.expr)?;
            crate::eval::infer_type(&expr, &scope)?;
            Ok(CompiledCheck {
                name: check.name.clone(),
                expr,
            })
        })
        .collect()
}

/// Evaluate a table's `CHECK` predicates against one candidate row. A NULL
/// result passes, exactly like `PostgreSQL`'s three-valued rule.
pub(crate) fn enforce_check_constraints(
    table: &Table,
    checks: &[CompiledCheck],
    row: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    let scope = Scope::single(table, &table.name.name);
    for check in checks {
        let value = crate::eval::eval(&check.expr, &scope, row, ctx)?;
        if matches!(value, Datum::Bool(false)) {
            return Err(ExecError::CheckViolation {
                table: table.name.to_string(),
                constraint: check.name.clone(),
            });
        }
    }
    Ok(())
}

/// Compute the stored values of every `GENERATED ALWAYS AS (…) STORED` column
/// for one candidate row, in place.
pub(crate) fn apply_generated_columns(
    table: &Table,
    row: &mut [Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    if table
        .columns
        .iter()
        .all(|column| column.generated.is_none())
    {
        return Ok(());
    }
    let scope = Scope::single(table, &table.name.name);
    let snapshot = row.to_vec();
    for (index, column) in table.columns.iter().enumerate() {
        let Some(source) = &column.generated else {
            continue;
        };
        let expr = crabka_pgparser::parser::parse_expression(source)?;
        let value = crate::eval::eval(&expr, &scope, &snapshot, ctx)?;
        row[index] = coerce(value, column.ty, ctx)?;
    }
    Ok(())
}

/// One decoded row version: its physical key, `xmin`, `xmax`, and column values.
type RowVersion = (Vec<u8>, u64, u64, Vec<Datum>);

/// One table's stored row versions, decoded so a schema change can rewrite them
/// positionally inside the DDL batch. DDL holds the global catalog lock, so no
/// concurrent writer can add a version between the read and the rewrite.
fn scan_all_row_versions(kv: &dyn Kv, table: &Table) -> Result<Vec<RowVersion>, ExecError> {
    kv.scan_prefix(&crabka_pgkv::key::table_prefix(table.id))?
        .into_iter()
        .map(|(key, bytes)| {
            let (xmin, xmax, row) = crabka_pgmvcc::version::decode_tuple(&bytes)?;
            Ok((key, xmin, xmax, row))
        })
        .collect()
}

/// The live rows among already-decoded row versions, as `(rowid, xmin, row)`,
/// the shape [`scan_live`] returns. This one is derived from an in-flight
/// `ALTER TABLE`'s working set instead of from storage.
fn live_row_versions(
    kv: &dyn Kv,
    table: &Table,
    versions: &[RowVersion],
) -> Result<Vec<(u64, u64, Vec<Datum>)>, ExecError> {
    let snapshot = all_committed_snapshot();
    let status = global_status(kv, kv, &snapshot);
    let mut live: HashMap<u64, (u64, Vec<Datum>)> = HashMap::new();
    for (key, xmin, xmax, row) in versions {
        if !crabka_pgmvcc::visibility::satisfies_mvcc(*xmin, *xmax, &snapshot, None, &status)? {
            continue;
        }
        let rowid = physical_rowid(table, crabka_pgmvcc::version::row_prefix_of(key)?)?;
        // The MVCC at-most-one-live invariant means the greatest-xmin live
        // version wins, exactly as `scan_live_interval` selects it.
        let slot = live.entry(rowid).or_insert_with(|| (*xmin, row.clone()));
        if slot.0 < *xmin {
            *slot = (*xmin, row.clone());
        }
    }
    let mut out: Vec<(u64, u64, Vec<Datum>)> = live
        .into_iter()
        .map(|(rowid, (xmin, row))| (rowid, xmin, row))
        .collect();
    out.sort_by_key(|(rowid, ..)| *rowid);
    Ok(out)
}

/// Whether a stored row version is settled dead: its inserting transaction
/// aborted, or its deleting one committed. No snapshot can ever see such a
/// version again, so a column rewrite may put anything in it. `PostgreSQL`'s
/// own table rewrite discards them outright.
///
/// Deliberately stricter than "invisible under an all-committed snapshot": a
/// version deleted by a still-running transaction is *not* settled, because
/// that transaction may yet abort and resurrect it.
fn version_is_settled_dead(kv: &dyn Kv, xmin: u64, xmax: u64) -> Result<bool, ExecError> {
    use crabka_pgmvcc::clog::XidStatus;

    let snapshot = all_committed_snapshot();
    let status = global_status(kv, kv, &snapshot);
    if status(xmin)? == XidStatus::Aborted {
        return Ok(true);
    }
    if xmax == 0 {
        return Ok(false);
    }
    Ok(status(xmax)? == XidStatus::Committed)
}

/// The in-progress state of one `ALTER TABLE` statement: later subcommands see
/// the effect of earlier ones, and everything is emitted as one atomic batch.
struct AlterTableState {
    table: Table,
    /// Row versions, rewritten in place by column add/drop/type changes.
    rows: Option<Vec<RowVersion>>,
    ops: Vec<crabka_pgkv::WriteOp>,
    /// Names of secondary indexes dropped by this statement; a later action must
    /// not resurrect them.
    dropped_indexes: Vec<String>,
    /// Indexes created by this statement. They are not in the catalog yet, so a
    /// later action that has to rebuild them cannot find them by listing.
    created_indexes: Vec<crabka_pgcatalog::Index>,
    /// Columns already retyped by this statement; `PostgreSQL` refuses a second
    /// type change for the same column in one `ALTER TABLE`.
    retyped_columns: Vec<String>,
    /// Foreign keys this statement added. They are not in the catalog yet, so a
    /// later subcommand can only find them here: a name collision check, a
    /// `VALIDATE`, or a `DROP`.
    created_foreign_keys: Vec<crabka_pgcatalog::ForeignKey>,
    /// Names of foreign keys this statement dropped; a later subcommand must not
    /// resurrect them from the catalog.
    dropped_foreign_keys: Vec<String>,
    /// Creation-order ids for the foreign keys this statement adds. One cursor
    /// spans every subcommand, because none of their records reach the KV until
    /// the whole batch commits. Two `ADD CONSTRAINT`s that read the stored
    /// counter would otherwise tie, and a tie has no defined firing order.
    foreign_key_ids: crabka_pgcatalog::ForeignKeyIds,
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
struct StagedKv<'a> {
    base: &'a dyn Kv,
    /// `None` marks a key the batch deletes.
    staged: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
}

impl<'a> StagedKv<'a> {
    fn new(base: &'a dyn Kv, ops: &[crabka_pgkv::WriteOp]) -> Self {
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
    fn stage(&self, ops: &[crabka_pgkv::WriteOp]) {
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
fn merge_staged<'s>(
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

fn staged_kv_is_read_only() -> crabka_pgkv::KvError {
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
    fn rows_mut(&mut self, kv: &dyn Kv) -> Result<&mut Vec<RowVersion>, ExecError> {
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
    fn live_rows(&mut self, kv: &dyn Kv) -> Result<Vec<(u64, u64, Vec<Datum>)>, ExecError> {
        self.rows_mut(kv)?;
        let versions = self.rows.as_ref().expect("row versions were just loaded");
        live_row_versions(kv, &self.table, versions)
    }

    fn column_index(&self, column: &str) -> Result<usize, ExecError> {
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
    fn current_indexes(&self, kv: &dyn Kv) -> Result<Vec<crabka_pgcatalog::Index>, ExecError> {
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
    fn current_foreign_keys(
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
    fn taken_constraint_names(&self, kv: &dyn Kv) -> Result<Vec<String>, ExecError> {
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
    fn staged_catalog<'a>(&self, kv: &'a dyn Kv) -> Result<StagedKv<'a>, ExecError> {
        let mut ops = crabka_pgcatalog::replace_table_schema_ops(
            kv,
            &self.table.name,
            &self.table.columns,
            &self.table.checks,
        )?;
        ops.extend_from_slice(&self.ops);
        Ok(StagedKv::new(kv, &ops))
    }
}

/// `ALTER TABLE [IF EXISTS] name <action> [, …]`.
fn alter_table_ops(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    table_name: &crabka_pgcatalog::RelationName,
    if_exists: bool,
    actions: &[crabka_pgparser::ast::AlterTableAction],
    own_xid: Option<u64>,
    catalog: Option<&Arc<dyn Kv>>,
) -> Result<(QueryResult, Vec<crabka_pgkv::WriteOp>), ExecError> {
    use crabka_pgparser::ast::AlterTableAction as Action;

    // RENAME TO is a statement of its own in PostgreSQL's grammar, so it never
    // shares a comma list and keeps its dedicated catalog path.
    if let [Action::RenameTable { new_name }] = actions {
        // `RENAME TO` never moves a relation between schemas: the new name is
        // unqualified and lands beside the old one, exactly as in PostgreSQL.
        let new_name = &table_name.sibling(new_name);
        return match crabka_pgcatalog::rename_table_ops(kv, table_name, new_name) {
            Ok(mut ops) => {
                ops.extend(rename_relation_comment_ops(kv, table_name, new_name)?);
                ops.extend(rename_table_view_ops(kv, table_name, new_name)?);
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

    let table = match crabka_pgcatalog::get_table(kv, table_name) {
        Ok(table) => table,
        // A view is a relation, so PostgreSQL reports the *action* as
        // unsupported for it rather than claiming the relation is missing —
        // and IF EXISTS does not suppress that, because the relation exists.
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_))
            if crabka_pgcatalog::get_view(kv, table_name).is_ok() =>
        {
            let action = actions.first().map_or("ALTER", alter_action_label);
            return Err(ExecError::WrongObjectType(format!(
                "ALTER action {action} cannot be performed on relation \"{table_name}\""
            )));
        }
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) if if_exists => {
            return Ok((command("ALTER TABLE"), Vec::new()));
        }
        Err(error) => return Err(error.into()),
    };
    let mut state = AlterTableState {
        table,
        rows: None,
        ops: Vec::new(),
        dropped_indexes: Vec::new(),
        created_indexes: Vec::new(),
        retyped_columns: Vec::new(),
        created_foreign_keys: Vec::new(),
        dropped_foreign_keys: Vec::new(),
        foreign_key_ids: crabka_pgcatalog::ForeignKeyIds::default(),
    };
    for action in actions {
        alter_table_action_ops(kv, resolution, &mut state, action, own_xid, catalog)?;
    }

    // The schema record is written once, after every action has folded into the
    // working column/CHECK lists.
    let mut ops = crabka_pgcatalog::replace_table_schema_ops(
        kv,
        table_name,
        &state.table.columns,
        &state.table.checks,
    )?;
    if let Some(rows) = state.rows {
        for (key, xmin, xmax, row) in rows {
            ops.push(crabka_pgkv::WriteOp::Put {
                key,
                value: crabka_pgmvcc::version::encode_tuple(xmin, xmax, &row),
            });
        }
    }
    ops.extend(state.ops);
    Ok((command("ALTER TABLE"), ops))
}

/// How `PostgreSQL` names an `ALTER TABLE` subcommand in the 42809 it raises
/// when the relation's kind does not support it.
fn alter_action_label(action: &crabka_pgparser::ast::AlterTableAction) -> &'static str {
    use crabka_pgparser::ast::AlterTableAction as Action;

    match action {
        Action::AddColumn { .. } => "ADD COLUMN",
        Action::DropColumn { .. } => "DROP COLUMN",
        Action::SetType { .. } => "ALTER COLUMN ... SET DATA TYPE",
        Action::SetNotNull(_) => "ALTER COLUMN ... SET NOT NULL",
        Action::DropNotNull(_) => "ALTER COLUMN ... DROP NOT NULL",
        Action::SetDefault { .. } => "ALTER COLUMN ... SET DEFAULT",
        Action::DropDefault(_) => "ALTER COLUMN ... DROP DEFAULT",
        Action::AddConstraint(_) => "ADD CONSTRAINT",
        Action::DropConstraint { .. } => "DROP CONSTRAINT",
        Action::ValidateConstraint(_) => "VALIDATE CONSTRAINT",
        Action::RenameColumn { .. } => "RENAME COLUMN",
        Action::RenameConstraint { .. } => "RENAME CONSTRAINT",
        Action::RenameTable { .. } => "RENAME",
        Action::SetStorageParameters(_) => "SET",
        Action::ResetStorageParameters(_) => "RESET",
        Action::SetTablespace(_) => "SET TABLESPACE",
        Action::OwnerTo(_) => "OWNER TO",
        Action::SetTriggerMode { .. } => "ENABLE/DISABLE TRIGGER",
        Action::AttachPartition { .. } => "ATTACH PARTITION",
        Action::DetachPartition { .. } => "DETACH PARTITION",
        Action::Unsupported(_) => "ALTER",
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one arm per PostgreSQL ALTER TABLE subcommand; splitting them hides the \
              shared working-state contract"
)]
fn alter_table_action_ops(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    state: &mut AlterTableState,
    action: &crabka_pgparser::ast::AlterTableAction,
    own_xid: Option<u64>,
    catalog: Option<&Arc<dyn Kv>>,
) -> Result<(), ExecError> {
    use crabka_pgparser::ast::AlterTableAction as Action;

    let ddl_ctx = crate::clock::EvalCtx::for_ddl(resolution, catalog);
    let table_name = state.table.name.clone();
    match action {
        Action::AddColumn {
            if_not_exists,
            column,
        } => {
            if state.table.column_index(&column.name).is_some() {
                if *if_not_exists {
                    return Ok(());
                }
                return Err(ExecError::DuplicateColumn {
                    column: column.name.clone(),
                    table: table_name.to_string(),
                });
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
            for (_, _, _, row) in state.rows_mut(kv)? {
                row.push(fill.clone());
            }
            state.table.columns.push(catalog_column);
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
                for (_rowid, _xmin, row) in &state.live_rows(kv)? {
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
                            constraint.name.as_deref(),
                            std::slice::from_ref(&column.name),
                            primary_key,
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
            if !dependents.is_empty() || !generated.is_empty() {
                if !*cascade {
                    return Err(ExecError::DependentObjectsStillExist(format!(
                        "cannot drop column {column} of table {table_name} because other \
                         objects depend on it"
                    )));
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
        Action::SetNotNull(column) => {
            let index = state.column_index(column)?;
            let live = state.live_rows(kv)?;
            for (_rowid, _xmin, row) in &live {
                if row.get(index).is_none_or(Datum::is_null) {
                    return Err(ExecError::ColumnContainsNullValues {
                        column: column.clone(),
                        table: table_name.to_string(),
                    });
                }
            }
            state.table.columns[index].not_null = true;
            Ok(())
        }
        Action::DropNotNull(column) => {
            let index = state.column_index(column)?;
            state.table.columns[index].not_null = false;
            Ok(())
        }
        Action::SetDefault { column, expr } => {
            let index = state.column_index(column)?;
            let ty = state.table.columns[index].ty;
            let value = coerce(
                crate::eval::eval(expr, &Scope::empty(), &[], &ddl_ctx)?,
                ty,
                &ddl_ctx,
            )?;
            ensure_default_can_be_persisted(&value)?;
            state.table.columns[index].default = Some(ColumnDefault::Value(value));
            Ok(())
        }
        Action::DropDefault(column) => {
            let index = state.column_index(column)?;
            state.table.columns[index].default = None;
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
        Action::SetType { column, ty, using } => {
            let index = state.column_index(column)?;
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
                .map(|(_, xmin, xmax, row)| {
                    let cast = match using {
                        Some(expr) => crate::eval::eval(expr, &scope, row, &ddl_ctx)
                            .and_then(|value| coerce(value, *ty, &ddl_ctx)),
                        None => {
                            let value = row.get(index).cloned().unwrap_or(Datum::Null);
                            if value.is_null() {
                                Ok(Datum::Null)
                            } else {
                                crabka_pgtypes::cast::cast(&value, *ty, &ddl_ctx.time_zone)
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
            for ((_, _, _, row), value) in state.rows_mut(kv)?.iter_mut().zip(rewritten) {
                if index < row.len() {
                    row[index] = value;
                }
            }
            state.table.columns[index].ty = *ty;
            // Every CHECK is stored as source text and re-resolved on write, so
            // one that no longer type-checks has to fail the ALTER rather than
            // leave a table nothing can be written to.
            let checks = std::mem::take(&mut state.table.checks);
            let revalidated = checks
                .iter()
                .try_for_each(|check| validate_check_predicate(&state.table, &check.expr));
            state.table.checks = checks;
            revalidated?;
            rebuild_indexes_on_column(kv, state, column)?;
            state.retyped_columns.push(column.clone());
            Ok(())
        }
        Action::AddConstraint(constraint) => match &constraint.kind {
            crabka_pgparser::ast::TableConstraintKind::Check(predicate) => add_check_constraint(
                state,
                constraint.name.clone(),
                &predicate.text,
                !constraint.attributes.not_valid,
                kv,
                &ddl_ctx,
            ),
            crabka_pgparser::ast::TableConstraintKind::PrimaryKey(columns) => {
                reject_not_valid(constraint.attributes.not_valid, "PRIMARY KEY")?;
                add_constraint_index(kv, state, constraint.name.as_deref(), columns, true)
            }
            crabka_pgparser::ast::TableConstraintKind::Unique { columns, .. } => {
                reject_not_valid(constraint.attributes.not_valid, "UNIQUE")?;
                add_constraint_index(kv, state, constraint.name.as_deref(), columns, false)
            }
            // `reject_not_valid` is deliberately NOT called here: `NOT VALID`
            // applies to CHECK *and* FOREIGN KEY, the two kinds PostgreSQL can
            // validate lazily.
            crabka_pgparser::ast::TableConstraintKind::ForeignKey {
                columns,
                references,
            } => add_foreign_key_constraint(
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
            ),
            crabka_pgparser::ast::TableConstraintKind::Exclude { method, elements } => {
                reject_not_valid(constraint.attributes.not_valid, "EXCLUDE")?;
                let new_index = exclusion_constraint_index(
                    constraint.name.as_deref(),
                    &state.table.name,
                    &state.table.columns,
                    method,
                    elements,
                )?;
                add_exclusion_constraint(kv, state, new_index)
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
                for (_, _, row) in &state.live_rows(kv)? {
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
        // Heap storage parameters and ownership have no counterpart in Crabka's
        // storage/role model, and PostgreSQL's observable outcome for a table
        // that has neither is the same: the command succeeds and changes no
        // queryable state.
        Action::SetStorageParameters(_) | Action::ResetStorageParameters(_) => Ok(()),
        Action::OwnerTo(role) => {
            if crabka_pgcatalog::role_exists(kv, role)? || role == "current_user" {
                return Ok(());
            }
            Err(ExecError::UndefinedObject(format!(
                "role \"{role}\" does not exist"
            )))
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
        Action::RenameTable { .. } => Err(ExecError::Syntax(
            "RENAME TO cannot be combined with other ALTER TABLE subcommands".into(),
        )),
        Action::AttachPartition { partition, bound } => {
            let partition =
                &resolve_relation(kv, resolution, partition, SchemaDisposition::Utility)?;
            let ops = attach_partition_ops(kv, &state.table, partition, bound, &ddl_ctx)?;
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
        Action::Unsupported(label) => Err(ExecError::Unsupported(format!(
            "ALTER TABLE subcommand is not supported: {label}"
        ))),
    }
}

/// Validate and record `ALTER TABLE parent ATTACH PARTITION child <bound>`.
///
/// The candidate must have every column the parent has (42804 otherwise), and
/// every row it already stores must satisfy the bound being attached (23514
/// otherwise). `PostgreSQL` scans the table before it will attach it.
fn attach_partition_ops(
    kv: &dyn Kv,
    parent: &Table,
    child: &crabka_pgcatalog::RelationName,
    bound: &crabka_pgparser::ast::PartitionBound,
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let scheme = crate::partition::scheme_of(kv, &parent.name)?
        .ok_or_else(|| ExecError::NotPartitioned(parent.name.to_string()))?;
    let candidate = crabka_pgcatalog::get_table(kv, child)?;
    if let Some(missing) = parent
        .columns
        .iter()
        .find(|column| candidate.column_index(&column.name).is_none())
    {
        return Err(ExecError::ChildMissingColumn(missing.name.clone()));
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
    for (_, _, stored) in live_row_versions(kv, &candidate, &versions)? {
        let row = ordinals
            .iter()
            .map(|ordinal| stored.get(*ordinal).cloned().unwrap_or(Datum::Null))
            .collect::<Vec<_>>();
        if !crate::partition::satisfies(&scheme, &resolved, &siblings, &row)? {
            return Err(ExecError::PartitionConstraintViolationOnExistingRows(
                child.to_string(),
            ));
        }
    }
    let mut ops = crate::partition::attach_ops(&parent.name, child, &resolved);
    ops.extend(crate::trigger::clone_partition_triggers(kv, parent, child)?);
    Ok(ops)
}

/// `NOT VALID` applies only to constraints `PostgreSQL` can validate lazily:
/// `CHECK` and `FOREIGN KEY`. An index-backed constraint must be built now.
fn reject_not_valid(not_valid: bool, kind: &str) -> Result<(), ExecError> {
    if not_valid {
        return Err(ExecError::Unsupported(format!(
            "{kind} constraints cannot be marked NOT VALID"
        )));
    }
    Ok(())
}

/// One `FOREIGN KEY` clause as a DDL statement writes it, in either spelling:
/// `[CONSTRAINT <name>] FOREIGN KEY (…) REFERENCES …` or a column-level
/// `REFERENCES`. Shared by `CREATE TABLE` and every `ALTER TABLE` subcommand
/// that can carry one.
struct AddForeignKey<'a> {
    name: Option<&'a str>,
    columns: &'a [String],
    reference: &'a crabka_pgparser::ast::ForeignKeyRef,
    attributes: crabka_pgparser::ast::ConstraintAttributes,
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
fn add_foreign_key_constraint(
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
fn validate_foreign_key_against_state(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    foreign_key: &crabka_pgcatalog::ForeignKey,
    own_xid: Option<u64>,
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    let rows: Vec<Vec<Datum>> = state
        .live_rows(kv)?
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
fn drop_foreign_key_constraint(kv: &dyn Kv, state: &mut AlterTableState, name: &str) -> bool {
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
fn validate_check_predicate(table: &Table, predicate: &str) -> Result<(), ExecError> {
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
fn add_check_constraint(
    state: &mut AlterTableState,
    name: Option<String>,
    predicate: &str,
    valid: bool,
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
    let live = state.live_rows(kv)?;
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
fn check_references_column(predicate: &str, column: &str, columns: &[String]) -> bool {
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

fn add_constraint_index(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    name: Option<&str>,
    columns: &[String],
    primary_key: bool,
) -> Result<(), ExecError> {
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
    let key_column_indices = columns
        .iter()
        .map(|column| state.column_index(column))
        .collect::<Result<Vec<_>, _>>()?;
    let rows = state.live_rows(kv)?;
    let new_index = crabka_pgcatalog::NewIndex {
        name: name.map_or_else(
            || constraint_index_name(&state.table.name, columns, primary_key),
            str::to_string,
        ),
        columns: columns.to_vec(),
        unique: true,
        placement: crabka_pgcatalog::IndexPlacement::Local,
        method: crabka_pgcatalog::IndexMethod::Btree,
        constraint: Some(if primary_key {
            crabka_pgcatalog::IndexConstraint::PrimaryKey
        } else {
            crabka_pgcatalog::IndexConstraint::Unique
        }),
    };
    let (index, index_ops) =
        crabka_pgcatalog::create_constraint_index_ops(kv, &state.table, &new_index)?;
    // PostgreSQL builds the unique index before it attaches NOT NULL, so
    // duplicate data is 23505 even when the key column also holds NULLs.
    let backfill = local_index_backfill_ops_for_rows(&rows, &state.table, &index)?;
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
    state.created_indexes.push(index);
    Ok(())
}

fn add_exclusion_constraint(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    new_index: crabka_pgcatalog::NewIndex,
) -> Result<(), ExecError> {
    if state.table.sharded {
        return Err(ExecError::Unsupported(
            "exclusion constraints on sharded tables are not supported".into(),
        ));
    }
    let rows = state.live_rows(kv)?;
    let (index, index_ops) =
        crabka_pgcatalog::create_constraint_index_ops(kv, &state.table, &new_index)?;
    let Some(crabka_pgcatalog::IndexConstraint::Exclusion(operators)) = &index.constraint else {
        unreachable!("exclusion helper creates an exclusion index")
    };
    for (offset, (_rowid, _xmin, row)) in rows.iter().enumerate() {
        let left = indexed_values(&state.table, &index, row)?;
        for (_rowid, _xmin, other) in &rows[..offset] {
            let right = indexed_values(&state.table, &index, other)?;
            if exclusion_keys_conflict(operators, &left, &right)? {
                return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                    "23P01",
                    format!(
                        "could not create exclusion constraint \"{}\"",
                        index.name
                    ),
                )));
            }
        }
    }
    state.ops.extend(index_ops);
    state.created_indexes.push(index);
    Ok(())
}

/// Whether `ALTER TABLE … ALTER COLUMN … TYPE` may rewrite `from` to `to`
/// without an explicit `USING`.
///
/// `PostgreSQL` coerces the stored value in *assignment* context. On top of the
/// ordinary assignment casts that admits every I/O-conversion cast whose target
/// is a string type. `int4 → text` needs no `USING`, and `text → int4` does.
/// It also admits the temporal narrowings `PostgreSQL` marks assignment-level.
fn alter_type_cast_allowed(from: ColumnType, to: ColumnType) -> bool {
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
fn rebuild_indexes_on_column(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    column: &str,
) -> Result<(), ExecError> {
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
    let rows = state.live_rows(kv)?;
    for index in &affected {
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
            &rows,
            &state.table,
            index,
        )?);
        state.ops.extend(ops);
    }
    Ok(())
}

/// Remove one column from the working schema and every stored row version, and
/// drop the indexes, `CHECK`s and foreign keys that depended on it. This is
/// `PostgreSQL`'s own `DROP COLUMN` dependency handling.
///
/// A foreign key *keyed on* the dropped column goes with it, exactly as its
/// index does. A foreign key that *references* the column is a different matter:
/// it hangs off the unique index proving the column a key, so the refusal comes
/// out of [`drop_index_by_name`] as that index is dropped.
fn drop_table_column(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    column: &str,
    cascade: bool,
) -> Result<(), ExecError> {
    let Some(index) = state.table.column_index(column) else {
        return Ok(());
    };
    let table_name = state.table.name.clone();
    let triggers = crabka_pgcatalog::trigger::triggers_for_table(kv, state.table.id)?;
    for trigger in triggers {
        let references_column = trigger
            .events
            .update_columns
            .iter()
            .any(|name| name == column)
            || trigger.when.as_ref().is_some_and(|predicate| {
                check_references_column(
                    predicate,
                    column,
                    &state
                        .table
                        .columns
                        .iter()
                        .map(|column| column.name.clone())
                        .collect::<Vec<_>>(),
                )
            });
        if references_column {
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
    for (_, _, _, row) in state.rows_mut(kv)? {
        if index < row.len() {
            row.remove(index);
        }
    }
    state.table.columns.remove(index);
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
    let column_names: Vec<String> = state.table.columns.iter().map(|c| c.name.clone()).collect();
    state
        .table
        .checks
        .retain(|check| !check_references_column(&check.expr, column, &column_names));
    Ok(())
}

/// Drop one index and its entries, refusing when a foreign key chose it as the
/// index proving its referenced columns unique.
///
/// `dropped` is how the *user's* statement named the object, which is what the
/// 2BP01's primary message quotes; every `DETAIL` line names the index, because
/// that is what the constraint actually depends on. `CASCADE` drops the
/// referencing constraints rather than the referencing relations.
fn drop_index_by_name(
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
fn rename_column_dependencies(
    kv: &dyn Kv,
    state: &mut AlterTableState,
    old_name: &str,
    new_name: &str,
) -> Result<(), ExecError> {
    let table_name = state.table.name.clone();
    for check in &mut state.table.checks {
        check.expr = rewrite_identifier_tokens(&check.expr, old_name, new_name);
    }
    for mut index in crabka_pgcatalog::list_table_indexes(kv, &table_name)? {
        if !index
            .columns
            .iter()
            .any(|key| index_key_reads_column(&state.table, key, old_name))
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
fn generated_columns_reading(table: &Table, column: &str) -> Vec<String> {
    use crabka_pgparser::ast::Expr;

    let scope = Scope::single(table, &table.name.name);
    let target = table.column_index(column);
    table
        .columns
        .iter()
        .filter(|candidate| {
            let Some(source) = &candidate.generated else {
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

fn index_key_reads_column(table: &Table, key: &str, column: &str) -> bool {
    use crabka_pgparser::ast::Expr;

    let Some(source) = crabka_pgcatalog::index_key_expression(key) else {
        return key == column;
    };
    let Ok(expr) = crabka_pgparser::parser::parse_expression(source) else {
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

/// Whether a stored view's definition reads `table` at all.
///
/// Views are stored as SQL text, so the binding is decided from the `FROM`
/// list exactly as a rename decides it. When the catalog cannot resolve some
/// relation in that list the answer is a plain token match, so a dependency is
/// never missed.
fn view_reads_relation(
    kv: &dyn Kv,
    definition: &str,
    table: &crabka_pgcatalog::RelationName,
) -> bool {
    match view_relation_bindings(kv, definition, table) {
        Some(bindings) => !bindings.qualifiers.is_empty(),
        None => definition_mentions_identifier(definition, &table.name),
    }
}

/// Whether a stored view's definition reads `column` of `table`.
///
/// A reference counts when it is a qualified `q.<column>` under one of the
/// relation's qualifiers, or a bare `<column>` no other relation in the view's
/// `FROM` could supply. `SELECT *` reads every column. An unresolvable `FROM`
/// list answers "yes", so a dependency is never missed.
fn view_reads_column(
    kv: &dyn Kv,
    definition: &str,
    table: &crabka_pgcatalog::RelationName,
    column: &str,
) -> bool {
    use crabka_pgparser::token::Token;

    let Some(bindings) = view_relation_bindings(kv, definition, table) else {
        return true;
    };
    if bindings.qualifiers.is_empty() {
        return false;
    }
    let Ok(tokens) = crabka_pgparser::lexer::lex(definition) else {
        return true;
    };
    if tokens.iter().any(|(token, _)| *token == Token::Star) {
        return true;
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

/// Rewrite every stored view's references to a renamed relation.
///
/// `PostgreSQL` stores a view as a parsed rule over relation oids, so renaming
/// a table it reads is invisible to it; Crabka stores view *text*, so the
/// the substitution must happen. The rewrite touches only positions the token
/// walk can prove: a `FROM`/`JOIN` relation slot, and a `<table>.<column>`
/// qualifier when that item carries no alias. Any other occurrence of the name
/// is `0A000`, not a silent change of what the view returns.
fn rename_table_view_ops(
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
fn rewrite_view_relation_name(definition: &str, old_name: &str, new_name: &str) -> Option<String> {
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
fn view_from_item_qualifier(
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
fn substitute_identifier_tokens(
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

fn definition_mentions_identifier(definition: &str, name: &str) -> bool {
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
fn is_partitioned_ref(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    reference: &crabka_pgparser::ast::RelationRef,
) -> Result<bool, ExecError> {
    let name = resolve_relation(kv, resolution, reference, SchemaDisposition::Reference)?;
    crate::partition::is_partitioned(kv, &name)
}

/// The ops that clear the foreign keys blocking a `DROP TABLE`, or the 2BP01
/// that refuses it.
///
/// `CASCADE` drops the referencing *constraint*, not the referencing relation.
/// The child table survives, minus the key.
fn drop_blocking_foreign_keys(
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
    Ok(crabka_pgcatalog::list_views(kv)?
        .into_iter()
        .filter(|view| view_can_reach_schema(&view.name, &view.definition, &table.schema))
        .filter(|view| match column {
            Some(column) => view_reads_column(kv, &view.definition, table, column),
            None => view_reads_relation(kv, &view.definition, table),
        })
        .map(|view| view.name)
        .collect())
}

/// Whether a stored view could read anything in `schema` at all.
///
/// A definition is matched by its identifiers, not by resolved dependencies, so
/// an unqualified `FROM orders` matches every `orders` in the database. That is
/// harmless within one schema and wrong across schemas: a session's temporary
/// `orders` would otherwise carry off a permanent view over a different table of
/// the same name when the namespace is emptied. A view outside `schema` reaches
/// into it only by naming it, so requiring the qualifier confines the match.
fn view_can_reach_schema(
    view: &crabka_pgcatalog::RelationName,
    definition: &str,
    schema: &str,
) -> bool {
    view.schema == schema || definition_mentions_identifier(definition, schema)
}

/// How a stored view's `FROM` list binds the relation being renamed: the
/// qualifiers under which it is visible (its own name plus every alias), and
/// the column names every *other* referenced relation contributes.
struct ViewRelationBindings {
    qualifiers: Vec<String>,
    other_columns: Vec<String>,
}

/// The bindings for `table` inside `definition`. `None` means the definition
/// references a relation the catalog cannot resolve, so no rewrite may be
/// attempted.
fn view_relation_bindings(
    kv: &dyn Kv,
    definition: &str,
    table: &crabka_pgcatalog::RelationName,
) -> Option<ViewRelationBindings> {
    use crabka_pgparser::token::{Keyword, Token};
    let tokens = crabka_pgparser::lexer::lex(definition).ok()?;
    let mut qualifiers = Vec::new();
    let mut other_columns = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let is_relation_lead_in =
            matches!(
                &tokens[index].0,
                Token::Keyword(Keyword::From | Keyword::Join)
            ) || (index > 0 && matches!(&tokens[index].0, Token::Comma) && !qualifiers.is_empty());
        index += 1;
        if !is_relation_lead_in {
            continue;
        }
        // A derived table or a function call in FROM: the catalog cannot
        // resolve it, so the rename is not provably safe.
        let Token::Ident(relation) = &tokens[index.min(tokens.len() - 1)].0 else {
            return None;
        };
        let mut alias = relation.clone();
        let mut next = index + 1;
        if matches!(
            tokens.get(next).map(|token| &token.0),
            Some(Token::Keyword(Keyword::As))
        ) {
            next += 1;
        }
        if let Some((Token::Ident(candidate), _)) = tokens.get(next)
            && !is_query_tail_keyword(candidate)
        {
            alias = candidate.clone();
        }
        if *relation == table.name {
            qualifiers.push(relation.clone());
            qualifiers.push(alias);
        } else {
            let other = crabka_pgcatalog::get_table(kv, &table.sibling(relation)).ok()?;
            other_columns.extend(other.columns.into_iter().map(|column| column.name));
        }
    }
    qualifiers.sort();
    qualifiers.dedup();
    Some(ViewRelationBindings {
        qualifiers,
        other_columns,
    })
}

fn is_query_tail_keyword(word: &str) -> bool {
    matches!(
        word,
        "where" | "group" | "order" | "having" | "limit" | "offset" | "union" | "on" | "using"
    )
}

/// Rewrite one stored view definition, or `None` when it does not reference the
/// renamed column at all. Returns `0A000` when the rewrite cannot be proven safe.
fn rewrite_view_definition(
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
    let Some(bindings) = view_relation_bindings(kv, definition, table) else {
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
fn rewrite_identifier_tokens(source: &str, old_name: &str, new_name: &str) -> String {
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

/// Move a relation's comments (and its columns') to a new relation name.
fn rename_relation_comment_ops(
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
fn constraint_index_named(
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
/// exists. Tables, views, indexes, and sequences share one namespace in
/// `PostgreSQL`, so a name resolves to at most one of them.
fn relation_kind(kv: &dyn Kv, name: &crabka_pgcatalog::RelationName) -> Option<&'static str> {
    if crabka_pgcatalog::get_table(kv, name).is_ok() {
        Some("table")
    } else if crabka_pgcatalog::get_view(kv, name).is_ok() {
        Some("view")
    } else if crabka_pgcatalog::get_index(kv, name).is_ok() {
        Some("index")
    } else if crabka_pgcatalog::get_sequence(kv, name).is_ok() {
        Some("sequence")
    } else {
        None
    }
}

/// `COMMENT ON <kind> <name>` names one relation kind and `PostgreSQL` enforces
/// it: a name that resolves to a relation of a *different* kind is 42809, and
/// only a name that resolves to nothing at all is the 42P01 relation lookup
/// failure. Crabka has no materialized views or foreign tables, so those kinds
/// never match a relation it can find.
fn require_relation_kind(
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
    Err(ExecError::WrongObjectType(format!(
        "\"{name}\" is not {article} {requested}"
    )))
}

fn comment_ops(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    object_kind: &str,
    object_name: &str,
    comment: Option<&str>,
) -> Result<(QueryResult, Vec<crabka_pgkv::WriteOp>), ExecError> {
    use crabka_pgcatalog::CommentObject;

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
            let table = crabka_pgcatalog::get_table(kv, &relation)?;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crabka_pgcatalog::RelationName;
    use crabka_pgparser::ast::{QueryBody, SelectStmt, SetExpr, Statement};
    use crabka_pgwire::engine::{Cell, Engine, FieldDescription, QueryResult, Session};

    use crate::{
        ExecError, PartialAggregateFunction, PartialAggregateSpec, SqlEngine, SqlSession,
        TopKColumn, TopKSpec,
        plan_dist::DistributedScanPlan,
        scanner::{PredicatePushdown, ProjectionPushdown, ScanRequest, ScannedRow},
        scope::{ColumnBinding, Scope},
    };

    struct RejectingRangeScanner;

    impl crate::RangeScanner for RejectingRangeScanner {
        fn scan(&self, _request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, ExecError> {
            Err(ExecError::Unsupported(
                "test scanner rejects table scans".into(),
            ))
        }
    }

    #[test]
    fn scan_pushdown_retry_is_limited_to_optional_predicate_or_projection() {
        let error = ExecError::Unsupported("predicate pushdown unsupported".into());

        assert!(super::should_retry_without_scan_pushdown(
            &error,
            &DistributedScanPlan {
                predicate: PredicatePushdown::Conjunctive(Vec::new()),
                projection: ProjectionPushdown::All,
                partial_aggregate: None,
                top_k: None,
                text_search: None,
            },
        ));

        assert!(!super::should_retry_without_scan_pushdown(
            &error,
            &DistributedScanPlan {
                predicate: PredicatePushdown::FullScan,
                projection: ProjectionPushdown::All,
                partial_aggregate: Some(PartialAggregateSpec {
                    function: PartialAggregateFunction::Sum,
                    column: Some(0),
                    group_by: Vec::new(),
                }),
                top_k: None,
                text_search: None,
            },
        ));

        assert!(!super::should_retry_without_scan_pushdown(
            &error,
            &DistributedScanPlan {
                predicate: PredicatePushdown::FullScan,
                projection: ProjectionPushdown::All,
                partial_aggregate: None,
                top_k: Some(TopKSpec {
                    order_by: vec![TopKColumn {
                        column: 0,
                        asc: true,
                    }],
                    limit: 1,
                }),
                text_search: None,
            },
        ));
    }

    #[test]
    fn global_status_derefs_prepared_to_range0_global_clog() {
        use crabka_pgkv::{Kv, MemKv};
        use crabka_pgmvcc::{
            clog::{XidStatus, put_op},
            xid::GLOBAL_XID_BASE,
        };

        use super::global_status;
        let (local, global) = (MemKv::new(), MemKv::new());
        let li = 5u64;
        let g = GLOBAL_XID_BASE + 1;
        local
            .write_batch(&[put_op(li, XidStatus::Prepared(g))])
            .expect("put prepared marker");
        // G in-doubt (not in global clog, gsnap says running) => InProgress (invisible)
        let running = crabka_pgmvcc::visibility::Snapshot {
            xmin: g,
            xmax: g + 1,
            xip: vec![g],
        };
        assert_eq!(
            global_status(&local, &global, &running)(li).expect("resolve in-doubt"),
            XidStatus::InProgress
        );
        // G committed + settled (gsnap moved past it) => Committed (visible)
        global
            .write_batch(&[put_op(g, XidStatus::Committed)])
            .expect("put global commit");
        let settled = crabka_pgmvcc::visibility::Snapshot {
            xmin: g + 2,
            xmax: g + 2,
            xip: vec![],
        };
        assert_eq!(
            global_status(&local, &global, &settled)(li).expect("resolve settled"),
            XidStatus::Committed
        );
        // A plain local xid is unaffected.
        local
            .write_batch(&[put_op(3, XidStatus::Committed)])
            .expect("put local commit");
        assert_eq!(
            global_status(&local, &global, &settled)(3).expect("resolve local"),
            XidStatus::Committed
        );
    }

    #[test]
    fn durable_global_snapshot_resolves_committed_against_range0() {
        use crabka_pgkv::{Kv, MemKv};
        use crabka_pgmvcc::{
            clog::{XidStatus, put_op},
            xid::GLOBAL_XID_BASE,
        };
        let local = MemKv::new(); // this range's clog
        let global = MemKv::new(); // range 0's global clog + meta
        let g = GLOBAL_XID_BASE + 5;

        local
            .write_batch(&[put_op(3, XidStatus::Prepared(g))])
            .expect("local prepared");
        // Range 0: g committed, next_global persisted past g — BIG-ENDIAN, the exact
        // on-disk layout the GTM allocator writes (correction C1).
        global
            .write_batch(&[put_op(g, XidStatus::Committed)])
            .expect("global committed");
        global
            .write_batch(&[crabka_pgkv::WriteOp::Put {
                key: crabka_pgkv::key::meta_next_global_xid_key(),
                value: (g + 1).to_be_bytes().to_vec(),
            }])
            .expect("persist next_global");

        let gsnap = crate::session::durable_global_snapshot(&global).expect("rebuild gsnap");
        let resolve = crate::exec::global_status(&local, &global, &gsnap);
        assert_eq!(
            resolve(3).expect("resolve"),
            XidStatus::Committed,
            "committed cross-range deleter resolves Committed via range 0's durable clog"
        );

        let g2 = GLOBAL_XID_BASE + 6;
        local
            .write_batch(&[put_op(4, XidStatus::Prepared(g2))])
            .expect("local prepared 2");
        global
            .write_batch(&[crabka_pgkv::WriteOp::Put {
                key: crabka_pgkv::key::meta_next_global_xid_key(),
                value: (g2 + 1).to_be_bytes().to_vec(),
            }])
            .expect("advance next_global past g2");
        let gsnap2 = crate::session::durable_global_snapshot(&global).expect("rebuild gsnap2");
        let resolve2 = crate::exec::global_status(&local, &global, &gsnap2);
        assert_eq!(
            resolve2(4).expect("resolve g2"),
            XidStatus::InProgress,
            "allocated-but-undecided cross-range deleter is invisible"
        );
    }

    async fn run_s(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
        s.simple_query(sql).await.expect("ok")
    }

    /// The rows a query returns, as text, so a DDL test can state the whole
    /// expected table rather than probing it field by field.
    async fn text_rows_of(session: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
        let results = session.simple_query(sql).await.expect(sql);
        results
            .into_iter()
            .flat_map(|result| match result {
                QueryResult::Rows { rows, .. } => rows,
                QueryResult::Command { .. } | QueryResult::Empty => Vec::new(),
            })
            .map(|row| {
                row.into_iter()
                    .map(|cell| cell.map(|cell| String::from_utf8_lossy(&cell.text).into_owned()))
                    .collect()
            })
            .collect()
    }

    async fn sqlstate_of(session: &mut SqlSession, sql: &str) -> String {
        session
            .simple_query(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("{sql} must fail"))
            .code
    }

    fn text_row(values: &[&str]) -> Vec<Option<String>> {
        values
            .iter()
            .map(|value| Some((*value).to_string()))
            .collect()
    }

    /// Two output columns of the same name would define a relation whose columns
    /// cannot be told apart, so `CREATE VIEW` refuses it with `PostgreSQL`'s
    /// 42701 before it creates anything. `CREATE TABLE AS` applies the same
    /// rule.
    #[tokio::test]
    async fn create_view_refuses_duplicate_output_column_names() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run_s(&mut session, "CREATE TABLE t (id int4, label text)").await;
        run_s(&mut session, "INSERT INTO t VALUES (1, 'one')").await;

        for sql in [
            "CREATE VIEW v AS SELECT id, id FROM t",
            "CREATE VIEW v AS SELECT id AS k, label AS k FROM t",
            // Two unnamed expressions both label `?column?`.
            "CREATE VIEW v AS SELECT 1 + 1, 2 + 2 FROM t",
        ] {
            assert!(sqlstate_of(&mut session, sql).await == "42701", "{sql}");
            // Nothing was created, so the name is still free.
            assert!(
                sqlstate_of(&mut session, "SELECT * FROM v").await == "42P01",
                "{sql}"
            );
        }

        // Names that only differ by quoting do not collide.
        run_s(
            &mut session,
            "CREATE VIEW v AS SELECT id AS k, label AS \"K\" FROM t",
        )
        .await;
        assert!(
            text_rows_of(&mut session, "SELECT k, \"K\" FROM v").await
                == vec![text_row(&["1", "one"])]
        );
    }

    /// `COLLATE` is a postfix operator on the collated types. Every collation
    /// this engine has orders text by byte value, so a supported one is a no-op;
    /// an unsupported name is 42704 and a non-collatable operand is 42804, both
    /// as in `PostgreSQL`.
    #[tokio::test]
    async fn collate_is_typed_like_postgresql() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run_s(&mut session, "CREATE TABLE t (a int4, b text)").await;
        run_s(&mut session, "INSERT INTO t VALUES (2, 'b'), (1, 'a')").await;

        assert!(
            text_rows_of(&mut session, "SELECT b FROM t ORDER BY b COLLATE \"C\"").await
                == vec![text_row(&["a"]), text_row(&["b"])]
        );
        assert!(
            text_rows_of(&mut session, "SELECT b COLLATE \"POSIX\" FROM t ORDER BY 1").await
                == vec![text_row(&["a"]), text_row(&["b"])]
        );
        assert!(
            text_rows_of(&mut session, "SELECT b FROM t WHERE b COLLATE \"C\" = 'a'").await
                == vec![text_row(&["a"])]
        );

        for sql in [
            "SELECT a COLLATE \"C\" FROM t",
            "SELECT a FROM t ORDER BY a COLLATE \"C\"",
        ] {
            assert!(sqlstate_of(&mut session, sql).await == "42804", "{sql}");
        }
        for sql in [
            "SELECT b COLLATE \"en_US\" FROM t",
            "SELECT b COLLATE c FROM t",
        ] {
            assert!(sqlstate_of(&mut session, sql).await == "42704", "{sql}");
        }
    }

    /// A `CHECK` constraint is persisted and enforced on INSERT, UPDATE and
    /// COPY, with PostgreSQL's SQLSTATE and its three-valued NULL rule.
    #[tokio::test]
    async fn check_constraints_are_enforced_on_every_write_path() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run_s(
            &mut session,
            "CREATE TABLE t (a int4, b int4 CHECK (b > 0), CONSTRAINT ck CHECK (a + b < 100))",
        )
        .await;

        let cases: &[(&str, &str)] = &[
            ("INSERT INTO t VALUES (1, -1)", "23514"),
            ("INSERT INTO t VALUES (60, 60)", "23514"),
        ];
        for (sql, expected) in cases {
            assert!(sqlstate_of(&mut session, sql).await == *expected, "{sql}");
        }

        // A NULL predicate is not false, so the row is accepted.
        run_s(&mut session, "INSERT INTO t VALUES (1, NULL)").await;
        run_s(&mut session, "INSERT INTO t VALUES (2, 3)").await;
        assert!(
            text_rows_of(&mut session, "SELECT a, b FROM t ORDER BY a").await
                == vec![vec![Some("1".into()), None], text_row(&["2", "3"]),]
        );

        assert!(sqlstate_of(&mut session, "UPDATE t SET b = -5 WHERE a = 2").await == "23514");
        assert!(
            text_rows_of(&mut session, "SELECT a, b FROM t WHERE a = 2").await
                == vec![text_row(&["2", "3"])]
        );
    }

    /// PostgreSQL's default `CHECK` names: `<table>_<column>_check` when the
    /// predicate references exactly one column, `<table>_check` otherwise, and
    /// a numeric suffix on collision.
    #[tokio::test]
    async fn unnamed_check_constraints_take_postgresql_default_names() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run_s(
            &mut session,
            "CREATE TABLE t (a int4 CHECK (a > 0), b int4 CHECK (b > 0), CHECK (a < b), CHECK (a <> 5))",
        )
        .await;
        let table = crabka_pgcatalog::get_table(engine.catalog_kv(), &RelationName::public("t"))
            .expect("table");
        assert!(
            table
                .checks
                .iter()
                .map(|check| check.name.clone())
                .collect::<Vec<_>>()
                == vec!["t_a_check", "t_b_check", "t_check", "t_a_check1"]
        );
    }

    /// `ADD COLUMN` back-fills stored rows with the new column's default and
    /// `DROP COLUMN` reclaims the position, so later reads line up.
    #[tokio::test]
    async fn add_and_drop_column_rewrite_stored_rows() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run_s(&mut session, "CREATE TABLE t (id int4, label text)").await;
        run_s(&mut session, "INSERT INTO t VALUES (1, 'x'), (2, 'y')").await;

        run_s(&mut session, "ALTER TABLE t ADD COLUMN n int4 DEFAULT 7").await;
        assert!(
            text_rows_of(&mut session, "SELECT id, label, n FROM t ORDER BY id").await
                == vec![text_row(&["1", "x", "7"]), text_row(&["2", "y", "7"])]
        );

        run_s(&mut session, "ALTER TABLE t DROP COLUMN label").await;
        assert!(
            text_rows_of(&mut session, "SELECT id, n FROM t ORDER BY id").await
                == vec![text_row(&["1", "7"]), text_row(&["2", "7"])]
        );
        assert!(sqlstate_of(&mut session, "SELECT label FROM t").await == "42703");
    }

    /// `SET NOT NULL` and `ADD CONSTRAINT … CHECK` back-validate the stored
    /// rows all-or-nothing, and only live rows count.
    #[tokio::test]
    async fn alter_table_back_validates_against_live_rows_only() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run_s(&mut session, "CREATE TABLE t (a int4)").await;
        run_s(&mut session, "INSERT INTO t VALUES (1), (NULL), (-3)").await;

        assert!(
            sqlstate_of(&mut session, "ALTER TABLE t ALTER COLUMN a SET NOT NULL").await == "23502"
        );
        assert!(
            sqlstate_of(
                &mut session,
                "ALTER TABLE t ADD CONSTRAINT ck CHECK (a > 0)"
            )
            .await
                == "23514"
        );

        run_s(&mut session, "DELETE FROM t WHERE a IS NULL OR a < 0").await;
        run_s(&mut session, "ALTER TABLE t ALTER COLUMN a SET NOT NULL").await;
        run_s(
            &mut session,
            "ALTER TABLE t ADD CONSTRAINT ck CHECK (a > 0)",
        )
        .await;
        assert!(sqlstate_of(&mut session, "INSERT INTO t VALUES (0)").await == "23514");
        assert!(sqlstate_of(&mut session, "INSERT INTO t VALUES (NULL)").await == "23502");
    }

    /// `RENAME COLUMN` rewrites the dependencies that name the column: the
    /// table's own `CHECK` predicates keep firing, and a stored view keeps
    /// returning the same rows under its original output labels.
    #[tokio::test]
    async fn rename_column_rewrites_check_and_view_dependencies() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run_s(
            &mut session,
            "CREATE TABLE t (a int4 CHECK (a > 0), b int4)",
        )
        .await;
        run_s(&mut session, "INSERT INTO t VALUES (1, 2)").await;
        run_s(
            &mut session,
            "CREATE VIEW v AS SELECT a, b FROM t WHERE a > 0",
        )
        .await;

        run_s(&mut session, "ALTER TABLE t RENAME COLUMN a TO a2").await;
        assert!(
            text_rows_of(&mut session, "SELECT a2, b FROM t").await == vec![text_row(&["1", "2"])]
        );
        // The view keeps its own output labels and still resolves.
        assert!(
            text_rows_of(&mut session, "SELECT a, b FROM v").await == vec![text_row(&["1", "2"])]
        );
        // The renamed CHECK is still enforced.
        assert!(sqlstate_of(&mut session, "INSERT INTO t VALUES (0, 1)").await == "23514");

        run_s(&mut session, "INSERT INTO t VALUES (5, 6)").await;
        assert!(
            text_rows_of(&mut session, "SELECT a, b FROM v ORDER BY a").await
                == vec![text_row(&["1", "2"]), text_row(&["5", "6"])]
        );
    }

    /// The rewrite is scoped by catalog resolution, not by name matching: a
    /// view over a *different* relation that happens to have a column of the
    /// same name is left alone, and keeps returning that relation's rows.
    #[tokio::test]
    async fn rename_column_leaves_a_same_named_column_of_another_relation_alone() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run_s(&mut session, "CREATE TABLE t (a int4)").await;
        run_s(&mut session, "CREATE TABLE u (a int4)").await;
        run_s(&mut session, "INSERT INTO t VALUES (1)").await;
        run_s(&mut session, "INSERT INTO u VALUES (9)").await;
        run_s(&mut session, "CREATE VIEW vu AS SELECT a FROM u").await;

        run_s(&mut session, "ALTER TABLE t RENAME COLUMN a TO b").await;
        assert!(text_rows_of(&mut session, "SELECT b FROM t").await == vec![text_row(&["1"])]);
        assert!(text_rows_of(&mut session, "SELECT a FROM vu").await == vec![text_row(&["9"])]);
        assert!(text_rows_of(&mut session, "SELECT a FROM u").await == vec![text_row(&["9"])]);
    }

    /// Identity and generated columns compute their values on every write, and
    /// a generated column is visible to a CHECK over it.
    #[tokio::test]
    async fn identity_and_generated_columns_are_computed_on_write() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run_s(
            &mut session,
            "CREATE TABLE t (id int4 GENERATED BY DEFAULT AS IDENTITY, a int4, \
             doubled int4 GENERATED ALWAYS AS (a * 2) STORED, CHECK (doubled < 100))",
        )
        .await;
        run_s(&mut session, "INSERT INTO t (a) VALUES (3)").await;
        run_s(&mut session, "INSERT INTO t (a) VALUES (4)").await;
        assert!(
            text_rows_of(&mut session, "SELECT id, a, doubled FROM t ORDER BY id").await
                == vec![text_row(&["1", "3", "6"]), text_row(&["2", "4", "8"])]
        );

        assert!(sqlstate_of(&mut session, "INSERT INTO t (a) VALUES (60)").await == "23514");
        run_s(&mut session, "UPDATE t SET a = 10 WHERE a = 3").await;
        assert!(
            text_rows_of(&mut session, "SELECT a, doubled FROM t ORDER BY a").await
                == vec![text_row(&["4", "8"]), text_row(&["10", "20"])]
        );
    }

    /// Index options whose semantics the scanner cannot honor are refused;
    /// non-btree access methods remain catalog metadata while scans stay exact.
    #[tokio::test]
    async fn unsupported_index_options_are_refused_not_silently_built() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run_s(&mut session, "CREATE TABLE t (a int4, b text)").await;
        for sql in [
            "CREATE INDEX i ON t (a) WHERE a > 5",
            "CREATE INDEX i ON t (a DESC)",
            "CREATE INDEX i ON t (a NULLS FIRST)",
            "CREATE INDEX i ON t (a) INCLUDE (b)",
        ] {
            assert!(sqlstate_of(&mut session, sql).await == "0A000", "{sql}");
        }
        for method in ["hash", "gist", "spgist"] {
            run_s(
                &mut session,
                &format!("CREATE INDEX t_{method}_idx ON t USING {method} (a)"),
            )
            .await;
        }
        // The supported spellings still build, including the default name.
        run_s(&mut session, "CREATE INDEX ON t (a)").await;
        assert!(
            crabka_pgcatalog::get_index(engine.catalog_kv(), &RelationName::public("t_a_idx"))
                .is_ok()
        );

        run_s(
            &mut session,
            "CREATE INDEX ON t USING spgist (int4range(a, a + 10))",
        )
        .await;
        run_s(&mut session, "INSERT INTO t VALUES (5, 'x'), (25, 'y')").await;
        assert!(
            text_rows_of(
                &mut session,
                "SELECT count(*) FROM t WHERE int4range(a, a + 10) <@ int4range(1, 20)",
            )
            .await
                == vec![text_row(&["1"])]
        );
        let expression = crabka_pgcatalog::get_index(
            engine.catalog_kv(),
            &RelationName::public("t_expr_idx"),
        )
        .expect("expression index");
        assert!(
            crabka_pgcatalog::index_key_expression(&expression.columns[0])
                == Some("int4range(a, a + 10)")
        );
        run_s(&mut session, "ALTER TABLE t RENAME COLUMN a TO n").await;
        let renamed = crabka_pgcatalog::get_index(
            engine.catalog_kv(),
            &RelationName::public("t_expr_idx"),
        )
        .expect("renamed expression index");
        assert!(
            crabka_pgcatalog::index_key_expression(&renamed.columns[0])
                == Some("int4range(n, n + 10)")
        );
        run_s(&mut session, "ALTER TABLE t DROP COLUMN n").await;
        assert!(
            crabka_pgcatalog::get_index(
                engine.catalog_kv(),
                &RelationName::public("t_expr_idx")
            )
            .is_err()
        );
    }

    /// The comma form applies every subcommand or none of them.
    #[tokio::test]
    async fn multi_subcommand_alter_table_is_atomic() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run_s(&mut session, "CREATE TABLE t (a int4)").await;
        run_s(&mut session, "INSERT INTO t VALUES (1)").await;

        run_s(
            &mut session,
            "ALTER TABLE t ADD COLUMN b int4 DEFAULT 2, ADD COLUMN c text DEFAULT 'c'",
        )
        .await;
        assert!(
            text_rows_of(&mut session, "SELECT a, b, c FROM t").await
                == vec![text_row(&["1", "2", "c"])]
        );

        // The second subcommand fails, so the first must not be applied.
        assert!(
            sqlstate_of(
                &mut session,
                "ALTER TABLE t ADD COLUMN d int4, DROP COLUMN nope"
            )
            .await
                == "42703"
        );
        assert!(sqlstate_of(&mut session, "SELECT d FROM t").await == "42703");
    }

    fn settled_snapshot() -> crabka_pgmvcc::visibility::Snapshot {
        crabka_pgmvcc::visibility::Snapshot {
            xmin: 0,
            xmax: u64::MAX,
            xip: Vec::new(),
        }
    }

    fn lookup_index_text(
        engine: &SqlEngine,
        table: &crabka_pgcatalog::Table,
        index: &crabka_pgcatalog::Index,
        value: &str,
    ) -> Vec<Vec<crabka_pgtypes::Datum>> {
        let snapshot = engine.procarray.snapshot();
        let gsnap = settled_snapshot();
        super::lookup_local_index_equal(
            &super::MvccReadContext {
                kv: engine.kv.as_ref(),
                global: engine.kv.as_ref(),
                global_snapshot: &gsnap,
                snapshot: &snapshot,
                own: None,
            },
            table,
            index,
            &[crabka_pgtypes::Datum::Text(value.into())],
        )
        .expect("index lookup")
        .into_iter()
        .map(|row| row.row)
        .collect()
    }

    #[tokio::test]
    async fn local_secondary_index_lookup_tracks_insert_update_delete() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4, name text)")
            .await
            .expect("create table");
        session
            .simple_query("CREATE INDEX t_name_idx ON t (name)")
            .await
            .expect("create index");
        session
            .simple_query("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'a')")
            .await
            .expect("insert");

        let table =
            crabka_pgcatalog::get_table(engine.catalog_kv.as_ref(), &RelationName::public("t"))
                .expect("table");
        let index = crabka_pgcatalog::list_table_indexes(
            engine.catalog_kv.as_ref(),
            &RelationName::public("t"),
        )
        .expect("indexes")
        .pop()
        .expect("index");
        assert_eq!(lookup_index_text(&engine, &table, &index, "a").len(), 2);
        assert_eq!(lookup_index_text(&engine, &table, &index, "b").len(), 1);

        session
            .simple_query("UPDATE t SET name = 'a' WHERE id = 2")
            .await
            .expect("update");
        assert_eq!(lookup_index_text(&engine, &table, &index, "a").len(), 3);
        assert!(lookup_index_text(&engine, &table, &index, "b").is_empty());

        session
            .simple_query("DELETE FROM t WHERE id = 1")
            .await
            .expect("delete");
        let rows = lookup_index_text(&engine, &table, &index, "a");
        let ids: Vec<_> = rows
            .iter()
            .map(|row| row.first().expect("id"))
            .cloned()
            .collect();
        assert_eq!(
            ids,
            vec![
                crabka_pgtypes::Datum::Int4(2),
                crabka_pgtypes::Datum::Int4(3)
            ]
        );
    }

    #[tokio::test]
    async fn drop_index_removes_catalog_metadata_and_local_entries_in_one_ddl_batch() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4, name text)")
            .await
            .expect("create table");
        session
            .simple_query("CREATE INDEX t_name_idx ON t (name)")
            .await
            .expect("create index");
        session
            .simple_query("INSERT INTO t VALUES (1, 'a')")
            .await
            .expect("insert indexed row");

        let index = crabka_pgcatalog::get_index(
            engine.catalog_kv.as_ref(),
            &RelationName::public("t_name_idx"),
        )
        .expect("index metadata");
        let entry_prefix = crabka_pgkv::key::secondary_index_prefix(index.table_id, index.id);
        assert_eq!(
            engine
                .kv
                .scan_prefix(&entry_prefix)
                .expect("scan index entries")
                .len(),
            1
        );

        session
            .simple_query("DROP INDEX t_name_idx")
            .await
            .expect("drop index");

        assert_eq!(
            crabka_pgcatalog::get_index(
                engine.catalog_kv.as_ref(),
                &RelationName::public("t_name_idx")
            )
            .expect_err("metadata removed")
            .sqlstate(),
            "42704"
        );
        assert!(
            engine
                .kv
                .scan_prefix(&entry_prefix)
                .expect("scan removed entries")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn select_uses_local_index_for_simple_equality_with_residual_filter() {
        let mut engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text, active bool)").await;
        run(
            &engine,
            "INSERT INTO t VALUES (1, 'a', true), (2, 'a', false), (3, 'b', true)",
        )
        .await;
        run(&engine, "CREATE INDEX t_name_idx ON t (name)").await;
        engine.set_range_scanner(Arc::new(RejectingRangeScanner));

        let result = run(
            &engine,
            "SELECT id FROM t WHERE name = 'a' AND active = true ORDER BY id",
        )
        .await;

        assert_eq!(rows_of(&result[0]).len(), 1);
        assert_eq!(text(&rows_of(&result[0])[0][0]).as_deref(), Some("1"));
    }

    #[tokio::test]
    async fn local_index_select_ignores_stale_entries_after_update_and_delete() {
        let mut engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text)").await;
        run(&engine, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'a')").await;
        run(&engine, "CREATE INDEX t_name_idx ON t (name)").await;
        run(&engine, "UPDATE t SET name = 'a' WHERE id = 2").await;
        run(&engine, "DELETE FROM t WHERE id = 1").await;
        engine.set_range_scanner(Arc::new(RejectingRangeScanner));

        let result = run(&engine, "SELECT id FROM t WHERE name = 'a' ORDER BY id").await;
        let ids = rows_of(&result[0])
            .iter()
            .map(|row| text(&row[0]).expect("id cell"))
            .collect::<Vec<_>>();

        assert_eq!(ids, vec!["2", "3"]);
    }

    #[tokio::test]
    async fn unsupported_index_shape_falls_back_to_table_scan_semantics() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text)").await;
        run(
            &engine,
            "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'aa')",
        )
        .await;
        run(&engine, "CREATE INDEX t_name_idx ON t (name)").await;

        let result = run(&engine, "SELECT id FROM t WHERE id > 1 ORDER BY id").await;
        let ids = rows_of(&result[0])
            .iter()
            .map(|row| text(&row[0]).expect("id cell"))
            .collect::<Vec<_>>();

        assert_eq!(ids, vec!["2", "3"]);
    }

    #[tokio::test]
    async fn local_secondary_index_entries_survive_durable_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let engine = SqlEngine::open(dir.path()).expect("open");
            let mut session = engine.connect();
            session
                .simple_query("CREATE TABLE t (id int4, name text)")
                .await
                .expect("create table");
            session
                .simple_query("CREATE INDEX t_name_idx ON t (name)")
                .await
                .expect("create index");
            session
                .simple_query("INSERT INTO t VALUES (1, 'persisted')")
                .await
                .expect("insert");
        }

        let reopened = SqlEngine::open(dir.path()).expect("reopen");
        let table =
            crabka_pgcatalog::get_table(reopened.catalog_kv.as_ref(), &RelationName::public("t"))
                .expect("table");
        let index = crabka_pgcatalog::list_table_indexes(
            reopened.catalog_kv.as_ref(),
            &RelationName::public("t"),
        )
        .expect("indexes")
        .pop()
        .expect("index");
        let rows = lookup_index_text(&reopened, &table, &index, "persisted");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], crabka_pgtypes::Datum::Int4(1));
    }

    #[tokio::test]
    async fn read_your_writes_via_own_xid_in_txn() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        s.simple_query("CREATE TABLE t (id int4)")
            .await
            .expect("create");
        s.simple_query("BEGIN").await.expect("begin");
        s.simple_query("INSERT INTO t VALUES (1)")
            .await
            .expect("insert");
        // Own uncommitted insert is visible to this txn (no write-set; via xid).
        assert_eq!(
            rows_of(&run_s(&mut s, "SELECT id FROM t").await[0]).len(),
            1
        );
        s.simple_query("ROLLBACK").await.expect("rollback");
        assert_eq!(
            rows_of(&run_s(&mut s, "SELECT id FROM t").await[0]).len(),
            0
        );
    }

    #[tokio::test]
    async fn another_session_cannot_see_uncommitted_rows() {
        let engine = SqlEngine::new();
        let mut writer = engine.connect();
        writer
            .simple_query("CREATE TABLE t (id int4)")
            .await
            .expect("create");
        writer.simple_query("BEGIN").await.expect("begin");
        writer
            .simple_query("INSERT INTO t VALUES (1)")
            .await
            .expect("insert");
        // A concurrent session must not see the in-progress row.
        let mut reader = engine.connect();
        assert_eq!(
            rows_of(&run_s(&mut reader, "SELECT id FROM t").await[0]).len(),
            0
        );
        writer.simple_query("COMMIT").await.expect("commit");
        // After commit a fresh snapshot sees it.
        assert_eq!(
            rows_of(&run_s(&mut reader, "SELECT id FROM t").await[0]).len(),
            1
        );
    }

    fn rows_of(r: &QueryResult) -> &Vec<Vec<Option<Cell>>> {
        match r {
            QueryResult::Rows { rows, .. } => rows,
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    fn fields_of(r: &QueryResult) -> &Vec<FieldDescription> {
        match r {
            QueryResult::Rows { fields, .. } => fields,
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    fn text(cell: &Option<Cell>) -> Option<String> {
        cell.as_ref()
            .map(|c| String::from_utf8(c.text.to_vec()).expect("cell text is valid UTF-8"))
    }

    #[tokio::test]
    async fn select_literal_no_from() {
        let engine = SqlEngine::new();
        let r = &run(&engine, "SELECT 1 + 1 AS two").await[0];
        assert_eq!(fields_of(r)[0].name, "two");
        assert_eq!(fields_of(r)[0].type_oid, crabka_pgtypes::oids::INT4);
        assert_eq!(text(&rows_of(r)[0][0]), Some("2".into()));
    }

    #[tokio::test]
    async fn select_where_order_limit() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text)").await;
        run(&engine, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").await;
        let r = &run(
            &engine,
            "SELECT name FROM t WHERE id > 1 ORDER BY id DESC LIMIT 5",
        )
        .await[0];
        let rows = rows_of(r);
        assert_eq!(rows.len(), 2);
        assert_eq!(text(&rows[0][0]), Some("c".into())); // id=3 first (DESC)
        assert_eq!(text(&rows[1][0]), Some("b".into()));
    }

    #[tokio::test]
    async fn select_star_projects_all_columns() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text)").await;
        run(&engine, "INSERT INTO t VALUES (7,'x')").await;
        let r = &run(&engine, "SELECT * FROM t").await[0];
        assert_eq!(
            fields_of(r)
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name"]
        );
        assert_eq!(text(&rows_of(r)[0][0]), Some("7".into()));
        assert_eq!(text(&rows_of(r)[0][1]), Some("x".into()));
    }

    #[tokio::test]
    async fn derived_table_in_from() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, v int4)").await;
        run(&engine, "INSERT INTO t VALUES (1,10),(2,20),(3,30)").await;
        let r = &run(
            &engine,
            "SELECT d.s FROM (SELECT v + 1 AS s FROM t WHERE id > 1) d ORDER BY d.s",
        )
        .await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("21".into()), Some("31".into())]);
    }

    #[tokio::test]
    async fn join_against_a_derived_table() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, v int4)").await;
        run(&engine, "INSERT INTO t VALUES (1,10),(2,20)").await;
        let r = &run(
            &engine,
            "SELECT t.id, d.mx FROM t JOIN (SELECT max(v) AS mx FROM t) d ON t.v = d.mx",
        )
        .await[0];
        assert_eq!(rows_of(r).len(), 1);
        assert_eq!(text(&rows_of(r)[0][0]), Some("2".into()));
    }

    #[tokio::test]
    async fn inner_join_on_equi_key() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE a (id int4, av text)").await;
        run(&engine, "CREATE TABLE b (id int4, bv text)").await;
        run(&engine, "INSERT INTO a VALUES (1,'a1'),(2,'a2'),(3,'a3')").await;
        run(&engine, "INSERT INTO b VALUES (2,'b2'),(3,'b3'),(4,'b4')").await;
        let r = &run(
            &engine,
            "SELECT a.av, b.bv FROM a JOIN b ON a.id = b.id ORDER BY a.id",
        )
        .await[0];
        let got: Vec<_> = rows_of(r)
            .iter()
            .map(|row| (text(&row[0]), text(&row[1])))
            .collect();
        assert_eq!(
            got,
            vec![
                (Some("a2".into()), Some("b2".into())),
                (Some("a3".into()), Some("b3".into()))
            ]
        );
    }

    #[tokio::test]
    async fn comma_form_is_a_cross_join_filtered_by_where() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE a (id int4)").await;
        run(&engine, "CREATE TABLE b (id int4)").await;
        run(&engine, "INSERT INTO a VALUES (1),(2)").await;
        run(&engine, "INSERT INTO b VALUES (2),(3)").await;
        let r = &run(&engine, "SELECT a.id FROM a, b WHERE a.id = b.id").await[0];
        assert_eq!(rows_of(r).len(), 1);
        assert_eq!(text(&rows_of(r)[0][0]), Some("2".into()));
    }

    #[tokio::test]
    async fn comma_equality_uses_the_bounded_indexed_join() {
        let engine = SqlEngine::new();
        let r = &run(
            &engine,
            "SELECT count(*) FROM generate_series(1, 2000) a(i), \
             generate_series(1, 2000) b(i) WHERE a.i = b.i",
        )
        .await[0];
        assert_eq!(text(&rows_of(r)[0][0]), Some("2000".into()));
    }

    #[tokio::test]
    async fn self_join_requires_distinct_aliases() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, mgr int4)").await;
        run(&engine, "INSERT INTO t VALUES (1, NULL),(2, 1)").await;
        let r = &run(
            &engine,
            "SELECT e.id, m.id FROM t e JOIN t m ON e.mgr = m.id",
        )
        .await[0];
        // Only (employee 2 -> manager 1) matches: e.id=2, m.id=1.
        assert_eq!(rows_of(r).len(), 1);
        assert_eq!(text(&rows_of(r)[0][0]), Some("2".into()));
        assert_eq!(text(&rows_of(r)[0][1]), Some("1".into()));
    }

    #[tokio::test]
    async fn unaliased_self_join_is_duplicate_alias_42712() {
        // The same qualifier on both sides of a join is rejected (PG 42712).
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1)").await;
        let err = engine
            .connect()
            .simple_query("SELECT * FROM t JOIN t ON t.id = t.id")
            .await
            .expect_err("duplicate table name");
        assert_eq!(err.code, "42712");
    }

    #[tokio::test]
    async fn ambiguous_bare_column_is_42702() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE a (id int4)").await;
        run(&engine, "CREATE TABLE b (id int4)").await;
        let err = engine
            .connect()
            .simple_query("SELECT id FROM a JOIN b ON a.id = b.id")
            .await
            .expect_err("ambiguous");
        assert_eq!(err.code, "42702");
    }

    #[tokio::test]
    async fn left_join_emits_nulls_for_unmatched() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE a (id int4)").await;
        run(&engine, "CREATE TABLE b (id int4, bv text)").await;
        run(&engine, "INSERT INTO a VALUES (1),(2)").await;
        run(&engine, "INSERT INTO b VALUES (2,'two')").await;
        let r = &run(
            &engine,
            "SELECT a.id, b.bv FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id",
        )
        .await[0];
        let got: Vec<_> = rows_of(r)
            .iter()
            .map(|row| (text(&row[0]), text(&row[1])))
            .collect();
        assert_eq!(
            got,
            vec![
                (Some("1".into()), None),
                (Some("2".into()), Some("two".into())),
            ]
        );
    }

    #[tokio::test]
    async fn using_join_merges_the_key_column() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE a (id int4, av text)").await;
        run(&engine, "CREATE TABLE b (id int4, bv text)").await;
        run(&engine, "INSERT INTO a VALUES (1,'a1'),(2,'a2')").await;
        run(&engine, "INSERT INTO b VALUES (2,'b2'),(3,'b3')").await;
        // SELECT * -> merged id first, then av, then bv.
        let r = &run(&engine, "SELECT * FROM a JOIN b USING (id)").await[0];
        assert_eq!(
            fields_of(r)
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "av", "bv"]
        );
        assert_eq!(rows_of(r).len(), 1);
        // Bare `id` is unambiguous after USING/NATURAL.
        let r2 = &run(&engine, "SELECT id FROM a NATURAL JOIN b").await[0];
        assert_eq!(rows_of(r2).len(), 1);
        assert_eq!(text(&rows_of(r2)[0][0]), Some("2".into()));
    }

    #[tokio::test]
    async fn select_command_tag_counts_rows() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1),(2)").await;
        match &run(&engine, "SELECT id FROM t").await[0] {
            QueryResult::Rows { tag, .. } => assert_eq!(tag, "SELECT 2"),
            other => panic!("got {other:?}"),
        }
    }

    #[tokio::test]
    async fn non_boolean_where_is_42804() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1)").await;
        let err = engine
            .connect()
            .simple_query("SELECT id FROM t WHERE id")
            .await
            .expect_err("non-bool");
        assert_eq!(err.code, "42804");
    }

    #[tokio::test]
    async fn null_orders_last_ascending() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (2),(null),(1)").await;
        let r = &run(&engine, "SELECT id FROM t ORDER BY id ASC").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("1".into()), Some("2".into()), None]); // NULLS LAST
    }

    #[tokio::test]
    async fn order_by_mixed_width_expression_key() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4)").await;
        run(&engine, "INSERT INTO t VALUES (1),(3),(2)").await;
        // a + 3000000000 promotes each key to int8; sort must still be 1,2,3.
        let r = &run(&engine, "SELECT a FROM t ORDER BY a + 3000000000 ASC").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(
            got,
            vec![Some("1".into()), Some("2".into()), Some("3".into())]
        );
    }

    #[tokio::test]
    async fn plain_select_order_by_position_and_alias_use_output() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4, b int4, name text)").await;
        run(
            &engine,
            "INSERT INTO t VALUES (1,20,'a'),(2,10,'b'),(3,30,'c')",
        )
        .await;

        let r = &run(&engine, "SELECT name FROM t ORDER BY 1 DESC").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(
            got,
            vec![Some("c".into()), Some("b".into()), Some("a".into())]
        );

        let r = &run(&engine, "SELECT a AS b FROM t ORDER BY b").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(
            got,
            vec![Some("1".into()), Some("2".into()), Some("3".into())]
        );

        let r = &run(&engine, "SELECT a AS b FROM t ORDER BY t.b").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(
            got,
            vec![Some("2".into()), Some("1".into()), Some("3".into())]
        );

        let r = &run(&engine, "SELECT a AS b FROM t ORDER BY b + 0").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(
            got,
            vec![Some("2".into()), Some("1".into()), Some("3".into())]
        );
    }

    #[tokio::test]
    async fn plain_select_order_by_pg_error_surface() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4, b int4)").await;
        run(&engine, "INSERT INTO t VALUES (1,20),(2,10)").await;

        let err = engine
            .connect()
            .simple_query("SELECT a FROM t ORDER BY 0")
            .await
            .expect_err("position zero");
        assert_eq!(err.code, "42P10");

        let err = engine
            .connect()
            .simple_query("SELECT a FROM t ORDER BY 999999999999999999999999999")
            .await
            .expect_err("overflow position");
        assert_eq!(err.code, "42601");

        let err = engine
            .connect()
            .simple_query("SELECT a AS x, b AS x FROM t ORDER BY x")
            .await
            .expect_err("ambiguous output label");
        assert_eq!(err.code, "42702");
    }

    #[tokio::test]
    async fn distinct_select_order_by_uses_output_only() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4, b int4)").await;
        run(&engine, "INSERT INTO t VALUES (1,20),(1,10),(2,30)").await;

        let r = &run(&engine, "SELECT DISTINCT a AS x FROM t ORDER BY x DESC").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("2".into()), Some("1".into())]);

        let r = &run(&engine, "SELECT DISTINCT a AS x FROM t ORDER BY 1 DESC").await[0];
        let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert_eq!(got, vec![Some("2".into()), Some("1".into())]);

        let err = engine
            .connect()
            .simple_query("SELECT DISTINCT a FROM t ORDER BY b")
            .await
            .expect_err("source-only distinct key");
        assert_eq!(err.code, "42P10");
    }

    fn order_scope() -> Scope {
        Scope {
            columns: vec![
                ColumnBinding {
                    qualifier: Some("t".into()),
                    name: "a".into(),
                    ty: crabka_pgtypes::ColumnType::Int4,
                },
                ColumnBinding {
                    qualifier: Some("t".into()),
                    name: "b".into(),
                    ty: crabka_pgtypes::ColumnType::Int4,
                },
            ],
        }
    }

    fn parsed_select(sql: &str) -> SelectStmt {
        match crabka_pgparser::parse(sql)
            .expect("parse")
            .pop()
            .expect("one")
        {
            Statement::Query(q) => match q.body {
                SetExpr::Query(QueryBody::Select(s)) => {
                    let mut s = *s;
                    s.order_by = q.order_by;
                    s.limit = q.limit;
                    s.offset = q.offset;
                    s.locking = q.locking;
                    s
                }
                other => panic!("expected select body, got {other:?}"),
            },
            other => panic!("expected select, got {other:?}"),
        }
    }

    #[test]
    fn lateral_cache_accepts_only_a_noop_offset() {
        let select = parsed_select(
            "SELECT * FROM outer_t, LATERAL (SELECT inner_t.id FROM inner_t \
             WHERE inner_t.id = outer_t.id OFFSET 0) q",
        );
        assert!(super::lateral_cacheable(&select.from[1]));

        let select = parsed_select(
            "SELECT * FROM outer_t, LATERAL (SELECT inner_t.id FROM inner_t \
             WHERE inner_t.id = outer_t.id OFFSET 1) q",
        );
        assert!(!super::lateral_cacheable(&select.from[1]));

        let select = parsed_select(
            "SELECT * FROM outer_t, LATERAL (SELECT random() FROM inner_t \
             WHERE inner_t.id = outer_t.id OFFSET 0) q",
        );
        assert!(!super::lateral_cacheable(&select.from[1]));
    }

    #[test]
    fn select_order_keys_resolve_positions_aliases_and_source_fallback() {
        use super::{SelectOrderKey, resolve_select_order_keys};

        let s = parsed_select("SELECT a AS x, b FROM t ORDER BY 1, x DESC, t.b, b + 0");
        let scope = order_scope();
        let (fields, out_exprs, _) =
            super::resolve_projection(&s.projection, &scope).expect("projection");
        let keys = resolve_select_order_keys(&s.order_by, &scope, &fields, &out_exprs, false)
            .expect("order keys");

        assert!(matches!(keys[0], SelectOrderKey::Output(0)));
        assert!(matches!(keys[1], SelectOrderKey::Output(0)));
        assert!(matches!(keys[2], SelectOrderKey::SourceExpr(_)));
        assert!(matches!(keys[3], SelectOrderKey::SourceExpr(_)));
    }

    #[test]
    fn select_order_keys_report_pg_errors() {
        use super::resolve_select_order_keys;

        let scope = order_scope();

        let bad_pos = parsed_select("SELECT a FROM t ORDER BY 0");
        let (fields, out_exprs, _) =
            super::resolve_projection(&bad_pos.projection, &scope).expect("projection");
        let err = resolve_select_order_keys(&bad_pos.order_by, &scope, &fields, &out_exprs, false)
            .expect_err("bad position");
        assert_eq!(err.into_pg().code, "42P10");

        let overflow = parsed_select("SELECT a FROM t ORDER BY 999999999999999999999999999");
        let (fields, out_exprs, _) =
            super::resolve_projection(&overflow.projection, &scope).expect("projection");
        let err = resolve_select_order_keys(&overflow.order_by, &scope, &fields, &out_exprs, false)
            .expect_err("overflow");
        assert_eq!(err.into_pg().code, "42601");

        let i32_overflow = parsed_select("SELECT a FROM t ORDER BY 2147483648");
        let (fields, out_exprs, _) =
            super::resolve_projection(&i32_overflow.projection, &scope).expect("projection");
        let err =
            resolve_select_order_keys(&i32_overflow.order_by, &scope, &fields, &out_exprs, false)
                .expect_err("i32 overflow");
        let pg = err.into_pg();
        assert_eq!(pg.code, "42601");
        assert_eq!(pg.message, "non-integer constant in ORDER BY");

        let duplicate = parsed_select("SELECT a AS x, b AS x FROM t ORDER BY x");
        let (fields, out_exprs, _) =
            super::resolve_projection(&duplicate.projection, &scope).expect("projection");
        let err =
            resolve_select_order_keys(&duplicate.order_by, &scope, &fields, &out_exprs, false)
                .expect_err("ambiguous output label");
        let pg = err.into_pg();
        assert_eq!(pg.code, "42702");
        assert_eq!(pg.message, "ORDER BY \"x\" is ambiguous");
    }

    #[test]
    fn select_order_keys_allow_identical_duplicate_output_labels() {
        use super::{SelectOrderKey, resolve_select_order_keys};

        let scope = order_scope();

        let duplicate_same_expr = parsed_select("SELECT a, a FROM t ORDER BY a");
        let (fields, out_exprs, _) =
            super::resolve_projection(&duplicate_same_expr.projection, &scope).expect("projection");
        let keys = resolve_select_order_keys(
            &duplicate_same_expr.order_by,
            &scope,
            &fields,
            &out_exprs,
            false,
        )
        .expect("identical duplicate output expressions are not ambiguous");
        assert_eq!(keys, vec![SelectOrderKey::Output(0)]);

        let duplicate_same_alias = parsed_select("SELECT a AS x, a AS x FROM t ORDER BY x");
        let (fields, out_exprs, _) =
            super::resolve_projection(&duplicate_same_alias.projection, &scope)
                .expect("projection");
        let keys = resolve_select_order_keys(
            &duplicate_same_alias.order_by,
            &scope,
            &fields,
            &out_exprs,
            false,
        )
        .expect("identical duplicate output aliases are not ambiguous");
        assert_eq!(keys, vec![SelectOrderKey::Output(0)]);
    }

    #[test]
    fn select_distinct_order_keys_require_output_columns() {
        use super::{SelectOrderKey, resolve_select_order_keys};

        let scope = order_scope();

        let by_alias = parsed_select("SELECT DISTINCT a AS x FROM t ORDER BY x");
        let (fields, out_exprs, _) =
            super::resolve_projection(&by_alias.projection, &scope).expect("projection");
        let keys = resolve_select_order_keys(&by_alias.order_by, &scope, &fields, &out_exprs, true)
            .expect("alias is output");
        assert_eq!(keys, vec![SelectOrderKey::Output(0)]);

        let by_select_expr = parsed_select("SELECT DISTINCT a AS x FROM t ORDER BY a");
        let (fields, out_exprs, _) =
            super::resolve_projection(&by_select_expr.projection, &scope).expect("projection");
        let keys =
            resolve_select_order_keys(&by_select_expr.order_by, &scope, &fields, &out_exprs, true)
                .expect("select-list expression is output");
        assert_eq!(keys, vec![SelectOrderKey::Output(0)]);

        let by_qualified_select_expr = parsed_select("SELECT DISTINCT a FROM t ORDER BY t.a");
        let (fields, out_exprs, _) =
            super::resolve_projection(&by_qualified_select_expr.projection, &scope)
                .expect("projection");
        let keys = resolve_select_order_keys(
            &by_qualified_select_expr.order_by,
            &scope,
            &fields,
            &out_exprs,
            true,
        )
        .expect("qualified select-list expression is output");
        assert_eq!(keys, vec![SelectOrderKey::Output(0)]);

        let missing_qualifier = parsed_select("SELECT DISTINCT a FROM t ORDER BY nope.a");
        let (fields, out_exprs, _) =
            super::resolve_projection(&missing_qualifier.projection, &scope).expect("projection");
        let err = resolve_select_order_keys(
            &missing_qualifier.order_by,
            &scope,
            &fields,
            &out_exprs,
            true,
        )
        .expect_err("missing qualified table");
        assert_eq!(err.into_pg().code, "42P01");

        let source_only = parsed_select("SELECT DISTINCT a FROM t ORDER BY b");
        let (fields, out_exprs, _) =
            super::resolve_projection(&source_only.projection, &scope).expect("projection");
        let err =
            resolve_select_order_keys(&source_only.order_by, &scope, &fields, &out_exprs, true)
                .expect_err("source-only key");
        let pg = err.into_pg();
        assert_eq!(pg.code, "42P10");
        assert_eq!(
            pg.message,
            "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
        );
    }

    async fn run(engine: &SqlEngine, sql: &str) -> Vec<QueryResult> {
        // Autocommit per statement: a fresh session per call preserves the same
        // semantics the old direct `engine.simple_query` had.
        engine.connect().simple_query(sql).await.expect("ok")
    }

    // ---- Q3: DISTINCT ON, LATERAL, ordering/limit breadth ----

    /// Every output cell of one statement, row-major, with NULL as `None`.
    async fn cells(engine: &SqlEngine, sql: &str) -> Vec<Vec<Option<String>>> {
        let results = run(engine, sql).await;
        rows_of(&results[results.len() - 1])
            .iter()
            .map(|row| row.iter().map(text).collect())
            .collect()
    }

    /// The SQLSTATE one statement fails with.
    async fn sqlstate(engine: &SqlEngine, sql: &str) -> String {
        engine
            .connect()
            .simple_query(sql)
            .await
            .expect_err("expected an error")
            .code
    }

    fn cell_rows(rows: &[&[&str]]) -> Vec<Vec<Option<String>>> {
        rows.iter()
            .map(|row| {
                row.iter()
                    .map(|value| (*value != "NULL").then(|| (*value).to_string()))
                    .collect()
            })
            .collect()
    }

    async fn q3_fixture() -> SqlEngine {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE q3 (id int4, grp int4, v int4)").await;
        run(
            &engine,
            "INSERT INTO q3 VALUES (1,10,100),(2,10,300),(3,20,50),(4,20,50),(5,NULL,NULL)",
        )
        .await;
        engine
    }

    #[tokio::test]
    async fn distinct_on_keeps_the_first_row_of_each_group() {
        use assert2::assert;
        let engine = q3_fixture().await;
        let cases: [(&str, &[&[&str]]); 4] = [
            (
                "SELECT DISTINCT ON (grp) grp, id FROM q3 ORDER BY grp, id",
                &[&["10", "1"], &["20", "3"], &["NULL", "5"]],
            ),
            (
                "SELECT DISTINCT ON (grp) grp, id FROM q3 ORDER BY grp, id DESC",
                &[&["10", "2"], &["20", "4"], &["NULL", "5"]],
            ),
            (
                "SELECT DISTINCT ON (grp) id FROM q3 ORDER BY grp DESC, id",
                &[&["5"], &["3"], &["1"]],
            ),
            (
                "SELECT DISTINCT ON (grp, v) id FROM q3 ORDER BY grp, v, id",
                &[&["1"], &["2"], &["3"], &["5"]],
            ),
        ];
        for (sql, want) in cases {
            assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
        }
    }

    #[tokio::test]
    async fn distinct_on_without_order_by_sorts_by_its_keys() {
        use assert2::assert;
        let engine = q3_fixture().await;
        assert!(
            cells(&engine, "SELECT DISTINCT ON (grp) grp FROM q3").await
                == cell_rows(&[&["10"], &["20"], &["NULL"]])
        );
    }

    /// `PostgreSQL`'s DISTINCT ON / ORDER BY rule is one-directional: every
    /// leading ORDER BY key must be a DISTINCT ON expression, but the ON list may
    /// hold expressions the ORDER BY never mentions. It is NOT a set match, and
    /// the queries in the accepted half below are exactly the ones a set match
    /// wrongly rejects.
    #[tokio::test]
    async fn distinct_on_adopts_the_leading_order_by_keys() {
        use assert2::assert;
        let engine = q3_fixture().await;
        let accepted: [(&str, &[&[&str]]); 6] = [
            // Order among the adopted keys is free.
            (
                "SELECT DISTINCT ON (grp, v) grp FROM q3 ORDER BY v, grp",
                &[&["20"], &["10"], &["10"], &["NULL"]],
            ),
            // An ON expression the ORDER BY never mentions is appended to the
            // dedup sort with default ASC NULLS LAST semantics.
            (
                "SELECT DISTINCT ON (grp, v) grp, v FROM q3 ORDER BY grp",
                &[
                    &["10", "100"],
                    &["10", "300"],
                    &["20", "50"],
                    &["NULL", "NULL"],
                ],
            ),
            (
                "SELECT DISTINCT ON (grp, id) grp, id FROM q3 ORDER BY grp DESC",
                &[
                    &["NULL", "5"],
                    &["20", "3"],
                    &["20", "4"],
                    &["10", "1"],
                    &["10", "2"],
                ],
            ),
            // An output ordinal and an output alias both name the select-list
            // column they stand for, on either side of the comparison.
            (
                "SELECT DISTINCT ON (1) grp, id FROM q3 ORDER BY 1, 2",
                &[&["10", "1"], &["20", "3"], &["NULL", "5"]],
            ),
            (
                "SELECT DISTINCT ON (g) grp AS g, id FROM q3 ORDER BY g, id DESC",
                &[&["10", "2"], &["20", "4"], &["NULL", "5"]],
            ),
            // DISTINCT ON over a grouped query dedups the grouped output.
            (
                "SELECT DISTINCT ON (grp) count(*) FROM q3 GROUP BY grp ORDER BY grp",
                &[&["2"], &["2"], &["1"]],
            ),
        ];
        for (sql, want) in accepted {
            assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
        }
        // 42P10 fires once an ORDER BY key has been skipped: for a later key that
        // IS in the ON list, and for an ON expression still needing appending.
        for sql in [
            "SELECT DISTINCT ON (grp) grp FROM q3 ORDER BY v",
            "SELECT DISTINCT ON (grp) grp FROM q3 ORDER BY id, grp",
            "SELECT DISTINCT ON (grp, v) grp FROM q3 ORDER BY grp, id",
        ] {
            assert!(sqlstate(&engine, sql).await == "42P10", "{sql}");
        }
    }

    /// A bare constant in ORDER BY / GROUP BY / DISTINCT ON is an output
    /// position, and `-` folds into it. Before this was modelled, `ORDER BY -1`
    /// and `ORDER BY 1.0` were accepted as constant expressions and silently
    /// dropped the sort.
    #[tokio::test]
    async fn sql92_constant_positions_are_validated() {
        use assert2::assert;
        let engine = q3_fixture().await;
        let cases: [(&str, &str); 10] = [
            ("SELECT id FROM q3 ORDER BY -1", "42P10"),
            ("SELECT id, grp FROM q3 ORDER BY -1, 1", "42P10"),
            ("SELECT id FROM q3 ORDER BY 0", "42P10"),
            ("SELECT id FROM q3 ORDER BY -0", "42P10"),
            ("SELECT id FROM q3 ORDER BY 1.0", "42601"),
            ("SELECT id FROM q3 ORDER BY 1e0", "42601"),
            ("SELECT id FROM q3 ORDER BY 'x'", "42601"),
            ("SELECT id FROM q3 ORDER BY true", "42601"),
            // Wider than int4, so a float constant in PostgreSQL — not a position.
            ("SELECT id FROM q3 ORDER BY 3000000000", "42601"),
            ("SELECT id FROM q3 GROUP BY -1", "42P10"),
        ];
        for (sql, want) in cases {
            assert!(sqlstate(&engine, sql).await == want, "{sql}");
        }
        // Unary `+` is an operator, not a sign, so `+1` is the constant 1 and
        // sorts every row equal rather than naming output column 1.
        assert!(
            cells(&engine, "SELECT id FROM q3 ORDER BY +1").await
                == cell_rows(&[&["1"], &["2"], &["3"], &["4"], &["5"]])
        );
    }

    #[tokio::test]
    async fn order_by_null_placement_follows_postgres() {
        use assert2::assert;
        let engine = q3_fixture().await;
        let cases: [(&str, &[&[&str]]); 6] = [
            (
                "SELECT v FROM q3 WHERE id IN (1,5) ORDER BY v",
                &[&["100"], &["NULL"]],
            ),
            (
                "SELECT v FROM q3 WHERE id IN (1,5) ORDER BY v DESC",
                &[&["NULL"], &["100"]],
            ),
            (
                "SELECT v FROM q3 WHERE id IN (1,5) ORDER BY v NULLS FIRST",
                &[&["NULL"], &["100"]],
            ),
            (
                "SELECT v FROM q3 WHERE id IN (1,5) ORDER BY v NULLS LAST",
                &[&["100"], &["NULL"]],
            ),
            (
                "SELECT v FROM q3 WHERE id IN (1,5) ORDER BY v DESC NULLS LAST",
                &[&["100"], &["NULL"]],
            ),
            (
                "SELECT v FROM q3 WHERE id IN (1,5) ORDER BY v ASC NULLS FIRST",
                &[&["NULL"], &["100"]],
            ),
        ];
        for (sql, want) in cases {
            assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
        }
    }

    #[tokio::test]
    async fn row_counts_accept_arbitrary_expressions() {
        use assert2::assert;
        let engine = q3_fixture().await;
        let cases: [(&str, &[&[&str]]); 7] = [
            (
                "SELECT id FROM q3 ORDER BY id LIMIT 1 + 1",
                &[&["1"], &["2"]],
            ),
            (
                "SELECT id FROM q3 ORDER BY id LIMIT (SELECT 2)",
                &[&["1"], &["2"]],
            ),
            ("SELECT id FROM q3 ORDER BY id OFFSET 3", &[&["4"], &["5"]]),
            (
                "SELECT id FROM q3 ORDER BY id LIMIT ALL OFFSET 4",
                &[&["5"]],
            ),
            (
                "SELECT id FROM q3 ORDER BY id LIMIT NULL OFFSET 4",
                &[&["5"]],
            ),
            (
                "SELECT id FROM q3 ORDER BY id OFFSET 3 ROWS FETCH NEXT 1 ROW ONLY",
                &[&["4"]],
            ),
            (
                "SELECT id FROM q3 ORDER BY id FETCH FIRST ROW ONLY",
                &[&["1"]],
            ),
        ];
        for (sql, want) in cases {
            assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
        }
        assert!(sqlstate(&engine, "SELECT id FROM q3 LIMIT -1").await == "2201W");
        assert!(sqlstate(&engine, "SELECT id FROM q3 OFFSET -1").await == "2201X");
    }

    #[tokio::test]
    async fn fetch_with_ties_extends_the_cut_through_equal_keys() {
        use assert2::assert;
        let engine = q3_fixture().await;
        let cases: [(&str, &[&[&str]]); 3] = [
            (
                "SELECT id, v FROM q3 ORDER BY v NULLS LAST FETCH FIRST 1 ROW WITH TIES",
                &[&["3", "50"], &["4", "50"]],
            ),
            (
                "SELECT id, v FROM q3 ORDER BY v NULLS LAST FETCH FIRST 1 ROW ONLY",
                &[&["3", "50"]],
            ),
            (
                "SELECT id, v FROM q3 ORDER BY v NULLS LAST OFFSET 2 FETCH FIRST 1 ROW WITH TIES",
                &[&["1", "100"]],
            ),
        ];
        for (sql, want) in cases {
            assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
        }
    }

    #[tokio::test]
    async fn lateral_items_are_evaluated_per_outer_row() {
        use assert2::assert;
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE lat (id int4, n int4)").await;
        run(&engine, "INSERT INTO lat VALUES (1,2),(2,0),(3,1)").await;
        let cases: [(&str, &[&[&str]]); 5] = [
            (
                "SELECT t.id, u.x FROM lat t, LATERAL (SELECT t.n * 10 AS x) u ORDER BY t.id",
                &[&["1", "20"], &["2", "0"], &["3", "10"]],
            ),
            (
                "SELECT t.id, g FROM lat t, LATERAL generate_series(1, t.n) g ORDER BY t.id, g",
                &[&["1", "1"], &["1", "2"], &["3", "1"]],
            ),
            // Implicit lateral: a function argument naming an earlier FROM item.
            (
                "SELECT t.id, g FROM lat t, generate_series(1, t.n) g ORDER BY t.id, g",
                &[&["1", "1"], &["1", "2"], &["3", "1"]],
            ),
            // LEFT JOIN LATERAL keeps an outer row whose lateral side is empty.
            (
                "SELECT t.id, g FROM lat t LEFT JOIN LATERAL generate_series(1, t.n) g ON true ORDER BY t.id, g",
                &[&["1", "1"], &["1", "2"], &["2", "NULL"], &["3", "1"]],
            ),
            (
                "SELECT t.id, u.x FROM lat t LEFT JOIN LATERAL (SELECT t.n AS x WHERE t.n > 1) u ON true ORDER BY t.id",
                &[&["1", "2"], &["2", "NULL"], &["3", "NULL"]],
            ),
        ];
        for (sql, want) in cases {
            assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
        }
    }

    /// `PostgreSQL` resolves an unqualified name inside a lateral item against
    /// the item's own FROM first, and only then against the outer row. The
    /// binder used to give up whenever the inner block had a FROM at all, which
    /// turned ordinary lateral queries into a spurious 42703.
    #[tokio::test]
    async fn lateral_unqualified_names_fall_back_to_the_outer_row() {
        use assert2::assert;
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE lo (id int4, nm text)").await;
        run(&engine, "INSERT INTO lo VALUES (1,'a'),(2,'b')").await;
        run(&engine, "CREATE TABLE li (a int4, b int4)").await;
        run(&engine, "INSERT INTO li VALUES (1,10),(1,20),(2,30)").await;
        let cases: [(&str, &[&[&str]]); 4] = [
            // `id` is not a column of `li`, so it binds to the outer row.
            (
                "SELECT o.id, q.b FROM lo o, LATERAL (SELECT b FROM li WHERE li.a = id) q \
                 ORDER BY 1, 2",
                &[&["1", "10"], &["1", "20"], &["2", "30"]],
            ),
            // `a` IS a column of `li`, so the inner one wins and nothing binds.
            (
                "SELECT o.id, q.b FROM lo o, LATERAL (SELECT b FROM li WHERE a = o.id) q \
                 ORDER BY 1, 2",
                &[&["1", "10"], &["1", "20"], &["2", "30"]],
            ),
            // A CTE inside the lateral item is walked too.
            (
                "SELECT o.id, q.v FROM lo o, LATERAL (WITH c AS (SELECT o.id AS v) \
                 SELECT * FROM c) q ORDER BY 1",
                &[&["1", "1"], &["2", "2"]],
            ),
            // With no inner FROM at all every name comes from the outer row.
            (
                "SELECT o.id, q.z FROM lo o, LATERAL (SELECT nm AS z) q ORDER BY 1",
                &[&["1", "a"], &["2", "b"]],
            ),
        ];
        for (sql, want) in cases {
            assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
        }
    }

    /// `RIGHT`/`FULL JOIN LATERAL` is legal in `PostgreSQL` whenever the lateral
    /// item reads nothing from the other side; only an actual reference is the
    /// error, and it is 42P10 naming the relation, not a blanket 0A000.
    #[tokio::test]
    async fn lateral_on_the_nullable_side_is_rejected_only_when_it_correlates() {
        use assert2::assert;
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE rj (id int4)").await;
        run(&engine, "INSERT INTO rj VALUES (1),(2)").await;
        let accepted: [(&str, &[&[&str]]); 3] = [
            (
                "SELECT * FROM rj RIGHT JOIN LATERAL (SELECT 9 AS z) q ON true ORDER BY 1",
                &[&["1", "9"], &["2", "9"]],
            ),
            (
                "SELECT * FROM rj FULL JOIN LATERAL (SELECT 9 AS z) q ON true ORDER BY 1",
                &[&["1", "9"], &["2", "9"]],
            ),
            (
                "SELECT * FROM rj RIGHT JOIN LATERAL generate_series(1,2) g ON true \
                 ORDER BY 1, 2",
                &[&["1", "1"], &["1", "2"], &["2", "1"], &["2", "2"]],
            ),
        ];
        for (sql, want) in accepted {
            assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
        }
        for sql in [
            "SELECT * FROM rj RIGHT JOIN LATERAL (SELECT rj.id AS z) q ON true",
            "SELECT * FROM rj FULL JOIN LATERAL (SELECT rj.id AS z) q ON true",
            "SELECT * FROM rj RIGHT JOIN LATERAL generate_series(1, rj.id) g ON true",
        ] {
            assert!(sqlstate(&engine, sql).await == "42P10", "{sql}");
        }
    }

    /// `SELECT *` over a relation whose column names repeat expands
    /// positionally, so it works even though a bare reference to the repeated
    /// name is still ambiguous.
    #[tokio::test]
    async fn wildcard_expands_positionally_over_repeated_column_names() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let cases: [(&str, &[&[&str]]); 4] = [
            (
                "SELECT * FROM ROWS FROM (generate_series(1,3), generate_series(1,2))",
                &[&["1", "1"], &["2", "2"], &["3", "NULL"]],
            ),
            (
                "SELECT * FROM ROWS FROM (generate_series(1,2), generate_series(1,1)) \
                 WITH ORDINALITY",
                &[&["1", "1", "1"], &["2", "NULL", "2"]],
            ),
            (
                "SELECT t.* FROM ROWS FROM (generate_series(1,2), generate_series(1,1)) t",
                &[&["1", "1"], &["2", "NULL"]],
            ),
            (
                "SELECT * FROM unnest(ARRAY[1,2], ARRAY['a','b','c'])",
                &[&["1", "a"], &["2", "b"], &["NULL", "c"]],
            ),
        ];
        for (sql, want) in cases {
            assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
        }
        // A bare reference to the repeated name is still 42702, as in PostgreSQL.
        assert!(
            sqlstate(
                &engine,
                "SELECT generate_series FROM ROWS FROM (generate_series(1,2), generate_series(1,1))"
            )
            .await
                == "42702"
        );
    }

    /// A base-table alias may rename columns, exactly like a derived table's.
    #[tokio::test]
    async fn a_base_table_alias_may_carry_a_column_list() {
        use assert2::assert;
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE ba (a int4, b int4, c int4)").await;
        run(&engine, "INSERT INTO ba VALUES (1,2,3)").await;
        let cases: [(&str, &[&[&str]]); 3] = [
            ("SELECT * FROM ba AS q(x)", &[&["1", "2", "3"]]),
            ("SELECT q.x, q.y FROM ba q(x, y)", &[&["1", "2"]]),
            ("SELECT x FROM ba AS q(x, y, z) WHERE z = 3", &[&["1"]]),
        ];
        for (sql, want) in cases {
            assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
        }
        // Too many names is 42P10, the same as for a derived table.
        assert!(sqlstate(&engine, "SELECT * FROM ba AS q(w, x, y, z)").await == "42P10");
    }

    /// The row-count clauses coerce to bigint by assignment, so a type with no
    /// such cast is 42804 naming it, not the 42846 an explicit cast would
    /// give.
    #[tokio::test]
    async fn limit_and_offset_reject_non_numeric_arguments() {
        use assert2::assert;
        let engine = q3_fixture().await;
        for sql in [
            "SELECT id FROM q3 LIMIT true",
            "SELECT id FROM q3 OFFSET true",
            "SELECT id FROM q3 LIMIT '2'::text",
            "SELECT id FROM q3 LIMIT '1 day'::interval",
        ] {
            assert!(sqlstate(&engine, sql).await == "42804", "{sql}");
        }
        // An untyped literal still resolves as bigint.
        assert!(
            cells(&engine, "SELECT id FROM q3 ORDER BY id LIMIT '2'")
                .await
                .len()
                == 2
        );
    }

    /// A null `REPEATABLE` seed is `invalid_tablesample_repeat`, which is a
    /// different SQLSTATE from the `invalid_tablesample_argument` a null or
    /// out-of-range percentage raises.
    #[tokio::test]
    async fn tablesample_null_seed_and_null_percentage_differ() {
        use assert2::assert;
        let engine = q3_fixture().await;
        let cases: [(&str, &str); 4] = [
            (
                "SELECT * FROM q3 TABLESAMPLE SYSTEM (50) REPEATABLE (NULL)",
                "2202G",
            ),
            (
                "SELECT * FROM q3 TABLESAMPLE BERNOULLI (50) REPEATABLE (NULL)",
                "2202G",
            ),
            ("SELECT * FROM q3 TABLESAMPLE SYSTEM (NULL)", "2202H"),
            ("SELECT * FROM q3 TABLESAMPLE SYSTEM (101)", "2202H"),
        ];
        for (sql, want) in cases {
            assert!(sqlstate(&engine, sql).await == want, "{sql}");
        }
    }

    /// `ORDER BY … USING <op>` takes its direction from the ordering operator,
    /// and its NULL placement from that direction.
    #[tokio::test]
    async fn order_by_using_takes_its_direction_from_the_operator() {
        use assert2::assert;
        let engine = q3_fixture().await;
        let cases: [(&str, &[&[&str]]); 3] = [
            (
                "SELECT grp FROM q3 WHERE id IN (1,3,5) ORDER BY grp USING <",
                &[&["10"], &["20"], &["NULL"]],
            ),
            (
                "SELECT grp FROM q3 WHERE id IN (1,3,5) ORDER BY grp USING >",
                &[&["NULL"], &["20"], &["10"]],
            ),
            (
                "SELECT grp FROM q3 WHERE id IN (1,3,5) ORDER BY grp USING > NULLS LAST",
                &[&["20"], &["10"], &["NULL"]],
            ),
        ];
        for (sql, want) in cases {
            assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
        }
        assert!(sqlstate(&engine, "SELECT grp FROM q3 ORDER BY grp USING <=").await == "42809");
        // `FOR READ ONLY` locks nothing and is accepted as a no-op.
        assert!(
            cells(&engine, "SELECT id FROM q3 ORDER BY id FOR READ ONLY")
                .await
                .len()
                == 5
        );
    }

    #[tokio::test]
    async fn a_non_lateral_derived_table_cannot_see_an_earlier_from_item() {
        use assert2::assert;
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE lat (id int4, n int4)").await;
        run(&engine, "INSERT INTO lat VALUES (1,2)").await;
        assert!(sqlstate(&engine, "SELECT * FROM lat t, (SELECT t.n AS x) u").await == "42P01");
    }

    #[tokio::test]
    async fn lateral_over_an_empty_outer_relation_keeps_the_columns() {
        use assert2::assert;
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE lat (id int4, n int4)").await;
        let results = run(
            &engine,
            "SELECT t.id, g FROM lat t, LATERAL generate_series(1, t.n) g",
        )
        .await;
        assert!(rows_of(&results[0]).is_empty());
        assert!(
            fields_of(&results[0])
                .iter()
                .map(|f| f.name.clone())
                .collect::<Vec<_>>()
                == vec!["id".to_string(), "g".to_string()]
        );
    }

    #[tokio::test]
    async fn with_ordinality_and_rows_from_expand_in_lockstep() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let cases: [(&str, &[&[&str]]); 5] = [
            (
                "SELECT * FROM generate_series(10, 30, 10) WITH ORDINALITY",
                &[&["10", "1"], &["20", "2"], &["30", "3"]],
            ),
            (
                "SELECT * FROM ROWS FROM (generate_series(1, 3), unnest(ARRAY['a','b'])) AS t(n, s)",
                &[&["1", "a"], &["2", "b"], &["3", "NULL"]],
            ),
            (
                "SELECT * FROM ROWS FROM (generate_series(1, 2)) WITH ORDINALITY AS t(a, b)",
                &[&["1", "1"], &["2", "2"]],
            ),
            // A bare alias renames a single-column item; ordinality keeps its name.
            (
                "SELECT g, ordinality FROM generate_series(1, 2) WITH ORDINALITY AS g",
                &[&["1", "1"], &["2", "2"]],
            ),
            // A shorter column-alias list renames only a prefix.
            (
                "SELECT a, ordinality FROM generate_series(1, 2) WITH ORDINALITY AS t(a)",
                &[&["1", "1"], &["2", "2"]],
            ),
        ];
        for (sql, want) in cases {
            assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
        }
        assert!(
            sqlstate(&engine, "SELECT * FROM generate_series(1, 2) AS t(a, b)").await == "42P10"
        );
        assert!(
            sqlstate(&engine, "SELECT * FROM generate_series(1, 2) AS t(a int4)").await == "42601"
        );
    }

    #[tokio::test]
    async fn tablesample_matches_postgres_at_its_deterministic_ends() {
        use assert2::assert;
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE samp (i int4)").await;
        run(&engine, "INSERT INTO samp VALUES (1),(2),(3),(4)").await;
        let cases: [(&str, &[&[&str]]); 4] = [
            (
                "SELECT count(*) FROM samp TABLESAMPLE BERNOULLI (100)",
                &[&["4"]],
            ),
            (
                "SELECT count(*) FROM samp TABLESAMPLE SYSTEM (100)",
                &[&["4"]],
            ),
            (
                "SELECT count(*) FROM samp TABLESAMPLE BERNOULLI (0)",
                &[&["0"]],
            ),
            (
                "SELECT count(*) FROM samp TABLESAMPLE SYSTEM (100) REPEATABLE (7)",
                &[&["4"]],
            ),
        ];
        for (sql, want) in cases {
            assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
        }
        let errors: [(&str, &str); 4] = [
            ("SELECT * FROM samp TABLESAMPLE FOO (50)", "42704"),
            ("SELECT * FROM samp TABLESAMPLE BERNOULLI (101)", "2202H"),
            ("SELECT * FROM samp TABLESAMPLE SYSTEM (-1)", "2202H"),
            ("SELECT * FROM samp TABLESAMPLE BERNOULLI (NULL)", "2202H"),
        ];
        for (sql, want) in errors {
            assert!(sqlstate(&engine, sql).await == want, "{sql}");
        }
    }

    #[tokio::test]
    async fn locking_reads_accept_every_strength_and_wait_policy() {
        use assert2::assert;
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE lk (id int4)").await;
        run(&engine, "INSERT INTO lk VALUES (1),(2)").await;
        for sql in [
            "SELECT id FROM lk ORDER BY id FOR UPDATE",
            "SELECT id FROM lk ORDER BY id FOR NO KEY UPDATE",
            "SELECT id FROM lk ORDER BY id FOR SHARE",
            "SELECT id FROM lk ORDER BY id FOR KEY SHARE",
            "SELECT id FROM lk ORDER BY id FOR UPDATE OF lk",
            "SELECT id FROM lk AS t ORDER BY id FOR UPDATE OF t",
            "SELECT id FROM lk ORDER BY id FOR UPDATE NOWAIT",
            "SELECT id FROM lk ORDER BY id FOR UPDATE SKIP LOCKED",
            "SELECT id FROM lk ORDER BY id FOR SHARE OF lk SKIP LOCKED",
        ] {
            assert!(
                cells(&engine, sql).await == cell_rows(&[&["1"], &["2"]]),
                "{sql}"
            );
        }
        // Nothing to lock: PostgreSQL just runs the query.
        assert!(cells(&engine, "SELECT 1 FOR UPDATE").await == cell_rows(&[&["1"]]));
        assert!(
            cells(&engine, "SELECT g FROM generate_series(1, 1) g FOR UPDATE").await
                == cell_rows(&[&["1"]])
        );
    }

    #[tokio::test]
    async fn locking_refusals_match_postgres_sqlstates() {
        use assert2::assert;
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE lk (id int4)").await;
        run(&engine, "INSERT INTO lk VALUES (1),(2)").await;
        let cases: [(&str, &str); 7] = [
            ("SELECT count(*) FROM lk FOR UPDATE", "0A000"),
            ("SELECT id FROM lk GROUP BY id FOR UPDATE", "0A000"),
            (
                "SELECT id FROM lk GROUP BY id HAVING count(*) > 0 FOR SHARE",
                "0A000",
            ),
            ("SELECT DISTINCT id FROM lk FOR UPDATE", "0A000"),
            ("SELECT id FROM lk UNION SELECT 3 FOR UPDATE", "0A000"),
            ("VALUES (1) FOR UPDATE", "0A000"),
            ("SELECT id FROM lk FOR UPDATE OF nosuch", "42P01"),
        ];
        for (sql, want) in cases {
            assert!(sqlstate(&engine, sql).await == want, "{sql}");
        }
    }

    fn single_text(result: &[QueryResult]) -> String {
        let [QueryResult::Rows { rows, .. }] = result else {
            panic!("expected rows");
        };
        let [row] = rows.as_slice() else {
            panic!("expected one row");
        };
        let [Some(cell)] = row.as_slice() else {
            panic!("expected one non-null cell");
        };
        String::from_utf8(cell.text.to_vec()).expect("cell is utf8")
    }

    #[tokio::test]
    async fn drop_table_if_exists_skips_missing_table() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let results = run(&engine, "DROP TABLE IF EXISTS missing").await;
        assert!(tag_of(&results[0]) == "DROP TABLE");
    }

    #[tokio::test]
    async fn drop_table_without_if_exists_errors_on_missing_table() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let err = engine
            .connect()
            .simple_query("DROP TABLE missing")
            .await
            .expect_err("missing table without IF EXISTS");
        assert!(err.code == "42P01");
    }

    #[tokio::test]
    async fn multi_table_drop_is_all_or_nothing() {
        use assert2::assert;
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE a (id int4 PRIMARY KEY)").await;
        run(&engine, "CREATE TABLE b (id int4 PRIMARY KEY)").await;

        // A missing name without IF EXISTS aborts the whole drop.
        let err = engine
            .connect()
            .simple_query("DROP TABLE a, missing, b")
            .await
            .expect_err("missing name aborts the whole drop");
        assert!(err.code == "42P01");
        run(&engine, "SELECT count(*) FROM a").await;
        run(&engine, "SELECT count(*) FROM b").await;

        // With IF EXISTS the existing names drop and the missing one is skipped.
        let results = run(&engine, "DROP TABLE IF EXISTS a, missing, b").await;
        assert!(tag_of(&results[0]) == "DROP TABLE");
        for table in ["a", "b"] {
            let err = engine
                .connect()
                .simple_query(&format!("SELECT count(*) FROM {table}"))
                .await
                .expect_err("table was dropped");
            assert!(err.code == "42P01");
        }
    }

    #[test]
    fn copy_text_stops_at_the_end_of_data_marker() {
        use assert2::assert;
        // Old-API clients (PQputline/PQendcopy — pgbench -i) send a final
        // `\.` line; it terminates the data and later lines are ignored.
        let rows = super::decode_copy_text(b"1\t0\t\\N\n\\.\n").expect("decode");
        assert!(rows == vec![vec![Some("1".into()), Some("0".into()), None]]);

        let rows = super::decode_copy_text(b"1\ta\n\\.\nignored\tafter\n").expect("decode");
        assert!(rows == vec![vec![Some("1".into()), Some("a".into())]]);

        // Without the marker, behavior is unchanged.
        let rows = super::decode_copy_text(b"1\ta\n2\tb\n").expect("decode");
        assert!(rows.len() == 2);
    }

    #[tokio::test]
    async fn truncate_empties_multiple_tables_atomically() {
        use assert2::assert;
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE ta (id int4 PRIMARY KEY)").await;
        run(&engine, "CREATE TABLE tb (id int4 PRIMARY KEY)").await;
        run(&engine, "INSERT INTO ta VALUES (1), (2), (3)").await;
        run(&engine, "INSERT INTO tb VALUES (7)").await;

        // A missing name aborts the whole statement before any rows go.
        let err = engine
            .connect()
            .simple_query("TRUNCATE ta, missing, tb")
            .await
            .expect_err("missing table aborts the whole truncate");
        assert!(err.code == "42P01");
        assert!(single_text(&run(&engine, "SELECT count(*) FROM ta").await) == "3");

        let results = run(&engine, "TRUNCATE TABLE ta, tb").await;
        assert!(tag_of(&results[0]) == "TRUNCATE TABLE");
        assert!(single_text(&run(&engine, "SELECT count(*) FROM ta").await) == "0");
        assert!(single_text(&run(&engine, "SELECT count(*) FROM tb").await) == "0");
    }

    #[tokio::test]
    async fn vacuum_is_an_accepted_hint_outside_transactions() {
        use assert2::assert;
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE tv (id int4 PRIMARY KEY)").await;

        let results = run(&engine, "VACUUM ANALYZE tv").await;
        assert!(tag_of(&results[0]) == "VACUUM");

        // PostgreSQL refuses VACUUM inside a transaction block (25001).
        let mut session = engine.connect();
        session.simple_query("BEGIN").await.expect("begin");
        let error = session
            .simple_query("VACUUM")
            .await
            .expect_err("vacuum in a transaction block");
        assert!(error.code == "25001");
    }

    #[tokio::test]
    async fn truncate_rolls_back_inside_a_transaction() {
        use assert2::assert;
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE tr (id int4 PRIMARY KEY)").await;
        run(&engine, "INSERT INTO tr VALUES (1), (2)").await;

        let mut session = engine.connect();
        session.simple_query("BEGIN").await.expect("begin");
        session.simple_query("TRUNCATE tr").await.expect("truncate");
        let counted = session
            .simple_query("SELECT count(*) FROM tr")
            .await
            .expect("count inside txn");
        assert!(single_text(&counted) == "0");
        session.simple_query("ROLLBACK").await.expect("rollback");

        assert!(single_text(&run(&engine, "SELECT count(*) FROM tr").await) == "2");
    }

    #[tokio::test]
    async fn truncate_restart_identity_fails_clear() {
        use assert2::assert;
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE ti (id int4 PRIMARY KEY)").await;
        let err = engine
            .connect()
            .simple_query("TRUNCATE ti RESTART IDENTITY")
            .await
            .expect_err("restart identity is a bounded refusal");
        assert!(err.code == "0A000");
    }

    #[tokio::test]
    async fn drop_sequence_if_exists_skips_missing_sequence() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let results = run(&engine, "DROP SEQUENCE IF EXISTS missing_seq").await;
        assert!(tag_of(&results[0]) == "DROP SEQUENCE");
        let err = engine
            .connect()
            .simple_query("DROP SEQUENCE missing_seq")
            .await
            .expect_err("missing sequence without IF EXISTS");
        assert!(err.code == "42P01");
    }

    #[tokio::test]
    async fn sequence_functions_and_drop_work() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE SEQUENCE s START WITH 10 INCREMENT BY 5")
            .await
            .expect("create sequence");

        assert_eq!(
            single_text(
                &session
                    .simple_query("SELECT nextval('s')")
                    .await
                    .expect("nextval")
            ),
            "10"
        );
        assert_eq!(
            single_text(
                &session
                    .simple_query("SELECT currval('s')")
                    .await
                    .expect("currval")
            ),
            "10"
        );
        assert_eq!(
            single_text(
                &session
                    .simple_query("SELECT nextval('s')")
                    .await
                    .expect("nextval")
            ),
            "15"
        );
        assert_eq!(
            single_text(
                &session
                    .simple_query("SELECT setval('s', 40, false)")
                    .await
                    .expect("setval")
            ),
            "40"
        );
        assert_eq!(
            single_text(
                &session
                    .simple_query("SELECT nextval('s')")
                    .await
                    .expect("nextval after setval")
            ),
            "40"
        );

        session.simple_query("DROP SEQUENCE s").await.expect("drop");
        let err = session
            .simple_query("SELECT nextval('s')")
            .await
            .expect_err("dropped sequence");
        assert_eq!(err.code, "42P01");
    }

    #[tokio::test]
    async fn currval_requires_session_nextval() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE SEQUENCE s").await;
        let err = engine
            .connect()
            .simple_query("SELECT currval('s')")
            .await
            .expect_err("currval before nextval");
        assert_eq!(err.code, "55000");
    }

    #[tokio::test]
    async fn sequence_bounds_and_cycle_are_enforced() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE SEQUENCE bounded START WITH 2 MAXVALUE 3 NO CYCLE")
            .await
            .expect("create bounded");
        assert_eq!(
            single_text(
                &session
                    .simple_query("SELECT nextval('bounded')")
                    .await
                    .expect("n1")
            ),
            "2"
        );
        assert_eq!(
            single_text(
                &session
                    .simple_query("SELECT nextval('bounded')")
                    .await
                    .expect("n2")
            ),
            "3"
        );
        let err = session
            .simple_query("SELECT nextval('bounded')")
            .await
            .expect_err("limit");
        assert_eq!(err.code, "2200H");

        session
            .simple_query("CREATE SEQUENCE cyc START WITH 2 MAXVALUE 3 CYCLE")
            .await
            .expect("create cycle");
        assert_eq!(
            single_text(
                &session
                    .simple_query("SELECT nextval('cyc')")
                    .await
                    .expect("c1")
            ),
            "2"
        );
        assert_eq!(
            single_text(
                &session
                    .simple_query("SELECT nextval('cyc')")
                    .await
                    .expect("c2")
            ),
            "3"
        );
        assert_eq!(
            single_text(
                &session
                    .simple_query("SELECT nextval('cyc')")
                    .await
                    .expect("c3")
            ),
            "1"
        );
    }

    #[tokio::test]
    async fn serial_insert_default_uses_backing_sequence() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id serial, name text)")
            .await
            .expect("create serial table");
        session
            .simple_query("INSERT INTO t (name) VALUES ('a'), ('b')")
            .await
            .expect("insert defaults");
        assert_eq!(
            single_text(
                &session
                    .simple_query("SELECT id FROM t ORDER BY id LIMIT 1")
                    .await
                    .expect("select")
            ),
            "1"
        );
        assert_eq!(
            single_text(
                &session
                    .simple_query("SELECT currval('t_id_seq')")
                    .await
                    .expect("currval")
            ),
            "2"
        );
    }

    #[tokio::test]
    async fn insert_then_count_via_kv() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text)").await;
        let r = run(&engine, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").await;
        assert_eq!(
            r,
            vec![QueryResult::Command {
                tag: "INSERT 0 2".into()
            }]
        );
        // A third single-row insert with explicit columns.
        let r = run(&engine, "INSERT INTO t (name, id) VALUES ('c', 3)").await;
        assert_eq!(
            r,
            vec![QueryResult::Command {
                tag: "INSERT 0 1".into()
            }]
        );
    }

    #[tokio::test]
    async fn insert_writes_a_versioned_row_visible_to_select() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1)").await;
        let r = &run(&engine, "SELECT id FROM t").await[0];
        assert_eq!(rows_of(r).len(), 1);
    }

    #[tokio::test]
    async fn insert_widens_int4_to_int8_column() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (big int8)").await;
        run(&engine, "INSERT INTO t VALUES (5)").await;
        // Round-trips through SELECT in Task 17; here just assert no error.
    }

    #[tokio::test]
    async fn insert_type_mismatch_is_42804() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (flag bool)").await;
        let err = engine
            .connect()
            .simple_query("INSERT INTO t VALUES (1)")
            .await
            .expect_err("mismatch");
        assert_eq!(err.code, "42804");
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn insert_into_missing_table_is_42P01() {
        let engine = SqlEngine::new();
        let err = engine
            .connect()
            .simple_query("INSERT INTO nope VALUES (1)")
            .await
            .expect_err("no table");
        assert_eq!(err.code, "42P01");
    }

    #[tokio::test]
    /// A short `VALUES` row is legal without a column list, because PostgreSQL
    /// fills the trailing columns from their defaults. But a statement that
    /// names more target columns than there are expressions is `42601`. Both
    /// were verified against the 18.4
    /// oracle; this test previously asserted `42804` for the legal form.
    async fn insert_row_shorter_than_the_table_fills_defaults() {
        use assert2::assert;

        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4, b int4)").await;

        // The legal form must not error, and the unnamed column must be NULL.
        run(&engine, "INSERT INTO t VALUES (1)").await;
        let rows = run(&engine, "SELECT a, b IS NULL FROM t").await;
        assert!(
            matches!(
                rows.as_slice(),
                [QueryResult::Rows { rows, .. }]
                    if rows.len() == 1 && rows[0].len() == 2
            ),
            "one row of two columns: {rows:?}"
        );

        let err = engine
            .connect()
            .simple_query("INSERT INTO t (a, b) VALUES (1)")
            .await
            .expect_err("more target columns than expressions");
        assert!(err.code == "42601");
    }

    #[tokio::test]
    async fn create_then_drop_table() {
        let engine = SqlEngine::new();
        let r = run(&engine, "CREATE TABLE t (id int4, name text)").await;
        assert_eq!(
            r,
            vec![QueryResult::Command {
                tag: "CREATE TABLE".into()
            }]
        );
        // Re-creating is a duplicate error (42P07), session survives.
        let err = engine
            .connect()
            .simple_query("CREATE TABLE t (id int4)")
            .await
            .expect_err("dup");
        assert_eq!(err.code, "42P07");
        let r = run(&engine, "DROP TABLE t").await;
        assert_eq!(
            r,
            vec![QueryResult::Command {
                tag: "DROP TABLE".into()
            }]
        );
        let err = engine
            .connect()
            .simple_query("DROP TABLE t")
            .await
            .expect_err("gone");
        assert_eq!(err.code, "42P01");
    }

    #[tokio::test]
    async fn empty_query_yields_empty_result() {
        let engine = SqlEngine::new();
        assert_eq!(run(&engine, "   ").await, vec![QueryResult::Empty]);
    }

    #[tokio::test]
    async fn syntax_error_is_42601() {
        let engine = SqlEngine::new();
        let err = engine
            .connect()
            .simple_query("SELCT 1")
            .await
            .expect_err("syntax");
        assert_eq!(err.code, "42601");
    }

    #[tokio::test]
    async fn describe_select_returns_field_types_without_executing() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4, name text)").await;
        let fields = engine
            .connect()
            .test_describe("SELECT id, name FROM t")
            .await
            .expect("describe");
        assert_eq!(
            fields.iter().map(|f| f.type_oid).collect::<Vec<_>>(),
            vec![crabka_pgtypes::oids::INT4, crabka_pgtypes::oids::TEXT]
        );
    }

    #[tokio::test]
    async fn describe_non_select_has_no_fields() {
        let engine = SqlEngine::new();
        let fields = engine
            .connect()
            .test_describe("CREATE TABLE t (id int4)")
            .await
            .expect("describe");
        assert!(fields.is_empty());
    }

    #[tokio::test]
    async fn describe_set_op_returns_first_branch_fields() {
        // Schema-only: a set-op query reports the first branch's column name(s) and
        // the unified type, without executing.
        let engine = SqlEngine::new();
        let fields = engine
            .connect()
            .test_describe("SELECT 1 AS x UNION SELECT 2")
            .await
            .expect("describe");
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "x"); // name from the FIRST branch
    }

    #[tokio::test]
    async fn describe_set_op_unifies_branch_types() {
        // The Describe path must run cross-branch type unification: int4 ∪ int8 → int8.
        let engine = SqlEngine::new();
        let fields = engine
            .connect()
            .test_describe("SELECT 1 AS x UNION SELECT 2::int8")
            .await
            .expect("describe");
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "x");
        assert_eq!(fields[0].type_oid, crabka_pgtypes::ColumnType::Int8.oid());
    }

    #[tokio::test]
    async fn two_inserts_are_both_visible() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1)").await;
        run(&engine, "INSERT INTO t VALUES (2)").await;
        let r = &run(&engine, "SELECT id FROM t ORDER BY id").await[0];
        assert_eq!(rows_of(r).len(), 2);
    }

    #[tokio::test]
    async fn select_on_empty_table_sees_no_rows() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        let r = &run(&engine, "SELECT id FROM t").await[0];
        assert_eq!(rows_of(r).len(), 0);
    }

    fn tag_of(r: &QueryResult) -> String {
        match r {
            QueryResult::Command { tag } => tag.clone(),
            other => panic!("expected Command, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn select_for_update_returns_rows() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1),(2),(3)").await;
        let r = &run(
            &engine,
            "SELECT id FROM t WHERE id > 1 ORDER BY id FOR UPDATE",
        )
        .await[0];
        assert_eq!(rows_of(r).len(), 2);
    }

    #[tokio::test]
    async fn for_update_in_txn_then_commit_releases() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4)").await;
        run(&engine, "INSERT INTO t VALUES (1)").await;
        let mut s = engine.connect();
        run_s(&mut s, "BEGIN").await;
        run_s(&mut s, "SELECT id FROM t FOR UPDATE").await; // takes a lock
        run_s(&mut s, "COMMIT").await; // must release; no hang
        // a fresh autocommit update of the same row must not block
        let r = run(&engine, "UPDATE t SET id = 9 WHERE id = 1").await;
        assert_eq!(tag_of(&r[0]), "UPDATE 1");
    }

    /// Regression test: `eval_plan_qual` must resolve a `Prepared(LA → g)` deleter
    /// against the CURRENT global clog (via `settled_global`), NOT the writer's
    /// pre-lock global snapshot (`gsnap`), which may still list `g` as in-flight.
    ///
    /// Scenario (reconstructed without concurrency):
    ///   - Cross-range txn `LA` (local xid on this range) UPDATE-committed row R
    ///     from value 100 (v1) to value 70 (v2), leaving local clog entry
    ///     `LA → Prepared(g1)` and global clog entry `g1 → Committed`.
    ///   - Writer W took its global snapshot BEFORE `g1` was committed, so that
    ///     snapshot still lists `g1` as in-flight (stale gsnap).
    ///   - W now holds the row lock and calls `eval_plan_qual`.
    ///
    /// With the fix (`settled_global`):
    ///   `resolve(LA) == Committed`, `changed_since_snapshot == true`, READ COMMITTED
    ///   re-finds under a fresh snapshot → returns v2 (value 70). Correct.
    ///
    /// Without the fix (using `gsnap` for resolve):
    ///   `resolve(LA) == InProgress` (g1 still in-doubt in stale gsnap) →
    ///   `changed_since_snapshot == false` → `find_visible_one` with stale snapshot
    ///   sees v1 as live (xmax=LA appears uncommitted) → returns v1 (value 100).
    ///   Lost update across the 2PC boundary.
    #[test]
    fn eval_plan_qual_settled_global_sees_committed_cross_range_version() {
        use std::sync::Arc;

        use crabka_pgcatalog::{Column, Table};
        use crabka_pgkv::{Kv, MemKv};
        use crabka_pgmvcc::{
            clog::{XidStatus, put_op},
            version::{encode_tuple, version_key_xid},
            visibility::Snapshot,
            xid::{FIRST_NORMAL_XID, GLOBAL_XID_BASE, INVALID_XID},
        };
        use crabka_pgtypes::{ColumnType, Datum};

        use super::eval_plan_qual;

        // ── xid assignments ─────────────────────────────────────────────────────
        let x0: u64 = FIRST_NORMAL_XID; // original inserter — settled, committed
        let la: u64 = FIRST_NORMAL_XID + 1; // cross-range txn's local xid (Prepared)
        let g1: u64 = GLOBAL_XID_BASE + 1; // global txn id
        let writer: u64 = FIRST_NORMAL_XID + 2; // writer calling eval_plan_qual

        // ── stores ──────────────────────────────────────────────────────────────
        // `kv` holds both the data range's row versions AND the local clog.
        // `global` holds only range-0's global clog.
        let kv = Arc::new(MemKv::new());
        let global = MemKv::new();

        // ── catalog table ────────────────────────────────────────────────────────
        // Table id 1, single int4 column "val".
        let table = Table {
            id: 1,
            name: RelationName::public("t"),
            columns: vec![Column::new("val", ColumnType::Int4)],
            sharded: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        };
        let rowid: u64 = 1;

        // ── write two versions of row R ──────────────────────────────────────────
        // v1: created by x0, deleted (xmax) by la — value 100 (the old row)
        kv.write_batch(&[crabka_pgkv::WriteOp::Put {
            key: version_key_xid(table.id, rowid, x0),
            value: encode_tuple(x0, la, &[Datum::Int4(100)]),
        }])
        .expect("write v1");
        // v2: created by la, live (xmax=INVALID_XID) — value 70 (the updated row)
        kv.write_batch(&[crabka_pgkv::WriteOp::Put {
            key: version_key_xid(table.id, rowid, la),
            value: encode_tuple(la, INVALID_XID, &[Datum::Int4(70)]),
        }])
        .expect("write v2");

        // ── local clog in `kv` ───────────────────────────────────────────────────
        // x0 is settled-committed.  la is in Prepared state → g1.
        kv.write_batch(&[
            put_op(x0, XidStatus::Committed),
            put_op(la, XidStatus::Prepared(g1)),
        ])
        .expect("write local clog");

        // ── global clog in `global` ──────────────────────────────────────────────
        // g1 has committed — but writer's global snapshot is stale (lists g1 as
        // in-flight), so eval_plan_qual MUST use settled_global, not stale_gsnap.
        global
            .write_batch(&[put_op(g1, XidStatus::Committed)])
            .expect("write global clog");

        // ── stale global snapshot (what the writer held pre-lock) ────────────────
        // g1 is listed as in-flight — this is the bug trigger.
        // NOTE: eval_plan_qual no longer accepts gsnap as a parameter (the fix
        // bakes settled_global internally), so this snapshot is used as the
        // *local* snapshot below, which represents the writer's view of local xids.
        // The global staleness is expressed via the local clog's Prepared marker.

        // ── procarray: writer is running; x0 and la are not ────────────────────
        // The fresh snapshot produced by procarray.snapshot() inside eval_plan_qual
        // will have xmax=writer+1, xip=[writer] — so la is below xmax and not in xip,
        // meaning satisfies_mvcc will ask the clog for la → Prepared(g1) →
        // settled_global → Committed → v2 visible. Correct.
        let procarray = crate::procarray::ProcArray::open(
            Arc::clone(&kv) as Arc<dyn crabka_pgkv::Kv>,
            crate::PersistMode::Durable,
        )
        .expect("procarray open");
        // Advance next_xid past x0, la, and writer by allocating writer's slot.
        let _xid_x0 = procarray.begin_write().expect("alloc x0 slot");
        let _xid_la = procarray.begin_write().expect("alloc la slot");
        let _xid_w = procarray.begin_write().expect("alloc writer slot");
        assert_eq!((_xid_x0, _xid_la, _xid_w), (x0, la, writer));
        // Mark x0 and la as finished (committed) so they are not in the running set.
        procarray.finish(_xid_x0);
        procarray.finish(_xid_la);
        // writer (xid=3) remains running.

        // ── local (txn) snapshot for the writer ─────────────────────────────────
        // Taken when the writer began. At that time la (xid=2) was still running
        // in the local sense because the Prepared marker hadn't been removed yet.
        // NOTE: in the real 2PC path la is deregistered from procarray at prepare,
        // so in practice it would not appear in xip here; but eval_plan_qual's
        // staleness bug is about the GLOBAL snapshot, not the local one. We make
        // la visible in the local snapshot to keep the test simple and focused:
        // x0 is settled (xid < xmax, not in xip) and la is settled too (same).
        // The critical stale element is the global clog Prepared → g1-in-doubt path,
        // which is exercised via the kv local-clog entry `la → Prepared(g1)`.
        //
        // Writer's local snapshot: xmax = writer, only writer in xip.
        // x0 and la are below xmax and not in xip → settled.
        // This is the snapshot held when the writer started, BEFORE it blocked on
        // the row lock. la's Prepared(g1) status makes g1 the relevant global txn.
        let writer_snapshot = Snapshot {
            xmin: writer,
            xmax: writer,      // writer itself started after x0 and la settled locally
            xip: vec![writer], // writer is the only running local txn
        };

        // ── call eval_plan_qual ──────────────────────────────────────────────────
        // With the fix: eval_plan_qual uses settled_global internally, so:
        //   resolve(la) → Prepared(g1) → g1 not in-doubt in settled_global → Committed
        //   changed_since_snapshot: xmax=la, la != INVALID_XID, la != writer,
        //     resolve(la)==Committed, !snapshot_can_see(writer_snapshot, la).
        //   snapshot_can_see(writer_snapshot, la): la=2 < xmax=3, la not in xip=[3]
        //     → la IS visible → snapshot_can_see = true → !true = false → NOT changed.
        //
        // Wait — if la is visible in writer_snapshot, changed_since_snapshot is false,
        // so we go to find_visible_one with writer_snapshot and settled_global.
        // With settled_global: resolve(la) = Committed.
        // v1: xmin=x0 (committed, visible), xmax=la (committed-visible) → NOT visible.
        // v2: xmin=la (committed-visible), xmax=INVALID_XID → visible. Returns v2. Correct.
        //
        // Without the fix (using stale gsnap where g1 is in-doubt):
        //   resolve(la) → Prepared(g1) → g1 in-doubt → InProgress
        //   changed_since_snapshot: resolve(la)==InProgress, not Committed → false
        //   find_visible_one with writer_snapshot and stale resolver:
        //     v1: xmin=x0 visible, xmax=la → resolve(la)=InProgress → not committed
        //         → xmax not committed-visible → v1 appears live → visible!
        //     v2: xmin=la → committed_visible(la): la not own, la < xmax, not in xip
        //         → NOT running → asks status: InProgress → NOT committed → v2 invisible
        //   Returns v1 (value 100). Bug.
        let result = eval_plan_qual(
            &super::MutationContext {
                kv: kv.as_ref(),
                global: &global,
                procarray: &procarray,
                snapshot: &writer_snapshot,
                xid: writer,
                repeatable_read: false,
            },
            &table,
            rowid,
        )
        .expect("eval_plan_qual must not error");

        // The fix: must see v2 (xmin=la, value=70), NOT v1 (value=100).
        let (_ret_key_xid, ret_xmin, ret_row) = result.expect("must find a version (not None)");
        assert_eq!(
            ret_xmin, la,
            "eval_plan_qual must return the cross-range committed version (xmin=la={la}), \
             not the stale pre-commit version (xmin=x0={x0})"
        );
        assert_eq!(
            ret_row,
            vec![Datum::Int4(70)],
            "eval_plan_qual must return value 70 (cross-range committed UPDATE result), \
             not value 100 (the stale pre-2PC-commit row) — lost-update bug"
        );
    }

    /// SP21: after a fresh-`g'` re-attempt, a row has TWO physical versions: the
    /// abandoned attempt's `Prepared(Li_old -> g)` with `g` Aborted, and the re-attempt's
    /// `Prepared(Li_new -> g')` with `g'` Committed. `find_visible_one` must return the
    /// committed-`g'` version (highest xmin) and never the aborted shadow; exactly one
    /// version is live (the assert holds).
    #[test]
    fn find_visible_one_returns_committed_reattempt_over_aborted_shadow() {
        use std::sync::Arc;

        use crabka_pgkv::{Kv, MemKv};
        use crabka_pgmvcc::{
            clog::{XidStatus, put_op},
            visibility::Snapshot,
            xid::{GLOBAL_XID_BASE, INVALID_XID},
        };
        use crabka_pgtypes::Datum;

        use super::{find_visible_one, global_status};

        let li_old: u64 = 5; // abandoned attempt's local xid
        let li_new: u64 = 9; // re-attempt's local xid (reseed -> strictly greater)
        let g: u64 = GLOBAL_XID_BASE + 1; // abandoned global xid (Aborted)
        let g2: u64 = GLOBAL_XID_BASE + 2; // fresh global xid (Committed)

        let kv = Arc::new(MemKv::new()); // holds the local clog
        let global = MemKv::new(); // range-0 global clog

        // `find_visible_one` reads ONLY the passed `versions` slice + the local/global clogs
        // (it never touches the kv row-version store), so seed just the two clogs here.
        // Local clog: both local xids are Prepared, deref to the global clog.
        kv.write_batch(&[
            put_op(li_old, XidStatus::Prepared(g)),
            put_op(li_new, XidStatus::Prepared(g2)),
        ])
        .expect("local clog");
        // Global clog: g Aborted (abandoned), g2 Committed (re-attempt).
        global
            .write_batch(&[
                put_op(g, XidStatus::Aborted),
                put_op(g2, XidStatus::Committed),
            ])
            .expect("global clog");

        // A settled snapshot: every xid is settled, so global_status reads the global clog.
        let settled = Snapshot {
            xmin: 0,
            xmax: u64::MAX,
            xip: Vec::new(),
        };
        // The two physical versions, both live (xmax = INVALID): old value 100, new value 70.
        let versions = vec![
            (li_old, INVALID_XID, vec![Datum::Int4(100)]),
            (li_new, INVALID_XID, vec![Datum::Int4(70)]),
        ];
        let got = find_visible_one(kv.as_ref(), &global, &settled, &settled, None, &versions)
            .expect("find_visible_one ok")
            .expect("a version is visible");
        assert_eq!(
            got.0, li_new,
            "the committed re-attempt version (highest xmin) wins"
        );
        assert_eq!(
            got.1,
            vec![Datum::Int4(70)],
            "value is the re-attempt's, not the aborted shadow's"
        );
        // Sanity: the aborted shadow really is invisible under this resolver.
        let resolve = global_status(kv.as_ref(), &global, &settled);
        assert!(matches!(resolve(li_old), Ok(XidStatus::Aborted)));
    }

    /// The explicit highest-xmin selection is order-independent, and the at-most-one-live
    /// invariant is debug-asserted. Two committed, non-deleted versions of one row are an
    /// artificial invariant violation: in DEBUG the assert fires (`should_panic`); in
    /// RELEASE the assert is compiled out and the greater xmin is returned regardless of
    /// the order the versions are presented.
    ///
    /// Debug-profile-dependent BY DESIGN: this repo's CI runs `cargo nextest` and
    /// `cargo llvm-cov nextest` in the debug profile, so the `debug_assert!` fires and the
    /// `should_panic` arm is exercised. Introducing a release/opt test profile would flip
    /// the expectation and require revisiting this `cfg_attr`.
    #[test]
    #[cfg_attr(debug_assertions, should_panic(expected = "at-most-one-live"))]
    fn find_visible_one_orders_by_xmin_and_flags_multiple_live() {
        use std::sync::Arc;

        use crabka_pgkv::{Kv, MemKv};
        use crabka_pgmvcc::{
            clog::{XidStatus, put_op},
            visibility::Snapshot,
            xid::INVALID_XID,
        };
        use crabka_pgtypes::Datum;

        use super::find_visible_one;

        let kv = Arc::new(MemKv::new());
        let global = MemKv::new();
        kv.write_batch(&[
            put_op(5, XidStatus::Committed),
            put_op(9, XidStatus::Committed),
        ])
        .expect("clog");
        let settled = Snapshot {
            xmin: 0,
            xmax: u64::MAX,
            xip: Vec::new(),
        };

        // Present them in DESCENDING order so last-wins would pick the LOWER xmin; the
        // explicit max must still pick 9.
        let versions = vec![
            (9u64, INVALID_XID, vec![Datum::Int4(70)]),
            (5u64, INVALID_XID, vec![Datum::Int4(100)]),
        ];
        let got = find_visible_one(kv.as_ref(), &global, &settled, &settled, None, &versions)
            .expect("ok"); // only reached in release builds
        assert_eq!(
            got.expect("visible").0,
            9,
            "highest xmin regardless of presentation order"
        );
    }

    // ───────────────────────── SP40 Task 14: pushdown ─────────────────────────

    mod pushdown {
        use std::sync::{Arc, Mutex};

        use crabka_pgcatalog::{ForeignServer, Table, UserMapping};
        use crabka_pgtypes::Datum;
        use crabka_pgwire::engine::{Engine, QueryResult, Session};

        use crate::{
            SqlEngine,
            clock::EvalCtx,
            error::ExecError,
            exec::extract_scan_bounds,
            foreign::{ForeignScanner, ImportFilter, ScanBounds},
        };

        /// Parse `where_sql` into a WHERE [`Expr`] and run it through
        /// `extract_scan_bounds`. The argument is the predicate text only.
        fn bounds_of(where_sql: &str) -> ScanBounds {
            let expr = crabka_pgparser::parser::parse_expr_for_test(where_sql)
                .expect("the WHERE predicate parses");
            extract_scan_bounds(Some(&expr))
        }

        #[test]
        fn partition_and_lower_bound_pushes_inclusive_start() {
            let b = bounds_of("_partition = 0 AND _offset >= 10");
            assert_eq!(b.start_offsets, vec![(0, 10)]);
            assert!(b.end_offsets.is_empty());
        }

        #[test]
        fn partition_and_upper_strict_pushes_exclusive_end() {
            // `_offset < 50` → exclusive end 50 (unchanged).
            let b = bounds_of("_partition = 1 AND _offset < 50");
            assert!(b.start_offsets.is_empty());
            assert_eq!(b.end_offsets, vec![(1, 50)]);
        }

        #[test]
        fn between_pushes_inclusive_start_and_exclusive_end_plus_one() {
            // BETWEEN bounds are inclusive: [5, 9] → start 5, exclusive end 10.
            let b = bounds_of("_partition = 2 AND _offset BETWEEN 5 AND 9");
            assert_eq!(b.start_offsets, vec![(2, 5)]);
            assert_eq!(b.end_offsets, vec![(2, 10)]);
        }

        #[test]
        fn strict_lower_and_inclusive_upper_apply_exclusivity_correctly() {
            // `_offset > 7` → start 8; `_offset <= 20` → exclusive end 21.
            let b = bounds_of("_partition = 3 AND _offset > 7 AND _offset <= 20");
            assert_eq!(b.start_offsets, vec![(3, 8)]);
            assert_eq!(b.end_offsets, vec![(3, 21)]);
        }

        #[test]
        fn reversed_operand_order_is_normalized() {
            // `10 <= _offset` ≡ `_offset >= 10`; `50 > _offset` ≡ `_offset < 50`.
            let b = bounds_of("_partition = 0 AND 10 <= _offset AND 50 > _offset");
            assert_eq!(b.start_offsets, vec![(0, 10)]);
            assert_eq!(b.end_offsets, vec![(0, 50)]);
        }

        #[test]
        fn timestamp_predicate_is_not_pushed() {
            // `_timestamp` cannot be represented in ScanBounds — stays residual.
            let b = bounds_of("_partition = 0 AND _timestamp > '2020-01-01'");
            assert_eq!(b, ScanBounds::default());
        }

        #[test]
        fn non_envelope_predicate_is_not_pushed() {
            let b = bounds_of("_partition = 0 AND id = 42");
            // The partition anchor exists but no offset bound → empty bounds.
            assert_eq!(b, ScanBounds::default());
        }

        #[test]
        fn bare_offset_without_partition_is_not_pushed() {
            // No `_partition =` to scope the offset to → cannot push.
            let b = bounds_of("_offset >= 10");
            assert_eq!(b, ScanBounds::default());
        }

        #[test]
        fn no_filter_yields_default_bounds() {
            assert_eq!(extract_scan_bounds(None), ScanBounds::default());
        }

        /// A scanner that RECORDS every `ScanBounds` it is handed and returns a
        /// fixed corpus of rows IGNORING the bounds. So a result-equivalence test
        /// proves the residual WHERE still filters, and a recording test proves the
        /// pushed bounds reached the scan.
        struct RecordingScanner {
            seen: Arc<Mutex<Vec<ScanBounds>>>,
            /// Fixed (partition, offset, value) corpus, returned verbatim.
            corpus: Vec<(i32, i64, i64)>,
        }

        impl ForeignScanner for RecordingScanner {
            fn scan(
                &self,
                table: &Table,
                _server: &ForeignServer,
                _mapping: Option<&UserMapping>,
                bounds: &ScanBounds,
                _ctx: &EvalCtx,
            ) -> Result<Vec<Vec<Datum>>, ExecError> {
                self.seen.lock().expect("lock").push(bounds.clone());
                // Envelope columns then one value column `v`; deliberately ignore
                // `bounds` to prove the residual WHERE re-filters.
                assert_eq!(table.columns.len(), 6, "5 envelope cols + value `v`");
                Ok(self
                    .corpus
                    .iter()
                    .map(|&(p, off, v)| {
                        vec![
                            Datum::Int4(p),
                            Datum::Int8(off),
                            Datum::Null, // _timestamp
                            Datum::Null, // _key
                            Datum::Null, // _headers
                            Datum::Int8(v),
                        ]
                    })
                    .collect())
            }

            fn import_schema(
                &self,
                _server: &ForeignServer,
                _mapping: Option<&UserMapping>,
                _filter: &ImportFilter,
            ) -> Result<Vec<crate::foreign::ImportedTable>, ExecError> {
                Ok(Vec::new())
            }
        }

        async fn seed_engine(
            corpus: Vec<(i32, i64, i64)>,
        ) -> (SqlEngine, Arc<Mutex<Vec<ScanBounds>>>) {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let mut engine = SqlEngine::new();
            engine.set_foreign_scanner(Arc::new(RecordingScanner {
                seen: Arc::clone(&seen),
                corpus,
            }));
            {
                let mut s = engine.connect();
                s.simple_query(
                    "CREATE SERVER k FOREIGN DATA WRAPPER kafka_fdw OPTIONS (bootstrap 'b:9092')",
                )
                .await
                .expect("create server");
                s.simple_query("CREATE FOREIGN TABLE f (v int8) SERVER k OPTIONS (topic 'topic')")
                    .await
                    .expect("create foreign table");
            }
            (engine, seen)
        }

        fn rows_of(r: &QueryResult) -> &Vec<Vec<Option<crabka_pgwire::engine::Cell>>> {
            match r {
                QueryResult::Rows { rows, .. } => rows,
                other => panic!("expected Rows, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn single_foreign_table_pushes_recorded_bounds() {
            let (engine, seen) = seed_engine(vec![(0, 10, 100)]).await;
            let mut s = engine.connect();
            s.simple_query("SELECT v FROM f WHERE _partition = 0 AND _offset >= 10")
                .await
                .expect("scan ok");
            let recorded = seen.lock().expect("lock");
            assert_eq!(recorded.len(), 1, "exactly one scan");
            assert_eq!(
                recorded[0].start_offsets,
                vec![(0, 10)],
                "the `_partition = 0 AND _offset >= 10` slice was pushed into the scan"
            );
        }

        #[tokio::test]
        async fn full_scan_when_no_pushable_predicate() {
            let (engine, seen) = seed_engine(vec![(0, 10, 100)]).await;
            let mut s = engine.connect();
            // A bare-offset predicate is NOT pushable → default (full) bounds.
            s.simple_query("SELECT v FROM f WHERE _offset >= 10")
                .await
                .expect("scan ok");
            let recorded = seen.lock().expect("lock");
            assert_eq!(recorded.len(), 1);
            assert_eq!(
                recorded[0],
                ScanBounds::default(),
                "an unanchored offset stays residual → full scan"
            );
        }

        #[tokio::test]
        async fn pushdown_does_not_change_results() {
            // The scanner returns rows OUTSIDE the pushed slice (offsets 5 and 10,
            // partitions 0 and 1) and ignores bounds; the residual WHERE must still
            // yield exactly the rows passing the full predicate.
            let corpus = vec![
                (0, 5, 50),   // _offset 5 < 10 → excluded by WHERE
                (0, 10, 100), // partition 0, offset 10, v=100 → kept
                (0, 12, 7),   // v=7, fails `v > 50` → excluded by WHERE
                (1, 10, 200), // partition 1 → excluded by `_partition = 0`
            ];
            let (engine, seen) = seed_engine(corpus).await;
            let mut s = engine.connect();
            let res = s
                .simple_query("SELECT v FROM f WHERE _partition = 0 AND _offset >= 10 AND v > 50")
                .await
                .expect("scan ok");
            // Only the (0,10,100) row passes the full predicate.
            let rows = rows_of(&res[0]);
            let got: Vec<_> = rows
                .iter()
                .map(|row| {
                    String::from_utf8(row[0].as_ref().expect("v not null").text.to_vec())
                        .expect("utf8")
                })
                .collect();
            assert_eq!(got, vec!["100".to_string()], "residual WHERE still applied");
            // And the bounds were pushed (proves it is a real pushdown, not a no-op).
            assert_eq!(seen.lock().expect("lock")[0].start_offsets, vec![(0, 10)]);
        }
    }

    // ─────────────────── Fix 2: CURRENT_USER / PUBLIC normalization ───────────

    /// `normalize_mapping_user` must map both `"current_user"` and `"public"`
    /// (any case) to `"public"`, and pass through any other user name unchanged.
    #[test]
    fn normalize_mapping_user_maps_current_user_and_public_to_public() {
        use super::normalize_mapping_user;
        assert_eq!(normalize_mapping_user("current_user"), "public");
        assert_eq!(normalize_mapping_user("CURRENT_USER"), "public");
        assert_eq!(normalize_mapping_user("Current_User"), "public");
        assert_eq!(normalize_mapping_user("public"), "public");
        assert_eq!(normalize_mapping_user("PUBLIC"), "public");
        // Named users pass through unchanged.
        assert_eq!(normalize_mapping_user("alice"), "alice");
        assert_eq!(normalize_mapping_user("bob"), "bob");
    }

    /// `CREATE USER MAPPING FOR CURRENT_USER` must be findable via
    /// `crabka_pgcatalog::get_user_mapping(kv, "public", server)`, which confirms
    /// the key is stored under "public", not "current_user".
    #[test]
    fn create_user_mapping_for_current_user_stored_under_public() {
        use crabka_pgkv::{Kv, MemKv};

        let kv = MemKv::new();
        let stmt = crabka_pgparser::parser::parse(
            "CREATE USER MAPPING FOR CURRENT_USER SERVER s OPTIONS (username 'u', password 'p')",
        )
        .expect("parse")
        .into_iter()
        .next()
        .expect("one statement");

        // execute_ddl must succeed and store under "public".
        let fctx = super::ForeignCtx::none();
        let (result, ops) = super::execute_ddl(&kv, &stmt, fctx).expect("execute_ddl ok");
        assert!(
            matches!(result, crabka_pgwire::engine::QueryResult::Command { tag } if tag == "CREATE USER MAPPING"),
            "expected CREATE USER MAPPING command tag"
        );
        kv.write_batch(&ops).expect("apply DDL ops");

        // The mapping must be retrievable under the "public" key.
        let mapping = crabka_pgcatalog::get_user_mapping(&kv, "public", "s")
            .expect("FOR CURRENT_USER mapping must be stored under 'public'");
        assert!(
            mapping.options.iter().any(|(k, _)| k == "username"),
            "options preserved"
        );
    }

    fn command_tag(r: &QueryResult) -> &str {
        match r {
            QueryResult::Command { tag } | QueryResult::Rows { tag, .. } => tag,
            QueryResult::Empty => panic!("expected a tagged result, got Empty"),
        }
    }

    /// Parse `sql` (a DELETE statement) and return its WHERE clause, exercising
    /// the same filter shapes the write path receives.
    fn delete_filter(sql: &str) -> Option<crabka_pgparser::ast::Expr> {
        let stmt = crabka_pgparser::parser::parse(sql)
            .expect("parse")
            .into_iter()
            .next()
            .expect("one statement");
        match stmt {
            Statement::Delete { filter, .. } => filter,
            other => panic!("expected DELETE, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn choose_write_index_probe_matches_single_column_equality_conjuncts() {
        use assert2::assert;

        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (id int4 PRIMARY KEY, flag text)").await;
        let table =
            crabka_pgcatalog::get_table(engine.catalog_kv.as_ref(), &RelationName::public("t"))
                .expect("table");

        let cases: &[(&str, Option<crabka_pgtypes::Datum>)] = &[
            (
                "DELETE FROM t WHERE id = 5",
                Some(crabka_pgtypes::Datum::Int4(5)),
            ),
            (
                "DELETE FROM t WHERE id = 5 AND flag = 'x'",
                Some(crabka_pgtypes::Datum::Int4(5)),
            ),
            (
                "DELETE FROM t WHERE flag = 'x' AND id = 5",
                Some(crabka_pgtypes::Datum::Int4(5)),
            ),
            (
                "DELETE FROM t WHERE 5 = id",
                Some(crabka_pgtypes::Datum::Int4(5)),
            ),
            // Non-indexed column, disjunction, computed column, wrong-type
            // literal, range comparison, and no filter all fall back.
            ("DELETE FROM t WHERE flag = 'x'", None),
            ("DELETE FROM t WHERE id = 5 OR flag = 'x'", None),
            ("DELETE FROM t WHERE id + 1 = 5", None),
            ("DELETE FROM t WHERE id = 5.5", None),
            ("DELETE FROM t WHERE id < 5", None),
            ("DELETE FROM t", None),
        ];
        for (sql, expected) in cases {
            let filter = delete_filter(sql);
            let probe = super::choose_write_index_probe(
                engine.catalog_kv.as_ref(),
                &table,
                filter.as_ref(),
            )
            .expect("choose probe");
            match (probe, expected) {
                (Some((index, value)), Some(want)) => {
                    assert!(index.columns == ["id"], "{sql}");
                    assert!(value == *want, "{sql}");
                }
                (None, None) => {}
                (got, want) => panic!("{sql}: got {got:?}, want {want:?}"),
            }
        }

        // Sharded tables never probe, even with a matching filter shape.
        let mut sharded = table.clone();
        sharded.sharded = true;
        let filter = delete_filter("DELETE FROM t WHERE id = 5");
        let probe =
            super::choose_write_index_probe(engine.catalog_kv.as_ref(), &sharded, filter.as_ref())
                .expect("choose probe");
        assert!(probe.is_none());
    }

    #[tokio::test]
    async fn insert_unique_probe_rejects_duplicates_within_and_across_statements() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE t (id int4 PRIMARY KEY, v text)").await;
        run_s(&mut s, "INSERT INTO t VALUES (1, 'a')").await;

        // Across committed rows.
        let err = s
            .simple_query("INSERT INTO t VALUES (1, 'b')")
            .await
            .expect_err("duplicate committed key");
        assert!(err.code == "23505");

        // Within one statement (rows not yet in the kv: pending-key dedup).
        let err = s
            .simple_query("INSERT INTO t VALUES (2, 'a'), (2, 'b')")
            .await
            .expect_err("duplicate within statement");
        assert!(err.code == "23505");

        // Across statements inside one transaction: the probe sees our own
        // uncommitted row via read-your-writes.
        run_s(&mut s, "BEGIN").await;
        run_s(&mut s, "INSERT INTO t VALUES (3, 'x')").await;
        let err = s
            .simple_query("INSERT INTO t VALUES (3, 'y')")
            .await
            .expect_err("duplicate of own uncommitted row");
        assert!(err.code == "23505");
        run_s(&mut s, "ROLLBACK").await;

        // The rolled-back insert left only dead index entries: the key is free.
        run_s(&mut s, "INSERT INTO t VALUES (3, 'z')").await;

        // UPDATE moving a row onto a held key is a violation too.
        let err = s
            .simple_query("UPDATE t SET id = 1 WHERE id = 3")
            .await
            .expect_err("update onto held key");
        assert!(err.code == "23505");
    }

    #[tokio::test]
    async fn insert_unique_probe_ignores_dead_versions_of_the_key() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE t (id int4 PRIMARY KEY, v text)").await;
        run_s(&mut s, "INSERT INTO t VALUES (1, 'a')").await;

        // Move the key away: the index keeps a dead entry for id=1 pointing at
        // the superseded version, which must not count as a holder.
        run_s(&mut s, "UPDATE t SET id = 2 WHERE id = 1").await;
        run_s(&mut s, "INSERT INTO t VALUES (1, 'b')").await;

        // Delete-then-reinsert: the deleted version's entry is dead too.
        run_s(&mut s, "DELETE FROM t WHERE id = 2").await;
        run_s(&mut s, "INSERT INTO t VALUES (2, 'c')").await;

        let r = &run_s(&mut s, "SELECT id, v FROM t ORDER BY id").await[0];
        let rows = rows_of(r);
        assert!(rows.len() == 2);
        assert!(text(&rows[0][0]) == Some("1".into()));
        assert!(text(&rows[0][1]) == Some("b".into()));
        assert!(text(&rows[1][0]) == Some("2".into()));
        assert!(text(&rows[1][1]) == Some("c".into()));
    }

    #[tokio::test]
    async fn point_update_via_index_probe_applies_residual_filter_and_returning() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(
            &mut s,
            "CREATE TABLE t (id int4 PRIMARY KEY, flag text, v text)",
        )
        .await;
        run_s(
            &mut s,
            "INSERT INTO t VALUES (1,'x','a'), (2,'x','b'), (3,'y','c')",
        )
        .await;

        // Indexed equality + residual conjunct: only id=2 matches both.
        let r = &run_s(&mut s, "UPDATE t SET v = 'z' WHERE id = 2 AND flag = 'x'").await[0];
        assert!(command_tag(r) == "UPDATE 1");

        // Residual conjunct rejects the probed row: no row is touched.
        let r = &run_s(&mut s, "UPDATE t SET v = 'w' WHERE id = 3 AND flag = 'x'").await[0];
        assert!(command_tag(r) == "UPDATE 0");

        // RETURNING reflects the updated row exactly.
        let r = &run_s(
            &mut s,
            "UPDATE t SET v = 'r' WHERE id = 1 AND flag = 'x' RETURNING id, v",
        )
        .await[0];
        assert!(command_tag(r) == "UPDATE 1");
        let returned = rows_of(r);
        assert!(returned.len() == 1);
        assert!(text(&returned[0][0]) == Some("1".into()));
        assert!(text(&returned[0][1]) == Some("r".into()));

        let r = &run_s(&mut s, "SELECT id, flag, v FROM t ORDER BY id").await[0];
        let rows = rows_of(r);
        let contents: Vec<Vec<Option<String>>> = rows
            .iter()
            .map(|row| row.iter().map(text).collect())
            .collect();
        assert!(
            contents
                == vec![
                    vec![Some("1".into()), Some("x".into()), Some("r".into())],
                    vec![Some("2".into()), Some("x".into()), Some("z".into())],
                    vec![Some("3".into()), Some("y".into()), Some("c".into())],
                ]
        );
    }

    #[tokio::test]
    async fn point_delete_via_index_probe_applies_residual_filter_and_returning() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(
            &mut s,
            "CREATE TABLE t (id int4 PRIMARY KEY, flag text, v text)",
        )
        .await;
        run_s(
            &mut s,
            "INSERT INTO t VALUES (1,'x','a'), (2,'x','b'), (3,'y','c')",
        )
        .await;

        // Residual conjunct rejects the probed row: nothing is deleted.
        let r = &run_s(&mut s, "DELETE FROM t WHERE id = 3 AND flag = 'x'").await[0];
        assert!(command_tag(r) == "DELETE 0");

        let r = &run_s(
            &mut s,
            "DELETE FROM t WHERE id = 1 AND flag = 'x' RETURNING id, v",
        )
        .await[0];
        assert!(command_tag(r) == "DELETE 1");
        let returned = rows_of(r);
        assert!(returned.len() == 1);
        assert!(text(&returned[0][0]) == Some("1".into()));
        assert!(text(&returned[0][1]) == Some("a".into()));

        let r = &run_s(&mut s, "SELECT id FROM t ORDER BY id").await[0];
        let rows = rows_of(r);
        assert!(rows.len() == 2);
        assert!(text(&rows[0][0]) == Some("2".into()));
        assert!(text(&rows[1][0]) == Some("3".into()));
    }

    #[tokio::test]
    async fn update_and_delete_fall_back_to_full_scan_for_non_indexed_predicates() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(
            &mut s,
            "CREATE TABLE t (id int4 PRIMARY KEY, flag text, v text)",
        )
        .await;
        run_s(
            &mut s,
            "INSERT INTO t VALUES (1,'x','a'), (2,'x','b'), (3,'y','c')",
        )
        .await;

        // Non-indexed equality: full scan, same result as the probe would give.
        let r = &run_s(&mut s, "UPDATE t SET v = 'q' WHERE flag = 'x'").await[0];
        assert!(command_tag(r) == "UPDATE 2");

        // Disjunction on the indexed column: not a conjunctive equality, so the
        // fallback full scan must handle it.
        let r = &run_s(&mut s, "UPDATE t SET v = 'd' WHERE id = 1 OR id = 3").await[0];
        assert!(command_tag(r) == "UPDATE 2");

        let r = &run_s(&mut s, "SELECT id, v FROM t ORDER BY id").await[0];
        let rows = rows_of(r);
        let contents: Vec<Vec<Option<String>>> = rows
            .iter()
            .map(|row| row.iter().map(text).collect())
            .collect();
        assert!(
            contents
                == vec![
                    vec![Some("1".into()), Some("d".into())],
                    vec![Some("2".into()), Some("q".into())],
                    vec![Some("3".into()), Some("d".into())],
                ]
        );

        // Range predicate on the indexed column: also a fallback.
        let r = &run_s(&mut s, "DELETE FROM t WHERE id > 1").await[0];
        assert!(command_tag(r) == "DELETE 2");
        let r = &run_s(&mut s, "SELECT id FROM t").await[0];
        assert!(rows_of(r).len() == 1);
        assert!(text(&rows_of(r)[0][0]) == Some("1".into()));
    }

    #[test]
    fn column_type_from_oid_maps_json_jsonb_and_every_array_oid() {
        use assert2::assert;
        use crabka_pgtypes::{ColumnType, ElemType, oids};

        for (oid, expected) in [
            // `json` is an input alias for `jsonb`.
            (oids::JSON, ColumnType::Jsonb),
            (oids::JSONB, ColumnType::Jsonb),
            (oids::JSONARRAY, ColumnType::Array(ElemType::Jsonb)),
        ] {
            assert!(super::column_type_from_oid(oid).expect("known oid") == expected);
        }
        for elem in ElemType::ALL {
            assert!(
                super::column_type_from_oid(elem.array_oid()).expect("array oid")
                    == ColumnType::Array(elem)
            );
        }
        assert!(super::column_type_from_oid(999_999).is_err());
    }

    #[test]
    fn pg_type_exposes_the_scalar_array_link_for_every_row() {
        use assert2::assert;
        use crabka_pgtypes::ElemType;

        let rows = super::builtin_type_rows();
        for scalar in rows.iter().filter(|row| row.array != 0) {
            let array = rows
                .iter()
                .find(|row| row.oid == scalar.array)
                .unwrap_or_else(|| panic!("{} has no array row", scalar.name));
            assert!(
                (array.elem, array.category, array.len, array.array) == (scalar.oid, "A", -1, 0)
            );
        }
        for array in rows.iter().filter(|row| row.category == "A") {
            assert!(
                rows.iter().any(|row| row.oid == array.elem),
                "{} has a dangling typelem",
                array.name
            );
        }
        // Every element type crabka can build an array of has a pg_type row.
        for elem in ElemType::ALL {
            let oid = i32::try_from(elem.array_oid()).expect("array oid fits in int4");
            assert!(rows.iter().any(|row| row.oid == oid), "{elem:?}");
        }
    }

    #[test]
    fn pg_type_rows_match_the_declared_column_list() {
        use assert2::assert;

        let columns = super::virtual_catalog_columns("pg_type");
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        assert!(
            names
                == [
                    "oid",
                    "typname",
                    "typlen",
                    "typcategory",
                    "typnamespace",
                    "typrelid",
                    "typtype",
                    "typdelim",
                    "typelem",
                    "typarray",
                    "typbasetype",
                ]
        );
        let rows = super::pg_type_rows();
        for row in &rows {
            assert!(row.len() == columns.len());
        }
        // The `_int4` row, in full: PostgreSQL's own values for OID 1007.
        let int4_array = rows
            .iter()
            .find(|row| row[0] == super::int(1007))
            .expect("_int4 row");
        assert!(
            *int4_array
                == vec![
                    super::int(1007),
                    super::text("_int4"),
                    super::int(-1),
                    super::text("A"),
                    super::int(super::PG_CATALOG_NAMESPACE_OID),
                    super::int(0),
                    super::text("b"),
                    super::text(","),
                    super::int(23),
                    super::int(0),
                    super::int(0),
                ]
        );
    }

    #[test]
    fn coerce_assigns_literals_and_arrays_to_jsonb_and_array_columns() {
        use assert2::assert;
        use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType};

        let ctx = crate::clock::EvalCtx::test_default();
        let jsonb = super::coerce(
            Datum::Text("{\"b\":2,\"a\":1}".into()),
            ColumnType::Jsonb,
            &ctx,
        )
        .expect("jsonb literal");
        assert!(
            jsonb
                == Datum::Jsonb(
                    crabka_pgtypes::jsonb::parse("{\"a\":1,\"b\":2}").expect("canonical parse")
                )
        );
        let array = super::coerce(
            Datum::Text("{1,2}".into()),
            ColumnType::Array(ElemType::Int4),
            &ctx,
        )
        .expect("array literal");
        assert!(
            array
                == Datum::Array(ArrayValue::new(
                    ElemType::Int4,
                    vec![Datum::Int4(1), Datum::Int4(2)]
                ))
        );
        // An int4[] value widens element-wise into a bigint[] column.
        let widened = super::coerce(
            Datum::Array(ArrayValue::new(ElemType::Int4, vec![Datum::Int4(7)])),
            ColumnType::Array(ElemType::Int8),
            &ctx,
        )
        .expect("element-wise widening");
        assert!(widened == Datum::Array(ArrayValue::new(ElemType::Int8, vec![Datum::Int8(7)])));
        // Malformed input is the type's input-function error, not 42804.
        assert!(super::coerce(Datum::Text("{".into()), ColumnType::Jsonb, &ctx).is_err());
    }

    /// The DDL gate covers every column type: exactly the types whose datums
    /// [`super::hash_bucket_for_row`] can hash are accepted as a hash shard key.
    #[test]
    fn only_hashable_column_types_are_accepted_as_a_hash_shard_key() {
        use assert2::assert;
        use crabka_pgcatalog::Column;
        use crabka_pgtypes::{ColumnType, ElemType};

        let sharding = |column: &str| {
            crabka_pgcatalog::ShardingStrategy::Hash(crabka_pgcatalog::HashSharding {
                columns: vec![column.to_string()],
                buckets: 4,
                co_location_group: None,
            })
        };
        for (ty, supported) in [
            (ColumnType::Int4, true),
            (ColumnType::Int8, true),
            (ColumnType::Text, true),
            (ColumnType::Varchar(Some(8)), true),
            (ColumnType::Char(Some(8)), true),
            (ColumnType::Bytea, true),
            (ColumnType::Uuid, true),
            (ColumnType::Regclass, true),
            (ColumnType::Bool, false),
            (ColumnType::Float8, false),
            (ColumnType::Numeric(None), false),
            (ColumnType::Date, false),
            (ColumnType::Time, false),
            (ColumnType::Timestamp, false),
            (ColumnType::Timestamptz, false),
            (ColumnType::Interval, false),
            (ColumnType::Jsonb, false),
            (ColumnType::Array(ElemType::Int4), false),
        ] {
            let columns = vec![Column::new("k", ty)];
            let result =
                super::ensure_hash_shard_key_types_are_supported(&columns, Some(&sharding("k")));
            assert!(result.is_ok() == supported, "{ty:?}");
            if !supported {
                let error = result.expect_err("unhashable key").into_pg();
                assert!(error.code == "0A000");
                assert!(error.message.contains("\"k\""), "{}", error.message);
                assert!(error.message.contains(ty.name()), "{}", error.message);
            }
        }
        // A hash column that does not exist is left to the catalog's own
        // undefined-column error, and an unsharded table has nothing to check.
        let columns = vec![Column::new("k", ColumnType::Jsonb)];
        assert!(
            super::ensure_hash_shard_key_types_are_supported(&columns, Some(&sharding("missing")))
                .is_ok()
        );
        assert!(super::ensure_hash_shard_key_types_are_supported(&columns, None).is_ok());
    }

    /// The write-path backstop still refuses an unhashable shard key. That is
    /// reachable for a table whose sharding was attached outside CREATE TABLE.
    #[test]
    fn hashing_a_row_refuses_an_unhashable_shard_key() {
        use assert2::assert;
        use crabka_pgcatalog::Column;
        use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType};

        let table = crabka_pgcatalog::Table {
            id: 1,
            name: RelationName::public("t"),
            columns: vec![Column::new("k", ColumnType::Jsonb)],
            sharded: true,
            sharding: Some(crabka_pgcatalog::ShardingStrategy::Hash(
                crabka_pgcatalog::HashSharding {
                    columns: vec!["k".into()],
                    buckets: 4,
                    co_location_group: None,
                },
            )),
            foreign: None,
            checks: Vec::new(),
        };
        for value in [
            Datum::Jsonb(crabka_pgtypes::jsonb::parse("{\"a\":1}").expect("jsonb")),
            Datum::Array(ArrayValue::new(ElemType::Int4, vec![Datum::Int4(1)])),
        ] {
            let error = super::hash_bucket_for_row(&table, &[value])
                .expect_err("unhashable")
                .into_pg();
            assert!(error.code == "0A000");
            assert!(error.message == "hash shard key type is not supported");
        }
        // A hashable value in the same slot still routes.
        assert!(
            super::hash_bucket_for_row(&table, &[Datum::Int4(1)])
                .expect("hashable")
                .is_some()
        );
    }

    /// The DDL path builds a hash sharding only from a one-column key, the
    /// arity [`super::hash_bucket_for_row`] can encode. The grammar already
    /// refuses a wider `SHARDED BY HASH` list, so this covers the seam for an
    /// AST built by something other than the parser. Bucket counts are still
    /// checked, and a valid spec still converts.
    #[test]
    fn the_ddl_path_builds_a_hash_sharding_only_from_one_column() {
        use assert2::assert;
        use crabka_pgparser::ast::{HashShardingSpec, ShardingSpec};

        let spec = |columns: &[&str], buckets: u32| {
            ShardingSpec::Hash(HashShardingSpec {
                columns: columns.iter().map(|column| (*column).to_string()).collect(),
                buckets,
                co_location_group: None,
            })
        };
        let arity = Some("hash sharding requires exactly one column");
        let buckets_message = Some("hash sharding bucket count must be a power of two");
        for (columns, buckets, expected) in [
            (&[][..], 4, arity),
            (&["a"][..], 4, None),
            (&["a", "b"][..], 4, arity),
            (&["a", "b", "c"][..], 4, arity),
            (&["a"][..], 0, buckets_message),
            (&["a"][..], 6, buckets_message),
        ] {
            let converted = super::hash_sharding_from_ast(&spec(columns, buckets));
            match expected {
                Some(message) => {
                    let error = converted.expect_err("refused").into_pg();
                    assert!(error.code == "0A000", "{columns:?}/{buckets}");
                    assert!(error.message == message, "{columns:?}/{buckets}");
                }
                None => assert!(
                    converted.expect("accepted")
                        == crabka_pgcatalog::ShardingStrategy::Hash(
                            crabka_pgcatalog::HashSharding {
                                columns: vec!["a".into()],
                                buckets,
                                co_location_group: None,
                            }
                        ),
                    "{columns:?}/{buckets}"
                ),
            }
        }
    }

    /// The write-path backstop behind the two creation gates: a multi-column
    /// hash sharding has no row encoding, so the write is refused rather than
    /// placing the row under the hash of its first column, where a route
    /// computed from the whole key never looks.
    #[test]
    fn hashing_a_row_refuses_a_multi_column_hash_shard_key() {
        use assert2::assert;
        use crabka_pgcatalog::Column;
        use crabka_pgtypes::{ColumnType, Datum};

        let table = crabka_pgcatalog::Table {
            id: 1,
            name: RelationName::public("t"),
            columns: vec![
                Column::new("a", ColumnType::Int4),
                Column::new("b", ColumnType::Int4),
            ],
            sharded: true,
            sharding: Some(crabka_pgcatalog::ShardingStrategy::Hash(
                crabka_pgcatalog::HashSharding {
                    columns: vec!["a".into(), "b".into()],
                    buckets: 4,
                    co_location_group: None,
                },
            )),
            foreign: None,
            checks: Vec::new(),
        };
        let error = super::hash_bucket_for_row(&table, &[Datum::Int4(1), Datum::Int4(2)])
            .expect_err("no row encoding for a two-column key")
            .into_pg();
        assert!(error.code == "0A000");
        assert!(error.message == "hash sharding requires exactly one hash column");
    }

    #[test]
    fn jsonb_and_array_defaults_render_as_quoted_literals() {
        use assert2::assert;
        use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType};

        let doc = Datum::Jsonb(crabka_pgtypes::jsonb::parse("{\"a\":1}").expect("parse"));
        assert!(super::format_default_value(&doc, ColumnType::Jsonb) == "'{\"a\": 1}'::jsonb");
        let array = Datum::Array(ArrayValue::new(
            ElemType::Int4,
            vec![Datum::Int4(1), Datum::Int4(2)],
        ));
        assert!(
            super::format_default_value(&array, ColumnType::Array(ElemType::Int4))
                == "'{1,2}'::integer[]"
        );
    }

    #[test]
    fn a_from_item_function_must_be_a_known_set_returning_function() {
        use assert2::assert;
        use crabka_pgparser::ast::Expr;
        use crabka_pgtypes::{ColumnType, Datum, ElemType};

        let call = |name: &str, arg: Expr| {
            vec![crabka_pgparser::ast::TableFuncCall {
                name: name.into(),
                args: vec![arg],
                column_defs: None,
            }]
        };
        let array_arg = Expr::Const {
            value: Datum::Null,
            ty: ColumnType::Array(ElemType::Text),
        };
        // A name no SRF registry entry claims is 42883, PostgreSQL's failed
        // function lookup, on both the row and the schema-only path.
        let unknown = call("no_such_function", array_arg);
        for relation in [
            crate::srf::from_item(
                &unknown,
                false,
                None,
                &None,
                &crate::clock::EvalCtx::test_default(),
            ),
            crate::srf::from_item_schema(&unknown, false, None, &None),
        ] {
            assert!(matches!(relation, Err(ExecError::UndefinedFunction(_))));
        }
        // A non-array `unnest` argument resolves to no `unnest` function at all.
        let scalar = call(
            "unnest",
            Expr::Const {
                value: Datum::Int4(1),
                ty: ColumnType::Int4,
            },
        );
        assert!(matches!(
            crate::srf::from_item_schema(&scalar, false, None, &None),
            Err(ExecError::UndefinedFunction(_))
        ));
    }

    #[tokio::test]
    async fn unnest_in_from_expands_an_array_argument() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        let r = &run_s(&mut s, "SELECT * FROM unnest('{3,1,2}'::int[])").await[0];
        assert!(fields_of(r)[0].name == "unnest");
        assert!(fields_of(r)[0].type_oid == crabka_pgtypes::oids::INT4);
        let values: Vec<Option<String>> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert!(values == vec![Some("3".into()), Some("1".into()), Some("2".into())]);

        let r = &run_s(
            &mut s,
            "SELECT * FROM unnest(int4multirange(int4range(1, 3), int4range(5, 7)))",
        )
        .await[0];
        let values: Vec<Option<String>> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert!(values == vec![Some("[1,3)".into()), Some("[5,7)".into())]);

        // Alias and column alias behave as they do for a derived table.
        let r = &run_s(
            &mut s,
            "SELECT u.x FROM unnest('{a,b}'::text[]) AS u(x) ORDER BY u.x",
        )
        .await[0];
        assert!(fields_of(r)[0].name == "x");
        let values: Vec<Option<String>> = rows_of(r).iter().map(|row| text(&row[0])).collect();
        assert!(values == vec![Some("a".into()), Some("b".into())]);

        // A NULL array and an empty array both expand to zero rows.
        for sql in [
            "SELECT * FROM unnest(NULL::int[])",
            "SELECT * FROM unnest('{}'::int[])",
        ] {
            assert!(rows_of(&run_s(&mut s, sql).await[0]).is_empty(), "{sql}");
        }

        // A name the SRF registry does not claim is 42883 in FROM position.
        let error = s
            .simple_query("SELECT * FROM no_such_function(1, 3)")
            .await
            .expect_err("no such table function");
        assert!(error.code == "42883", "{error:?}");
    }

    #[tokio::test]
    async fn jsonb_and_array_columns_round_trip_through_ddl() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(
            &mut s,
            "CREATE TABLE t (id int4 PRIMARY KEY, j jsonb, a int[])",
        )
        .await;
        run_s(
            &mut s,
            "INSERT INTO t (id, j, a) VALUES (1, '{\"b\":2,\"a\":1}', '{3,4}'), (2, NULL, '{}')",
        )
        .await;
        let r = &run_s(&mut s, "SELECT j, a FROM t ORDER BY id").await[0];
        assert!(fields_of(r)[0].type_oid == crabka_pgtypes::oids::JSONB);
        assert!(fields_of(r)[1].type_oid == crabka_pgtypes::oids::INT4ARRAY);
        let values: Vec<Vec<Option<String>>> = rows_of(r)
            .iter()
            .map(|row| row.iter().map(text).collect())
            .collect();
        assert!(
            values
                == vec![
                    vec![Some("{\"a\": 1, \"b\": 2}".into()), Some("{3,4}".into())],
                    vec![None, Some("{}".into())],
                ]
        );

        // jsonb/array defaults persist and apply; a default of a type the
        // catalog still cannot encode is refused at DDL time instead.
        run_s(
            &mut s,
            "CREATE TABLE d (id int4, j jsonb DEFAULT '{}', a int[] DEFAULT '{1}')",
        )
        .await;
        run_s(&mut s, "INSERT INTO d (id) VALUES (1)").await;
        let r = &run_s(&mut s, "SELECT j, a FROM d").await[0];
        assert!(
            rows_of(r)[0].iter().map(text).collect::<Vec<_>>()
                == vec![Some("{}".into()), Some("{1}".into())]
        );
        let error = s
            .simple_query("CREATE TABLE e (d date DEFAULT '2020-01-01'::date)")
            .await
            .expect_err("an unencodable default is refused");
        assert!(error.code == "0A000", "{error:?}");
    }

    #[tokio::test]
    async fn pg_index_marks_the_primary_key_index() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE t (id int4 PRIMARY KEY, v text)").await;
        run_s(&mut s, "CREATE UNIQUE INDEX t_v_key ON t (v)").await;
        let r = &run_s(
            &mut s,
            "SELECT indisunique, indisprimary FROM pg_index ORDER BY indexrelid",
        )
        .await[0];
        let values: Vec<Vec<Option<String>>> = rows_of(r)
            .iter()
            .map(|row| row.iter().map(text).collect())
            .collect();
        assert!(
            values
                == vec![
                    vec![Some("t".into()), Some("t".into())],
                    vec![Some("t".into()), Some("f".into())],
                ]
        );
    }

    /// Build a table and its indexes for the arbiter-resolution tests.
    /// `indexes` entries are `(name, columns, unique, is_constraint)`.
    fn arbiter_fixture(
        columns: &[&str],
        indexes: &[(&str, &[&str], bool, bool)],
    ) -> (crabka_pgcatalog::Table, Vec<crabka_pgcatalog::Index>) {
        let table = crabka_pgcatalog::Table {
            id: 1,
            name: RelationName::public("t"),
            columns: columns
                .iter()
                .map(|name| crabka_pgcatalog::Column::new(*name, crabka_pgtypes::ColumnType::Int4))
                .collect(),
            sharded: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        };
        let indexes = indexes
            .iter()
            .enumerate()
            .map(
                |(i, (name, cols, unique, constraint))| crabka_pgcatalog::Index {
                    id: i as u32 + 1,
                    name: (*name).to_string(),
                    table: RelationName::public("t"),
                    table_id: 1,
                    columns: cols.iter().map(|c| (*c).to_string()).collect(),
                    unique: *unique,
                    placement: crabka_pgcatalog::IndexPlacement::Local,
                    method: crabka_pgcatalog::IndexMethod::Btree,
                    constraint: constraint.then_some(crabka_pgcatalog::IndexConstraint::Unique),
                },
            )
            .collect();
        (table, indexes)
    }

    #[test]
    fn arbiter_resolution_matches_column_sets_and_constraint_names() {
        use assert2::assert;
        use crabka_pgparser::ast::OnConflictTarget;

        let (table, indexes) = arbiter_fixture(
            &["a", "b", "c"],
            &[
                ("t_pkey", &["a"], true, true),
                ("t_ab_key", &["a", "b"], true, true),
                ("t_c_idx", &["c"], false, false),
                ("t_c_uniq", &["c"], true, false),
            ],
        );
        let names = |target: &OnConflictTarget| {
            super::resolve_arbiter_indexes(&table, &indexes, target)
                .map(|found| found.iter().map(|i| i.name.clone()).collect::<Vec<_>>())
        };
        let columns = |cols: &[&str]| OnConflictTarget::Columns {
            columns: cols.iter().map(|c| (*c).to_string()).collect(),
            index_predicate: None,
        };

        // No target: every unique local index arbitrates (the non-unique one
        // never does).
        assert!(
            names(&OnConflictTarget::None)
                == Ok(vec!["t_pkey".into(), "t_ab_key".into(), "t_c_uniq".into()])
        );
        // Column-set inference, order-insensitive: `(b, a)` finds `UNIQUE (a, b)`.
        assert!(names(&columns(&["a"])) == Ok(vec!["t_pkey".into()]));
        assert!(names(&columns(&["b", "a"])) == Ok(vec!["t_ab_key".into()]));
        // A unique index needs no constraint to be inferred by columns.
        assert!(names(&columns(&["c"])) == Ok(vec!["t_c_uniq".into()]));
        // A subset/superset of an index's columns is not a match: 42P10.
        assert!(names(&columns(&["b"])) == Err(ExecError::OnConflictNoArbiter));
        assert!(
            names(&columns(&["a", "b", "c"])) == Err(ExecError::OnConflictNoArbiter),
            "no index covers all three columns"
        );
        // An unknown inference column is 42703, checked before arbitration.
        assert!(names(&columns(&["nope"])) == Err(ExecError::UndefinedColumn("nope".into())));
        // ON CONSTRAINT resolves by name, but only for constraint-backed indexes.
        assert!(
            names(&OnConflictTarget::OnConstraint("t_ab_key".into()))
                == Ok(vec!["t_ab_key".into()])
        );
        for name in ["t_c_uniq", "t_c_idx", "nosuch"] {
            assert!(
                names(&OnConflictTarget::OnConstraint(name.into()))
                    == Err(ExecError::UndefinedConstraint {
                        name: name.into(),
                        table: "t".into(),
                    }),
                "ON CONSTRAINT {name}"
            );
        }
        // An inference predicate is refused: there are no partial indexes to match.
        let predicated = OnConflictTarget::Columns {
            columns: vec!["a".into()],
            index_predicate: Some(crabka_pgparser::ast::Expr::BoolLiteral(true)),
        };
        assert!(matches!(
            super::resolve_arbiter_indexes(&table, &indexes, &predicated),
            Err(ExecError::Unsupported(_))
        ));
    }

    #[test]
    fn arbiter_resolution_without_unique_indexes_is_empty_not_an_error() {
        use assert2::assert;
        use crabka_pgparser::ast::OnConflictTarget;

        // `DO NOTHING` with no target on a table with no unique index: legal,
        // and every row simply inserts.
        let (table, indexes) = arbiter_fixture(&["a"], &[("t_a_idx", &["a"], false, false)]);
        let found = super::resolve_arbiter_indexes(&table, &indexes, &OnConflictTarget::None);
        assert!(found == Ok(Vec::new()));
    }

    #[tokio::test]
    async fn on_conflict_do_nothing_and_do_update_upsert() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE t (id int4 PRIMARY KEY, v text)").await;
        run_s(&mut s, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").await;

        // DO NOTHING skips the conflicting row and does not count it.
        let r = &run_s(
            &mut s,
            "INSERT INTO t VALUES (1, 'x'), (3, 'c') ON CONFLICT (id) DO NOTHING",
        )
        .await[0];
        assert!(matches!(r, QueryResult::Command { tag } if tag == "INSERT 0 1"));

        // DO UPDATE upserts: the conflicting row is updated (and counted), the
        // new one inserted. RETURNING reports the post-image.
        let r = &run_s(
            &mut s,
            "INSERT INTO t VALUES (1, 'x'), (4, 'd') \
             ON CONFLICT (id) DO UPDATE SET v = excluded.v || t.v RETURNING id, v",
        )
        .await[0];
        let values: Vec<Vec<Option<String>>> = rows_of(r)
            .iter()
            .map(|row| row.iter().map(text).collect())
            .collect();
        assert!(
            values
                == vec![
                    vec![Some("1".into()), Some("xa".into())],
                    vec![Some("4".into()), Some("d".into())]
                ]
        );

        let r = &run_s(&mut s, "SELECT id, v FROM t ORDER BY id").await[0];
        let values: Vec<Vec<Option<String>>> = rows_of(r)
            .iter()
            .map(|row| row.iter().map(text).collect())
            .collect();
        assert!(
            values
                == vec![
                    vec![Some("1".into()), Some("xa".into())],
                    vec![Some("2".into()), Some("b".into())],
                    vec![Some("3".into()), Some("c".into())],
                    vec![Some("4".into()), Some("d".into())],
                ]
        );
    }
    // ---- D6: foreign keys wired into the local write path ----

    #[tokio::test]
    async fn on_delete_cascade_removes_the_referencing_rows() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE p (id int4 PRIMARY KEY)").await;
        run_s(
            &mut s,
            "CREATE TABLE c (id int4 PRIMARY KEY, p int4 REFERENCES p (id) ON DELETE CASCADE)",
        )
        .await;
        run_s(&mut s, "INSERT INTO p VALUES (1), (2)").await;
        run_s(&mut s, "INSERT INTO c VALUES (10, 1), (11, 1), (12, 2)").await;
        run_s(&mut s, "DELETE FROM p WHERE id = 1").await;
        assert!(
            text_rows_of(&mut s, "SELECT id FROM c ORDER BY id").await
                == vec![vec![Some("12".to_string())]]
        );
    }

    #[tokio::test]
    async fn a_cascade_cycle_between_two_tables_terminates() {
        use assert2::assert;

        // a -> b -> a, both ON DELETE CASCADE. The cascade comes back around to
        // the row the statement itself deleted, which the drain reads through
        // the staged batch and therefore sees as gone; a cascade that revisits a
        // row *it* deleted cannot be recognised that way and is stopped by
        // `StatementWrites` instead.
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE cyc_a (id int4 PRIMARY KEY, b int4)").await;
        run_s(
            &mut s,
            "CREATE TABLE cyc_b (id int4 PRIMARY KEY, \
             a int4 REFERENCES cyc_a (id) ON DELETE CASCADE)",
        )
        .await;
        run_s(
            &mut s,
            "ALTER TABLE cyc_a ADD CONSTRAINT cyc_a_b_fkey \
             FOREIGN KEY (b) REFERENCES cyc_b (id) ON DELETE CASCADE",
        )
        .await;
        run_s(&mut s, "INSERT INTO cyc_a VALUES (1, NULL)").await;
        run_s(&mut s, "INSERT INTO cyc_b VALUES (1, 1)").await;
        run_s(&mut s, "UPDATE cyc_a SET b = 1 WHERE id = 1").await;
        run_s(&mut s, "DELETE FROM cyc_a WHERE id = 1").await;
        assert!(text_rows_of(&mut s, "SELECT id FROM cyc_a").await == Vec::<Vec<_>>::new());
        assert!(text_rows_of(&mut s, "SELECT id FROM cyc_b").await == Vec::<Vec<_>>::new());
    }

    #[tokio::test]
    async fn a_self_referencing_cascade_terminates_on_a_tree_and_on_a_self_loop() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(
            &mut s,
            "CREATE TABLE tree (id int4 PRIMARY KEY, \
             parent int4 REFERENCES tree (id) ON DELETE CASCADE)",
        )
        .await;
        run_s(&mut s, "INSERT INTO tree VALUES (1, NULL), (2, 1), (3, 2)").await;
        // A row that references itself: the cascade revisits the very row the
        // statement deleted, and stops there.
        run_s(&mut s, "INSERT INTO tree VALUES (4, 4)").await;
        run_s(&mut s, "DELETE FROM tree WHERE id = 4").await;
        run_s(&mut s, "DELETE FROM tree WHERE id = 1").await;
        assert!(text_rows_of(&mut s, "SELECT id FROM tree").await == Vec::<Vec<_>>::new());
    }

    #[tokio::test]
    async fn on_update_cascade_follows_the_referenced_key() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE up (id int4 PRIMARY KEY, v int4)").await;
        run_s(
            &mut s,
            "CREATE TABLE uc (a int4 REFERENCES up (id) ON UPDATE CASCADE)",
        )
        .await;
        run_s(&mut s, "INSERT INTO up VALUES (1, 100)").await;
        run_s(&mut s, "INSERT INTO uc VALUES (1)").await;
        // A non-key update of the parent leaves the child alone and never
        // touches the key lock.
        run_s(&mut s, "UPDATE up SET v = 200 WHERE id = 1").await;
        assert!(
            text_rows_of(&mut s, "SELECT a FROM uc").await == vec![vec![Some("1".to_string())]]
        );
        run_s(&mut s, "UPDATE up SET id = 2 WHERE id = 1").await;
        assert!(
            text_rows_of(&mut s, "SELECT a FROM uc").await == vec![vec![Some("2".to_string())]]
        );
    }

    #[tokio::test]
    async fn set_null_onto_a_not_null_column_is_the_ordinary_23502() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE np (id int4 PRIMARY KEY)").await;
        run_s(
            &mut s,
            "CREATE TABLE nc (a int4 NOT NULL REFERENCES np (id) ON DELETE SET NULL)",
        )
        .await;
        run_s(&mut s, "INSERT INTO np VALUES (1)").await;
        run_s(&mut s, "INSERT INTO nc VALUES (1)").await;
        assert!(sqlstate_of(&mut s, "DELETE FROM np WHERE id = 1").await == "23502");
    }

    #[tokio::test]
    async fn restrict_and_no_action_report_different_sqlstates() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE rp1 (id int4 PRIMARY KEY)").await;
        run_s(&mut s, "CREATE TABLE rc1 (a int4 REFERENCES rp1 (id))").await;
        run_s(&mut s, "CREATE TABLE rp2 (id int4 PRIMARY KEY)").await;
        run_s(
            &mut s,
            "CREATE TABLE rc2 (a int4 REFERENCES rp2 (id) ON DELETE RESTRICT)",
        )
        .await;
        run_s(
            &mut s,
            "INSERT INTO rp1 VALUES (1); INSERT INTO rc1 VALUES (1)",
        )
        .await;
        run_s(
            &mut s,
            "INSERT INTO rp2 VALUES (1); INSERT INTO rc2 VALUES (1)",
        )
        .await;
        assert!(sqlstate_of(&mut s, "DELETE FROM rp1 WHERE id = 1").await == "23503");
        assert!(sqlstate_of(&mut s, "DELETE FROM rp2 WHERE id = 1").await == "23001");
    }

    #[tokio::test]
    async fn truncate_refuses_a_child_outside_the_set_and_cascade_widens_it() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE tp (id int4 PRIMARY KEY)").await;
        run_s(
            &mut s,
            "CREATE TABLE tc (id int4 PRIMARY KEY, p int4 REFERENCES tp (id) ON DELETE CASCADE)",
        )
        .await;
        run_s(
            &mut s,
            "INSERT INTO tp VALUES (1); INSERT INTO tc VALUES (9, 1)",
        )
        .await;
        assert!(sqlstate_of(&mut s, "TRUNCATE tp").await == "0A000");
        // Naming both relations empties both, and the ON DELETE CASCADE never
        // fires: TRUNCATE widens the set, it does not run referential actions.
        run_s(&mut s, "TRUNCATE tp, tc").await;
        assert!(text_rows_of(&mut s, "SELECT id FROM tc").await == Vec::<Vec<_>>::new());
        run_s(
            &mut s,
            "INSERT INTO tp VALUES (1); INSERT INTO tc VALUES (9, 1)",
        )
        .await;
        run_s(&mut s, "TRUNCATE tp CASCADE").await;
        assert!(text_rows_of(&mut s, "SELECT id FROM tp").await == Vec::<Vec<_>>::new());
        assert!(text_rows_of(&mut s, "SELECT id FROM tc").await == Vec::<Vec<_>>::new());
    }

    #[tokio::test]
    async fn dropping_a_referenced_object_is_2bp01_and_cascade_drops_the_constraint() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE dp (id int4 PRIMARY KEY)").await;
        run_s(&mut s, "CREATE TABLE dc (a int4 REFERENCES dp (id))").await;
        assert!(sqlstate_of(&mut s, "DROP TABLE dp").await == "2BP01");
        assert!(sqlstate_of(&mut s, "ALTER TABLE dp DROP CONSTRAINT dp_pkey").await == "2BP01");
        // CASCADE drops the referencing CONSTRAINT, not the referencing table.
        run_s(&mut s, "DROP TABLE dp CASCADE").await;
        run_s(&mut s, "INSERT INTO dc VALUES (42)").await;
        assert!(
            text_rows_of(&mut s, "SELECT a FROM dc").await == vec![vec![Some("42".to_string())]]
        );
    }

    #[tokio::test]
    async fn a_mutually_referencing_pair_can_be_dropped_together() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE mp (id int4 PRIMARY KEY, other int4)").await;
        run_s(
            &mut s,
            "CREATE TABLE mc (id int4 PRIMARY KEY, a int4 REFERENCES mp (id))",
        )
        .await;
        run_s(
            &mut s,
            "ALTER TABLE mp ADD CONSTRAINT mp_other_fkey FOREIGN KEY (other) REFERENCES mc (id)",
        )
        .await;
        run_s(&mut s, "DROP TABLE mp, mc").await;
    }

    #[tokio::test]
    async fn adding_a_foreign_key_back_validates_stored_rows_unless_not_valid() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE bp (id int4 PRIMARY KEY)").await;
        run_s(&mut s, "CREATE TABLE bc (a int4)").await;
        run_s(&mut s, "INSERT INTO bc VALUES (7)").await;
        assert!(
            sqlstate_of(
                &mut s,
                "ALTER TABLE bc ADD CONSTRAINT bv FOREIGN KEY (a) REFERENCES bp (id)"
            )
            .await
                == "23503"
        );
        run_s(
            &mut s,
            "ALTER TABLE bc ADD CONSTRAINT bv FOREIGN KEY (a) REFERENCES bp (id) NOT VALID",
        )
        .await;
        // NOT VALID skips the scan but still governs every later write.
        assert!(sqlstate_of(&mut s, "INSERT INTO bc VALUES (8)").await == "23503");
        assert!(sqlstate_of(&mut s, "ALTER TABLE bc VALIDATE CONSTRAINT bv").await == "23503");
        run_s(&mut s, "INSERT INTO bp VALUES (7)").await;
        run_s(&mut s, "ALTER TABLE bc VALIDATE CONSTRAINT bv").await;
    }

    #[tokio::test]
    async fn a_foreign_key_added_beside_a_column_validates_the_rewritten_rows() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE ap (id int4 PRIMARY KEY)").await;
        run_s(&mut s, "INSERT INTO ap VALUES (5)").await;
        run_s(&mut s, "CREATE TABLE ac (x int4)").await;
        run_s(&mut s, "INSERT INTO ac VALUES (1)").await;
        // The added column fills the existing row with 5, which the constraint
        // must see — storage still holds the row without the column at all.
        run_s(
            &mut s,
            "ALTER TABLE ac ADD COLUMN a int4 DEFAULT 5, \
             ADD CONSTRAINT ac_fk FOREIGN KEY (a) REFERENCES ap (id)",
        )
        .await;
    }

    #[tokio::test]
    async fn renaming_a_referenced_column_rewrites_the_foreign_key() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE qp (id int4 PRIMARY KEY)").await;
        run_s(&mut s, "CREATE TABLE qc (a int4 REFERENCES qp (id))").await;
        run_s(&mut s, "ALTER TABLE qp RENAME COLUMN id TO ident").await;
        run_s(&mut s, "INSERT INTO qp VALUES (1)").await;
        run_s(&mut s, "INSERT INTO qc VALUES (1)").await;
        assert!(sqlstate_of(&mut s, "INSERT INTO qc VALUES (2)").await == "23503");
    }

    #[tokio::test]
    async fn a_foreign_key_can_be_renamed_and_dropped() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE zp (id int4 PRIMARY KEY)").await;
        run_s(&mut s, "CREATE TABLE zc (a int4 REFERENCES zp (id))").await;
        run_s(
            &mut s,
            "ALTER TABLE zc RENAME CONSTRAINT zc_a_fkey TO zc_renamed",
        )
        .await;
        assert!(sqlstate_of(&mut s, "INSERT INTO zc VALUES (1)").await == "23503");
        run_s(&mut s, "ALTER TABLE zc DROP CONSTRAINT zc_renamed").await;
        run_s(&mut s, "INSERT INTO zc VALUES (1)").await;
    }

    #[tokio::test]
    async fn a_foreign_key_on_a_partitioned_table_is_refused_by_name() {
        use assert2::assert;

        let engine = SqlEngine::new();
        let mut s = engine.connect();
        run_s(&mut s, "CREATE TABLE pp (id int4 PRIMARY KEY)").await;
        let error = s
            .simple_query(
                "CREATE TABLE part (id int4, a int4 REFERENCES pp (id)) PARTITION BY RANGE (id)",
            )
            .await
            .expect_err("partitioned foreign key");
        assert!(error.code == "0A000");
        assert!(
            error.message
                == "foreign key constraint \"part_a_fkey\" on a partitioned table is not supported"
        );
    }
}
