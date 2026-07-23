//! Per-statement execution.

use std::{
    collections::{BTreeSet, HashSet},
    fmt::Write as _,
    sync::Arc,
};

use bytes::Bytes;
use crabka_pgcatalog::{Column, ColumnDefault, Sequence, Table, TableId};
use crabka_pgkv::Kv;
use crabka_pgparser::ast::{Expr, FuncArgs, OrderItem, SelectItem, SelectStmt, Statement};
use crabka_pgtypes::{ColumnType, Datum};
use crabka_pgwire::engine::{Cell, FieldDescription, QueryResult};
use zerocopy::{FromBytes, byteorder::big_endian::U64};

use crate::{
    error::ExecError,
    foreign::{ForeignScanner, ScanBounds},
    join::{Relation, join_relations},
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

/// SP40: the foreign-table read context threaded through the SELECT pipeline. It
/// carries the registered scanner (the `kafka_fdw` seam) and the current user (for
/// resolving the per-user `UserMapping`). Bundled into one borrowed struct so the
/// already-wide read signatures gain a single argument rather than two. Paths that
/// never reach a registered scanner (`describe`, the schema-only build) use
/// `ForeignCtx::none()`; a foreign `SELECT` with no scanner registered returns
/// `0A000` ("foreign tables require the `kafka` feature").
#[derive(Clone, Copy)]
pub(crate) struct ForeignCtx<'a> {
    pub scanner: Option<&'a Arc<dyn ForeignScanner>>,
    pub current_user: &'a str,
}

impl ForeignCtx<'_> {
    /// A context with no scanner and the conventional `"public"` user — for paths
    /// that never reach a registered scanner (schema-only describe).
    pub(crate) fn none() -> Self {
        Self {
            scanner: None,
            current_user: "public",
        }
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
    /// dead versions of the rows it writes (the session computes it once per
    /// statement). Insert-only statements pass `None`: they re-read no
    /// chains, so there is nothing to prune. The prune deletes ride this
    /// statement's own commit batch, so they replicate and replay like any
    /// other write.
    pub prune_horizon: Option<u64>,
    /// Bound on every lock wait this statement performs (`None` waits
    /// indefinitely under the engine-local deadlock detector). Set for
    /// sessions that can be enlisted in a cross-range transaction, whose
    /// deadlock cycles span engines and are invisible to any one engine's
    /// wait-for graph.
    pub lock_wait_cap: Option<std::time::Duration>,
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
/// persisting it — the session routes the returned ops through the durable-write
/// seam (so DDL replicates too). The session holds the catalog lock across the
/// read+commit (serializing DDL globally). Non-DDL is unreachable here (routed
/// via `run_one`) but handled defensively to keep the match total. Validation
/// (42P07 on duplicate, 42P01 on a missing drop) is unchanged — only the write
/// destination moved.
pub(crate) fn execute_ddl(
    kv: &dyn Kv,
    stmt: &Statement,
    fctx: ForeignCtx,
) -> Result<(QueryResult, Vec<crabka_pgkv::WriteOp>), ExecError> {
    match stmt {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            sharded,
            sharding,
        } => {
            reject_unsupported_check_constraints(columns, constraints)?;
            let pending_indexes =
                create_table_unique_indexes(name, columns, constraints, *sharded)?;
            let primary_key_columns = create_table_primary_key_columns(columns, constraints);
            let ddl_ctx = crate::clock::EvalCtx::test_default();
            let mut serial_sequences = Vec::new();
            let cols = columns
                .iter()
                .map(|c| {
                    column_from_ast(
                        name,
                        c,
                        &ddl_ctx,
                        &mut serial_sequences,
                        &primary_key_columns,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let sharding = sharding.as_ref().map(hash_sharding_from_ast).transpose()?;
            let (id, mut ops) = crabka_pgcatalog::create_table_with_sharding_ops(
                kv,
                name,
                cols.clone(),
                crabka_pgcatalog::TableOptions { sharded: *sharded },
                sharding.as_ref(),
            )?;
            let table = crabka_pgcatalog::Table {
                id,
                name: name.clone(),
                columns: cols,
                sharded: *sharded,
                sharding,
                foreign: None,
            };
            ops.extend(crabka_pgcatalog::create_indexes_on_table_ops(
                kv,
                &table,
                &pending_indexes,
            )?);
            for (sequence_name, sequence) in serial_sequences {
                ops.extend(crabka_pgcatalog::create_sequence_ops(
                    kv,
                    &sequence_name,
                    sequence,
                )?);
            }
            Ok((
                QueryResult::Command {
                    tag: "CREATE TABLE".into(),
                },
                ops,
            ))
        }
        Statement::DropTable { names, if_exists } => {
            // All-or-nothing across the name list, matching PostgreSQL: ops are
            // only applied after every name resolves (or is skipped by
            // IF EXISTS), so a missing name without IF EXISTS drops nothing.
            let mut ops = Vec::new();
            let mut tag = "DROP TABLE";
            for name in names {
                if let Some(sequence_name) = name.strip_prefix("__crabka_sequence__:") {
                    tag = "DROP SEQUENCE";
                    match crabka_pgcatalog::drop_sequence_ops(kv, sequence_name) {
                        Ok(sequence_ops) => ops.extend(sequence_ops),
                        Err(crabka_pgcatalog::CatalogError::UndefinedSequence(_)) if *if_exists => {}
                        Err(error) => return Err(error.into()),
                    }
                } else {
                    match crabka_pgcatalog::drop_table_ops(kv, name) {
                        Ok(table_ops) => ops.extend(table_ops),
                        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) if *if_exists => {}
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            Ok((command(tag), ops))
        }
        Statement::AlterTableRename { table, rename } => match rename {
            crabka_pgparser::ast::AlterTableRename::Table { new_name } => Ok((
                command("ALTER TABLE"),
                crabka_pgcatalog::rename_table_ops(kv, table, new_name)?,
            )),
            crabka_pgparser::ast::AlterTableRename::Column { .. } => Err(
                ExecError::Unsupported(
                    "ALTER TABLE RENAME COLUMN is not supported because stored view and index dependencies cannot be rewritten safely".into(),
                ),
            ),
        },
        Statement::AlterTableAddPrimaryKey {
            table,
            constraint_name,
            columns,
        } => alter_table_add_primary_key_ops(kv, table, constraint_name.as_deref(), columns),
        Statement::CreateView {
            name,
            definition,
            query,
        } => {
            validate_view_definition(kv, query)?;
            let columns = crate::query::describe_query_expr(kv, query)?
                .into_iter()
                .map(|field| {
                    Ok(Column::new(
                        field.name,
                        column_type_from_oid(field.type_oid)?,
                    ))
                })
                .collect::<Result<Vec<_>, ExecError>>()?;
            let ops = crabka_pgcatalog::create_view_ops(kv, name, definition.clone(), columns)?;
            Ok((command("CREATE VIEW"), ops))
        }
        Statement::DropView { name, if_exists } => {
            let ops = match crabka_pgcatalog::drop_view_ops(kv, name) {
                Ok(ops) => ops,
                Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) if *if_exists => Vec::new(),
                Err(error) => return Err(error.into()),
            };
            Ok((command("DROP VIEW"), ops))
        }
        Statement::CreateIndex {
            name,
            table,
            columns,
            unique,
            placement,
        } => {
            if table == "__crabka_sequence__" {
                let sequence = sequence_from_encoded_options(columns)?;
                let ops = crabka_pgcatalog::create_sequence_ops(kv, name, sequence)?;
                return Ok((command("CREATE SEQUENCE"), ops));
            }
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
            let (id, mut ops) = crabka_pgcatalog::create_index_ops(
                kv,
                name,
                table,
                columns.clone(),
                *unique,
                placement,
            )?;
            if placement == crabka_pgcatalog::IndexPlacement::Local {
                let table_meta = crabka_pgcatalog::get_table(kv, table)?;
                reject_unwritable_local_index(&table_meta)?;
                let index = crabka_pgcatalog::Index {
                    id,
                    name: name.clone(),
                    table: table.clone(),
                    table_id: table_meta.id,
                    columns: columns.clone(),
                    unique: *unique,
                    placement,
                    constraint: None,
                };
                ops.extend(local_index_backfill_ops(kv, &table_meta, &index)?);
            }
            Ok((command("CREATE INDEX"), ops))
        }
        Statement::DropIndex { name, if_exists } => {
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
            for (key, _) in kv.scan_prefix(&crabka_pgkv::key::secondary_index_prefix(
                index.table_id,
                index.id,
            ))? {
                ops.push(crabka_pgkv::WriteOp::Delete { key });
            }
            Ok((command("DROP INDEX"), ops))
        }
        Statement::CreateRole { name, can_login } => {
            let ops = crabka_pgcatalog::create_role_ops(kv, name, *can_login)?;
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
            let ops =
                crabka_pgcatalog::grant_table_privileges_ops(kv, table, grantees, privileges)?;
            Ok((command("GRANT"), ops))
        }
        Statement::RevokeTablePrivileges {
            privileges,
            table,
            grantees,
        } => {
            let ops =
                crabka_pgcatalog::revoke_table_privileges_ops(kv, table, grantees, privileges)?;
            Ok((command("REVOKE"), ops))
        }
        Statement::CreateFdw { name, options } => {
            let ops = crabka_pgcatalog::create_fdw_ops(kv, name, options.clone())?;
            Ok((command("CREATE FOREIGN DATA WRAPPER"), ops))
        }
        Statement::DropFdw { name, if_exists } => {
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
        Statement::DropServer { name, if_exists } => {
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
        } => {
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
                name,
                cols,
                server,
                options.clone(),
            )?;
            Ok((command("CREATE FOREIGN TABLE"), ops))
        }
        Statement::DropForeignTable { name, if_exists } => {
            // A foreign table shares the ordinary table catalog key, so `drop_table`
            // removes it (catalog entry + sequence + any rows).
            let ops = match crabka_pgcatalog::drop_table_ops(kv, name) {
                Ok(ops) => ops,
                Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) if *if_exists => Vec::new(),
                Err(e) => return Err(e.into()),
            };
            Ok((command("DROP FOREIGN TABLE"), ops))
        }
        // The catalog has no ALTER for foreign objects, and phase-1 querying does
        // not need one — surface a clear 0A000 rather than silently no-op'ing.
        Statement::AlterServer { .. } | Statement::AlterUserMapping { .. } => Err(
            ExecError::CompatibilityRefusal(
                stmt.compatibility_refusal()
                    .expect("ALTER refusal metadata is centralized on the AST"),
            ),
        ),
        // SP40: IMPORT FOREIGN SCHEMA discovers the server's tables through the
        // registered scanner (the `kafka_fdw` seam enumerates Kafka topics and
        // derives each topic's value columns from its Schema Registry subject),
        // then materializes a foreign table per discovered table through the same
        // write-op seam as local DDL.
        //
        // `remote_schema` is accepted but unused in phase 1 (Kafka has no nested
        // schemas); `into_schema` is a flat namespace today — the discovered table
        // name is used verbatim as the catalog name.
        Statement::ImportForeignSchema {
            remote_schema: _,
            selector,
            server,
            into_schema: _,
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
            for table in tables {
                let (_id, mut table_ops) = crabka_pgcatalog::create_foreign_table_ops(
                    kv,
                    &table.name,
                    table.columns,
                    &srv.name,
                    table.options,
                )?;
                ops.append(&mut table_ops);
            }
            Ok((command("IMPORT FOREIGN SCHEMA"), ops))
        }
        _ => Err(ExecError::Unsupported("not a DDL statement".into())),
    }
}

fn validate_view_definition(
    catalog_kv: &dyn Kv,
    query: &crabka_pgparser::ast::QueryExpr,
) -> Result<(), ExecError> {
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
    for table in &select.from {
        let crabka_pgparser::ast::TableExpr::Table { name, .. } = table else {
            return Err(ExecError::Unsupported(
                "CREATE VIEW does not support joins or derived tables".into(),
            ));
        };
        if crabka_pgcatalog::get_view(catalog_kv, name).is_ok() {
            return Err(ExecError::Unsupported(
                "CREATE VIEW does not support references to other views".into(),
            ));
        }
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
    Ok(())
}

fn validate_view_expr(expr: &Expr) -> Result<(), ExecError> {
    match expr {
        Expr::Param(_) => Err(ExecError::Unsupported(
            "CREATE VIEW does not support query parameters".into(),
        )),
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } => {
            validate_view_expr(expr)
        }
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

fn reject_unsupported_check_constraints(
    columns: &[crabka_pgparser::ast::ColumnDef],
    constraints: &[crabka_pgparser::ast::TableConstraint],
) -> Result<(), ExecError> {
    let has_column_check = columns.iter().any(|column| {
        column.constraints.iter().any(|constraint| {
            matches!(constraint, crabka_pgparser::ast::ColumnConstraint::Check(_))
        })
    });
    let has_table_check = constraints
        .iter()
        .any(|constraint| matches!(constraint, crabka_pgparser::ast::TableConstraint::Check(_)));
    if has_column_check || has_table_check {
        return Err(ExecError::Unsupported(
            "CHECK constraints in CREATE TABLE are parsed but not enforced yet".into(),
        ));
    }
    Ok(())
}

fn create_table_unique_indexes(
    table_name: &str,
    columns: &[crabka_pgparser::ast::ColumnDef],
    constraints: &[crabka_pgparser::ast::TableConstraint],
    sharded: bool,
) -> Result<Vec<crabka_pgcatalog::NewIndex>, ExecError> {
    let mut indexes = Vec::new();
    for column in columns {
        for constraint in &column.constraints {
            match constraint {
                crabka_pgparser::ast::ColumnConstraint::PrimaryKey => {
                    indexes.push(create_table_constraint_index(
                        table_name,
                        std::slice::from_ref(&column.name),
                        true,
                    ));
                }
                crabka_pgparser::ast::ColumnConstraint::Unique => {
                    indexes.push(create_table_constraint_index(
                        table_name,
                        std::slice::from_ref(&column.name),
                        false,
                    ));
                }
                crabka_pgparser::ast::ColumnConstraint::NotNull
                | crabka_pgparser::ast::ColumnConstraint::Default(_)
                | crabka_pgparser::ast::ColumnConstraint::Check(_) => {}
            }
        }
    }
    for constraint in constraints {
        match constraint {
            crabka_pgparser::ast::TableConstraint::PrimaryKey(columns) => {
                indexes.push(create_table_constraint_index(table_name, columns, true));
            }
            crabka_pgparser::ast::TableConstraint::Unique(columns) => {
                indexes.push(create_table_constraint_index(table_name, columns, false));
            }
            crabka_pgparser::ast::TableConstraint::Check(_) => {}
        }
    }

    if sharded && !indexes.is_empty() {
        return Err(ExecError::Unsupported(
            "PRIMARY KEY and UNIQUE constraints on sharded tables are not supported until global enforcement exists".into(),
        ));
    }
    Ok(indexes)
}

fn create_table_constraint_index(
    table_name: &str,
    columns: &[String],
    primary_key: bool,
) -> crabka_pgcatalog::NewIndex {
    let suffix = if primary_key { "pkey" } else { "key" };
    let name = if primary_key {
        format!("{table_name}_pkey")
    } else {
        format!("{table_name}_{}_{suffix}", columns.join("_"))
    };
    crabka_pgcatalog::NewIndex {
        name,
        columns: columns.to_vec(),
        unique: true,
        placement: crabka_pgcatalog::IndexPlacement::Local,
        constraint: Some(if primary_key {
            crabka_pgcatalog::IndexConstraint::PrimaryKey
        } else {
            crabka_pgcatalog::IndexConstraint::Unique
        }),
    }
}

/// Build the `ALTER TABLE … ADD PRIMARY KEY` batch: validate existing rows
/// (NOT NULL first, then uniqueness through the shared unique-index backfill,
/// matching PostgreSQL's error order), mark the key columns NOT NULL in the
/// schema record, and create the constraint-backed `<table>_pkey` local unique
/// index — all-or-nothing, nothing commits on any error.
fn alter_table_add_primary_key_ops(
    kv: &dyn Kv,
    table_name: &str,
    constraint_name: Option<&str>,
    columns: &[String],
) -> Result<(QueryResult, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let table = match crabka_pgcatalog::get_table(kv, table_name) {
        Ok(table) => table,
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_))
            if crabka_pgcatalog::get_view(kv, table_name).is_ok() =>
        {
            return Err(ExecError::WrongObjectType(format!(
                "\"{table_name}\" is not a table"
            )));
        }
        Err(error) => return Err(error.into()),
    };
    if table.foreign.is_some() {
        return Err(ExecError::WrongObjectType(format!(
            "\"{table_name}\" is not a table"
        )));
    }
    if table.sharded {
        return Err(ExecError::Unsupported(
            "PRIMARY KEY and UNIQUE constraints on sharded tables are not supported until global enforcement exists"
                .into(),
        ));
    }
    let indexes = crabka_pgcatalog::list_table_indexes(kv, table_name)?;
    if indexes
        .iter()
        .any(|index| index.constraint == Some(crabka_pgcatalog::IndexConstraint::PrimaryKey))
    {
        return Err(ExecError::InvalidTableDefinition(format!(
            "multiple primary keys for table \"{table_name}\" are not allowed"
        )));
    }
    // Resolve the key columns first so a bad name is 42703 before any data scan.
    let key_column_indices = columns
        .iter()
        .map(|column| {
            table
                .column_index(column)
                .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    // PostgreSQL sets the key columns NOT NULL before building the index, so an
    // existing NULL is 23502 ahead of any duplicate-key 23505. One live-row scan
    // feeds both the NULL validation and the unique-index backfill.
    let all_committed = all_committed_snapshot();
    let rows = scan_live(kv, kv, &all_committed, &all_committed, None, &table)?;
    for (_rowid, _xmin, row) in &rows {
        for (column, column_index) in columns.iter().zip(&key_column_indices) {
            if row[*column_index].is_null() {
                return Err(ExecError::ColumnContainsNullValues {
                    column: column.clone(),
                    table: table_name.to_string(),
                });
            }
        }
    }
    let mut ops = crabka_pgcatalog::set_columns_not_null_ops(kv, table_name, columns)?;
    let new_index = crabka_pgcatalog::NewIndex {
        name: constraint_name.map_or_else(|| format!("{table_name}_pkey"), str::to_string),
        columns: columns.to_vec(),
        unique: true,
        placement: crabka_pgcatalog::IndexPlacement::Local,
        constraint: Some(crabka_pgcatalog::IndexConstraint::PrimaryKey),
    };
    let (index, index_ops) = crabka_pgcatalog::create_constraint_index_ops(kv, &table, &new_index)?;
    ops.extend(index_ops);
    ops.extend(local_index_backfill_ops_for_rows(&rows, &table, &index)?);
    Ok((command("ALTER TABLE"), ops))
}

fn create_table_primary_key_columns<'a>(
    columns: &'a [crabka_pgparser::ast::ColumnDef],
    constraints: &'a [crabka_pgparser::ast::TableConstraint],
) -> HashSet<&'a str> {
    let mut primary_key_columns = HashSet::new();
    for column in columns {
        if column.constraints.iter().any(|constraint| {
            matches!(
                constraint,
                crabka_pgparser::ast::ColumnConstraint::PrimaryKey
            )
        }) {
            primary_key_columns.insert(column.name.as_str());
        }
    }
    for constraint in constraints {
        if let crabka_pgparser::ast::TableConstraint::PrimaryKey(columns) = constraint {
            primary_key_columns.extend(columns.iter().map(String::as_str));
        }
    }
    primary_key_columns
}

fn column_from_ast(
    table_name: &str,
    column: &crabka_pgparser::ast::ColumnDef,
    ctx: &crate::clock::EvalCtx,
    serial_sequences: &mut Vec<(String, Sequence)>,
    primary_key_columns: &HashSet<&str>,
) -> Result<Column, ExecError> {
    let mut catalog_column = Column::new(column.name.clone(), column.ty);
    if primary_key_columns.contains(column.name.as_str()) {
        catalog_column.not_null = true;
    }
    if column.serial.is_some() {
        let sequence_name = format!("{table_name}_{}_seq", column.name);
        catalog_column.not_null = true;
        catalog_column.default = Some(ColumnDefault::NextVal(sequence_name.clone()));
        serial_sequences.push((
            sequence_name,
            Sequence::new(1, 1, None, None, Some(1), false),
        ));
    }
    for constraint in &column.constraints {
        match constraint {
            crabka_pgparser::ast::ColumnConstraint::NotNull => catalog_column.not_null = true,
            crabka_pgparser::ast::ColumnConstraint::Default(expr) => {
                let value = crate::eval::eval(expr, &Scope::empty(), &[], ctx)?;
                let value = coerce(value, column.ty, ctx)?;
                ensure_default_can_be_persisted(&value)?;
                catalog_column.default = Some(ColumnDefault::Value(value));
            }
            crabka_pgparser::ast::ColumnConstraint::PrimaryKey
            | crabka_pgparser::ast::ColumnConstraint::Unique => {}
            crabka_pgparser::ast::ColumnConstraint::Check(_) => {
                unreachable!(
                    "reject_unsupported_check_constraints rejects these before column conversion"
                )
            }
        }
    }
    Ok(catalog_column)
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
    ) {
        return Ok(());
    }
    Err(ExecError::Unsupported(
        "defaults for date/time, interval, and bytea columns are not persisted yet".into(),
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

fn build_insert_row(
    table: &Table,
    target_idx: &[usize],
    row_exprs: &[Expr],
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Datum>, ExecError> {
    let mut row = table
        .columns
        .iter()
        .map(|column| default_value(column, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    for (slot, expr) in target_idx.iter().zip(row_exprs.iter()) {
        let value = match expr {
            Expr::Default => default_value(&table.columns[*slot], ctx)?,
            Expr::StringLiteral(value) => {
                let target = table.columns[*slot].ty;
                if target == crabka_pgtypes::ColumnType::Bytea {
                    Datum::Bytea(crate::session::decode_bytea_text(value)?)
                } else {
                    crabka_pgtypes::cast::cast(&Datum::Text(value.clone()), target, &ctx.time_zone)?
                }
            }
            _ => {
                let value = crate::eval::eval(expr, &Scope::empty(), &[], ctx)?;
                coerce(value, table.columns[*slot].ty, ctx)?
            }
        };
        row[*slot] = value;
    }
    enforce_not_null(table, &row)?;
    Ok(row)
}

fn build_copy_row(
    table: &Table,
    target_idx: &[usize],
    row_values: &[Option<String>],
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Datum>, ExecError> {
    let mut row = table
        .columns
        .iter()
        .map(|column| default_value(column, ctx))
        .collect::<Result<Vec<_>, _>>()?;
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
    enforce_not_null(table, &row)?;
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

fn enforce_not_null(table: &Table, row: &[Datum]) -> Result<(), ExecError> {
    for (column, value) in table.columns.iter().zip(row.iter()) {
        if column.not_null && value.is_null() {
            return Err(ExecError::NotNullViolation(column.name.clone()));
        }
    }
    Ok(())
}

fn default_value(column: &Column, ctx: &crate::clock::EvalCtx) -> Result<Datum, ExecError> {
    let Some(default) = &column.default else {
        return Ok(Datum::Null);
    };
    match default {
        ColumnDefault::Value(value) => Ok(value.clone()),
        ColumnDefault::NextVal(sequence) => {
            let runtime = ctx.sequence.as_ref().ok_or_else(|| {
                ExecError::Unsupported("sequence defaults require a SQL session".into())
            })?;
            let value = runtime.manager.nextval(&*runtime.kv, sequence)?;
            runtime
                .currvals
                .lock()
                .expect("sequence currvals")
                .insert(sequence.clone(), value);
            coerce(Datum::Int8(value), column.ty, ctx)
        }
    }
}

/// The write path (INSERT/UPDATE/DELETE) with concurrent writers (SP6). Builds
/// the version write ops tagged with the transaction's `xid` and returns them
/// WITHOUT writing — the session assembles the final batch (clog for autocommit)
/// and writes once. INSERT allocates rowids via the `SequenceManager` (which
/// persists the sequence durably itself). UPDATE/DELETE lock each candidate row
/// exclusively via the `RowLockManager` (blocking until granted, or 40P01 on a
/// deadlock), then re-check the row's current state under EvalPlanQual: a
/// concurrent committed change is a 40001 under REPEATABLE READ, or a re-find
/// under READ COMMITTED. Reads resolve via `satisfies_mvcc` with the txn's own
/// xid (read-your-writes).
pub(crate) async fn execute_write(
    write_ctx: &WriteContext<'_>,
    stmt: &Statement,
) -> Result<(QueryResult, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let catalog_kv = write_ctx.catalog_kv;
    let kv = write_ctx.kv;
    let lockmgr = write_ctx.lockmgr;
    let seq = write_ctx.seq;
    let xid = write_ctx.xid;
    let ctx = write_ctx.eval_ctx;
    let mut ops: Vec<crabka_pgkv::WriteOp> = Vec::new();
    match stmt {
        Statement::Insert {
            table,
            columns,
            rows,
            returning,
        } => {
            if rows.is_empty() {
                return Ok((
                    QueryResult::Command {
                        tag: "INSERT 0 0".into(),
                    },
                    ops,
                ));
            }
            let t = crabka_pgcatalog::get_table(catalog_kv, table)?;
            let local_indexes = writable_local_indexes(catalog_kv, &t)?;
            let mut pending_unique_keys = HashSet::new();
            let target_idx = resolve_targets(&t, columns)?;
            // Reserve a contiguous block of rowids atomically. In Durable mode the
            // SequenceManager persists the new next-rowid itself (seq_op is None).
            // In Replicated mode it returns the seq Put for us to fold into this
            // same commit batch (max-merged by the replicated state machine).
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
                if row_exprs.len() != target_idx.len() {
                    return Err(ExecError::TypeMismatch(
                        "INSERT has the wrong number of expressions for the target columns".into(),
                    ));
                }
                let full = build_insert_row(&t, &target_idx, row_exprs, ctx)?;
                enforce_unique_local_indexes(
                    write_ctx,
                    &t,
                    &local_indexes,
                    rowid,
                    &full,
                    &mut pending_unique_keys,
                )
                .await?;
                if returning.is_some() {
                    returned_rows.push(full.clone());
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
            }
            let tag = format!("INSERT 0 {n_rows}");
            let result = returning_result(&t, returning.as_deref(), returned_rows, tag, ctx)?;
            Ok((result, ops))
        }
        Statement::Update {
            table,
            assignments,
            filter,
            returning,
        } => {
            let t = crabka_pgcatalog::get_table(catalog_kv, table)?;
            let local_indexes = writable_local_indexes(catalog_kv, &t)?;
            let mut pending_unique_keys = HashSet::new();
            let scope = Scope::single(&t, &t.name);
            // Resolve each assignment's target column index up front (42703 on miss).
            let targets: Vec<(usize, &Expr)> = assignments
                .iter()
                .map(|(col, expr)| {
                    t.column_index(col)
                        .map(|idx| (idx, expr))
                        .ok_or_else(|| ExecError::UndefinedColumn(col.clone()))
                })
                .collect::<Result<_, _>>()?;
            let mut n: u64 = 0;
            let mut returned_rows = returning.as_ref().map(|_| Vec::new()).unwrap_or_default();
            for (rowid, _xmin, scanned_row) in write_candidate_rows(write_ctx, &t, filter.as_ref())?
            {
                // 1. Filter on the snapshot-visible row FIRST — do not lock rows
                //    that don't match the WHERE clause (avoids over-locking and
                //    restores row-level write concurrency for different rows).
                if !row_matches(filter.as_ref(), &scope, &scanned_row, ctx)? {
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
                if !row_matches(filter.as_ref(), &scope, &cur_row, ctx)? {
                    continue; // no longer matches the WHERE clause
                }
                let mut next = cur_row.clone();
                for (idx, expr) in &targets {
                    let v = crate::eval::eval(expr, &scope, &cur_row, ctx)?;
                    next[*idx] = coerce(v, t.columns[*idx].ty, ctx)?;
                }
                enforce_not_null(&t, &next)?;
                enforce_unique_local_index_updates(
                    write_ctx,
                    &t,
                    &local_indexes,
                    rowid,
                    &cur_row,
                    &next,
                    &mut pending_unique_keys,
                )
                .await?;
                if returning.is_some() {
                    returned_rows.push(next.clone());
                }
                if cur_xmin == xid {
                    // Updating my own uncommitted version: overwrite in place
                    // (last-write-wins within the txn; no new tuple, xmax stays
                    // invalid). PostgreSQL uses cmin/cmax here; we have no command
                    // ids, so in-place replacement is the faithful observable result.
                    ops.push(crabka_pgkv::WriteOp::Put {
                        key: crabka_pgmvcc::version::version_key_xid(t.id, rowid, xid),
                        value: crabka_pgmvcc::version::encode_tuple(
                            xid,
                            crabka_pgmvcc::xid::INVALID_XID,
                            &next,
                        ),
                    });
                } else {
                    // Supersede a committed version: stamp its xmax, write a new
                    // tuple. The stamp targets the version's PHYSICAL key
                    // (`cur_key_xid`) — for a frozen tuple the header xmin
                    // (`FROZEN_XID`) no longer names its key.
                    ops.push(crabka_pgkv::WriteOp::Put {
                        key: crabka_pgmvcc::version::version_key_xid(t.id, rowid, cur_key_xid),
                        value: crabka_pgmvcc::version::encode_tuple(cur_xmin, xid, &cur_row),
                    });
                    ops.push(crabka_pgkv::WriteOp::Put {
                        key: crabka_pgmvcc::version::version_key_xid(t.id, rowid, xid),
                        value: crabka_pgmvcc::version::encode_tuple(
                            xid,
                            crabka_pgmvcc::xid::INVALID_XID,
                            &next,
                        ),
                    });
                }
                ops.extend(local_index_entry_ops(&t, &local_indexes, rowid, &next)?);
                // Opportunistic per-rowid chain pruning (local engines only):
                // we hold this row's exclusive lock and just re-read its chain,
                // so reclaim its dead versions in the same commit batch. The
                // versions this statement writes (`cur_xmin`, `xid`) are never
                // pruned, and `next`'s indexed values count as survivors.
                if let Some(horizon) = write_ctx.prune_horizon {
                    ops.extend(
                        prune_rowid_chain_ops(
                            kv,
                            &t,
                            &local_indexes,
                            &ChainPruneRequest {
                                rowid,
                                horizon,
                                keep_xids: &[cur_key_xid, xid],
                                new_row: Some(&next),
                                freeze_below: None,
                            },
                        )?
                        .ops,
                    );
                }
                n += 1;
            }
            let tag = format!("UPDATE {n}");
            let result = returning_result(&t, returning.as_deref(), returned_rows, tag, ctx)?;
            Ok((result, ops))
        }
        Statement::Delete {
            table,
            filter,
            returning,
        } => {
            let t = crabka_pgcatalog::get_table(catalog_kv, table)?;
            let local_indexes = writable_local_indexes(catalog_kv, &t)?;
            let scope = Scope::single(&t, &t.name);
            let mut n: u64 = 0;
            let mut returned_rows = returning.as_ref().map(|_| Vec::new()).unwrap_or_default();
            for (rowid, _xmin, scanned_row) in write_candidate_rows(write_ctx, &t, filter.as_ref())?
            {
                // 1. Filter on the snapshot-visible row FIRST — do not lock rows
                //    that don't match the WHERE clause.
                if !row_matches(filter.as_ref(), &scope, &scanned_row, ctx)? {
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
                if !row_matches(filter.as_ref(), &scope, &cur_row, ctx)? {
                    continue; // no longer matches the WHERE clause
                }
                if returning.is_some() {
                    returned_rows.push(cur_row.clone());
                }
                if cur_xmin == xid {
                    // Deleting my own uncommitted version: PostgreSQL stamps
                    // xmax=xid so it is invisible to me. version_key is the same
                    // key; overwrite it with xmax set.
                    ops.push(crabka_pgkv::WriteOp::Put {
                        key: crabka_pgmvcc::version::version_key_xid(t.id, rowid, xid),
                        value: crabka_pgmvcc::version::encode_tuple(xid, xid, &cur_row),
                    });
                } else {
                    // Set xmax = my xid on the matched version (keep its row
                    // bytes), targeting its PHYSICAL key — see the UPDATE arm.
                    ops.push(crabka_pgkv::WriteOp::Put {
                        key: crabka_pgmvcc::version::version_key_xid(t.id, rowid, cur_key_xid),
                        value: crabka_pgmvcc::version::encode_tuple(cur_xmin, xid, &cur_row),
                    });
                }
                // Opportunistic per-rowid chain pruning (local engines only) —
                // see the UPDATE arm. The tombstoned current version survives
                // (its xmax is our in-progress xid), so its index entries stay;
                // an engine-level `vacuum` reclaims the chain once the delete
                // commits below a future horizon.
                if let Some(horizon) = write_ctx.prune_horizon {
                    ops.extend(
                        prune_rowid_chain_ops(
                            kv,
                            &t,
                            &local_indexes,
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
                n += 1;
            }
            let tag = format!("DELETE {n}");
            let result = returning_result(&t, returning.as_deref(), returned_rows, tag, ctx)?;
            Ok((result, ops))
        }
        Statement::Truncate {
            names,
            restart_identity,
        } => {
            if *restart_identity {
                return Err(ExecError::Unsupported(
                    "TRUNCATE RESTART IDENTITY is not supported: SERIAL sequence ownership is not tracked".into(),
                ));
            }
            // Validate every name (and refuse sharded targets) before touching
            // any table: the statement is all-or-nothing across the list.
            for name in names {
                let t = crabka_pgcatalog::get_table(catalog_kv, name)?;
                if table_uses_global_visibility(&t) {
                    return Err(ExecError::Unsupported(
                        "TRUNCATE on sharded tables is not supported".into(),
                    ));
                }
            }
            // Desugar to an unfiltered DELETE per table: TRUNCATE shares the
            // MVCC write path (row locks, xmax stamping, rollback) rather
            // than clearing storage, so it is transactional like PostgreSQL's.
            for name in names {
                let delete = Statement::Delete {
                    table: name.clone(),
                    filter: None,
                    returning: None,
                };
                let (_, delete_ops) = Box::pin(execute_write(write_ctx, &delete)).await?;
                ops.extend(delete_ops);
            }
            Ok((command("TRUNCATE TABLE"), ops))
        }
        _ => Err(ExecError::Unsupported("not a write statement".into())),
    }
}

fn returning_result(
    table: &Table,
    returning: Option<&[SelectItem]>,
    rows: Vec<Vec<Datum>>,
    tag: String,
    ctx: &crate::clock::EvalCtx,
) -> Result<QueryResult, ExecError> {
    let Some(returning) = returning else {
        return Ok(QueryResult::Command { tag });
    };

    let scope = Scope::single(table, &table.name);
    let (fields, out_exprs, _tys) = resolve_projection(returning, &scope)?;
    let projected = project_rows(&out_exprs, &scope, &rows, ctx)?;
    Ok(rows_result_with_tag(
        fields,
        &projected,
        &ctx.time_zone,
        tag,
    ))
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
    let mut ops = Vec::new();
    let table = crabka_pgcatalog::get_table(catalog_kv, &copy.table)?;
    let local_indexes = writable_local_indexes(catalog_kv, &table)?;
    let mut pending_unique_keys = HashSet::new();
    let target_idx = resolve_targets(&table, &copy.columns)?;
    let n_rows = rows.len() as u64;
    if n_rows == 0 {
        return Ok((command("COPY 0"), ops));
    }
    let (start, seq_op) = seq.alloc(kv, table.id, n_rows)?;
    if let Some(op) = seq_op {
        ops.push(op);
    }
    for (rowid, row_values) in (start..).zip(rows.iter()) {
        if row_values.len() != target_idx.len() {
            return Err(ExecError::TypeMismatch(
                "COPY row has the wrong number of fields for the target columns".into(),
            ));
        }
        let full = build_copy_row(&table, &target_idx, row_values, ctx)?;
        enforce_unique_local_indexes(
            write_ctx,
            &table,
            &local_indexes,
            rowid,
            &full,
            &mut pending_unique_keys,
        )
        .await?;
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(table.id, rowid, snapshot_xid),
            value: crabka_pgmvcc::version::encode_tuple(
                snapshot_xid,
                crabka_pgmvcc::xid::INVALID_XID,
                &full,
            ),
        });
        ops.extend(local_index_entry_ops(&table, &local_indexes, rowid, &full)?);
    }
    Ok((command(&format!("COPY {n_rows}")), ops))
}

pub(crate) fn execute_timestamp_copy_write(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    seq: &crate::seq::SequenceManager,
    copy: &crabka_pgparser::ast::CopyStmt,
    rows: &[Vec<Option<String>>],
    ctx: &crate::clock::EvalCtx,
) -> Result<TimestampWritePlan, ExecError> {
    let table = crabka_pgcatalog::get_table(catalog_kv, &copy.table)?;
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
/// for its duration (until COMMIT/ROLLBACK in an explicit transaction). Shared
/// mode never blocks other DML — it only lets unique-index DDL (CREATE UNIQUE
/// INDEX backfill, CREATE TABLE with a unique constraint), which takes the
/// same lock EXCLUSIVELY, wait out in-flight writers and block new ones while
/// it scans. Same-key DML conflicts serialize through per-key locks in the
/// `RowLockManager` instead (see `enforce_unique_local_index`).
pub(crate) enum UniqueLocalSerialization {
    None,
    Shared,
}

pub(crate) fn write_requires_unique_local_serialization(
    catalog_kv: &dyn Kv,
    stmt: &Statement,
) -> Result<UniqueLocalSerialization, ExecError> {
    let table_name = match stmt {
        Statement::Insert { table, .. }
        | Statement::Update { table, .. }
        | Statement::Delete { table, .. } => table,
        _ => return Ok(UniqueLocalSerialization::None),
    };
    table_requires_unique_local_serialization(catalog_kv, table_name)
}

pub(crate) fn copy_requires_unique_local_serialization(
    catalog_kv: &dyn Kv,
    copy: &crabka_pgparser::ast::CopyStmt,
) -> Result<UniqueLocalSerialization, ExecError> {
    table_requires_unique_local_serialization(catalog_kv, &copy.table)
}

pub(crate) fn ddl_requires_unique_local_serialization(stmt: &Statement) -> bool {
    match stmt {
        Statement::CreateIndex {
            unique: true,
            placement: crabka_pgparser::ast::IndexPlacement::Local,
            ..
        }
        // ADD PRIMARY KEY back-validates and backfills a local unique index,
        // so it must wait out in-flight writers exactly like CREATE UNIQUE
        // INDEX does.
        | Statement::AlterTableAddPrimaryKey { .. } => true,
        Statement::CreateTable {
            columns,
            constraints,
            ..
        } => create_table_has_unique_constraint(columns, constraints),
        _ => false,
    }
}

fn create_table_has_unique_constraint(
    columns: &[crabka_pgparser::ast::ColumnDef],
    constraints: &[crabka_pgparser::ast::TableConstraint],
) -> bool {
    columns.iter().any(|column| {
        column.constraints.iter().any(|constraint| {
            matches!(
                constraint,
                crabka_pgparser::ast::ColumnConstraint::PrimaryKey
                    | crabka_pgparser::ast::ColumnConstraint::Unique
            )
        })
    }) || constraints.iter().any(|constraint| {
        matches!(
            constraint,
            crabka_pgparser::ast::TableConstraint::PrimaryKey(_)
                | crabka_pgparser::ast::TableConstraint::Unique(_)
        )
    })
}

fn table_requires_unique_local_serialization(
    catalog_kv: &dyn Kv,
    table_name: &str,
) -> Result<UniqueLocalSerialization, ExecError> {
    let table = crabka_pgcatalog::get_table(catalog_kv, table_name)?;
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
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let all_committed = all_committed_snapshot();
    let rows = scan_live(kv, kv, &all_committed, &all_committed, None, table)?;
    local_index_backfill_ops_for_rows(&rows, table, index)
}

/// Backfill index entries for already-scanned live rows. A UNIQUE index
/// back-validates the existing data: a duplicate non-NULL key is a 23505
/// before any op is committed (rows with a NULL key column are not indexed,
/// matching SQL NULL-distinct semantics).
fn local_index_backfill_ops_for_rows(
    rows: &[(u64, u64, Vec<Datum>)],
    table: &Table,
    index: &crabka_pgcatalog::Index,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let mut seen = HashSet::new();
    let mut ops = Vec::with_capacity(rows.len());
    for (rowid, _xmin, row) in rows {
        let values = indexed_values(table, index, row)?;
        if values.iter().any(Datum::is_null) {
            continue;
        }
        if index.unique && !seen.insert(values.clone()) {
            return Err(ExecError::UniqueViolation(index.name.clone()));
        }
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgkv::key::secondary_index_entry_key(table.id, index.id, &values, *rowid),
            value: Vec::new(),
        });
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
    pending_unique_keys: &mut HashSet<PendingUniqueKey>,
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
        enforce_unique_local_index(
            write_ctx,
            table,
            index,
            rowid,
            new_values,
            pending_unique_keys,
        )
        .await?;
    }
    Ok(())
}

async fn enforce_unique_local_indexes(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    indexes: &[crabka_pgcatalog::Index],
    rowid: u64,
    row: &[Datum],
    pending_unique_keys: &mut HashSet<PendingUniqueKey>,
) -> Result<(), ExecError> {
    for index in indexes.iter().filter(|index| index.unique) {
        let values = indexed_values(table, index, row)?;
        enforce_unique_local_index(write_ctx, table, index, rowid, values, pending_unique_keys)
            .await?;
    }
    Ok(())
}

async fn enforce_unique_local_index(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    rowid: u64,
    values: Vec<Datum>,
    pending_unique_keys: &mut HashSet<PendingUniqueKey>,
) -> Result<(), ExecError> {
    if values.iter().any(Datum::is_null) {
        // SQL unique ignores NULLs: nothing to enforce, so no key lock either.
        return Ok(());
    }
    let pending_key = (index.id, values.clone());
    if !pending_unique_keys.insert(pending_key) {
        return Err(ExecError::UniqueViolation(index.name.clone()));
    }
    // Serialize check-then-write PER KEY: take this key's exclusive lock (in
    // the row-lock manager, so it shares the deadlock wait-for graph and is
    // released with the row locks at COMMIT/ROLLBACK) before probing. Without
    // it, two concurrent writers of the same key would both pass the probe —
    // neither sees the other's uncommitted version — and both commit. A waiter
    // that wakes here after the holder's terminal outcome re-probes against
    // the then-current committed state: a violation if the holder committed,
    // success if it rolled back.
    write_ctx
        .lockmgr
        .acquire_key(
            crate::lockmgr::LockKey::UniqueKey(crabka_pgkv::key::secondary_index_entry_prefix(
                table.id, index.id, &values,
            )),
            crate::lockmgr::LockMode::Exclusive,
            write_ctx.xid,
            write_ctx.lock_wait_cap,
        )
        .await
        .map_err(lock_acquire_error)?;
    // Probe the index for exactly this key instead of scanning the whole table.
    // The probe keeps the scan path's visibility (all-committed local + global
    // snapshots plus our own xid), and `lookup_local_index_equal` resolves each
    // candidate rowid through MVCC and re-checks its visible row's values, so
    // dead entries left by old versions or aborted writers never count. A
    // visible holder other than the row being written is a violation.
    let mvcc = write_ctx.mvcc_read();
    let current_visibility = all_committed_snapshot();
    let probe = MvccReadContext {
        kv: mvcc.kv,
        global: mvcc.global,
        global_snapshot: &current_visibility,
        snapshot: &current_visibility,
        own: mvcc.own,
    };
    let holders = lookup_local_index_equal(&probe, table, index, &values)?;
    if holders.iter().any(|holder| holder.rowid != rowid) {
        return Err(ExecError::UniqueViolation(index.name.clone()));
    }
    Ok(())
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
    indexes
        .iter()
        .map(|index| {
            Ok(crabka_pgkv::WriteOp::Put {
                key: crabka_pgkv::key::secondary_index_entry_key(
                    table.id,
                    index.id,
                    &indexed_values(table, index, row)?,
                    rowid,
                ),
                value: Vec::new(),
            })
        })
        .collect()
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
/// live snapshot still sees — one whose committed deleter is in that
/// snapshot's `xip` or above its `xmax` — keeps `horizon` at or below the
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
/// collide with a concurrent writer's puts — a writer only writes the newest
/// committed version's key (stamping `xmax`) and its own new key, and neither
/// is ever dead — but the survivor computation for shared index entries must
/// not race a writer re-adding the same indexed values for this rowid.
///
/// Engine kinds: sound everywhere, because the returned ops are folded into
/// the caller's own commit batch — on replicated engines they replicate
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
    /// Version-key xids this batch itself (re)writes — never deleted or
    /// frozen, whatever their current on-disk state.
    pub keep_xids: &'a [u64],
    /// The row this batch is writing, when any; its indexed values count as
    /// survivors so a shared index entry is never deleted out from under the
    /// incoming version.
    pub new_row: Option<&'a [Datum]>,
    /// When given (`vacuum` only), additionally rewrite every surviving
    /// version whose creator committed below this floor to `FROZEN_XID`
    /// (visible to every snapshot without a clog lookup) — the precondition
    /// for truncating the clog below the horizon. Freezing is invisible to
    /// every snapshot: a registered snapshot's `xmin` is at or above the
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

/// Emit at most one `xid_chain_prune_engaged` debug line per second, carrying
/// the current horizon and the chain/deletion counts accumulated since the
/// previous line. A live node logging a low `horizon` with growing `rows` and
/// zero `pruned` shows the write path consults the horizon but finds nothing
/// dead; non-zero `pruned` confirms end-to-end reclamation.
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
    let mut index_entries: u64 = 0;
    // An index entry key `(values, rowid)` is SHARED by every version of this
    // row carrying `values`: delete it only when no surviving version — nor
    // the row this batch is writing — still carries those values. Chains are
    // short (pruning keeps them O(1)), so linear survivor probes suffice.
    let mut removed: Vec<(crabka_pgcatalog::IndexId, Vec<Datum>)> = Vec::new();
    for index in local_indexes {
        let mut survivors: Vec<Vec<Datum>> = Vec::with_capacity(surviving.len() + 1);
        for row in &surviving {
            survivors.push(indexed_values(table, index, row)?);
        }
        if let Some(row) = new_row {
            survivors.push(indexed_values(table, index, row)?);
        }
        for (_, row) in &dead {
            let values = indexed_values(table, index, row)?;
            if survivors.contains(&values)
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
            index_entries += 1;
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
        index_entries,
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
        if indexed_values(table, index, &row)? == values {
            rows.push(ScannedRow { rowid, xmin, row });
        }
    }
    Ok(rows)
}

/// Choose the index probe for an UPDATE/DELETE filter: a top-level
/// `column = literal` conjunct matching a single-column local index. Returns
/// `None` — full scan — for sharded tables, for filters outside the pushdown
/// subset (reusing the SELECT path's extraction, so only exact-type literals on
/// supported column types qualify), and when no index matches.
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
        Statement::Insert { columns, rows, .. } => {
            execute_timestamp_insert(kv, seq, &table, &global_indexes, columns, rows, ctx)
        }
        Statement::Update {
            assignments,
            filter,
            ..
        } => execute_timestamp_update(
            kv,
            &table,
            &global_indexes,
            assignments,
            filter.as_ref(),
            ctx,
        ),
        Statement::Delete { filter, .. } => {
            execute_timestamp_delete(kv, &table, &global_indexes, filter.as_ref(), ctx)
        }
        _ => unreachable!("matched above"),
    }
}

fn execute_timestamp_insert(
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
    let target_idx = resolve_targets(table, columns)?;
    let n_rows = rows.len() as u64;
    let (start, seq_op) = seq.alloc(kv, table.id, n_rows)?;
    let mut writes = Vec::with_capacity(rows.len());
    for (rowid, row_exprs) in (start..).zip(rows.iter()) {
        if row_exprs.len() != target_idx.len() {
            return Err(ExecError::TypeMismatch(
                "INSERT has the wrong number of expressions for the target columns".into(),
            ));
        }
        let full = build_insert_row(table, &target_idx, row_exprs, ctx)?;
        let bucket = hash_bucket_for_row(table, &full)?;
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
    Ok(TimestampWritePlan {
        result: command(&format!("INSERT 0 {n_rows}")),
        writes,
        commit_ops: seq_op.into_iter().collect(),
    })
}

fn execute_timestamp_update(
    kv: &dyn Kv,
    table: &Table,
    global_indexes: &[&crabka_pgcatalog::Index],
    assignments: &[(String, Expr)],
    filter: Option<&Expr>,
    ctx: &crate::clock::EvalCtx,
) -> Result<TimestampWritePlan, ExecError> {
    let scope = Scope::single(table, &table.name);
    let targets = assignments
        .iter()
        .map(|(column, expr)| {
            table
                .column_index(column)
                .map(|index| (index, expr))
                .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))
        })
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
        enforce_not_null(table, &next)?;
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
    kv: &dyn Kv,
    table: &Table,
    global_indexes: &[&crabka_pgcatalog::Index],
    filter: Option<&Expr>,
    ctx: &crate::clock::EvalCtx,
) -> Result<TimestampWritePlan, ExecError> {
    let scope = Scope::single(table, &table.name);
    let rows = scan_ts_live_interval(kv, kv, table, ReadTimestamp::MAX, None, RowInterval::ALL)?;
    let mut writes = Vec::new();
    for ScannedRow { rowid, row, .. } in rows {
        if !row_matches(filter, &scope, &row, ctx)? {
            continue;
        }
        let global_index_intents =
            global_index_delete_intents_for_row(table, global_indexes, rowid, &row)?;
        let bucket = hash_bucket_for_row(table, &row)?;
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
    let column = hash
        .columns
        .first()
        .ok_or_else(|| ExecError::Unsupported("hash sharding catalog has no hash column".into()))?;
    let index = table
        .column_index(column)
        .ok_or_else(|| ExecError::Unsupported("hash sharding catalog column mismatch".into()))?;
    let bytes = match &row[index] {
        Datum::Int4(value) => value.to_be_bytes().to_vec(),
        Datum::Int8(value) => value.to_be_bytes().to_vec(),
        Datum::Text(value) => value.as_bytes().to_vec(),
        Datum::Bytea(value) => value.clone(),
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
/// iff `xid` was neither still running at, nor started after, the snapshot —
/// mirroring the negation of `Snapshot::is_running`.
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
/// answer — so both ranges' Prepared rows flip visible together at the single
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
/// frozen tuple keeps its original key while its header reads `FROZEN_XID` —
/// writers must stamp `xmax` on the physical key, never one reconstructed
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

/// Coerce an evaluated value into a target column type (assignment context). `ctx`
/// supplies the session zone for any temporal numeric conversion.
fn coerce(
    value: crabka_pgtypes::Datum,
    target: crabka_pgtypes::ColumnType,
    ctx: &crate::clock::EvalCtx,
) -> Result<crabka_pgtypes::Datum, ExecError> {
    use crabka_pgtypes::{ColumnType, Datum, TypeError};
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
        (Datum::Text(s), ColumnType::Varchar(limit)) => {
            Datum::Text(crabka_pgtypes::string::apply_varchar_typmod(&s, limit)?)
        }
        (Datum::Text(s), ColumnType::Char(limit)) => {
            Datum::Text(crabka_pgtypes::string::apply_char_typmod(&s, limit)?)
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
/// offset pushdown applicable — a join, a comma-FROM (cross join), or a derived
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
    let [crabka_pgparser::ast::TableExpr::Table { name, .. }] = from else {
        return false;
    };
    if ctes.lookup(name).is_some() {
        return false;
    }
    crabka_pgcatalog::get_table(catalog_kv, name).is_ok_and(|t| t.foreign.is_some())
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
) -> Result<Relation, ExecError> {
    let ctx = read_ctx.eval_ctx;
    let mut iter = from.iter();
    let first = iter
        .next()
        .ok_or_else(|| ExecError::Unsupported("build_from on empty FROM".into()))?;
    let mut acc = build_table_expr(read_ctx, first, bounds, scan_plan)?;
    for te in iter {
        // A comma-FROM (multiple tables) is a cross join — no single-table
        // pushdown applies, so subsequent items always scan in full.
        let next = build_table_expr(read_ctx, te, None, None)?;
        acc = join_relations(
            acc,
            next,
            crabka_pgparser::ast::JoinKind::Cross,
            &crabka_pgparser::ast::JoinConstraint::None,
            ctx,
        )?;
    }
    Ok(acc)
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
        TableExpr::Table { name, alias } => {
            if let Some(rel) = ctes.lookup(name) {
                let qualifier = alias.as_deref().unwrap_or(name);
                return Ok(crate::cte::requalify_cte(rel, qualifier));
            }
            if let Some(rel) = virtual_catalog_relation(catalog_kv, name, alias.as_deref())? {
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
                    let qualifier = alias.as_deref().unwrap_or(&view.name);
                    return requalify_view_relation(relation, &view, qualifier);
                }
                Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => {}
                Err(error) => return Err(error.into()),
            }
            let t = crabka_pgcatalog::get_table(catalog_kv, name)?;
            let qualifier = alias.as_deref().unwrap_or(&t.name);
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
                let rows = scanner.scan(&t, &server, mapping.as_ref(), scan_bounds, ctx)?;
                let scope = Scope::single(&t, qualifier);
                return Ok(Relation { scope, rows });
            }
            let scope = Scope::single(&t, qualifier);
            let default_scan_plan = crate::plan_dist::DistributedScanPlan::default();
            let distributed_plan = scan_plan.unwrap_or(&default_scan_plan);
            if let Some(rows) = try_scan_with_local_index(read_ctx, &t, distributed_plan)? {
                let rows = rows.into_iter().map(|scanned| scanned.row).collect();
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
                crate::scanner::BLOCKING_QUERY_MEMORY_BYTES,
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
                        crate::scanner::BLOCKING_QUERY_MEMORY_BYTES,
                    )?
                }
                Err(error) => return Err(error),
            }
            .into_iter()
            .map(|scanned| scanned.row)
            .collect();
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
            let r = build_table_expr(read_ctx, right, None, None)?;
            join_relations(l, r, *kind, constraint, ctx)
        }
        TableExpr::Derived {
            subquery,
            alias,
            columns,
        } => {
            let inner = crate::query::query_to_relation_with_ctes(read_ctx, subquery)?;
            crate::values::requalify_derived(inner, alias, columns)
        }
    }
}

fn try_distributed_inner_equi_join(
    read_ctx: &crate::subquery::SubCtx<'_>,
    left_expr: &crabka_pgparser::ast::TableExpr,
    right_expr: &crabka_pgparser::ast::TableExpr,
    kind: crabka_pgparser::ast::JoinKind,
    constraint: &crabka_pgparser::ast::JoinConstraint,
) -> Result<Option<Relation>, ExecError> {
    let catalog_kv = read_ctx.catalog_kv;
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
        },
        TableExpr::Table {
            name: right_name,
            alias: right_alias,
        },
    ) = (left_expr, right_expr)
    else {
        return Ok(None);
    };
    if ctes.lookup(left_name).is_some() || ctes.lookup(right_name).is_some() {
        return Ok(None);
    }
    let JoinConstraint::On(Expr::Binary {
        op: BinaryOp::Eq,
        left: key_left,
        right: key_right,
    }) = constraint
    else {
        return Ok(None);
    };
    let table = |name: &str| match crabka_pgcatalog::get_table(catalog_kv, name) {
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
    let left_qualifier = left_alias.as_deref().unwrap_or(&left_table.name);
    let right_qualifier = right_alias.as_deref().unwrap_or(&right_table.name);
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
            table_name: left_table.name.clone(),
            interval: RowInterval::ALL,
        },
        right: JoinTableInterval {
            table_id: u64::from(right_table.id),
            table_name: right_table.name.clone(),
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
    let Some((table, qualifier)) =
        single_sharded_base_table(read_ctx.catalog_kv, s, read_ctx.ctes)?
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
/// folding instead of materializing every visible row before folding.
///
/// Fires on exactly one ordinary non-sharded base table when every projection
/// item decomposes into scalar expressions over pushdown-model aggregate calls
/// (`CAST(count(*) AS BIGINT)`, `COALESCE(sum(x), 0)`, `sum(a) / count(*)`, …
/// — or the narrow grouped shape), with a WHERE that parses into the strict
/// pushdown predicate subset. Everything else — `DISTINCT`, `HAVING`, bare
/// ungrouped columns, aggregates over non-column arguments, whole-row reads,
/// non-pushdown filters — keeps the materializing scan and its whole-result
/// memory budget.
fn try_execute_local_streaming_aggregate(
    read_ctx: &crate::subquery::SubCtx<'_>,
    s: &SelectStmt,
) -> Result<Option<Relation>, ExecError> {
    if !is_streamable_aggregate_select(s) {
        return Ok(None);
    }
    let Some((table, qualifier)) = single_local_base_table(read_ctx.catalog_kv, s, read_ctx.ctes)?
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
        crate::scanner::BLOCKING_QUERY_MEMORY_BYTES,
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
    /// The narrow grouped shape: one spec whose finalized rows — group key
    /// columns then the aggregate, ordered by key — ARE the output rows.
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
/// model — the caller keeps the materializing scan.
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
    s: &SelectStmt,
    ctes: &crate::cte::CteContext,
) -> Result<Option<(Table, String)>, ExecError> {
    let [crabka_pgparser::ast::TableExpr::Table { name, alias }] = s.from.as_slice() else {
        return Ok(None);
    };
    if ctes.lookup(name).is_some() || virtual_table(name).is_some() {
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
    if table.sharded || table.foreign.is_some() {
        return Ok(None);
    }
    let qualifier = alias.clone().unwrap_or_else(|| table.name.clone());
    Ok(Some((table, qualifier)))
}

/// Match a FROM that is exactly one sharded base table.
///
/// CTE, view, virtual-catalog, local, and foreign relations all resolve
/// through their own scan paths, so they deliberately return `None` here —
/// as does a relation that does not exist at all, whose undefined-table
/// error surfaces from the materializing path instead.
fn single_sharded_base_table(
    catalog_kv: &dyn Kv,
    s: &SelectStmt,
    ctes: &crate::cte::CteContext,
) -> Result<Option<(Table, String)>, ExecError> {
    let [crabka_pgparser::ast::TableExpr::Table { name, alias }] = s.from.as_slice() else {
        return Ok(None);
    };
    if ctes.lookup(name).is_some() || virtual_table(name).is_some() {
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
    let qualifier = alias.clone().unwrap_or_else(|| table.name.clone());
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
    if s.distinct || s.having.is_some() || s.limit.is_some() || s.offset.is_some() {
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
    if !table.sharded || !is_top_k_candidate(s) {
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
    let limit = u64::try_from(s.limit.expect("candidate has a positive limit"))
        .map_err(|_| ExecError::Unsupported("top-k LIMIT is outside u64 range".into()))?;
    Ok(Some(crate::TopKSpec { order_by, limit }))
}

fn is_top_k_candidate(s: &SelectStmt) -> bool {
    if s.distinct
        || !s.group_by.is_empty()
        || s.having.is_some()
        || s.offset.is_some()
        || crate::agg::is_aggregate_query(s)
        || s.order_by.is_empty()
    {
        return false;
    }
    s.limit.is_some_and(|limit| limit > 0)
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

fn hash_sharding_from_ast(
    sharding: &crabka_pgparser::ast::ShardingSpec,
) -> Result<crabka_pgcatalog::ShardingStrategy, ExecError> {
    match sharding {
        crabka_pgparser::ast::ShardingSpec::Hash(hash) => {
            if hash.columns.is_empty() {
                return Err(ExecError::Unsupported(
                    "hash sharding requires at least one column".into(),
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
    let ctes = read_ctx.ctes;
    let ctx = read_ctx.eval_ctx;
    let fctx = read_ctx.fctx;
    reject_nested_relation_locking(s)?;

    // SP34: resolve this (sub)query's uncorrelated subquery expressions to constants
    // first, under the same snapshot handles. Nested subqueries recurse here.
    let resolved = crate::subquery::resolve_in_select(read_ctx, s)?;
    let s = &resolved;
    let relation = if s.from.is_empty() {
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
            [crabka_pgparser::ast::TableExpr::Table { name, alias }]
                if ctes.lookup(name).is_none()
                    && crabka_pgcatalog::get_table(catalog_kv, name)
                        .is_ok_and(|table| table.foreign.is_none()) =>
            {
                let table = crabka_pgcatalog::get_table(catalog_kv, name)?;
                let mut plan =
                    crate::plan_dist::plan_scan(&table, s.filter.as_ref(), &s.projection);
                plan.projection = crate::ProjectionPushdown::All;
                plan.partial_aggregate = None;
                let qualifier = alias.as_deref().unwrap_or(&table.name);
                plan.top_k = top_k_pushdown_for_select(&table, qualifier, s)?;
                Some(plan)
            }
            _ => None,
        };
        build_from(read_ctx, &s.from, pushed.as_ref(), scan_plan.as_ref())?
    };
    let mut kept = Vec::new();
    for row in &relation.rows {
        if row_matches(s.filter.as_ref(), &relation.scope, row, ctx)? {
            kept.push(row.clone());
        }
    }
    let (fields, out_exprs, tys) = resolve_projection(&s.projection, &relation.scope)?;
    let out_scope = Scope {
        columns: fields
            .iter()
            .zip(&tys)
            .map(|(f, ty)| ColumnBinding {
                qualifier: None, // a projected result has no base-table qualifier
                name: f.name.clone(),
                ty: *ty,
            })
            .collect(),
    };
    let rows = if crate::agg::is_aggregate_query(s) {
        crate::agg::aggregate_rows(s, &relation.scope, kept, ctx)?
    } else {
        project_rows_ordered(s, &relation.scope, &fields, &out_exprs, kept, ctx)?
    };
    Ok(Relation {
        scope: out_scope,
        rows,
    })
}

pub(crate) fn build_from_schema_with_ctes(
    catalog_kv: &dyn Kv,
    from: &[crabka_pgparser::ast::TableExpr],
    ctes: &crate::cte::CteContext,
) -> Result<Relation, ExecError> {
    let mut iter = from.iter();
    let first = iter
        .next()
        .ok_or_else(|| ExecError::Unsupported("build_from_schema on empty FROM".into()))?;
    let mut acc = build_table_expr_schema_with_ctes(catalog_kv, first, ctes)?;
    for te in iter {
        let next = build_table_expr_schema_with_ctes(catalog_kv, te, ctes)?;
        // Schema-only: no rows, so no ON predicate is ever evaluated — a default
        // (UTC/epoch) eval context is correct here.
        acc = join_relations(
            acc,
            next,
            crabka_pgparser::ast::JoinKind::Cross,
            &crabka_pgparser::ast::JoinConstraint::None,
            &crate::clock::EvalCtx::test_default(),
        )?;
    }
    Ok(acc)
}

fn build_table_expr_schema_with_ctes(
    catalog_kv: &dyn Kv,
    te: &crabka_pgparser::ast::TableExpr,
    ctes: &crate::cte::CteContext,
) -> Result<Relation, ExecError> {
    use crabka_pgparser::ast::TableExpr;
    match te {
        TableExpr::Table { name, alias } => {
            if let Some(rel) = ctes.lookup(name) {
                let qualifier = alias.as_deref().unwrap_or(name);
                let mut rel = crate::cte::requalify_cte(rel, qualifier);
                rel.rows.clear();
                return Ok(rel);
            }
            if let Some(rel) = virtual_catalog_relation_schema(name, alias.as_deref()) {
                return Ok(rel);
            }
            match crabka_pgcatalog::get_view(catalog_kv, name) {
                Ok(view) => {
                    let qualifier = alias.as_deref().unwrap_or(&view.name);
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
            let qualifier = alias.as_deref().unwrap_or(&t.name);
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
            let l = build_table_expr_schema_with_ctes(catalog_kv, left, ctes)?;
            let r = build_table_expr_schema_with_ctes(catalog_kv, right, ctes)?;
            // Schema-only: no rows, so no ON predicate is ever evaluated.
            join_relations(
                l,
                r,
                *kind,
                constraint,
                &crate::clock::EvalCtx::test_default(),
            )
        }
        TableExpr::Derived {
            subquery,
            alias,
            columns,
        } => {
            let fields = crate::query::describe_query_expr_with_ctes(catalog_kv, subquery, ctes)?;
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

const PG_CATALOG_NAMESPACE_OID: i32 = 11;
const INFORMATION_SCHEMA_NAMESPACE_OID: i32 = 13_370;
const PUBLIC_NAMESPACE_OID: i32 = 2200;
const CURRENT_DATABASE: &str = "postgres";

#[derive(Debug, Clone, Copy)]
struct BuiltinTypeRow {
    oid: i32,
    name: &'static str,
    len: i16,
    category: &'static str,
}

fn virtual_catalog_relation(
    catalog_kv: &dyn Kv,
    name: &str,
    alias: Option<&str>,
) -> Result<Option<Relation>, ExecError> {
    let Some(table) = virtual_table(name) else {
        return Ok(None);
    };
    let rows = virtual_catalog_rows(catalog_kv, table)?;
    Ok(Some(Relation {
        scope: Scope::single(&virtual_catalog_table(table), alias.unwrap_or(name)),
        rows,
    }))
}

fn virtual_catalog_relation_schema(name: &str, alias: Option<&str>) -> Option<Relation> {
    let table = virtual_table(name)?;
    Some(Relation {
        scope: Scope::single(&virtual_catalog_table(table), alias.unwrap_or(name)),
        rows: Vec::new(),
    })
}

fn virtual_table(name: &str) -> Option<&'static str> {
    match name.strip_prefix("pg_catalog.").unwrap_or(name) {
        "pg_namespace" => Some("pg_namespace"),
        "pg_class" => Some("pg_class"),
        "pg_attribute" => Some("pg_attribute"),
        "pg_type" => Some("pg_type"),
        "pg_range" => Some("pg_range"),
        "pg_index" => Some("pg_index"),
        "pg_settings" => Some("pg_settings"),
        "pg_roles" => Some("pg_roles"),
        "pg_user" => Some("pg_user"),
        _ => None,
    }
    .or_else(|| match name.strip_prefix("information_schema.")? {
        "schemata" => Some("information_schema.schemata"),
        "tables" => Some("information_schema.tables"),
        "columns" => Some("information_schema.columns"),
        _ => None,
    })
}

fn virtual_catalog_table(name: &str) -> Table {
    Table {
        id: virtual_relation_oid(name) as u32,
        name: virtual_relation_name(name).to_string(),
        columns: virtual_catalog_columns(name),
        sharded: false,
        sharding: None,
        foreign: None,
    }
}

fn virtual_catalog_columns(name: &str) -> Vec<Column> {
    use ColumnType::{Bool, Int4, Text};
    match name {
        "pg_namespace" => cols(&[("oid", Int4), ("nspname", Text)]),
        "pg_class" => cols(&[
            ("oid", Int4),
            ("relname", Text),
            ("relnamespace", Int4),
            ("relkind", Text),
            ("relnatts", Int4),
            ("relhasindex", Bool),
            ("reltype", Int4),
        ]),
        "pg_attribute" => cols(&[
            ("attrelid", Int4),
            ("attname", Text),
            ("atttypid", Int4),
            ("attnum", Int4),
            ("attnotnull", Bool),
            ("attisdropped", Bool),
            ("atttypmod", Int4),
        ]),
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
            ("typelem", Int4),
            ("typbasetype", Int4),
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
        "pg_index" => cols(&[
            ("indexrelid", Int4),
            ("indrelid", Int4),
            ("indisunique", Bool),
            ("indisprimary", Bool),
            ("indkey", Text),
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
        "pg_roles" => cols(&[
            ("rolname", Text),
            ("rolcanlogin", Bool),
            ("rolsuper", Bool),
            ("rolcreaterole", Bool),
            ("rolcreatedb", Bool),
            ("rolreplication", Bool),
            ("rolbypassrls", Bool),
        ]),
        "pg_user" => cols(&[("usename", Text), ("usesuper", Bool), ("usecreatedb", Bool)]),
        "information_schema.schemata" => cols(&[("catalog_name", Text), ("schema_name", Text)]),
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
        _ => Vec::new(),
    }
}

fn cols(defs: &[(&str, ColumnType)]) -> Vec<Column> {
    defs.iter()
        .map(|(name, ty)| Column::new(*name, *ty))
        .collect()
}

fn virtual_catalog_rows(catalog_kv: &dyn Kv, name: &str) -> Result<Vec<Vec<Datum>>, ExecError> {
    match name {
        "pg_namespace" => Ok(pg_namespace_rows()),
        "pg_class" => pg_class_rows(catalog_kv),
        "pg_attribute" => pg_attribute_rows(catalog_kv),
        "pg_type" => Ok(pg_type_rows()),
        // Zero rows: no built-in type in the exposed scalar slice is a range
        // type. Drivers still LEFT JOIN it in their typeinfo queries.
        "pg_range" => Ok(Vec::new()),
        "pg_index" => pg_index_rows(catalog_kv),
        "pg_settings" => pg_settings_rows(),
        "pg_roles" => pg_roles_rows(catalog_kv),
        "pg_user" => pg_user_rows(catalog_kv),
        "information_schema.schemata" => Ok(information_schema_schemata_rows()),
        "information_schema.tables" => information_schema_tables_rows(catalog_kv),
        "information_schema.columns" => information_schema_columns_rows(catalog_kv),
        _ => Ok(Vec::new()),
    }
}

fn pg_namespace_rows() -> Vec<Vec<Datum>> {
    vec![
        vec![int(PG_CATALOG_NAMESPACE_OID), text("pg_catalog")],
        vec![
            int(INFORMATION_SCHEMA_NAMESPACE_OID),
            text("information_schema"),
        ],
        vec![int(PUBLIC_NAMESPACE_OID), text("public")],
    ]
}

fn pg_class_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let indexes = crabka_pgcatalog::list_indexes(catalog_kv)?;
    let indexed_table_ids = indexes
        .iter()
        .map(|index| index.table_id)
        .collect::<std::collections::BTreeSet<_>>();
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(catalog_kv)? {
        rows.push(pg_class_row(
            oid_i32(table.id)?,
            &table.name,
            if table.foreign.is_some() { "f" } else { "r" },
            table.columns.len(),
            indexed_table_ids.contains(&table.id),
            PUBLIC_NAMESPACE_OID,
        )?);
    }
    for virtual_table in virtual_table_names() {
        let table = virtual_catalog_table(virtual_table);
        rows.push(pg_class_row(
            virtual_relation_oid(virtual_table),
            &table.name,
            "v",
            table.columns.len(),
            false,
            virtual_relation_namespace_oid(virtual_table),
        )?);
    }
    for index in indexes {
        rows.push(pg_class_row(
            catalog_index_oid(index.id)?,
            &index.name,
            "i",
            0,
            false,
            PUBLIC_NAMESPACE_OID,
        )?);
    }
    Ok(rows)
}

fn pg_class_row(
    oid: i32,
    relname: &str,
    relkind: &str,
    relnatts: usize,
    relhasindex: bool,
    relnamespace: i32,
) -> Result<Vec<Datum>, ExecError> {
    Ok(vec![
        int(oid),
        text(relname),
        int(relnamespace),
        text(relkind),
        int(usize_i32(relnatts)?),
        Datum::Bool(relhasindex),
        int(0),
    ])
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
    Ok(rows)
}

fn information_schema_schemata_rows() -> Vec<Vec<Datum>> {
    ["public", "pg_catalog", "information_schema"]
        .into_iter()
        .map(|schema| vec![text(CURRENT_DATABASE), text(schema)])
        .collect()
}

fn information_schema_tables_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crabka_pgcatalog::list_tables(catalog_kv)?
        .into_iter()
        .map(|table| {
            vec![
                text(CURRENT_DATABASE),
                text("public"),
                text(&table.name),
                text(if table.foreign.is_some() {
                    "FOREIGN"
                } else {
                    "BASE TABLE"
                }),
            ]
        })
        .collect::<Vec<_>>())
}

fn information_schema_columns_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    for table in crabka_pgcatalog::list_tables(catalog_kv)? {
        for (idx, column) in table.columns.iter().enumerate() {
            rows.push(vec![
                text("public"),
                text(&table.name),
                text(&column.name),
                int(usize_i32(idx + 1)?),
                text(column.ty.name()),
                text(if column.not_null { "NO" } else { "YES" }),
                column_default_datum(column),
            ]);
        }
    }
    Ok(rows)
}

fn column_default_datum(column: &Column) -> Datum {
    let Some(default) = &column.default else {
        return Datum::Null;
    };
    text(&format_column_default(default, column.ty))
}

fn format_column_default(default: &ColumnDefault, ty: ColumnType) -> String {
    match default {
        ColumnDefault::NextVal(sequence) => {
            format!("nextval('{}'::regclass)", escape_sql_string(sequence))
        }
        ColumnDefault::Value(value) => format_default_value(value, ty),
    }
}

fn format_default_value(value: &Datum, ty: ColumnType) -> String {
    match value {
        Datum::Null => "NULL".to_string(),
        Datum::Bool(true) => "true".to_string(),
        Datum::Bool(false) => "false".to_string(),
        Datum::Int4(value) => value.to_string(),
        Datum::Int8(value) => value.to_string(),
        Datum::Float8(value) => value.to_string(),
        Datum::Numeric(value) => value.to_string(),
        Datum::Text(value) => {
            let mut out = String::new();
            let _ = write!(out, "'{}'::{}", escape_sql_string(value), ty.name());
            out
        }
        Datum::Date(_)
        | Datum::Time(_)
        | Datum::Timestamp(_)
        | Datum::Timestamptz(_)
        | Datum::Interval(_)
        | Datum::Bytea(_) => "<unsupported>".to_string(),
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn attribute_rows_for_table(relid: i32, table: &Table) -> Result<Vec<Vec<Datum>>, ExecError> {
    table
        .columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            Ok(vec![
                int(relid),
                text(&column.name),
                int(oid_i32(column.ty.oid())?),
                int(usize_i32(idx + 1)?),
                Datum::Bool(column.not_null),
                Datum::Bool(false),
                int(column.ty.typmod()),
            ])
        })
        .collect()
}

fn pg_type_rows() -> Vec<Vec<Datum>> {
    builtin_type_rows()
        .iter()
        .map(|ty| {
            vec![
                int(ty.oid),
                text(ty.name),
                int(i32::from(ty.len)),
                text(ty.category),
                int(PG_CATALOG_NAMESPACE_OID),
                int(0),
                // Every exposed built-in is a base scalar: typtype 'b', no
                // array element type, and no domain base type — matching
                // PostgreSQL 18's pg_type for these OIDs.
                text("b"),
                int(0),
                int(0),
            ]
        })
        .collect()
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
            Ok(vec![
                int(catalog_index_oid(index.id)?),
                int(oid_i32(index.table_id)?),
                Datum::Bool(index.unique),
                Datum::Bool(false),
                text(&indkey),
            ])
        })
        .collect()
}

fn pg_settings_rows() -> Result<Vec<Vec<Datum>>, ExecError> {
    crate::session::guc_settings_runtime()?
        .into_iter()
        .map(|setting| {
            Ok(vec![
                text(&setting.name),
                text(&setting.value),
                Datum::Null,
                text("Client Connection Defaults / Statement Behavior"),
                text("Crabka session parameter"),
                text("user"),
                text(&setting.vartype),
                text("session"),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                text(&setting.boot_val),
                text(&setting.reset_val),
                Datum::Bool(false),
            ])
        })
        .collect()
}

fn pg_roles_rows(catalog_kv: &dyn Kv) -> Result<Vec<Vec<Datum>>, ExecError> {
    Ok(crabka_pgcatalog::list_roles(catalog_kv)?
        .into_iter()
        .map(|role| {
            vec![
                text(&role.name),
                Datum::Bool(role.can_login),
                Datum::Bool(false),
                Datum::Bool(false),
                Datum::Bool(false),
                Datum::Bool(false),
                Datum::Bool(false),
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

fn virtual_table_names() -> &'static [&'static str] {
    &[
        "pg_namespace",
        "pg_class",
        "pg_attribute",
        "pg_type",
        "pg_range",
        "pg_index",
        "pg_settings",
        "pg_roles",
        "pg_user",
        "information_schema.schemata",
        "information_schema.tables",
        "information_schema.columns",
    ]
}

fn virtual_relation_name(name: &str) -> &str {
    name.rsplit_once('.').map_or(name, |(_, relation)| relation)
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
pub(crate) fn resolve_regclass(catalog_kv: &dyn Kv, name: &str) -> Result<i32, ExecError> {
    let trimmed = name.trim();
    let bare = trimmed
        .strip_prefix("pg_catalog.")
        .or_else(|| trimmed.strip_prefix("public."))
        .unwrap_or(trimmed);
    if virtual_table_names().contains(&bare) {
        return Ok(virtual_relation_oid(bare));
    }
    let table = crabka_pgcatalog::get_table(catalog_kv, bare)?;
    oid_i32(table.id)
}

fn virtual_relation_oid(name: &str) -> i32 {
    match name {
        "pg_namespace" => 2615,
        "pg_class" => 1259,
        "pg_attribute" => 1249,
        "pg_type" => 1247,
        "pg_range" => 3541,
        "pg_index" => 2610,
        "pg_settings" => 100_001,
        "pg_roles" => 1261,
        "pg_user" => 100_002,
        "information_schema.schemata" => 100_010,
        "information_schema.tables" => 100_011,
        "information_schema.columns" => 100_012,
        _ => 0,
    }
}

fn builtin_type_rows() -> &'static [BuiltinTypeRow] {
    &[
        BuiltinTypeRow {
            oid: 2205,
            name: "regclass",
            len: 4,
            category: "N",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::BOOL as i32,
            name: "bool",
            len: 1,
            category: "B",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::BYTEA as i32,
            name: "bytea",
            len: -1,
            category: "U",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INT8 as i32,
            name: "int8",
            len: 8,
            category: "N",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INT4 as i32,
            name: "int4",
            len: 4,
            category: "N",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TEXT as i32,
            name: "text",
            len: -1,
            category: "S",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::BPCHAR as i32,
            name: "bpchar",
            len: -1,
            category: "S",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::VARCHAR as i32,
            name: "varchar",
            len: -1,
            category: "S",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::FLOAT8 as i32,
            name: "float8",
            len: 8,
            category: "N",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::NUMERIC as i32,
            name: "numeric",
            len: -1,
            category: "N",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::DATE as i32,
            name: "date",
            len: 4,
            category: "D",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TIME as i32,
            name: "time",
            len: 8,
            category: "D",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TIMESTAMP as i32,
            name: "timestamp",
            len: 8,
            category: "D",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::TIMESTAMPTZ as i32,
            name: "timestamptz",
            len: 8,
            category: "D",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::INTERVAL as i32,
            name: "interval",
            len: 16,
            category: "T",
        },
        BuiltinTypeRow {
            oid: crabka_pgtypes::oids::UUID as i32,
            name: "uuid",
            len: 16,
            category: "U",
        },
    ]
}

pub(crate) fn format_type_name(oid: i32) -> Option<&'static str> {
    builtin_type_rows()
        .iter()
        .find(|ty| ty.oid == oid)
        .map(|ty| ty.name)
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

pub(crate) fn execute_read(
    read_ctx: &crate::subquery::SubCtx<'_>,
    stmt: &Statement,
) -> Result<QueryResult, ExecError> {
    let Statement::Query(q) = stmt else {
        return Err(ExecError::Unsupported("not a query statement".into()));
    };
    let rel = crate::query::query_to_relation(read_ctx, q)?;
    Ok(crate::query::relation_to_rows_result(
        rel,
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
    mode: crate::lockmgr::LockMode,
    lock_wait_cap: Option<std::time::Duration>,
    s: &SelectStmt,
) -> Result<QueryResult, ExecError> {
    let catalog_kv = read_ctx.catalog_kv;
    let kv = read_ctx.kv;
    let global = read_ctx.global;
    let gsnap = read_ctx.gsnap;
    let snapshot = read_ctx.snapshot;
    let xid = read_ctx.own.ok_or_else(|| {
        ExecError::Unsupported("locking SELECT requires a transaction xid".into())
    })?;
    let ctx = read_ctx.eval_ctx;
    // SP34: resolve uncorrelated subqueries (e.g. in the WHERE of a FOR UPDATE) to
    // constants first, under this statement's snapshot handles.
    let resolved = crate::subquery::resolve_in_select(read_ctx, s)?;
    let s = &resolved;
    // FOR UPDATE/SHARE is not allowed with aggregation (PostgreSQL 0A000).
    if crate::agg::is_aggregate_query(s) {
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE is not allowed with aggregate functions or GROUP BY".into(),
        ));
    }
    // SP28: nor with SELECT DISTINCT (PostgreSQL 0A000).
    if s.distinct {
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE is not allowed with DISTINCT clause".into(),
        ));
    }
    // FOR UPDATE/SHARE requires exactly one base table — there are no rows to lock
    // in a FROM-less SELECT, and a join is not supported (0A000).
    let t = match s.from.as_slice() {
        [crabka_pgparser::ast::TableExpr::Table { name, .. }] => {
            crabka_pgcatalog::get_table(catalog_kv, name)?
        }
        [] => {
            return Err(ExecError::Unsupported(
                "FOR UPDATE/SHARE requires a FROM clause".into(),
            ));
        }
        _ => {
            return Err(ExecError::Unsupported(
                "FOR UPDATE/SHARE with a join is not supported".into(),
            ));
        }
    };
    let scope = Scope::single(&t, &t.name);

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
        lockmgr
            .acquire(t.id, rowid, mode, xid, lock_wait_cap)
            .await
            .map_err(lock_acquire_error)?;

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

    project_order_limit(s, &scope, kept, ctx)
}

/// Apply DISTINCT / ORDER BY / OFFSET / LIMIT and projection, returning the
/// projected output Datum rows. Shared by the top-level row path and derived
/// tables. `ctx` carries the session zone + transaction/statement clock used by
/// temporal eval.
fn project_rows_ordered(
    s: &SelectStmt,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    mut kept: Vec<Vec<Datum>>,
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let order_keys = resolve_select_order_keys(&s.order_by, scope, fields, out_exprs, s.distinct)?;

    // SP39: SELECT DISTINCT projects FIRST, dedups output rows, then ORDER BY
    // sorts the deduped output. PostgreSQL requires every sort key to refer to
    // the select-list output (ordinal, alias/name, or the exact select expression).
    if s.distinct {
        let mut projected = project_rows(out_exprs, scope, &kept, ctx)?;
        ensure_blocking_rows_fit(&projected)?;
        let mut seen: std::collections::HashSet<Vec<Datum>> = std::collections::HashSet::new();
        projected.retain(|r| seen.insert(r.clone()));
        if !s.order_by.is_empty() {
            let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = projected
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
            keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, &s.order_by));
            projected = keyed.into_iter().map(|(_, r)| r).collect();
        }
        apply_offset_limit(&mut projected, s.offset, s.limit);
        return Ok(projected);
    }

    // Non-DISTINCT keeps the existing source-row ordering shape so non-projected
    // source expressions still work, but output ordinals/labels evaluate the
    // corresponding projection expression for each source row.
    if !s.order_by.is_empty() {
        let mut keyed: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(kept.len());
        let mut keyed_bytes = 0usize;
        for row in kept {
            let mut keys = Vec::with_capacity(order_keys.len());
            for key in &order_keys {
                keys.push(match key {
                    SelectOrderKey::Output(i) => {
                        crate::eval::eval(&out_exprs[*i], scope, &row, ctx)?
                    }
                    SelectOrderKey::SourceExpr(expr) => crate::eval::eval(expr, scope, &row, ctx)?,
                });
            }
            let bytes = crate::scanner::datum_row_bytes(&keys)
                .saturating_add(crate::scanner::datum_row_bytes(&row));
            if keyed_bytes.saturating_add(bytes) > crate::scanner::BLOCKING_QUERY_MEMORY_BYTES {
                return Err(crate::scanner::memory_budget_exceeded());
            }
            keyed_bytes += bytes;
            keyed.push((keys, row));
        }
        keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, &s.order_by));
        kept = keyed.into_iter().map(|(_, row)| row).collect();
    }
    apply_offset_limit(&mut kept, s.offset, s.limit);
    project_rows(out_exprs, scope, &kept, ctx)
}

fn ensure_blocking_rows_fit(rows: &[Vec<Datum>]) -> Result<(), ExecError> {
    let bytes = rows.iter().fold(0usize, |bytes, row| {
        bytes.saturating_add(crate::scanner::datum_row_bytes(row))
    });
    if bytes > crate::scanner::BLOCKING_QUERY_MEMORY_BYTES {
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
    Ok(rows_result(fields, &rows, &ctx.time_zone))
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
    tz: &jiff::tz::TimeZone,
) -> QueryResult {
    rows_result_with_tag(fields, projected, tz, format!("SELECT {}", projected.len()))
}

fn rows_result_with_tag(
    fields: Vec<FieldDescription>,
    projected: &[Vec<Datum>],
    tz: &jiff::tz::TimeZone,
    tag: String,
) -> QueryResult {
    let rows: Vec<Vec<Option<Cell>>> = projected
        .iter()
        .map(|r| r.iter().map(|d| datum_to_cell(d, tz)).collect())
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
    if let Expr::IntLiteral(s) = &item.expr {
        let pos: i32 = s
            .parse()
            .map_err(|_| ExecError::Syntax("non-integer constant in ORDER BY".into()))?;
        if pos <= 0 || pos as usize > fields.len() {
            return Err(ExecError::InvalidColumnReference(format!(
                "ORDER BY position {pos} is not in select list"
            )));
        }
        return Ok(SelectOrderKey::Output(pos as usize - 1));
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

/// Expand the projection list into output FieldDescriptions, the expressions
/// that produce each column, and each column's `ColumnType` (the third element
/// lets `select_to_relation` build a derived table's output scope without
/// re-inferring types).
type ResolvedProjection = (Vec<FieldDescription>, Vec<Expr>, Vec<ColumnType>);

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
                if scope.columns.is_empty() {
                    return Err(ExecError::Unsupported(
                        "SELECT * with no FROM clause is not supported".into(),
                    ));
                }
                for c in &scope.columns {
                    fields.push(field(&c.name, c.ty));
                    exprs.push(Expr::Column {
                        table: c.qualifier.clone(),
                        name: c.name.clone(),
                    });
                    tys.push(c.ty);
                }
            }
            SelectItem::QualifiedWildcard(q) => {
                let cols: Vec<_> = scope
                    .columns
                    .iter()
                    .filter(|c| c.qualifier.as_deref() == Some(q))
                    .collect();
                if cols.is_empty() {
                    return Err(ExecError::MissingFromEntry(q.clone()));
                }
                for c in cols {
                    fields.push(field(&c.name, c.ty));
                    exprs.push(Expr::Column {
                        table: c.qualifier.clone(),
                        name: c.name.clone(),
                    });
                    tys.push(c.ty);
                }
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| derived_name(expr));
                let ty = crate::eval::infer_type(expr, scope)?;
                fields.push(field(&name, ty));
                exprs.push(expr.clone());
                tys.push(ty);
            }
        }
    }
    Ok((fields, exprs, tys))
}

fn derived_name(expr: &Expr) -> String {
    match expr {
        Expr::Column { name, .. } => name.clone(),
        // PostgreSQL names an aggregate output column after the function.
        Expr::Func(fc) => fc.name.clone(),
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
        crabka_pgtypes::oids::INT4 => ColumnType::Int4,
        crabka_pgtypes::oids::INT8 => ColumnType::Int8,
        crabka_pgtypes::oids::TEXT => ColumnType::Text,
        crabka_pgtypes::oids::VARCHAR => ColumnType::Varchar(None),
        crabka_pgtypes::oids::BPCHAR => ColumnType::Char(None),
        crabka_pgtypes::oids::FLOAT8 => ColumnType::Float8,
        crabka_pgtypes::oids::NUMERIC => ColumnType::Numeric(None),
        crabka_pgtypes::oids::DATE => ColumnType::Date,
        crabka_pgtypes::oids::TIME => ColumnType::Time,
        crabka_pgtypes::oids::TIMESTAMP => ColumnType::Timestamp,
        crabka_pgtypes::oids::TIMESTAMPTZ => ColumnType::Timestamptz,
        crabka_pgtypes::oids::INTERVAL => ColumnType::Interval,
        crabka_pgtypes::oids::UUID => ColumnType::Uuid,
        _ => {
            return Err(ExecError::Unsupported(format!(
                "unknown query field type oid {oid}"
            )));
        }
    })
}

pub(crate) fn datum_to_cell(d: &Datum, tz: &jiff::tz::TimeZone) -> Option<Cell> {
    if d.is_null() {
        return None;
    }
    Some(Cell {
        text: Bytes::from(crabka_pgtypes::encoding::encode_text(d, tz)),
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
            // NULLS LAST for ASC: null is "greater"; NULLS FIRST for DESC.
            (true, false) => {
                if item.asc {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, true) => {
                if item.asc {
                    Ordering::Less
                } else {
                    Ordering::Greater
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
    sql: &str,
) -> Result<Vec<crabka_pgwire::engine::FieldDescription>, ExecError> {
    let statements = crabka_pgparser::parse(sql)?;
    let Some(statement) = statements.first() else {
        return Ok(Vec::new());
    };
    describe_statement(catalog_kv, statement)
}

pub(crate) fn describe_statement(
    catalog_kv: &dyn Kv,
    statement: &Statement,
) -> Result<Vec<crabka_pgwire::engine::FieldDescription>, ExecError> {
    match statement {
        Statement::Query(q) => crate::query::describe_query_expr(catalog_kv, q),
        Statement::Insert {
            table, returning, ..
        }
        | Statement::Update {
            table, returning, ..
        }
        | Statement::Delete {
            table, returning, ..
        } => describe_returning(catalog_kv, table, returning.as_deref()),
        _ => Ok(Vec::new()),
    }
}

fn describe_returning(
    catalog_kv: &dyn Kv,
    table: &str,
    returning: Option<&[SelectItem]>,
) -> Result<Vec<FieldDescription>, ExecError> {
    let Some(returning) = returning else {
        return Ok(Vec::new());
    };

    let table = crabka_pgcatalog::get_table(catalog_kv, table)?;
    let scope = Scope::single(&table, &table.name);
    let (fields, _exprs, _tys) = resolve_projection(returning, &scope)?;
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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

        let table = crabka_pgcatalog::get_table(engine.catalog_kv.as_ref(), "t").expect("table");
        let index = crabka_pgcatalog::list_table_indexes(engine.catalog_kv.as_ref(), "t")
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

        let index = crabka_pgcatalog::get_index(engine.catalog_kv.as_ref(), "t_name_idx")
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
            crabka_pgcatalog::get_index(engine.catalog_kv.as_ref(), "t_name_idx")
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
        let table = crabka_pgcatalog::get_table(reopened.catalog_kv.as_ref(), "t").expect("table");
        let index = crabka_pgcatalog::list_table_indexes(reopened.catalog_kv.as_ref(), "t")
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
    async fn insert_wrong_arity_is_42804() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int4, b int4)").await;
        let err = engine
            .connect()
            .simple_query("INSERT INTO t VALUES (1)")
            .await
            .expect_err("arity");
        assert_eq!(err.code, "42804");
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
            name: "t".into(),
            columns: vec![Column::new("val", ColumnType::Int4)],
            sharded: false,
            sharding: None,
            foreign: None,
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

    /// SP21: after a fresh-`g'` re-attempt, a row has TWO physical versions — the
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
        /// fixed corpus of rows IGNORING the bounds — so a result-equivalence test
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
    /// `crabka_pgcatalog::get_user_mapping(kv, "public", server)` — confirming the key is
    /// stored under "public", not "current_user".
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
        let fctx = super::ForeignCtx {
            scanner: None,
            current_user: "public",
        };
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
        let table = crabka_pgcatalog::get_table(engine.catalog_kv.as_ref(), "t").expect("table");

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
}
