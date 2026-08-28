//! Executor contexts shared by read, write, and trigger paths.

use super::*;

/// Rows a top-level command has claimed while executing PL/pgSQL trigger SQL.
///
/// The executor itself keeps statement-local writes in [`StatementWrites`].
/// A trigger body re-enters it through the session actor, so that local state
/// cannot see the outer write. This small shared set crosses only that actor
/// boundary and is dropped with the command.
#[derive(Debug, Default)]
pub(crate) struct CommandRowClaimState {
    pub(super) operations: HashMap<(TableId, u64, u64), CommandOperation>,
    pub(super) row_operations: HashMap<(TableId, u64), CommandOperation>,
}

pub(crate) type CommandRowClaims = Arc<Mutex<CommandRowClaimState>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CommandOperation {
    Updated,
    Deleted,
    UpdatedOrDeleted,
}

impl CommandOperation {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Updated => "updated",
            Self::Deleted => "deleted",
            Self::UpdatedOrDeleted => "updated or deleted",
        }
    }
}

/// Work staged by a sharded-table timestamp DML statement.
pub struct TimestampWritePlan {
    /// SQL command result to return after a successful timestamp commit.
    pub result: QueryResult,
    /// Row/index intents prewritten and resolved by the timestamp participant.
    pub writes: Vec<TimestampWrite>,
    /// Positions in `writes` that need caller-supplied physical row IDs.
    ///
    /// An in-place timestamp update keeps its existing row ID; inserts and
    /// cross-bucket updates need a fresh one.
    pub fresh_rowid_writes: Vec<usize>,
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
/// The relationship between a reference that failed to resolve and the scope one
/// query level out that could have supplied it — all that separates
/// `PostgreSQL`'s wordings for an out-of-reach FROM-clause entry.
#[derive(Clone, Copy)]
pub(crate) struct ForeignCtx<'a> {
    pub scanner: Option<&'a Arc<dyn ForeignScanner>>,
    pub current_user: &'a str,
    /// The role the session authenticated as, which `SET ROLE` does not move.
    ///
    /// It rides alongside [`ForeignCtx::current_user`] because `SESSION_USER`
    /// and `CURRENT_USER` are different grantees under `SET ROLE`, and a
    /// grantee resolved from the wrong one writes the grant to the wrong role.
    pub session_user: &'a str,
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
    /// The session's `row_security` GUC. `false` makes a query that a policy
    /// would have filtered fail with 42501 instead.
    ///
    /// It rides here rather than on `EvalCtx` for a reason worth keeping: this
    /// struct has two construction sites, `EvalCtx` has seventeen across nine
    /// files, and a security input that seventeen call sites must remember to
    /// set is one that some of them will not.
    pub row_security: bool,
    /// The session default used when a table creation omits `USING`.
    pub default_table_access_method: &'a str,
}

impl ForeignCtx<'_> {
    /// A context with no scanner and the conventional `"public"` user, for
    /// paths that never reach a registered scanner (schema-only describe).
    pub(crate) fn none() -> Self {
        Self {
            scanner: None,
            current_user: "public",
            session_user: "public",
            resolution: crate::relname::ResolutionScope::default_scope(),
            catalog: None,
            reserved_table_ids: None,
            own_xid: None,
            row_security: true,
            default_table_access_method: "heap",
        }
    }

    /// The role this session acts as for ownership and row security.
    ///
    /// `PUBLIC` is a pseudo-role that owns nothing and holds no attributes; a
    /// session carrying it authenticated as nobody and is acting as the
    /// bootstrap superuser, which is the role its decisions must be made under.
    pub(crate) fn effective_role(&self) -> &str {
        if self.current_user == crabka_pgcatalog::PUBLIC_ROLE {
            crabka_pgcatalog::BOOTSTRAP_ROLE
        } else {
            self.current_user
        }
    }

    /// Whose eyes a DDL statement's diagnostics are written for.
    ///
    /// `EvalCtx::for_ddl` leaves `current_user` at the conventional `"public"`,
    /// so a DDL path that needs the acting role has to take it from here — the
    /// evaluation context a DDL statement builds does not carry it.
    pub(crate) fn describer(&self) -> crate::rls::Describer {
        crate::rls::Describer::seen_by(self.current_user, self.row_security)
    }

    /// The next reserved id, or the shared counter when there is no block or the
    /// block is spent.
    pub(crate) fn table_id(&self) -> crabka_pgcatalog::TableIdSource {
        self.reserved_table_ids
            .and_then(|reserved| reserved.lock().expect("table ids").pop())
            .map_or(
                crabka_pgcatalog::TableIdSource::Counter,
                crabka_pgcatalog::TableIdSource::Reserved,
            )
    }

    /// Ownership and id allocation for a relation this statement creates. Every
    /// `CREATE` path in `execute_ddl` goes through here, so a new relation
    /// cannot acquire an owner other than the session's own without naming one.
    pub(crate) fn table_creation(&self) -> crabka_pgcatalog::TableCreation<'_> {
        crabka_pgcatalog::TableCreation {
            owner: self.effective_role(),
            id: self.table_id(),
            // `CREATE MATERIALIZED VIEW` builds its own `TableCreation` so it can
            // attach the query; every other creation here makes an ordinary
            // relation.
            materialized: None,
        }
    }
}

/// Map a refused blocking acquire to its statement-level error (both 40P01).
pub(crate) fn lock_acquire_error(error: crate::lockmgr::AcquireError) -> ExecError {
    match error {
        crate::lockmgr::AcquireError::Deadlock => ExecError::Deadlock,
        crate::lockmgr::AcquireError::CapExpired => ExecError::LockWaitCapExpired,
    }
}

#[derive(Clone, Copy)]
pub(crate) struct WriteContext<'a> {
    pub catalog_kv: &'a dyn Kv,
    pub kv: &'a dyn Kv,
    pub global: &'a dyn Kv,
    pub global_snapshot: &'a crabka_pgmvcc::visibility::Snapshot,
    pub procarray: &'a crate::procarray::ProcArray,
    pub lockmgr: &'a crate::lockmgr::RowLockManager,
    pub lock_owner: crate::lockmgr::LockOwner,
    pub seq: &'a crate::seq::SequenceManager,
    pub snapshot: &'a crabka_pgmvcc::visibility::Snapshot,
    pub xid: u64,
    /// The PostgreSQL command number that owns this write statement.
    pub command_id: u32,
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
    /// Claims owned by the surrounding command when trigger SQL re-enters the
    /// write actor. `None` for normal standalone executor calls.
    pub command_row_claims: Option<&'a CommandRowClaims>,
    pub trigger_write: bool,
    /// The open transaction's deferred referential checks, which the
    /// end-of-statement drain promotes into and `COMMIT` drains.
    ///
    /// `None` in autocommit, where the statement *is* the transaction: nothing
    /// is promoted, because no later statement could repair a violation and the
    /// end-of-statement drain and a commit-time one would report the same thing
    /// at the same moment.
    pub deferred_fk: Option<&'a std::sync::Mutex<crate::fk::DeferredConstraints>>,
    /// The row-security recursion guard for the queries that feed this write.
    pub policy_stack: &'a crate::rls::PolicyStack,
    /// The `WITH CHECK OPTION`s of the views this statement was rewritten
    /// through, empty for a statement that named a relation directly.
    ///
    /// It rides on the write context rather than being threaded as an argument
    /// because the rewrite hands the base-relation statement back to
    /// [`execute_write_body`], and from there the checks have to reach whichever
    /// of the seven write paths that statement takes — every one of which
    /// already builds its per-row check through [`WriteContext::row_check`].
    pub view_checks: &'a [crate::viewwrite::ViewCheck],
    /// The `WHERE` of the views a `MERGE` was rewritten through, restricting
    /// which rows of the relation underneath the statement may match.
    ///
    /// The other three write statements fold the same qual into their own
    /// `WHERE`. A `MERGE` has none, and its `ON` condition is not a substitute:
    /// a target row the view hides would then read as unmatched, and a `WHEN
    /// NOT MATCHED BY SOURCE THEN DELETE` clause would delete a row the
    /// statement could not even see. So it filters the candidate rows instead,
    /// which is where `PostgreSQL` puts it — on the scan of the target.
    ///
    /// It rides here for the reason [`WriteContext::view_checks`] does: the
    /// rewrite hands the base-relation statement back to
    /// [`execute_write_body`], and there is no room in the statement text to
    /// carry it.
    pub merge_target_qual: Option<&'a Expr>,
    /// The relation whose privileges and row-security policies decide this
    /// write, when the rows it touches are stored in a different one.
    ///
    /// `UPDATE parent` over an inheritance tree or a partitioned table writes
    /// each descendant's own storage, but `PostgreSQL` takes both decisions
    /// from the relation the statement *named*. A role holding `UPDATE` on the
    /// parent and nothing at all on a child still writes the child's rows
    /// through the parent, and the parent's policies filter those rows even
    /// when the child has row security disabled — so resolving either against
    /// the relation being physically written denies writes `PostgreSQL` allows
    /// and shows rows it hides.
    ///
    /// The qual this yields is bound by column *name* against whichever
    /// relation is actually being scanned, so a child that stores the parent's
    /// columns in a different order is still filtered on the right ones.
    ///
    /// `None` — the ordinary case — means the written relation governs itself.
    pub governing: Option<&'a Table>,
}

impl<'a> WriteContext<'a> {
    /// The relation whose ACL and policies this write answers to: the one the
    /// statement named, which is the written relation itself unless a tree
    /// write set [`WriteContext::governing`].
    pub(super) fn governor<'b>(&'b self, written: &'b Table) -> &'b Table {
        self.governing.unwrap_or(written)
    }
    /// The read context a write's feeding query runs under: the write's own
    /// snapshot and xid, so it sees this transaction's earlier statements but
    /// not this statement's own (uncommitted, unwritten) rows.
    pub(super) fn read_ctx<'b>(
        &'b self,
        ctes: &'b crate::cte::CteContext,
    ) -> crate::subquery::SubCtx<'b>
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
            command_id: Some(self.command_id),
            ctes,
            eval_ctx: self.eval_ctx,
            fctx: self.fctx,
            range_scanner: self.range_scanner,
            blocking_query_memory: self.blocking_query_memory,
            statement_memory: crate::scanner::StatementMemory::new(self.blocking_query_memory),
            security_role: self.fctx.effective_role(),
            policy_stack: self.policy_stack,
            refs: None,
            explain_plan_state: None,
        }
    }

    /// The read context a *policy qual* of this write executes its subqueries
    /// under.
    ///
    /// [`Self::read_ctx`] with no CTEs, deliberately. A policy qual is catalog
    /// text that was compiled long before this statement was typed, so the
    /// statement's `WITH` names are not in scope for it — a qual that resolved
    /// one would read whatever the caller chose to bind that name to, which is
    /// the caller steering their own policy.
    pub(super) fn policy_read_ctx(&self) -> crate::subquery::SubCtx<'_> {
        static NO_CTES: std::sync::LazyLock<crate::cte::CteContext> =
            std::sync::LazyLock::new(crate::cte::CteContext::empty);
        self.read_ctx(&NO_CTES)
    }

    /// The row-security decision context this write makes its decisions in.
    /// The check a row this statement writes into `table` must satisfy, for the
    /// policy command the statement runs under.
    ///
    /// Also where the privilege to write that row at all is tested. Every write
    /// path already compiles a check here before it writes anything, including
    /// the `INSERT` paths that never reach `write_candidate_rows` — a plain
    /// `INSERT`, `INSERT … SELECT`, a partition-routed insert, `MERGE`'s insert
    /// action, and `COPY … FROM`. Putting the test anywhere else would mean
    /// finding all of them again.
    ///
    /// `modified` names the columns the statement supplied values for, which
    /// only matters if a check option rejects a row and the caller may not read
    /// the relation — see [`crate::rls::describe_row`].
    pub(super) fn row_check(
        &self,
        table: &Table,
        command: crabka_pgcatalog::policy::PolicyCommand,
        modified: &[String],
    ) -> Result<crate::rls::WriteChecks, ExecError> {
        let governor = self.governor(table);
        crate::privilege::require(
            &self.privileges(),
            &governor.name,
            &governor.owner,
            crate::privilege::RelationKind::Table,
            crate::privilege::Privilege::for_written_row(command),
        )?;
        let security = crate::rls::RowSecurityCheck::compile(
            &self.policy_read_ctx(),
            governor,
            command,
            crate::rls::CheckSubject::NewRow,
        )?;
        Ok(crate::rls::WriteChecks::through_views(
            security,
            self.view_checks.to_vec(),
            modified.to_vec(),
            self.describer(),
        ))
    }

    /// The columns `target_idx` names, which is what a statement "supplied
    /// values for" — upstream's `modifiedCols`, by name so the set survives the
    /// permutation of a row into a leaf partition's own column order.
    pub(super) fn modified_columns(table: &Table, target_idx: &[usize]) -> Vec<String> {
        target_idx
            .iter()
            .filter_map(|ordinal| table.columns.get(*ordinal))
            .map(|column| column.name.clone())
            .collect()
    }

    /// Whose eyes a rejected row would be described to — `CURRENT_USER`, not
    /// the role whose rights the statement borrows, for the reason
    /// [`crate::rls::Describer`] gives.
    pub(super) fn describer(&self) -> crate::rls::Describer {
        crate::rls::Describer::seen_by(&self.eval_ctx.current_user, self.fctx.row_security)
    }

    /// The privilege decision context this write makes its decisions in.
    pub(super) fn privileges(&self) -> crate::privilege::PrivilegeCtx<'_> {
        crate::privilege::PrivilegeCtx::new(self.catalog_kv, self.fctx.effective_role())
    }
}

#[derive(Clone, Copy)]
pub(super) struct MvccReadContext<'a> {
    pub(super) kv: &'a dyn Kv,
    pub(super) global: &'a dyn Kv,
    pub(super) global_snapshot: &'a crabka_pgmvcc::visibility::Snapshot,
    pub(super) snapshot: &'a crabka_pgmvcc::visibility::Snapshot,
    pub(super) own: Option<u64>,
    pub(super) command_id: Option<u32>,
}

impl WriteContext<'_> {
    pub(super) fn mvcc_read(&self) -> MvccReadContext<'_> {
        MvccReadContext {
            kv: self.kv,
            global: self.global,
            global_snapshot: self.global_snapshot,
            snapshot: self.snapshot,
            own: Some(self.xid),
            command_id: None,
        }
    }
}

pub(super) struct MutationContext<'a> {
    pub(super) kv: &'a dyn Kv,
    pub(super) global: &'a dyn Kv,
    pub(super) procarray: &'a crate::procarray::ProcArray,
    pub(super) snapshot: &'a crabka_pgmvcc::visibility::Snapshot,
    pub(super) xid: u64,
    /// Command visibility used by a locked re-read.
    ///
    /// A write re-reads after it has staged rows for this command, so it sees
    /// the post-command image rather than a tuple this command has deleted.
    pub(super) command_id: Option<u32>,
    pub(super) repeatable_read: bool,
    /// Carried so that [`eval_plan_qual`] can fill in the virtual generated
    /// columns of the version it re-reads from storage.
    pub(super) eval_ctx: &'a crate::clock::EvalCtx,
}

impl WriteContext<'_> {
    pub(super) fn mutation(&self) -> MutationContext<'_> {
        MutationContext {
            kv: self.kv,
            global: self.global,
            procarray: self.procarray,
            snapshot: self.snapshot,
            xid: self.xid,
            command_id: Some(self.command_id.wrapping_add(1)),
            repeatable_read: self.repeatable_read,
            eval_ctx: self.eval_ctx,
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
    pub(super) fn staged_mutation<'b>(&'b self, staged: &'b StagedKv<'b>) -> MutationContext<'b> {
        MutationContext {
            kv: staged,
            ..self.mutation()
        }
    }
}
