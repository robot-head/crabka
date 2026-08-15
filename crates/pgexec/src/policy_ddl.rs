//! `CREATE`/`ALTER`/`DROP POLICY` — the SQL surface over
//! [`crabka_pgcatalog::policy`].
//!
//! Two rules are enforced here rather than in the catalog, because both need
//! the session:
//!
//! - **Only the relation's owner may touch its policies.** Without this, any
//!   role that can name a relation could `DROP POLICY` on it and read
//!   everything. It is the reason table ownership had to land before policy
//!   DDL was reachable.
//! - **A sharded relation cannot be put under row security.** Its writes go
//!   through the timestamp path, which carries no context to evaluate a policy
//!   qual under, so a policy there would be stored and never enforced — a
//!   silent total bypass. Refusing the DDL is what makes
//!   [`crate::rls::CheckExemption::ShardedRelation`] sound.
//!
//! Quals are stored as the source text the user wrote, not as a serialized
//! expression, so the catalog crate needs no parser; the enforcement path
//! re-parses per statement, so a policy never carries a stale plan, and
//! `pg_policy.polqual` re-parses and deparses, so what the catalog reports is
//! PostgreSQL's normalized rendering rather than the text. The parser hands
//! both forms over ([`ast::PolicyQual`]), so the text stored and the expression
//! enforced cannot disagree.

use crabka_pgcatalog::{
    RelationName, Table,
    policy::{Policy, PolicyChange, PolicyCommand},
};
use crabka_pgkv::{Kv, WriteOp};
use crabka_pgparser::ast;
use crabka_pgwire::engine::QueryResult;

use crate::{error::ExecError, exec::ForeignCtx};

type DdlResult = Result<(QueryResult, Vec<WriteOp>), ExecError>;

fn command(tag: &str) -> QueryResult {
    QueryResult::Command { tag: tag.into() }
}

/// The relation a policy statement names, checked for the two things policy DDL
/// requires of it: the session owns it, and it is one row security can reach.
fn policy_target(
    kv: &dyn Kv,
    name: &RelationName,
    fctx: ForeignCtx<'_>,
) -> Result<Table, ExecError> {
    let table = crabka_pgcatalog::get_table(kv, name)?;
    require_owner(kv, &table, fctx)?;
    crate::rls::refuse_sharded_row_security(&table)?;
    Ok(table)
}

/// `PostgreSQL`'s ownership test for a relation's policies, spelled the way it
/// spells it.
///
/// Membership rather than string equality: a member of an owning group owns the
/// relation for this purpose. `role_has_privs_of` is the predicate `PostgreSQL`
/// matches with — `role_can_set` counts roles a session may merely `SET ROLE`
/// to, which is a wider set and would hand policy control to more roles than
/// own the relation.
pub(crate) fn require_owner(
    kv: &dyn Kv,
    table: &Table,
    fctx: ForeignCtx<'_>,
) -> Result<(), ExecError> {
    crate::privilege::require_ownership(
        kv,
        &table.name,
        &table.owner,
        crate::privilege::RelationKind::Table,
        fctx.effective_role(),
    )
}

const fn catalog_command(command: ast::PolicyCommand) -> PolicyCommand {
    match command {
        ast::PolicyCommand::All => PolicyCommand::All,
        ast::PolicyCommand::Select => PolicyCommand::Select,
        ast::PolicyCommand::Insert => PolicyCommand::Insert,
        ast::PolicyCommand::Update => PolicyCommand::Update,
        ast::PolicyCommand::Delete => PolicyCommand::Delete,
    }
}

/// Every role a `TO` list names must exist, and `CURRENT_USER`/`SESSION_USER`
/// resolve to the session's role the way they do in an owner position.
///
/// An empty list is `PUBLIC` and is stored empty.
fn resolve_roles(
    kv: &dyn Kv,
    roles: &[String],
    fctx: ForeignCtx<'_>,
) -> Result<Vec<String>, ExecError> {
    let mut resolved = Vec::with_capacity(roles.len());
    for role in roles {
        let role = match role.as_str() {
            "current_user" | "user" | "current_role" | "session_user" => {
                fctx.effective_role().to_string()
            }
            named => named.to_string(),
        };
        if !crabka_pgcatalog::role_exists(kv, &role)? {
            return Err(ExecError::UndefinedObject(format!(
                "role \"{role}\" does not exist"
            )));
        }
        resolved.push(role);
    }
    Ok(resolved)
}

/// A qual is validated at creation, not first enforced, so a policy that can
/// never be evaluated is a `CREATE POLICY` error rather than a surprise on some
/// later `SELECT`.
fn validate_qual(qual: &ast::PolicyQual) -> Result<(), ExecError> {
    crate::rls::validate_policy_qual(&qual.source)
}

fn qual_source(qual: Option<&ast::PolicyQual>) -> Result<Option<String>, ExecError> {
    match qual {
        Some(qual) => {
            validate_qual(qual)?;
            Ok(Some(qual.source.clone()))
        }
        None => Ok(None),
    }
}

/// `CREATE POLICY name ON table …`.
///
/// # Errors
///
/// Returns 42501 when the session does not own the relation, 42710 when the
/// relation already carries a policy of that name, 0A000 for a qual row
/// security cannot yet evaluate safely, and catalog errors.
pub(crate) fn create(
    kv: &dyn Kv,
    policy: &ast::CreatePolicy,
    name: RelationName,
    fctx: ForeignCtx<'_>,
) -> DdlResult {
    let table = policy_target(kv, &name, fctx)?;
    // PostgreSQL rejects WITH CHECK on a policy for a command that writes no
    // row, rather than storing a clause nothing will ever read.
    if policy.with_check.is_some()
        && matches!(
            policy.command,
            ast::PolicyCommand::Select | ast::PolicyCommand::Delete
        )
    {
        return Err(ExecError::Syntax(format!(
            "WITH CHECK cannot be applied to {} policies",
            match policy.command {
                ast::PolicyCommand::Select => "SELECT",
                _ => "DELETE",
            }
        )));
    }
    let record = Policy {
        oid: 0,
        name: policy.name.clone(),
        table_id: table.id,
        command: catalog_command(policy.command),
        permissive: policy.permissive,
        roles: resolve_roles(kv, &policy.roles, fctx)?,
        using: qual_source(policy.using.as_ref())?,
        with_check: qual_source(policy.with_check.as_ref())?,
    };
    let ops = crabka_pgcatalog::policy::create_policy_ops(kv, &record)?;
    Ok((command("CREATE POLICY"), ops))
}

/// `ALTER POLICY name ON table {RENAME TO … | [TO …] [USING …] [WITH CHECK …]}`.
///
/// # Errors
///
/// Returns 42501 when the session does not own the relation, 42704 when no
/// policy of that name is attached to it, 42710 on a rename onto a taken name,
/// and catalog errors.
pub(crate) fn alter(
    kv: &dyn Kv,
    name: &str,
    relation: RelationName,
    action: &ast::AlterPolicyAction,
    fctx: ForeignCtx<'_>,
) -> DdlResult {
    let table = policy_target(kv, &relation, fctx)?;
    let ops = match action {
        ast::AlterPolicyAction::RenameTo(new_name) => {
            crabka_pgcatalog::policy::rename_policy_ops(kv, table.id, name, new_name)?
        }
        ast::AlterPolicyAction::Change(change) => {
            let ast::AlterPolicyChange {
                roles,
                using,
                with_check,
            } = change.as_ref();
            let change = PolicyChange {
                roles: roles
                    .as_ref()
                    .map(|roles| resolve_roles(kv, roles, fctx))
                    .transpose()?,
                using: qual_source(using.as_ref())?,
                with_check: qual_source(with_check.as_ref())?,
            };
            crabka_pgcatalog::policy::alter_policy_ops(kv, table.id, name, &change)?
        }
    };
    Ok((command("ALTER POLICY"), ops))
}

/// `DROP POLICY [IF EXISTS] name ON table`.
///
/// # Errors
///
/// Returns 42501 when the session does not own the relation, 42704 when no
/// policy of that name is attached to it and `IF EXISTS` was not written, and
/// catalog errors.
pub(crate) fn drop(
    kv: &dyn Kv,
    name: &str,
    relation: RelationName,
    if_exists: bool,
    fctx: ForeignCtx<'_>,
) -> DdlResult {
    // `DROP POLICY IF EXISTS` on a relation that does not exist is a notice in
    // PostgreSQL too, so the missing relation is answered before the missing
    // policy — and before the ownership test, which has nothing to test.
    let table = match crabka_pgcatalog::get_table(kv, &relation) {
        Ok(table) => table,
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) if if_exists => {
            return Ok((command("DROP POLICY"), Vec::new()));
        }
        Err(error) => return Err(error.into()),
    };
    require_owner(kv, &table, fctx)?;
    match crabka_pgcatalog::policy::drop_policy_ops(kv, table.id, name) {
        Ok(ops) => Ok((command("DROP POLICY"), ops)),
        Err(crabka_pgcatalog::CatalogError::UndefinedPolicy { .. }) if if_exists => {
            Ok((command("DROP POLICY"), Vec::new()))
        }
        Err(error) => Err(error.into()),
    }
}
