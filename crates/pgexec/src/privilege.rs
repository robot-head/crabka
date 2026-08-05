//! Table privileges: the one place a `GRANT` is turned into a yes or a no.
//!
//! `GRANT`/`REVOKE` have written catalog rows since long before anything read
//! them, and the whole `has_*_privilege` family answered `true` unconditionally.
//! This module is what makes those rows mean something.
//!
//! # Where the check happens
//!
//! Not at N call sites. Row security learned that lesson first, and this module
//! borrows its seams rather than opening new ones:
//!
//! - **reads** — [`ReadPermit`] is a required argument of
//!   `exec::scan_stored_relation`, the single function every stored-relation
//!   read funnels through, and the only way to obtain one is
//!   [`ReadPermit::acquire`], which performs the check. A new scan path either
//!   asks for the permission or does not compile.
//! - **writes** — `exec::write_candidate_rows` already gathers the rows an
//!   `UPDATE`/`DELETE`/`MERGE` may act on and already decides the row-security
//!   `USING` qual there. It now takes a [`WriteAction`] in place of a bare
//!   `PolicyCommand`, so naming the command *is* naming the privilege.
//! - **inserted rows** — `WriteContext::row_check`, which every write path
//!   already calls to compile its `WITH CHECK`.
//!
//! # What bypasses a missing grant
//!
//! [`holds`] lists every bypass and nothing else does, in `PostgreSQL`'s order:
//! the superuser, the relation's owner, a grant to the role, a grant to
//! `PUBLIC`, and a grant to a role whose privileges the role holds. That last
//! one is matched with [`crabka_pgcatalog::role_has_privs_of`] — *not*
//! `role_can_set`, which counts memberships granted `WITH INHERIT FALSE` and so
//! would hand a role privileges it must `SET ROLE` to use. It is the same
//! predicate a row-security policy's `TO` list is matched with, for the same
//! reason.
//!
//! # What stays readable
//!
//! Catalog and `information_schema` relations. They are virtual here — they
//! never reach [`ReadPermit::acquire`] because `build_base_table` answers them
//! before it looks for a stored relation — which lands in the same place
//! `PostgreSQL` does, where they carry a `PUBLIC` `SELECT` grant.

use crabka_pgcatalog::{RelationName, Table};
use crabka_pgkv::Kv;

use crate::error::ExecError;

/// One privilege on one relation, spelled the way the catalog stores it and
/// named the way `PostgreSQL`'s errors name the command that needs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Truncate,
}

impl Privilege {
    /// The privilege that authorizes writing one row under `command`.
    ///
    /// Only `INSERT` and `UPDATE` compose a row to write; the other policy
    /// commands never reach a write-side check, and mapping them onto `SELECT`
    /// is the fail-closed answer for a caller that finds a way to.
    pub(crate) const fn for_written_row(command: crabka_pgcatalog::policy::PolicyCommand) -> Self {
        use crabka_pgcatalog::policy::PolicyCommand;
        match command {
            PolicyCommand::Insert => Self::Insert,
            PolicyCommand::Update => Self::Update,
            PolicyCommand::Delete => Self::Delete,
            PolicyCommand::All | PolicyCommand::Select => Self::Select,
        }
    }

    /// The catalog's spelling, which is uppercase.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Truncate => "TRUNCATE",
        }
    }
}

/// What `PostgreSQL` calls the relation in `permission denied for …`.
///
/// A view says `view` and a table says `table`; getting this wrong is a
/// one-word diff on every denial the upstream corpus expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelationKind {
    Table,
    View,
}

impl RelationKind {
    pub(crate) const fn noun(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::View => "view",
        }
    }
}

/// Everything a privilege decision reads, in one borrowed value.
///
/// Deliberately the same shape as [`crate::rls::RlsCtx`] minus the GUC: the
/// catalog handle and the role being judged. A unit test can build one over a
/// bare [`crabka_pgkv::MemKv`].
#[derive(Clone, Copy)]
pub(crate) struct PrivilegeCtx<'a> {
    catalog_kv: &'a dyn Kv,
    /// The role whose grants decide this. Ordinarily the session's
    /// `current_user`; a session that never authenticated resolves this to the
    /// bootstrap superuser through `ForeignCtx::effective_role`, which is what
    /// keeps an unauthenticated session unaffected by this whole module.
    role: &'a str,
}

impl<'a> PrivilegeCtx<'a> {
    pub(crate) const fn new(catalog_kv: &'a dyn Kv, role: &'a str) -> Self {
        Self { catalog_kv, role }
    }
}

/// Whether `role` may exercise `privilege` on `relation`, owned by `owner`.
///
/// Every bypass is here and nowhere else, so the set of ways to reach a
/// relation without a grant is one function long.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub(crate) fn holds(
    ctx: &PrivilegeCtx<'_>,
    relation: &RelationName,
    owner: &str,
    privilege: Privilege,
) -> Result<bool, ExecError> {
    holds_named(ctx, relation, owner, privilege.name())
}

/// The same decision for a privilege named as the catalog stores it.
///
/// Split out for `has_*_privilege`, which must answer about the privileges a
/// `GRANT` can write (`REFERENCES`, `TRIGGER`, `MAINTAIN`) and not only the five
/// this module gates a command on. Enforcement keeps the typed
/// [`Privilege`] so no command can be gated on a name that was never granted.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub(crate) fn holds_named(
    ctx: &PrivilegeCtx<'_>,
    relation: &RelationName,
    owner: &str,
    privilege: &str,
) -> Result<bool, ExecError> {
    // The superuser holds every privilege on every object and has no grant rows
    // to find. `role_is_superuser` rather than the row-security exemption test:
    // `BYPASSRLS` lets a role see past a policy, which is a different power from
    // being allowed to read a relation at all.
    if crate::rls::role_is_superuser(ctx.catalog_kv, ctx.role)? {
        return Ok(true);
    }
    // The owner holds every privilege on its own relation implicitly — there is
    // no self-grant row, and `GRANT`ing to yourself is a no-op in PostgreSQL.
    // Membership, not string equality: a member of an owning group owns the
    // relation for this purpose.
    if crabka_pgcatalog::role_has_privs_of(ctx.catalog_kv, ctx.role, owner)? {
        return Ok(true);
    }
    // A grant naming the role itself, and a grant to PUBLIC, are both point
    // lookups on an exact key.
    for grantee in [ctx.role, crabka_pgcatalog::PUBLIC_ROLE] {
        if crabka_pgcatalog::has_stored_table_privilege(
            ctx.catalog_kv,
            relation,
            grantee,
            privilege,
        )? {
            return Ok(true);
        }
    }
    // Finally, a grant to some role whose privileges this one holds. Scanning
    // the relation's own grants and testing each grantee costs the relation's
    // grant count; enumerating every role in the cluster and probing each would
    // cost the cluster's role count, and this runs per statement per relation.
    for granted in crabka_pgcatalog::table_privileges_of(ctx.catalog_kv, relation)? {
        if granted.privilege == privilege
            && crabka_pgcatalog::role_has_privs_of(ctx.catalog_kv, ctx.role, &granted.grantee)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Raise `PostgreSQL`'s 42501 unless the role holds `privilege` on the relation.
///
/// # Errors
///
/// Returns 42501 when the privilege is not held, or storage/corruption errors
/// from the catalog KV seam.
pub(crate) fn require(
    ctx: &PrivilegeCtx<'_>,
    relation: &RelationName,
    owner: &str,
    kind: RelationKind,
    privilege: Privilege,
) -> Result<(), ExecError> {
    if holds(ctx, relation, owner, privilege)? {
        return Ok(());
    }
    Err(ExecError::PermissionDenied {
        kind: kind.noun(),
        relation: relation.name.clone(),
    })
}

/// Raise `PostgreSQL`'s `must be owner of <kind> <name>` unless `role` owns the
/// relation.
///
/// Ownership, not a privilege: no `GRANT` confers it, and the only two ways
/// past it are membership in the owning role and being the superuser. It sits
/// here beside [`holds`] so the set of ways to pass an ownership test is as
/// short and as visible as the set of ways to pass a privilege test.
///
/// # Errors
///
/// Returns 42501 when the role does not own the relation, or
/// storage/corruption errors from the catalog KV seam.
pub(crate) fn require_ownership(
    kv: &dyn Kv,
    relation: &RelationName,
    owner: &str,
    kind: RelationKind,
    role: &str,
) -> Result<(), ExecError> {
    if crabka_pgcatalog::role_has_privs_of(kv, role, owner)?
        || crate::rls::role_is_superuser(kv, role)?
    {
        return Ok(());
    }
    Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
        "42501",
        format!("must be owner of {} {}", kind.noun(), relation.name),
    )))
}

/// Proof that the session may read the rows of one stored relation.
///
/// A token rather than a bare call, for the reason [`crate::rls::RawScan`] is a
/// type rather than a `Vec`: `exec::scan_stored_relation` takes one, and
/// [`Self::acquire`] is the only way to make one, so a scan path that has not
/// asked whether the session may read the relation cannot be written. The
/// alternative — a `require` call the author of the next scan path has to
/// remember — is the failure mode both this module and row security exist to
/// design out.
pub(crate) struct ReadPermit {
    /// Deliberately not `()`: a unit struct with public construction would let
    /// a caller conjure the proof it is supposed to earn.
    _private: (),
}

impl ReadPermit {
    /// Check `SELECT` on `table` and, if it is held, admit the read.
    ///
    /// # Errors
    ///
    /// Returns 42501 when the session may not read `table`, or
    /// storage/corruption errors from the catalog KV seam.
    pub(crate) fn acquire(ctx: &PrivilegeCtx<'_>, table: &Table) -> Result<Self, ExecError> {
        require(
            ctx,
            &table.name,
            &table.owner,
            RelationKind::Table,
            Privilege::Select,
        )?;
        Ok(Self { _private: () })
    }

    /// The permit, or `None` when the session may not read `table`.
    ///
    /// For a caller whose right answer to a denial is to decline rather than to
    /// raise — an optimizer fast path, whose fallback is the ordinary read that
    /// raises the denial itself. Using this where [`Self::acquire`] belongs
    /// turns a denial into a slower correct answer, never into an admitted one.
    ///
    /// # Errors
    ///
    /// Returns storage/corruption errors from the catalog KV seam.
    pub(crate) fn offer(ctx: &PrivilegeCtx<'_>, table: &Table) -> Result<Option<Self>, ExecError> {
        holds(ctx, &table.name, &table.owner, Privilege::Select)
            .map(|held| held.then_some(Self { _private: () }))
    }

    /// The permit a child of an inheritance or partition tree is read under:
    /// its parent's.
    ///
    /// `PostgreSQL` checks the ACL of the relation the query named and none of
    /// the descendants it expands to, so `SELECT * FROM parent` reads a child
    /// the session was never granted, and `SELECT * FROM child` still fails.
    /// Handing the parent's own permit down is how that rule is stated here —
    /// there is no second check to accidentally add.
    ///
    /// The parent permit is required and never read: a tree scan that has not
    /// been given one cannot make one up.
    pub(crate) const fn inherited(_parent: &Self) -> Self {
        Self { _private: () }
    }
}

/// What a row-changing statement is doing to its target.
///
/// This replaced a bare [`crabka_pgcatalog::policy::PolicyCommand`] parameter on
/// `exec::write_candidate_rows` rather than sitting beside it. Two facts vary
/// together — which policies row security applies, and which privilege the
/// session must hold — and a caller given two parameters can pair them wrongly.
/// `TRUNCATE` is the case that proves it: it desugars to a per-relation
/// `DELETE`, so it meets `DELETE` policies while needing the `TRUNCATE`
/// privilege and not `DELETE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteAction {
    /// `UPDATE`. Needs `UPDATE`, plus `SELECT` when it reads the target's
    /// columns.
    Update,
    /// `DELETE`. Needs `DELETE`, plus `SELECT` when it reads the target's
    /// columns.
    Delete,
    /// The per-relation `DELETE` a `TRUNCATE` desugars to. Needs `TRUNCATE`
    /// alone: the privilege was checked on the whole truncate set before the
    /// first relation was touched, and `TRUNCATE` reads no column.
    Truncate,
    /// `MERGE`. Needs `UPDATE` and `SELECT` — `UPDATE` because it is the
    /// narrower of the two commands a matched row can meet (a row this
    /// statement cannot update it also cannot delete), and `SELECT`
    /// unconditionally because its `ON` condition reads the target.
    Merge,
}

impl WriteAction {
    /// The policy command row security applies to the rows this action gathers.
    pub(crate) const fn policy_command(self) -> crabka_pgcatalog::policy::PolicyCommand {
        use crabka_pgcatalog::policy::PolicyCommand;
        match self {
            Self::Update | Self::Merge => PolicyCommand::Update,
            Self::Delete | Self::Truncate => PolicyCommand::Delete,
        }
    }

    /// The privileges this action needs on its target, given whether the
    /// statement reads any of the target's own columns.
    ///
    /// `reads_columns` is the caller's answer to `PostgreSQL`'s `selectedCols`
    /// question, and it is per *relation*: `UPDATE a SET c = true FROM b WHERE
    /// b.x = 5` reads a column of `b` and none of `a`, so it needs `SELECT` on
    /// `b` (which `b`'s own read gate demands) and not on `a`.
    fn privileges(self, reads_columns: bool) -> Vec<Privilege> {
        let (needed, may_read) = match self {
            Self::Update => (Privilege::Update, true),
            Self::Delete => (Privilege::Delete, true),
            Self::Truncate => (Privilege::Truncate, false),
            Self::Merge => (Privilege::Update, true),
        };
        let mut privileges = vec![needed];
        if may_read && (reads_columns || self == Self::Merge) {
            privileges.push(Privilege::Select);
        }
        privileges
    }
}

/// Check every privilege a row-changing statement needs on its target.
///
/// # Errors
///
/// Returns 42501 for the first privilege the session does not hold, or
/// storage/corruption errors from the catalog KV seam.
pub(crate) fn require_write(
    ctx: &PrivilegeCtx<'_>,
    table: &Table,
    action: WriteAction,
    reads_columns: bool,
) -> Result<(), ExecError> {
    for privilege in action.privileges(reads_columns) {
        require(
            ctx,
            &table.name,
            &table.owner,
            RelationKind::Table,
            privilege,
        )?;
    }
    Ok(())
}

/// Whether an `UPDATE` or `DELETE` reads any column of its own target.
///
/// This is the `reads_columns` input to [`require_write`], and it is why
/// `UPDATE atest2 SET col2 = true FROM atest1 WHERE atest1.a = 5` succeeds for a
/// role holding only `UPDATE` on `atest2`: the `WHERE` reads `atest1`, not the
/// target, and `atest1`'s own read gate is what demands `SELECT` there. A rule
/// as coarse as "the statement has a `WHERE`" fails that case, which the
/// upstream corpus tests directly.
///
/// Everything a statement can read the target through is enumerated here — its
/// filter, its `RETURNING` list, and its assignment right-hand sides — rather
/// than at each caller, so a clause added later is added once.
pub(crate) fn dml_reads_target(
    table: &Table,
    qualifier: &str,
    filter: Option<&crabka_pgparser::ast::Expr>,
    returning: Option<&crabka_pgparser::ast::Returning>,
    assignments: &[crabka_pgparser::ast::Assignment],
) -> bool {
    use crabka_pgparser::ast::{AssignmentValue, SelectItem};
    let reads = |expr| expr_reads_relation(table, qualifier, expr);
    if filter.is_some_and(reads) {
        return true;
    }
    let returned = returning.is_some_and(|returning| {
        returning.items.iter().any(|item| match item {
            // `*` and `t.*` name every column the target has; a qualified one
            // that names some other relation is that relation's business.
            SelectItem::Wildcard => true,
            SelectItem::QualifiedWildcard(named) => named.eq_ignore_ascii_case(qualifier),
            SelectItem::Expr { expr, .. } => reads(expr),
        })
    });
    if returned {
        return true;
    }
    assignments.iter().any(|assignment| {
        // A subscripted target reads the column it updates in place: `SET
        // j['a'] = e` keeps the rest of the stored value.
        !assignment.subscripts.is_empty()
            || match &assignment.value {
                AssignmentValue::Expr(expr) => reads(expr),
                AssignmentValue::Row(exprs) => exprs.iter().any(reads),
                // A sub-select's own `FROM` is gated by its own read of
                // whatever it names; it correlates to the target only through
                // expressions this walk already descends into.
                AssignmentValue::Subquery(_) => false,
            }
    })
}

/// Whether `expr` reads a column of the relation bound to `qualifier`.
///
/// A bare column name counts when the target declares it. That over-counts a
/// name the target shares with a joined relation — an ambiguous reference,
/// which the executor rejects anyway — and never under-counts, which is the
/// direction a privilege test should err in.
fn expr_reads_relation(table: &Table, qualifier: &str, expr: &crabka_pgparser::ast::Expr) -> bool {
    use crabka_pgparser::ast::Expr;
    match expr {
        Expr::Column {
            table: Some(named), ..
        } => named.eq_ignore_ascii_case(qualifier),
        Expr::Column { table: None, name } => table.column_index(name).is_some(),
        _ => crate::exec::expr_children(expr)
            .into_iter()
            .any(|child| expr_reads_relation(table, qualifier, child)),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::{Column, RelationName, RoleAttribute, RoleAttributes, Table};
    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgtypes::ColumnType;

    use super::{
        Privilege, PrivilegeCtx, ReadPermit, RelationKind, WriteAction, dml_reads_target, holds,
        require, require_write,
    };

    const OWNER: &str = "owner_role";

    fn table() -> Table {
        Table {
            id: 42,
            name: RelationName::new("public", "document"),
            owner: OWNER.into(),
            columns: vec![
                Column::new("id", ColumnType::Int4),
                Column::new("body", ColumnType::Text),
            ],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        }
    }

    /// A catalog holding `document`, its owner, and whatever extra roles and
    /// memberships a case needs.
    fn store(roles: &[(&str, RoleAttributes, &[&str])]) -> MemKv {
        let kv = MemKv::new();
        let table = table();
        let (_, ops) = crabka_pgcatalog::create_table_with_options_ops(
            &kv,
            &table.name,
            table.columns.clone(),
            crabka_pgcatalog::TableOptions::default(),
            Vec::new(),
            crabka_pgcatalog::TableCreation {
                owner: OWNER,
                id: crabka_pgcatalog::TableIdSource::Counter,
            },
        )
        .expect("create table");
        kv.write_batch(&ops).expect("apply");
        for (name, attributes, member_of) in
            std::iter::once(&(OWNER, RoleAttributes::default(), &[] as &[&str])).chain(roles)
        {
            let member_of: Vec<String> = member_of.iter().map(|role| (*role).to_string()).collect();
            let ops = crabka_pgcatalog::create_role_with_memberships_ops(
                &kv,
                name,
                true,
                *attributes,
                &member_of,
            )
            .expect("create role");
            kv.write_batch(&ops).expect("apply");
        }
        kv
    }

    fn grant(kv: &MemKv, grantee: &str, privileges: &[&str]) {
        let privileges: Vec<String> = privileges.iter().map(|p| (*p).to_string()).collect();
        let ops = crabka_pgcatalog::grant_table_privileges_ops(
            kv,
            &table().name,
            &[grantee.to_string()],
            &privileges,
        )
        .expect("grant");
        kv.write_batch(&ops).expect("apply");
    }

    fn superuser() -> RoleAttributes {
        let mut attributes = RoleAttributes::default();
        attributes.set(RoleAttribute::Superuser, true);
        attributes
    }

    /// Every way a role reaches a relation, and the one way it does not.
    #[test]
    fn bypass_matrix() {
        struct Case {
            name: &'static str,
            role: &'static str,
            granted_to: Option<&'static str>,
            expected: bool,
        }
        let cases = [
            Case {
                name: "a stranger with no grant is denied",
                role: "stranger",
                granted_to: None,
                expected: false,
            },
            Case {
                name: "the owner needs no grant",
                role: OWNER,
                granted_to: None,
                expected: true,
            },
            Case {
                name: "a member of the owning role owns it too",
                role: "heir",
                granted_to: None,
                expected: true,
            },
            Case {
                name: "the superuser needs no grant",
                role: "root",
                granted_to: None,
                expected: true,
            },
            Case {
                name: "a grant naming the role admits it",
                role: "stranger",
                granted_to: Some("stranger"),
                expected: true,
            },
            Case {
                name: "a grant to PUBLIC admits every role",
                role: "stranger",
                granted_to: Some("public"),
                expected: true,
            },
            Case {
                name: "a grant to a role whose privileges this one holds admits it",
                role: "reader",
                granted_to: Some("readers"),
                expected: true,
            },
            Case {
                name: "a grant to an unrelated role does not admit it",
                role: "reader",
                granted_to: Some("writers"),
                expected: false,
            },
        ];
        for case in cases {
            let kv = store(&[
                ("stranger", RoleAttributes::default(), &[]),
                ("heir", RoleAttributes::default(), &[OWNER]),
                ("root", superuser(), &[]),
                ("readers", RoleAttributes::default(), &[]),
                ("writers", RoleAttributes::default(), &[]),
                ("reader", RoleAttributes::default(), &["readers"]),
            ]);
            if let Some(grantee) = case.granted_to {
                grant(&kv, grantee, &["SELECT"]);
            }
            let ctx = PrivilegeCtx::new(&kv, case.role);
            let held = holds(&ctx, &table().name, OWNER, Privilege::Select).expect("decide");
            assert!(held == case.expected, "{}", case.name);
        }
    }

    /// A grant of one privilege is not a grant of another.
    #[test]
    fn a_grant_is_per_privilege() {
        let kv = store(&[("stranger", RoleAttributes::default(), &[])]);
        grant(&kv, "stranger", &["SELECT"]);
        let ctx = PrivilegeCtx::new(&kv, "stranger");
        for (privilege, expected) in [
            (Privilege::Select, true),
            (Privilege::Insert, false),
            (Privilege::Update, false),
            (Privilege::Delete, false),
            (Privilege::Truncate, false),
        ] {
            assert!(holds(&ctx, &table().name, OWNER, privilege).expect("decide") == expected);
        }
    }

    /// `GRANT ALL` reaches every command.
    #[test]
    fn grant_all_covers_every_command() {
        let kv = store(&[("stranger", RoleAttributes::default(), &[])]);
        grant(&kv, "stranger", &["ALL"]);
        let ctx = PrivilegeCtx::new(&kv, "stranger");
        for privilege in [
            Privilege::Select,
            Privilege::Insert,
            Privilege::Update,
            Privilege::Delete,
            Privilege::Truncate,
        ] {
            assert!(holds(&ctx, &table().name, OWNER, privilege).expect("decide"));
        }
    }

    /// The denial `PostgreSQL` spells, for both nouns.
    #[test]
    fn denial_message_and_sqlstate() {
        let kv = store(&[("stranger", RoleAttributes::default(), &[])]);
        let ctx = PrivilegeCtx::new(&kv, "stranger");
        for (kind, expected) in [
            (RelationKind::Table, "permission denied for table document"),
            (RelationKind::View, "permission denied for view document"),
        ] {
            let error = require(&ctx, &table().name, OWNER, kind, Privilege::Select)
                .expect_err("a stranger holds nothing");
            let reported = error.into_pg();
            assert!(reported.code == "42501");
            assert!(reported.message == expected);
        }
    }

    /// A read permit cannot be had without the `SELECT` privilege, and a child
    /// of a tree rides the parent's.
    #[test]
    fn read_permit_requires_select() {
        let kv = store(&[("stranger", RoleAttributes::default(), &[])]);
        let ctx = PrivilegeCtx::new(&kv, "stranger");
        assert!(ReadPermit::acquire(&ctx, &table()).is_err());
        grant(&kv, "stranger", &["SELECT"]);
        let permit = ReadPermit::acquire(&ctx, &table()).expect("granted");
        let _child = ReadPermit::inherited(&permit);
    }

    /// Which privileges each write action demands, and the `SELECT` a statement
    /// picks up by reading its target's columns.
    #[test]
    fn write_actions_demand_their_privileges() {
        struct Case {
            name: &'static str,
            action: WriteAction,
            reads_columns: bool,
            /// The grants that make the statement succeed, minimally.
            sufficient: &'static [&'static str],
            /// A grant set that is one privilege short.
            insufficient: &'static [&'static str],
        }
        let cases = [
            Case {
                name: "UPDATE with a constant assignment needs only UPDATE",
                action: WriteAction::Update,
                reads_columns: false,
                sufficient: &["UPDATE"],
                insufficient: &["SELECT"],
            },
            Case {
                name: "UPDATE reading its target needs SELECT as well",
                action: WriteAction::Update,
                reads_columns: true,
                sufficient: &["UPDATE", "SELECT"],
                insufficient: &["UPDATE"],
            },
            Case {
                name: "DELETE without a filter needs only DELETE",
                action: WriteAction::Delete,
                reads_columns: false,
                sufficient: &["DELETE"],
                insufficient: &["SELECT"],
            },
            Case {
                name: "DELETE reading its target needs SELECT as well",
                action: WriteAction::Delete,
                reads_columns: true,
                sufficient: &["DELETE", "SELECT"],
                insufficient: &["DELETE"],
            },
            Case {
                name: "TRUNCATE needs TRUNCATE and never DELETE",
                action: WriteAction::Truncate,
                reads_columns: false,
                sufficient: &["TRUNCATE"],
                insufficient: &["DELETE", "SELECT"],
            },
            Case {
                name: "MERGE always needs UPDATE and SELECT",
                action: WriteAction::Merge,
                reads_columns: false,
                sufficient: &["UPDATE", "SELECT"],
                insufficient: &["UPDATE"],
            },
        ];
        for case in cases {
            for (grants, should_pass) in [(case.sufficient, true), (case.insufficient, false)] {
                let kv = store(&[("stranger", RoleAttributes::default(), &[])]);
                grant(&kv, "stranger", grants);
                let ctx = PrivilegeCtx::new(&kv, "stranger");
                let outcome =
                    require_write(&ctx, &table(), case.action, case.reads_columns).is_ok();
                assert!(outcome == should_pass, "{}", case.name);
            }
        }
    }

    /// Row security and privileges must ask the same question of a write, so a
    /// `TRUNCATE` still meets `DELETE` policies while needing `TRUNCATE`.
    #[test]
    fn write_actions_map_to_policy_commands() {
        use crabka_pgcatalog::policy::PolicyCommand;
        for (action, command) in [
            (WriteAction::Update, PolicyCommand::Update),
            (WriteAction::Delete, PolicyCommand::Delete),
            (WriteAction::Truncate, PolicyCommand::Delete),
            (WriteAction::Merge, PolicyCommand::Update),
        ] {
            assert!(action.policy_command() == command);
        }
    }

    /// The `selectedCols` test, taken from whole statements so the clauses it
    /// walks are the ones a real `UPDATE` or `DELETE` carries.
    ///
    /// The case that matters most is `UPDATE … SET c = true FROM other WHERE
    /// other.a = 5`: it has a `WHERE` and still reads none of its own target's
    /// columns, so it must not demand `SELECT` on the target.
    #[test]
    fn target_column_reads() {
        use crabka_pgparser::ast::Statement;
        struct Case {
            name: &'static str,
            sql: &'static str,
            reads: bool,
        }
        let cases = [
            Case {
                name: "a constant assignment reads nothing",
                sql: "UPDATE document SET body = 'x'",
                reads: false,
            },
            Case {
                name: "a self-referencing assignment reads the target",
                sql: "UPDATE document SET id = id + 1",
                reads: true,
            },
            Case {
                name: "a filter on the target reads it",
                sql: "UPDATE document SET body = 'x' WHERE id = 1",
                reads: true,
            },
            Case {
                name: "a filter qualified by the target reads it",
                sql: "UPDATE document SET body = 'x' WHERE document.id = 1",
                reads: true,
            },
            Case {
                name: "a filter that reads only a joined relation does not",
                sql: "UPDATE document SET body = 'x' FROM other WHERE other.a = 5",
                reads: false,
            },
            Case {
                name: "RETURNING a target column reads it",
                sql: "UPDATE document SET body = 'x' RETURNING id",
                reads: true,
            },
            Case {
                name: "RETURNING * reads the target",
                sql: "UPDATE document SET body = 'x' RETURNING *",
                reads: true,
            },
            Case {
                name: "RETURNING a constant reads nothing",
                sql: "UPDATE document SET body = 'x' RETURNING 1",
                reads: false,
            },
            Case {
                name: "an unfiltered DELETE reads nothing",
                sql: "DELETE FROM document",
                reads: false,
            },
            Case {
                name: "a filtered DELETE reads the target",
                sql: "DELETE FROM document WHERE id = 1",
                reads: true,
            },
            Case {
                name: "a nested reference is still a read",
                sql: "DELETE FROM document WHERE coalesce(id, 0) > 0",
                reads: true,
            },
        ];
        for case in cases {
            let parsed = crabka_pgparser::parse(case.sql).expect("parse");
            let (filter, returning, assignments) = match parsed.as_slice() {
                [
                    Statement::Update {
                        filter,
                        returning,
                        assignments,
                        ..
                    },
                ] => (filter.as_ref(), returning.as_ref(), assignments.as_slice()),
                [
                    Statement::Delete {
                        filter, returning, ..
                    },
                ] => (filter.as_ref(), returning.as_ref(), [].as_slice()),
                other => panic!("{}: unexpected parse {other:?}", case.name),
            };
            let read = dml_reads_target(&table(), "document", filter, returning, assignments);
            assert!(read == case.reads, "{}", case.name);
        }
    }
}
