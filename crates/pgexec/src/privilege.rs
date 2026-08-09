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
//!
//! # Who may change the roles themselves
//!
//! [`require_role_create`], [`require_role_alter`], [`require_role_drop`] and
//! [`require_role_grant`] judge a role against the *role* catalog rather than
//! against one relation's grants. They belong here because they decide who the
//! rest of this module is even asked about: a session that can write a role
//! record can give itself `SUPERUSER`, or grant itself into a table's owning
//! role, and walk past every other gate in the file.
//!
//! `PostgreSQL` 16 moved role administration off `CREATEROLE` alone and onto
//! `CREATEROLE` **plus the `ADMIN` option on the target role**, and 18 keeps
//! that. This catalog stores a membership as a bare key with no payload — see
//! [`crabka_pgparser::ast::Statement::GrantRoles`] — so there is nowhere to
//! record an admin right and nothing to read one back from. The rule is
//! implemented as far as it is representable and no further:
//!
//! * The parts that read only role *attributes* are exact. Whether the acting
//!   role holds `SUPERUSER`, `CREATEROLE`, `CREATEDB`, `REPLICATION` or
//!   `BYPASSRLS` is a fact this catalog holds, so `CREATE ROLE` and the
//!   `SUPERUSER` gate on `ALTER ROLE` match `PostgreSQL` statement for
//!   statement.
//! * The parts that need the `ADMIN` option admit the superuser and nobody
//!   else. That is narrower than `PostgreSQL`, where a `CREATEROLE` role may
//!   alter, drop and hand out the roles it administers. Narrow is the safe
//!   direction to be wrong in, and widening it means storing the admin right
//!   first.
//!
//! The `DETAIL` lines still state `PostgreSQL`'s rule, which is the rule a
//! client reading one is reading about.

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

/// Raise 42501 unless `actor` may run `CREATE ROLE` with these options.
///
/// `PostgreSQL` checks `CREATEROLE` first and the individual attributes after,
/// and it gates an attribute on the **value** the statement asks for, not on
/// the option being written: `CREATE ROLE r NOSUPERUSER` needs no `SUPERUSER`.
/// `ALTER ROLE` is the other way round — see [`require_role_alter`].
///
/// # Errors
///
/// Returns 42501 when the role may not create this role, or storage/corruption
/// errors from the catalog KV seam.
pub(crate) fn require_role_create(
    kv: &dyn Kv,
    actor: &str,
    options: crabka_pgparser::ast::RoleOptions,
) -> Result<(), ExecError> {
    if crate::rls::role_is_superuser(kv, actor)? {
        return Ok(());
    }
    if !role_holds(kv, actor, crabka_pgcatalog::RoleAttribute::CreateRole)? {
        return Err(role_denial(
            "create role",
            "Only roles with the CREATEROLE attribute may create roles.".into(),
        ));
    }
    // `PostgreSQL` reports the first of these the statement asks for, in this
    // order. `CREATEROLE` is absent deliberately: a role holding it may hand it
    // on, which is why it needs no gate of its own.
    for (asked, attribute, name) in gated_attributes(options) {
        if asked == Some(true) && !role_holds(kv, actor, attribute)? {
            return Err(role_denial(
                "create role",
                format!(
                    "Only roles with the {name} attribute may create roles with the {name} attribute."
                ),
            ));
        }
    }
    Ok(())
}

/// Raise 42501 unless `actor` may run `ALTER ROLE target WITH <options>`.
///
/// `PostgreSQL` orders this differently from `CREATE ROLE`, and the difference
/// is load-bearing: `SUPERUSER` is judged **before** the general gate, so a role
/// with no rights over the target that asks for `SUPERUSER` is told about
/// `SUPERUSER` rather than about the target. The remaining attribute gates come
/// after the general one. Here the general gate admits only the superuser, who
/// then passes all of them, so those later gates are unreachable and are not
/// spelled out.
///
/// An attribute is gated on being **written**, whatever value it is written
/// with: `ALTER ROLE r NOSUPERUSER` is refused for the same reason
/// `ALTER ROLE r SUPERUSER` is.
///
/// An option list that writes nothing changes nothing. `PostgreSQL` lets a role
/// change its own password and refuses it on anyone else's, and this parser
/// drops `PASSWORD`, so a self-directed `ALTER ROLE … WITH PASSWORD …` arrives
/// here as an empty option list. Allowing an empty list on the acting role
/// alone is that rule as far as this engine can see it.
///
/// # Errors
///
/// Returns 42501 when the role may not alter `target`, or storage/corruption
/// errors from the catalog KV seam.
pub(crate) fn require_role_alter(
    kv: &dyn Kv,
    actor: &str,
    target: &str,
    options: crabka_pgparser::ast::RoleOptions,
) -> Result<(), ExecError> {
    if crate::rls::role_is_superuser(kv, actor)? {
        return Ok(());
    }
    if options.superuser.is_some() {
        return Err(role_denial(
            "alter role",
            "Only roles with the SUPERUSER attribute may change the SUPERUSER attribute.".into(),
        ));
    }
    if !writes_any_attribute(options) && actor == target {
        return Ok(());
    }
    Err(role_denial(
        "alter role",
        format!(
            "Only roles with the CREATEROLE attribute and the ADMIN option on role \"{target}\" may alter this role."
        ),
    ))
}

/// Raise 42501 unless `actor` may run `DROP ROLE`.
///
/// # Errors
///
/// Returns 42501 when the role may not drop roles, or storage/corruption errors
/// from the catalog KV seam.
pub(crate) fn require_role_drop(kv: &dyn Kv, actor: &str) -> Result<(), ExecError> {
    if crate::rls::role_is_superuser(kv, actor)? {
        return Ok(());
    }
    Err(role_denial(
        "drop role",
        "Only roles with the CREATEROLE attribute and the ADMIN option on the target roles may drop roles."
            .into(),
    ))
}

/// Which way a membership is moving, for the one error message that differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoleGrant {
    Grant,
    Revoke,
}

impl RoleGrant {
    const fn verb(self) -> &'static str {
        match self {
            Self::Grant => "grant",
            Self::Revoke => "revoke",
        }
    }
}

/// Raise 42501 unless `actor` may hand out — or take back — membership in
/// `granted`.
///
/// This is the gate that keeps every other one honest. A membership is
/// ownership for [`holds`], so a role that can grant itself into a table's
/// owning role has read and written every relation that role owns without any
/// grant naming it, and has passed the relation's row-security policies too.
/// `CREATE ROLE … IN ROLE r` writes the same membership and goes through here
/// for that reason.
///
/// # Errors
///
/// Returns 42501 when the role holds no admin right over `granted`, or
/// storage/corruption errors from the catalog KV seam.
pub(crate) fn require_role_grant(
    kv: &dyn Kv,
    actor: &str,
    granted: &str,
    direction: RoleGrant,
) -> Result<(), ExecError> {
    if crate::rls::role_is_superuser(kv, actor)? {
        return Ok(());
    }
    let verb = direction.verb();
    Err(ExecError::Remote(
        crabka_pgwire::error::PgError::error(
            "42501",
            format!("permission denied to {verb} role \"{granted}\""),
        )
        .with_detail(format!(
            "Only roles with the ADMIN option on role \"{granted}\" may {verb} this role."
        )),
    ))
}

/// The attributes `PostgreSQL` gates on the acting role holding the same one,
/// in the order it reports them.
fn gated_attributes(
    options: crabka_pgparser::ast::RoleOptions,
) -> [(Option<bool>, crabka_pgcatalog::RoleAttribute, &'static str); 4] {
    use crabka_pgcatalog::RoleAttribute;
    [
        (options.superuser, RoleAttribute::Superuser, "SUPERUSER"),
        (options.createdb, RoleAttribute::CreateDb, "CREATEDB"),
        (
            options.replication,
            RoleAttribute::Replication,
            "REPLICATION",
        ),
        (options.bypassrls, RoleAttribute::BypassRls, "BYPASSRLS"),
    ]
}

/// Whether the option list changes any stored attribute or the login flag.
fn writes_any_attribute(options: crabka_pgparser::ast::RoleOptions) -> bool {
    let crabka_pgparser::ast::RoleOptions {
        superuser,
        inherit,
        createrole,
        createdb,
        login,
        replication,
        bypassrls,
    } = options;

    [
        superuser,
        inherit,
        createrole,
        createdb,
        login,
        replication,
        bypassrls,
    ]
    .into_iter()
    .any(|written| written.is_some())
}

/// Whether `role` holds one boolean role attribute.
///
/// The bootstrap role holds every attribute and has no `pg_authid` row to read
/// one from, which is the same exception [`crate::rls::role_is_superuser`]
/// makes; a role that does not exist holds none.
fn role_holds(
    kv: &dyn Kv,
    role: &str,
    attribute: crabka_pgcatalog::RoleAttribute,
) -> Result<bool, ExecError> {
    if role == crabka_pgcatalog::BOOTSTRAP_ROLE {
        return Ok(true);
    }
    match crabka_pgcatalog::get_role(kv, role) {
        Ok(role) => Ok(role.attributes.has(attribute)),
        Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// `PostgreSQL`'s `permission denied to <verb>` with the `DETAIL` that names the
/// rule.
fn role_denial(verb: &str, detail: String) -> ExecError {
    ExecError::Remote(
        crabka_pgwire::error::PgError::error("42501", format!("permission denied to {verb}"))
            .with_detail(detail),
    )
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
    /// `MERGE`, carrying the kinds of action its `WHEN` clauses were written
    /// with. Needs one privilege per kind, plus `SELECT` for the target its
    /// `ON` condition reads.
    Merge(MergeClauses),
}

/// Which kinds of action a `MERGE`'s `WHEN` clauses can take.
///
/// A `MERGE` is not one command. `PostgreSQL` requires `INSERT`, `UPDATE` and
/// `DELETE` on the target for the clauses that spell each, and it decides that
/// once, at statement start, "whether or not particular `WHEN` clauses are
/// executed". So this is a property of the statement text and not of the rows
/// the statement meets: a `MERGE` whose `DELETE` clause never fires is still
/// refused when the role holds no `DELETE`.
///
/// Three booleans rather than three separate [`WriteAction`] variants because
/// one statement carries all three at once, and the privilege check runs once
/// before the first row is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct MergeClauses {
    insert: bool,
    update: bool,
    delete: bool,
}

impl MergeClauses {
    /// The kinds of action this clause list can take.
    ///
    /// `DO NOTHING` contributes nothing, which is why a `MERGE` written only
    /// with `DO NOTHING` clauses needs no write privilege at all.
    pub(crate) fn of(clauses: &[crabka_pgparser::ast::MergeWhen]) -> Self {
        use crabka_pgparser::ast::MergeAction;
        let mut found = Self::default();
        for clause in clauses {
            match clause.action {
                MergeAction::Insert { .. } => found.insert = true,
                MergeAction::Update(_) => found.update = true,
                MergeAction::Delete => found.delete = true,
                MergeAction::DoNothing => {}
            }
        }
        found
    }
}

impl WriteAction {
    /// The policy command row security applies to the rows this action gathers.
    ///
    /// `MERGE` gathers its rows by *reading* the target — the join its `ON`
    /// condition drives is a scan — so `SELECT` policies decide which rows it
    /// can see, exactly as they decide what a `SELECT` returns. A row a
    /// `SELECT` policy hides is therefore not matched at all, and a `WHEN NOT
    /// MATCHED` clause fires for it. The `UPDATE` and `DELETE` policies do
    /// still apply, per row and per action, but they are not a filter: see
    /// [`crate::rls::CheckSubject::TargetRow`] for why failing one raises
    /// rather than skips.
    pub(crate) const fn policy_command(self) -> crabka_pgcatalog::policy::PolicyCommand {
        use crabka_pgcatalog::policy::PolicyCommand;
        match self {
            Self::Update => PolicyCommand::Update,
            Self::Delete | Self::Truncate => PolicyCommand::Delete,
            Self::Merge(_) => PolicyCommand::Select,
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
        // A MERGE needs one privilege per kind of clause it was written with,
        // and each one is independent: a role holding UPDATE and not DELETE may
        // run the matched-update MERGE and not the matched-delete one.
        //
        // Its `SELECT` is unconditional, which is stricter than `PostgreSQL`:
        // there the target must be read for `SELECT` to be demanded, and
        // `MERGE … ON false` reads none of it. Every MERGE with a reason to run
        // reads the target through its `ON` condition, and demanding a
        // privilege the statement does not use refuses a statement that would
        // have worked rather than admitting one that should not.
        let (needed, may_read): (&[Privilege], bool) = match self {
            Self::Update => (&[Privilege::Update], true),
            Self::Delete => (&[Privilege::Delete], true),
            Self::Truncate => (&[Privilege::Truncate], false),
            Self::Merge(clauses) => {
                let mut privileges = Vec::with_capacity(4);
                for (wanted, privilege) in [
                    (clauses.insert, Privilege::Insert),
                    (clauses.update, Privilege::Update),
                    (clauses.delete, Privilege::Delete),
                ] {
                    if wanted {
                        privileges.push(privilege);
                    }
                }
                privileges.push(Privilege::Select);
                return privileges;
            }
        };
        let mut privileges = needed.to_vec();
        if may_read && reads_columns {
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
        ExecError, MergeClauses, Privilege, PrivilegeCtx, ReadPermit, RelationKind, RoleGrant,
        WriteAction, dml_reads_target, holds, require, require_role_alter, require_role_create,
        require_role_drop, require_role_grant, require_write,
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
            materialized: None,
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
                materialized: None,
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
        attributes_with(&[RoleAttribute::Superuser])
    }

    fn attributes_with(held: &[RoleAttribute]) -> RoleAttributes {
        let mut attributes = RoleAttributes::default();
        for attribute in held {
            attributes.set(*attribute, true);
        }
        attributes
    }

    /// The clause kinds of a `MERGE` written with `clauses`, taken from the
    /// parser rather than from a hand-built `MergeClauses`, so the mapping this
    /// checks is the one a real statement produces.
    fn merge_clauses(clauses: &str) -> MergeClauses {
        let sql = format!("MERGE INTO document USING source ON document.id = source.id {clauses}");
        let parsed = crabka_pgparser::parse(&sql).expect("parse");
        let [crabka_pgparser::ast::Statement::Merge { clauses, .. }] = parsed.as_slice() else {
            panic!("not a MERGE: {sql}")
        };
        MergeClauses::of(clauses)
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
            // Every MERGE below is written with the clause set its name gives,
            // and needs one privilege per clause kind plus SELECT. The
            // `insufficient` column is the grant set that is short of exactly
            // the privilege the extra clause added, which is the escalation
            // this pairing exists to refuse.
            Case {
                name: "MERGE with only DO NOTHING clauses needs only SELECT",
                action: WriteAction::Merge(MergeClauses::default()),
                reads_columns: false,
                sufficient: &["SELECT"],
                insufficient: &[],
            },
            Case {
                name: "matched-update MERGE needs UPDATE and SELECT",
                action: WriteAction::Merge(merge_clauses(
                    "WHEN MATCHED THEN UPDATE SET body = 'x'",
                )),
                reads_columns: false,
                sufficient: &["UPDATE", "SELECT"],
                insufficient: &["SELECT"],
            },
            Case {
                name: "matched-delete MERGE needs DELETE, not UPDATE",
                action: WriteAction::Merge(merge_clauses("WHEN MATCHED THEN DELETE")),
                reads_columns: false,
                sufficient: &["DELETE", "SELECT"],
                insufficient: &["UPDATE", "SELECT"],
            },
            Case {
                name: "insert-only MERGE needs INSERT, not UPDATE",
                action: WriteAction::Merge(merge_clauses(
                    "WHEN NOT MATCHED THEN INSERT VALUES (1, 'x')",
                )),
                reads_columns: false,
                sufficient: &["INSERT", "SELECT"],
                insufficient: &["UPDATE", "SELECT"],
            },
            Case {
                name: "update-and-delete MERGE needs both, and UPDATE alone is short",
                action: WriteAction::Merge(merge_clauses(
                    "WHEN MATCHED AND id > 1 THEN UPDATE SET body = 'x' WHEN MATCHED THEN DELETE",
                )),
                reads_columns: false,
                sufficient: &["UPDATE", "DELETE", "SELECT"],
                insufficient: &["UPDATE", "SELECT"],
            },
            Case {
                name: "all three clause kinds need all three privileges",
                action: WriteAction::Merge(merge_clauses(
                    "WHEN MATCHED THEN UPDATE SET body = 'x' \
                     WHEN NOT MATCHED BY SOURCE THEN DELETE \
                     WHEN NOT MATCHED THEN INSERT VALUES (1, 'x')",
                )),
                reads_columns: false,
                sufficient: &["INSERT", "UPDATE", "DELETE", "SELECT"],
                insufficient: &["INSERT", "UPDATE", "SELECT"],
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
    ///
    /// A `MERGE` gathers its rows by reading the target, so `SELECT` policies
    /// decide what it can see. Its `UPDATE` and `DELETE` policies apply per row
    /// and per action instead, which is not a filter and so not this question.
    #[test]
    fn write_actions_map_to_policy_commands() {
        use crabka_pgcatalog::policy::PolicyCommand;
        for (action, command) in [
            (WriteAction::Update, PolicyCommand::Update),
            (WriteAction::Delete, PolicyCommand::Delete),
            (WriteAction::Truncate, PolicyCommand::Delete),
            (
                WriteAction::Merge(MergeClauses::default()),
                PolicyCommand::Select,
            ),
            (
                WriteAction::Merge(merge_clauses("WHEN MATCHED THEN DELETE")),
                PolicyCommand::Select,
            ),
        ] {
            assert!(action.policy_command() == command);
        }
    }

    /// The clause set is read off the statement text, so a clause that never
    /// fires still counts. `PostgreSQL` decides a `MERGE`'s privileges at
    /// statement start "whether or not particular `WHEN` clauses are executed".
    #[test]
    fn merge_clause_kinds_come_from_the_statement() {
        for (sql, expected) in [
            ("WHEN MATCHED THEN DO NOTHING", MergeClauses::default()),
            (
                "WHEN MATCHED THEN UPDATE SET body = 'x'",
                MergeClauses {
                    insert: false,
                    update: true,
                    delete: false,
                },
            ),
            (
                "WHEN MATCHED THEN DELETE",
                MergeClauses {
                    insert: false,
                    update: false,
                    delete: true,
                },
            ),
            (
                "WHEN NOT MATCHED THEN INSERT VALUES (1, 'x')",
                MergeClauses {
                    insert: true,
                    update: false,
                    delete: false,
                },
            ),
            (
                "WHEN MATCHED AND id > 9000 THEN DELETE \
                 WHEN MATCHED THEN UPDATE SET body = 'x' \
                 WHEN NOT MATCHED THEN INSERT VALUES (1, 'x') \
                 WHEN NOT MATCHED BY SOURCE THEN DO NOTHING",
                MergeClauses {
                    insert: true,
                    update: true,
                    delete: true,
                },
            ),
        ] {
            assert!(merge_clauses(sql) == expected, "{sql}");
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

    /// What a role-administration gate decided, stated whole: a refusal carries
    /// its SQLSTATE, message and `DETAIL`, so a case pins the answer a client
    /// sees rather than only that an answer happened.
    #[derive(Debug, PartialEq, Eq)]
    enum Decision {
        Allowed,
        Denied {
            sqlstate: String,
            message: String,
            detail: Option<String>,
        },
    }

    fn decide(outcome: Result<(), ExecError>) -> Decision {
        let Some(error) = outcome.err() else {
            return Decision::Allowed;
        };
        let rendered = error.into_pg();
        Decision::Denied {
            detail: rendered
                .diagnostics
                .as_ref()
                .and_then(|fields| fields.detail.clone()),
            sqlstate: rendered.code,
            message: rendered.message,
        }
    }

    fn refusal(message: &str, detail: &str) -> Decision {
        Decision::Denied {
            sqlstate: "42501".to_string(),
            message: message.to_string(),
            detail: Some(detail.to_string()),
        }
    }

    /// Who may write the role catalog, checked against `postgres:18.4`.
    ///
    /// This is the gate everything else in the module rests on: a role that can
    /// set `SUPERUSER` on itself holds every privilege on every relation, and a
    /// role that can grant itself into an owning role owns that role's tables.
    /// So each case states the whole refusal — code, message and `DETAIL` — and
    /// each success is a statement `PostgreSQL` also admits.
    #[test]
    fn role_administration_gates() {
        use crabka_pgparser::ast::RoleOptions;

        let plain = RoleOptions::default();
        let wants_superuser = RoleOptions {
            superuser: Some(true),
            ..RoleOptions::default()
        };
        let drops_superuser = RoleOptions {
            superuser: Some(false),
            ..RoleOptions::default()
        };
        let wants_createdb = RoleOptions {
            createdb: Some(true),
            ..RoleOptions::default()
        };
        let wants_createrole = RoleOptions {
            createrole: Some(true),
            ..RoleOptions::default()
        };
        let wants_bypassrls = RoleOptions {
            bypassrls: Some(true),
            ..RoleOptions::default()
        };

        let kv = store(&[
            ("stranger", RoleAttributes::default(), &[]),
            ("root", superuser(), &[]),
            (
                "creator",
                attributes_with(&[RoleAttribute::CreateRole]),
                &[],
            ),
            (
                "dbcreator",
                attributes_with(&[RoleAttribute::CreateRole, RoleAttribute::CreateDb]),
                &[],
            ),
        ]);

        let alter_denied = |target: &str| {
            format!(
                "Only roles with the CREATEROLE attribute and the ADMIN option on role \"{target}\" may alter this role."
            )
        };
        let grant_denied = |role: &str| {
            format!("Only roles with the ADMIN option on role \"{role}\" may grant this role.")
        };

        struct Case<'a> {
            name: &'a str,
            outcome: Result<(), ExecError>,
            expected: Decision,
        }
        let cases = [
            // CREATE ROLE: CREATEROLE first, then the attribute the statement
            // asks for, and an attribute asked for as false is not asked for.
            Case {
                name: "an ordinary role cannot create a role",
                outcome: require_role_create(&kv, "stranger", plain),
                expected: refusal(
                    "permission denied to create role",
                    "Only roles with the CREATEROLE attribute may create roles.",
                ),
            },
            Case {
                name: "CREATEROLE creates an ordinary role",
                outcome: require_role_create(&kv, "creator", plain),
                expected: Decision::Allowed,
            },
            Case {
                name: "CREATEROLE passes on its own attribute",
                outcome: require_role_create(&kv, "creator", wants_createrole),
                expected: Decision::Allowed,
            },
            Case {
                name: "CREATEROLE cannot create a superuser",
                outcome: require_role_create(&kv, "creator", wants_superuser),
                expected: refusal(
                    "permission denied to create role",
                    "Only roles with the SUPERUSER attribute may create roles with the SUPERUSER attribute.",
                ),
            },
            Case {
                name: "CREATEROLE cannot create a BYPASSRLS role",
                outcome: require_role_create(&kv, "creator", wants_bypassrls),
                expected: refusal(
                    "permission denied to create role",
                    "Only roles with the BYPASSRLS attribute may create roles with the BYPASSRLS attribute.",
                ),
            },
            Case {
                name: "CREATEROLE cannot create a CREATEDB role without CREATEDB",
                outcome: require_role_create(&kv, "creator", wants_createdb),
                expected: refusal(
                    "permission denied to create role",
                    "Only roles with the CREATEDB attribute may create roles with the CREATEDB attribute.",
                ),
            },
            Case {
                name: "CREATEROLE with CREATEDB may pass CREATEDB on",
                outcome: require_role_create(&kv, "dbcreator", wants_createdb),
                expected: Decision::Allowed,
            },
            Case {
                name: "NOSUPERUSER asks for nothing, so CREATEROLE may write it",
                outcome: require_role_create(&kv, "creator", drops_superuser),
                expected: Decision::Allowed,
            },
            Case {
                name: "the superuser creates anything",
                outcome: require_role_create(&kv, "root", wants_superuser),
                expected: Decision::Allowed,
            },
            // ALTER ROLE: SUPERUSER is judged before the general gate, and an
            // attribute is judged on being written rather than on its value.
            Case {
                name: "a role cannot make itself a superuser",
                outcome: require_role_alter(&kv, "stranger", "stranger", wants_superuser),
                expected: refusal(
                    "permission denied to alter role",
                    "Only roles with the SUPERUSER attribute may change the SUPERUSER attribute.",
                ),
            },
            Case {
                name: "clearing SUPERUSER is still changing SUPERUSER",
                outcome: require_role_alter(&kv, "stranger", "stranger", drops_superuser),
                expected: refusal(
                    "permission denied to alter role",
                    "Only roles with the SUPERUSER attribute may change the SUPERUSER attribute.",
                ),
            },
            Case {
                name: "a role cannot make another role a superuser",
                outcome: require_role_alter(&kv, "stranger", "root", wants_superuser),
                expected: refusal(
                    "permission denied to alter role",
                    "Only roles with the SUPERUSER attribute may change the SUPERUSER attribute.",
                ),
            },
            Case {
                name: "a role cannot give itself BYPASSRLS",
                outcome: require_role_alter(&kv, "stranger", "stranger", wants_bypassrls),
                expected: refusal("permission denied to alter role", &alter_denied("stranger")),
            },
            Case {
                name: "a role cannot give itself CREATEROLE",
                outcome: require_role_alter(&kv, "stranger", "stranger", wants_createrole),
                expected: refusal("permission denied to alter role", &alter_denied("stranger")),
            },
            Case {
                name: "CREATEROLE alone does not carry the ADMIN option this catalog cannot store",
                outcome: require_role_alter(&kv, "creator", "stranger", wants_createdb),
                expected: refusal("permission denied to alter role", &alter_denied("stranger")),
            },
            Case {
                name: "an option list that writes nothing is allowed on yourself",
                outcome: require_role_alter(&kv, "stranger", "stranger", plain),
                expected: Decision::Allowed,
            },
            Case {
                name: "an option list that writes nothing is refused on anyone else",
                outcome: require_role_alter(&kv, "stranger", "root", plain),
                expected: refusal("permission denied to alter role", &alter_denied("root")),
            },
            Case {
                name: "the superuser alters anything",
                outcome: require_role_alter(&kv, "root", "stranger", wants_superuser),
                expected: Decision::Allowed,
            },
            // DROP ROLE and the membership gates.
            Case {
                name: "an ordinary role cannot drop a role",
                outcome: require_role_drop(&kv, "stranger"),
                expected: refusal(
                    "permission denied to drop role",
                    "Only roles with the CREATEROLE attribute and the ADMIN option on the target roles may drop roles.",
                ),
            },
            Case {
                name: "the superuser drops roles",
                outcome: require_role_drop(&kv, "root"),
                expected: Decision::Allowed,
            },
            Case {
                name: "a role cannot grant itself into an owning role",
                outcome: require_role_grant(&kv, "stranger", OWNER, RoleGrant::Grant),
                expected: refusal(
                    &format!("permission denied to grant role \"{OWNER}\""),
                    &grant_denied(OWNER),
                ),
            },
            Case {
                name: "CREATEROLE does not carry the ADMIN option either",
                outcome: require_role_grant(&kv, "creator", OWNER, RoleGrant::Grant),
                expected: refusal(
                    &format!("permission denied to grant role \"{OWNER}\""),
                    &grant_denied(OWNER),
                ),
            },
            Case {
                name: "a revoke names itself a revoke",
                outcome: require_role_grant(&kv, "stranger", OWNER, RoleGrant::Revoke),
                expected: refusal(
                    &format!("permission denied to revoke role \"{OWNER}\""),
                    &format!(
                        "Only roles with the ADMIN option on role \"{OWNER}\" may revoke this role."
                    ),
                ),
            },
            Case {
                name: "the superuser grants any membership",
                outcome: require_role_grant(&kv, "root", OWNER, RoleGrant::Grant),
                expected: Decision::Allowed,
            },
        ];
        for case in cases {
            assert!(decide(case.outcome) == case.expected, "{}", case.name);
        }
    }

    /// A membership is ownership for [`holds`], which is why
    /// [`require_role_grant`] is a security gate and not a tidiness one.
    ///
    /// Without it, an ordinary role reaches every relation an owning role owns
    /// by granting itself the membership — no `GRANT` on the relation, and the
    /// relation's row security passes too.
    #[test]
    fn membership_is_ownership() {
        let kv = store(&[
            ("stranger", RoleAttributes::default(), &[]),
            ("insider", RoleAttributes::default(), &[OWNER]),
        ]);
        let table = table();
        for (role, reaches) in [("stranger", false), ("insider", true)] {
            let ctx = PrivilegeCtx::new(&kv, role);
            for privilege in [
                Privilege::Select,
                Privilege::Insert,
                Privilege::Update,
                Privilege::Delete,
            ] {
                let held = holds(&ctx, &table.name, &table.owner, privilege).expect("decide");
                assert!(held == reaches, "{role} {privilege:?}");
            }
        }
    }
}
