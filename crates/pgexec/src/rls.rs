//! Row-level security: the one place raw rows of a stored relation may be
//! turned into a relation the rest of the executor can see.
//!
//! **Nothing here fires yet.** Every decision keys off
//! [`crabka_pgcatalog::Table::row_security`], and no reachable SQL sets that
//! flag — there is no `ALTER TABLE … ENABLE ROW LEVEL SECURITY` production and
//! no executor DDL arm for one. The module is wired into every read path so
//! that flipping the flag is the whole of the next slice, and so that the
//! wiring can be reviewed and tested while it is still inert.
//!
//! # Why the types are shaped like this
//!
//! The read path used to have six ways to produce rows of a stored relation and
//! no shared choke point. A seventh would have been written the same way, and
//! would have leaked. So the rows themselves are made *un-typeable*: a scan
//! produces a [`RawScan`], whose fields are private and whose only exit is
//! [`apply_row_security`]. An optimizer that wants to skip the gate cannot —
//! it has nothing to return. A pushdown that wants a relation's `Table` takes
//! an [`UnrestrictedTable`], which can only be built from an explicit
//! [`RowSecurity::Open`] decision. Both properties are enforced by the
//! compiler, not by remembering to call something.
//!
//! # Why the fold has no branches
//!
//! [`combine_policy_quals`] is two folds over the applicable policies.
//! `OR`'s identity is `FALSE`, so "row security is on and no permissive policy
//! matched" *is* the empty permissive fold: default-deny is the identity
//! element, not a branch someone can forget to write. `AND`'s identity is
//! `TRUE`, so an empty restrictive set changes nothing, and a restrictive
//! policy with no permissive policy beside it folds onto `FALSE` and stays
//! `FALSE` — restrictive policies never grant.
//!
//! # Which relation's policies apply
//!
//! For an inheritance or partition tree, `PostgreSQL` applies the **parent's**
//! policies to every row the tree yields, and none of the children's. A
//! [`RawScan`] therefore carries the relation whose policies govern its rows,
//! which for a tree is the parent that was named, not the child a row came
//! from. [`RawScan::absorb`] is how a child's rows join a parent's scan, and it
//! discards the child's governing relation by construction.

use std::sync::Mutex;

use crabka_pgcatalog::{
    RoleAttribute, Table, TableId,
    policy::{Policy, PolicyCommand},
};
use crabka_pgkv::Kv;
use crabka_pgparser::ast::{BinaryOp, Expr};
use crabka_pgtypes::Datum;

use crate::{error::ExecError, join::Relation, scope::Scope};

/// Everything a row-security decision reads, in one borrowed value.
///
/// Deliberately narrow: the catalog handle, the role whose policies apply, and
/// the `row_security` GUC. Nothing else takes part in the decision, so nothing
/// else can accidentally change it, and a unit test can build one over a bare
/// [`crabka_pgkv::MemKv`].
#[derive(Clone, Copy)]
pub struct RlsCtx<'a> {
    catalog_kv: &'a dyn Kv,
    /// The role whose policies apply to this read. Ordinarily the session's
    /// `current_user`; a later slice substitutes a view's owner here, which is
    /// the whole reason it is a field rather than being re-derived per call.
    role: &'a str,
    /// The `row_security` GUC. `false` means "fail rather than filter".
    row_security: bool,
}

impl<'a> RlsCtx<'a> {
    /// A decision context over `catalog_kv`, acting as `role`, with the
    /// session's `row_security` setting.
    #[must_use]
    pub const fn new(catalog_kv: &'a dyn Kv, role: &'a str, row_security: bool) -> Self {
        Self {
            catalog_kv,
            role,
            row_security,
        }
    }
}

/// What row security decided for one relation and one command.
///
/// The three variants are deliberately not two: "unrestricted" and "restricted
/// by a qual that happens to admit everything" are different facts, and only
/// the first may skip the gate. [`Self::Open`] is reachable **only** from
/// [`bypass_applies`], never from an empty policy list.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RowSecurity {
    /// An explicit bypass: the relation has row security disabled, or the role
    /// is exempt from it. The rows may be read as they are.
    Open,
    /// Row security applies. Only rows for which `qual` evaluates to true are
    /// visible; `NULL` is not true, so a `NULL` qual hides the row.
    Restricted { relation: String, qual: Expr },
    /// `row_security = off` with at least one policy that would have applied —
    /// `PostgreSQL` fails the query rather than silently filtering it (42501).
    Refuse { relation: String },
}

/// A table proven not subject to row security for this read.
///
/// The optimizer pushdowns take this instead of `&Table`. Its only constructors
/// return `None` unless the decision was [`RowSecurity::Open`], so a pushdown
/// that forgets about row security does not compile rather than silently
/// reading past a policy. Falling through to `None` is fail-closed: the caller
/// drops to the ordinary gated scan, which is slower and never wrong.
#[derive(Clone, Copy)]
pub(crate) struct UnrestrictedTable<'a>(&'a Table);

impl<'a> UnrestrictedTable<'a> {
    /// Decide row security for `table` and admit it only if the answer was an
    /// explicit bypass.
    ///
    /// # Errors
    ///
    /// Returns storage/corruption errors from the catalog KV seam, or a
    /// refusal to compile an unsafe policy qual.
    pub(crate) fn read(ctx: &RlsCtx<'_>, table: &'a Table) -> Result<Option<Self>, ExecError> {
        Ok(Self::from_decision(
            &decide(ctx, table, PolicyCommand::Select)?,
            table,
        ))
    }

    /// Admit `table` when `decision` is an explicit bypass, for a caller that
    /// has already made the decision and would otherwise make it twice.
    pub(crate) const fn from_decision(decision: &RowSecurity, table: &'a Table) -> Option<Self> {
        match decision {
            RowSecurity::Open => Some(Self(table)),
            RowSecurity::Restricted { .. } | RowSecurity::Refuse { .. } => None,
        }
    }

    /// The relation, now that it is known not to need filtering.
    pub(crate) const fn get(self) -> &'a Table {
        self.0
    }
}

/// The rows of one stored relation, before row security.
///
/// Private fields, and the only function that turns one into a
/// [`Relation`] is [`apply_row_security`]. That is the point: a scan path
/// cannot return rows the gate has not seen, because it has no way to build the
/// type the caller wants.
pub(crate) struct RawScan {
    scope: Scope,
    rows: Vec<Vec<Datum>>,
    /// The relation whose policies govern these rows. For an inheritance or
    /// partition tree this is the **parent**, never the child a row came from.
    governing: Table,
}

impl RawScan {
    /// The rows `table` itself yielded, governed by `table`'s own policies.
    pub(crate) fn of_relation(table: &Table, scope: Scope, rows: Vec<Vec<Datum>>) -> Self {
        Self {
            scope,
            rows,
            governing: table.clone(),
        }
    }

    /// An empty scan of an inheritance or partition parent, ready to absorb its
    /// children's rows. The parent's policies govern every row that lands here.
    pub(crate) fn tree_of(parent: &Table, qualifier: &str) -> Self {
        Self {
            scope: Scope::single(parent, qualifier),
            rows: Vec::new(),
            governing: parent.clone(),
        }
    }

    /// Absorb a child scan, permuting each row into this parent's column order.
    ///
    /// `ordinals[i]` is the index in the child's row of the parent's `i`th
    /// column — a partition attached by `ATTACH PARTITION` may declare the same
    /// columns in a different order, and `PostgreSQL` maps them by name.
    ///
    /// The child's governing relation is dropped here, which is exactly
    /// `PostgreSQL`'s rule: a child's own policies do not apply to a read of the
    /// parent.
    pub(crate) fn absorb(&mut self, child: Self, ordinals: &[usize]) {
        self.rows.extend(child.rows.into_iter().map(|row| {
            ordinals
                .iter()
                .map(|ordinal| row.get(*ordinal).cloned().unwrap_or(Datum::Null))
                .collect::<Vec<_>>()
        }));
    }
}

/// The gate. Turn the raw rows of a stored relation into a readable relation,
/// applying the governing relation's row-security policies.
///
/// # Errors
///
/// Returns 42501 when `row_security = off` and a policy would have applied,
/// 42P17 when a policy qual reads the relation its own policy protects, and
/// storage/parse errors from compiling the qual.
pub(crate) fn apply_row_security(
    read_ctx: &crate::subquery::SubCtx<'_>,
    raw: RawScan,
) -> Result<Relation, ExecError> {
    let RawScan {
        scope,
        rows,
        governing,
    } = raw;
    match decide(&read_ctx.rls(), &governing, PolicyCommand::Select)? {
        RowSecurity::Open => Ok(Relation { scope, rows }),
        RowSecurity::Refuse { relation } => Err(ExecError::RowSecurityRefused(relation)),
        RowSecurity::Restricted { relation, qual } => {
            // A policy qual that reads its own relation would re-enter this
            // function forever. PostgreSQL reports it rather than overflowing
            // the stack, and so must we: the qual is attacker-supplied SQL, so
            // an overflow here is remotely triggerable.
            if read_ctx.policy_stack.holds(governing.id) {
                return Err(ExecError::PolicyRecursion(relation));
            }
            let _entered = read_ctx.policy_stack.enter(governing.id);
            // Resolve subqueries inside the qual once per scan, not per row —
            // and inside the recursion guard, since that is where a
            // self-referencing qual re-enters the read path.
            let qual = crate::subquery::resolve_expr(read_ctx, &qual)?;
            let mut kept = Vec::with_capacity(rows.len());
            for row in rows {
                if crate::exec::row_matches(Some(&qual), &scope, &row, read_ctx.eval_ctx)? {
                    kept.push(row);
                }
            }
            Ok(Relation { scope, rows: kept })
        }
    }
}

/// Decide row security for one relation and one command.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam, or a refusal to
/// compile a policy qual that cannot be trusted yet.
pub(crate) fn decide(
    ctx: &RlsCtx<'_>,
    table: &Table,
    command: PolicyCommand,
) -> Result<RowSecurity, ExecError> {
    // The dormant path, and the cheap one: a relation without row security
    // reads exactly one bool and touches no catalog key.
    if bypass_applies(ctx, table)? {
        return Ok(RowSecurity::Open);
    }
    let relation = table.name.name.clone();
    let applicable = applicable_policies(ctx, table, command)?;
    // PostgreSQL's rule, from the `row_security` documentation: with the GUC
    // off, a query fails if at least one policy would otherwise have applied.
    // With no policy applicable there is nothing to fail over, and the empty
    // permissive fold below denies every row anyway.
    if !ctx.row_security && !applicable.is_empty() {
        return Ok(RowSecurity::Refuse { relation });
    }
    let mut permissive = Vec::new();
    let mut restrictive = Vec::new();
    for policy in applicable {
        let Some(source) = policy.using.as_deref() else {
            // A policy with no USING qual contributes nothing to a read. That
            // is not "admit everything": leaving it out of the permissive fold
            // is what keeps the default-deny identity intact.
            continue;
        };
        let qual = compile_qual(source)?;
        if policy.permissive {
            permissive.push(qual);
        } else {
            restrictive.push(qual);
        }
    }
    Ok(RowSecurity::Restricted {
        relation,
        qual: combine_policy_quals(permissive, restrictive),
    })
}

/// Why a read may skip row security entirely.
///
/// Every bypass is listed here and nowhere else, so the set of ways to see
/// unfiltered rows is one function long.
fn bypass_applies(ctx: &RlsCtx<'_>, table: &Table) -> Result<bool, ExecError> {
    if !table.row_security {
        return Ok(true);
    }
    if role_is_exempt(ctx)? {
        return Ok(true);
    }
    // The owner reads its own relation unfiltered unless it asked not to with
    // FORCE ROW LEVEL SECURITY. Membership, not string equality: a member of an
    // owning group owns the relation for this purpose, and `role_has_privs_of`
    // is the predicate PostgreSQL matches roles with — `role_can_set` counts
    // roles the session may merely `SET ROLE` to, which is a wider set.
    if !table.force_row_security
        && crabka_pgcatalog::role_has_privs_of(ctx.catalog_kv, ctx.role, &table.owner)?
    {
        return Ok(true);
    }
    Ok(false)
}

/// Whether the role itself is exempt from every relation's row security:
/// `SUPERUSER` or `BYPASSRLS`.
fn role_is_exempt(ctx: &RlsCtx<'_>) -> Result<bool, ExecError> {
    // The bootstrap role is the cluster's superuser by definition and has no
    // `pg_authid` row to read the attribute from.
    if ctx.role == crabka_pgcatalog::BOOTSTRAP_ROLE {
        return Ok(true);
    }
    let attributes = match crabka_pgcatalog::get_role(ctx.catalog_kv, ctx.role) {
        Ok(role) => role.attributes,
        // A role that does not exist holds no attributes, so it is exempt from
        // nothing. Erroring here would turn a dropped role into a hard failure
        // on every read instead of a filtered one.
        Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    Ok(attributes.has(RoleAttribute::Superuser) || attributes.has(RoleAttribute::BypassRls))
}

/// The relation's policies that apply to `command` for `ctx.role`.
fn applicable_policies(
    ctx: &RlsCtx<'_>,
    table: &Table,
    command: PolicyCommand,
) -> Result<Vec<Policy>, ExecError> {
    let mut applicable = Vec::new();
    for policy in crabka_pgcatalog::policy::policies_for_table(ctx.catalog_kv, table.id)? {
        if policy_applies(ctx, &policy, command)? {
            applicable.push(policy);
        }
    }
    Ok(applicable)
}

/// Whether one policy applies: its command matches, and its `TO` list matches
/// the role.
fn policy_applies(
    ctx: &RlsCtx<'_>,
    policy: &Policy,
    command: PolicyCommand,
) -> Result<bool, ExecError> {
    // `ALL` is not a shorthand for the other four — it applies to every
    // command *in addition to* any command-specific policy.
    if policy.command != PolicyCommand::All && policy.command != command {
        return Ok(false);
    }
    // An empty `TO` list is `PUBLIC`, which matches every role. Reading it as
    // "no role matches" would silently drop a policy that grants rows.
    if policy.applies_to_public() {
        return Ok(true);
    }
    for role in &policy.roles {
        if crabka_pgcatalog::role_has_privs_of(ctx.catalog_kv, ctx.role, role)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Fold a relation's applicable policy quals into the single expression a row
/// must satisfy.
///
/// Two folds, no branches. `OR`'s identity is `FALSE`, so the permissive fold
/// starts at `FALSE` and an empty permissive set stays `FALSE` — default-deny
/// is the identity element rather than a case to remember. `AND`'s identity is
/// `TRUE`, so the restrictive fold leaves the result alone when there is
/// nothing restrictive, and folds onto `FALSE` when there is nothing permissive,
/// because a restrictive policy removes rows and never adds any.
///
/// The literal `FALSE` seed is deliberately left in the expression rather than
/// optimized away when the permissive set is non-empty: the moment the seed
/// becomes conditional, the identity stops being structural.
#[must_use]
pub(crate) fn combine_policy_quals(permissive: Vec<Expr>, restrictive: Vec<Expr>) -> Expr {
    let visible = permissive
        .into_iter()
        .fold(Expr::BoolLiteral(false), |left, right| {
            binary(BinaryOp::Or, left, right)
        });
    restrictive
        .into_iter()
        .fold(visible, |left, right| binary(BinaryOp::And, left, right))
}

fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Parse a stored policy qual, refusing the ones that cannot be trusted yet.
///
/// # Errors
///
/// Returns 0A000 for a qual that probes table privileges, or a parse error.
fn compile_qual(source: &str) -> Result<Expr, ExecError> {
    if let Some(probe) = privilege_probe(source) {
        // Every `has_*_privilege` function returns true unconditionally today
        // (see `catalog_fn`), because no per-relation privilege enforcement
        // exists. A policy written around one would therefore admit every row
        // to every role — the exact leak this module exists to prevent. Refuse
        // until privileges are enforced.
        return Err(ExecError::Unsupported(format!(
            "row-level-security policy qual uses {probe}, which is not enforced yet"
        )));
    }
    Ok(crabka_pgparser::parser::parse_expression(source)?)
}

/// The first privilege-probing function named anywhere in a qual's source.
///
/// Deliberately textual, and deliberately over-eager. The expression walkers in
/// `exec` do not descend into a subquery's own clauses, so a tree walk would
/// miss `EXISTS (SELECT … WHERE has_table_privilege(…))` — the one shape most
/// worth catching. Matching the source text over-rejects (a column that happens
/// to be named after one of these functions is refused too) and never
/// under-rejects, which is the direction a security check should fail in.
fn privilege_probe(source: &str) -> Option<&'static str> {
    let lowered = source.to_ascii_lowercase();
    crate::catalog_fn::PRIVILEGE_FUNCTIONS
        .into_iter()
        .find(|name| lowered.contains(name))
}

/// The scan plan a relation under row security may hand the scanner.
///
/// `None` means the plan is safe as it stands (the relation is not under row
/// security). Otherwise the returned plan keeps only the pushdowns that cannot
/// observe rows the policy qual has not yet removed:
///
/// - **partial aggregate** is cleared — an aggregate folded inside the scanner
///   sums rows the qual would have hidden, and the sum cannot be un-summed.
/// - **top-K** is cleared — it truncates the row set before the qual runs, so
///   the qual would filter an already-wrong prefix.
/// - **projection** is widened to every column — the qual may reference a
///   column the query itself never selects.
/// - **predicate** and **text search** are kept. Both are column/operator/
///   literal only ([`crate::scanner::ColumnPredicate`] holds a `Datum`, not an
///   expression), so no user-supplied code rides them below the gate, and
///   discarding rows a `WHERE` would have discarded anyway cannot disclose
///   anything.
///
/// This runs at the site that builds the [`crate::scanner::ScanRequest`],
/// rather than trusting each caller to have sanitized its own plan.
#[must_use]
pub(crate) fn sanitize_scan_plan(
    decision: &RowSecurity,
    plan: &crate::plan_dist::DistributedScanPlan,
) -> Option<crate::plan_dist::DistributedScanPlan> {
    match decision {
        RowSecurity::Open => None,
        RowSecurity::Restricted { .. } | RowSecurity::Refuse { .. } => {
            Some(crate::plan_dist::DistributedScanPlan {
                predicate: plan.predicate.clone(),
                projection: crate::ProjectionPushdown::All,
                partial_aggregate: None,
                top_k: None,
                text_search: plan.text_search.clone(),
            })
        }
    }
}

/// The relations whose policy quals are being evaluated right now, innermost
/// last.
///
/// Lives on [`crate::subquery::SubCtx`], which is already threaded through
/// every read path, so the guard costs one field instead of an argument on
/// twenty functions.
/// A `Mutex` rather than a `RefCell` because a `WriteContext` holding one
/// crosses an `await` in a `Send` future; the stack is only ever touched by the
/// one thread executing the statement, so it is never contended.
#[derive(Default)]
pub(crate) struct PolicyStack(Mutex<Vec<TableId>>);

impl PolicyStack {
    /// Whether `table` already has a policy qual under evaluation.
    pub(crate) fn holds(&self, table: TableId) -> bool {
        self.0.lock().expect("policy stack").contains(&table)
    }

    /// Mark `table`'s policy qual as under evaluation until the guard drops.
    pub(crate) fn enter(&self, table: TableId) -> PolicyGuard<'_> {
        self.0.lock().expect("policy stack").push(table);
        PolicyGuard(self)
    }
}

/// Pops the relation [`PolicyStack::enter`] pushed, including on the error
/// paths out of qual evaluation — a stack that leaked an entry would report
/// 42P17 on an unrelated later read of the same relation.
pub(crate) struct PolicyGuard<'a>(&'a PolicyStack);

impl Drop for PolicyGuard<'_> {
    fn drop(&mut self) {
        self.0.0.lock().expect("policy stack").pop();
    }
}

/// The `USING` qual an `UPDATE` or `DELETE`'s candidate rows must satisfy.
///
/// Separate from [`RowSecurityCheck`] because the two quals are different
/// expressions with different failure modes: this one silently skips a row,
/// that one raises an error.
pub(crate) struct RowSecurityUsing(RowSecurity);

impl RowSecurityUsing {
    /// Decide the `USING` side for a write against `table`.
    ///
    /// # Errors
    ///
    /// Returns catalog errors, or a refusal to compile an unsafe policy qual.
    pub(crate) fn compile(
        ctx: &RlsCtx<'_>,
        table: &Table,
        command: PolicyCommand,
    ) -> Result<Self, ExecError> {
        Ok(Self(decide(ctx, table, command)?))
    }

    /// The candidate rows the statement may act on.
    ///
    /// # Errors
    ///
    /// Returns 42501 when `row_security = off` and a policy would have applied,
    /// or an evaluation error from the qual.
    pub(crate) fn retain_visible(
        &self,
        table: &Table,
        rows: Vec<(u64, u64, Vec<Datum>)>,
        ctx: &crate::clock::EvalCtx,
    ) -> Result<Vec<(u64, u64, Vec<Datum>)>, ExecError> {
        let (relation, qual) = match &self.0 {
            RowSecurity::Open => return Ok(rows),
            RowSecurity::Refuse { relation } => {
                return Err(ExecError::RowSecurityRefused(relation.clone()));
            }
            RowSecurity::Restricted { relation, qual } => (relation, qual),
        };
        let scope = Scope::single(table, relation);
        let mut kept = Vec::with_capacity(rows.len());
        for (rowid, xmin, row) in rows {
            if crate::exec::row_matches(Some(qual), &scope, &row, ctx)? {
                kept.push((rowid, xmin, row));
            }
        }
        Ok(kept)
    }
}

/// The `WITH CHECK` qual a row a statement writes must satisfy.
///
/// Constructed and tested, but **not yet called from any write path**, and that
/// is deliberate rather than an oversight. `PostgreSQL` runs `WITH CHECK` after
/// `BEFORE ROW` triggers, because a `BEFORE` trigger returns a *replacement*
/// row and that replacement is what gets written; a check that ran before the
/// trigger would let a trigger launder a row past its own policy. Placing it
/// correctly means routing it through `trigger::fire_before_row`, which is the
/// write-enforcement slice's work. Wiring it into `finish_written_row` now — the
/// obvious-looking spot — would ship the bug.
pub struct RowSecurityCheck(RowSecurity);

impl RowSecurityCheck {
    /// Decide the `WITH CHECK` side for a write against `table`.
    ///
    /// A policy with no `WITH CHECK` qual falls back to its `USING` qual, as
    /// `PostgreSQL` does, so a policy that hides a row also forbids writing
    /// one it would hide.
    ///
    /// # Errors
    ///
    /// Returns catalog errors, or a refusal to compile an unsafe policy qual.
    pub fn compile(
        ctx: &RlsCtx<'_>,
        table: &Table,
        command: PolicyCommand,
    ) -> Result<Self, ExecError> {
        if bypass_applies(ctx, table)? {
            return Ok(Self(RowSecurity::Open));
        }
        let relation = table.name.name.clone();
        let applicable = applicable_policies(ctx, table, command)?;
        if !ctx.row_security && !applicable.is_empty() {
            return Ok(Self(RowSecurity::Refuse { relation }));
        }
        let mut permissive = Vec::new();
        let mut restrictive = Vec::new();
        for policy in applicable {
            let Some(source) = policy.with_check.as_deref().or(policy.using.as_deref()) else {
                continue;
            };
            let qual = compile_qual(source)?;
            if policy.permissive {
                permissive.push(qual);
            } else {
                restrictive.push(qual);
            }
        }
        Ok(Self(RowSecurity::Restricted {
            relation,
            qual: combine_policy_quals(permissive, restrictive),
        }))
    }

    /// Whether a row this statement is about to write satisfies the policy.
    ///
    /// # Errors
    ///
    /// Returns 42501 when `row_security = off` and a policy would have applied,
    /// or an evaluation error from the qual.
    pub fn permits_row(
        &self,
        table: &Table,
        row: &[Datum],
        ctx: &crate::clock::EvalCtx,
    ) -> Result<bool, ExecError> {
        match &self.0 {
            RowSecurity::Open => Ok(true),
            RowSecurity::Refuse { relation } => {
                Err(ExecError::RowSecurityRefused(relation.clone()))
            }
            RowSecurity::Restricted { relation, qual } => {
                crate::exec::row_matches(Some(qual), &Scope::single(table, relation), row, ctx)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::{
        Column, RelationName, RoleAttributes, Table,
        policy::{Policy, PolicyCommand},
    };
    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgtypes::ColumnType;

    use super::{
        RlsCtx, RowSecurity, RowSecurityCheck, UnrestrictedTable, combine_policy_quals, decide,
        sanitize_scan_plan,
    };

    const OWNER: &str = "owner_role";

    fn table(row_security: bool, force_row_security: bool) -> Table {
        Table {
            id: 42,
            name: RelationName::new("public", "document"),
            owner: OWNER.into(),
            columns: vec![Column::new("id", ColumnType::Int4)],
            sharded: false,
            row_security,
            force_row_security,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        }
    }

    fn policy(name: &str, permissive: bool, using: &str, roles: &[&str]) -> Policy {
        Policy {
            oid: 0,
            name: name.into(),
            table_id: 42,
            command: PolicyCommand::All,
            permissive,
            roles: roles.iter().map(|role| (*role).to_string()).collect(),
            using: Some(using.into()),
            with_check: None,
        }
    }

    fn store(policies: &[Policy]) -> MemKv {
        let kv = MemKv::new();
        for policy in policies {
            let ops = crabka_pgcatalog::policy::create_policy_ops(&kv, policy).expect("create");
            kv.write_batch(&ops).expect("apply");
        }
        kv
    }

    fn role(kv: &MemKv, name: &str, attributes: RoleAttributes) {
        member_role(kv, name, attributes, &[]);
    }

    fn member_role(kv: &MemKv, name: &str, attributes: RoleAttributes, member_of: &[&str]) {
        let member_of: Vec<String> = member_of.iter().map(|role| (*role).to_string()).collect();
        let ops = crabka_pgcatalog::create_role_with_memberships_ops(
            kv, name, true, attributes, &member_of,
        )
        .expect("create role");
        kv.write_batch(&ops).expect("apply");
    }

    /// Render a decision as a short label so a table-driven case states one
    /// expectation rather than a match arm.
    fn label(decision: &RowSecurity) -> &'static str {
        match decision {
            RowSecurity::Open => "open",
            RowSecurity::Restricted { .. } => "restricted",
            RowSecurity::Refuse { .. } => "refuse",
        }
    }

    /// The dormancy invariant, at the level the decision is made: with the
    /// relation's `row_security` flag clear, a deny-everything policy, a role
    /// that owns nothing and holds no attributes, and the GUC either way, the
    /// answer is always an unconditional bypass.
    #[test]
    fn a_relation_without_row_security_always_bypasses() {
        let kv = store(&[policy("deny", true, "false", &[])]);
        role(&kv, "stranger", RoleAttributes::default());
        for guc in [true, false] {
            let ctx = RlsCtx::new(&kv, "stranger", guc);
            let decision =
                decide(&ctx, &table(false, false), PolicyCommand::Select).expect("decide");
            assert!(decision == RowSecurity::Open);
            assert!(UnrestrictedTable::from_decision(&decision, &table(false, false)).is_some());
        }
    }

    /// An empty policy list is never `Open`: row security with nothing
    /// applicable denies every row, and stays a `Restricted` decision so no
    /// pushdown can mistake it for a bypass.
    #[test]
    fn row_security_with_no_policies_is_restricted_not_open() {
        let kv = store(&[]);
        role(&kv, "stranger", RoleAttributes::default());
        let ctx = RlsCtx::new(&kv, "stranger", true);
        let decision = decide(&ctx, &table(true, false), PolicyCommand::Select).expect("decide");
        assert!(label(&decision) == "restricted");
        assert!(UnrestrictedTable::from_decision(&decision, &table(true, false)).is_none());
    }

    #[test]
    fn bypass_matrix() {
        struct Case {
            name: &'static str,
            role: &'static str,
            attributes: RoleAttributes,
            force: bool,
            guc: bool,
            expected: &'static str,
        }
        let superuser = {
            let mut attributes = RoleAttributes::default();
            attributes.set(crabka_pgcatalog::RoleAttribute::Superuser, true);
            attributes
        };
        let bypass = {
            let mut attributes = RoleAttributes::default();
            attributes.set(crabka_pgcatalog::RoleAttribute::BypassRls, true);
            attributes
        };
        let cases = [
            Case {
                name: "owner without FORCE reads unfiltered",
                role: OWNER,
                attributes: RoleAttributes::default(),
                force: false,
                guc: true,
                expected: "open",
            },
            Case {
                name: "owner with FORCE is filtered like anyone else",
                role: OWNER,
                attributes: RoleAttributes::default(),
                force: true,
                guc: true,
                expected: "restricted",
            },
            Case {
                name: "a member of the owning role owns it too",
                role: "member",
                attributes: RoleAttributes::default(),
                force: false,
                guc: true,
                expected: "open",
            },
            Case {
                name: "BYPASSRLS beats FORCE",
                role: "bypasser",
                attributes: bypass,
                force: true,
                guc: true,
                expected: "open",
            },
            Case {
                name: "SUPERUSER beats FORCE",
                role: "root",
                attributes: superuser,
                force: true,
                guc: true,
                expected: "open",
            },
            Case {
                name: "a stranger is filtered",
                role: "stranger",
                attributes: RoleAttributes::default(),
                force: false,
                guc: true,
                expected: "restricted",
            },
            Case {
                name: "row_security = off refuses rather than filters",
                role: "stranger",
                attributes: RoleAttributes::default(),
                force: false,
                guc: false,
                expected: "refuse",
            },
            Case {
                name: "row_security = off still bypasses for an exempt role",
                role: "bypasser",
                attributes: bypass,
                force: true,
                guc: false,
                expected: "open",
            },
        ];
        for case in cases {
            let kv = store(&[policy("visible", true, "id > 0", &[])]);
            role(&kv, OWNER, RoleAttributes::default());
            if case.role != OWNER {
                let member_of: &[&str] = if case.role == "member" { &[OWNER] } else { &[] };
                member_role(&kv, case.role, case.attributes, member_of);
            }
            let ctx = RlsCtx::new(&kv, case.role, case.guc);
            let decision =
                decide(&ctx, &table(true, case.force), PolicyCommand::Select).expect("decide");
            assert!(label(&decision) == case.expected, "{}", case.name);
        }
    }

    /// A policy's `TO` list is matched by inherited privilege, and an empty list
    /// is `PUBLIC`.
    #[test]
    fn policy_role_matching() {
        struct Case {
            name: &'static str,
            roles: &'static [&'static str],
            expected: &'static str,
        }
        let cases = [
            Case {
                name: "an empty TO list is PUBLIC and always applies",
                roles: &[],
                expected: "id > 0",
            },
            Case {
                name: "a role named directly applies",
                roles: &["reader"],
                expected: "id > 0",
            },
            Case {
                name: "a role inherited through membership applies",
                roles: &["readers"],
                expected: "id > 0",
            },
            Case {
                name: "an unrelated role does not apply",
                roles: &["writers"],
                expected: "denied",
            },
        ];
        for case in cases {
            let kv = store(&[policy("visible", true, "id > 0", case.roles)]);
            role(&kv, OWNER, RoleAttributes::default());
            role(&kv, "readers", RoleAttributes::default());
            role(&kv, "writers", RoleAttributes::default());
            member_role(&kv, "reader", RoleAttributes::default(), &["readers"]);
            let ctx = RlsCtx::new(&kv, "reader", true);
            let RowSecurity::Restricted { qual, .. } =
                decide(&ctx, &table(true, false), PolicyCommand::Select).expect("decide")
            else {
                panic!("{}: expected a restricted decision", case.name);
            };
            let rendered = format!("{qual:?}");
            let applied = if rendered.contains("Gt") {
                "id > 0"
            } else {
                "denied"
            };
            assert!(applied == case.expected, "{}", case.name);
        }
    }

    /// A policy whose command does not match the statement's is not applied.
    #[test]
    fn policy_command_matching() {
        for (policy_command, statement, applies) in [
            (PolicyCommand::All, PolicyCommand::Select, true),
            (PolicyCommand::Select, PolicyCommand::Select, true),
            (PolicyCommand::Insert, PolicyCommand::Select, false),
            (PolicyCommand::Update, PolicyCommand::Delete, false),
            (PolicyCommand::Delete, PolicyCommand::Delete, true),
        ] {
            let mut stored = policy("visible", true, "id > 0", &[]);
            stored.command = policy_command;
            let kv = store(&[stored]);
            role(&kv, "stranger", crabka_pgcatalog::RoleAttributes::default());
            let ctx = RlsCtx::new(&kv, "stranger", true);
            let RowSecurity::Restricted { qual, .. } =
                decide(&ctx, &table(true, false), statement).expect("decide")
            else {
                panic!("expected a restricted decision");
            };
            assert!(format!("{qual:?}").contains("Gt") == applies);
        }
    }

    fn qual(sql: &str) -> crabka_pgparser::ast::Expr {
        crabka_pgparser::parser::parse_expression(sql).expect("parse")
    }

    /// The fold, stated as the four facts that make default-deny structural.
    #[test]
    fn qual_folds() {
        struct Case {
            name: &'static str,
            permissive: Vec<&'static str>,
            restrictive: Vec<&'static str>,
            /// The rows of `id` in `1..=4` the folded qual admits.
            admits: Vec<i32>,
        }
        let cases = [
            Case {
                name: "no policy at all denies every row",
                permissive: vec![],
                restrictive: vec![],
                admits: vec![],
            },
            Case {
                name: "one permissive policy admits its own rows",
                permissive: vec!["id > 2"],
                restrictive: vec![],
                admits: vec![3, 4],
            },
            Case {
                name: "permissive policies OR together",
                permissive: vec!["id = 1", "id = 4"],
                restrictive: vec![],
                admits: vec![1, 4],
            },
            Case {
                name: "restrictive policies AND onto the permissive result",
                permissive: vec!["id > 1"],
                restrictive: vec!["id < 4"],
                admits: vec![2, 3],
            },
            Case {
                name: "restrictive policies AND together",
                permissive: vec!["true"],
                restrictive: vec!["id > 1", "id < 4"],
                admits: vec![2, 3],
            },
            Case {
                name: "a restrictive policy alone still denies everything",
                permissive: vec![],
                restrictive: vec!["true"],
                admits: vec![],
            },
        ];
        let table = table(true, false);
        let scope = crate::scope::Scope::single(&table, "document");
        let ctx = crate::clock::EvalCtx::test_default();
        for case in cases {
            let folded = combine_policy_quals(
                case.permissive.iter().map(|sql| qual(sql)).collect(),
                case.restrictive.iter().map(|sql| qual(sql)).collect(),
            );
            let admitted: Vec<i32> = (1..=4)
                .filter(|id| {
                    crate::exec::row_matches(
                        Some(&folded),
                        &scope,
                        &[crabka_pgtypes::Datum::Int4(*id)],
                        &ctx,
                    )
                    .expect("evaluate")
                })
                .collect();
            assert!(admitted == case.admits, "{}", case.name);
        }
    }

    /// A qual whose value is NULL hides the row, as `PostgreSQL`'s policy
    /// evaluation does — the opposite of a `CHECK` constraint.
    #[test]
    fn a_null_qual_hides_the_row() {
        let table = table(true, false);
        let scope = crate::scope::Scope::single(&table, "document");
        let ctx = crate::clock::EvalCtx::test_default();
        let folded = combine_policy_quals(vec![qual("id > NULL")], Vec::new());
        assert!(
            !crate::exec::row_matches(
                Some(&folded),
                &scope,
                &[crabka_pgtypes::Datum::Int4(1)],
                &ctx
            )
            .expect("evaluate")
        );
    }

    /// A policy qual that probes table privileges is refused rather than
    /// compiled: those functions return true unconditionally today, so the
    /// policy would admit every row to every role.
    #[test]
    fn a_privilege_probing_qual_is_refused() {
        for source in [
            "has_table_privilege('document', 'SELECT')",
            "id > 0 AND HAS_COLUMN_PRIVILEGE('document', 'id', 'SELECT')",
            "EXISTS (SELECT 1 WHERE has_any_column_privilege('document', 'SELECT'))",
        ] {
            let kv = store(&[policy("probe", true, source, &[])]);
            role(&kv, "stranger", crabka_pgcatalog::RoleAttributes::default());
            let ctx = RlsCtx::new(&kv, "stranger", true);
            let error = decide(&ctx, &table(true, false), PolicyCommand::Select)
                .expect_err("privilege probe should be refused");
            assert!(error.into_pg().message.contains("not enforced yet"));
        }
    }

    /// The pushdowns a relation under row security may not keep, and the one it
    /// may.
    #[test]
    fn scan_plan_sanitizer() {
        let predicate =
            crate::scanner::PredicatePushdown::Conjunctive(vec![crate::scanner::ColumnPredicate {
                column: 0,
                op: crate::scanner::PredicateOp::Eq,
                value: crabka_pgtypes::Datum::Int4(7),
            }]);
        let plan = crate::plan_dist::DistributedScanPlan {
            predicate: predicate.clone(),
            projection: crate::ProjectionPushdown::Columns(vec![0]),
            partial_aggregate: Some(crate::PartialAggregateSpec {
                function: crate::PartialAggregateFunction::Count,
                column: None,
                group_by: Vec::new(),
            }),
            top_k: Some(crate::TopKSpec {
                order_by: vec![crate::TopKColumn {
                    column: 0,
                    asc: true,
                }],
                limit: 3,
            }),
            text_search: None,
        };
        assert!(sanitize_scan_plan(&RowSecurity::Open, &plan).is_none());
        let restricted = RowSecurity::Restricted {
            relation: "document".into(),
            qual: qual("id > 0"),
        };
        let sanitized = sanitize_scan_plan(&restricted, &plan).expect("plan is rewritten");
        assert!(
            sanitized
                == crate::plan_dist::DistributedScanPlan {
                    predicate,
                    projection: crate::ProjectionPushdown::All,
                    partial_aggregate: None,
                    top_k: None,
                    text_search: None,
                }
        );
    }

    /// The write-side `WITH CHECK` qual falls back to `USING` when a policy has
    /// no `WITH CHECK` of its own, and a row is judged by it the same way a read
    /// is.
    #[test]
    fn write_check_falls_back_to_using() {
        let kv = store(&[policy("visible", true, "id > 2", &[])]);
        role(&kv, "stranger", crabka_pgcatalog::RoleAttributes::default());
        let ctx = RlsCtx::new(&kv, "stranger", true);
        let table = table(true, false);
        let check =
            RowSecurityCheck::compile(&ctx, &table, PolicyCommand::Insert).expect("compile");
        let eval = crate::clock::EvalCtx::test_default();
        assert!(
            !check
                .permits_row(&table, &[crabka_pgtypes::Datum::Int4(1)], &eval)
                .expect("evaluate")
        );
        assert!(
            check
                .permits_row(&table, &[crabka_pgtypes::Datum::Int4(3)], &eval)
                .expect("evaluate")
        );
    }
}
