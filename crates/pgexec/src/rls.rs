//! Row-level security: the one place raw rows of a stored relation may be
//! turned into a relation the rest of the executor can see.
//!
//! Every decision keys off [`crabka_pgcatalog::Table::row_security`], which
//! `ALTER TABLE … ENABLE ROW LEVEL SECURITY` sets and `CREATE POLICY` fills in
//! around. Reads pass through [`apply_row_security`]; writes pass through
//! [`RowSecurityUsing`] on the rows they may act on and [`RowSecurityCheck`] on
//! the rows they leave behind.
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
//! [`RowSecurity::Open`] decision *and* a [`crate::privilege::ReadPermit`].
//! Both properties are enforced by the compiler, not by remembering to call
//! something.
//!
//! The permit is the second half, and it was added after the first half had
//! already caught the same bug once: `SELECT count(*)` reaches the aggregate
//! pushdown, which proved the relation unrestricted by row security and then
//! read it without anyone having asked whether the session could read it at
//! all. Making the privilege proof part of the same value is why that shape
//! cannot come back.
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
    /// Check that the session may read `table` at all, decide row security for
    /// it, and admit it only if both answers were an explicit bypass.
    ///
    /// The privilege side declines by returning `None` rather than by raising
    /// 42501. Every caller of this function is an optimizer fast path whose
    /// fallback is the ordinary gated read, and that read raises the denial
    /// itself, in the order `PostgreSQL` raises it and naming the relation the
    /// query actually named. Declining here is therefore fail-closed twice
    /// over: the pushdown does not happen, and the path that does happen is the
    /// one that checks.
    ///
    /// # Errors
    ///
    /// Returns storage/corruption errors from the catalog KV seam, or a
    /// refusal to compile an unsafe policy qual.
    pub(crate) fn read(
        privileges: &crate::privilege::PrivilegeCtx<'_>,
        ctx: &RlsCtx<'_>,
        table: &'a Table,
    ) -> Result<Option<Self>, ExecError> {
        let Some(permit) = crate::privilege::ReadPermit::offer(privileges, table)? else {
            return Ok(None);
        };
        Ok(Self::from_decision(
            &permit,
            &decide(ctx, table, PolicyCommand::Select)?,
            table,
        ))
    }

    /// Admit `table` when `decision` is an explicit bypass, for a caller that
    /// holds a permit already and has already made the row-security decision.
    ///
    /// The permit is taken by reference and never read: it is there so this
    /// constructor cannot be reached without one, which is what makes
    /// "unrestricted" mean *both* freedoms rather than only the row-security
    /// one. Six optimizer pushdowns take an `UnrestrictedTable`; a seventh
    /// written the same way is safe by construction.
    pub(crate) const fn from_decision(
        _permit: &crate::privilege::ReadPermit,
        decision: &RowSecurity,
        table: &'a Table,
    ) -> Option<Self> {
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
        self.rows.extend(
            child
                .rows
                .into_iter()
                .map(|row| crate::exec::permuted_row(&row, ordinals)),
        );
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
            let qual = crate::bind::BoundExpr::new(&qual, &scope);
            let mut kept = Vec::with_capacity(rows.len());
            for row in rows {
                if crate::exec::row_matches(Some(qual.expr()), &scope, &row, read_ctx.eval_ctx)? {
                    kept.push(row);
                }
            }
            Ok(Relation { scope, rows: kept })
        }
    }
}

/// The predicate a locking read judges a row by *before* it locks it.
///
/// [`apply_row_security`] is the gate for a scan that can hand the gate a
/// finished row set. A locking read cannot: it takes the row lock the moment it
/// accepts a row, and a lock is observable — `NOWAIT` reports it, `SKIP LOCKED`
/// steps around it, and an ordinary waiter blocks on it. Filtering afterwards
/// would return the right rows and still have told the caller which hidden ones
/// exist, so the locking read compiles the same policies into a predicate and
/// asks it first.
///
/// `PostgreSQL` judges the row against the `UPDATE` policies as well as the
/// `SELECT` ones, at every lock strength. Measured on 18.4, a relation with a
/// `FOR SELECT` policy admitting one row and a `FOR UPDATE` policy admitting a
/// different one returns nothing to any of `FOR UPDATE`, `FOR NO KEY UPDATE`,
/// `FOR SHARE` or `FOR KEY SHARE`; so does a relation with a `FOR SELECT`
/// policy and no `UPDATE` policy at all, whose empty permissive fold denies
/// every row.
pub(crate) struct LockingReadGate(Option<Expr>);

impl LockingReadGate {
    /// Compile `governing`'s policies for a locking read of its rows.
    ///
    /// # Errors
    ///
    /// Returns 42501 when `row_security = off` and a policy would have applied,
    /// 42P17 when a policy qual reads the relation its own policy protects, and
    /// storage/parse errors from compiling the quals.
    pub(crate) fn compile(
        read_ctx: &crate::subquery::SubCtx<'_>,
        governing: &Table,
    ) -> Result<Self, ExecError> {
        let mut quals = Vec::new();
        for command in [PolicyCommand::Select, PolicyCommand::Update] {
            match decide(&read_ctx.rls(), governing, command)? {
                RowSecurity::Open => {}
                RowSecurity::Refuse { relation } => {
                    return Err(ExecError::RowSecurityRefused(relation));
                }
                RowSecurity::Restricted { relation, qual } => {
                    // See `apply_row_security`: a qual that reads its own
                    // relation is reported rather than recursed into, and the
                    // subqueries inside it are resolved once per scan, under
                    // the guard that catches exactly that.
                    if read_ctx.policy_stack.holds(governing.id) {
                        return Err(ExecError::PolicyRecursion(relation));
                    }
                    let _entered = read_ctx.policy_stack.enter(governing.id);
                    quals.push(crate::subquery::resolve_expr(read_ctx, &qual)?);
                }
            }
        }
        Ok(Self(
            quals
                .into_iter()
                .reduce(|left, right| binary(BinaryOp::And, left, right)),
        ))
    }

    /// The predicate, or `None` when no policy applies to this read.
    pub(crate) fn qual(&self) -> Option<&Expr> {
        self.0.as_ref()
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

/// Whether `role` is the cluster superuser, for the ownership tests that
/// `PostgreSQL` lets a superuser pass.
///
/// Deliberately *not* [`role_is_exempt`], which also admits `BYPASSRLS`.
/// Bypassing a policy and being allowed to delete one are different powers, and
/// conflating them would let any `BYPASSRLS` role strip another role's
/// protection rather than merely see past it.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub(crate) fn role_is_superuser(kv: &dyn Kv, role: &str) -> Result<bool, ExecError> {
    if role == crabka_pgcatalog::BOOTSTRAP_ROLE {
        return Ok(true);
    }
    match crabka_pgcatalog::get_role(kv, role) {
        Ok(role) => Ok(role.attributes.has(RoleAttribute::Superuser)),
        Err(crabka_pgcatalog::CatalogError::UndefinedObject(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// Refuse, at `CREATE`/`ALTER POLICY` time, a qual row security cannot evaluate
/// safely yet.
///
/// Validating here rather than at first enforcement means a policy that could
/// never be applied is a DDL error the author sees, not a surprise on somebody
/// else's later `SELECT`.
///
/// # Errors
///
/// Returns 0A000 for a qual that probes table privileges, or a parse error.
pub(crate) fn validate_policy_qual(source: &str) -> Result<(), ExecError> {
    compile_qual(source).map(|_| ())
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
        // These few still return true unconditionally (see `catalog_fn`), and a
        // policy written around one would therefore admit every row to every
        // role — the exact leak this module exists to prevent.
        return Err(ExecError::Unsupported(format!(
            "row-level-security policy qual uses {probe}, which is not enforced yet"
        )));
    }
    Ok(crabka_pgparser::parser::parse_expression(source)?)
}

/// The privilege functions a policy qual still may not name.
///
/// This was once the whole `has_*_privilege` family, because the whole family
/// answered `true`. The relation-scoped members now answer from the grants
/// `GRANT` wrote — see [`crate::privilege`] — so a policy may use them, and
/// upstream `PostgreSQL` policies do. What is left is the object kinds this
/// catalog stores no ACL for at all: a database, a language, a tablespace, a
/// type, a foreign server or wrapper, a large object, a routine, a sequence, a
/// configuration parameter. For those the function is still a constant `true`,
/// so a policy resting on one would silently admit everything, and refusing it
/// at `CREATE POLICY` is the fail-closed answer.
const UNENFORCED_PRIVILEGE_FUNCTIONS: &[&str] = &[
    "has_database_privilege",
    "has_schema_privilege",
    "has_sequence_privilege",
    "has_function_privilege",
    "has_language_privilege",
    "has_server_privilege",
    "has_foreign_data_wrapper_privilege",
    "has_tablespace_privilege",
    "has_type_privilege",
    "has_parameter_privilege",
    "has_largeobject_privilege",
];

/// The first still-unenforced privilege function named anywhere in a qual's
/// source.
///
/// Deliberately textual, and deliberately over-eager. The expression walkers in
/// `exec` do not descend into a subquery's own clauses, so a tree walk would
/// miss `EXISTS (SELECT … WHERE has_function_privilege(…))` — the one shape most
/// worth catching. Matching the source text over-rejects (a column that happens
/// to be named after one of these functions is refused too) and never
/// under-rejects, which is the direction a security check should fail in.
fn privilege_probe(source: &str) -> Option<&'static str> {
    let lowered = source.to_ascii_lowercase();
    UNENFORCED_PRIVILEGE_FUNCTIONS
        .iter()
        .copied()
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
        let qual = crate::bind::BoundExpr::new(qual, &scope);
        let mut kept = Vec::with_capacity(rows.len());
        for (rowid, xmin, row) in rows {
            if crate::exec::row_matches(Some(qual.expr()), &scope, &row, ctx)? {
                kept.push((rowid, xmin, row));
            }
        }
        Ok(kept)
    }
}

/// The two write-side checks `PostgreSQL` runs. They differ in both the qual
/// they read and the row they name in the error, and the two always vary
/// together — so they are one value rather than two booleans a caller could
/// pair the wrong way round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckSubject {
    /// The row the statement is about to write. Reads each policy's `WITH
    /// CHECK`, falling back to its `USING` when it declares none — so a policy
    /// that hides a row also forbids writing one it would hide.
    NewRow,
    /// The stored row an `INSERT … ON CONFLICT DO UPDATE` found and is about to
    /// change. Reads the `USING` qual whatever the policy's `WITH CHECK` says,
    /// because the question is "may I see this row", not "may I write this
    /// one" — and failing it is an error, not a skip, since silently declining
    /// to update would tell the caller the row is not there.
    TargetRow,
}

impl CheckSubject {
    /// The source text this subject checks against, and whether the error
    /// message calls it a `USING` expression.
    ///
    /// The two questions are not the same, and `PostgreSQL` is explicit about
    /// the difference (`src/test/regress/expected/rowsecurity.out`): a policy
    /// that declares no `WITH CHECK` has its `USING` qual read as the check,
    /// but the violation is *not* reported as a `USING` expression, "because
    /// it's an RLS UPDATE check that originated as a USING qual … as opposed to
    /// an explicit USING qual that is ordinarily a security barrier". Only the
    /// second — the explicit `USING` judged against a row the statement found —
    /// is named that way.
    fn qual_of(self, policy: &Policy) -> Option<(&str, bool)> {
        match self {
            Self::NewRow => match policy.with_check.as_deref() {
                Some(source) => Some((source, false)),
                None => policy.using.as_deref().map(|source| (source, false)),
            },
            Self::TargetRow => policy.using.as_deref().map(|source| (source, true)),
        }
    }
}

/// Why a write may skip its row-security check.
///
/// Every exemption is a named variant rather than a bare `None`, so the set of
/// writes that reach storage unchecked is one enum long and each one carries
/// its own justification.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CheckExemption {
    /// `DELETE` removes a row rather than writing one, and `PostgreSQL`'s
    /// policies carry no `WITH CHECK` for it. The `DELETE` `USING` qual has
    /// already judged the row: for a plain `DELETE` as a filter at
    /// `write_candidate_rows`, and for a `MERGE` as a per-row
    /// [`CheckSubject::TargetRow`] check that raises. This exemption is about
    /// the *new* row, of which a delete has none.
    RemovesRows,
    /// A referential action (`ON DELETE CASCADE`, `ON UPDATE SET NULL`, …),
    /// which `PostgreSQL` runs as the referenced relation's owner with row
    /// security off — the check would otherwise let a policy break referential
    /// integrity.
    ReferentialAction,
    /// A sharded relation, whose writes go through the timestamp path and carry
    /// no session context to decide a policy under.
    ///
    /// Backed by a refusal rather than a hope: [`refuse_sharded_row_security`]
    /// rejects `ALTER TABLE … ENABLE ROW LEVEL SECURITY` on a sharded relation,
    /// and the timestamp write path re-asserts it, so this exemption can never
    /// apply to a relation that has policies.
    ShardedRelation,
}

/// Refuse to put a sharded relation under row security.
///
/// A sharded write is planned by the timestamp path, which has no read context
/// to evaluate a policy qual against. Rather than let such a relation carry a
/// flag nothing enforces — a silent total bypass — the flag cannot be set.
///
/// # Errors
///
/// Returns 0A000 when `table` is sharded.
pub(crate) fn refuse_sharded_row_security(table: &Table) -> Result<(), ExecError> {
    if table.sharded {
        return Err(ExecError::Unsupported(format!(
            "row-level security on sharded relation \"{}\" is not supported",
            table.name.name
        )));
    }
    Ok(())
}

/// One policy's contribution to a write-side check, kept separable so the
/// error can name the policy that rejected the row the way `PostgreSQL` does.
struct PolicyCheckQual {
    qual: Expr,
    /// The policy's name, when exactly one policy produced this qual.
    /// `PostgreSQL` leaves the name out of a violation folded from several.
    policy: Option<String>,
    /// The qual came from the policy's `USING` clause.
    from_using: bool,
}

enum CheckPlan {
    Open,
    Refuse { relation: String },
    Restricted(Box<RestrictedCheck>),
}

/// The compiled quals a write must satisfy, and how a failure names itself.
struct RestrictedCheck {
    relation: String,
    /// Which row this check judges, which is also how the violation names it.
    subject: CheckSubject,
    /// The permissive policies' quals, OR-folded into one.
    permissive: PolicyCheckQual,
    /// The restrictive policies, each checked on its own so the violation names
    /// the one that rejected the row.
    restrictive: Vec<PolicyCheckQual>,
}

/// The `WITH CHECK` qual a row a statement writes must satisfy.
///
/// `PostgreSQL` runs this **after** `BEFORE ROW` triggers, because a `BEFORE`
/// trigger returns a *replacement* row and that replacement is what gets
/// written; a check that ran before the trigger would let a trigger launder a
/// row past its own policy. That is why the only caller is
/// [`crate::trigger::fire_before_row`], which holds the row the write will
/// actually use — putting it in `finish_written_row`, the obvious-looking spot,
/// would ship the bug.
pub(crate) struct RowSecurityCheck(CheckPlan);

impl RowSecurityCheck {
    /// Decide the write-side check for `command` against `table`.
    ///
    /// # Errors
    ///
    /// Returns catalog errors, or a refusal to compile an unsafe policy qual.
    pub(crate) fn compile(
        ctx: &RlsCtx<'_>,
        table: &Table,
        command: PolicyCommand,
        subject: CheckSubject,
    ) -> Result<Self, ExecError> {
        if bypass_applies(ctx, table)? {
            return Ok(Self(CheckPlan::Open));
        }
        let relation = table.name.name.clone();
        let applicable = applicable_policies(ctx, table, command)?;
        if !ctx.row_security && !applicable.is_empty() {
            return Ok(Self(CheckPlan::Refuse { relation }));
        }
        let mut permissive = Vec::new();
        let mut restrictive = Vec::new();
        for policy in applicable {
            let Some((source, from_using)) = subject.qual_of(&policy) else {
                continue;
            };
            let checked = PolicyCheckQual {
                qual: compile_qual(source)?,
                policy: Some(policy.name.clone()),
                from_using,
            };
            if policy.permissive {
                permissive.push(checked);
            } else {
                restrictive.push(checked);
            }
        }
        Ok(Self(CheckPlan::Restricted(Box::new(RestrictedCheck {
            relation,
            subject,
            permissive: fold_permissive_checks(permissive),
            restrictive,
        }))))
    }

    /// A check for a write that row security does not reach, for the stated
    /// reason.
    pub(crate) const fn exempt(_why: CheckExemption) -> Self {
        Self(CheckPlan::Open)
    }

    /// Raise 42501 unless the row this statement is about to write satisfies
    /// the relation's policies.
    ///
    /// # Errors
    ///
    /// Returns 42501 when the row fails a policy, or when `row_security = off`
    /// and a policy would have applied; and evaluation errors from the qual.
    pub(crate) fn permit_row(
        &self,
        table: &Table,
        row: &[Datum],
        ctx: &crate::clock::EvalCtx,
    ) -> Result<(), ExecError> {
        let restricted = match &self.0 {
            CheckPlan::Open => return Ok(()),
            CheckPlan::Refuse { relation } => {
                return Err(ExecError::RowSecurityRefused(relation.clone()));
            }
            CheckPlan::Restricted(restricted) => restricted.as_ref(),
        };
        let RestrictedCheck {
            relation,
            subject,
            permissive,
            restrictive,
        } = restricted;
        let scope = Scope::single(table, relation);
        // The permissive fold first, then each restrictive policy on its own:
        // PostgreSQL reports the first check the row fails, and a restrictive
        // policy can only ever be the reason a row that passed the permissive
        // fold is still rejected.
        for checked in std::iter::once(permissive).chain(restrictive) {
            if !crate::exec::row_matches(Some(&checked.qual), &scope, row, ctx)? {
                return Err(ExecError::RowSecurityCheckViolation {
                    relation: relation.clone(),
                    policy: checked.policy.clone(),
                    using_expression: checked.from_using,
                    target_row: *subject == CheckSubject::TargetRow,
                });
            }
        }
        Ok(())
    }
}

/// Every per-row check a write must pass before its row reaches storage.
///
/// Two checks with the same shape and the same evaluation point, bundled so no
/// write path can apply one and forget the other: the relation's row-security
/// `WITH CHECK`, and the `WITH CHECK OPTION` of each view the statement was
/// rewritten through. Both judge the row *after* `BEFORE ROW` triggers have had
/// their say — see [`RowSecurityCheck`] for why that timing is not negotiable —
/// so [`crate::trigger::fire_before_row`] is the only caller of either.
pub(crate) struct WriteChecks {
    security: RowSecurityCheck,
    /// Innermost view first, which is the order `PostgreSQL` reports a row that
    /// fails more than one view's option.
    views: Vec<crate::viewwrite::ViewCheck>,
}

impl WriteChecks {
    /// A write that reaches no view, carrying only the relation's own policies.
    pub(crate) const fn plain(security: RowSecurityCheck) -> Self {
        Self {
            security,
            views: Vec::new(),
        }
    }

    /// The same, plus the check options collected while rewriting through a
    /// chain of views.
    pub(crate) fn through_views(
        security: RowSecurityCheck,
        views: Vec<crate::viewwrite::ViewCheck>,
    ) -> Self {
        Self { security, views }
    }

    /// A write that neither row security nor a view check reaches, for the
    /// stated reason.
    pub(crate) const fn exempt(why: CheckExemption) -> Self {
        Self::plain(RowSecurityCheck::exempt(why))
    }

    /// Raise unless the row this statement is about to write satisfies both.
    ///
    /// The view options run first, because a row that a view's own
    /// qualification excludes was never a row of that view — `PostgreSQL`
    /// reports the check-option violation rather than a policy one.
    ///
    /// # Errors
    ///
    /// 44000 for a failed check option, or whatever [`RowSecurityCheck::permit_row`]
    /// raises.
    pub(crate) fn permit_row(
        &self,
        table: &Table,
        row: &[Datum],
        ctx: &crate::clock::EvalCtx,
    ) -> Result<(), ExecError> {
        // The scope is built only when there is a check to evaluate against it.
        // `Scope::single` clones the relation's whole binding list, and this
        // runs once per written row on every write path in the engine — the
        // overwhelming majority of which reach no view at all.
        if !self.views.is_empty() {
            let scope = Scope::single(table, &table.name.name);
            for check in &self.views {
                if !crate::exec::row_matches(Some(&check.qual), &scope, row, ctx)? {
                    return Err(ExecError::ViewCheckOptionViolation {
                        view: check.view.clone(),
                        row: crate::viewwrite::failing_row(row, ctx),
                    });
                }
            }
        }
        self.security.permit_row(table, row, ctx)
    }
}

/// OR-fold the permissive checks into one nameless check.
///
/// Nameless even when exactly one policy contributed, which is `PostgreSQL`'s
/// deliberate choice and not an omission: failing the permissive fold means *no*
/// policy granted permission to write the row, rather than some particular
/// policy having been violated, so there is no one policy to blame. A
/// restrictive policy is the opposite — it is the only thing that can reject a
/// row the fold already admitted — and each keeps its own name.
///
/// The `FALSE` seed is the same identity [`combine_policy_quals`] relies on:
/// row security with nothing permissive applicable rejects every row without
/// anyone writing that case.
fn fold_permissive_checks(permissive: Vec<PolicyCheckQual>) -> PolicyCheckQual {
    let from_using = matches!(permissive.as_slice(), [only] if only.from_using);
    let qual = permissive
        .into_iter()
        .fold(Expr::BoolLiteral(false), |left, right| {
            binary(BinaryOp::Or, left, right.qual)
        });
    PolicyCheckQual {
        qual,
        policy: None,
        from_using,
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
            materialized: None,
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

    /// A permit for the relation, taken as its owner — these cases are about
    /// the row-security half of `UnrestrictedTable`, so the privilege half is
    /// satisfied the way it always is for an owner.
    fn owner_permit(kv: &MemKv) -> crate::privilege::ReadPermit {
        crate::privilege::ReadPermit::acquire(
            &crate::privilege::PrivilegeCtx::new(kv, OWNER),
            &table(false, false),
        )
        .expect("the owner may read its own relation")
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
            assert!(
                UnrestrictedTable::from_decision(
                    &owner_permit(&kv),
                    &decision,
                    &table(false, false)
                )
                .is_some()
            );
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
        assert!(
            UnrestrictedTable::from_decision(&owner_permit(&kv), &decision, &table(true, false))
                .is_none()
        );
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

    /// A policy qual may probe *table* privileges, because those functions now
    /// answer from the grants `GRANT` wrote rather than saying yes to everyone.
    ///
    /// The object kinds this catalog stores no ACL for are still refused: a
    /// policy resting on one of those would admit every row to every role,
    /// which is the leak the blanket refusal was there to stop.
    #[test]
    fn only_still_unenforced_privilege_probes_are_refused() {
        struct Case {
            source: &'static str,
            refused: bool,
        }
        let cases = [
            Case {
                source: "has_table_privilege('document', 'SELECT')",
                refused: false,
            },
            Case {
                source: "id > 0 AND HAS_COLUMN_PRIVILEGE('document', 'id', 'SELECT')",
                refused: false,
            },
            Case {
                source: "EXISTS (SELECT 1 WHERE has_any_column_privilege('document', 'SELECT'))",
                refused: false,
            },
            Case {
                source: "has_function_privilege('f()', 'EXECUTE')",
                refused: true,
            },
            Case {
                source: "EXISTS (SELECT 1 WHERE has_largeobject_privilege(1, 'SELECT'))",
                refused: true,
            },
        ];
        for case in cases {
            let kv = store(&[policy("probe", true, case.source, &[])]);
            role(&kv, "stranger", crabka_pgcatalog::RoleAttributes::default());
            let ctx = RlsCtx::new(&kv, "stranger", true);
            let decided = decide(&ctx, &table(true, false), PolicyCommand::Select);
            match decided {
                Ok(_) => assert!(!case.refused, "{} should be refused", case.source),
                Err(error) => {
                    assert!(case.refused, "{} should compile", case.source);
                    assert!(error.into_pg().message.contains("not enforced yet"));
                }
            }
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
    /// is — reported as an ordinary check violation, naming neither the policy
    /// nor a `USING` expression, exactly as `PostgreSQL` reports it.
    #[test]
    fn write_check_falls_back_to_using() {
        let kv = store(&[policy("visible", true, "id > 2", &[])]);
        role(&kv, "stranger", crabka_pgcatalog::RoleAttributes::default());
        let ctx = RlsCtx::new(&kv, "stranger", true);
        let table = table(true, false);
        let check = RowSecurityCheck::compile(
            &ctx,
            &table,
            PolicyCommand::Insert,
            crate::rls::CheckSubject::NewRow,
        )
        .expect("compile");
        let eval = crate::clock::EvalCtx::test_default();
        let rejected = check
            .permit_row(&table, &[crabka_pgtypes::Datum::Int4(1)], &eval)
            .expect_err("a row the USING qual hides may not be written");
        assert!(
            rejected.into_pg().message
                == "new row violates row-level security policy for table \"document\""
        );
        assert!(
            check
                .permit_row(&table, &[crabka_pgtypes::Datum::Int4(3)], &eval)
                .is_ok()
        );
    }
}
