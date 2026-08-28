//! Automatically updatable views — writing through a view that carries no
//! `INSTEAD OF` trigger.
//!
//! `PostgreSQL` lets an `INSERT`, `UPDATE` or `DELETE` name a view directly
//! when the view's body is simple enough that each of its rows stands for
//! exactly one row of one underlying relation. The statement is then *rewritten*
//! onto that relation: the view's output columns are replaced by the
//! expressions they were selected from, the view's own `WHERE` is folded into
//! the statement's, and the ordinary table write path does the rest. Everything
//! that path already knows — defaults, generated columns, constraints, row
//! triggers, referential actions, `RETURNING` — therefore applies unchanged,
//! which is the reason the rewrite happens here rather than a second write path
//! being grown beside it.
//!
//! The rule for "simple enough" is [`query_refusal`], and it is `PostgreSQL`'s
//! `view_query_is_auto_updatable` clause for clause, including the wording of
//! the `DETAIL` each failure reports and the order the clauses are tested in.
//! Both are observable: the `DETAIL` is the only thing that tells a user *which*
//! clause made their view read-only, and a view that is both `DISTINCT` and
//! grouped reports only `DISTINCT`.
//!
//! Three things here are easy to get wrong and are settled once:
//!
//! * **Updatability is a property of the view; assignability is a property of a
//!   column.** A view whose select list mixes plain column references with
//!   computed expressions is still updatable — only the computed columns refuse
//!   assignment. That is SQL:1999 feature T111, and it is why [`ColumnMap::base`]
//!   is an `Option` rather than the whole view being rejected.
//! * **The chain stops at the first relation that can be written directly.** A
//!   view over a view over a table folds into one rewrite; a view over a view
//!   that carries an `INSTEAD OF` trigger stops at that view, because from there
//!   the trigger, not this rewrite, performs the write. `WITH CHECK OPTION`
//!   stops there too, which is why a cascaded option above a trigger-bearing
//!   view checks nothing below it.
//! * **`WITH CHECK OPTION` is collected here and applied elsewhere.** The row a
//!   check judges is the one that reaches storage, known only after defaults and
//!   `BEFORE` triggers have run. The quals travel to that point on
//!   [`crate::rls::WriteChecks`].
//!
//! Column references are canonicalized as the chain is walked, so every
//! reference to the relation being written ends up qualified by one agreed name.
//! Each level writes its body against its own `FROM` item's name or alias, and
//! those names mean nothing to the level above; rewriting them all to a single
//! qualifier is what lets the levels compose by column name alone.

use crabka_pgcatalog::{RelationName, View, ViewCheckOption};
use crabka_pgkv::Kv;
use crabka_pgparser::ast::{
    BinaryOp, DistinctClause, Expr, QueryBody, QueryExpr, RelationRef, Returning, SelectItem,
    SelectStmt, SetExpr, Statement, TableExpr,
};

use crate::error::ExecError;

/// `PostgreSQL`'s detail for a body whose `FROM` is not one plain relation.
/// Four different shapes report it, so it is named once.
const NOT_SINGLE_RELATION: &str =
    "Views that do not select from a single table or view are not automatically updatable.";

/// Which of the three write commands a statement performs on a view.
///
/// Carried rather than inferred from the statement, because every message this
/// module raises is spelled differently for each and the spellings are not
/// derivable from one another — "insert into", "update", "delete from".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ViewCommand {
    Insert,
    Update,
    Delete,
}

/// One write performed on a view: the command, and whether a `MERGE` action
/// spelled it.
///
/// `MERGE` is not a fourth command. Each of its `WHEN` clauses inserts, updates
/// or deletes, and `PostgreSQL` judges the view's updatability once per command
/// the clause list can reach — which is why [`ViewWrite`] pairs a command with
/// a spelling rather than growing a variant. What the spelling changes is the
/// wording: an unassignable column reports "cannot merge into column", and the
/// hint offers only an `INSTEAD OF` trigger, because a rule cannot make a view
/// mergeable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ViewWrite {
    command: ViewCommand,
    merge: bool,
}

impl ViewWrite {
    /// A write the statement spelled as itself — a plain `INSERT`, `UPDATE` or
    /// `DELETE` naming the view.
    pub(crate) const fn direct(command: ViewCommand) -> Self {
        Self {
            command,
            merge: false,
        }
    }

    /// A write one `MERGE` `WHEN` clause performs.
    pub(crate) const fn merged(command: ViewCommand) -> Self {
        Self {
            command,
            merge: true,
        }
    }

    /// The command this write performs, which decides the privilege it needs.
    pub(crate) const fn command(self) -> ViewCommand {
        self.command
    }

    /// The `cannot … view "v"` message raised when the view is not
    /// automatically updatable.
    pub(crate) fn refusal(self, view: &str) -> String {
        match self.command {
            ViewCommand::Insert => format!("cannot insert into view \"{view}\""),
            ViewCommand::Update => format!("cannot update view \"{view}\""),
            ViewCommand::Delete => format!("cannot delete from view \"{view}\""),
        }
    }

    /// The `HINT` naming what a user could add to make the write work. Rules
    /// are offered even though this engine has none: the hint is advice about
    /// `PostgreSQL`-compatible SQL, not a report of what is implemented here.
    /// A `MERGE` action is offered no rule at all, because `PostgreSQL` refuses
    /// to merge into a relation that carries one.
    pub(crate) const fn hint(self) -> &'static str {
        match (self.command, self.merge) {
            (ViewCommand::Insert, false) => {
                "To enable inserting into the view, provide an INSTEAD OF INSERT trigger or an \
                 unconditional ON INSERT DO INSTEAD rule."
            }
            (ViewCommand::Update, false) => {
                "To enable updating the view, provide an INSTEAD OF UPDATE trigger or an \
                 unconditional ON UPDATE DO INSTEAD rule."
            }
            (ViewCommand::Delete, false) => {
                "To enable deleting from the view, provide an INSTEAD OF DELETE trigger or an \
                 unconditional ON DELETE DO INSTEAD rule."
            }
            (ViewCommand::Insert, true) => {
                "To enable inserting into the view using MERGE, provide an INSTEAD OF INSERT \
                 trigger."
            }
            (ViewCommand::Update, true) => {
                "To enable updating the view using MERGE, provide an INSTEAD OF UPDATE trigger."
            }
            (ViewCommand::Delete, true) => {
                "To enable deleting from the view using MERGE, provide an INSTEAD OF DELETE \
                 trigger."
            }
        }
    }

    /// `DELETE` names no columns, so it is the one write that does not require
    /// the view to have an updatable column at all.
    const fn assigns_columns(self) -> bool {
        !matches!(self.command, ViewCommand::Delete)
    }

    /// The `cannot … column "c" of view "v"` message for assigning to a column
    /// the base relation does not have.
    fn column_refusal(self, column: &str, view: &str) -> String {
        if self.merge {
            return format!("cannot merge into column \"{column}\" of view \"{view}\"");
        }
        match self.command {
            ViewCommand::Insert => {
                format!("cannot insert into column \"{column}\" of view \"{view}\"")
            }
            // A DELETE assigns nothing and so never reaches here; spelling it
            // as an update keeps the match total without an arm a later caller
            // could make reachable without noticing.
            ViewCommand::Update | ViewCommand::Delete => {
                format!("cannot update column \"{column}\" of view \"{view}\"")
            }
        }
    }
}

/// One output column of the view being written, resolved against the relation
/// the write lands on.
#[derive(Debug, Clone)]
pub(crate) struct ColumnMap {
    /// The column's name as the view presents it.
    pub(crate) name: String,
    /// What the column was selected from, rewritten to reference the relation
    /// the write lands on.
    pub(crate) expr: Expr,
    /// The column of that relation this one assigns to, when it is a plain
    /// reference to one. `None` for a computed column, which may be read
    /// through the view but never written.
    pub(crate) base: Option<String>,
    /// Why [`Self::base`] is `None`, for the message an assignment raises.
    refusal: &'static str,
}

/// One view's `WITH CHECK OPTION`, waiting for a row to judge.
///
/// The qual is written against the relation the write lands on, so it evaluates
/// in the same single-relation scope the row-security checks use.
#[derive(Debug, Clone)]
pub(crate) struct ViewCheck {
    /// The view whose option this is — the name the violation reports, which is
    /// not necessarily the view the statement named.
    pub(crate) view: String,
    pub(crate) qual: Expr,
}

/// One view of the chain, before the levels are composed.
struct Level {
    view: String,
    option: Option<ViewCheckOption>,
    /// The view's `WHERE`, canonicalized against the level below's columns.
    qual: Option<Expr>,
    /// This view's output columns, in terms of the level below's columns.
    columns: Vec<ColumnMap>,
}

/// A write through a view, resolved to the relation it lands on.
#[derive(Debug)]
pub(crate) struct ViewRewrite {
    /// The relation the rewritten statement targets: a table, or a view that
    /// carries an `INSTEAD OF` trigger for this write.
    pub(crate) target: RelationName,
    /// One entry per output column of the view the statement named, in order.
    pub(crate) columns: Vec<ColumnMap>,
    /// Every level's `WHERE`, to be `AND`ed into the statement's own.
    quals: Vec<Expr>,
    /// The check options to enforce on the row that reaches storage, innermost
    /// view first — the order `PostgreSQL` reports a row that fails more than
    /// one of them.
    pub(crate) checks: Vec<ViewCheck>,
    /// The role whose privileges and row-security policies decide the write on
    /// [`Self::target`]. A view runs its body as its owner unless it was created
    /// `security_invoker`, and the write the body implies runs as the same role.
    pub(crate) run_as: String,
}

impl ViewRewrite {
    /// The map entry for one of the view's output columns, or 42703.
    ///
    /// # Errors
    ///
    /// `PostgreSQL`'s undefined-column error for a name the view does not have.
    pub(crate) fn column(&self, name: &str) -> Result<&ColumnMap, ExecError> {
        self.columns
            .iter()
            .find(|column| column.name == name)
            .ok_or_else(|| ExecError::UndefinedColumn(name.to_string()))
    }

    /// The base column an assignment to `name` writes.
    ///
    /// # Errors
    ///
    /// 42703 for a name the view does not have, or `PostgreSQL`'s 0A000 refusal
    /// naming why the column cannot be assigned to.
    pub(crate) fn assignable(
        &self,
        name: &str,
        view: &str,
        write: ViewWrite,
    ) -> Result<String, ExecError> {
        let column = self.column(name)?;
        column
            .base
            .clone()
            .ok_or_else(|| ExecError::ViewColumnNotUpdatable {
                message: write.column_refusal(name, view),
                detail: column.refusal,
            })
    }

    /// The check quals, rewritten to evaluate against a row of `target` in the
    /// unqualified single-relation scope a written row is judged in.
    pub(crate) fn row_checks(&self, target: &str) -> Vec<ViewCheck> {
        self.checks
            .iter()
            .map(|check| ViewCheck {
                view: check.view.clone(),
                qual: requalify(&check.qual, target, None),
            })
            .collect()
    }

    /// Rewrite one expression the *statement* wrote, replacing each reference
    /// to one of the view's output columns with what the view selected it from.
    ///
    /// A reference qualified by the view's qualifier is rewritten wherever it
    /// appears, including inside a subquery, where it is an outer reference to
    /// the view. An unqualified one is rewritten only at the top level, because
    /// inside a subquery it resolves against that subquery's own `FROM` list
    /// first — and only when it names a view column, because otherwise it names
    /// a `FROM`/`USING` item of the statement.
    pub(crate) fn rewrite_statement_expr(&self, expr: &Expr, qualifier: &str) -> Expr {
        map_expr(expr, false, &mut |node, nested| {
            let Expr::Column { table, name } = node else {
                return None;
            };
            let names_view = match table.as_deref() {
                Some(written) => written == qualifier,
                None => !nested,
            };
            names_view
                .then(|| self.columns.iter().find(|column| &column.name == name))
                .flatten()
                .map(|column| column.expr.clone())
        })
    }

    /// Refuse a reference to a column the view does not project.
    ///
    /// `PostgreSQL` resolves a statement against the view's rowtype before it
    /// rewrites anything, so a name the view hides is 42703 there. Here the
    /// rewrite would otherwise hand the name to the relation underneath, which
    /// may well have a column of that name — the view's whole purpose can be to
    /// hide it.
    ///
    /// `strict` says whether an unqualified name can be judged at all: it can
    /// only when the statement has no `FROM`/`USING` list of its own for the
    /// name to have come from.
    ///
    /// # Errors
    ///
    /// 42703 naming the column.
    pub(crate) fn reject_foreign_columns(
        &self,
        expr: &Expr,
        qualifier: &str,
        strict: bool,
    ) -> Result<(), ExecError> {
        let mut missing = None;
        map_expr(expr, false, &mut |node, nested| {
            if let Expr::Column { table, name } = node {
                let judged = match table.as_deref() {
                    Some(written) => written == qualifier,
                    None => strict && !nested,
                };
                if judged && !self.columns.iter().any(|column| &column.name == name) {
                    missing.get_or_insert_with(|| name.clone());
                }
            }
            None
        });
        match missing {
            Some(name) => Err(ExecError::UndefinedColumn(name)),
            None => Ok(()),
        }
    }

    /// The 42703 a `RETURNING` list owes for naming a column the view does not
    /// project, or `Ok(())` when every reference it makes is the view's.
    ///
    /// [`Self::reject_foreign_columns`] guards the `WHERE` for the reason
    /// stated where it is called: substitution leaves an unknown name alone,
    /// and the base relation the rewrite puts underneath answers it. A
    /// `RETURNING` list was never guarded, so `UPDATE v SET c = … RETURNING
    /// hidden` handed back a column of the base relation to a caller holding
    /// nothing but the view — the leak a view exists to prevent, and one the
    /// caller could not get through `SELECT` by any spelling.
    ///
    /// The `OLD`/`NEW` image aliases are judged with the view's own qualifier.
    /// They name the same relation through a second spelling, so admitting them
    /// unjudged would leave `old.hidden` open once `hidden` is closed.
    pub(crate) fn reject_foreign_returning(
        &self,
        returning: Option<&Returning>,
        qualifier: &str,
        strict: bool,
    ) -> Result<(), ExecError> {
        let Some(returning) = returning else {
            return Ok(());
        };
        let images = [
            returning.old_alias.as_deref().unwrap_or("old"),
            returning.new_alias.as_deref().unwrap_or("new"),
        ];
        for item in &returning.items {
            match item {
                SelectItem::Expr { expr, alias: _ } => {
                    self.reject_foreign_columns(expr, qualifier, strict)?;
                    for image in images {
                        self.reject_foreign_columns(expr, image, true)?;
                    }
                }
                // `*` and `old.*` expand to the view's own columns, through
                // `wildcard_items` and through the image's declared columns.
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {}
            }
        }
        Ok(())
    }

    /// Rewrite an expression written in an `ON CONFLICT` clause.
    ///
    /// Same as [`Self::rewrite_statement_expr`], plus the `excluded`
    /// pseudo-relation: it presents the *view's* rowtype to the writer and the
    /// base relation's to the executor, so `excluded.c` becomes whatever the
    /// view selected `c` from, re-qualified onto `excluded`. That is what makes
    /// `excluded` usable for a view column the base relation does not have —
    /// the expression is evaluated over the proposed row either way.
    pub(crate) fn rewrite_conflict_expr(&self, expr: &Expr, qualifier: &str) -> Expr {
        /// The pseudo-relation an `ON CONFLICT DO UPDATE` reads the proposed
        /// row through.
        const EXCLUDED: &str = "excluded";
        map_expr(expr, false, &mut |node, nested| {
            let Expr::Column { table, name } = node else {
                return None;
            };
            let found = || self.columns.iter().find(|column| &column.name == name);
            match table.as_deref() {
                Some(EXCLUDED) => {
                    found().map(|column| requalify(&column.expr, qualifier, Some(EXCLUDED)))
                }
                Some(written) if written == qualifier => found().map(|column| column.expr.clone()),
                None if !nested => found().map(|column| column.expr.clone()),
                _ => None,
            }
        })
    }

    /// Move every reference to the written relation onto another qualifier.
    ///
    /// An `ON CONFLICT DO UPDATE` reads the *stored* row through the target's
    /// own bare relation name — the range-table entry an `INSERT` adds is
    /// aliased to it — so the statement's qualifier has to become that name
    /// rather than being dropped: a bare column would be ambiguous against
    /// `excluded`, which mirrors every column of the target.
    pub(crate) fn requalify_expr(expr: &Expr, from: &str, to: &str) -> Expr {
        requalify(expr, from, Some(to))
    }

    /// Drop the statement's qualifier from every reference to the written
    /// relation.
    ///
    /// `INSERT` has no `AS` clause to hang a qualifier on, and the name its
    /// `RETURNING` scope carries is the relation's *catalog* name — schema
    /// qualified outside `public`. Leaving the references bare sidesteps having
    /// to reproduce that spelling, and an `INSERT` has no `FROM` list for a
    /// bare name to be ambiguous against.
    pub(crate) fn unqualify_expr(expr: &Expr, from: &str) -> Expr {
        requalify(expr, from, None)
    }

    /// The select items a `RETURNING *` expands to — the view's columns, named
    /// as the view names them.
    ///
    /// Expanded here rather than left to the rewritten statement because a `*`
    /// there would name the columns of the relation underneath, which is not
    /// what the user asked to see.
    pub(crate) fn wildcard_items(&self) -> Vec<SelectItem> {
        self.columns
            .iter()
            .map(|column| SelectItem::Expr {
                expr: column.expr.clone(),
                alias: Some(column.name.clone()),
            })
            .collect()
    }

    /// The view's qualifications `AND`ed onto a statement's own `WHERE`.
    pub(crate) fn restrict(&self, filter: Option<Expr>) -> Option<Expr> {
        self.quals.iter().cloned().fold(filter, |acc, qual| {
            Some(match acc {
                None => qual,
                Some(left) => Expr::Binary {
                    op: BinaryOp::And,
                    left: Box::new(left),
                    right: Box::new(qual),
                },
            })
        })
    }
}

/// Why a view's body is not automatically updatable, or `None` when it is.
///
/// Exposed so `CREATE VIEW` can refuse a `WITH CHECK OPTION` on a body no write
/// could ever be rewritten through, naming the same clause a write would.
pub(crate) fn query_refusal(query: &QueryExpr) -> Option<&'static str> {
    let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
        return Some(
            "Views containing UNION, INTERSECT, or EXCEPT are not automatically updatable.",
        );
    };
    if !matches!(select.distinct, DistinctClause::All) {
        return Some("Views containing DISTINCT are not automatically updatable.");
    }
    if !select.group_by.is_empty() || select.grouping.is_some() {
        return Some("Views containing GROUP BY are not automatically updatable.");
    }
    if select.having.is_some() {
        return Some("Views containing HAVING are not automatically updatable.");
    }
    if query.with.is_some() {
        return Some("Views containing WITH are not automatically updatable.");
    }
    if query.limit.is_some()
        || query.offset.is_some()
        || select.limit.is_some()
        || select.offset.is_some()
    {
        return Some("Views containing LIMIT or OFFSET are not automatically updatable.");
    }
    let projected: Vec<Expr> = select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expr { expr, .. } => Some(expr.clone()),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => None,
        })
        .collect();
    if projected.iter().any(crate::agg::contains_aggregate) {
        return Some("Views that return aggregate functions are not automatically updatable.");
    }
    if !select.window_calls.is_empty() {
        return Some("Views that return window functions are not automatically updatable.");
    }
    if crate::srf::exprs_contain_srf(&projected) {
        return Some("Views that return set-returning functions are not automatically updatable.");
    }
    sole_source(select).err()
}

/// The single `FROM` item an updatable view selects from, with the name its own
/// column references are qualified by.
fn sole_source(select: &SelectStmt) -> Result<(&RelationRef, &str), &'static str> {
    let [
        TableExpr::Table {
            name,
            alias,
            columns,
            sample,
            ..
        },
    ] = select.from.as_slice()
    else {
        return Err(NOT_SINGLE_RELATION);
    };
    if sample.is_some() {
        return Err("Views containing TABLESAMPLE are not automatically updatable.");
    }
    // A `FROM t AS q(x, y)` alias list renames the relation's columns for the
    // body only. Resolving through it is possible, but refusing leaves the view
    // read-only — which is where every view in this engine was before this
    // module existed — while guessing at it would write the wrong column.
    if columns.is_some() {
        return Err(NOT_SINGLE_RELATION);
    }
    Ok((name, alias.as_deref().unwrap_or(&name.name)))
}

/// Why a stored view's body cannot be written through automatically, or `None`
/// when it can.
///
/// Exposed for the one decision made outside the chain walk: a `MERGE` whose
/// target view carries `INSTEAD OF` triggers for some of its actions and not
/// others is reported differently depending on whether the rest of the actions
/// had a rewrite available to them.
pub(crate) fn body_refusal(view: &View) -> Option<&'static str> {
    parse_body(view).err()
}

/// The parsed body of a stored view, or the reason it disqualifies the view.
fn parse_body(view: &View) -> Result<QueryExpr, &'static str> {
    let Ok(statements) = crabka_pgparser::parse(&view.definition) else {
        return Err(NOT_SINGLE_RELATION);
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return Err(NOT_SINGLE_RELATION);
    };
    match query_refusal(query) {
        Some(detail) => Err(detail),
        None => Ok(query.clone()),
    }
}

/// The view's output columns paired with the expressions they were selected
/// from, with wildcards expanded against the source relation's columns.
///
/// Names come from the catalog rather than the body, because `CREATE VIEW v (x,
/// y) AS …` renames an output column without touching the stored text.
fn projected_columns(
    select: &SelectStmt,
    qualifier: &str,
    source_columns: &[String],
    view: &View,
) -> Vec<ColumnMap> {
    const NOT_A_BASE_COLUMN: &str =
        "View columns that are not columns of their base relation are not updatable.";
    let mut exprs: Vec<Expr> = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Expr { expr, .. } => exprs.push(expr.clone()),
            SelectItem::Wildcard => exprs.extend(source_columns.iter().map(|name| Expr::Column {
                table: Some(qualifier.to_string()),
                name: name.clone(),
            })),
            SelectItem::QualifiedWildcard(written) => {
                exprs.extend(source_columns.iter().map(|name| Expr::Column {
                    table: Some(written.clone()),
                    name: name.clone(),
                }));
            }
        }
    }
    view.columns
        .iter()
        .zip(exprs)
        .map(|(column, expr)| {
            let base = match &expr {
                Expr::Column { table, name } if table.as_deref().is_none_or(|t| t == qualifier) => {
                    source_columns
                        .iter()
                        .find(|candidate| *candidate == name)
                        .cloned()
                }
                _ => None,
            };
            ColumnMap {
                name: column.name.clone(),
                expr,
                base,
                refusal: NOT_A_BASE_COLUMN,
            }
        })
        .collect()
}

/// The column names of whatever relation a view's body selects from.
fn source_columns(kv: &dyn Kv, name: &RelationName) -> Result<Vec<String>, ExecError> {
    if let Ok(table) = crabka_pgcatalog::get_table(kv, name) {
        return Ok(table
            .columns
            .into_iter()
            .map(|column| column.name)
            .collect());
    }
    let view = crabka_pgcatalog::get_view(kv, name)?;
    Ok(view.columns.into_iter().map(|column| column.name).collect())
}

/// What the chain walk needs from its caller, which owns the session state this
/// module deliberately does not.
pub(crate) struct ViewWriteCtx<'a> {
    pub(crate) kv: &'a dyn Kv,
    pub(crate) resolution: &'a crate::relname::ResolutionScope,
    /// Whether a relation carries an `INSTEAD OF` row trigger for this write —
    /// the condition that stops the walk, because from there the trigger
    /// performs the write.
    pub(crate) instead_trigger: &'a dyn Fn(&RelationName) -> Result<bool, ExecError>,
    /// Asserts that `role` may perform this write on the named view, the way
    /// `PostgreSQL` checks the view's own ACL before it looks at the body.
    pub(crate) permit: &'a dyn Fn(&View, &str) -> Result<(), ExecError>,
}

/// Resolve the chain of automatically updatable views under `view` down to the
/// relation a write lands on.
///
/// `target` is the qualifier every reference to that relation ends up carrying:
/// the alias the statement gave the view, or the view's own name. Fixing it
/// before the walk is what lets the levels compose — each level rewrites its
/// body's own qualifier to `target`, so the level above substitutes by column
/// name alone.
///
/// `writes` is every command the statement can perform through the view, in the
/// order the statement spelled them — one entry for a plain `INSERT`, `UPDATE`
/// or `DELETE`, and one per command a `MERGE`'s `WHEN` clauses can reach. A
/// refusal names the first of them the view cannot serve, which is the order
/// `PostgreSQL` reports a multi-action `MERGE` in.
///
/// # Errors
///
/// `PostgreSQL`'s 55000 refusal, naming the *innermost* view that is not
/// automatically updatable — the one a user has to fix, which is not
/// necessarily the one they wrote. Also catalog and parse errors.
pub(crate) fn resolve(
    ctx: &ViewWriteCtx<'_>,
    view: &View,
    writes: &[ViewWrite],
    target: &str,
    role: &str,
) -> Result<ViewRewrite, ExecError> {
    // Every write the statement performs is refused by an unupdatable body, so
    // the first one the statement spelled is the one reported.
    let Some(first) = writes.first().copied() else {
        return Err(ExecError::Unsupported(
            "a write through a view must perform at least one command".into(),
        ));
    };
    let mut levels: Vec<Level> = Vec::new();
    let mut current = view.clone();
    let mut run_as = role.to_string();
    let relation = loop {
        (ctx.permit)(&current, &run_as)?;
        let query = parse_body(&current).map_err(|detail| ExecError::ViewNotUpdatable {
            message: first.refusal(&current.name.name),
            detail,
            hint: first.hint(),
        })?;
        let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
            unreachable!("query_refusal rejects every body that is not a SELECT");
        };
        let (reference, qualifier) = sole_source(select)
            .unwrap_or_else(|_| unreachable!("query_refusal rejects every other FROM shape"));
        let source = crate::relname::resolve_relation(
            ctx.kv,
            &ctx.resolution.for_stored_body(&current.name.schema),
            reference,
            crate::relname::SchemaDisposition::Reference,
        )?;
        let names = source_columns(ctx.kv, &source)?;
        let columns = projected_columns(select, qualifier, &names, &current)
            .into_iter()
            .map(|column| ColumnMap {
                expr: canonicalize(&column.expr, qualifier, &names, target, false),
                ..column
            })
            .collect::<Vec<_>>();
        // A `DELETE` names no columns and so survives a view with none that can
        // be assigned; the refusal is owed to the first write that does name
        // them, which for a `MERGE` is the first such `WHEN` clause.
        if !columns.iter().any(|column| column.base.is_some())
            && let Some(write) = writes.iter().find(|write| write.assigns_columns())
        {
            return Err(ExecError::ViewNotUpdatable {
                message: write.refusal(&current.name.name),
                detail: "Views that have no updatable columns are not automatically updatable.",
                hint: write.hint(),
            });
        }
        levels.push(Level {
            view: current.name.name.clone(),
            option: current.options.check_option,
            qual: select
                .filter
                .as_ref()
                .map(|filter| canonicalize(filter, qualifier, &names, target, false)),
            columns,
        });
        // The body — and the write it implies — runs as the view's owner
        // unless the view keeps the caller's identity.
        if !current.options.security_invoker {
            run_as = current.owner.clone();
        }
        // Descend only into a view this rewrite can write through itself. A
        // view with an `INSTEAD OF` trigger for this write is where the chain
        // ends: the trigger, not the rewrite, performs the write from there.
        match crabka_pgcatalog::get_view(ctx.kv, &source) {
            Ok(inner) if !(ctx.instead_trigger)(&source)? => current = inner,
            _ => break source,
        }
    };
    Ok(compose(levels, relation, target, run_as))
}

/// Fold the walked levels into one rewrite, innermost level first.
fn compose(
    levels: Vec<Level>,
    target_relation: RelationName,
    target: &str,
    run_as: String,
) -> ViewRewrite {
    // Which levels' quals are enforced as check options. A view enforces its
    // own qual when it carries any check option, and every level below a
    // `CASCADED` one enforces its qual whether or not it declares an option —
    // including levels below a `LOCAL` view that itself sits under a cascade.
    let mut inherited = false;
    let enforced: Vec<bool> = levels
        .iter()
        .map(|level| {
            let enforced = level.option.is_some() || inherited;
            inherited = inherited || level.option == Some(ViewCheckOption::Cascaded);
            enforced
        })
        .collect();

    let mut columns: Vec<ColumnMap> = Vec::new();
    let mut quals: Vec<(usize, Expr)> = Vec::new();
    for (index, level) in levels.iter().enumerate().rev() {
        if let Some(qual) = &level.qual {
            quals.push((index, substitute(qual, target, &columns)));
        }
        columns = level
            .columns
            .iter()
            .map(|column| ColumnMap {
                name: column.name.clone(),
                expr: substitute(&column.expr, target, &columns),
                base: column
                    .base
                    .as_ref()
                    .and_then(|name| columns.iter().find(|below| &below.name == name))
                    .map_or_else(|| column.base.clone(), |below| below.base.clone()),
                refusal: column.refusal,
            })
            .collect();
    }
    let checks = quals
        .iter()
        .filter(|(index, _)| enforced[*index])
        .map(|(index, qual)| ViewCheck {
            view: levels[*index].view.clone(),
            qual: qual.clone(),
        })
        .collect();
    ViewRewrite {
        target: target_relation,
        columns,
        quals: quals.into_iter().map(|(_, qual)| qual).collect(),
        checks,
        run_as,
    }
}

/// Rewrite every reference to the relation `qualifier` names so that it is
/// qualified by `target` instead.
///
/// An unqualified reference is rewritten only when it names one of `columns`
/// *and* the walk is not inside a subquery, which is where an unqualified name
/// resolves against the subquery's own `FROM` list first. A reference already
/// qualified by `qualifier` is rewritten wherever it appears, because that is
/// exactly the outer reference the rewrite has to carry along.
fn canonicalize(
    expr: &Expr,
    qualifier: &str,
    columns: &[String],
    target: &str,
    in_subquery: bool,
) -> Expr {
    map_expr(expr, in_subquery, &mut |node, nested| {
        let Expr::Column { table, name } = node else {
            return None;
        };
        let names_source = match table.as_deref() {
            Some(written) => written == qualifier,
            None => !nested && columns.iter().any(|column| column == name),
        };
        names_source.then(|| Expr::Column {
            table: Some(target.to_string()),
            name: name.clone(),
        })
    })
}

/// Replace every reference qualified by `target` with the expression `map`
/// gives for that name.
///
/// A name the map does not know is left alone: the statement's own columns are
/// resolved against the view before this runs, so anything still unaccounted
/// for belongs to a `FROM`/`USING` item rather than to the view.
fn substitute(expr: &Expr, target: &str, map: &[ColumnMap]) -> Expr {
    map_expr(expr, false, &mut |node, _| {
        let Expr::Column { table, name } = node else {
            return None;
        };
        (table.as_deref() == Some(target))
            .then(|| map.iter().find(|column| &column.name == name))
            .flatten()
            .map(|column| column.expr.clone())
    })
}

/// Rename one qualifier throughout an expression, or drop it.
fn requalify(expr: &Expr, from: &str, to: Option<&str>) -> Expr {
    map_expr(expr, false, &mut |node, _| {
        let Expr::Column { table, name } = node else {
            return None;
        };
        (table.as_deref() == Some(from)).then(|| Expr::Column {
            table: to.map(ToString::to_string),
            name: name.clone(),
        })
    })
}

/// Rebuild `expr` with `replace` applied to every node, subqueries included.
///
/// `replace` is offered each node together with whether it sits inside a
/// subquery; returning `Some` substitutes the node and stops the descent into
/// it, which is what keeps a substituted expression from being rewritten again
/// by the same pass.
pub(crate) fn map_expr(
    expr: &Expr,
    in_subquery: bool,
    replace: &mut impl FnMut(&Expr, bool) -> Option<Expr>,
) -> Expr {
    if let Some(replacement) = replace(expr, in_subquery) {
        return replacement;
    }
    let mut out = expr.clone();
    for child in crate::exec::expr_children_mut(&mut out) {
        *child = map_expr(child, in_subquery, replace);
    }
    for query in crate::exec::query_children_mut(&mut out) {
        map_query(query, replace);
    }
    out
}

/// [`map_expr`] over every expression of a query, which is by construction
/// inside a subquery.
///
/// The walk has to reach every expression: one it misses leaves a reference to
/// a relation the rewritten statement no longer has in scope, which fails
/// loudly with `missing FROM-clause entry` rather than writing the wrong row.
pub(crate) fn map_query(
    query: &mut QueryExpr,
    replace: &mut impl FnMut(&Expr, bool) -> Option<Expr>,
) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.ctes {
            if let crabka_pgparser::ast::CteBody::Query(body) = &mut cte.body {
                map_query(body, replace);
            }
        }
    }
    map_set_expr(&mut query.body, replace);
    for item in &mut query.order_by {
        item.expr = map_expr(&item.expr, true, replace);
    }
    for bound in query.limit.iter_mut().chain(query.offset.iter_mut()) {
        *bound = map_expr(bound, true, replace);
    }
}

fn map_set_expr(body: &mut SetExpr, replace: &mut impl FnMut(&Expr, bool) -> Option<Expr>) {
    match body {
        SetExpr::Query(QueryBody::Select(select)) => map_select(select, replace),
        SetExpr::Query(QueryBody::Nested(inner)) => map_query(inner, replace),
        SetExpr::Query(QueryBody::Values(values)) => {
            for cell in values.rows.iter_mut().flatten() {
                *cell = map_expr(cell, true, replace);
            }
        }
        SetExpr::SetOp { left, right, .. } => {
            map_set_expr(left, replace);
            map_set_expr(right, replace);
        }
    }
}

fn map_select(select: &mut SelectStmt, replace: &mut impl FnMut(&Expr, bool) -> Option<Expr>) {
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            *expr = map_expr(expr, true, replace);
        }
    }
    for item in &mut select.from {
        map_table_expr(item, replace);
    }
    for expr in select
        .filter
        .iter_mut()
        .chain(select.having.iter_mut())
        .chain(select.limit.iter_mut())
        .chain(select.offset.iter_mut())
        .chain(select.group_by.iter_mut())
    {
        *expr = map_expr(expr, true, replace);
    }
    for item in &mut select.order_by {
        item.expr = map_expr(&item.expr, true, replace);
    }
    for call in &mut select.window_calls {
        if let crabka_pgparser::ast::FuncArgs::Exprs(args) = &mut call.args {
            for arg in args {
                *arg = map_expr(arg, true, replace);
            }
        }
        if let Some(filter) = &mut call.filter {
            *filter = map_expr(filter, true, replace);
        }
    }
}

fn map_table_expr(item: &mut TableExpr, replace: &mut impl FnMut(&Expr, bool) -> Option<Expr>) {
    match item {
        TableExpr::Table { .. } => {}
        TableExpr::Derived { subquery, .. } => map_query(subquery, replace),
        TableExpr::Join {
            left,
            right,
            constraint,
            ..
        } => {
            map_table_expr(left, replace);
            map_table_expr(right, replace);
            if let crabka_pgparser::ast::JoinConstraint::On(expr) = constraint {
                *expr = map_expr(expr, true, replace);
            }
        }
        TableExpr::Function { functions, .. } => {
            for call in functions {
                for arg in call.arguments_mut() {
                    *arg = map_expr(arg, true, replace);
                }
            }
        }
        TableExpr::JsonTable(table) => {
            table.context = map_expr(&table.context, true, replace);
        }
        TableExpr::XmlTable(table) => {
            for expression in table.exprs_mut() {
                *expression = map_expr(expression, true, replace);
            }
        }
    }
}

/// `1 << CMD_UPDATE`, `1 << CMD_INSERT` and `1 << CMD_DELETE` — the bits
/// `pg_relation_is_updatable` returns, and the mask of all three.
pub(crate) const UPDATE_EVENT: i32 = 1 << 2;
pub(crate) const INSERT_EVENT: i32 = 1 << 3;
pub(crate) const DELETE_EVENT: i32 = 1 << 4;
pub(crate) const ALL_EVENTS: i32 = UPDATE_EVENT | INSERT_EVENT | DELETE_EVENT;

/// A view's select list and the relation it selects from, for the catalog
/// predicates. `None` when the view is not automatically updatable at all.
fn updatable_shape(kv: &dyn Kv, view: &View) -> Option<(Vec<ColumnMap>, RelationName)> {
    let query = parse_body(view).ok()?;
    let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
        return None;
    };
    let (reference, qualifier) = sole_source(select).ok()?;
    let source = crate::relname::resolve_relation(
        kv,
        &crate::relname::ResolutionScope::default_scope().for_stored_body(&view.name.schema),
        reference,
        crate::relname::SchemaDisposition::Reference,
    )
    .ok()?;
    let names = source_columns(kv, &source).ok()?;
    Some((projected_columns(select, qualifier, &names, view), source))
}

/// Which of the three writes `relation` admits — `PostgreSQL`'s
/// `relation_is_updatable`.
///
/// This answers the catalog predicates rather than driving a write:
/// `pg_relation_is_updatable`, `information_schema.views.is_updatable`, and the
/// columns derived from them. A table admits everything; a view admits `DELETE`
/// as soon as its body is simple enough, and `INSERT`/`UPDATE` only if it also
/// has a column that can be assigned to — and in both cases only as far as the
/// relation underneath admits the same.
///
/// `include_triggers` is `pg_relation_is_updatable`'s second argument:
/// `information_schema` passes `false`, because the SQL standard's
/// `is_updatable` asks about the view definition rather than about what a
/// trigger might do with it.
///
/// `include_cols` narrows the question to a subset of the relation's columns,
/// which is how `pg_column_is_updatable` asks about one. It is carried down the
/// chain as the *base* columns those map to, so a column assignable at the top
/// still answers `false` when the level underneath will not take it.
pub(crate) fn relation_updatable_events(
    kv: &dyn Kv,
    name: &RelationName,
    include_triggers: bool,
    include_cols: Option<&[String]>,
    depth: usize,
) -> i32 {
    /// A chain longer than this is a catalog cycle rather than a view.
    const MAX_DEPTH: usize = 100;
    if depth > MAX_DEPTH {
        return 0;
    }
    // A table admits every write whatever column was asked about — PostgreSQL
    // settles the relation before it looks at the column number, which is why
    // an out-of-range one still answers `true` for a table.
    if crabka_pgcatalog::get_table(kv, name).is_ok()
        || crate::catalog_rel::catalog_relation(&name.to_string()).is_some()
    {
        return ALL_EVENTS;
    }
    let Ok(view) = crabka_pgcatalog::get_view(kv, name) else {
        return 0;
    };
    let mut events = if include_triggers {
        trigger_events(kv, name)
    } else {
        0
    };
    if events == ALL_EVENTS {
        return events;
    }
    let Some((columns, source)) = updatable_shape(kv, &view) else {
        return events;
    };
    let assignable: Vec<String> = columns
        .iter()
        .filter(|column| include_cols.is_none_or(|wanted| wanted.contains(&column.name)))
        .filter_map(|column| column.base.clone())
        .collect();
    let auto = if assignable.is_empty() {
        DELETE_EVENT
    } else {
        ALL_EVENTS
    };
    events |= auto
        & relation_updatable_events(kv, &source, include_triggers, Some(&assignable), depth + 1);
    events
}

/// The events a relation's enabled `INSTEAD OF` row triggers admit.
fn trigger_events(kv: &dyn Kv, name: &RelationName) -> i32 {
    let Ok(oids) = crate::catalog_rel::view_oids(kv) else {
        return 0;
    };
    let Some(id) = oids
        .get(name)
        .copied()
        .and_then(|oid| u32::try_from(oid).ok())
    else {
        return 0;
    };
    let Ok(triggers) = crabka_pgcatalog::trigger::triggers_for_table(kv, id) else {
        return 0;
    };
    triggers
        .iter()
        .filter(|trigger| {
            trigger.timing == crabka_pgcatalog::trigger::TriggerTiming::InsteadOf
                && trigger.level == crabka_pgcatalog::trigger::TriggerLevel::Row
        })
        .fold(0, |events, trigger| {
            events
                | if trigger.events.insert {
                    INSERT_EVENT
                } else {
                    0
                }
                | if trigger.events.update {
                    UPDATE_EVENT
                } else {
                    0
                }
                | if trigger.events.delete {
                    DELETE_EVENT
                } else {
                    0
                }
        })
}

/// Whether one column of a relation can be assigned to —
/// `pg_column_is_updatable`. `attnum` is one-based, as in the catalog.
///
/// `PostgreSQL` requires the relation to admit both `UPDATE` and `DELETE`, not
/// merely that the column be assignable: the comment on its own implementation
/// says the choice is deliberate and kept in C so it can change without an
/// initdb.
pub(crate) fn column_is_updatable(
    kv: &dyn Kv,
    name: &RelationName,
    attnum: i32,
    include_triggers: bool,
) -> bool {
    // System columns are never updatable, and a table answers before the column
    // number is even looked at — so the guard has to come first.
    if attnum <= 0 {
        return false;
    }
    let wanted = column_at(kv, name, attnum);
    let events = relation_updatable_events(kv, name, include_triggers, wanted.as_deref(), 0);
    events & (UPDATE_EVENT | DELETE_EVENT) == UPDATE_EVENT | DELETE_EVENT
}

/// The one-column restriction `pg_column_is_updatable` asks about, or `None`
/// for a relation whose columns this module does not enumerate — where the
/// answer does not depend on the column anyway.
fn column_at(kv: &dyn Kv, name: &RelationName, attnum: i32) -> Option<Vec<String>> {
    let index = usize::try_from(attnum - 1).ok()?;
    let view = crabka_pgcatalog::get_view(kv, name).ok()?;
    // A column number past the end restricts to nothing, which is exactly the
    // "no assignable column" answer PostgreSQL gives for a view.
    Some(
        view.columns
            .get(index)
            .map(|column| column.name.clone())
            .into_iter()
            .collect(),
    )
}
