use super::*;
/// The read handles a bind pass needs to describe an inner FROM clause, plus the
/// per-lateral-join cache of those descriptions.
///
/// The FROM structure of a lateral item is identical for every outer row, so the
/// blocks are described once (in walk order) and reused for the remaining rows.
pub(super) struct LateralBinder<'a> {
    catalog_kv: &'a dyn Kv,
    resolution: &'a crate::relname::ResolutionScope,
    ctes: &'a crate::cte::CteContext,
    /// The column names each query block's FROM provides, in walk order, per
    /// walked expression. `None` for a block whose FROM could not be described.
    ///
    /// The index is only meaningful within one expression: a statement that
    /// defers several correlated expressions walks a different set of query
    /// blocks for each, and one shared cache would hand the second expression
    /// the first one's FROM.
    described: Vec<Vec<Option<Vec<String>>>>,
    /// Which walked expression [`described`](Self::described) is caching for.
    walk: usize,
    /// Uncorrelated subqueries nested in a deferred lazy expression. Each is
    /// initialized on first use and then reused for the rest of the statement.
    pub(super) initplans: Vec<LazyInitPlan>,
    /// Narrow correlated scalar lookups, materialized only if their expression
    /// is reached and then reused for the rest of the statement.
    pub(super) scalar_lookups: Vec<CorrelatedScalarLookup>,
}

const INITPLAN_MARKER: &str = "\0crabka_initplan";
const INITPLAN_LHS_MARKER: &str = "\0crabka_initplan_lhs";
const SCALAR_LOOKUP_MARKER: &str = "\0crabka_scalar_lookup";

pub(super) struct LazyInitPlan {
    pub(super) template: Expr,
    pub(super) result_type: ColumnType,
    pub(super) resolved: Option<Expr>,
}

pub(super) struct CorrelatedScalarLookup {
    pub(super) query: crabka_pgparser::ast::QueryExpr,
    /// The inner relation, behind the proof that the session may read it and
    /// that no policy filters it.
    ///
    /// This scan answers every outer row out of one hash of the whole inner
    /// relation, so it has to be the whole relation the session is entitled
    /// to — there is no per-row read left for a gate to sit in front of.
    /// Keeping the relation only inside the proof is what stops a later edit
    /// scanning it without one; [`plan_correlated_scalar_lookup`] is the only
    /// place the proof is obtained, and it declines the pushdown when it
    /// cannot be.
    pub(super) table: crate::rls::UnrestrictedRelation,
    pub(super) key_column: usize,
    pub(super) result_column: usize,
    pub(super) result_type: ColumnType,
    pub(super) outer_on_left: bool,
    state: CorrelatedScalarLookupState,
}

fn build_correlated_scalar_lookup(
    read_ctx: &crate::subquery::SubCtx<'_>,
    plan: &CorrelatedScalarLookup,
) -> Result<CorrelatedScalarLookupState, ExecError> {
    // The gated relation, which is the only one this function can name.
    let table = plan.table.get();
    // The lookup projects columns straight out of storage, where a virtual
    // generated column is a NULL placeholder; the ordinary scan materializes it.
    if has_virtual_generated(table) {
        return Ok(CorrelatedScalarLookupState::Fallback);
    }
    let projected_columns = if plan.key_column == plan.result_column {
        vec![plan.key_column]
    } else {
        vec![plan.key_column, plan.result_column]
    };
    let projected_regclass_columns = projected_columns
        .iter()
        .enumerate()
        .filter_map(|(projected, source)| match table.columns[*source].ty {
            ColumnType::Array(elem) => {
                holds_reg(elem.column_type()).map(|kind| (projected, kind, true))
            }
            ty => holds_reg(ty).map(|kind| (projected, kind, false)),
        })
        .collect::<Vec<_>>();
    let result_column = usize::from(plan.key_column != plan.result_column);
    let scan_attempt = read_ctx.statement_memory.reserve();
    let scanned = match crate::scanner::collect_cursor_bounded(
        read_ctx.range_scanner,
        ScanRequest {
            local: read_ctx.kv,
            global: read_ctx.global,
            global_snapshot: read_ctx.gsnap,
            snapshot: read_ctx.snapshot,
            own_xid: read_ctx.own,
            command_id: read_ctx.command_id,
            read_ts: None,
            own_start_ts: None,
            table,
            interval: RowInterval::ALL,
            predicate: PredicatePushdown::FullScan,
            projection: crate::ProjectionPushdown::Columns(projected_columns),
            partial_aggregate: None,
            top_k: None,
        },
        scan_attempt.memory(),
    ) {
        Ok(rows) => rows,
        Err(ExecError::Unsupported(_)) => return Ok(CorrelatedScalarLookupState::Fallback),
        Err(ExecError::Remote(error)) if error.code == "53200" => {
            return Ok(CorrelatedScalarLookupState::Fallback);
        }
        Err(error) => return Err(error),
    };
    let mut rows: Vec<Vec<Datum>> = scanned.into_iter().map(|row| row.row).collect();
    resolve_regclass_at(
        read_ctx.catalog_kv,
        read_ctx.eval_ctx.resolution(),
        &projected_regclass_columns,
        &mut rows,
    )?;
    let saw_rows = !rows.is_empty();
    let mut values = HashMap::new();
    let mut bytes = 0usize;
    for row in &rows {
        let Some(key) = row.first() else {
            return Err(ExecError::Unsupported(
                "correlated scalar lookup key is outside the scanned row".into(),
            ));
        };
        if key.is_null() {
            continue;
        }
        if !crate::join::hashes_like_it_compares(key) {
            return Ok(CorrelatedScalarLookupState::Fallback);
        }
        if values.contains_key(key) {
            continue;
        }
        let value = if result_column == 0 {
            None
        } else {
            let Some(value) = row.get(result_column) else {
                return Err(ExecError::Unsupported(
                    "correlated scalar lookup result is outside the scanned row".into(),
                ));
            };
            Some(value.clone())
        };
        bytes = bytes.saturating_add(crate::scanner::datum_row_bytes(std::slice::from_ref(key)));
        if let Some(value) = &value {
            bytes =
                bytes.saturating_add(crate::scanner::datum_row_bytes(std::slice::from_ref(value)));
        }
        if crate::scanner::exceeds_query_memory(bytes, read_ctx.blocking_query_memory) {
            return Ok(CorrelatedScalarLookupState::Fallback);
        }
        values.insert(key.clone(), value);
    }
    drop(rows);
    scan_attempt.replace_with(bytes)?;
    Ok(CorrelatedScalarLookupState::Ready { values, saw_rows })
}

fn resolve_correlated_scalar_fallback(
    read_ctx: &crate::subquery::SubCtx<'_>,
    plan: &CorrelatedScalarLookup,
    key_expr: &Expr,
) -> Result<Expr, ExecError> {
    let temporary = read_ctx.statement_memory.reserve();
    let mut query = plan.query.clone();
    let crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Select(select)) =
        &mut query.body
    else {
        unreachable!("a scalar lookup plan always has one SELECT body")
    };
    let Expr::Binary { left, right, .. } = select
        .filter
        .as_mut()
        .expect("a scalar lookup plan always has an equality filter")
    else {
        unreachable!("a scalar lookup plan always has an equality filter")
    };
    if plan.outer_on_left {
        **left = key_expr.clone();
    } else {
        **right = key_expr.clone();
    }
    let result = crate::subquery::resolve_expr(read_ctx, &Expr::ScalarSubquery(Box::new(query)));
    drop(temporary);
    result
}

enum CorrelatedScalarLookupState {
    Uninitialized,
    Ready {
        values: HashMap<Datum, Option<Datum>>,
        saw_rows: bool,
    },
    Fallback,
}

impl<'a> LateralBinder<'a> {
    pub(super) fn new(
        catalog_kv: &'a dyn Kv,
        resolution: &'a crate::relname::ResolutionScope,
        ctes: &'a crate::cte::CteContext,
    ) -> Self {
        Self {
            catalog_kv,
            resolution,
            ctes,
            described: Vec::new(),
            walk: 0,
            initplans: Vec::new(),
            scalar_lookups: Vec::new(),
        }
    }

    /// Bind the next expression against its own block-description cache.
    pub(super) fn walking(&mut self, walk: usize) {
        self.walk = walk;
    }

    pub(super) fn with_initplans(mut self, initplans: Vec<LazyInitPlan>) -> Self {
        self.initplans = initplans;
        self
    }

    pub(super) fn with_scalar_lookups(
        mut self,
        scalar_lookups: Vec<CorrelatedScalarLookup>,
    ) -> Self {
        self.scalar_lookups = scalar_lookups;
        self
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
    pub(super) fn bind(
        &mut self,
        te: &crabka_pgparser::ast::TableExpr,
        outer: &Scope,
        row: &[Datum],
    ) -> (crabka_pgparser::ast::TableExpr, Option<String>) {
        let mut bound = te.clone();
        let ctes = self.ctes.child();
        let mut pass = BindPass {
            binder: self,
            outer,
            row,
            visited: 0,
            referenced: None,
            substituted: false,
            ctes,
            error: None,
        };
        let shadow = Shadow::default();
        pass.table_expr(&mut bound, &shadow);
        let referenced = pass.referenced;
        (bound, referenced)
    }

    /// Specialize one query expression against an enclosing row. The boolean
    /// reports whether any outer value was substituted; unlike `referenced`, it
    /// also covers USING/NATURAL-coalesced columns, whose scope binding has no
    /// relation qualifier.
    pub(super) fn bind_query(
        &mut self,
        query: &crabka_pgparser::ast::QueryExpr,
        outer: &Scope,
        row: &[Datum],
    ) -> Result<(crabka_pgparser::ast::QueryExpr, bool), ExecError> {
        let mut bound = query.clone();
        let ctes = self.ctes.child();
        let mut pass = BindPass {
            binder: self,
            outer,
            row,
            visited: 0,
            referenced: None,
            substituted: false,
            ctes,
            error: None,
        };
        pass.query(&mut bound, &Shadow::default());
        match pass.error {
            Some(error) => Err(error),
            None => Ok((bound, pass.substituted)),
        }
    }

    /// Specialize an expression and every query nested inside it against an
    /// enclosing row.
    pub(super) fn bind_expr(
        &mut self,
        expr: &Expr,
        outer: &Scope,
        row: &[Datum],
    ) -> Result<(Expr, bool), ExecError> {
        let mut bound = expr.clone();
        let ctes = self.ctes.child();
        let mut pass = BindPass {
            binder: self,
            outer,
            row,
            visited: 0,
            referenced: None,
            substituted: false,
            ctes,
            error: None,
        };
        pass.expr(&mut bound, &Shadow::default());
        match pass.error {
            Some(error) => Err(error),
            None => Ok((bound, pass.substituted)),
        }
    }

    pub(super) fn resolve_initplan(
        &mut self,
        read_ctx: &crate::subquery::SubCtx<'_>,
        index: usize,
        lhs: Option<&Expr>,
    ) -> Result<Expr, ExecError> {
        let plan = self
            .initplans
            .get_mut(index)
            .ok_or_else(|| ExecError::Unsupported("invalid deferred subquery marker".into()))?;
        if plan.resolved.is_none() {
            plan.resolved = Some(crate::subquery::resolve_expr(read_ctx, &plan.template)?);
        }
        let mut resolved = plan
            .resolved
            .clone()
            .expect("the deferred subquery was just initialized");
        if let Some(lhs) = lhs {
            substitute_initplan_lhs(&mut resolved, lhs);
        }
        Ok(resolved)
    }

    pub(super) fn resolve_scalar_lookup(
        &mut self,
        read_ctx: &crate::subquery::SubCtx<'_>,
        index: usize,
        key_expr: &Expr,
    ) -> Result<Expr, ExecError> {
        let plan = self.scalar_lookups.get(index).ok_or_else(|| {
            ExecError::Unsupported("invalid correlated scalar lookup marker".into())
        })?;
        if matches!(&plan.state, CorrelatedScalarLookupState::Uninitialized) {
            let state = build_correlated_scalar_lookup(read_ctx, plan)?;
            self.scalar_lookups[index].state = state;
        }
        let plan = &self.scalar_lookups[index];
        match &plan.state {
            CorrelatedScalarLookupState::Uninitialized => unreachable!("lookup was initialized"),
            CorrelatedScalarLookupState::Fallback => {
                resolve_correlated_scalar_fallback(read_ctx, plan, key_expr)
            }
            CorrelatedScalarLookupState::Ready { values, saw_rows } => {
                if !saw_rows {
                    return Ok(Expr::Const {
                        value: Datum::Null,
                        ty: plan.result_type,
                    });
                }
                let resolved_key_expr = crate::subquery::resolve_expr(read_ctx, key_expr)?;
                let key =
                    crate::eval::eval(&resolved_key_expr, &Scope::empty(), &[], read_ctx.eval_ctx)?;
                if key.is_null() {
                    return Ok(Expr::Const {
                        value: Datum::Null,
                        ty: plan.result_type,
                    });
                }
                if !crate::join::hashes_like_it_compares(&key) {
                    return resolve_correlated_scalar_fallback(read_ctx, plan, key_expr);
                }
                Ok(Expr::Const {
                    value: values.get(&key).map_or(Datum::Null, |value| {
                        value.clone().unwrap_or_else(|| key.clone())
                    }),
                    ty: plan.result_type,
                })
            }
        }
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
    /// Whether any outer column was substituted, including an unqualified
    /// coalesced column whose scope binding has no relation qualifier.
    substituted: bool,
    /// Query-local CTEs visible at the current query block.
    ctes: crate::cte::CteContext,
    /// A bindable ambiguous outer reference must retain 42702 instead of being
    /// left for the inner scope to misreport as an undefined column.
    error: Option<ExecError>,
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

impl Shadow {
    /// Do the FROM clauses this shadow covers supply `name`?
    ///
    /// `None` — reachable only for a bare name — is "some covered FROM could not
    /// be described", so no level can be ruled in or out.
    fn supplies(&self, qualifier: Option<&str>, name: &str) -> Option<bool> {
        match qualifier {
            Some(qualifier) => Some(self.qualifiers.iter().any(|q| q == qualifier)),
            None => Some(self.columns.as_ref()?.iter().any(|column| column == name)),
        }
    }

    /// Widen this shadow by the names `from` introduces.
    fn extend_by(
        &mut self,
        catalog_kv: &dyn Kv,
        resolution: &crate::relname::ResolutionScope,
        ctes: &crate::cte::CteContext,
        from: &[crabka_pgparser::ast::TableExpr],
    ) {
        collect_qualifiers(from, &mut self.qualifiers);
        match from_column_names(catalog_kv, resolution, ctes, from) {
            Some(names) => {
                if let Some(columns) = &mut self.columns {
                    columns.extend(names);
                }
            }
            None => self.columns = None,
        }
    }
}

/// The unqualified column names `from` supplies, or `None` when its schema
/// cannot be described here — a FROM naming a relation that does not exist is
/// for the ordinary read path to report, not for a name-shadowing pass.
///
/// Described as if the statement read every system column, because this answers
/// what the FROM *can* supply and not what one statement asked of it. Whether a
/// scan builds `tableoid` or `ctid` is a width optimisation; a stored relation
/// offers both names whatever the statement spells. Describing without them
/// left a bare `ctid` inside an uncorrelated subquery unshadowed, so the binder
/// bound it to the enclosing row: `SELECT a FROM t WHERE ctid IN (SELECT ctid
/// FROM s WHERE a = 2)` compared each row of `t` against its own `ctid` and
/// matched every one of them.
fn from_column_names(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    ctes: &crate::cte::CteContext,
    from: &[crabka_pgparser::ast::TableExpr],
) -> Option<Vec<String>> {
    if from.is_empty() {
        return Some(Vec::new());
    }
    let refs = crate::scope::StatementRefs::every_system_column();
    build_from_schema_described(catalog_kv, resolution, from, ctes, None, Some(&refs))
        .ok()
        .map(|relation| {
            relation
                .scope
                .columns
                .into_iter()
                .map(|column| column.name)
                .collect()
        })
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
        let walk = self.binder.walk;
        if walk >= self.binder.described.len() {
            self.binder.described.resize_with(walk + 1, Vec::new);
        }
        if index >= self.binder.described[walk].len() {
            let names = from_column_names(
                self.binder.catalog_kv,
                self.binder.resolution,
                &self.ctes,
                from,
            );
            self.binder.described[walk].push(names);
        }
        self.binder.described[walk][index].clone()
    }

    fn table_expr(&mut self, te: &mut crabka_pgparser::ast::TableExpr, shadow: &Shadow) {
        use crabka_pgparser::ast::TableExpr;
        match te {
            TableExpr::Table { .. } => {}
            TableExpr::Derived { subquery, .. } => self.query(subquery, shadow),
            TableExpr::Function { functions, .. } => {
                for call in functions {
                    for arg in call.arguments_mut() {
                        self.expr(arg, shadow);
                    }
                }
            }
            TableExpr::JsonTable(table) => {
                for expr in table.exprs_mut() {
                    self.expr(expr, shadow);
                }
            }
            TableExpr::XmlTable(table) => {
                for expr in table.exprs_mut() {
                    self.expr(expr, shadow);
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
        let parent_ctes = self.ctes.clone();
        // A CTE inside a lateral item may reference the outer row too, so the
        // WITH list is part of the walk. Describe each bound item in declaration
        // order so the query body can distinguish its columns from outer ones.
        if let Some(with) = &mut query.with {
            for cte in &mut with.ctes {
                match &mut cte.body {
                    crabka_pgparser::ast::CteBody::Query(body) => self.query(body, shadow),
                    // A data-modifying CTE is not a lateral read path; leaving it
                    // alone keeps the reference to be reported by name resolution.
                    crabka_pgparser::ast::CteBody::Dml(_) => {}
                }
                if let Ok(relation) = crate::cte::describe_cte_relation(
                    self.binder.catalog_kv,
                    self.binder.resolution,
                    cte,
                    with.recursive,
                    &self.ctes,
                ) {
                    self.ctes.insert(cte.name.clone(), relation);
                }
            }
        }
        self.set_expr(&mut query.body, shadow);
        let order_shadow = match &query.body {
            crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Select(
                select,
            )) => {
                let mut inner = self.extended(shadow, &select.from);
                if let Some(columns) = &mut inner.columns {
                    columns.extend(select.projection.iter().filter_map(|item| match item {
                        SelectItem::Expr { expr, alias } => {
                            Some(alias.clone().unwrap_or_else(|| derived_name(expr)))
                        }
                        _ => None,
                    }));
                }
                inner
            }
            // A set operation or nested query exposes its output columns to the
            // tail ORDER BY. If that shape cannot be described cheaply here,
            // leave bare names to its own resolver instead of mistaking them for
            // outer references.
            _ => Shadow {
                qualifiers: shadow.qualifiers.clone(),
                columns: None,
            },
        };
        for item in &mut query.order_by {
            self.expr(&mut item.expr, &order_shadow);
        }
        for expr in query.limit.iter_mut().chain(query.offset.iter_mut()) {
            self.expr(expr, shadow);
        }
        self.ctes = parent_ctes;
    }

    fn set_expr(&mut self, body: &mut crabka_pgparser::ast::SetExpr, shadow: &Shadow) {
        use crabka_pgparser::ast::{QueryBody, SetExpr};
        match body {
            SetExpr::Query(QueryBody::Select(select)) => {
                for item in &mut select.from {
                    self.table_expr(item, shadow);
                }
                // A local FunctionScan may refer to the lateral row. Substitute
                // that value before describing the local FROM scope; otherwise
                // schema inference fails on the outer name and hides every
                // subsequent outer reference in this SELECT.
                let inner = self.extended(shadow, &select.from);
                // Outer-column substitution can turn `id` into a constant and
                // erase the FigureColname the lateral relation exposes. Pin
                // the label before rewriting the expression tree.
                for item in &mut select.projection {
                    if let SelectItem::Expr { expr, alias } = item
                        && alias.is_none()
                    {
                        *alias = Some(derived_name(expr));
                    }
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
        if self.whole_row_field(expr, shadow) {
            return;
        }
        if let Expr::Column { table, name } = expr {
            let bindable = match table {
                Some(qualifier) => !shadow.qualifiers.iter().any(|q| q == qualifier),
                None => shadow
                    .columns
                    .as_ref()
                    .is_some_and(|columns| !columns.iter().any(|column| column == name)),
            };
            if bindable {
                match self.outer.resolve(table.as_deref(), name) {
                    Ok(index) => {
                        self.substituted = true;
                        if self.referenced.is_none() {
                            self.referenced = self.outer.columns[index].qualifier.clone();
                        }
                        *expr = Expr::Const {
                            value: self.row[index].clone(),
                            ty: self.outer.ty_at(index),
                        };
                    }
                    Err(error @ ExecError::AmbiguousColumn(_)) if self.error.is_none() => {
                        self.error = Some(error);
                    }
                    // A bare name that is no outer column may still name an
                    // outer *relation*, and then it is that relation's whole
                    // row — the same range-table fallback `eval` takes for a
                    // reference it can resolve in place. Without it a
                    // correlated subquery reported 42703 for a whole row every
                    // uncorrelated path already answered.
                    Err(ExecError::UndefinedColumn(_)) => self.whole_row(expr, shadow),
                    Err(_) => {}
                }
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

    /// Substitute a correlated `(relation).field` as one scalar.  Replacing
    /// only its whole-row child would leave the inner inference path with a
    /// named relation type that is deliberately absent from the user-type
    /// registry.
    fn whole_row_field(&mut self, expr: &mut Expr, shadow: &Shadow) -> bool {
        let Expr::FieldSelect { base, field } = &*expr else {
            return false;
        };
        let Expr::Column {
            table: None,
            name: qualifier,
        } = &**base
        else {
            return false;
        };
        if shadow.qualifiers.iter().any(|q| q == qualifier)
            || shadow
                .columns
                .as_ref()
                .is_some_and(|columns| columns.iter().any(|column| column == qualifier))
            || !matches!(
                self.outer.resolve(None, qualifier),
                Err(ExecError::UndefinedColumn(_))
            )
        {
            return false;
        }
        let Some(ty) = self.outer.whole_row_field_type(qualifier, field) else {
            return false;
        };
        let Some(value) = self.outer.refs_value(qualifier, self.row) else {
            return false;
        };
        let Ok(value) = crate::eval::select_field(&value, field) else {
            return false;
        };
        self.substituted = true;
        if self.referenced.is_none() {
            self.referenced = Some(qualifier.clone());
        }
        *expr = Expr::Const { value, ty };
        true
    }

    /// Substitute `expr` — a bare name that resolved to no outer column — with
    /// the whole row of the outer relation it names, when it names one.
    ///
    /// The enclosing FROM clauses are consulted first and win outright: an
    /// inner relation of the same name is what `PostgreSQL` binds, so
    /// `SELECT (SELECT count(*) FROM ca WHERE ca IS NOT NULL) FROM ca` counts
    /// the inner `ca`. `Shadow::columns` cannot answer that on its own — it
    /// holds the names a FROM *supplies*, and a relation's own name is not
    /// among them — so the qualifier list is what rules the outer row out.
    ///
    /// Only an unqualified name reaches this: `PostgreSQL` reads `s.t` as
    /// "column `t` of range-table entry `s`" and reports a missing FROM entry
    /// for `s`, never as the whole row of `s.t`.
    ///
    /// A row an outer join invented for that side has no whole row, so
    /// [`Scope::whole_row_value`] hands back NULL and the substitution carries
    /// it — which is what keeps `(,)` and NULL distinguishable in one result
    /// set. Leaving `expr` untouched when nothing matches keeps the inner
    /// scope's own 42703 exactly as it was.
    fn whole_row(&mut self, expr: &mut Expr, shadow: &Shadow) {
        let Expr::Column {
            table: None,
            name: qualifier,
        } = &*expr
        else {
            return;
        };
        if shadow.qualifiers.iter().any(|q| q == qualifier) {
            return;
        }
        let Some(value) = self.outer.refs_value(qualifier, self.row) else {
            return;
        };
        self.substituted = true;
        if self.referenced.is_none() {
            self.referenced = Some(qualifier.clone());
        }
        *expr = Expr::Const {
            value,
            ty: self
                .outer
                .whole_row_type(qualifier)
                .unwrap_or(ColumnType::Record(None)),
        };
    }
}

/// Every qualifier a FROM list introduces (alias if present, else the relation
/// or function name).
pub(super) fn collect_qualifiers(from: &[crabka_pgparser::ast::TableExpr], out: &mut Vec<String>) {
    use crabka_pgparser::ast::TableExpr;
    for item in from {
        match item {
            TableExpr::Table { name, alias, .. } => {
                out.push(alias.clone().unwrap_or_else(|| name.name.clone()));
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
            TableExpr::JsonTable(table) => out.push(
                table
                    .alias
                    .clone()
                    .unwrap_or_else(|| "json_table".to_string()),
            ),
            TableExpr::XmlTable(table) => out.push(
                table
                    .alias
                    .clone()
                    .unwrap_or_else(|| "xmltable".to_string()),
            ),
            TableExpr::Join { left, right, .. } => {
                collect_qualifiers(std::slice::from_ref(left), out);
                collect_qualifiers(std::slice::from_ref(right), out);
            }
        }
    }
}

/// [`select_exprs_mut`]'s shared-reference twin, for the walks that only read.
fn select_exprs(select: &SelectStmt) -> Vec<&Expr> {
    let mut out: Vec<&Expr> = Vec::new();
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            out.push(expr);
        }
    }
    out.extend(&select.filter);
    out.extend(&select.group_by);
    out.extend(&select.having);
    out.extend(select.order_by.iter().map(|item| &item.expr));
    if let crabka_pgparser::ast::DistinctClause::On(on) = &select.distinct {
        out.extend(on);
    }
    for call in &select.window_calls {
        if let FuncArgs::Exprs(args) = &call.args {
            out.extend(args);
        }
        out.extend(&call.filter);
        if let crabka_pgparser::ast::WindowRef::Spec(spec) = &call.over {
            out.extend(window_spec_exprs(spec));
        }
    }
    for window in &select.windows {
        out.extend(window_spec_exprs(&window.spec));
    }
    out
}

fn window_spec_exprs(spec: &crabka_pgparser::ast::WindowSpec) -> Vec<&Expr> {
    let mut out = Vec::new();
    out.extend(&spec.partition_by);
    out.extend(spec.order_by.iter().map(|item| &item.expr));
    if let Some(frame) = &spec.frame {
        for bound in [&frame.start, &frame.end] {
            match bound {
                crabka_pgparser::ast::FrameBound::Preceding(expr)
                | crabka_pgparser::ast::FrameBound::Following(expr) => out.push(expr),
                crabka_pgparser::ast::FrameBound::UnboundedPreceding
                | crabka_pgparser::ast::FrameBound::CurrentRow
                | crabka_pgparser::ast::FrameBound::UnboundedFollowing => {}
            }
        }
    }
    out
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
    for call in &mut select.window_calls {
        if let FuncArgs::Exprs(args) = &mut call.args {
            out.extend(args);
        }
        out.extend(call.filter.iter_mut());
        if let crabka_pgparser::ast::WindowRef::Spec(spec) = &mut call.over {
            out.extend(window_spec_exprs_mut(spec));
        }
    }
    for window in &mut select.windows {
        out.extend(window_spec_exprs_mut(&mut window.spec));
    }
    out
}

fn window_spec_exprs_mut(spec: &mut crabka_pgparser::ast::WindowSpec) -> Vec<&mut Expr> {
    let mut out = Vec::new();
    out.extend(&mut spec.partition_by);
    out.extend(spec.order_by.iter_mut().map(|item| &mut item.expr));
    if let Some(frame) = &mut spec.frame {
        for bound in [&mut frame.start, &mut frame.end] {
            match bound {
                crabka_pgparser::ast::FrameBound::Preceding(expr)
                | crabka_pgparser::ast::FrameBound::Following(expr) => out.push(expr),
                crabka_pgparser::ast::FrameBound::UnboundedPreceding
                | crabka_pgparser::ast::FrameBound::CurrentRow
                | crabka_pgparser::ast::FrameBound::UnboundedFollowing => {}
            }
        }
    }
    out
}

fn initplan_lhs_marker() -> Expr {
    Expr::Column {
        table: Some(INITPLAN_LHS_MARKER.into()),
        name: "value".into(),
    }
}

fn initplan_marker(index: usize, lhs: Option<Expr>) -> Expr {
    let mut args = vec![Expr::IntLiteral(index.to_string())];
    args.extend(lhs);
    Expr::Func(FuncCall {
        sql_syntax: false,
        name: INITPLAN_MARKER.into(),
        distinct: false,
        args: FuncArgs::Exprs(args),
        order_by: Vec::new(),
        within_group: false,
        filter: None,
    })
}

pub(super) fn initplan_parts(expr: &Expr) -> Option<(usize, Option<&Expr>)> {
    let Expr::Func(FuncCall {
        sql_syntax: false,
        name,
        distinct: false,
        args: FuncArgs::Exprs(args),
        order_by,
        within_group: false,
        filter: None,
    }) = expr
    else {
        return None;
    };
    if name != INITPLAN_MARKER || !order_by.is_empty() {
        return None;
    }
    if !(1..=2).contains(&args.len()) {
        return None;
    }
    let Expr::IntLiteral(index) = &args[0] else {
        return None;
    };
    Some((index.parse().ok()?, args.get(1)))
}

fn scalar_lookup_marker(index: usize, key: Expr) -> Expr {
    Expr::Func(FuncCall {
        sql_syntax: false,
        name: SCALAR_LOOKUP_MARKER.into(),
        distinct: false,
        args: FuncArgs::Exprs(vec![Expr::IntLiteral(index.to_string()), key]),
        order_by: Vec::new(),
        within_group: false,
        filter: None,
    })
}

pub(super) fn scalar_lookup_parts(expr: &Expr) -> Option<(usize, &Expr)> {
    let Expr::Func(FuncCall {
        sql_syntax: false,
        name,
        distinct: false,
        args: FuncArgs::Exprs(args),
        order_by,
        within_group: false,
        filter: None,
    }) = expr
    else {
        return None;
    };
    if name != SCALAR_LOOKUP_MARKER || args.len() != 2 || !order_by.is_empty() {
        return None;
    }
    let Expr::IntLiteral(index) = &args[0] else {
        return None;
    };
    Some((index.parse().ok()?, &args[1]))
}

fn plan_correlated_scalar_lookup(
    read_ctx: &crate::subquery::SubCtx<'_>,
    query: &crabka_pgparser::ast::QueryExpr,
    outer: &Scope,
) -> Result<Option<(CorrelatedScalarLookup, Expr)>, ExecError> {
    use crabka_pgparser::ast::{DistinctClause, QueryBody, SetExpr, TableExpr};

    if query.with.is_some()
        || !query.order_by.is_empty()
        || !matches!(query.limit, Some(Expr::IntLiteral(ref value)) if value == "1")
        || query.offset.is_some()
        || query.with_ties
        || query.locking.is_some()
    {
        return Ok(None);
    }
    let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
        return Ok(None);
    };
    if !matches!(select.distinct, DistinctClause::All)
        || !select.group_by.is_empty()
        || select.grouping.is_some()
        || select.having.is_some()
        || !select.windows.is_empty()
        || !select.window_calls.is_empty()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.with_ties
        || select.locking.is_some()
    {
        return Ok(None);
    }
    let [SelectItem::Expr { .. }] = select.projection.as_slice() else {
        return Ok(None);
    };
    let [
        TableExpr::Table {
            name,
            alias,
            columns: None,
            sample: None,
            ..
        },
    ] = select.from.as_slice()
    else {
        return Ok(None);
    };
    if name.schema.is_none() && read_ctx.ctes.lookup(&name.name).is_some() {
        return Ok(None);
    }
    let resolved_name = resolve_relation(
        read_ctx.catalog_kv,
        read_ctx.fctx.resolution,
        name,
        SchemaDisposition::Reference,
    )?;
    let table = match crabka_pgcatalog::get_table(read_ctx.catalog_kv, &resolved_name) {
        Ok(table) => table,
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    // A materialized view whose contents have never been computed is an error
    // to read, not an empty relation, and this hash never opens the path that
    // says so: it read the empty row space and answered `NULL` for every outer
    // key, while the same subquery without the `LIMIT 1` raised 55000.
    // Declining hands both shapes to the path that refuses it.
    if table
        .materialized
        .as_ref()
        .is_some_and(|matview| !matview.populated)
        || table.sharded
        || table.sharding.is_some()
        || table.foreign.is_some()
        || crate::partition::is_partitioned(read_ctx.catalog_kv, &table.name)?
        || crate::partition::parent_of(read_ctx.catalog_kv, &table.name)?.is_some()
        || !crate::inheritance::parents_of(read_ctx.catalog_kv, &table.name)?.is_empty()
        || !crate::inheritance::children_of(read_ctx.catalog_kv, &table.name)?.is_empty()
    {
        return Ok(None);
    }
    let qualifier = alias.as_deref().unwrap_or(&table.name.name);
    let inner = Scope::single(&table, qualifier);
    let nulls = vec![Datum::Null; outer.width()];
    let mut binder =
        LateralBinder::new(read_ctx.catalog_kv, read_ctx.fctx.resolution, read_ctx.ctes);
    let (bound_query, correlated) = binder.bind_query(query, outer, &nulls)?;
    if !correlated {
        return Ok(None);
    }
    let SetExpr::Query(QueryBody::Select(bound_select)) = &bound_query.body else {
        unreachable!("binding preserves a scalar lookup SELECT body")
    };
    let [
        SelectItem::Expr {
            expr: bound_projection,
            ..
        },
    ] = bound_select.projection.as_slice()
    else {
        unreachable!("binding preserves a scalar lookup projection")
    };
    let Some(result_column) = direct_lookup_column(bound_projection, &inner) else {
        return Ok(None);
    };
    let Some(Expr::Binary {
        op: BinaryOp::Eq,
        left: original_left,
        right: original_right,
    }) = select.filter.as_ref()
    else {
        return Ok(None);
    };
    let Some(Expr::Binary {
        op: BinaryOp::Eq,
        left: bound_left,
        right: bound_right,
    }) = bound_select.filter.as_ref()
    else {
        unreachable!("binding preserves a scalar lookup equality")
    };
    let (key_column, outer_expr, outer_on_left) = if let Some(column) =
        direct_lookup_column(bound_left, &inner)
        && !lookup_expr_contains_column(bound_right)
    {
        (column, original_right.as_ref(), false)
    } else if let Some(column) = direct_lookup_column(bound_right, &inner)
        && !lookup_expr_contains_column(bound_left)
    {
        (column, original_left.as_ref(), true)
    } else {
        return Ok(None);
    };
    if !lookup_key_is_immutable(read_ctx.catalog_kv, outer_expr) {
        return Ok(None);
    }
    let key_type = table.columns[key_column].ty;
    if crate::eval::infer_type(outer_expr, outer)? != key_type {
        return Ok(None);
    }
    let result_type = table.columns[result_column].ty;
    // The two gates every other stored-relation read passes and this one did
    // not: the read permit and row security. The lookup hashes the whole inner
    // relation and answers every outer row from that hash, so an ungranted
    // relation was readable by anyone who could name a key — with
    // `generate_series` as the outer relation, by anyone at all — and a policy
    // that hid a row hid it from every path but this one.
    //
    // Declining is the whole answer. The correlated fallback re-runs the
    // subquery through `subquery::resolve_expr`, which raises the 42501 itself
    // and filters through `rls::apply_row_security` with the once-per-scan qual
    // resolution and the recursion guard this hash has nowhere to run. Nor
    // could a filtering fast path be the same fast path: the hash is built from
    // a two-column projection, while a policy qual may read any column of the
    // row, so filtering here means scanning the relation whole.
    //
    // The proof is taken by value because the plan outlives this function and
    // is read once per outer row; it is the only field the scan can reach the
    // relation through.
    let Some(table) =
        crate::rls::UnrestrictedRelation::read(&read_ctx.privileges(), &read_ctx.rls(), table)?
    else {
        return Ok(None);
    };
    Ok(Some((
        CorrelatedScalarLookup {
            query: query.clone(),
            table,
            key_column,
            result_column,
            result_type,
            outer_on_left,
            state: CorrelatedScalarLookupState::Uninitialized,
        },
        outer_expr.clone(),
    )))
}

fn plan_correlated_exists_lookup(
    read_ctx: &crate::subquery::SubCtx<'_>,
    query: &crabka_pgparser::ast::QueryExpr,
    outer: &Scope,
) -> Result<Option<(CorrelatedScalarLookup, Expr)>, ExecError> {
    use crabka_pgparser::ast::{QueryBody, SetExpr};

    if query.limit.is_some() {
        return Ok(None);
    }
    let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
        return Ok(None);
    };
    if crate::grouping::is_grouping_query(select)
        || crate::srf::projection_contains_srf(&select.projection)
    {
        return Ok(None);
    }
    let Some(Expr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
    }) = select.filter.as_ref()
    else {
        return Ok(None);
    };

    // For an equality match the inner key cannot be NULL, so EXISTS is the
    // same as selecting that key once and testing it for NULL. Trying both
    // operands lets the scalar planner identify the inner column without a
    // second copy of its relation-resolution rules.
    for projection in [left.as_ref(), right.as_ref()] {
        let mut scalar = query.clone();
        scalar.limit = Some(Expr::IntLiteral("1".into()));
        let SetExpr::Query(QueryBody::Select(candidate)) = &mut scalar.body else {
            unreachable!("the EXISTS lookup has one SELECT body")
        };
        candidate.projection = vec![SelectItem::Expr {
            expr: projection.clone(),
            alias: None,
        }];
        if let Some(lookup) = plan_correlated_scalar_lookup(read_ctx, &scalar, outer)? {
            return Ok(Some(lookup));
        }
    }
    Ok(None)
}

fn direct_lookup_column(expr: &Expr, scope: &Scope) -> Option<usize> {
    let Expr::Column { table, name } = expr else {
        return None;
    };
    scope.resolve(table.as_deref(), name).ok()
}

fn lookup_expr_contains_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column { .. })
        || expr_children(expr)
            .into_iter()
            .any(lookup_expr_contains_column)
}

fn lookup_key_is_immutable(catalog_kv: &dyn Kv, expr: &Expr) -> bool {
    if !immutable_row_predicate(expr) {
        return false;
    }
    let mut immutable = true;
    crate::grouping::visit_expr(expr, &mut |node| {
        let Expr::Func(call) = node else {
            return;
        };
        immutable &= match crabka_pgcatalog::routine::routines_named(catalog_kv, &call.name) {
            Ok(routines) if routines.is_empty() => is_immutable_function(&call.name),
            Ok(routines) => routines.iter().all(|routine| routine.volatility == 'i'),
            Err(_) => false,
        };
    });
    immutable
}

fn substitute_initplan_lhs(expr: &mut Expr, lhs: &Expr) {
    if matches!(
        expr,
        Expr::Column {
            table: Some(table),
            name,
        } if table == INITPLAN_LHS_MARKER && name == "value"
    ) {
        *expr = lhs.clone();
        return;
    }
    for child in expr_children_mut(expr) {
        substitute_initplan_lhs(child, lhs);
    }
}

fn direct_subquery_is_correlated(
    read_ctx: &crate::subquery::SubCtx<'_>,
    expr: &Expr,
    outer: &Scope,
) -> Result<bool, ExecError> {
    let Some(query) = direct_subquery(expr) else {
        return Ok(false);
    };
    let nulls = vec![Datum::Null; outer.width()];
    let mut binder =
        LateralBinder::new(read_ctx.catalog_kv, read_ctx.fctx.resolution, read_ctx.ctes);
    Ok(binder.bind_query(query, outer, &nulls)?.1)
}

/// Replace uncorrelated subqueries under an expression that must remain deferred
/// with on-demand initplan markers. This keeps CASE/COALESCE lazy while retaining
/// PostgreSQL's once-only execution for an initplan that is eventually selected.
pub(super) fn install_lazy_initplans(
    read_ctx: &crate::subquery::SubCtx<'_>,
    expr: &Expr,
    outer: &Scope,
    deferred: bool,
    initplans: &mut Vec<LazyInitPlan>,
    scalar_lookups: &mut Vec<CorrelatedScalarLookup>,
) -> Result<Expr, ExecError> {
    let direct_correlated = direct_subquery_is_correlated(read_ctx, expr, outer)?;
    // Keep one materialized lookup per query so retained hash state stays
    // within the query's single blocking-memory budget. Additional eligible
    // subqueries remain on the ordinary correlated fallback path.
    if scalar_lookups.is_empty()
        && direct_correlated
        && let Expr::ScalarSubquery(query) = expr
        && let Some((lookup, key)) = plan_correlated_scalar_lookup(read_ctx, query, outer)?
    {
        let index = scalar_lookups.len();
        scalar_lookups.push(lookup);
        return Ok(scalar_lookup_marker(index, key));
    }
    if scalar_lookups.is_empty()
        && direct_correlated
        && let Expr::Exists(query) = expr
        && let Some((lookup, key)) = plan_correlated_exists_lookup(read_ctx, query, outer)?
    {
        let index = scalar_lookups.len();
        scalar_lookups.push(lookup);
        return Ok(Expr::IsNull {
            expr: Box::new(scalar_lookup_marker(index, key)),
            negated: true,
        });
    }
    let lazy_correlated = matches!(expr, Expr::Func(_) | Expr::Case { .. })
        && expression_contains_correlated_subquery(read_ctx, expr, outer);
    let child_deferred = deferred || direct_correlated || lazy_correlated;

    // A non-LATERAL derived table beneath a correlated predicate is still
    // independent of the outer row. Resolve its projection now, before the
    // DML row loop clones and executes the enclosing predicate.
    if direct_correlated
        && matches!(
            expr,
            Expr::ScalarSubquery(_) | Expr::ArraySubquery(_) | Expr::Exists(_)
        )
    {
        return resolve_uncorrelated_derived_projections(read_ctx, expr, outer);
    }

    if deferred && direct_subquery(expr).is_some() && !direct_correlated {
        let (template, lhs) = match expr {
            Expr::InSubquery {
                expr: lhs,
                subquery,
                negated,
            } => (
                Expr::InSubquery {
                    expr: Box::new(initplan_lhs_marker()),
                    subquery: subquery.clone(),
                    negated: *negated,
                },
                Some(install_lazy_initplans(
                    read_ctx,
                    lhs,
                    outer,
                    true,
                    initplans,
                    scalar_lookups,
                )?),
            ),
            Expr::Quantified {
                expr: lhs,
                op,
                all,
                subquery,
            } => (
                Expr::Quantified {
                    expr: Box::new(initplan_lhs_marker()),
                    op: *op,
                    all: *all,
                    subquery: subquery.clone(),
                },
                Some(install_lazy_initplans(
                    read_ctx,
                    lhs,
                    outer,
                    true,
                    initplans,
                    scalar_lookups,
                )?),
            ),
            _ => (expr.clone(), None),
        };
        let typed = replace_subqueries_with_typed_nulls(read_ctx, &template)?;
        let result_type = crate::eval::infer_type(&typed, &Scope::empty())?;
        let index = initplans.len();
        initplans.push(LazyInitPlan {
            template,
            result_type,
            resolved: None,
        });
        return Ok(initplan_marker(index, lhs));
    }

    let mut planned = expr.clone();
    for child in expr_children_mut(&mut planned) {
        *child = install_lazy_initplans(
            read_ctx,
            child,
            outer,
            child_deferred,
            initplans,
            scalar_lookups,
        )?;
    }
    Ok(planned)
}

/// Fold subqueries in non-LATERAL derived projections before their correlated
/// enclosing predicate enters its row loop.
pub(super) fn resolve_uncorrelated_derived_projections(
    read_ctx: &crate::subquery::SubCtx<'_>,
    expr: &Expr,
    outer: &Scope,
) -> Result<Expr, ExecError> {
    let mut resolved = expr.clone();
    let query = match &mut resolved {
        Expr::ScalarSubquery(query) | Expr::ArraySubquery(query) | Expr::Exists(query) => query,
        _ => return Ok(resolved),
    };
    resolve_derived_query_projections(read_ctx, query, outer, false)?;
    Ok(resolved)
}

fn resolve_derived_query_projections(
    read_ctx: &crate::subquery::SubCtx<'_>,
    query: &mut crabka_pgparser::ast::QueryExpr,
    outer: &Scope,
    resolve_projections: bool,
) -> Result<(), ExecError> {
    if let Some(with) = &mut query.with {
        for cte in &mut with.ctes {
            if let crabka_pgparser::ast::CteBody::Query(query) = &mut cte.body {
                resolve_derived_query_projections(read_ctx, query, outer, false)?;
            }
        }
    }
    resolve_derived_set_projections(read_ctx, &mut query.body, outer, resolve_projections)
}

fn resolve_derived_set_projections(
    read_ctx: &crate::subquery::SubCtx<'_>,
    body: &mut crabka_pgparser::ast::SetExpr,
    outer: &Scope,
    resolve_projections: bool,
) -> Result<(), ExecError> {
    use crabka_pgparser::ast::{QueryBody, SetExpr};

    match body {
        SetExpr::Query(QueryBody::Select(select)) => {
            for table in &mut select.from {
                resolve_derived_table_projections(read_ctx, table, outer)?;
            }
            if resolve_projections {
                let nulls = vec![Datum::Null; outer.width()];
                for item in &mut select.projection {
                    let SelectItem::Expr { expr, .. } = item else {
                        continue;
                    };
                    let mut binder = LateralBinder::new(
                        read_ctx.catalog_kv,
                        read_ctx.fctx.resolution,
                        read_ctx.ctes,
                    );
                    if !binder.bind_expr(expr, outer, &nulls)?.1 {
                        *expr = crate::subquery::resolve_expr(read_ctx, expr)?;
                    }
                }
            }
            Ok(())
        }
        SetExpr::Query(QueryBody::Values(_)) => Ok(()),
        SetExpr::Query(QueryBody::Nested(query)) => {
            resolve_derived_query_projections(read_ctx, query, outer, resolve_projections)
        }
        SetExpr::SetOp { left, right, .. } => {
            resolve_derived_set_projections(read_ctx, left, outer, resolve_projections)?;
            resolve_derived_set_projections(read_ctx, right, outer, resolve_projections)
        }
    }
}

fn resolve_derived_table_projections(
    read_ctx: &crate::subquery::SubCtx<'_>,
    table: &mut crabka_pgparser::ast::TableExpr,
    outer: &Scope,
) -> Result<(), ExecError> {
    match table {
        crabka_pgparser::ast::TableExpr::Derived {
            subquery, lateral, ..
        } => resolve_derived_query_projections(read_ctx, subquery, outer, !*lateral),
        crabka_pgparser::ast::TableExpr::Join { left, right, .. } => {
            resolve_derived_table_projections(read_ctx, left, outer)?;
            resolve_derived_table_projections(read_ctx, right, outer)
        }
        _ => Ok(()),
    }
}

/// A `SELECT` with its subqueries planned: the rewritten statement plus the
/// deferred work executing it needs.
pub(super) struct PlannedSubqueries {
    pub(super) select: SelectStmt,
    /// The `WHERE` clause reads the source row, so it is evaluated per row.
    pub(super) correlated_filter: bool,
    pub(super) initplans: Vec<LazyInitPlan>,
    pub(super) scalar_lookups: Vec<CorrelatedScalarLookup>,
    /// Select-list / `ORDER BY` / `DISTINCT ON` expressions that read the source
    /// row. `None` — the overwhelmingly common case — leaves projection on
    /// exactly the path it took before this existed.
    pub(super) row_exprs: Option<CorrelatedRowExprs>,
}

impl PlannedSubqueries {
    /// A statement whose subqueries all folded once, ahead of the row loop.
    fn uncorrelated(select: SelectStmt) -> Self {
        Self {
            select,
            correlated_filter: false,
            initplans: Vec::new(),
            scalar_lookups: Vec::new(),
            row_exprs: None,
        }
    }
}

pub(super) fn resolve_select_subqueries(
    read_ctx: &crate::subquery::SubCtx<'_>,
    select: &SelectStmt,
) -> Result<PlannedSubqueries, ExecError> {
    let preserved = preserve_projection_labels_before_subquery_resolution(select);
    let select = &preserved;
    let correlatable_filter = select.filter.as_ref().is_some_and(contains_subquery);
    // Only a statement that actually contains a subquery in a row-local clause
    // can have a correlated one there. Everything else — every ordinary select
    // list in the system — leaves here without describing its FROM.
    if !correlatable_filter && !row_clauses_contain_subquery(select) {
        return Ok(PlannedSubqueries::uncorrelated(
            crate::subquery::resolve_in_select(read_ctx, select)?,
        ));
    }
    let scope = if select.from.is_empty() {
        Scope::empty()
    } else {
        match build_from_schema_described(
            read_ctx.catalog_kv,
            read_ctx.fctx.resolution,
            &select.from,
            read_ctx.ctes,
            Some(read_ctx.eval_ctx),
            read_ctx.refs,
        ) {
            Ok(described) => described.scope,
            // A FROM the schema pass cannot describe is not a new failure to
            // report: without a correlated WHERE this statement reached the
            // ordinary path before, and it still does. The real read below
            // raises whatever the FROM is actually wrong about.
            Err(_) if !correlatable_filter => {
                return Ok(PlannedSubqueries::uncorrelated(
                    crate::subquery::resolve_in_select(read_ctx, select)?,
                ));
            }
            Err(error) => return Err(error),
        }
    };
    let (select, row_exprs) = plan_correlated_row_exprs(read_ctx, select, &scope)?;
    if !correlatable_filter {
        return Ok(PlannedSubqueries {
            row_exprs,
            ..PlannedSubqueries::uncorrelated(crate::subquery::resolve_in_select(
                read_ctx, &select,
            )?)
        });
    }
    let (select, correlated_filter, initplans, scalar_lookups) =
        prepare_correlated_subqueries(read_ctx, &select, &scope)?;
    Ok(PlannedSubqueries {
        select,
        correlated_filter,
        initplans,
        scalar_lookups,
        row_exprs,
    })
}

/// Subquery resolution folds an expression to a constant, but the output label
/// belongs to the expression as written. Preserve it before that fold erases
/// its `FigureColname` shape.
fn preserve_projection_labels_before_subquery_resolution(select: &SelectStmt) -> SelectStmt {
    let mut preserved = select.clone();
    for item in &mut preserved.projection {
        let SelectItem::Expr { expr, alias } = item else {
            continue;
        };
        if alias.is_none() && contains_subquery(expr) {
            *alias = Some(derived_name(expr));
        }
    }
    preserved
}

/// Does any clause evaluated once per source row hold a subquery?
///
/// The select list, `ORDER BY` and `DISTINCT ON` are the clauses whose
/// expressions [`plan_correlated_row_exprs`] can defer; a subquery anywhere
/// else is either folded once or handled by the `WHERE` path.
fn row_clauses_contain_subquery(select: &SelectStmt) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => contains_subquery(expr),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    }) || select
        .order_by
        .iter()
        .any(|item| contains_subquery(&item.expr))
        || select
            .distinct
            .on_exprs()
            .is_some_and(|on| on.iter().any(contains_subquery))
}

/// Whether a SELECT still needs the legacy subquery folding path.
pub(crate) fn select_contains_subquery(select: &SelectStmt) -> bool {
    row_clauses_contain_subquery(select)
        || select.filter.as_ref().is_some_and(contains_subquery)
        || select.having.as_ref().is_some_and(contains_subquery)
        || select.limit.as_ref().is_some_and(contains_subquery)
        || select.offset.as_ref().is_some_and(contains_subquery)
}

/// Row-local expressions that read the source row, and the hidden relation
/// columns they are materialized into.
#[derive(Default)]
pub(super) struct CorrelatedRowExprs {
    /// The original expressions, used to give two clauses that spell the same
    /// correlated expression the same hidden column — which is what lets
    /// `DISTINCT ON (expr) … ORDER BY expr` still recognize its own key.
    pub(super) sources: Vec<Expr>,
    /// One planned expression per hidden column, in column order.
    pub(super) exprs: Vec<Expr>,
    /// The hidden scope bindings the markers resolve through, in the same order.
    pub(super) bindings: Vec<ColumnBinding>,
    pub(super) initplans: Vec<LazyInitPlan>,
    pub(super) scalar_lookups: Vec<CorrelatedScalarLookup>,
}

impl CorrelatedRowExprs {
    /// Reserve a hidden column for `expr` if it reads `outer`, returning the
    /// marker that stands in for it. `None` leaves the expression where it is.
    pub(super) fn defer(
        &mut self,
        read_ctx: &crate::subquery::SubCtx<'_>,
        expr: &Expr,
        outer: &Scope,
    ) -> Result<Option<Expr>, ExecError> {
        if !contains_subquery(expr) || !validate_correlated_subqueries(read_ctx, expr, outer)? {
            return Ok(None);
        }
        if let Some(index) = self.sources.iter().position(|source| source == expr) {
            return Ok(Some(correlated_marker(index)));
        }
        let planned = install_lazy_initplans(
            read_ctx,
            expr,
            outer,
            false,
            &mut self.initplans,
            &mut self.scalar_lookups,
        )?;
        // Resolve the siblings that do not read the source row once, here, so
        // only the correlated subtrees are left to run per row.
        let planned = crate::subquery::resolve_expr_skipping(read_ctx, &planned, &mut |node| {
            let candidate = scalar_lookup_parts(node).is_some()
                || direct_subquery(node).is_some()
                || matches!(node, Expr::Func(_) | Expr::Case { .. });
            scalar_lookup_parts(node).is_some()
                || candidate && expression_contains_correlated_subquery(read_ctx, node, outer)
        })?;
        let ty = correlated_expr_type(
            read_ctx,
            &planned,
            outer,
            &self.initplans,
            &self.scalar_lookups,
        )?;
        let index = self.exprs.len();
        self.sources.push(expr.clone());
        self.exprs.push(planned);
        self.bindings.push(ColumnBinding {
            exposure: Exposure::Output,
            qualifier: Some(crate::scope::CORRELATED_QUALIFIER.to_string()),
            name: index.to_string(),
            ty,
        });
        Ok(Some(correlated_marker(index)))
    }
}

/// Replace every source-row reference inside a select-list sub-select with a
/// NULL of the referenced column's type, so the sub-select can be *described*
/// without the row it would need to be *run*.
///
/// A sub-select's FROM can read the source row — `generate_series(1,
/// array_upper(p.attrs, 1))` — and describing that FROM needs the reference's
/// type, not its value. Only the sub-selects are rewritten: the references
/// outside them already resolve against `scope`, and turning one into a constant
/// would rename the output column it labels.
pub(crate) fn describable_projection(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    ctes: &crate::cte::CteContext,
    projection: &[SelectItem],
    scope: &Scope,
) -> Result<Vec<SelectItem>, ExecError> {
    let nulls = vec![Datum::Null; scope.width()];
    let mut binder = LateralBinder::new(catalog_kv, resolution, ctes);
    projection
        .iter()
        .enumerate()
        .map(|(walk, item)| {
            let SelectItem::Expr { expr, alias } = item else {
                return Ok(item.clone());
            };
            binder.walking(walk);
            let mut expr = expr.clone();
            bind_subqueries_to_row(&mut binder, &mut expr, scope, &nulls)?;
            Ok(SelectItem::Expr {
                expr,
                alias: alias.clone(),
            })
        })
        .collect()
}

fn bind_subqueries_to_row(
    binder: &mut LateralBinder<'_>,
    expr: &mut Expr,
    scope: &Scope,
    row: &[Datum],
) -> Result<(), ExecError> {
    for query in query_children_mut(expr) {
        *query = binder.bind_query(query, scope, row)?.0;
    }
    for child in expr_children_mut(expr) {
        bind_subqueries_to_row(binder, child, scope, row)?;
    }
    Ok(())
}

/// The static type of a deferred expression: the type it infers to once every
/// outer reference is a NULL of its column's type and every subquery a NULL of
/// the type its projection describes to.
fn correlated_expr_type(
    read_ctx: &crate::subquery::SubCtx<'_>,
    planned: &Expr,
    outer: &Scope,
    initplans: &[LazyInitPlan],
    scalar_lookups: &[CorrelatedScalarLookup],
) -> Result<ColumnType, ExecError> {
    let nulls = vec![Datum::Null; outer.width()];
    let mut binder =
        LateralBinder::new(read_ctx.catalog_kv, read_ctx.fctx.resolution, read_ctx.ctes);
    let (bound, _) = binder.bind_expr(planned, outer, &nulls)?;
    let typed = type_without_evaluating_subqueries(read_ctx, &bound, initplans, scalar_lookups)?;
    crate::eval::infer_type(&typed, outer)
}

/// Does any clause [`plan_correlated_row_exprs`] would defer call an aggregate
/// belonging to `select`'s own query level?
pub(super) fn defers_statement_level_aggregate(
    read_ctx: &crate::subquery::SubCtx<'_>,
    select: &SelectStmt,
    outer: &Scope,
) -> bool {
    let pass = OuterAggregatePass {
        levels: AggregateLevels {
            read_ctx,
            statement: outer,
        },
    };
    select.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => pass.expr(expr, &[]),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    }) || select
        .order_by
        .iter()
        .any(|item| pass.expr(&item.expr, &[]))
        || select
            .distinct
            .on_exprs()
            .is_some_and(|on| on.iter().any(|expr| pass.expr(expr, &[])))
}

/// Finds an aggregate that `PostgreSQL` would assign to the *enclosing* query
/// level rather than to the sub-select it is written in.
///
/// `check_agg_arguments` gives an aggregate the level of the innermost variable
/// its arguments read, ignoring variables local to sub-selects *inside* those
/// arguments. So in
///
/// ```sql
/// SELECT (SELECT max((SELECT i.b FROM inner i WHERE i.a = o.a))) FROM outer o
/// ```
///
/// the only variable `max` reads is `o.a`, one level further out than the
/// sub-select `max` is written in, so the aggregate belongs to the OUTER query —
/// which becomes a grouping query and folds to a single row. Materializing that
/// select-list expression into a per-row hidden column would answer one row per
/// `outer` row instead, which is why the shape has to be recognized before
/// deferral rather than after.
///
/// A name the pass cannot attribute to a level answers "yes" too: declining to
/// defer keeps the missing-FROM-clause error the statement already gets, and an
/// old error beats a new wrong answer.
struct OuterAggregatePass<'a, 'b> {
    levels: AggregateLevels<'a, 'b>,
}

/// Which levels an aggregate's arguments read, to the one resolution the passes
/// over [`AggregateLevels`] need.
#[derive(Default)]
struct AggregateReach {
    /// A variable resolving to the statement's own FROM.
    statement: bool,
    /// A variable resolving to one of the sub-selects in between.
    enclosing: bool,
    /// A variable belonging to no level this pass could describe.
    unknown: bool,
}

/// `PostgreSQL`'s `check_agg_arguments` reduced to the question both aggregate
/// passes ask: does an aggregate written under some enclosing sub-selects take
/// its level from `statement`'s own FROM clause?
///
/// The two callers differ only in what they do with an unattributable name —
/// [`OuterAggregatePass`] declines to defer, [`FromClauseAggregatePass`] declines
/// to reject — so the walk itself reports what it found and leaves the policy to
/// them.
pub(super) struct AggregateLevels<'a, 'b> {
    pub(super) read_ctx: &'a crate::subquery::SubCtx<'b>,
    /// The statement's own FROM: the level whose variables settle the question.
    pub(super) statement: &'a Scope,
}

impl OuterAggregatePass<'_, '_> {
    /// Walk `expr`, written inside the sub-selects whose FROM clauses are
    /// `enclosing` (outermost first). An empty `enclosing` puts `expr` at the
    /// statement's own level, where an aggregate is already
    /// [`crate::grouping::is_grouping_query`]'s business.
    fn expr(&self, expr: &Expr, enclosing: &[&[crabka_pgparser::ast::TableExpr]]) -> bool {
        if !enclosing.is_empty()
            && let Expr::Func(call) = expr
            && crate::agg::is_aggregate_call(call)
            && self.belongs_to_statement(call, enclosing)
        {
            return true;
        }
        expr_children(expr)
            .into_iter()
            .any(|child| self.expr(child, enclosing))
            || direct_subquery(expr).is_some_and(|query| self.query(query, enclosing))
    }

    fn query(
        &self,
        query: &crabka_pgparser::ast::QueryExpr,
        enclosing: &[&[crabka_pgparser::ast::TableExpr]],
    ) -> bool {
        // A tail ORDER BY / LIMIT is evaluated at the query's own level, so it
        // walks under whatever FROM its body introduces. A set operation has no
        // single such FROM; its branches each describe their own.
        let body_from = match &query.body {
            crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Select(
                select,
            )) => select.from.as_slice(),
            _ => &[],
        };
        let inner = [enclosing, &[body_from]].concat();
        self.set_expr(&query.body, enclosing)
            || query
                .order_by
                .iter()
                .map(|item| &item.expr)
                .chain(&query.limit)
                .chain(&query.offset)
                .any(|expr| self.expr(expr, &inner))
            || query
                .with
                .iter()
                .flat_map(|with| &with.ctes)
                .any(|cte| match &cte.body {
                    crabka_pgparser::ast::CteBody::Query(body) => self.query(body, enclosing),
                    // A data-modifying CTE is not a read path this rewrite ever
                    // reaches; whatever it references is resolved elsewhere.
                    crabka_pgparser::ast::CteBody::Dml(_) => false,
                })
    }

    fn set_expr(
        &self,
        body: &crabka_pgparser::ast::SetExpr,
        enclosing: &[&[crabka_pgparser::ast::TableExpr]],
    ) -> bool {
        use crabka_pgparser::ast::{QueryBody, SetExpr};
        match body {
            SetExpr::Query(QueryBody::Select(select)) => {
                let inner = [enclosing, &[select.from.as_slice()]].concat();
                select_exprs(select)
                    .into_iter()
                    .any(|expr| self.expr(expr, &inner))
                    || select
                        .from
                        .iter()
                        .any(|item| self.table_expr(item, enclosing))
            }
            SetExpr::Query(QueryBody::Values(values)) => values
                .rows
                .iter()
                .flatten()
                .any(|expr| self.expr(expr, enclosing)),
            SetExpr::Query(QueryBody::Nested(nested)) => self.query(nested, enclosing),
            SetExpr::SetOp { left, right, .. } => {
                self.set_expr(left, enclosing) || self.set_expr(right, enclosing)
            }
        }
    }

    /// A FROM item is described by its own level, so a sub-select inside one
    /// sits one level down from the query the item belongs to — not inside it.
    fn table_expr(
        &self,
        te: &crabka_pgparser::ast::TableExpr,
        enclosing: &[&[crabka_pgparser::ast::TableExpr]],
    ) -> bool {
        use crabka_pgparser::ast::TableExpr;
        match te {
            TableExpr::Table { .. } => false,
            TableExpr::Derived { subquery, .. } => self.query(subquery, enclosing),
            TableExpr::Function { functions, .. } => functions
                .iter()
                .flat_map(|call| call.arguments())
                .any(|arg| self.expr(arg, enclosing)),
            TableExpr::JsonTable(table) => table
                .exprs()
                .into_iter()
                .any(|expr| self.expr(expr, enclosing)),
            TableExpr::XmlTable(table) => table
                .exprs()
                .into_iter()
                .any(|expr| self.expr(expr, enclosing)),
            TableExpr::Join {
                left,
                right,
                constraint,
                ..
            } => {
                let inner = [
                    enclosing,
                    &[std::slice::from_ref(left.as_ref())],
                    &[std::slice::from_ref(right.as_ref())],
                ]
                .concat();
                self.table_expr(left, enclosing)
                    || self.table_expr(right, enclosing)
                    || matches!(constraint, crabka_pgparser::ast::JoinConstraint::On(on)
                        if self.expr(on, &inner))
            }
        }
    }

    /// Does this aggregate call, written under the `enclosing` sub-selects,
    /// belong to the statement's own level?
    fn belongs_to_statement(
        &self,
        call: &FuncCall,
        enclosing: &[&[crabka_pgparser::ast::TableExpr]],
    ) -> bool {
        let reach = self.levels.reach_of(call, enclosing);
        reach.unknown || (reach.statement && !reach.enclosing)
    }
}

impl AggregateLevels<'_, '_> {
    /// Which levels the arguments of `call`, written under the `enclosing`
    /// sub-selects (outermost first), read.
    fn reach_of(
        &self,
        call: &FuncCall,
        enclosing: &[&[crabka_pgparser::ast::TableExpr]],
    ) -> AggregateReach {
        let mut covered = Shadow::default();
        for from in enclosing {
            covered.extend_by(
                self.read_ctx.catalog_kv,
                self.read_ctx.fctx.resolution,
                self.read_ctx.ctes,
                from,
            );
        }
        let mut reach = AggregateReach::default();
        // `count(*)` reads nothing, so it belongs where it is written.
        if let FuncArgs::Exprs(args) = &call.args {
            for arg in args {
                self.reach(arg, &covered, &Shadow::default(), &mut reach);
            }
        }
        // `FILTER (WHERE …)` is levelled with the arguments it filters.
        if let Some(filter) = &call.filter {
            self.reach(filter, &covered, &Shadow::default(), &mut reach);
        }
        reach
    }

    /// Record which levels `expr` — an aggregate argument — reads.
    ///
    /// `covered` holds the names the enclosing sub-selects supply; `local` those
    /// supplied by sub-selects *within* the argument, whose variables
    /// `check_agg_arguments` ignores outright.
    fn reach(&self, expr: &Expr, covered: &Shadow, local: &Shadow, reach: &mut AggregateReach) {
        if let Expr::Column { table, name } = expr {
            let qualifier = table.as_deref();
            let local = local.supplies(qualifier, name);
            let covered = covered.supplies(qualifier, name);
            // Local to a sub-select of the argument, which is exactly what
            // `check_agg_arguments` ignores.
            if local == Some(true) {
                return;
            }
            if covered == Some(true) {
                reach.enclosing = true;
                return;
            }
            // Past every level in between. A name the statement's own FROM does
            // not supply is read at no level this decision turns on, so leaving
            // a FROM in between undescribed costs nothing for it.
            if self.statement.resolve(qualifier, name).is_err() {
                return;
            }
            // It resolves here — unless a FROM in between that could not be
            // described supplies it too, which is what cannot be settled.
            if local.is_none() || covered.is_none() {
                reach.unknown = true;
            } else {
                reach.statement = true;
            }
            return;
        }
        for child in expr_children(expr) {
            self.reach(child, covered, local, reach);
        }
        if let Some(query) = direct_subquery(expr) {
            self.reach_query(query, covered, local, reach);
        }
    }

    fn reach_query(
        &self,
        query: &crabka_pgparser::ast::QueryExpr,
        covered: &Shadow,
        local: &Shadow,
        reach: &mut AggregateReach,
    ) {
        let crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Select(select)) =
            &query.body
        else {
            // A set operation or nested body reads names this pass does not lay
            // out; leave the aggregate unattributed rather than guess.
            reach.unknown = true;
            return;
        };
        let mut inner = local.clone();
        inner.extend_by(
            self.read_ctx.catalog_kv,
            self.read_ctx.fctx.resolution,
            self.read_ctx.ctes,
            &select.from,
        );
        for expr in select_exprs(select) {
            self.reach(expr, covered, &inner, reach);
        }
        for expr in query.order_by.iter().map(|item| &item.expr) {
            self.reach(expr, covered, &inner, reach);
        }
    }
}

/// Where an aggregate that belongs to the query level owning a FROM clause was
/// written, which is all that separates `PostgreSQL`'s three messages for it.
#[derive(Clone, Copy)]
enum FromAggregateSite {
    /// A sub-select used as a FROM item.
    Subselect,
    /// The arguments of a function (or `JSON_TABLE`) used as a FROM item.
    Function,
    /// A join's `ON` condition.
    JoinCondition,
}

impl FromAggregateSite {
    /// `PostgreSQL` words each site separately rather than through
    /// `ParseExprKindName`, so the strings are spelled out here too.
    fn message(self) -> &'static str {
        match self {
            Self::Subselect => {
                "aggregate functions are not allowed in FROM clause of their own query level"
            }
            Self::Function => "aggregate functions are not allowed in functions in FROM",
            Self::JoinCondition => "aggregate functions are not allowed in JOIN conditions",
        }
    }
}

/// The expression-kind context one aggregate is judged in: the FROM-clause
/// construct it was reached through, and whether it is written at the level that
/// owns the clause rather than inside a sub-select somewhere below it.
#[derive(Clone, Copy)]
struct FromAggregateContext {
    site: FromAggregateSite,
    /// An aggregate reading no variable keeps the level it is written at, so
    /// this is what decides whether that level is the one being checked.
    owning_level: bool,
}

impl FromAggregateContext {
    /// The same site, one query level down.
    fn nested(self) -> Self {
        Self {
            owning_level: false,
            ..self
        }
    }
}

/// Rejects an aggregate that `PostgreSQL` assigns to the query level whose FROM
/// clause is being built.
///
/// `check_agglevels_and_constraints` walks up to the level the aggregate's
/// arguments give it and asks what *that* level is transforming; a level in the
/// middle of its own FROM clause refuses. So
/// `SELECT 1 FROM t a, LATERAL (SELECT max(a.i) FROM u) ss` is an error while
/// `SELECT 1 FROM t a, LATERAL (SELECT max(u.i) FROM u) ss` is not — only the
/// first aggregate reads a variable of the level that owns the FROM clause. It
/// is the level, not the `LATERAL` keyword, that decides: `max(a.i + u.i)`
/// takes the *innermost* level its arguments read and so is allowed.
///
/// Levels below this one need no walk of their own here. A nested FROM item is
/// its own FROM clause, checked when that query builds, so the pass only ever
/// asks whether the level lands exactly here.
pub(super) struct FromClauseAggregatePass<'a, 'b> {
    pub(super) levels: AggregateLevels<'a, 'b>,
}

/// Does `te` write any aggregate call at all?
///
/// A pure AST walk with no catalog reads, which is what keeps
/// [`FromClauseAggregatePass`] free for the FROM clauses — nearly all of them —
/// that cannot trip the rule.
fn from_item_calls_aggregate(te: &crabka_pgparser::ast::TableExpr) -> bool {
    use crabka_pgparser::ast::{JoinConstraint, TableExpr};
    match te {
        TableExpr::Table { .. } => false,
        TableExpr::Derived { subquery, .. } => query_calls_aggregate(subquery),
        TableExpr::Function { functions, .. } => functions
            .iter()
            .flat_map(|call| call.arguments())
            .any(expr_calls_aggregate),
        TableExpr::JsonTable(table) => table.exprs().into_iter().any(expr_calls_aggregate),
        TableExpr::XmlTable(table) => table.exprs().into_iter().any(expr_calls_aggregate),
        TableExpr::Join {
            left,
            right,
            constraint,
            ..
        } => {
            from_item_calls_aggregate(left)
                || from_item_calls_aggregate(right)
                || matches!(constraint, JoinConstraint::On(on) if expr_calls_aggregate(on))
        }
    }
}

/// The [`from_item_calls_aggregate`] pre-filter over one expression, following
/// sub-selects because an aggregate inside one can still belong out here.
fn expr_calls_aggregate(expr: &Expr) -> bool {
    if let Expr::Func(call) = expr
        && crate::agg::is_aggregate_call(call)
    {
        return true;
    }
    expr_children(expr).into_iter().any(expr_calls_aggregate)
        || direct_subquery(expr).is_some_and(query_calls_aggregate)
}

/// The [`from_item_calls_aggregate`] pre-filter over one query expression.
fn query_calls_aggregate(query: &crabka_pgparser::ast::QueryExpr) -> bool {
    use crabka_pgparser::ast::{CteBody, QueryBody, SetExpr};
    fn body(set: &SetExpr) -> bool {
        match set {
            SetExpr::Query(QueryBody::Select(select)) => {
                select_exprs(select).into_iter().any(expr_calls_aggregate)
                    || select.from.iter().any(from_item_calls_aggregate)
            }
            SetExpr::Query(QueryBody::Values(values)) => {
                values.rows.iter().flatten().any(expr_calls_aggregate)
            }
            SetExpr::Query(QueryBody::Nested(nested)) => query_calls_aggregate(nested),
            SetExpr::SetOp { left, right, .. } => body(left) || body(right),
        }
    }
    body(&query.body)
        || query
            .order_by
            .iter()
            .map(|item| &item.expr)
            .chain(&query.limit)
            .chain(&query.offset)
            .any(expr_calls_aggregate)
        || query
            .with
            .iter()
            .flat_map(|with| &with.ctes)
            .any(|cte| match &cte.body {
                CteBody::Query(body) => query_calls_aggregate(body),
                CteBody::Dml(_) => false,
            })
}

impl FromClauseAggregatePass<'_, '_> {
    /// Check one FROM item, and the join constraint that attaches it, against
    /// the level rule.
    pub(super) fn check(
        &self,
        te: &crabka_pgparser::ast::TableExpr,
        constraint: &crabka_pgparser::ast::JoinConstraint,
    ) -> Result<(), ExecError> {
        if let crabka_pgparser::ast::JoinConstraint::On(on) = constraint {
            self.expr(on, &[], Self::owning(FromAggregateSite::JoinCondition))?;
        }
        self.table_expr(te)
    }

    fn owning(site: FromAggregateSite) -> FromAggregateContext {
        FromAggregateContext {
            site,
            owning_level: true,
        }
    }

    /// A FROM item is transformed at the level that owns it, so nothing its own
    /// FROM clause introduces is in scope for it yet — which is why `enclosing`
    /// starts empty however deep the join tree is.
    fn table_expr(&self, te: &crabka_pgparser::ast::TableExpr) -> Result<(), ExecError> {
        use crabka_pgparser::ast::{JoinConstraint, TableExpr};
        if !from_item_calls_aggregate(te) {
            return Ok(());
        }
        match te {
            TableExpr::Table { .. } => Ok(()),
            // A sub-select that is not `LATERAL` cannot see this query level at
            // all, so a reference reaching it is a scope error and never gets as
            // far as being levelled — `PostgreSQL` reports the reference.
            TableExpr::Derived {
                subquery,
                lateral: true,
                ..
            } => self.query(
                subquery,
                &[],
                Self::owning(FromAggregateSite::Subselect).nested(),
            ),
            TableExpr::Derived { .. } => Ok(()),
            TableExpr::Function { functions, .. } => functions
                .iter()
                .flat_map(|call| call.arguments())
                .try_for_each(|arg| self.expr(arg, &[], Self::owning(FromAggregateSite::Function))),
            TableExpr::JsonTable(table) => table.exprs().into_iter().try_for_each(|expr| {
                self.expr(expr, &[], Self::owning(FromAggregateSite::Function))
            }),
            TableExpr::XmlTable(table) => table.exprs().into_iter().try_for_each(|expr| {
                self.expr(expr, &[], Self::owning(FromAggregateSite::Function))
            }),
            TableExpr::Join {
                left,
                right,
                constraint,
                ..
            } => {
                self.table_expr(left)?;
                self.table_expr(right)?;
                match constraint {
                    JoinConstraint::On(on) => {
                        self.expr(on, &[], Self::owning(FromAggregateSite::JoinCondition))
                    }
                    JoinConstraint::Using(_) | JoinConstraint::Natural | JoinConstraint::None => {
                        Ok(())
                    }
                }
            }
        }
    }

    fn expr(
        &self,
        expr: &Expr,
        enclosing: &[&[crabka_pgparser::ast::TableExpr]],
        ctx: FromAggregateContext,
    ) -> Result<(), ExecError> {
        if let Expr::Func(call) = expr
            && crate::agg::is_aggregate_call(call)
            && self.belongs_to_this_level(call, enclosing, ctx)
        {
            return Err(ExecError::FunctionError {
                sqlstate: "42803",
                message: ctx.site.message().into(),
            });
        }
        expr_children(expr)
            .into_iter()
            .try_for_each(|child| self.expr(child, enclosing, ctx))?;
        match direct_subquery(expr) {
            Some(query) => self.query(query, enclosing, ctx.nested()),
            None => Ok(()),
        }
    }

    fn query(
        &self,
        query: &crabka_pgparser::ast::QueryExpr,
        enclosing: &[&[crabka_pgparser::ast::TableExpr]],
        ctx: FromAggregateContext,
    ) -> Result<(), ExecError> {
        use crabka_pgparser::ast::{CteBody, QueryBody, SetExpr};
        // A tail ORDER BY / LIMIT is evaluated at the query's own level, so it
        // walks under whatever FROM its body introduces. A set operation has no
        // single such FROM; its branches each describe their own.
        let body_from = match &query.body {
            SetExpr::Query(QueryBody::Select(select)) => select.from.as_slice(),
            _ => &[],
        };
        let inner = [enclosing, &[body_from]].concat();
        self.set_expr(&query.body, enclosing, ctx)?;
        query
            .order_by
            .iter()
            .map(|item| &item.expr)
            .chain(&query.limit)
            .chain(&query.offset)
            .try_for_each(|expr| self.expr(expr, &inner, ctx))?;
        query
            .with
            .iter()
            .flat_map(|with| &with.ctes)
            .try_for_each(|cte| match &cte.body {
                CteBody::Query(body) => self.query(body, enclosing, ctx),
                // A data-modifying CTE is a write path, not a FROM item this
                // level's rule reaches.
                CteBody::Dml(_) => Ok(()),
            })
    }

    fn set_expr(
        &self,
        body: &crabka_pgparser::ast::SetExpr,
        enclosing: &[&[crabka_pgparser::ast::TableExpr]],
        ctx: FromAggregateContext,
    ) -> Result<(), ExecError> {
        use crabka_pgparser::ast::{QueryBody, SetExpr};
        match body {
            SetExpr::Query(QueryBody::Select(select)) => {
                let inner = [enclosing, &[select.from.as_slice()]].concat();
                select_exprs(select)
                    .into_iter()
                    .try_for_each(|expr| self.expr(expr, &inner, ctx))?;
                // A FROM item of this sub-select belongs to *its* level, so it
                // keeps the enclosing chain the sub-select itself sits in.
                select
                    .from
                    .iter()
                    .try_for_each(|item| self.nested_table_expr(item, enclosing, ctx))
            }
            SetExpr::Query(QueryBody::Values(values)) => values
                .rows
                .iter()
                .flatten()
                .try_for_each(|expr| self.expr(expr, enclosing, ctx)),
            SetExpr::Query(QueryBody::Nested(nested)) => self.query(nested, enclosing, ctx),
            SetExpr::SetOp { left, right, .. } => {
                self.set_expr(left, enclosing, ctx)?;
                self.set_expr(right, enclosing, ctx)
            }
        }
    }

    /// A FROM item of a sub-select *inside* the item being checked. Its own
    /// aggregates are the inner level's business — this walk is only looking for
    /// one whose arguments reach back out to the level owning the FROM clause,
    /// so the site stays the one the outermost item fixed.
    fn nested_table_expr(
        &self,
        te: &crabka_pgparser::ast::TableExpr,
        enclosing: &[&[crabka_pgparser::ast::TableExpr]],
        ctx: FromAggregateContext,
    ) -> Result<(), ExecError> {
        use crabka_pgparser::ast::{JoinConstraint, TableExpr};
        let ctx = ctx.nested();
        match te {
            TableExpr::Table { .. } => Ok(()),
            TableExpr::Derived {
                subquery,
                lateral: true,
                ..
            } => self.query(subquery, enclosing, ctx),
            TableExpr::Derived { .. } => Ok(()),
            TableExpr::Function { functions, .. } => functions
                .iter()
                .flat_map(|call| call.arguments())
                .try_for_each(|arg| self.expr(arg, enclosing, ctx)),
            TableExpr::JsonTable(table) => table
                .exprs()
                .into_iter()
                .try_for_each(|expr| self.expr(expr, enclosing, ctx)),
            TableExpr::XmlTable(table) => table
                .exprs()
                .into_iter()
                .try_for_each(|expr| self.expr(expr, enclosing, ctx)),
            TableExpr::Join {
                left,
                right,
                constraint,
                ..
            } => {
                self.nested_table_expr(left, enclosing, ctx)?;
                self.nested_table_expr(right, enclosing, ctx)?;
                match constraint {
                    JoinConstraint::On(on) => {
                        let inner = [
                            enclosing,
                            &[std::slice::from_ref(left.as_ref())],
                            &[std::slice::from_ref(right.as_ref())],
                        ]
                        .concat();
                        self.expr(on, &inner, ctx)
                    }
                    JoinConstraint::Using(_) | JoinConstraint::Natural | JoinConstraint::None => {
                        Ok(())
                    }
                }
            }
        }
    }

    /// Does this aggregate take its level from the query that owns the FROM
    /// clause being built?
    fn belongs_to_this_level(
        &self,
        call: &FuncCall,
        enclosing: &[&[crabka_pgparser::ast::TableExpr]],
        ctx: FromAggregateContext,
    ) -> bool {
        let reach = self.levels.reach_of(call, enclosing);
        // A name no level could be found for leaves the aggregate's level
        // unsettled, and an aggregate this pass cannot place is one it must not
        // reject — the statement keeps whatever error it already had.
        if reach.unknown || reach.enclosing {
            return false;
        }
        // A variable of the owning query's own FROM fixes the level outright.
        // With no variable at all the aggregate keeps the level it is written
        // at, which settles it only for one written in the FROM clause itself.
        reach.statement || ctx.owning_level
    }
}
