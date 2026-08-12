//! S6: `EXPLAIN` over the interpreter.
//!
//! Gres has no cost-based planner, so this module does not pretend to one. It
//! renders the shape the interpreter will actually execute: the scan, the
//! filter it pushes onto that scan, the aggregation, the sort, and the limit.
//! It uses `PostgreSQL`'s node names and text layout. For single-relation
//! plans, `EXPLAIN (COSTS OFF)` over the statement shapes the engine executes
//! therefore prints text byte-identical to `PostgreSQL`. Text that depends on a
//! planner decision, such as join order, index choice, or parallelism,
//! deliberately does not, and the compatibility matrix says so.
//!
//! Estimates are the one thing that cannot be honest here, so `EXPLAIN` without
//! `COSTS OFF` prints a fixed zero-cost estimate and does not invent numbers.

use std::fmt::Write as _;

use crabka_pgparser::ast::{
    ArraySubscript, BinaryOp, DistinctClause, ExplainFormat, ExplainOptions, Expr, FuncArgs,
    MatchKind, OrderItem, QueryBody, QueryExpr, SelectItem, SelectStmt, SetExpr, Statement,
    TableExpr, UnaryOp,
};

/// One node of the rendered plan tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlanNode {
    /// The `PostgreSQL` node name (`Seq Scan`, `HashAggregate`, `Sort`, …).
    pub(crate) node_type: String,
    /// The relation a scan node reads, when it has one.
    pub(crate) relation: Option<String>,
    /// The relation's alias, which `PostgreSQL` prints after the name when it
    /// differs from it.
    pub(crate) alias: Option<String>,
    /// `key: value` detail lines printed under the node, in `PostgreSQL`'s order.
    pub(crate) details: Vec<(String, String)>,
    pub(crate) children: Vec<PlanNode>,
}

impl PlanNode {
    fn new(node_type: &str) -> Self {
        Self {
            node_type: node_type.to_string(),
            relation: None,
            alias: None,
            details: Vec::new(),
            children: Vec::new(),
        }
    }

    fn with_child(mut self, child: Self) -> Self {
        self.children.push(child);
        self
    }

    fn detail(mut self, key: &str, value: String) -> Self {
        self.details.push((key.to_string(), value));
        self
    }

    /// The node's first text line, without indentation.
    fn headline(&self) -> String {
        let mut line = self.node_type.clone();
        if let Some(relation) = &self.relation {
            line.push_str(" on ");
            line.push_str(relation);
            if let Some(alias) = &self.alias
                && alias != relation
            {
                line.push(' ');
                line.push_str(alias);
            }
        }
        line
    }
}

/// Build the plan tree the interpreter will execute for `statement`.
pub(crate) fn plan_statement(statement: &Statement) -> PlanNode {
    match statement {
        Statement::Query(query) => plan_query(query),
        Statement::Insert { table, source, .. } => {
            let child = match source {
                crabka_pgparser::ast::InsertSource::Values(rows) if rows.len() > 1 => {
                    PlanNode::new("Values Scan").with_relation("*VALUES*")
                }
                crabka_pgparser::ast::InsertSource::Query(query) => plan_query(query),
                _ => PlanNode::new("Result"),
            };
            let mut node = PlanNode::new("Insert");
            node.relation = Some(table.name.clone());
            node.with_child(child)
        }
        Statement::Update { table, filter, .. } => {
            let mut node = PlanNode::new("Update");
            node.relation = Some(table.name.clone());
            node.with_child(scan_node(&table.name, None, filter.as_ref()))
        }
        Statement::Delete { table, filter, .. } => {
            let mut node = PlanNode::new("Delete");
            node.relation = Some(table.name.clone());
            node.with_child(scan_node(&table.name, None, filter.as_ref()))
        }
        other => PlanNode::new(utility_node_type(other)),
    }
}

impl PlanNode {
    fn with_relation(mut self, relation: &str) -> Self {
        self.relation = Some(format!("\"{relation}\""));
        self
    }
}

/// The node name for a statement kind that has no scan plan at all.
const fn utility_node_type(statement: &Statement) -> &'static str {
    match statement {
        Statement::Utility(_) => "Utility Statement",
        _ => "Result",
    }
}

fn plan_query(query: &QueryExpr) -> PlanNode {
    let mut node = plan_set_expr(&query.body);
    if !query.order_by.is_empty() {
        node = PlanNode::new("Sort")
            .detail(
                "Sort Key",
                plan_sort_key(&query.order_by, body_projection(&query.body)),
            )
            .with_child(node);
    }
    if query.limit.is_some() || query.offset.is_some() {
        node = PlanNode::new("Limit").with_child(node);
    }
    node
}

fn plan_set_expr(body: &SetExpr) -> PlanNode {
    match body {
        SetExpr::Query(QueryBody::Select(select)) => plan_select(select),
        SetExpr::Query(QueryBody::Values(values)) => {
            let mut node = PlanNode::new("Values Scan").with_relation("*VALUES*");
            node.alias = None;
            let _ = values;
            node
        }
        SetExpr::Query(QueryBody::Nested(query)) => plan_query(query),
        SetExpr::SetOp {
            all, left, right, ..
        } => {
            let append = PlanNode::new("Append")
                .with_child(plan_set_expr(left))
                .with_child(plan_set_expr(right));
            if *all {
                append
            } else {
                PlanNode::new("HashAggregate").with_child(append)
            }
        }
    }
}

fn plan_select(select: &SelectStmt) -> PlanNode {
    let mut node = plan_from(&select.from, select.filter.as_ref());
    let aggregated = select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Expr { expr, .. } if contains_aggregate(expr)))
        || select.having.is_some();
    if !select.group_by.is_empty() {
        let mut aggregate =
            PlanNode::new("HashAggregate").detail("Group Key", expr_list(&select.group_by));
        if let Some(having) = &select.having {
            aggregate = aggregate.detail("Filter", deparse(having));
        }
        node = aggregate.with_child(node);
    } else if aggregated {
        // An ungrouped `HAVING` is still a predicate the aggregate node applies
        // — the engine folds every row, then tests the one result row and emits
        // nothing when it fails — so it prints as this node's `Filter`, exactly
        // as the grouped branch above does. Leaving it off claimed a plan that
        // returns the aggregate unconditionally.
        let mut aggregate = PlanNode::new("Aggregate");
        if let Some(having) = &select.having {
            aggregate = aggregate.detail("Filter", deparse(having));
        }
        node = aggregate.with_child(node);
    }
    match &select.distinct {
        DistinctClause::All => {}
        DistinctClause::Distinct => {
            let keys = select
                .projection
                .iter()
                .filter_map(|item| match item {
                    SelectItem::Expr { expr, .. } => Some(deparse_bare(expr)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let mut aggregate = PlanNode::new("HashAggregate");
            if !keys.is_empty() {
                aggregate = aggregate.detail("Group Key", keys.join(", "));
            }
            node = aggregate.with_child(node);
        }
        DistinctClause::On(exprs) => {
            node = PlanNode::new("Unique")
                .detail("Group Key", expr_list(exprs))
                .with_child(node);
        }
    }
    if !select.order_by.is_empty() {
        node = PlanNode::new("Sort")
            .detail(
                "Sort Key",
                plan_sort_key(&select.order_by, &select.projection),
            )
            .with_child(node);
    }
    if select.limit.is_some() || select.offset.is_some() {
        node = PlanNode::new("Limit").with_child(node);
    }
    node
}

fn plan_from(from: &[TableExpr], filter: Option<&Expr>) -> PlanNode {
    match from {
        [] => {
            let mut node = PlanNode::new("Result");
            if let Some(filter) = filter {
                node = node.detail("One-Time Filter", deparse(filter));
            }
            node
        }
        // A single-relation plan prints unqualified column references.
        [one] => plan_table_expr_unqualified(one, filter),
        many => {
            let mut node = plan_table_expr(&many[0], None);
            for item in &many[1..] {
                node = PlanNode::new("Nested Loop")
                    .with_child(node)
                    .with_child(plan_table_expr(item, None));
            }
            if let Some(filter) = filter {
                node = node.detail("Join Filter", deparse(filter));
            }
            node
        }
    }
}

/// A single-relation FROM: `PostgreSQL` prints its column references without
/// the relation qualifier even when the query wrote one.
fn plan_table_expr_unqualified(item: &TableExpr, filter: Option<&Expr>) -> PlanNode {
    match (item, filter) {
        (TableExpr::Table { name, alias, .. }, filter) => {
            let mut node = PlanNode::new("Seq Scan");
            node.relation = Some(name.name.clone());
            node.alias = alias.clone();
            if let Some(filter) = filter {
                node = node.detail("Filter", deparse_with(filter, false));
            }
            node
        }
        (item, filter) => plan_table_expr(item, filter),
    }
}

fn plan_table_expr(item: &TableExpr, filter: Option<&Expr>) -> PlanNode {
    match item {
        TableExpr::Table { name, alias, .. } => scan_node(&name.name, alias.as_deref(), filter),
        TableExpr::Derived {
            subquery, alias, ..
        } => {
            let mut node = PlanNode::new("Subquery Scan");
            node.relation = Some(alias.clone());
            let mut node = node.with_child(plan_query(subquery));
            if let Some(filter) = filter {
                node.details.insert(0, ("Filter".into(), deparse(filter)));
            }
            node
        }
        TableExpr::Join { left, right, .. } => {
            let mut node = PlanNode::new("Nested Loop")
                .with_child(plan_table_expr(left, None))
                .with_child(plan_table_expr(right, None));
            if let Some(filter) = filter {
                node = node.detail("Join Filter", deparse(filter));
            }
            node
        }
        other => {
            let _ = other;
            PlanNode::new("Function Scan")
        }
    }
}

fn scan_node(table: &str, alias: Option<&str>, filter: Option<&Expr>) -> PlanNode {
    let mut node = PlanNode::new("Seq Scan");
    node.relation = Some(table.to_string());
    node.alias = alias.map(str::to_string);
    if let Some(filter) = filter {
        node = node.detail("Filter", deparse(filter));
    }
    node
}

/// Does `expr` aggregate, and therefore need an `Aggregate` plan node above the
/// scan?
///
/// This asks the executor's own resolver rather than keeping a list here. A
/// private list drifts: the one this replaced named fourteen aggregates and had
/// not learned `json_agg`, the bitwise trio, the range pair, `var_pop`/`var_samp`,
/// `stddev_pop`/`stddev_samp`, or any of the two-variable statistical family, so
/// `EXPLAIN SELECT corr(a, b) FROM t` printed a bare `Seq Scan` where
/// `PostgreSQL` prints an `Aggregate`. Deferring also means a user-defined
/// aggregate is explained like a built-in one, for free.
fn contains_aggregate(expr: &Expr) -> bool {
    crate::agg::contains_aggregate(expr)
}

fn expr_list(exprs: &[Expr]) -> String {
    exprs
        .iter()
        .map(deparse_bare)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The `Sort Key` of a `Sort` plan node.
///
/// A `Sort` evaluates nothing. `set_dummy_tlist_references` rewrites its target
/// list into bare references to the node underneath, and `ruleutils` prints
/// such a reference by deparsing whatever it points at — wrapped in
/// parentheses, "because our caller probably assumed a Var is a simple
/// expression" (`get_special_variable`). A reference that lands on a plain
/// column *is* a simple expression and gets none. That is the whole rule, and
/// it is why `Sort Key: id DESC` carries no parentheses while
/// `Sort Key: (count(*))` carries a pair the expression never asked for.
///
/// The node that computes an expression — an `Aggregate`'s `Group Key` — prints
/// it without the extra pair, which is why the two lines of one plan can differ
/// by exactly one parenthesis.
fn plan_sort_key(items: &[OrderItem], projection: &[SelectItem]) -> String {
    items
        .iter()
        .map(|item| {
            let target = sort_target(&item.expr, projection);
            let mut key = match target {
                Expr::Column { .. } => deparse_bare(target),
                other => format!("({})", deparse_bare(other)),
            };
            key.push_str(&sort_order_suffix(item));
            key
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// What an `ORDER BY` item actually sorts on — `findTargetlistEntrySQL92`.
///
/// A bare name is an *output* column name before it is anything else: the
/// SELECT list's aliases beat the FROM clause's columns, which is the one place
/// `ORDER BY` reads differently from `GROUP BY`. A bare integer is a position in
/// that same list. Everything else is an expression over the input and stands
/// as written.
///
/// The name never survives into the plan either way — the sort clause points at
/// a target-list entry, and the entry holds the expression — so `EXPLAIN` prints
/// the expression and never the alias the query wrote.
fn sort_target<'a>(item: &'a Expr, projection: &'a [SelectItem]) -> &'a Expr {
    match item {
        Expr::Column { table: None, name } => match output_column(projection, name) {
            // A target that is the column of that same name says nothing the
            // written reference does not, except a table qualifier — and this
            // deparser prints a qualifier only where the query wrote one, while
            // `EXPLAIN` prints one whenever the plan reads more than one
            // relation. Substituting `SELECT t.a … ORDER BY a`'s target would
            // add a qualifier to a single-relation plan, which is the case this
            // module gets right today. Keep what was written.
            Some(Expr::Column { name: column, .. }) if column == name => item,
            Some(target) => target,
            None => item,
        },
        Expr::IntLiteral(digits) => digits
            .parse::<usize>()
            .ok()
            .and_then(|position| nth_output_column(projection, position))
            .unwrap_or(item),
        _ => item,
    }
}

/// The target-list entry `name` names, if the SELECT list exposes one.
///
/// An entry's output name is its `AS` alias, or the column's own name when it
/// is a bare column reference. A wildcard exposes names this module cannot see
/// without the catalog, so a list holding one may fail to find a match that
/// `PostgreSQL` would — which leaves the item deparsed as written, the reading
/// this had before it could resolve anything.
fn output_column<'a>(projection: &'a [SelectItem], name: &str) -> Option<&'a Expr> {
    projection.iter().find_map(|item| match item {
        SelectItem::Expr {
            expr,
            alias: Some(alias),
        } if alias == name => Some(expr),
        SelectItem::Expr { expr, alias: None } => match expr {
            Expr::Column { name: column, .. } if column == name => Some(expr),
            _ => None,
        },
        _ => None,
    })
}

/// The `position`'th output column, counting from one.
///
/// `None` when the list holds a wildcard, because then the positions this
/// module can count are not the positions `PostgreSQL` counts.
fn nth_output_column(projection: &[SelectItem], position: usize) -> Option<&Expr> {
    if projection
        .iter()
        .any(|item| !matches!(item, SelectItem::Expr { .. }))
    {
        return None;
    }
    match projection.get(position.checked_sub(1)?)? {
        SelectItem::Expr { expr, .. } => Some(expr),
        _ => None,
    }
}

/// The SELECT list an `ORDER BY` at this level resolves against. Empty for a
/// set operation, whose output names are the leftmost branch's but whose
/// expressions are not any one branch's.
fn body_projection(body: &SetExpr) -> &[SelectItem] {
    match body {
        SetExpr::Query(QueryBody::Select(select)) => &select.projection,
        SetExpr::Query(QueryBody::Nested(query)) => body_projection(&query.body),
        SetExpr::Query(QueryBody::Values(_)) | SetExpr::SetOp { .. } => &[],
    }
}

/// A sort-key list under an explicit qualification choice.
///
/// This is `get_rule_orderby`, which is what an *aggregate's* own `ORDER BY`
/// deparses through. It is not the plan-node path: no target list stands
/// between the aggregate and its sort expressions, so none of
/// [`plan_sort_key`]'s parentheses apply here.
fn sort_key_with(items: &[OrderItem], qualify: bool) -> String {
    items
        .iter()
        .map(|item| {
            let mut key = deparse_bare_with(&item.expr, qualify);
            key.push_str(&sort_order_suffix(item));
            key
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The direction and NULL-placement spelling that follows a sort key.
///
/// One place for both paths, so an aggregate's `ORDER BY` and a `Sort` node's
/// key can never disagree about them — in `PostgreSQL` they are both
/// `show_sortorder_options`'s wording.
fn sort_order_suffix(item: &OrderItem) -> String {
    let mut suffix = String::new();
    if !item.asc {
        suffix.push_str(" DESC");
    }
    // PostgreSQL prints the NULLS clause only when it is not the default for
    // the direction (NULLS LAST for ASC, NULLS FIRST for DESC).
    if item.nulls_first == item.asc {
        suffix.push_str(if item.nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    suffix
}

/// Deparse `expr` the way `PostgreSQL` prints a `Filter:` line: operator nodes
/// carry their own parentheses.
fn deparse(expr: &Expr) -> String {
    deparse_with(expr, true)
}

/// Deparse `expr`, and drop single-relation column qualifiers when `qualify`
/// is false.
///
/// `PostgreSQL` prints a bare column name whenever the plan has one relation.
/// It qualifies the name only when more than one relation is in scope.
fn deparse_with(expr: &Expr, qualify: bool) -> String {
    match expr {
        Expr::Binary { op, left, right } => format!(
            "({} {} {})",
            deparse_with(left, qualify),
            binary_op_text(*op),
            deparse_with(right, qualify)
        ),
        Expr::IsNull { expr, negated } => format!(
            "({} IS {}NULL)",
            deparse_with(expr, qualify),
            if *negated { "NOT " } else { "" }
        ),
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => format!("(NOT {})", deparse_with(expr, qualify)),
        other => deparse_bare_with(other, qualify),
    }
}

/// Deparse `expr` without the outermost parentheses. This is the spelling
/// `PostgreSQL` uses in `Group Key`/`Sort Key` lists.
fn deparse_bare(expr: &Expr) -> String {
    deparse_bare_with(expr, true)
}

/// The operand of a `::` cast, parenthesised the way `get_coercion_expr` does.
///
/// `PostgreSQL` wraps a coercion's argument in parentheses unconditionally —
/// `(a)::text`, `((a + b))::pg_lsn` — with one exception: a constant that is
/// already the target type is printed by `get_const_expr` instead, which
/// decorates it with `::type` itself and needs no wrapping. A literal is that
/// constant, which is why the corpus has `'42'::bigint` and `NULL::integer`
/// beside `(stringu1)::text`.
fn cast_operand(expr: &Expr, qualify: bool) -> String {
    let rendered = deparse_bare_with(expr, qualify);
    if matches!(
        expr,
        Expr::IntLiteral(_)
            | Expr::NumericLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::BitStringLiteral(_)
            | Expr::BoolLiteral(_)
            | Expr::NullLiteral
    ) {
        return rendered;
    }
    format!("({rendered})")
}

/// A call rendered the ordinary way, as `name(args)` with the modifiers that
/// change which rows an aggregate folds.
///
/// `viewdef::func_text` renders the same envelope for `pg_get_viewdef`, against
/// the same oracle. The two are deliberately separate today — that module's
/// version also rewrites the XML constructors and the SQL value functions,
/// neither of which belongs in a plan line — but they must not drift: this
/// function dropping all three modifiers while its sibling printed them is
/// exactly the bug this comment exists to stop recurring.
fn deparse_plain_call(call: &crabka_pgparser::ast::FuncCall, qualify: bool) -> String {
    let args = match &call.args {
        FuncArgs::Star => "*".to_string(),
        FuncArgs::Exprs(args) => args
            .iter()
            .map(|arg| deparse_bare_with(arg, qualify))
            .collect::<Vec<_>>()
            .join(", "),
    };
    let order_by = if call.order_by.is_empty() {
        String::new()
    } else {
        format!(" ORDER BY {}", sort_key_with(&call.order_by, qualify))
    };
    // `get_agg_expr` adds no parentheses of its own around the predicate; the
    // operator node it holds brings whatever it needs.
    let filter = call.filter.as_ref().map_or_else(String::new, |predicate| {
        format!(" FILTER (WHERE {})", deparse_with(predicate, qualify))
    });
    format!(
        "{}({}{args}{order_by}){filter}",
        call.name,
        if call.distinct { "DISTINCT " } else { "" }
    )
}

fn deparse_bare_with(expr: &Expr, qualify: bool) -> String {
    match expr {
        // A SQL/JSON expression deparses as its function name plus the operands
        // it evaluates; the option clauses are not reconstructed.
        Expr::SqlJson(json) => format!(
            "{}({})",
            json.output_label(),
            json.children()
                .into_iter()
                .map(|child| deparse_bare_with(child, qualify))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Column { table, name } => match table {
            Some(table) if qualify => format!("{table}.{name}"),
            _ => name.clone(),
        },
        Expr::FieldSelect { base, field } => {
            format!("({}).{field}", deparse_bare_with(base, qualify))
        }
        Expr::FieldSelectAll(base) => format!("({}).*", deparse_bare_with(base, qualify)),
        Expr::IntLiteral(digits) | Expr::NumericLiteral(digits) => digits.clone(),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        // `pg_get_expr` renders a bit constant with its type name quoted,
        // because `bit` is a reserved word.
        Expr::BitStringLiteral(bits) => format!("'{bits}'::\"bit\""),
        Expr::BoolLiteral(value) => if *value { "true" } else { "false" }.to_string(),
        Expr::NullLiteral => "NULL".to_string(),
        Expr::Param(index) => format!("${index}"),
        Expr::Collate { expr, collation } => format!(
            "{} COLLATE \"{collation}\"",
            deparse_bare_with(expr, qualify)
        ),
        // An aggregate's `DISTINCT`, its own `ORDER BY` and its `FILTER` all
        // change which rows it folds and in what order, so all three print:
        // `count(*)` and `count(*) FILTER (WHERE …)` are different aggregates,
        // and a plan line that spelled them alike would describe neither.
        //
        // `viewdef::func_text` renders the same envelope for `pg_get_viewdef`,
        // against the same oracle. The two are deliberately separate today —
        // that module's version also rewrites the XML constructors and the SQL
        // value functions, neither of which belongs in a plan line — but they
        // must not drift: this arm dropping all three modifiers while its
        // sibling printed them is exactly the bug this comment exists to stop
        // recurring.
        // `AT TIME ZONE` and `AT LOCAL` reach the planner as `timezone` calls
        // that remember their spelling, and `ruleutils.c` prints the spelling
        // back in a plan line exactly as it does in a view definition. An
        // operator operand keeps its own parentheses under the construct.
        Expr::Func(call) if call.sql_syntax && call.name == "timezone" => {
            let operand = |expr: &Expr| match expr {
                Expr::Binary { .. } | Expr::Unary { .. } => {
                    format!("({})", deparse_bare_with(expr, qualify))
                }
                other => deparse_bare_with(other, qualify),
            };
            match &call.args {
                FuncArgs::Exprs(args) => match args.as_slice() {
                    // Note the reversed argument order: `timezone` takes the
                    // zone first.
                    [zone, value] => {
                        format!("({} AT TIME ZONE {})", operand(value), operand(zone))
                    }
                    [value] => format!("({} AT LOCAL)", operand(value)),
                    _ => deparse_plain_call(call, qualify),
                },
                FuncArgs::Star => deparse_plain_call(call, qualify),
            }
        }
        Expr::Func(call) => deparse_plain_call(call, qualify),
        Expr::Cast { expr, ty } => format!("{}::{}", cast_operand(expr, qualify), ty.name()),
        Expr::Unary { op, expr } => match op {
            UnaryOp::Neg => format!("-{}", deparse_bare_with(expr, qualify)),
            UnaryOp::Plus => format!("+{}", deparse_bare_with(expr, qualify)),
            UnaryOp::Not => format!("(NOT {})", deparse_with(expr, qualify)),
            other => format!("{other:?} {}", deparse_bare_with(expr, qualify)),
        },
        // Forms that carry their own parentheses: `deparse_with` matches these
        // explicitly, so this delegation terminates.
        Expr::Binary { .. } | Expr::IsNull { .. } => deparse_with(expr, qualify),
        Expr::Like {
            expr,
            pattern,
            negated,
            kind,
            ..
        } => format!(
            "({} {} {})",
            deparse_bare_with(expr, qualify),
            match (kind, negated) {
                (MatchKind::Like, false) => "~~",
                (MatchKind::Like, true) => "!~~",
                (MatchKind::ILike, false) => "~~*",
                (MatchKind::ILike, true) => "!~~*",
                (MatchKind::Similar, false) => "SIMILAR TO",
                (MatchKind::Similar, true) => "NOT SIMILAR TO",
            },
            deparse_bare_with(pattern, qualify)
        ),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => format!(
            "({} {}BETWEEN {} AND {})",
            deparse_bare_with(expr, qualify),
            if *negated { "NOT " } else { "" },
            deparse_bare_with(low, qualify),
            deparse_bare_with(high, qualify)
        ),
        Expr::InList {
            expr,
            list,
            negated,
        } => format!(
            "({} {}IN ({}))",
            deparse_bare_with(expr, qualify),
            if *negated { "NOT " } else { "" },
            expr_list_with(list, qualify)
        ),
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            use std::fmt::Write as _;

            let mut text = "CASE".to_string();
            if let Some(operand) = operand {
                text.push(' ');
                text.push_str(&deparse_bare_with(operand, qualify));
            }
            for (when, then) in whens {
                write!(
                    text,
                    " WHEN {} THEN {}",
                    deparse_bare_with(when, qualify),
                    deparse_bare_with(then, qualify)
                )
                .expect("writing to a String cannot fail");
            }
            if let Some(else_result) = else_result {
                write!(text, " ELSE {}", deparse_bare_with(else_result, qualify))
                    .expect("writing to a String cannot fail");
            }
            text.push_str(" END");
            text
        }
        Expr::ArrayLiteral(elements) => {
            format!("ARRAY[{}]", expr_list_with(elements, qualify))
        }
        Expr::Row(fields) => format!("ROW({})", expr_list_with(fields, qualify)),
        Expr::Subscript { base, index } => format!(
            "{}[{}]",
            deparse_bare_with(base, qualify),
            deparse_bare_with(index, qualify)
        ),
        Expr::ArrayRef { base, subscripts } => {
            let mut text = deparse_bare_with(base, qualify);
            for subscript in subscripts {
                match subscript {
                    ArraySubscript::Index(index) => {
                        text.push('[');
                        text.push_str(&deparse_bare_with(index, qualify));
                        text.push(']');
                    }
                    ArraySubscript::Slice { lower, upper } => {
                        let bound = |e: &Option<Expr>| {
                            e.as_ref()
                                .map_or(String::new(), |e| deparse_bare_with(e, qualify))
                        };
                        text.push('[');
                        text.push_str(&bound(lower));
                        text.push(':');
                        text.push_str(&bound(upper));
                        text.push(']');
                    }
                }
            }
            text
        }
        Expr::ArraySubquery(_) => "ARRAY(SubPlan)".to_string(),
        Expr::Const { value, .. } => format!("{value:?}"),
        Expr::Default => "DEFAULT".to_string(),
        // Subquery-bearing forms have no stable one-line spelling and never
        // appear in a scan filter; naming the shape keeps the plan readable
        // without recursing back into a caller that cannot handle them either.
        Expr::ScalarSubquery(_) => "(SubPlan)".to_string(),
        Expr::Exists(_) => "(EXISTS SubPlan)".to_string(),
        Expr::InSubquery { expr, negated, .. } => format!(
            "({} {}IN SubPlan)",
            deparse_bare_with(expr, qualify),
            if *negated { "NOT " } else { "" }
        ),
        Expr::Quantified { expr, all, .. } | Expr::QuantifiedArray { expr, all, .. } => format!(
            "({} {} SubPlan)",
            deparse_bare_with(expr, qualify),
            if *all { "ALL" } else { "ANY" }
        ),
    }
}

/// Deparse a comma-separated expression list without outer parentheses.
fn expr_list_with(exprs: &[Expr], qualify: bool) -> String {
    exprs
        .iter()
        .map(|expr| deparse_bare_with(expr, qualify))
        .collect::<Vec<_>>()
        .join(", ")
}

/// How EXPLAIN spells a binary operator.
///
/// Delegates to [`crate::eval::op_spelling`] rather than keeping a second table.
/// This was a partial copy ending in `_ => "?"`, so every operator it had not
/// been taught — the whole jsonb family, and every geometric operator — printed
/// as a literal `?` in a `Filter:` line. A catch-all in a deparser cannot fail
/// loudly, so the only durable fix is to have ONE exhaustive table that the
/// compiler forces someone to extend when a `BinaryOp` variant is added.
fn binary_op_text(op: BinaryOp) -> &'static str {
    crate::eval::op_spelling(op)
}

/// Render the plan for the wire, one output line per element.
///
/// The root node reports `actual_rows`, which is the count `EXPLAIN ANALYZE`
/// measured for the whole statement.
pub(crate) fn render_with_rows(
    node: &PlanNode,
    options: &ExplainOptions,
    actual_rows: usize,
) -> Vec<String> {
    match options.format {
        ExplainFormat::Text => render_text(node, options, actual_rows),
        ExplainFormat::Json => vec![render_json(node)],
        ExplainFormat::Yaml => vec![render_yaml(node)],
        ExplainFormat::Xml => vec![render_xml(node)],
    }
}

fn render_text(node: &PlanNode, options: &ExplainOptions, actual_rows: usize) -> Vec<String> {
    let mut lines = Vec::new();
    render_text_node(node, options, 0, actual_rows, &mut lines);
    lines
}

fn render_text_node(
    node: &PlanNode,
    options: &ExplainOptions,
    depth: usize,
    actual_rows: usize,
    lines: &mut Vec<String>,
) {
    let root = depth == 0;
    // PostgreSQL puts a child's `->` arrow two columns in from its parent's
    // detail column, and every node's detail lines six columns in from its
    // parent's: arrow at `2 + 6*(depth-1)`, details at `2 + 6*depth`.
    let mut headline = if root {
        node.headline()
    } else {
        let arrow_indent = " ".repeat(2 + (depth - 1) * 6);
        format!("{arrow_indent}->  {}", node.headline())
    };
    if options.costs {
        headline.push_str(" (cost=0.00..0.00 rows=0 width=0)");
    }
    if options.analyze {
        // Only the root node's row count is measured; Gres has no per-node
        // instrumentation, so an inner node reports zero rather than inventing.
        let rows = if root { actual_rows } else { 0 };
        write!(headline, " (actual rows={rows}.00 loops=1)").expect("String write");
    }
    lines.push(headline);
    let detail_indent = " ".repeat(2 + depth * 6);
    for (key, value) in &node.details {
        lines.push(format!("{detail_indent}{key}: {value}"));
    }
    for child in &node.children {
        render_text_node(child, options, depth + 1, actual_rows, lines);
    }
}

fn render_json(node: &PlanNode) -> String {
    let mut lines = vec![
        "[".to_string(),
        "  {".to_string(),
        "    \"Plan\": {".to_string(),
    ];
    json_node(node, 6, &mut lines);
    lines.push("    }".to_string());
    lines.push("  }".to_string());
    lines.push("]".to_string());
    lines.join("\n")
}

/// Emit one plan node's JSON body (without its enclosing braces) at `indent`.
fn json_node(node: &PlanNode, indent: usize, lines: &mut Vec<String>) {
    let pad = " ".repeat(indent);
    let mut fields = vec![
        format!("{pad}\"Node Type\": \"{}\"", json_escape(&node.node_type)),
        format!("{pad}\"Parallel Aware\": false"),
        format!("{pad}\"Async Capable\": false"),
    ];
    if let Some(relation) = &node.relation {
        let alias = node.alias.clone().unwrap_or_else(|| relation.clone());
        fields.push(format!(
            "{pad}\"Relation Name\": \"{}\"",
            json_escape(relation)
        ));
        fields.push(format!("{pad}\"Alias\": \"{}\"", json_escape(&alias)));
    }
    fields.push(format!("{pad}\"Disabled\": false"));
    for (key, value) in &node.details {
        fields.push(format!(
            "{pad}\"{}\": \"{}\"",
            json_escape(key),
            json_escape(value)
        ));
    }
    let last = fields.len().saturating_sub(1);
    for (index, field) in fields.into_iter().enumerate() {
        if index == last && node.children.is_empty() {
            lines.push(field);
        } else {
            lines.push(format!("{field},"));
        }
    }
    if node.children.is_empty() {
        return;
    }
    lines.push(format!("{pad}\"Plans\": ["));
    let child_last = node.children.len() - 1;
    for (index, child) in node.children.iter().enumerate() {
        lines.push(format!("{pad}  {{"));
        json_node(child, indent + 4, lines);
        lines.push(format!(
            "{pad}  }}{}",
            if index == child_last { "" } else { "," }
        ));
    }
    lines.push(format!("{pad}]"));
}

fn json_escape(text: &str) -> String {
    text.replace('\\', "\\\\").replace('"', "\\\"")
}

fn render_yaml(node: &PlanNode) -> String {
    let mut lines = vec!["- Plan:".to_string()];
    yaml_node(node, 4, &mut lines);
    lines.join("\n")
}

fn yaml_node(node: &PlanNode, indent: usize, lines: &mut Vec<String>) {
    let pad = " ".repeat(indent);
    lines.push(format!("{pad}Node Type: \"{}\"", node.node_type));
    lines.push(format!("{pad}Parallel Aware: false"));
    lines.push(format!("{pad}Async Capable: false"));
    if let Some(relation) = &node.relation {
        lines.push(format!("{pad}Relation Name: \"{relation}\""));
        let alias = node.alias.clone().unwrap_or_else(|| relation.clone());
        lines.push(format!("{pad}Alias: \"{alias}\""));
    }
    lines.push(format!("{pad}Disabled: false"));
    for (key, value) in &node.details {
        lines.push(format!("{pad}{key}: \"{value}\""));
    }
    if !node.children.is_empty() {
        lines.push(format!("{pad}Plans:"));
        for child in &node.children {
            yaml_node(child, indent + 4, lines);
        }
    }
}

fn render_xml(node: &PlanNode) -> String {
    let mut lines = vec![
        "<explain xmlns=\"http://www.postgresql.org/2009/explain\">".to_string(),
        "  <Query>".to_string(),
    ];
    xml_node(node, 4, &mut lines);
    lines.push("  </Query>".to_string());
    lines.push("</explain>".to_string());
    lines.join("\n")
}

fn xml_node(node: &PlanNode, indent: usize, lines: &mut Vec<String>) {
    let pad = " ".repeat(indent);
    lines.push(format!("{pad}<Plan>"));
    let inner = " ".repeat(indent + 2);
    lines.push(format!(
        "{inner}<Node-Type>{}</Node-Type>",
        xml_escape(&node.node_type)
    ));
    lines.push(format!("{inner}<Parallel-Aware>false</Parallel-Aware>"));
    lines.push(format!("{inner}<Async-Capable>false</Async-Capable>"));
    if let Some(relation) = &node.relation {
        lines.push(format!(
            "{inner}<Relation-Name>{}</Relation-Name>",
            xml_escape(relation)
        ));
        let alias = node.alias.clone().unwrap_or_else(|| relation.clone());
        lines.push(format!("{inner}<Alias>{}</Alias>", xml_escape(&alias)));
    }
    lines.push(format!("{inner}<Disabled>false</Disabled>"));
    for (key, value) in &node.details {
        let tag = key.replace(' ', "-");
        lines.push(format!(
            "{inner}<{tag}>{}</{tag}>",
            xml_escape(value),
            tag = tag
        ));
    }
    if !node.children.is_empty() {
        lines.push(format!("{inner}<Plans>"));
        for child in &node.children {
            xml_node(child, indent + 4, lines);
        }
        lines.push(format!("{inner}</Plans>"));
    }
    lines.push(format!("{pad}</Plan>"));
}

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn plan_text(sql: &str, options: &ExplainOptions) -> Vec<String> {
        let parsed = crabka_pgparser::parse(sql).expect("statement parses");
        let [statement] = parsed.as_slice() else {
            panic!("expected exactly one statement");
        };
        render_with_rows(&plan_statement(statement), options, 0)
    }

    /// The deparser is two mutually recursive functions. An expression form
    /// that neither of them matches used to bounce between them until the stack
    /// ran out. That aborted the whole process, not just the statement. Every
    /// filter shape must therefore terminate and name itself.
    #[test]
    fn every_filter_expression_shape_deparses_without_recursing_forever() {
        let filters = [
            "f1 SIMILAR TO '_[_[:alpha:]_]_'",
            "f1 NOT SIMILAR TO 'a%'",
            "f1 LIKE 'a%'",
            "f1 NOT ILIKE 'a%'",
            "f1 BETWEEN 'a' AND 'b'",
            "f1 NOT IN ('a', 'b')",
            "CASE WHEN f1 = 'a' THEN 1 ELSE 2 END = 1",
            "f1 = ANY(ARRAY['a', 'b'])",
            "(ARRAY['a'])[1] = f1",
            "ROW(f1, f1) = ROW('a', 'b')",
            "f1 = (SELECT 'a')",
            "EXISTS (SELECT 1)",
            "f1 IN (SELECT 'a')",
            "f1 = ALL (SELECT 'a')",
            "f1 IS NOT NULL AND NOT (f1 = 'a')",
        ];

        for filter in filters {
            let lines = plan_text(
                &format!("SELECT * FROM t WHERE {filter}"),
                &ExplainOptions::default(),
            );
            assert!(
                lines.iter().any(|line| line.contains("Filter:")),
                "no Filter line for `{filter}`: {lines:?}"
            );
        }
    }

    fn costs_off() -> ExplainOptions {
        ExplainOptions {
            costs: false,
            ..ExplainOptions::default()
        }
    }

    /// Each expected block is PostgreSQL 18.4's own `EXPLAIN (COSTS OFF)` text,
    /// captured from the oracle.
    #[test]
    fn costs_off_text_matches_the_postgres_oracle_for_interpreter_shapes() {
        let cases: &[(&str, &[&str])] = &[
            ("SELECT * FROM d1", &["Seq Scan on d1"]),
            (
                "SELECT * FROM d1 WHERE id = 1",
                &["Seq Scan on d1", "  Filter: (id = 1)"],
            ),
            (
                "SELECT * FROM d1 WHERE s = 'a'",
                &["Seq Scan on d1", "  Filter: (s = 'a'::text)"],
            ),
            (
                "SELECT * FROM d1 WHERE id > 1 AND s < 'b'",
                &["Seq Scan on d1", "  Filter: ((id > 1) AND (s < 'b'::text))"],
            ),
            (
                "SELECT * FROM d1 WHERE id IS NULL",
                &["Seq Scan on d1", "  Filter: (id IS NULL)"],
            ),
            (
                "SELECT * FROM d1 WHERE id + 1 = 2",
                &["Seq Scan on d1", "  Filter: ((id + 1) = 2)"],
            ),
            (
                "SELECT count(*) FROM d1",
                &["Aggregate", "  ->  Seq Scan on d1"],
            ),
            (
                "SELECT count(*) FROM d1 WHERE id = 1",
                &[
                    "Aggregate",
                    "  ->  Seq Scan on d1",
                    "        Filter: (id = 1)",
                ],
            ),
            (
                "SELECT id, count(*) FROM d1 GROUP BY id",
                &["HashAggregate", "  Group Key: id", "  ->  Seq Scan on d1"],
            ),
            (
                "SELECT id, s FROM d1 GROUP BY id, s",
                &[
                    "HashAggregate",
                    "  Group Key: id, s",
                    "  ->  Seq Scan on d1",
                ],
            ),
            (
                "SELECT id, count(*) FROM d1 GROUP BY id HAVING count(*) > 1",
                &[
                    "HashAggregate",
                    "  Group Key: id",
                    "  Filter: (count(*) > 1)",
                    "  ->  Seq Scan on d1",
                ],
            ),
            (
                "SELECT DISTINCT id FROM d1",
                &["HashAggregate", "  Group Key: id", "  ->  Seq Scan on d1"],
            ),
            (
                "SELECT * FROM d1 ORDER BY id DESC, s",
                &["Sort", "  Sort Key: id DESC, s", "  ->  Seq Scan on d1"],
            ),
            // A `Sort` evaluates nothing, so its key is a reference to the node
            // below and prints inside a pair of parentheses of its own — unless
            // the reference lands on a plain column, as the two above do.
            (
                "SELECT * FROM d1 ORDER BY id + 1",
                &["Sort", "  Sort Key: ((id + 1))", "  ->  Seq Scan on d1"],
            ),
            (
                "SELECT s FROM d1 ORDER BY lower(s)",
                &["Sort", "  Sort Key: (lower(s))", "  ->  Seq Scan on d1"],
            ),
            (
                "SELECT * FROM d1 ORDER BY (id + 1) DESC NULLS LAST",
                &[
                    "Sort",
                    "  Sort Key: ((id + 1)) DESC NULLS LAST",
                    "  ->  Seq Scan on d1",
                ],
            ),
            // `ORDER BY` names an output column before it names anything else,
            // and the plan carries the expression the name stood for.
            (
                "SELECT id + 1 AS x FROM d1 ORDER BY x",
                &["Sort", "  Sort Key: ((id + 1))", "  ->  Seq Scan on d1"],
            ),
            (
                "SELECT id AS x FROM d1 ORDER BY x",
                &["Sort", "  Sort Key: id", "  ->  Seq Scan on d1"],
            ),
            // The written reference stands where the target adds only a
            // qualifier: PostgreSQL prints one here only when the plan reads
            // more than one relation, and this one reads d1 alone.
            (
                "SELECT d1.id FROM d1 ORDER BY id",
                &["Sort", "  Sort Key: id", "  ->  Seq Scan on d1"],
            ),
            (
                "SELECT id, s FROM d1 ORDER BY 2",
                &["Sort", "  Sort Key: s", "  ->  Seq Scan on d1"],
            ),
            (
                "SELECT count(*) FROM d1 ORDER BY count(*)",
                &[
                    "Sort",
                    "  Sort Key: (count(*))",
                    "  ->  Aggregate",
                    "        ->  Seq Scan on d1",
                ],
            ),
            // The `HashAggregate` computes the key, so it prints without the
            // extra pair the `Sort` above it adds to the very same expression.
            (
                "SELECT DISTINCT id + 1 FROM d1 ORDER BY 1",
                &[
                    "Sort",
                    "  Sort Key: ((id + 1))",
                    "  ->  HashAggregate",
                    "        Group Key: (id + 1)",
                    "        ->  Seq Scan on d1",
                ],
            ),
            // A cast parenthesises what it converts; a literal constant carries
            // its own type decoration instead and is left alone.
            (
                "SELECT * FROM d1 WHERE id::text = 'a'",
                &["Seq Scan on d1", "  Filter: ((id)::text = 'a'::text)"],
            ),
            (
                "SELECT * FROM d1 ORDER BY (id)::text",
                &["Sort", "  Sort Key: ((id)::text)", "  ->  Seq Scan on d1"],
            ),
            (
                "SELECT * FROM d1 LIMIT 2 OFFSET 1",
                &["Limit", "  ->  Seq Scan on d1"],
            ),
            (
                "SELECT * FROM d1 x WHERE x.id = 1",
                &["Seq Scan on d1 x", "  Filter: (id = 1)"],
            ),
            ("SELECT 1 + 1", &["Result"]),
            (
                "UPDATE d1 SET s = 'z' WHERE id = 1",
                &[
                    "Update on d1",
                    "  ->  Seq Scan on d1",
                    "        Filter: (id = 1)",
                ],
            ),
            (
                "DELETE FROM d1 WHERE id = 1",
                &[
                    "Delete on d1",
                    "  ->  Seq Scan on d1",
                    "        Filter: (id = 1)",
                ],
            ),
            (
                "INSERT INTO d1 VALUES (3, 'c')",
                &["Insert on d1", "  ->  Result"],
            ),
        ];
        for (sql, expected) in cases {
            assert!(plan_text(sql, &costs_off()) == *expected, "{sql}");
        }
    }

    #[test]
    fn a_qualified_filter_column_keeps_its_qualifier_only_when_written() {
        // PostgreSQL drops a single-relation qualifier in non-VERBOSE text, which
        // is what the deparser reproduces for the alias form above; a genuinely
        // multi-relation reference keeps it.
        let lines = plan_text("SELECT * FROM d1, d2 WHERE d1.id = d2.id", &costs_off());
        assert!(lines[0] == "Nested Loop");
        assert!(lines[1] == "  Join Filter: (d1.id = d2.id)");
    }

    #[test]
    fn json_yaml_and_xml_wrap_the_same_tree() {
        let json = plan_text(
            "SELECT * FROM d1",
            &ExplainOptions {
                costs: false,
                format: ExplainFormat::Json,
                ..ExplainOptions::default()
            },
        );
        assert!(json.len() == 1);
        assert!(json[0].contains("\"Node Type\": \"Seq Scan\""));
        assert!(json[0].contains("\"Relation Name\": \"d1\""));
        assert!(json[0].starts_with("[\n  {\n    \"Plan\": {"));
        assert!(json[0].ends_with(']'));

        let yaml = plan_text(
            "SELECT * FROM d1",
            &ExplainOptions {
                costs: false,
                format: ExplainFormat::Yaml,
                ..ExplainOptions::default()
            },
        );
        assert!(yaml[0].starts_with("- Plan:"));
        assert!(yaml[0].contains("Node Type: \"Seq Scan\""));

        let xml = plan_text(
            "SELECT * FROM d1",
            &ExplainOptions {
                costs: false,
                format: ExplainFormat::Xml,
                ..ExplainOptions::default()
            },
        );
        assert!(xml[0].starts_with("<explain xmlns="));
        assert!(xml[0].contains("<Node-Type>Seq Scan</Node-Type>"));
    }
}
