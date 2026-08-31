//! S6: `EXPLAIN` over the interpreter.
//!
//! Gres has no cost-based planner, so this module does not pretend to one. It
//! renders the shape the interpreter will actually execute: the scan, the
//! filter it pushes onto that scan, the aggregation, the sort, and the limit.
//! It uses `PostgreSQL`'s node names and text layout. For single-relation
//! plans, `EXPLAIN (COSTS OFF)` over the statement shapes the engine executes
//! therefore prints text byte-identical to `PostgreSQL`. Text that depends on a
//! planner decision, such as join order or index choice, deliberately does
//! not. `debug_parallel_query` is the exception: its forced `Gather` shape is
//! rendered explicitly for regression compatibility.
//!
//! Costs still use the conservative zero-cost placeholder until the cost model
//! lands.  Row estimates, however, come from the persisted `ANALYZE` slots
//! whenever this syntactic plan has one ordinary base relation.

use std::fmt::Write as _;

use crabka_pgparser::ast::{
    ArraySubscript, BinaryOp, DistinctClause, ExplainFormat, ExplainOptions, Expr, FuncArgs,
    MatchKind, OrderItem, QueryBody, QueryExpr, SelectItem, SelectStmt, SetExpr, Statement,
    TableExpr, UnaryOp, WithClause,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct CtePlan {
    name: String,
    plan: Box<PlanNode>,
}

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
    init_plans: Vec<CtePlan>,
    pub(crate) children: Vec<PlanNode>,
    output: Vec<String>,
    actual: Option<PlanActual>,
    estimated_rows: Option<u64>,
}

/// Measurements gathered by the executor for one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PlanActual {
    rows: u64,
    loops: u64,
    rows_removed: u64,
}

impl PlanNode {
    fn new(node_type: &str) -> Self {
        Self {
            node_type: node_type.to_string(),
            relation: None,
            alias: None,
            details: Vec::new(),
            init_plans: Vec::new(),
            children: Vec::new(),
            output: Vec::new(),
            actual: None,
            estimated_rows: None,
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

/// Render the planner's forced-parallel shape for `debug_parallel_query`.
pub(crate) fn debug_parallel_gather(child: PlanNode) -> PlanNode {
    let output = child
        .output
        .iter()
        .map(|value| format!("({value})"))
        .collect();
    let mut gather = PlanNode::new("Gather")
        .detail("Workers Planned", "1".into())
        .detail("Single Copy", "true".into())
        .with_child(child);
    gather.output = output;
    gather
}

/// Apply the first statistics-backed row estimate to a rendered plan.  This
/// deliberately owns only one base table and simple restriction expressions;
/// a join estimate must wait for the cost planner's join and equivalence-class
/// machinery instead of multiplying unrelated selectivities here.
pub(crate) fn apply_catalog_estimate(
    catalog_kv: &dyn crabka_pgkv::Kv,
    resolution: &crate::relname::ResolutionScope,
    ctx: &crate::clock::EvalCtx,
    statement: &Statement,
    plan: &mut PlanNode,
) {
    let Statement::Query(query) = statement else {
        return;
    };
    let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
        return;
    };
    let [TableExpr::Table { name, only, .. }] = select.from.as_slice() else {
        return;
    };
    let Ok(relation) = crate::relname::resolve_relation(
        catalog_kv,
        resolution,
        name,
        crate::relname::SchemaDisposition::Reference,
    ) else {
        return;
    };
    let Ok(table) = crabka_pgcatalog::get_table(catalog_kv, &relation) else {
        return;
    };
    let partitioned = crate::partition::is_partitioned(catalog_kv, &relation).unwrap_or(false);
    let inheritance_children =
        crate::inheritance::has_children(catalog_kv, &relation).unwrap_or(false);
    let inherited = !only && (inheritance_children || partitioned);
    if !*only && inheritance_children && !partitioned {
        let mut members = vec![table.clone()];
        for descendant in crate::inheritance::descendants(catalog_kv, &relation).unwrap_or_default()
        {
            if let Ok(table) = crabka_pgcatalog::get_table(catalog_kv, &descendant) {
                members.push(table);
            }
        }
        let estimates = members
            .iter()
            .map(|table| {
                let rows = relation_rows(catalog_kv, &table.name);
                let input_rows = select.filter.as_ref().map_or(rows, |filter| {
                    rows * restriction_selectivity(catalog_kv, table, rows, false, ctx, filter)
                });
                (table, input_rows)
            })
            .collect::<Vec<_>>();
        let input_rows = estimates
            .iter()
            .map(|(_, rows)| row_estimate(*rows) as f64)
            .sum();
        let output_rows =
            extended_group_estimate(catalog_kv, &table, true, select).unwrap_or_else(|| {
                estimates
                    .iter()
                    .map(|(table, rows)| {
                        row_estimate(
                            estimate_group_rows(catalog_kv, table, *rows, false, select)
                                .unwrap_or(*rows),
                        ) as f64
                    })
                    .sum()
            });
        set_estimated_rows(plan, output_rows, input_rows);
        return;
    }
    // A partitioned table's `reltuples` describes its whole tree, while an
    // `ONLY` scan has no local rows to estimate.
    let rows = if *only && partitioned {
        0.0
    } else {
        relation_rows(catalog_kv, &relation)
    };
    let selectivity = select.filter.as_ref().map_or(1.0, |filter| {
        restriction_selectivity(catalog_kv, &table, rows, inherited, ctx, filter)
    });
    let input_rows = rows * selectivity;
    let output_rows = estimate_group_rows(catalog_kv, &table, input_rows, inherited, select)
        .unwrap_or(input_rows);
    set_estimated_rows(plan, output_rows, input_rows);
}

fn relation_rows(
    catalog_kv: &dyn crabka_pgkv::Kv,
    relation: &crabka_pgcatalog::RelationName,
) -> f64 {
    crate::relstats::of(catalog_kv, relation)
        .ok()
        .map(|stats| f64::from(stats.reltuples))
        .filter(|rows| *rows >= 0.0)
        .unwrap_or(1_000.0)
}

fn set_estimated_rows(node: &mut PlanNode, rows: f64, child_rows: f64) {
    node.estimated_rows = Some(row_estimate(rows));
    for child in &mut node.children {
        set_estimated_rows(child, child_rows, child_rows);
    }
}

fn row_estimate(rows: f64) -> u64 {
    if !rows.is_finite() || rows <= 1.0 {
        1
    } else {
        rows.round() as u64
    }
}

fn estimate_group_rows(
    catalog_kv: &dyn crabka_pgkv::Kv,
    table: &crabka_pgcatalog::Table,
    input_rows: f64,
    inherited: bool,
    select: &crabka_pgparser::ast::SelectStmt,
) -> Option<f64> {
    if let Some(rows) = extended_group_estimate(catalog_kv, table, inherited, select) {
        return Some(rows);
    }
    let keys = group_keys(select, table)?;
    let statistics = crabka_pgcatalog::statistics::list(catalog_kv)
        .ok()?
        .into_iter()
        .filter(|object| object.table_id == table.id)
        .collect::<Vec<_>>();
    let mut remaining = keys.clone();
    let mut distincts = Vec::new();
    loop {
        let mut best: Option<(Vec<(GroupKey, i16)>, f64)> = None;
        for object in &statistics {
            let Some(object_keys) = statistic_object_keys(object) else {
                continue;
            };
            let matched = matching_statistics_keys(&object_keys, &remaining, table);
            let positions = matched
                .iter()
                .map(|(_, position)| *position)
                .collect::<Vec<_>>();
            let Some(rows) = statistics_data(object, inherited)
                .and_then(|data| data.ndistinct.as_deref())
                .and_then(|data| ndistinct_for_keys(data, &positions))
            else {
                continue;
            };
            if matched.len() >= 2
                && best
                    .as_ref()
                    .is_none_or(|(best, _)| statistics_match_is_better(&matched, best))
            {
                best = Some((matched, rows));
            }
        }
        let Some((matched, rows)) = best else {
            break;
        };
        remaining.retain(|wanted| {
            !matched
                .iter()
                .any(|(key, _)| statistic_key_matches_group_key(key, wanted, table))
        });
        distincts.push(rows);
    }
    let mut attnums = remaining
        .iter()
        .map(|key| group_key_attributes(key, table))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    attnums.sort_unstable();
    attnums.dedup();
    let relation_rows = relation_rows(catalog_kv, &table.name).max(input_rows);
    let attribute_distincts = attnums
        .iter()
        .filter_map(|attnum| {
            let stats = crate::attrstats::get(
                catalog_kv,
                &crate::attrstats::AttributeStatsKey {
                    relation: table.name.clone(),
                    attnum: *attnum,
                    inherited: false,
                },
            )
            .ok()??;
            let n_distinct = f64::from(stats.n_distinct?);
            Some(if n_distinct < 0.0 {
                -n_distinct * relation_rows
            } else if n_distinct > 0.0 {
                n_distinct
            } else {
                1.0 / crate::plan::selfuncs::DEFAULT_EQ_SEL
            })
        })
        .collect::<Vec<_>>();
    let has_all_attribute_distincts = attribute_distincts.len() == attnums.len();
    distincts.extend(attribute_distincts);
    has_all_attribute_distincts
        .then(|| crate::plan::selfuncs::estimate_num_groups(input_rows, relation_rows, &distincts))
}

fn group_keys(
    select: &crabka_pgparser::ast::SelectStmt,
    table: &crabka_pgcatalog::Table,
) -> Option<Vec<GroupKey>> {
    if select.group_by.is_empty() {
        return None;
    }
    let mut keys = select
        .group_by
        .iter()
        .map(|expr| group_key(expr, &select.projection, table))
        .collect::<Option<Vec<_>>>()?;
    keys.sort_unstable();
    keys.dedup();
    Some(keys)
}

fn extended_group_estimate(
    catalog_kv: &dyn crabka_pgkv::Kv,
    table: &crabka_pgcatalog::Table,
    inherited: bool,
    select: &crabka_pgparser::ast::SelectStmt,
) -> Option<f64> {
    let keys = group_keys(select, table)?;
    let statistics = crabka_pgcatalog::statistics::list(catalog_kv)
        .ok()?
        .into_iter()
        .filter(|object| object.table_id == table.id)
        .collect::<Vec<_>>();
    statistics.iter().find_map(|object| {
        let positions = statistics_positions(&object, &keys, table)?;
        statistics_data(object, inherited)
            .and_then(|data| data.ndistinct.as_deref())
            .and_then(|data| ndistinct_for_keys(&data, &positions))
    })
}

fn statistics_data(
    object: &crabka_pgcatalog::statistics::Statistics,
    inherited: bool,
) -> Option<&crabka_pgcatalog::statistics::StatisticsData> {
    if inherited {
        object.inherited_data.as_ref()
    } else {
        object.data.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum GroupKey {
    Attribute(i16),
    Expression(String),
}

fn group_key(
    expr: &Expr,
    projection: &[SelectItem],
    table: &crabka_pgcatalog::Table,
) -> Option<GroupKey> {
    match expr {
        Expr::IntLiteral(position) => {
            match projection.get(position.parse::<usize>().ok()?.checked_sub(1)?)? {
                SelectItem::Expr { expr, .. } => group_key(expr, projection, table),
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => None,
            }
        }
        Expr::Column { .. } => column_attnum(expr, table).map(GroupKey::Attribute),
        expr => Some(GroupKey::Expression(crate::viewdef::expression_text(
            expr,
            crabka_pgtypes::encoding::OutputStyle::with_zone(&jiff::tz::TimeZone::UTC),
        ))),
    }
}

fn column_attnum(expr: &Expr, table: &crabka_pgcatalog::Table) -> Option<i16> {
    let column = column_name(expr)?;
    table
        .columns
        .iter()
        .position(|definition| definition.name == column)
        .and_then(|position| i16::try_from(position + 1).ok())
}

fn ndistinct_for_keys(data: &str, keys: &[i16]) -> Option<f64> {
    let key = keys
        .iter()
        .map(i16::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let (_, value) = data.split_once(&format!("\"{key}\":"))?;
    value
        .trim_start()
        .split([',', '}'])
        .next()?
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value >= 1.0)
}

fn statistics_positions(
    object: &crabka_pgcatalog::statistics::Statistics,
    keys: &[GroupKey],
    table: &crabka_pgcatalog::Table,
) -> Option<Vec<i16>> {
    let object_keys = statistic_object_keys(object)?;
    let positions = matching_statistics_keys(&object_keys, keys, table)
        .into_iter()
        .map(|(_, position)| position)
        .collect::<Vec<_>>();
    let covered = keys.iter().all(|wanted| {
        object_keys
            .iter()
            .any(|(key, _)| statistic_key_matches_group_key(key, wanted, table))
            || matches!(wanted, GroupKey::Expression(expression)
                if statistic_expression_is_grouped_by_attributes(expression, keys, table))
    });
    (covered && !positions.is_empty()).then_some(positions)
}

fn statistic_object_keys(
    object: &crabka_pgcatalog::statistics::Statistics,
) -> Option<Vec<(GroupKey, i16)>> {
    let mut expression = 0_i16;
    object
        .keys
        .iter()
        .map(|attnum| {
            let key = if *attnum == 0 {
                expression -= 1;
                GroupKey::Expression(
                    object
                        .expressions
                        .get(usize::try_from(-expression - 1).ok()?)?
                        .clone(),
                )
            } else {
                GroupKey::Attribute(*attnum)
            };
            Some((key, if *attnum == 0 { expression } else { *attnum }))
        })
        .collect()
}

fn matching_statistics_keys(
    object_keys: &[(GroupKey, i16)],
    keys: &[GroupKey],
    table: &crabka_pgcatalog::Table,
) -> Vec<(GroupKey, i16)> {
    object_keys
        .iter()
        .filter(|(key, _)| {
            keys.iter()
                .any(|wanted| statistic_key_matches_group_key(key, wanted, table))
        })
        .cloned()
        .collect()
}

fn statistic_key_matches_group_key(
    statistic_key: &GroupKey,
    group_key: &GroupKey,
    table: &crabka_pgcatalog::Table,
) -> bool {
    statistic_key == group_key
        || matches!(statistic_key, GroupKey::Attribute(_))
            && group_key_attribute(group_key, table) == group_key_attribute(statistic_key, table)
}

fn statistics_match_is_better(candidate: &[(GroupKey, i16)], best: &[(GroupKey, i16)]) -> bool {
    let expression_matches = |keys: &[(GroupKey, i16)]| {
        keys.iter()
            .filter(|(key, _)| matches!(key, GroupKey::Expression(_)))
            .count()
    };
    (expression_matches(candidate), candidate.len()) > (expression_matches(best), best.len())
}

fn group_key_attribute(key: &GroupKey, table: &crabka_pgcatalog::Table) -> Option<i16> {
    match key {
        GroupKey::Attribute(attnum) => Some(*attnum),
        GroupKey::Expression(expression) => {
            statistic_expression_inverts_attribute(expression, table)
        }
    }
}

fn group_key_attributes(key: &GroupKey, table: &crabka_pgcatalog::Table) -> Option<Vec<i16>> {
    match key {
        GroupKey::Attribute(attnum) => Some(vec![*attnum]),
        GroupKey::Expression(expression) => {
            let expression = crabka_pgparser::parser::parse_expression(expression).ok()?;
            let mut attributes = Vec::new();
            (expression_group_attributes(&expression, table, &mut attributes)
                && !attributes.is_empty())
            .then_some(attributes)
        }
    }
}

fn statistic_expression_inverts_attribute(
    expression: &str,
    table: &crabka_pgcatalog::Table,
) -> Option<i16> {
    let Expr::Binary { op, left, right } =
        crabka_pgparser::parser::parse_expression(expression).ok()?
    else {
        return None;
    };
    match (op, left.as_ref(), right.as_ref()) {
        (BinaryOp::Add, Expr::Column { .. }, literal)
        | (BinaryOp::Sub, Expr::Column { .. }, literal)
            if matches!(literal, Expr::IntLiteral(_) | Expr::NumericLiteral(_)) =>
        {
            column_attnum(left.as_ref(), table)
        }
        (BinaryOp::Add, literal, Expr::Column { .. })
            if matches!(literal, Expr::IntLiteral(_) | Expr::NumericLiteral(_)) =>
        {
            column_attnum(right.as_ref(), table)
        }
        (BinaryOp::Mul, Expr::Column { .. }, Expr::IntLiteral(value)) if value != "0" => {
            column_attnum(left.as_ref(), table)
        }
        (BinaryOp::Mul, Expr::IntLiteral(value), Expr::Column { .. }) if value != "0" => {
            column_attnum(right.as_ref(), table)
        }
        _ => None,
    }
}

fn statistic_expression_is_grouped_by_attributes(
    expression: &str,
    keys: &[GroupKey],
    table: &crabka_pgcatalog::Table,
) -> bool {
    let expression = match crabka_pgparser::parser::parse_expression(expression) {
        Ok(expression) => expression,
        Err(_) => return false,
    };
    let mut attributes = Vec::new();
    expression_group_attributes(&expression, table, &mut attributes)
        && !attributes.is_empty()
        && attributes
            .into_iter()
            .all(|attnum| keys.contains(&GroupKey::Attribute(attnum)))
}

fn expression_group_attributes(
    expression: &Expr,
    table: &crabka_pgcatalog::Table,
    attributes: &mut Vec<i16>,
) -> bool {
    match expression {
        Expr::Column { .. } => column_attnum(expression, table).is_some_and(|attnum| {
            if !attributes.contains(&attnum) {
                attributes.push(attnum);
            }
            true
        }),
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Const { .. } => true,
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => {
            expression_group_attributes(expr, table, attributes)
        }
        Expr::Binary { left, right, .. } => {
            expression_group_attributes(left, table, attributes)
                && expression_group_attributes(right, table, attributes)
        }
        _ => false,
    }
}

fn statistics_key_positions(
    object: &crabka_pgcatalog::statistics::Statistics,
    wanted: &GroupKey,
) -> Option<usize> {
    let mut expression = 0_usize;
    object
        .keys
        .iter()
        .enumerate()
        .find_map(|(position, attnum)| {
            let key = if *attnum == 0 {
                let key = GroupKey::Expression(object.expressions.get(expression)?.clone());
                expression += 1;
                key
            } else {
                GroupKey::Attribute(*attnum)
            };
            (key == *wanted).then_some(position)
        })
}

fn statistics_data_position(
    object: &crabka_pgcatalog::statistics::Statistics,
    wanted: usize,
) -> Option<i16> {
    let mut expression = 0_i16;
    object.keys.iter().enumerate().find_map(|(index, key)| {
        if *key == 0 {
            expression -= 1;
        }
        (index == wanted).then_some(if *key == 0 { expression } else { *key })
    })
}

fn restriction_selectivity(
    catalog_kv: &dyn crabka_pgkv::Kv,
    table: &crabka_pgcatalog::Table,
    rows: f64,
    inherited: bool,
    ctx: &crate::clock::EvalCtx,
    expr: &Expr,
) -> f64 {
    if let Some(selectivity) =
        extended_mcv_selectivity(catalog_kv, table, rows, inherited, ctx, expr)
    {
        return selectivity;
    }
    if let Some(selectivity) =
        functional_dependency_selectivity(catalog_kv, table, rows, inherited, ctx, expr)
    {
        return selectivity;
    }
    if let Some(selectivity) =
        quantified_all_equality_selectivity(catalog_kv, table, rows, ctx, expr)
    {
        return selectivity;
    }
    if let Some(selectivity) = quantified_inequality_selectivity(catalog_kv, table, rows, ctx, expr)
    {
        return selectivity;
    }
    if let Some(clause) = mcv_clause_for_expr(expr, table, ctx)
        && matches!(clause.key, GroupKey::Attribute(_))
    {
        return scalar_mcv_clause_selectivity(catalog_kv, table, rows, ctx, &clause);
    }
    match expr {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            restriction_selectivity(catalog_kv, table, rows, inherited, ctx, left)
                * restriction_selectivity(catalog_kv, table, rows, inherited, ctx, right)
        }
        Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } => {
            let left = restriction_selectivity(catalog_kv, table, rows, inherited, ctx, left);
            let right = restriction_selectivity(catalog_kv, table, rows, inherited, ctx, right);
            left + right - left * right
        }
        Expr::Binary { op, left, right }
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::Lt
                    | BinaryOp::Le
                    | BinaryOp::Gt
                    | BinaryOp::Ge
            ) =>
        {
            estimate_binary_restriction(catalog_kv, table, rows, ctx, *op, left, right)
        }
        Expr::IsNull { expr, negated } => column_statistics(catalog_kv, table, rows, expr, ctx)
            .map_or(crate::plan::selfuncs::DEFAULT_EQ_SEL, |stats| {
                crate::plan::selfuncs::nulltestsel(stats.as_stats(), !negated)
            }),
        Expr::Like { .. } => crate::plan::selfuncs::patternsel(),
        _ => crate::plan::selfuncs::DEFAULT_INEQ_SEL,
    }
}

/// Adjust an equality conjunction when an `ANALYZE`-derived functional
/// dependency says one constrained key determines another constrained key.
fn functional_dependency_selectivity(
    catalog_kv: &dyn crabka_pgkv::Kv,
    table: &crabka_pgcatalog::Table,
    rows: f64,
    inherited: bool,
    ctx: &crate::clock::EvalCtx,
    expr: &Expr,
) -> Option<f64> {
    let mut clauses = Vec::new();
    if !collect_mcv_clauses(expr, table, ctx, &mut clauses) || clauses.len() < 2 {
        return None;
    }
    let scalar = clauses
        .iter()
        .map(|clause| scalar_mcv_clause_selectivity(catalog_kv, table, rows, ctx, clause))
        .collect::<Vec<_>>();
    let mut selectivity = scalar.iter().product::<f64>();
    let mut adjusted = false;
    let mut implied = Vec::new();
    let mut applied = Vec::new();
    for object in crabka_pgcatalog::statistics::list(catalog_kv).ok()? {
        if object.table_id != table.id {
            continue;
        }
        let clause_positions = clauses
            .iter()
            .map(|clause| {
                statistics_key_positions(&object, &clause.key)
                    .and_then(|index| statistics_data_position(&object, index))
            })
            .collect::<Vec<_>>();
        let Some(mut dependencies) = statistics_data(&object, inherited)
            .and_then(|data| data.dependencies.as_deref())
            .and_then(decode_dependencies)
        else {
            continue;
        };
        dependencies.sort_by(|left, right| right.2.total_cmp(&left.2));
        for (determinants, dependent, degree) in dependencies {
            let Some(dependent_index) = clause_positions
                .iter()
                .position(|position| *position == Some(dependent))
            else {
                continue;
            };
            if implied.contains(&dependent_index) {
                continue;
            }
            let determinant = determinants
                .iter()
                .map(|position| {
                    clause_positions
                        .iter()
                        .position(|candidate| *candidate == Some(*position))
                })
                .collect::<Option<Vec<_>>>();
            let Some(determinant) = determinant else {
                continue;
            };
            if dependencies_are_reciprocal(&determinant, dependent_index, &applied) {
                continue;
            }
            let determinant_selectivity = determinant
                .iter()
                .map(|index| scalar[*index])
                .product::<f64>();
            let dependent_selectivity = scalar[dependent_index];
            if determinant_selectivity <= f64::EPSILON || dependent_selectivity <= f64::EPSILON {
                continue;
            }
            let dependent_given_determinant =
                (dependent_selectivity / determinant_selectivity).min(1.0);
            let corrected =
                (1.0 - degree) * dependent_selectivity + degree * dependent_given_determinant;
            selectivity = selectivity / dependent_selectivity * corrected;
            implied.push(dependent_index);
            applied.push((determinant, dependent_index));
            adjusted = true;
        }
    }
    adjusted.then_some(selectivity.clamp(0.0, 1.0))
}

fn dependencies_are_reciprocal(
    determinants: &[usize],
    dependent: usize,
    applied: &[(Vec<usize>, usize)],
) -> bool {
    applied.iter().any(|(prior_determinants, prior_dependent)| {
        prior_determinants.contains(&dependent) && determinants.contains(prior_dependent)
    })
}

fn decode_dependencies(data: &str) -> Option<Vec<(Vec<i16>, i16, f64)>> {
    data.strip_prefix('{')?
        .strip_suffix('}')?
        .split(", \"")
        .map(|entry| {
            let (dependency, degree) = entry.trim().trim_start_matches('"').split_once("\":")?;
            let (determinants, dependent) = dependency.split_once("=>")?;
            let determinants = determinants
                .split(',')
                .map(|value| value.trim().parse::<i16>().ok())
                .collect::<Option<Vec<_>>>()?;
            let degree = degree.trim().parse::<f64>().ok()?;
            (degree.is_finite() && (0.0..=1.0).contains(&degree)).then_some((
                determinants,
                dependent.trim().parse::<i16>().ok()?,
                degree,
            ))
        })
        .collect()
}

#[derive(Debug)]
struct McvClause {
    key: GroupKey,
    expr: Expr,
    predicate: McvPredicate,
}

#[derive(Debug)]
enum McvExpr {
    Clause(McvClause),
    And(Box<McvExpr>, Box<McvExpr>),
    Or(Box<McvExpr>, Box<McvExpr>),
}

#[derive(Debug)]
enum McvPredicate {
    Equal(Vec<McvValue>),
    Inequality { op: BinaryOp, value: McvValue },
    NotNull,
}

#[derive(Debug)]
struct McvValue {
    text: Option<String>,
    scalar_value: Option<crabka_pgtypes::Datum>,
}

/// Estimate a complete equality predicate from a matching extended MCV list.
/// The MCV portion gives its observed frequency; the remainder retains the
/// scalar estimate after removing the MCV population from both sides.
fn extended_mcv_selectivity(
    catalog_kv: &dyn crabka_pgkv::Kv,
    table: &crabka_pgcatalog::Table,
    rows: f64,
    inherited: bool,
    ctx: &crate::clock::EvalCtx,
    expr: &Expr,
) -> Option<f64> {
    let expression = mcv_expr_for_expr(expr, table, ctx)?;
    let mut clauses = Vec::new();
    mcv_expr_clauses(&expression, &mut clauses);
    if clauses.len() < 2 {
        return None;
    }
    let scalar = mcv_expr_scalar(catalog_kv, table, rows, ctx, &expression);
    for object in crabka_pgcatalog::statistics::list(catalog_kv).ok()? {
        if object.table_id != table.id {
            continue;
        }
        let Some(positions) = clauses
            .iter()
            .map(|clause| statistics_key_positions(&object, &clause.key))
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        let Some(items) = statistics_data(&object, inherited)
            .and_then(|data| data.mcv.as_deref())
            .and_then(crabka_pgcatalog::statistics::decode_mcv)
        else {
            continue;
        };
        let mut unique_positions = positions.clone();
        unique_positions.sort_unstable();
        unique_positions.dedup();
        if unique_positions.len() != positions.len() {
            continue;
        }
        let Some(frequencies) = items
            .iter()
            .map(|item| {
                Some((
                    item.frequency.parse::<f64>().ok()?,
                    item.base_frequency.parse::<f64>().ok()?,
                ))
            })
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        let (mut mcv_selectivity, mut mcv_base_selectivity, mut mcv_total, mut mcv_base_total) =
            (0.0, 0.0, 0.0, 0.0);
        for (item, (frequency, base_frequency)) in items.iter().zip(frequencies) {
            if !frequency.is_finite()
                || !base_frequency.is_finite()
                || !(0.0..=1.0).contains(&frequency)
                || !(0.0..=1.0).contains(&base_frequency)
            {
                mcv_total = f64::NAN;
                break;
            }
            mcv_total += frequency;
            mcv_base_total += base_frequency;
            if mcv_expr_matches(&expression, item, &clauses, &positions, table, ctx) {
                mcv_selectivity += frequency;
                mcv_base_selectivity += base_frequency;
            }
        }
        if !mcv_total.is_finite() || mcv_total > 1.0 + 1e-12 || mcv_base_total > 1.0 + 1e-12 {
            continue;
        }
        mcv_total = mcv_total.min(1.0);
        mcv_base_total = mcv_base_total.min(1.0);
        let remaining_base = (1.0 - mcv_base_total).max(0.0);
        let remainder = if remaining_base > f64::EPSILON {
            ((scalar - mcv_base_selectivity) / remaining_base).clamp(0.0, 1.0)
        } else {
            0.0
        };
        return Some((mcv_selectivity + (1.0 - mcv_total).max(0.0) * remainder).clamp(0.0, 1.0));
    }
    extended_mcv_conjunction_selectivity(catalog_kv, table, rows, inherited, ctx, expr)
}

/// Combine independent MCV objects for a conjunction when no single object
/// covers every clause.
fn extended_mcv_conjunction_selectivity(
    catalog_kv: &dyn crabka_pgkv::Kv,
    table: &crabka_pgcatalog::Table,
    rows: f64,
    inherited: bool,
    ctx: &crate::clock::EvalCtx,
    expr: &Expr,
) -> Option<f64> {
    let mut clauses = Vec::new();
    if !collect_mcv_clauses(expr, table, ctx, &mut clauses) || clauses.len() < 2 {
        return None;
    }
    let scalar = clauses
        .iter()
        .map(|clause| scalar_mcv_clause_selectivity(catalog_kv, table, rows, ctx, clause))
        .collect::<Vec<_>>();
    let mut selectivity = scalar.iter().product::<f64>();
    let mut estimated = vec![false; clauses.len()];
    let mut applied = false;
    let objects = crabka_pgcatalog::statistics::list(catalog_kv).ok()?;

    loop {
        let mut best = None;
        for object in &objects {
            if object.table_id != table.id
                || statistics_data(object, inherited)
                    .and_then(|data| data.mcv.as_deref())
                    .is_none()
            {
                continue;
            }
            let positions = clauses
                .iter()
                .enumerate()
                .filter_map(|(index, clause)| {
                    (!estimated[index]).then(|| {
                        statistics_key_positions(object, &clause.key)
                            .map(|position| (index, position))
                    })?
                })
                .collect::<Vec<_>>();
            if positions.len() >= 2
                && best
                    .as_ref()
                    .is_none_or(|(_, best_positions): &(_, Vec<(usize, usize)>)| {
                        positions.len() > best_positions.len()
                    })
            {
                best = Some((object, positions));
            }
        }
        let Some((object, positions)) = best else {
            break;
        };
        let Some(items) = statistics_data(object, inherited)
            .and_then(|data| data.mcv.as_deref())
            .and_then(crabka_pgcatalog::statistics::decode_mcv)
        else {
            break;
        };
        let Some(frequencies) = items
            .iter()
            .map(|item| {
                Some((
                    item.frequency.parse::<f64>().ok()?,
                    item.base_frequency.parse::<f64>().ok()?,
                ))
            })
            .collect::<Option<Vec<_>>>()
        else {
            break;
        };
        let (mut matched, mut matched_base, mut total, mut base_total) = (0.0, 0.0, 0.0, 0.0);
        for (item, (frequency, base_frequency)) in items.iter().zip(frequencies) {
            if !frequency.is_finite()
                || !base_frequency.is_finite()
                || !(0.0..=1.0).contains(&frequency)
                || !(0.0..=1.0).contains(&base_frequency)
            {
                total = f64::NAN;
                break;
            }
            total += frequency;
            base_total += base_frequency;
            if positions.iter().all(|(index, position)| {
                mcv_item_matches(
                    item.values.get(*position),
                    &clauses[*index].key,
                    &clauses[*index].predicate,
                    table,
                    ctx,
                )
            }) {
                matched += frequency;
                matched_base += base_frequency;
            }
        }
        if !total.is_finite() || total > 1.0 + 1e-12 || base_total > 1.0 + 1e-12 {
            break;
        }
        let base_selectivity = positions
            .iter()
            .map(|(index, _)| scalar[*index])
            .product::<f64>();
        if base_selectivity <= f64::EPSILON {
            break;
        }
        let remaining_base = (1.0 - base_total.min(1.0)).max(0.0);
        let remainder = if remaining_base > f64::EPSILON {
            ((base_selectivity - matched_base) / remaining_base).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let corrected = matched + (1.0 - total.min(1.0)).max(0.0) * remainder;
        selectivity = selectivity / base_selectivity * corrected;
        for (index, _) in positions {
            estimated[index] = true;
        }
        applied = true;
    }
    applied.then_some(selectivity.clamp(0.0, 1.0))
}

fn mcv_expr_for_expr(
    expr: &Expr,
    table: &crabka_pgcatalog::Table,
    ctx: &crate::clock::EvalCtx,
) -> Option<McvExpr> {
    match expr {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => Some(McvExpr::And(
            Box::new(mcv_expr_for_expr(left, table, ctx)?),
            Box::new(mcv_expr_for_expr(right, table, ctx)?),
        )),
        Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } => mcv_clause_for_expr(expr, table, ctx)
            .map(McvExpr::Clause)
            .or_else(|| {
                Some(McvExpr::Or(
                    Box::new(mcv_expr_for_expr(left, table, ctx)?),
                    Box::new(mcv_expr_for_expr(right, table, ctx)?),
                ))
            }),
        _ => mcv_clause_for_expr(expr, table, ctx).map(McvExpr::Clause),
    }
}

fn mcv_expr_clauses<'a>(expression: &'a McvExpr, clauses: &mut Vec<&'a McvClause>) {
    match expression {
        McvExpr::Clause(clause) => clauses.push(clause),
        McvExpr::And(left, right) | McvExpr::Or(left, right) => {
            mcv_expr_clauses(left, clauses);
            mcv_expr_clauses(right, clauses);
        }
    }
}

fn mcv_expr_scalar(
    catalog_kv: &dyn crabka_pgkv::Kv,
    table: &crabka_pgcatalog::Table,
    rows: f64,
    ctx: &crate::clock::EvalCtx,
    expression: &McvExpr,
) -> f64 {
    match expression {
        McvExpr::Clause(clause) => {
            scalar_mcv_clause_selectivity(catalog_kv, table, rows, ctx, clause)
        }
        McvExpr::And(left, right) => {
            mcv_expr_scalar(catalog_kv, table, rows, ctx, left)
                * mcv_expr_scalar(catalog_kv, table, rows, ctx, right)
        }
        McvExpr::Or(left, right) => {
            let left = mcv_expr_scalar(catalog_kv, table, rows, ctx, left);
            let right = mcv_expr_scalar(catalog_kv, table, rows, ctx, right);
            (left + right - left * right).clamp(0.0, 1.0)
        }
    }
}

fn mcv_expr_matches(
    expression: &McvExpr,
    item: &crabka_pgcatalog::statistics::McvItem,
    clauses: &[&McvClause],
    positions: &[usize],
    table: &crabka_pgcatalog::Table,
    ctx: &crate::clock::EvalCtx,
) -> bool {
    match expression {
        McvExpr::Clause(clause) => clauses
            .iter()
            .zip(positions)
            .find_map(|(candidate, position)| (candidate.key == clause.key).then_some(*position))
            .is_some_and(|position| {
                mcv_item_matches(
                    item.values.get(position),
                    &clause.key,
                    &clause.predicate,
                    table,
                    ctx,
                )
            }),
        McvExpr::And(left, right) => {
            mcv_expr_matches(left, item, clauses, positions, table, ctx)
                && mcv_expr_matches(right, item, clauses, positions, table, ctx)
        }
        McvExpr::Or(left, right) => {
            mcv_expr_matches(left, item, clauses, positions, table, ctx)
                || mcv_expr_matches(right, item, clauses, positions, table, ctx)
        }
    }
}

fn scalar_mcv_clause_selectivity(
    catalog_kv: &dyn crabka_pgkv::Kv,
    table: &crabka_pgcatalog::Table,
    rows: f64,
    ctx: &crate::clock::EvalCtx,
    clause: &McvClause,
) -> f64 {
    let default = default_mcv_selectivity(&clause.predicate);
    column_statistics(catalog_kv, table, rows, &clause.expr, ctx).map_or(default, |stats| {
        match &clause.predicate {
            McvPredicate::Equal(values) => values
                .iter()
                .map(|value| {
                    mcv_scalar_value(value, &clause.expr, table, ctx)
                        .as_ref()
                        .map_or_else(
                            || crate::plan::selfuncs::nulltestsel(stats.as_stats(), true),
                            |value| crate::plan::selfuncs::eqsel(stats.as_stats(), Some(value)),
                        )
                })
                .sum::<f64>()
                .min(1.0),
            McvPredicate::Inequality { op, value } => {
                let inequality = match op {
                    BinaryOp::Lt => crate::plan::selfuncs::Inequality::Less,
                    BinaryOp::Le => crate::plan::selfuncs::Inequality::LessEqual,
                    BinaryOp::Gt => crate::plan::selfuncs::Inequality::Greater,
                    BinaryOp::Ge => crate::plan::selfuncs::Inequality::GreaterEqual,
                    _ => return default,
                };
                mcv_scalar_value(value, &clause.expr, table, ctx)
                    .as_ref()
                    .and_then(|value| stats.scalar_inequality(value, inequality))
                    .unwrap_or(default)
            }
            McvPredicate::NotNull => crate::plan::selfuncs::nulltestsel(stats.as_stats(), false),
        }
    })
}

fn mcv_scalar_value(
    value: &McvValue,
    expr: &Expr,
    table: &crabka_pgcatalog::Table,
    ctx: &crate::clock::EvalCtx,
) -> Option<crabka_pgtypes::Datum> {
    value.scalar_value.clone().or_else(|| {
        crate::eval::infer_type(expr, &crate::scope::Scope::single(table, &table.name.name))
            .ok()
            .and_then(|ty| {
                value.text.as_ref().and_then(|text| {
                    crate::eval::cast_value_in(
                        &crabka_pgtypes::Datum::Text(text.clone()),
                        ty,
                        ctx.output_style(),
                    )
                    .ok()
                })
            })
    })
}

fn default_mcv_selectivity(predicate: &McvPredicate) -> f64 {
    match predicate {
        McvPredicate::Equal(values) => {
            (values.len() as f64 * crate::plan::selfuncs::DEFAULT_EQ_SEL).min(1.0)
        }
        McvPredicate::Inequality { .. } => crate::plan::selfuncs::DEFAULT_INEQ_SEL,
        McvPredicate::NotNull => 1.0 - crate::plan::selfuncs::DEFAULT_EQ_SEL,
    }
}

fn collect_mcv_clauses(
    expr: &Expr,
    table: &crabka_pgcatalog::Table,
    ctx: &crate::clock::EvalCtx,
    clauses: &mut Vec<McvClause>,
) -> bool {
    match expr {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            collect_mcv_clauses(left, table, ctx, clauses)
                && collect_mcv_clauses(right, table, ctx, clauses)
        }
        _ => mcv_clause_for_expr(expr, table, ctx).is_some_and(|clause| {
            if !matches!(
                &clause.predicate,
                McvPredicate::Equal(_) | McvPredicate::NotNull
            ) {
                return false;
            }
            clauses.push(clause);
            true
        }),
    }
}

fn mcv_clause_for_expr(
    expr: &Expr,
    table: &crabka_pgcatalog::Table,
    ctx: &crate::clock::EvalCtx,
) -> Option<McvClause> {
    match expr {
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } => mcv_clause(left, right, table, ctx).or_else(|| mcv_clause(right, left, table, ctx)),
        Expr::Binary { op, left, right }
            if matches!(
                op,
                BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
            ) =>
        {
            mcv_inequality_clause(left, right, *op, table, ctx)
                .or_else(|| mcv_inequality_clause(right, left, reverse_inequality(*op), table, ctx))
        }
        Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } => merge_mcv_or_clauses(
            [left.as_ref(), right.as_ref()]
                .into_iter()
                .map(|expr| mcv_clause_for_expr(expr, table, ctx))
                .collect::<Option<Vec<_>>>()?,
        ),
        Expr::InList {
            expr,
            list,
            negated: false,
        } => mcv_list_clause(expr, list, table, ctx),
        Expr::QuantifiedArray {
            expr,
            op: BinaryOp::Eq,
            all: false,
            array,
        } => mcv_array_clause(expr, array, table, ctx),
        Expr::QuantifiedArray {
            expr,
            op,
            all,
            array,
        } if matches!(
            op,
            BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
        ) =>
        {
            mcv_quantified_inequality_clause(expr, array, *op, *all, table, ctx)
        }
        Expr::Column { .. } => bool_mcv_clause(expr, false, table, ctx),
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => bool_mcv_clause(expr, true, table, ctx),
        Expr::IsNull { expr, negated } => null_mcv_clause(expr, *negated, table),
        _ => None,
    }
}

fn merge_mcv_or_clauses(clauses: Vec<McvClause>) -> Option<McvClause> {
    let mut clauses = clauses.into_iter();
    let mut combined = clauses.next()?;
    for clause in clauses {
        if clause.key != combined.key {
            return None;
        }
        let (McvPredicate::Equal(combined_values), McvPredicate::Equal(values)) =
            (&mut combined.predicate, clause.predicate)
        else {
            return None;
        };
        for value in values {
            if !combined_values
                .iter()
                .any(|candidate| candidate.text == value.text)
            {
                combined_values.push(value);
            }
        }
    }
    Some(combined)
}

fn mcv_item_matches(
    value: Option<&Option<String>>,
    key: &GroupKey,
    predicate: &McvPredicate,
    table: &crabka_pgcatalog::Table,
    ctx: &crate::clock::EvalCtx,
) -> bool {
    match (value, predicate) {
        (Some(value), McvPredicate::Equal(wanted)) => wanted
            .iter()
            .any(|candidate| candidate.text.as_ref() == value.as_ref()),
        (Some(Some(text)), McvPredicate::Inequality { op, value }) => {
            let GroupKey::Attribute(attnum) = key else {
                return false;
            };
            let Some(index) = usize::try_from(*attnum - 1).ok() else {
                return false;
            };
            let Some(definition) = table.columns.get(index) else {
                return false;
            };
            let Some(wanted) = value.scalar_value.as_ref() else {
                return false;
            };
            let Ok(actual) = crate::eval::cast_value_in(
                &crabka_pgtypes::Datum::Text(text.clone()),
                definition.ty,
                ctx.output_style(),
            ) else {
                return false;
            };
            let Some(ordering) = crabka_pgtypes::ops::compare(&actual, wanted).ok().flatten()
            else {
                return false;
            };
            matches!(
                (op, ordering),
                (BinaryOp::Lt, std::cmp::Ordering::Less)
                    | (
                        BinaryOp::Le,
                        std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                    )
                    | (BinaryOp::Gt, std::cmp::Ordering::Greater)
                    | (
                        BinaryOp::Ge,
                        std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                    )
            )
        }
        (Some(Some(_)), McvPredicate::NotNull) => true,
        _ => false,
    }
}

fn reverse_inequality(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Le => BinaryOp::Ge,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Ge => BinaryOp::Le,
        _ => unreachable!("only comparison operators are reversed"),
    }
}

fn mcv_inequality_clause(
    key_expr: &Expr,
    literal: &Expr,
    op: BinaryOp,
    table: &crabka_pgcatalog::Table,
    ctx: &crate::clock::EvalCtx,
) -> Option<McvClause> {
    let key = mcv_key(key_expr, table, ctx)?;
    let literal = crate::eval::eval(literal, &crate::scope::Scope::empty(), &[], ctx).ok()?;
    let value = mcv_value(key.clone(), literal, table, ctx)?;
    Some(McvClause {
        key,
        expr: key_expr.clone(),
        predicate: McvPredicate::Inequality { op, value },
    })
}

fn null_mcv_clause(
    expr: &Expr,
    negated: bool,
    table: &crabka_pgcatalog::Table,
) -> Option<McvClause> {
    Some(McvClause {
        key: GroupKey::Attribute(column_attnum(expr, table)?),
        expr: expr.clone(),
        predicate: if negated {
            McvPredicate::NotNull
        } else {
            McvPredicate::Equal(vec![McvValue {
                text: None,
                scalar_value: None,
            }])
        },
    })
}

fn bool_mcv_clause(
    expr: &Expr,
    negated: bool,
    table: &crabka_pgcatalog::Table,
    ctx: &crate::clock::EvalCtx,
) -> Option<McvClause> {
    let attnum = column_attnum(expr, table)?;
    (table.columns.get(usize::try_from(attnum - 1).ok()?)?.ty == crabka_pgtypes::ColumnType::Bool)
        .then_some(())?;
    let value = crabka_pgtypes::Datum::Bool(!negated);
    let text = String::from_utf8(crabka_pgtypes::encoding::encode_text_in(
        &value,
        ctx.output_style(),
    ))
    .ok()?;
    Some(McvClause {
        key: GroupKey::Attribute(attnum),
        expr: expr.clone(),
        predicate: McvPredicate::Equal(vec![McvValue {
            text: Some(text),
            scalar_value: Some(value),
        }]),
    })
}

fn mcv_clause(
    key_expr: &Expr,
    literal: &Expr,
    table: &crabka_pgcatalog::Table,
    ctx: &crate::clock::EvalCtx,
) -> Option<McvClause> {
    let key = mcv_key(key_expr, table, ctx)?;
    let literal = crate::eval::eval(literal, &crate::scope::Scope::empty(), &[], ctx).ok()?;
    let value = mcv_value(key.clone(), literal, table, ctx)?;
    Some(McvClause {
        key,
        expr: key_expr.clone(),
        predicate: McvPredicate::Equal(vec![value]),
    })
}

fn mcv_list_clause(
    key_expr: &Expr,
    literals: &[Expr],
    table: &crabka_pgcatalog::Table,
    ctx: &crate::clock::EvalCtx,
) -> Option<McvClause> {
    let key = mcv_key(key_expr, table, ctx)?;
    let values = literals
        .iter()
        .filter_map(|literal| {
            crate::eval::eval(literal, &crate::scope::Scope::empty(), &[], ctx)
                .ok()
                .and_then(|value| mcv_value(key.clone(), value, table, ctx))
        })
        .collect::<Vec<_>>();
    (!values.is_empty()).then_some(McvClause {
        key,
        expr: key_expr.clone(),
        predicate: McvPredicate::Equal(values),
    })
}

fn mcv_array_clause(
    key_expr: &Expr,
    array: &Expr,
    table: &crabka_pgcatalog::Table,
    ctx: &crate::clock::EvalCtx,
) -> Option<McvClause> {
    let key = mcv_key(key_expr, table, ctx)?;
    let crabka_pgtypes::Datum::Array(array) =
        crate::eval::eval(array, &crate::scope::Scope::empty(), &[], ctx).ok()?
    else {
        return None;
    };
    let values = array
        .elems
        .into_iter()
        .filter_map(|value| mcv_value(key.clone(), value, table, ctx))
        .collect::<Vec<_>>();
    (!values.is_empty()).then_some(McvClause {
        key,
        expr: key_expr.clone(),
        predicate: McvPredicate::Equal(values),
    })
}

fn mcv_quantified_inequality_clause(
    key_expr: &Expr,
    array: &Expr,
    op: BinaryOp,
    all: bool,
    table: &crabka_pgcatalog::Table,
    ctx: &crate::clock::EvalCtx,
) -> Option<McvClause> {
    let key = mcv_key(key_expr, table, ctx)?;
    if !matches!(key, GroupKey::Attribute(_)) {
        return None;
    }
    let crabka_pgtypes::Datum::Array(array) =
        crate::eval::eval(array, &crate::scope::Scope::empty(), &[], ctx).ok()?
    else {
        return None;
    };
    if all && array.elems.iter().any(crabka_pgtypes::Datum::is_null) {
        return None;
    }
    let mut values = array
        .elems
        .into_iter()
        .filter(|value| !value.is_null())
        .filter_map(|value| mcv_value(key.clone(), value, table, ctx));
    let mut boundary = values.next()?;
    let use_maximum = matches!(
        (op, all),
        (BinaryOp::Lt | BinaryOp::Le, false) | (BinaryOp::Gt | BinaryOp::Ge, true)
    );
    for value in values {
        let ordering = crabka_pgtypes::ops::compare(
            boundary.scalar_value.as_ref()?,
            value.scalar_value.as_ref()?,
        )
        .ok()??;
        if (use_maximum && ordering.is_lt()) || (!use_maximum && ordering.is_gt()) {
            boundary = value;
        }
    }
    Some(McvClause {
        key,
        expr: key_expr.clone(),
        predicate: McvPredicate::Inequality {
            op,
            value: boundary,
        },
    })
}

fn quantified_all_equality_selectivity(
    catalog_kv: &dyn crabka_pgkv::Kv,
    table: &crabka_pgcatalog::Table,
    rows: f64,
    ctx: &crate::clock::EvalCtx,
    expr: &Expr,
) -> Option<f64> {
    let Expr::QuantifiedArray {
        expr,
        op: BinaryOp::Eq,
        all: true,
        array,
    } = expr
    else {
        return None;
    };
    let key = mcv_key(expr, table, ctx)?;
    let crabka_pgtypes::Datum::Array(array) =
        crate::eval::eval(array, &crate::scope::Scope::empty(), &[], ctx).ok()?
    else {
        return None;
    };
    let Some(first) = array.elems.first() else {
        return Some(1.0);
    };
    let Some(first) = mcv_value(key.clone(), first.clone(), table, ctx) else {
        return Some(0.0);
    };
    for value in &array.elems[1..] {
        let Some(value) = mcv_value(key.clone(), value.clone(), table, ctx) else {
            return Some(0.0);
        };
        if value.text != first.text {
            return Some(0.0);
        }
    }
    let clause = McvClause {
        key,
        expr: *expr.clone(),
        predicate: McvPredicate::Equal(vec![first]),
    };
    Some(scalar_mcv_clause_selectivity(
        catalog_kv, table, rows, ctx, &clause,
    ))
}

fn quantified_inequality_selectivity(
    catalog_kv: &dyn crabka_pgkv::Kv,
    table: &crabka_pgcatalog::Table,
    rows: f64,
    ctx: &crate::clock::EvalCtx,
    expr: &Expr,
) -> Option<f64> {
    let Expr::QuantifiedArray {
        expr,
        op,
        all,
        array,
    } = expr
    else {
        return None;
    };
    if !matches!(
        op,
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
    ) {
        return None;
    }
    let ty = crate::eval::infer_type(expr, &crate::scope::Scope::single(table, &table.name.name))
        .ok()?;
    let crabka_pgtypes::Datum::Array(array) =
        crate::eval::eval(array, &crate::scope::Scope::empty(), &[], ctx).ok()?
    else {
        return None;
    };
    if *all && array.elems.iter().any(crabka_pgtypes::Datum::is_null) {
        return Some(0.0);
    }
    let values = array
        .elems
        .iter()
        .filter(|value| !value.is_null())
        .map(|value| crate::eval::cast_value_in(value, ty, ctx.output_style()).ok())
        .collect::<Option<Vec<_>>>()?;
    if values.is_empty() {
        return Some(if *all { 1.0 } else { 0.0 });
    }
    let statistics = column_statistics(catalog_kv, table, rows, expr, ctx);
    Some(
        values
            .into_iter()
            .fold(if *all { 1.0 } else { 0.0 }, |selectivity, value| {
                let individual = statistics.as_ref().map_or(
                    crate::plan::selfuncs::DEFAULT_INEQ_SEL,
                    |statistics| {
                        decoded_restriction_selectivity(statistics, *op, Some(value), false)
                    },
                );
                if *all {
                    selectivity * individual
                } else {
                    selectivity + individual - selectivity * individual
                }
            }),
    )
}

fn mcv_key(
    key_expr: &Expr,
    table: &crabka_pgcatalog::Table,
    ctx: &crate::clock::EvalCtx,
) -> Option<GroupKey> {
    Some(match key_expr {
        Expr::Column { .. } => GroupKey::Attribute(column_attnum(key_expr, table)?),
        _ if crate::eval::eval(key_expr, &crate::scope::Scope::empty(), &[], ctx).is_ok() => {
            return None;
        }
        _ => GroupKey::Expression(crate::viewdef::expression_text(
            key_expr,
            crabka_pgtypes::encoding::OutputStyle::with_zone(&jiff::tz::TimeZone::UTC),
        )),
    })
}

fn mcv_value(
    key: GroupKey,
    value: crabka_pgtypes::Datum,
    table: &crabka_pgcatalog::Table,
    ctx: &crate::clock::EvalCtx,
) -> Option<McvValue> {
    (!matches!(value, crabka_pgtypes::Datum::Null)).then_some(())?;
    let (value, scalar_value) = match key {
        GroupKey::Attribute(attnum) => {
            let definition = table.columns.get(usize::try_from(attnum - 1).ok()?)?;
            let value =
                crate::eval::cast_value_in(&value, definition.ty, ctx.output_style()).ok()?;
            (value.clone(), Some(value))
        }
        GroupKey::Expression(_) => (value, None),
    };
    let text = String::from_utf8(crabka_pgtypes::encoding::encode_text_in(
        &value,
        ctx.output_style(),
    ))
    .ok()?;
    Some(McvValue {
        text: Some(text),
        scalar_value,
    })
}

fn estimate_binary_restriction(
    catalog_kv: &dyn crabka_pgkv::Kv,
    table: &crabka_pgcatalog::Table,
    rows: f64,
    ctx: &crate::clock::EvalCtx,
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
) -> f64 {
    let (key, constant, reversed) = match (
        crate::eval::eval(left, &crate::scope::Scope::empty(), &[], ctx).is_ok(),
        crate::eval::eval(right, &crate::scope::Scope::empty(), &[], ctx).is_ok(),
    ) {
        (false, true) => (left, right, false),
        (true, false) => (right, left, true),
        _ => return crate::plan::selfuncs::DEFAULT_INEQ_SEL,
    };
    let Some(ty) =
        crate::eval::infer_type(key, &crate::scope::Scope::single(table, &table.name.name)).ok()
    else {
        return crate::plan::selfuncs::DEFAULT_INEQ_SEL;
    };
    let Some(stats) = column_statistics(catalog_kv, table, rows, key, ctx) else {
        return if matches!(op, BinaryOp::Eq | BinaryOp::Ne) {
            crate::plan::selfuncs::DEFAULT_EQ_SEL
        } else {
            crate::plan::selfuncs::DEFAULT_INEQ_SEL
        };
    };
    decoded_restriction_selectivity(&stats, op, literal_for_type(constant, ty, ctx), reversed)
}

fn decoded_restriction_selectivity(
    stats: &crate::plan::selfuncs::DecodedColumnStats,
    op: BinaryOp,
    constant: Option<crabka_pgtypes::Datum>,
    reversed: bool,
) -> f64 {
    match op {
        BinaryOp::Eq => crate::plan::selfuncs::eqsel(stats.as_stats(), constant.as_ref()),
        BinaryOp::Ne => crate::plan::selfuncs::neqsel(stats.as_stats(), constant.as_ref()),
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            let inequality = match (op, reversed) {
                (BinaryOp::Lt, false) | (BinaryOp::Gt, true) => {
                    crate::plan::selfuncs::Inequality::Less
                }
                (BinaryOp::Le, false) | (BinaryOp::Ge, true) => {
                    crate::plan::selfuncs::Inequality::LessEqual
                }
                (BinaryOp::Gt, false) | (BinaryOp::Lt, true) => {
                    crate::plan::selfuncs::Inequality::Greater
                }
                (BinaryOp::Ge, false) | (BinaryOp::Le, true) => {
                    crate::plan::selfuncs::Inequality::GreaterEqual
                }
                _ => unreachable!("comparison branch only receives inequalities"),
            };
            constant
                .as_ref()
                .and_then(|constant| stats.scalar_inequality(constant, inequality))
                .unwrap_or(crate::plan::selfuncs::DEFAULT_INEQ_SEL)
        }
        _ => unreachable!("binary restriction only receives comparison operators"),
    }
}

fn column_statistics(
    catalog_kv: &dyn crabka_pgkv::Kv,
    table: &crabka_pgcatalog::Table,
    rows: f64,
    expr: &Expr,
    ctx: &crate::clock::EvalCtx,
) -> Option<crate::plan::selfuncs::DecodedColumnStats> {
    if let Some(column) = column_name(expr) {
        let (position, definition) = table
            .columns
            .iter()
            .enumerate()
            .find(|(_, definition)| definition.name == column)?;
        let stats = crate::attrstats::get(
            catalog_kv,
            &crate::attrstats::AttributeStatsKey {
                relation: table.name.clone(),
                attnum: i16::try_from(position + 1).ok()?,
                inherited: false,
            },
        )
        .ok()??;
        return crate::plan::selfuncs::decode_catalog_stats(&stats, definition.ty, rows, ctx);
    }
    expression_statistics(catalog_kv, table, rows, expr, ctx)
}

fn expression_statistics(
    catalog_kv: &dyn crabka_pgkv::Kv,
    table: &crabka_pgcatalog::Table,
    rows: f64,
    expr: &Expr,
    ctx: &crate::clock::EvalCtx,
) -> Option<crate::plan::selfuncs::DecodedColumnStats> {
    let GroupKey::Expression(key) = mcv_key(expr, table, ctx)? else {
        return None;
    };
    let ty = crate::eval::infer_type(expr, &crate::scope::Scope::single(table, &table.name.name))
        .ok()?;
    for object in crabka_pgcatalog::statistics::list(catalog_kv).ok()? {
        if object.table_id != table.id {
            continue;
        }
        let Some(index) = object.expressions.iter().position(|value| value == &key) else {
            continue;
        };
        let Some(data) = object.data else {
            continue;
        };
        let Some(expression) = data.expression_stats.get(index) else {
            continue;
        };
        let stats = crate::attrstats::AttributeStats {
            null_frac: expression
                .null_frac
                .as_deref()
                .and_then(|value| value.parse().ok()),
            avg_width: expression.avg_width,
            n_distinct: expression
                .n_distinct
                .as_deref()
                .and_then(|value| value.parse().ok()),
            most_common_vals: expression.most_common_vals.clone(),
            most_common_freqs: expression.most_common_freqs.clone(),
            ..crate::attrstats::AttributeStats::default()
        };
        return crate::plan::selfuncs::decode_catalog_stats(&stats, ty, rows, ctx);
    }
    None
}

fn column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column { name, .. } => Some(name),
        _ => None,
    }
}

fn literal_for_type(
    expr: &Expr,
    ty: crabka_pgtypes::ColumnType,
    ctx: &crate::clock::EvalCtx,
) -> Option<crabka_pgtypes::Datum> {
    let value = crate::eval::eval(expr, &crate::scope::Scope::empty(), &[], ctx).ok()?;
    (!matches!(value, crabka_pgtypes::Datum::Null))
        .then(|| crate::eval::cast_value_in(&value, ty, ctx.output_style()).ok())?
}

/// Build a renderable tree from the plan the executor actually ran.
pub(crate) fn plan_runtime_state(state: &crate::plan::query::PlanState) -> PlanNode {
    use crate::plan::query::PlanNode as ExecutableNode;

    let mut node = PlanNode::new(match &state.plan.node {
        ExecutableNode::Result => "Result",
        ExecutableNode::SeqScan { .. } => "Seq Scan",
        ExecutableNode::Filter { .. } => "Filter",
        ExecutableNode::Aggregate { .. } => "Aggregate",
        ExecutableNode::Sort { .. } => "Sort",
        ExecutableNode::Unique { .. } => "Unique",
        ExecutableNode::Limit { .. } => "Limit",
        ExecutableNode::ProjectSet { .. } => "ProjectSet",
        ExecutableNode::WindowAgg { .. } => "WindowAgg",
        ExecutableNode::ValuesScan => "Values Scan",
        ExecutableNode::FunctionScan => "Function Scan",
        ExecutableNode::SubqueryScan { .. } => "Subquery Scan",
        ExecutableNode::CteScan => "CTE Scan",
        ExecutableNode::NamedTuplestoreScan => "Named Tuplestore Scan",
        ExecutableNode::TableFunctionScan => "Table Function Scan",
        ExecutableNode::NestedLoop { .. } => "Nested Loop",
    });
    node.actual = Some(PlanActual {
        rows: state.ntuples,
        loops: state.nloops,
        rows_removed: state.rows_removed,
    });
    node.children = state.children.iter().map(plan_runtime_state).collect();
    node
}

/// Apply executor counters to the syntactic tree that supplies PostgreSQL's
/// relation names and detail text. The executor has an explicit `Filter` node;
/// PostgreSQL prints that state on its scan or join child instead.
pub(crate) fn apply_runtime_state(rendered: &mut PlanNode, runtime: &PlanNode) {
    rendered.actual = runtime.actual;
    for (child, runtime_child) in rendered.children.iter_mut().zip(&runtime.children) {
        apply_runtime_state(child, runtime_child);
    }
}

/// Build the plan tree the interpreter will execute for `statement`.
pub(crate) fn plan_statement(statement: &Statement) -> PlanNode {
    let (mut node, with) = match statement {
        Statement::Query(query) => (plan_query(query), None),
        Statement::Insert {
            table,
            source,
            with,
            ..
        } => {
            let child = match source {
                crabka_pgparser::ast::InsertSource::Values(rows) if rows.len() > 1 => {
                    PlanNode::new("Values Scan").with_relation("*VALUES*")
                }
                crabka_pgparser::ast::InsertSource::Query(query) => plan_query(query),
                _ => PlanNode::new("Result"),
            };
            let mut node = PlanNode::new("Insert");
            node.relation = Some(table.name.clone());
            (node.with_child(child), with.as_ref())
        }
        Statement::Update {
            table,
            filter,
            with,
            ..
        } => {
            let mut node = PlanNode::new("Update");
            node.relation = Some(table.name.clone());
            (
                node.with_child(scan_node(&table.name, None, filter.as_ref())),
                with.as_ref(),
            )
        }
        Statement::Delete {
            table,
            filter,
            with,
            ..
        } => {
            let mut node = PlanNode::new("Delete");
            node.relation = Some(table.name.clone());
            (
                node.with_child(scan_node(&table.name, None, filter.as_ref())),
                with.as_ref(),
            )
        }
        other => (PlanNode::new(utility_node_type(other)), None),
    };
    attach_ctes(&mut node, with);
    mark_cte_scans(&mut node, with);
    node
}

/// Plan a statement after the unconditional `DO INSTEAD` rewrite that its
/// executor path applies.  The action keeps its target and conflict clause;
/// the client statement supplies the source and outer `WITH` list.
pub(crate) fn plan_statement_with_rewrite(
    catalog_kv: &dyn crabka_pgkv::Kv,
    resolution: &crate::relname::ResolutionScope,
    statement: &Statement,
) -> Result<PlanNode, crate::error::ExecError> {
    let Some((event, reference)) = (match statement {
        Statement::Insert { table, .. } => Some((crabka_pgcatalog::rule::RuleEvent::Insert, table)),
        Statement::Update { table, .. } => Some((crabka_pgcatalog::rule::RuleEvent::Update, table)),
        Statement::Delete { table, .. } => Some((crabka_pgcatalog::rule::RuleEvent::Delete, table)),
        _ => None,
    }) else {
        return Ok(plan_statement(statement));
    };
    let name = crate::relname::resolve_relation(
        catalog_kv,
        resolution,
        reference,
        crate::relname::SchemaDisposition::Reference,
    )?;
    let Ok(table) = crabka_pgcatalog::get_table(catalog_kv, &name) else {
        return Ok(plan_statement(statement));
    };
    let mut actions = crabka_pgcatalog::rule::rules_for_table(catalog_kv, table.id)?
        .into_iter()
        .filter(|rule| {
            rule.instead
                && rule.event == event
                && rule.condition.is_none()
                && crate::exec::rule_is_enabled(rule.enabled)
                && !rule.action.eq_ignore_ascii_case("nothing")
        });
    let Some(rule) = actions.next() else {
        return Ok(plan_statement(statement));
    };
    if actions.next().is_some() {
        return Ok(plan_statement(statement));
    }
    let source = rule
        .action
        .strip_prefix('(')
        .and_then(|action| action.strip_suffix(')'))
        .unwrap_or(&rule.action);
    let mut actions = crabka_pgparser::parse(source)?;
    let [action] = actions.as_mut_slice() else {
        return Ok(plan_statement(statement));
    };
    if let (
        Statement::Insert {
            source: action_source,
            with: action_with,
            ..
        },
        Statement::Insert { source, with, .. },
    ) = (&mut *action, statement)
    {
        *action_source = source.clone();
        *action_with = with.clone();
    }
    let mut plan = plan_statement(&action);
    if let Statement::Insert {
        table,
        on_conflict: Some(conflict),
        ..
    } = &action
    {
        let name = crate::relname::resolve_relation(
            catalog_kv,
            resolution,
            table,
            crate::relname::SchemaDisposition::Reference,
        )?;
        let table = crabka_pgcatalog::get_table(catalog_kv, &name)?;
        let indexes = crate::exec::writable_local_indexes(catalog_kv, &table)?;
        let arbiters = crate::exec::resolve_arbiter_indexes(&table, &indexes, &conflict.target)?;
        plan.details.push((
            "Conflict Resolution".into(),
            match conflict.action {
                crabka_pgparser::ast::OnConflictAction::DoNothing => "NOTHING",
                crabka_pgparser::ast::OnConflictAction::DoUpdate { .. } => "UPDATE",
            }
            .into(),
        ));
        if !arbiters.is_empty() {
            plan.details.push((
                "Conflict Arbiter Indexes".into(),
                arbiters
                    .iter()
                    .map(|index| index.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }
        if let crabka_pgparser::ast::OnConflictAction::DoUpdate {
            filter: Some(filter),
            ..
        } = &conflict.action
        {
            plan.details.push((
                "Conflict Filter".into(),
                deparse(&type_conflict_literals(filter, &table)),
            ));
        }
    }
    Ok(plan)
}

fn attach_ctes(node: &mut PlanNode, with: Option<&WithClause>) {
    let Some(with) = with else {
        return;
    };
    node.init_plans = with
        .ctes
        .iter()
        .map(|cte| CtePlan {
            name: cte.name.clone(),
            plan: Box::new(match &cte.body {
                crabka_pgparser::ast::CteBody::Query(query) => plan_query(query),
                crabka_pgparser::ast::CteBody::Dml(statement) => plan_statement(statement),
            }),
        })
        .collect();
}

fn mark_cte_scans(node: &mut PlanNode, with: Option<&WithClause>) {
    let Some(with) = with else {
        return;
    };
    for child in &mut node.children {
        if child.node_type == "Seq Scan"
            && child
                .relation
                .as_ref()
                .is_some_and(|relation| with.ctes.iter().any(|cte| &cte.name == relation))
        {
            child.node_type = "CTE Scan".into();
        }
        mark_cte_scans(child, Some(with));
    }
}

fn type_conflict_literals(expr: &Expr, table: &crabka_pgcatalog::Table) -> Expr {
    let scope = crate::scope::Scope::single(table, "excluded");
    crate::viewwrite::map_expr(expr, false, &mut |node, _| match node {
        Expr::Binary { op, left, right } => {
            let cast = |literal: &Expr, other: &Expr| {
                matches!(literal, Expr::StringLiteral(_))
                    && matches!(
                        crate::eval::infer_type(other, &scope),
                        Ok(crabka_pgtypes::ColumnType::Char(_))
                    )
            };
            if cast(left, right) {
                Some(Expr::Binary {
                    op: *op,
                    left: Box::new(Expr::Cast {
                        expr: left.clone(),
                        ty: crabka_pgtypes::ColumnType::Char(None),
                    }),
                    right: right.clone(),
                })
            } else if cast(right, left) {
                Some(Expr::Binary {
                    op: *op,
                    left: left.clone(),
                    right: Box::new(Expr::Cast {
                        expr: right.clone(),
                        ty: crabka_pgtypes::ColumnType::Char(None),
                    }),
                })
            } else {
                None
            }
        }
        _ => None,
    })
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
    attach_ctes(&mut node, query.with.as_ref());
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
    node.output = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::Wildcard => "*".to_string(),
            SelectItem::QualifiedWildcard(table) => format!("{table}.*"),
            SelectItem::Expr { expr, alias } => alias.clone().unwrap_or_else(|| deparse_bare(expr)),
        })
        .collect();
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

/// What a subscript is taken of, parenthesized the way `get_rule_expr` does.
///
/// The rule is [`crate::viewdef::subscript_base_is_bare`], which the view
/// deparser answers against the same oracle: a bare column or a field
/// selection stands alone, everything else gains a pair. So a plan line reads
/// `(string_to_array(t, ','::text))[1]`, not `string_to_array(t, ','::text)[1]`.
fn subscript_base(base: &Expr, qualify: bool) -> String {
    let text = deparse_bare_with(base, qualify);
    if crate::viewdef::subscript_base_is_bare(base) {
        text
    } else {
        format!("({text})")
    }
}

/// What a field selection is taken of, parenthesized the way `get_rule_expr`
/// does — [`crate::viewdef::field_base_is_bare`], the mirror image of the
/// subscript rule. A column base keeps its pair, so a plan line still reads
/// `(c).f`, but `(c).h.g` and `carr[1].f` shed the one this used to add
/// unconditionally.
fn field_base(base: &Expr, qualify: bool) -> String {
    let text = deparse_bare_with(base, qualify);
    if crate::viewdef::field_base_is_bare(base) {
        text
    } else {
        format!("({text})")
    }
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
        FuncArgs::Named { positional, named } => positional
            .iter()
            .map(|arg| deparse_bare_with(arg, qualify))
            .chain(
                named
                    .iter()
                    .map(|(label, arg)| format!("{label} => {}", deparse_bare_with(arg, qualify))),
            )
            .collect::<Vec<_>>()
            .join(", "),
        FuncArgs::Variadic { positional, array } => positional
            .iter()
            .map(|arg| deparse_bare_with(arg, qualify))
            .chain(std::iter::once(format!(
                "VARIADIC {}",
                deparse_bare_with(array, qualify)
            )))
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
            format!("{}.{field}", field_base(base, qualify))
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
                FuncArgs::Named { .. } => deparse_plain_call(call, qualify),
                FuncArgs::Variadic { .. } => deparse_plain_call(call, qualify),
            }
        }
        Expr::Func(call) => deparse_plain_call(call, qualify),
        Expr::Cast { expr, ty } => match (&**expr, ty) {
            (Expr::StringLiteral(text), crabka_pgtypes::ColumnType::Char(_)) => {
                format!("'{}'::bpchar", text.replace('\'', "''"))
            }
            _ => format!("{}::{}", cast_operand(expr, qualify), ty.name()),
        },
        // A sign on a numeric literal is not an operator by the time
        // PostgreSQL deparses anything: `doNegate` folds it into the literal's
        // own spelling, so what reaches `ruleutils` is one `Const` and
        // `get_const_expr` writes `'-3'::integer`. Gres has no typed constant
        // here — the plan is built from the AST, and the AST does not know
        // whether `-3` is an `int4`, a `bigint` or a `numeric` — so it cannot
        // pick the type label, and guessing one has been measured to cost more
        // corpus lines through psql's ruler than the wrong spelling does.
        // Folding the sign is the part that is right either way, and it keeps
        // the operator arm below from claiming `-3` is an operator node.
        Expr::Unary {
            op: op @ (UnaryOp::Neg | UnaryOp::Plus),
            expr,
        } if matches!(expr.as_ref(), Expr::IntLiteral(_) | Expr::NumericLiteral(_)) => {
            let sign = if matches!(op, UnaryOp::Neg) { "-" } else { "+" };
            format!("{sign}{}", deparse_bare_with(expr, qualify))
        }
        // Every other unary form is spelled by the one table `pg_get_viewdef`
        // uses, with `PRETTY_PAREN` cleared because `EXPLAIN` deparses at
        // `prettyFlags = 0`. That is why a plan line reads `(- q1)` and
        // `(b IS TRUE)` rather than the `-q1` and `IsTrue b` this arm printed
        // while it kept a second, incomplete table of its own.
        Expr::Unary { op, expr } => {
            crate::viewdef::unary_expr_text(*op, &deparse_bare_with(expr, qualify), false)
        }
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
            subscript_base(base, qualify),
            deparse_bare_with(index, qualify)
        ),
        Expr::ArrayRef { base, subscripts } => {
            let mut text = subscript_base(base, qualify);
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
        ExplainFormat::Json => vec![render_json(node, options, actual_rows)],
        ExplainFormat::Yaml => vec![render_yaml(node, options, actual_rows)],
        ExplainFormat::Xml => vec![render_xml(node, options, actual_rows)],
    }
}

fn render_text(node: &PlanNode, options: &ExplainOptions, actual_rows: usize) -> Vec<String> {
    let mut lines = Vec::new();
    render_text_node(node, options, None, actual_rows, &mut lines);
    lines
}

fn render_text_node(
    node: &PlanNode,
    options: &ExplainOptions,
    arrow_indent: Option<usize>,
    actual_rows: usize,
    lines: &mut Vec<String>,
) {
    let root = arrow_indent.is_none();
    // PostgreSQL puts a child's `->` arrow two columns in from its parent's
    // detail column, and every node's detail lines six columns in from its
    // parent's: arrow at `2 + 6*(depth-1)`, details at `2 + 6*depth`.
    let mut headline = if root {
        node.headline()
    } else {
        format!(
            "{}->  {}",
            " ".repeat(arrow_indent.expect("child indent")),
            node.headline()
        )
    };
    if options.costs {
        write!(
            headline,
            " (cost=0.00..0.00 rows={} width=0)",
            node.estimated_rows.unwrap_or(0)
        )
        .expect("String write");
    }
    if let Some(actual) = explain_actual(node, options, actual_rows, root) {
        if actual.loops == 0 {
            headline.push_str(" (never executed)");
        } else {
            write!(
                headline,
                " (actual rows={}.00 loops={})",
                actual.rows, actual.loops
            )
            .expect("String write");
        }
    }
    lines.push(headline);
    let detail = arrow_indent.map_or(2, |indent| indent + 6);
    let detail_indent = " ".repeat(detail);
    if options.verbose && !node.output.is_empty() {
        lines.push(format!("{detail_indent}Output: {}", node.output.join(", ")));
    }
    for (key, value) in &node.details {
        lines.push(format!("{detail_indent}{key}: {value}"));
    }
    if (matches!(node.node_type.as_str(), "Filter")
        || node.details.iter().any(|(key, _)| key == "Filter"))
        && let Some(actual) = node.actual
        && actual.rows_removed > 0
    {
        lines.push(format!(
            "{detail_indent}Rows Removed by Filter: {}",
            actual.rows_removed
        ));
    }
    for cte in &node.init_plans {
        lines.push(format!("{detail_indent}CTE {}", cte.name));
        render_text_node(&cte.plan, options, Some(detail + 2), actual_rows, lines);
    }
    let child_indent = arrow_indent.map_or(2, |indent| indent + 6);
    for child in &node.children {
        render_text_node(child, options, Some(child_indent), actual_rows, lines);
    }
}

fn render_json(node: &PlanNode, options: &ExplainOptions, actual_rows: usize) -> String {
    let mut lines = vec![
        "[".to_string(),
        "  {".to_string(),
        "    \"Plan\": {".to_string(),
    ];
    json_node(node, options, actual_rows, true, 6, &mut lines);
    lines.push("    }".to_string());
    lines.push("  }".to_string());
    lines.push("]".to_string());
    lines.join("\n")
}

/// Emit one plan node's JSON body (without its enclosing braces) at `indent`.
fn json_node(
    node: &PlanNode,
    options: &ExplainOptions,
    actual_rows: usize,
    root: bool,
    indent: usize,
    lines: &mut Vec<String>,
) {
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
    if options.verbose && !node.output.is_empty() {
        let output = node
            .output
            .iter()
            .map(|value| format!("\"{}\"", json_escape(value)))
            .collect::<Vec<_>>()
            .join(", ");
        fields.push(format!("{pad}\"Output\": [{output}]"));
    }
    if let Some(actual) = explain_actual(node, options, actual_rows, root) {
        fields.push(format!("{pad}\"Actual Rows\": {}", actual.rows));
        fields.push(format!("{pad}\"Actual Loops\": {}", actual.loops));
        if actual.rows_removed > 0 {
            fields.push(format!(
                "{pad}\"Rows Removed by Filter\": {}",
                actual.rows_removed
            ));
        }
    }
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
        json_node(child, options, actual_rows, false, indent + 4, lines);
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

fn render_yaml(node: &PlanNode, options: &ExplainOptions, actual_rows: usize) -> String {
    let mut lines = vec!["- Plan:".to_string()];
    yaml_node(node, options, actual_rows, true, 4, &mut lines);
    lines.join("\n")
}

fn yaml_node(
    node: &PlanNode,
    options: &ExplainOptions,
    actual_rows: usize,
    root: bool,
    indent: usize,
    lines: &mut Vec<String>,
) {
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
    if options.verbose && !node.output.is_empty() {
        let output = node
            .output
            .iter()
            .map(|value| format!("\"{value}\""))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!("{pad}Output: [{output}]"));
    }
    if let Some(actual) = explain_actual(node, options, actual_rows, root) {
        lines.push(format!("{pad}Actual Rows: {}", actual.rows));
        lines.push(format!("{pad}Actual Loops: {}", actual.loops));
        if actual.rows_removed > 0 {
            lines.push(format!(
                "{pad}Rows Removed by Filter: {}",
                actual.rows_removed
            ));
        }
    }
    for (key, value) in &node.details {
        lines.push(format!("{pad}{key}: \"{value}\""));
    }
    if !node.children.is_empty() {
        lines.push(format!("{pad}Plans:"));
        for child in &node.children {
            yaml_node(child, options, actual_rows, false, indent + 4, lines);
        }
    }
}

fn render_xml(node: &PlanNode, options: &ExplainOptions, actual_rows: usize) -> String {
    let mut lines = vec![
        "<explain xmlns=\"http://www.postgresql.org/2009/explain\">".to_string(),
        "  <Query>".to_string(),
    ];
    xml_node(node, options, actual_rows, true, 4, &mut lines);
    lines.push("  </Query>".to_string());
    lines.push("</explain>".to_string());
    lines.join("\n")
}

fn xml_node(
    node: &PlanNode,
    options: &ExplainOptions,
    actual_rows: usize,
    root: bool,
    indent: usize,
    lines: &mut Vec<String>,
) {
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
    if options.verbose && !node.output.is_empty() {
        lines.push(format!("{inner}<Output>"));
        for value in &node.output {
            lines.push(format!("{inner}  <Item>{}</Item>", xml_escape(value)));
        }
        lines.push(format!("{inner}</Output>"));
    }
    if let Some(actual) = explain_actual(node, options, actual_rows, root) {
        lines.push(format!("{inner}<Actual-Rows>{}</Actual-Rows>", actual.rows));
        lines.push(format!(
            "{inner}<Actual-Loops>{}</Actual-Loops>",
            actual.loops
        ));
        if actual.rows_removed > 0 {
            lines.push(format!(
                "{inner}<Rows-Removed-by-Filter>{}</Rows-Removed-by-Filter>",
                actual.rows_removed
            ));
        }
    }
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
            xml_node(child, options, actual_rows, false, indent + 4, lines);
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

fn explain_actual(
    node: &PlanNode,
    options: &ExplainOptions,
    actual_rows: usize,
    root: bool,
) -> Option<PlanActual> {
    options.analyze.then(|| {
        node.actual.unwrap_or(PlanActual {
            rows: u64::try_from(if root { actual_rows } else { 0 }).expect("row count fits u64"),
            loops: 1,
            rows_removed: 0,
        })
    })
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

    #[test]
    fn debug_parallel_gather_wraps_the_query_output() {
        let mut child = PlanNode::new("Result");
        child.output.push("value".into());
        assert_eq!(
            render_with_rows(
                &debug_parallel_gather(child),
                &ExplainOptions {
                    costs: false,
                    verbose: true,
                    ..ExplainOptions::default()
                },
                0,
            ),
            vec![
                "Gather",
                "  Output: (value)",
                "  Workers Planned: 1",
                "  Single Copy: true",
                "  ->  Result",
                "        Output: value",
            ]
        );
    }

    #[test]
    fn dependencies_payload_decodes_positions_and_degrees() {
        assert!(
            decode_dependencies(r#"{"1 => 2": 1.000000, "1, 2 => 3": 0.500000}"#)
                == Some(vec![(vec![1], 2, 1.0), (vec![1, 2], 3, 0.5)])
        );
    }

    #[test]
    fn dependency_selection_rejects_reciprocal_cycles_but_keeps_chains() {
        let applied = vec![(vec![1], 2)];

        assert!(dependencies_are_reciprocal(&[2], 1, &applied));
        assert!(!dependencies_are_reciprocal(&[2], 3, &applied));
    }

    #[test]
    fn row_estimates_round_like_postgres() {
        assert!(row_estimate(f64::NAN) == 1);
        assert!(row_estimate(0.25) == 1);
        assert!(row_estimate(200.000_001) == 200);
        assert!(row_estimate(200.6) == 201);
    }

    #[test]
    fn statistics_columns_match_group_expressions_but_not_the_reverse() {
        let table = crabka_pgcatalog::Table {
            id: 1,
            name: crabka_pgcatalog::RelationName::public("t"),
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            columns: vec![
                crabka_pgcatalog::Column::new("a", crabka_pgtypes::ColumnType::Int4),
                crabka_pgcatalog::Column::new("b", crabka_pgtypes::ColumnType::Int4),
            ],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            materialized: None,
            checks: Vec::new(),
        };
        let column = GroupKey::Attribute(2);
        let expression = GroupKey::Expression("(b + 1)".into());

        assert!(statistic_key_matches_group_key(
            &column,
            &expression,
            &table
        ));
        assert!(!statistic_key_matches_group_key(
            &expression,
            &column,
            &table
        ));
    }

    #[test]
    fn exact_expression_statistics_win_equal_width_matches() {
        let attributes = vec![(GroupKey::Attribute(1), 1), (GroupKey::Attribute(2), 2)];
        let expression = vec![
            (GroupKey::Attribute(1), 1),
            (GroupKey::Expression("(c * 10)".into()), -1),
        ];

        assert!(statistics_match_is_better(&expression, &attributes));
        assert!(!statistics_match_is_better(&attributes, &expression));
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

    #[test]
    fn runtime_tree_renders_each_nodes_actual_counters() {
        use crate::{
            plan::query::{Plan as ExecutablePlan, PlanNode as ExecutableNode, PlanState},
            scope::Scope,
        };

        let scan = ExecutablePlan {
            target_list: Vec::new(),
            quals: Vec::new(),
            node: ExecutableNode::SeqScan { scanrelid: 1 },
        };
        let filter = ExecutablePlan {
            target_list: Vec::new(),
            quals: Vec::new(),
            node: ExecutableNode::Filter {
                input: Box::new(scan.clone()),
            },
        };
        let mut state = PlanState::new(filter, Scope::empty());
        state.nloops = 1;
        state.ntuples = 2;
        state.rows_removed = 1;
        let mut child = PlanState::new(scan, Scope::empty());
        child.nloops = 1;
        child.ntuples = 3;
        state.children.push(child);

        let options = ExplainOptions {
            analyze: true,
            costs: false,
            ..ExplainOptions::default()
        };
        assert!(
            render_with_rows(&plan_runtime_state(&state), &options, 0)
                == vec![
                    "Filter (actual rows=2.00 loops=1)",
                    "  Rows Removed by Filter: 1",
                    "  ->  Seq Scan (actual rows=3.00 loops=1)",
                ]
        );

        let runtime = plan_runtime_state(&state);
        let json = render_with_rows(
            &runtime,
            &ExplainOptions {
                format: ExplainFormat::Json,
                ..options.clone()
            },
            0,
        );
        assert!(json[0].contains("\"Actual Rows\": 2"));
        assert!(json[0].contains("\"Rows Removed by Filter\": 1"));

        let yaml = render_with_rows(
            &runtime,
            &ExplainOptions {
                format: ExplainFormat::Yaml,
                ..options.clone()
            },
            0,
        );
        assert!(yaml[0].contains("Actual Loops: 1"));
        assert!(yaml[0].contains("Rows Removed by Filter: 1"));

        let xml = render_with_rows(
            &runtime,
            &ExplainOptions {
                format: ExplainFormat::Xml,
                ..options
            },
            0,
        );
        assert!(xml[0].contains("<Actual-Rows>2</Actual-Rows>"));
        assert!(xml[0].contains("<Rows-Removed-by-Filter>1</Rows-Removed-by-Filter>"));
    }

    #[test]
    fn runtime_tree_marks_an_unentered_node_never_executed() {
        use crate::{
            plan::query::{Plan as ExecutablePlan, PlanNode as ExecutableNode, PlanState},
            scope::Scope,
        };

        let plan = ExecutablePlan {
            target_list: Vec::new(),
            quals: Vec::new(),
            node: ExecutableNode::SeqScan { scanrelid: 1 },
        };
        let state = PlanState::new(plan, Scope::empty());
        let options = ExplainOptions {
            analyze: true,
            costs: false,
            ..ExplainOptions::default()
        };

        assert!(
            render_with_rows(&plan_runtime_state(&state), &options, 0)
                == vec!["Seq Scan (never executed)"]
        );
    }

    #[test]
    fn runtime_tree_omits_zero_filter_removals() {
        use crate::{
            plan::query::{Plan as ExecutablePlan, PlanNode as ExecutableNode, PlanState},
            scope::Scope,
        };

        let scan = ExecutablePlan {
            target_list: Vec::new(),
            quals: Vec::new(),
            node: ExecutableNode::SeqScan { scanrelid: 1 },
        };
        let filter = ExecutablePlan {
            target_list: Vec::new(),
            quals: Vec::new(),
            node: ExecutableNode::Filter {
                input: Box::new(scan),
            },
        };
        let mut state = PlanState::new(filter, Scope::empty());
        state.nloops = 1;
        let options = ExplainOptions {
            analyze: true,
            costs: false,
            ..ExplainOptions::default()
        };

        assert!(
            render_with_rows(&plan_runtime_state(&state), &options, 0)
                == vec!["Filter (actual rows=0.00 loops=1)"]
        );
        let runtime = plan_runtime_state(&state);
        for format in [ExplainFormat::Json, ExplainFormat::Yaml, ExplainFormat::Xml] {
            let removed = if format == ExplainFormat::Xml {
                "Rows-Removed-by-Filter"
            } else {
                "Rows Removed by Filter"
            };
            assert!(
                !render_with_rows(
                    &runtime,
                    &ExplainOptions {
                        format,
                        ..options.clone()
                    },
                    0,
                )[0]
                .contains(removed)
            );
        }
    }

    #[test]
    fn verbose_text_includes_the_plans_output_list() {
        let options = ExplainOptions {
            verbose: true,
            costs: false,
            ..ExplainOptions::default()
        };
        let lines = plan_text("SELECT id AS key FROM d1 WHERE id = 1", &options);

        assert!(lines == ["Seq Scan on d1", "  Output: key", "  Filter: (id = 1)",]);
        assert!(
            plan_text(
                "SELECT id AS key FROM d1",
                &ExplainOptions {
                    format: ExplainFormat::Json,
                    ..options.clone()
                },
            )[0]
            .contains("\"Output\": [\"key\"]")
        );
        assert!(
            plan_text(
                "SELECT id AS key FROM d1",
                &ExplainOptions {
                    format: ExplainFormat::Yaml,
                    ..options.clone()
                },
            )[0]
            .contains("Output: [\"key\"]")
        );
        let xml = plan_text(
            "SELECT id AS key FROM d1",
            &ExplainOptions {
                format: ExplainFormat::Xml,
                ..options
            },
        );
        assert!(xml[0].contains("<Output>"));
        assert!(xml[0].contains("<Item>key</Item>"));
    }

    #[test]
    fn offset_without_limit_still_has_a_limit_node() {
        let parsed = crabka_pgparser::parse("SELECT id FROM d1").expect("parse");
        let [Statement::Query(query)] = parsed.as_slice() else {
            panic!("expected query");
        };
        let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
            panic!("expected select");
        };
        let mut select = (**select).clone();
        select.offset = Some(Expr::IntLiteral("1".into()));
        assert!(
            render_with_rows(&plan_select(&select), &costs_off(), 0)
                == ["Limit", "  ->  Seq Scan on d1"]
        );
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

    /// Every unary form, in `PostgreSQL` 18.4's own `EXPLAIN (COSTS OFF)` text
    /// over a `d1` that carries a column of each type the operator needs.
    ///
    /// `get_oper_expr` writes a prefix operator, one space, then the operand,
    /// and `EXPLAIN` deparses with the pretty flag off, so the whole node
    /// carries a pair of parentheses. A `BooleanTest` reads the same way from
    /// the other side. `IS DOCUMENT` is the one exception in the set, and
    /// `IS NOT DOCUMENT` is not a node at all — the grammar builds
    /// `NOT (… IS DOCUMENT)` and the deparser prints that back.
    #[test]
    fn unary_operators_match_the_postgres_oracle() {
        let cases: &[(&str, &str)] = &[
            ("- id > 0", "  Filter: ((- id) > 0)"),
            ("+ id > 0", "  Filter: ((+ id) > 0)"),
            ("@ id > 0", "  Filter: ((@ id) > 0)"),
            ("~ id > 0", "  Filter: ((~ id) > 0)"),
            ("b IS TRUE", "  Filter: (b IS TRUE)"),
            ("b IS NOT TRUE", "  Filter: (b IS NOT TRUE)"),
            ("b IS FALSE", "  Filter: (b IS FALSE)"),
            ("b IS NOT FALSE", "  Filter: (b IS NOT FALSE)"),
            ("b IS UNKNOWN", "  Filter: (b IS UNKNOWN)"),
            ("b IS NOT UNKNOWN", "  Filter: (b IS NOT UNKNOWN)"),
            ("NOT b", "  Filter: (NOT b)"),
            ("x IS DOCUMENT", "  Filter: x IS DOCUMENT"),
            ("x IS NOT DOCUMENT", "  Filter: (NOT x IS DOCUMENT)"),
            ("?- l", "  Filter: (?- l)"),
            ("?| l", "  Filter: (?| l)"),
            ("(@@ bx) IS NOT NULL", "  Filter: ((@@ bx) IS NOT NULL)"),
            ("(!! q) IS NOT NULL", "  Filter: ((!! q) IS NOT NULL)"),
        ];
        for (predicate, expected) in cases {
            let lines = plan_text(&format!("SELECT * FROM d1 WHERE {predicate}"), &costs_off());
            assert!(lines == ["Seq Scan on d1", *expected], "{predicate}");
        }

        // A `Sort` references the node below it, and `get_special_variable`
        // parenthesizes a reference that does not land on a plain column — so
        // each of these carries the operator's own pair inside the sort key's.
        let keys: &[(&str, &str)] = &[
            ("- id", "  Sort Key: ((- id))"),
            ("|/ f", "  Sort Key: ((|/ f))"),
            ("||/ f", "  Sort Key: ((||/ f))"),
            ("@-@ l", "  Sort Key: ((@-@ l))"),
            ("# p", "  Sort Key: ((# p))"),
        ];
        for (key, expected) in keys {
            let lines = plan_text(&format!("SELECT * FROM d1 ORDER BY {key}"), &costs_off());
            assert!(
                lines == ["Sort", *expected, "  ->  Seq Scan on d1"],
                "{key}"
            );
        }

        // A sign on a literal is not an operator. PostgreSQL folds it into the
        // constant and answers `Filter: ((id = '-3'::integer) AND
        // (n = '-1.5'::numeric))`; this module cannot pick the type label
        // without knowing the constant's type, so it writes the folded
        // spelling and no label. What it must not do is print `(- 3)`, which
        // is neither engine's answer and is wider than both starts of one.
        let folded = plan_text("SELECT * FROM d1 WHERE id = -3 AND n = -1.5", &costs_off());
        assert!(folded == ["Seq Scan on d1", "  Filter: ((id = -3) AND (n = -1.5))"]);

        // The node that computes the expression prints it without that extra
        // pair, exactly as it does for any other grouping key.
        let grouped = plan_text("SELECT DISTINCT b IS TRUE FROM d1", &costs_off());
        assert!(
            grouped
                == [
                    "HashAggregate",
                    "  Group Key: (b IS TRUE)",
                    "  ->  Seq Scan on d1",
                ]
        );
    }

    /// `get_rule_expr` parenthesizes a subscript's base unless it is a plain
    /// column or a field selection. Each expectation is `PostgreSQL` 18.4's.
    #[test]
    fn a_subscript_base_is_parenthesised_unless_it_is_a_column_or_a_field() {
        let cases: &[(&str, &str)] = &[
            ("arr[1] = 1", "  Filter: (arr[1] = 1)"),
            ("(c).f[1] = 1", "  Filter: ((c).f[1] = 1)"),
        ];
        for (predicate, expected) in cases {
            let lines = plan_text(&format!("SELECT * FROM d1 WHERE {predicate}"), &costs_off());
            assert!(lines == ["Seq Scan on d1", *expected], "{predicate}");
        }

        let keys: &[(&str, &str)] = &[
            ("arr[1:2]", "  Sort Key: (arr[1:2])"),
            (
                "(string_to_array(s, ','))[1]",
                "  Sort Key: ((string_to_array(s, ','::text))[1])",
            ),
            (
                "(string_to_array(s, ','))[1:2]",
                "  Sort Key: ((string_to_array(s, ','::text))[1:2])",
            ),
        ];
        for (key, expected) in keys {
            let lines = plan_text(&format!("SELECT * FROM d1 ORDER BY {key}"), &costs_off());
            assert!(
                lines == ["Sort", *expected, "  ->  Seq Scan on d1"],
                "{key}"
            );
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
        assert!(!json[0].contains("\"Plans\""));
        assert!(json[0].contains("\"Alias\": \"d1\",\n      \"Disabled\""));
        assert!(json[0].contains("\"Disabled\": false\n    }"));

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
        assert!(!yaml[0].contains("Plans:"));

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
        assert!(!xml[0].contains("<Plans>"));
    }

    #[test]
    fn structured_formats_keep_nested_plan_layout_and_hide_non_verbose_output() {
        let options = ExplainOptions {
            costs: false,
            ..ExplainOptions::default()
        };
        for format in [ExplainFormat::Json, ExplainFormat::Yaml, ExplainFormat::Xml] {
            assert!(
                !plan_text(
                    "SELECT id FROM d1",
                    &ExplainOptions {
                        format,
                        ..options.clone()
                    },
                )[0]
                .contains("Output")
            );
        }

        let json = plan_text(
            "SELECT id FROM d1 ORDER BY id",
            &ExplainOptions {
                format: ExplainFormat::Json,
                ..options.clone()
            },
        );
        assert!(json[0].contains("\"Plans\": [\n        {\n          \"Node Type\": \"Seq Scan\""));
        assert!(json[0].contains("\n        }\n      ]"));

        let yaml = plan_text(
            "SELECT id FROM d1 ORDER BY id",
            &ExplainOptions {
                format: ExplainFormat::Yaml,
                ..options.clone()
            },
        );
        assert!(yaml[0].contains("Plans:\n        Node Type: \"Seq Scan\""));

        let xml = plan_text(
            "SELECT id FROM d1 ORDER BY id",
            &ExplainOptions {
                format: ExplainFormat::Xml,
                ..options
            },
        );
        assert!(
            xml[0].contains("<Plans>\n        <Plan>\n          <Node-Type>Seq Scan</Node-Type>")
        );
        assert!(xml[0].contains("        </Plan>\n      </Plans>"));
    }
}
