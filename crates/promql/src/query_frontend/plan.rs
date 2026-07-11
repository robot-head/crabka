use crabka_blockstore::QUERY_SHARD_LABEL;
use promql_parser::{
    label as prom_label,
    parser::{
        AggregateExpr, Expr, LabelModifier, VectorSelector,
        token::{
            T_AVG, T_BOTTOMK, T_COUNT, T_GROUP, T_MAX, T_MIN, T_STDDEV, T_STDVAR, T_SUM, T_TOPK,
            TokenType,
        },
    },
};

use super::{
    FrontendRangeQuery, MomentReduction, QueryFrontendOptions, QueryShard, QueryShardExecution,
    QueryShardReducer, RankReduction,
};
use crate::{PromqlError, engine::MAX_RESOLUTION_POINTS, parse_promql};

/// Plan query-frontend fan-out for a Prometheus range query.
///
/// Time splitting happens first. Sub-range boundaries align to *absolute*
/// multiples of `split_interval_ms` (Mimir-style): every evaluation timestamp
/// `start + n*step` is assigned to the absolute split window
/// `floor(t / split_interval) * split_interval`, and the eval points falling in
/// one window form one sub-range `[first_eval, last_eval]`. Eval points stay on
/// the caller's step grid, so each step appears in exactly one sub-range.
///
/// Absolute alignment is what makes the range-result cache reusable across
/// overlapping queries: an interior split window contains the same eval points
/// for any query that shares the step phase and fully covers that window, so
/// the interior sub-range (hence its cache key) is byte-for-byte identical even
/// when the surrounding window slides. Only the partial leading/trailing
/// windows clipped by the query bounds differ between such queries.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn plan_range_query(
    query: &str,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    opts: QueryFrontendOptions,
) -> Result<Vec<FrontendRangeQuery>, PromqlError> {
    if step_ms <= 0 {
        return Err(PromqlError::Plan(
            "query range step must be positive".into(),
        ));
    }
    if opts.split_interval_ms <= 0 {
        return Err(PromqlError::Plan(
            "query split interval must be positive".into(),
        ));
    }
    if opts.shard_count == 0 {
        return Err(PromqlError::Plan(
            "query shard count must be positive".into(),
        ));
    }
    if start_ms > end_ms {
        return Ok(Vec::new());
    }
    check_range_resolution(start_ms, end_ms, step_ms)?;

    let shard_count = if query_supports_frontend_sharding(query)? {
        opts.shard_count
    } else {
        1
    };
    let split_interval = opts.split_interval_ms;
    let mut subqueries = Vec::new();
    let mut eval = start_ms;
    // Track the open sub-range: the absolute window it belongs to plus the first
    // and last eval timestamps seen in that window.
    let mut current: Option<(i64, i64, i64)> = None;

    while eval <= end_ms {
        let window = absolute_split_window(eval, split_interval);
        match current.as_mut() {
            Some((open_window, _, last)) if *open_window == window => {
                *last = eval;
            }
            _ => {
                if let Some((_, range_start, range_end)) = current.take() {
                    push_sharded_subqueries(
                        &mut subqueries,
                        query,
                        range_start,
                        range_end,
                        step_ms,
                        shard_count,
                    );
                }
                current = Some((window, eval, eval));
            }
        }

        let Some(next_eval) = eval.checked_add(step_ms) else {
            break;
        };
        eval = next_eval;
    }

    if let Some((_, range_start, range_end)) = current {
        push_sharded_subqueries(
            &mut subqueries,
            query,
            range_start,
            range_end,
            step_ms,
            shard_count,
        );
    }

    Ok(subqueries)
}

/// Reject a range query whose resolution exceeds the per-timeseries point cap,
/// matching Prometheus's unconditional front-gate
/// (`(end - start) / step > maxResolution`, integer division, where
/// `maxResolution` is [`MAX_RESOLUTION_POINTS`]). Enforced before the per-step
/// fan-out so an abusive resolution errors instead of expanding into ~1e11
/// sub-queries. `step_ms` is already validated positive by [`plan_range_query`].
fn check_range_resolution(start_ms: i64, end_ms: i64, step_ms: i64) -> Result<(), PromqlError> {
    if step_ms <= 0 {
        return Ok(());
    }
    let intervals = end_ms.saturating_sub(start_ms) / step_ms;
    if intervals > i64::try_from(MAX_RESOLUTION_POINTS).unwrap_or(i64::MAX) {
        return Err(PromqlError::Plan(
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)"
                .into(),
        ));
    }
    Ok(())
}

/// Absolute split window a timestamp belongs to: the greatest multiple of
/// `split_interval` that is `<= ts`. Uses flooring division so negative
/// timestamps still align downward.
fn absolute_split_window(ts: i64, split_interval: i64) -> i64 {
    let quotient = ts.div_euclid(split_interval);
    quotient.saturating_mul(split_interval)
}

fn query_supports_frontend_sharding(query: &str) -> Result<bool, PromqlError> {
    let expr = parse_promql(query)?;
    Ok(avg_partial_queries(&expr).is_some()
        || moment_partial_queries(&expr).is_some()
        || rank_reduction(&expr).is_some()
        || expr_supports_frontend_sharding(&expr))
}

pub(super) fn query_shard_execution(query: &str) -> Result<QueryShardExecution, PromqlError> {
    let expr = parse_promql(query)?;
    if let Some((sum_query, count_query)) = avg_partial_queries(&expr) {
        return Ok(QueryShardExecution::Avg {
            sum_query,
            count_query,
        });
    }
    if let Some((sum_query, count_query, sum_squares_query, kind)) = moment_partial_queries(&expr) {
        return Ok(QueryShardExecution::Moments {
            sum_query,
            count_query,
            sum_squares_query,
            kind,
        });
    }
    if let Some((k, kind, modifier)) = rank_reduction(&expr) {
        return Ok(QueryShardExecution::Rank { k, kind, modifier });
    }
    Ok(QueryShardExecution::Merge(expr_shard_reducer(&expr)))
}

fn avg_partial_queries(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::Aggregate(aggregate)
            if aggregate.op.id() == T_AVG
                && aggregate.param.is_none()
                && !expr_contains_aggregate(&aggregate.expr)
                && expr_supports_frontend_sharding(&aggregate.expr) =>
        {
            let mut sum_aggregate = aggregate.clone();
            sum_aggregate.op = TokenType::new(T_SUM);
            let mut count_aggregate = aggregate.clone();
            count_aggregate.op = TokenType::new(T_COUNT);
            Some((
                Expr::Aggregate(sum_aggregate).to_string(),
                Expr::Aggregate(count_aggregate).to_string(),
            ))
        }
        Expr::Paren(paren) => avg_partial_queries(&paren.expr),
        _ => None,
    }
}

fn moment_partial_queries(expr: &Expr) -> Option<(String, String, String, MomentReduction)> {
    match expr {
        Expr::Aggregate(aggregate)
            if matches!(aggregate.op.id(), T_STDDEV | T_STDVAR)
                && aggregate.param.is_none()
                && !expr_contains_aggregate(&aggregate.expr)
                && expr_supports_frontend_sharding(&aggregate.expr) =>
        {
            let kind = if aggregate.op.id() == T_STDDEV {
                MomentReduction::Stddev
            } else {
                MomentReduction::Stdvar
            };
            let mut sum_aggregate = aggregate.clone();
            sum_aggregate.op = TokenType::new(T_SUM);
            let mut count_aggregate = aggregate.clone();
            count_aggregate.op = TokenType::new(T_COUNT);
            let squared_expr =
                parse_promql(&format!("({}) * ({})", aggregate.expr, aggregate.expr)).ok()?;
            let mut sum_squares_aggregate = aggregate.clone();
            sum_squares_aggregate.op = TokenType::new(T_SUM);
            sum_squares_aggregate.expr = Box::new(squared_expr);
            Some((
                Expr::Aggregate(sum_aggregate).to_string(),
                Expr::Aggregate(count_aggregate).to_string(),
                Expr::Aggregate(sum_squares_aggregate).to_string(),
                kind,
            ))
        }
        Expr::Paren(paren) => moment_partial_queries(&paren.expr),
        _ => None,
    }
}

fn rank_reduction(expr: &Expr) -> Option<(usize, RankReduction, Option<LabelModifier>)> {
    match expr {
        Expr::Aggregate(aggregate)
            if matches!(aggregate.op.id(), T_BOTTOMK | T_TOPK)
                && !expr_contains_aggregate(&aggregate.expr)
                && expr_supports_frontend_sharding(&aggregate.expr) =>
        {
            let kind = if aggregate.op.id() == T_TOPK {
                RankReduction::Top
            } else {
                RankReduction::Bottom
            };
            Some((aggregate_k(aggregate)?, kind, aggregate.modifier.clone()))
        }
        Expr::Paren(paren) => rank_reduction(&paren.expr),
        _ => None,
    }
}

fn aggregate_k(aggregate: &AggregateExpr) -> Option<usize> {
    let param = aggregate.param.as_ref()?;
    let Expr::NumberLiteral(number) = param.as_ref() else {
        return None;
    };
    if !number.val.is_finite() || number.val < 0.0 || number.val.fract() != 0.0 {
        return None;
    }
    number.val.to_string().parse::<usize>().ok()
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate(_) => true,
        Expr::Unary(unary) => expr_contains_aggregate(&unary.expr),
        Expr::Binary(binary) => {
            expr_contains_aggregate(&binary.lhs) || expr_contains_aggregate(&binary.rhs)
        }
        Expr::Paren(paren) => expr_contains_aggregate(&paren.expr),
        Expr::Subquery(subquery) => expr_contains_aggregate(&subquery.expr),
        Expr::Call(call) => call
            .args
            .args
            .iter()
            .any(|arg| expr_contains_aggregate(arg)),
        Expr::VectorSelector(_)
        | Expr::MatrixSelector(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Extension(_) => false,
    }
}

fn expr_shard_reducer(expr: &Expr) -> QueryShardReducer {
    match expr {
        Expr::Aggregate(aggregate) => match aggregate.op.id() {
            T_SUM | T_COUNT => QueryShardReducer::Sum,
            T_MIN => QueryShardReducer::Min,
            T_MAX => QueryShardReducer::Max,
            _ => QueryShardReducer::First,
        },
        Expr::Paren(paren) => expr_shard_reducer(&paren.expr),
        _ => QueryShardReducer::First,
    }
}

fn expr_supports_frontend_sharding(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate(aggregate) => {
            matches!(aggregate.op.id(), T_SUM | T_COUNT | T_GROUP | T_MIN | T_MAX)
                && aggregate
                    .param
                    .as_ref()
                    .is_none_or(|param| expr_supports_frontend_sharding(param))
                && expr_supports_frontend_sharding(&aggregate.expr)
        }
        Expr::Unary(unary) => expr_supports_frontend_sharding(&unary.expr),
        Expr::Binary(binary) => {
            expr_supports_frontend_sharding(&binary.lhs)
                && expr_supports_frontend_sharding(&binary.rhs)
        }
        Expr::Paren(paren) => expr_supports_frontend_sharding(&paren.expr),
        Expr::Subquery(subquery) => expr_supports_frontend_sharding(&subquery.expr),
        Expr::Call(call) => call
            .args
            .args
            .iter()
            .all(|arg| expr_supports_frontend_sharding(arg)),
        Expr::VectorSelector(_)
        | Expr::MatrixSelector(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Extension(_) => true,
    }
}

fn push_sharded_subqueries(
    subqueries: &mut Vec<FrontendRangeQuery>,
    query: &str,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    shard_count: usize,
) {
    if shard_count == 1 {
        subqueries.push(FrontendRangeQuery {
            query: query.to_string(),
            start_ms,
            end_ms,
            step_ms,
            shard: None,
        });
        return;
    }

    for index in 1..=shard_count {
        subqueries.push(FrontendRangeQuery {
            query: query.to_string(),
            start_ms,
            end_ms,
            step_ms,
            shard: Some(QueryShard {
                index,
                total: shard_count,
            }),
        });
    }
}

pub(super) fn query_with_shard_selector(
    query: &str,
    shard: QueryShard,
) -> Result<String, PromqlError> {
    let mut expr = parse_promql(query)?;
    inject_shard_into_expr(&mut expr, shard);
    Ok(expr.to_string())
}

fn inject_shard_into_expr(expr: &mut Expr, shard: QueryShard) {
    match expr {
        Expr::Aggregate(aggregate) => {
            if let Some(param) = aggregate.param.as_mut() {
                inject_shard_into_expr(param, shard);
            }
            inject_shard_into_expr(&mut aggregate.expr, shard);
        }
        Expr::Unary(unary) => inject_shard_into_expr(&mut unary.expr, shard),
        Expr::Binary(binary) => {
            inject_shard_into_expr(&mut binary.lhs, shard);
            inject_shard_into_expr(&mut binary.rhs, shard);
        }
        Expr::Paren(paren) => inject_shard_into_expr(&mut paren.expr, shard),
        Expr::Subquery(subquery) => inject_shard_into_expr(&mut subquery.expr, shard),
        Expr::VectorSelector(selector) => inject_shard_into_selector(selector, shard),
        Expr::MatrixSelector(selector) => inject_shard_into_selector(&mut selector.vs, shard),
        Expr::Call(call) => {
            for arg in &mut call.args.args {
                inject_shard_into_expr(arg, shard);
            }
        }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => {}
    }
}

fn inject_shard_into_selector(selector: &mut VectorSelector, shard: QueryShard) {
    if selector
        .matchers
        .matchers
        .iter()
        .any(|matcher| matcher.name == QUERY_SHARD_LABEL)
    {
        return;
    }

    selector.matchers.matchers.push(prom_label::Matcher::new(
        prom_label::MatchOp::Equal,
        QUERY_SHARD_LABEL,
        &shard.selector_value(),
    ));
}
