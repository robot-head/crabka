use async_trait::async_trait;
use crabka_units::prelude::*;

use super::{
    FrontendRangeQuery, FrontendRangeRequest, MomentReduction, QueryShardExecution,
    QueryShardReducer,
    cache::RangeQueryCache,
    merge::{
        divide_range_query_results, merge_range_query_results_with_reducer,
        reduce_moment_range_query_results, reduce_rank_range_query_results,
    },
    plan::{plan_range_query, query_shard_execution, query_with_shard_selector},
};
use crate::{MetricStore, PromqlEngine, PromqlError, QueryResult};

/// Executes one planned range subquery.
#[async_trait]
pub trait RangeQueryExecutor: Send + Sync {
    async fn execute_range_query(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError>;
}

#[async_trait]
impl<S: MetricStore> RangeQueryExecutor for PromqlEngine<S> {
    async fn execute_range_query(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError> {
        let query_text = match query.shard {
            Some(shard) => query_with_shard_selector(&query.query, shard)?,
            None => query.query.clone(),
        };

        self.query_range(
            tenant,
            &query_text,
            query.start_ms,
            query.end_ms,
            query.step,
        )
        .await
    }
}

/// Executes a range query through query-frontend planning, cache, and merge.
#[tracing::instrument(
    name = "promql.query_frontend_range",
    level = "info",
    skip_all,
    fields(
        tenant = %request.tenant,
        query = %request.query,
        start_ms = request.start_ms,
        end_ms = request.end_ms,
        step_ms = request.step.millis_i64()
    ),
    err
)]
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn execute_range_query_frontend<E, C>(
    executor: &E,
    cache: &C,
    request: &FrontendRangeRequest,
) -> Result<QueryResult, PromqlError>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    let execution = query_shard_execution(&request.query)?;
    if let QueryShardExecution::Avg {
        sum_query,
        count_query,
    } = execution
    {
        return execute_avg_range_query_frontend(
            executor,
            cache,
            request,
            &sum_query,
            &count_query,
        )
        .await;
    }
    if let QueryShardExecution::Moments {
        sum_query,
        count_query,
        sum_squares_query,
        kind,
    } = execution
    {
        return execute_moment_range_query_frontend(
            executor,
            cache,
            request,
            &sum_query,
            &count_query,
            &sum_squares_query,
            kind,
        )
        .await;
    }
    let rank = if let QueryShardExecution::Rank { k, kind, modifier } = &execution {
        Some((*k, *kind, modifier.clone()))
    } else {
        None
    };

    let planned = plan_range_query(
        &request.query,
        request.start_ms,
        request.end_ms,
        request.step,
        request.opts,
    )?;
    let results = execute_planned_range_queries(executor, cache, &request.tenant, planned).await?;

    let QueryShardExecution::Merge(reducer) = execution else {
        if let Some((k, kind, modifier)) = rank {
            let merged = merge_range_query_results_with_reducer(results, QueryShardReducer::First)?;
            return reduce_rank_range_query_results(merged, k, kind, modifier.as_ref());
        }
        unreachable!("partial query execution returned early")
    };
    merge_range_query_results_with_reducer(results, reducer)
}

/// Executes the planned sub-queries concurrently, one per sub-range and shard.
///
/// The planned sub-queries are independent, so this function dispatches them all
/// at once with [`futures::future::join_all`] and does not await them one by
/// one. It collects the results by planned position, so the order does not
/// depend on which sub-query completes first. The matrix-stitching merge needs
/// that deterministic order. The [`RangeQueryExecutor`] and [`RangeQueryCache`]
/// bounds are `Send + Sync`, so the per-sub-query futures are `Send` and safe to
/// drive together.
pub(super) async fn execute_planned_range_queries<E, C>(
    executor: &E,
    cache: &C,
    tenant: &str,
    planned: Vec<FrontendRangeQuery>,
) -> Result<Vec<QueryResult>, PromqlError>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    let futures = planned
        .iter()
        .map(|subquery| execute_single_range_query(executor, cache, tenant, subquery));
    futures::future::join_all(futures)
        .await
        .into_iter()
        .collect()
}

async fn execute_single_range_query<E, C>(
    executor: &E,
    cache: &C,
    tenant: &str,
    subquery: &FrontendRangeQuery,
) -> Result<QueryResult, PromqlError>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    if let Some(result) = cache.get(tenant, subquery).await? {
        return Ok(result);
    }
    let result = executor.execute_range_query(tenant, subquery).await?;
    cache.insert(tenant, subquery, result.clone()).await?;
    Ok(result)
}

async fn execute_avg_range_query_frontend<E, C>(
    executor: &E,
    cache: &C,
    request: &FrontendRangeRequest,
    sum_query: &str,
    count_query: &str,
) -> Result<QueryResult, PromqlError>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    let sum_plan = plan_range_query(
        sum_query,
        request.start_ms,
        request.end_ms,
        request.step,
        request.opts,
    )?;
    let count_plan = plan_range_query(
        count_query,
        request.start_ms,
        request.end_ms,
        request.step,
        request.opts,
    )?;
    let sum_results =
        execute_planned_range_queries(executor, cache, &request.tenant, sum_plan).await?;
    let count_results =
        execute_planned_range_queries(executor, cache, &request.tenant, count_plan).await?;
    let sums = merge_range_query_results_with_reducer(sum_results, QueryShardReducer::Sum)?;
    let counts = merge_range_query_results_with_reducer(count_results, QueryShardReducer::Sum)?;
    divide_range_query_results(sums, counts)
}

async fn execute_moment_range_query_frontend<E, C>(
    executor: &E,
    cache: &C,
    request: &FrontendRangeRequest,
    sum_query: &str,
    count_query: &str,
    sum_squares_query: &str,
    kind: MomentReduction,
) -> Result<QueryResult, PromqlError>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    let sum_plan = plan_range_query(
        sum_query,
        request.start_ms,
        request.end_ms,
        request.step,
        request.opts,
    )?;
    let count_plan = plan_range_query(
        count_query,
        request.start_ms,
        request.end_ms,
        request.step,
        request.opts,
    )?;
    let sum_squares_plan = plan_range_query(
        sum_squares_query,
        request.start_ms,
        request.end_ms,
        request.step,
        request.opts,
    )?;
    let sum_results =
        execute_planned_range_queries(executor, cache, &request.tenant, sum_plan).await?;
    let count_results =
        execute_planned_range_queries(executor, cache, &request.tenant, count_plan).await?;
    let sum_squares_results =
        execute_planned_range_queries(executor, cache, &request.tenant, sum_squares_plan).await?;
    let sums = merge_range_query_results_with_reducer(sum_results, QueryShardReducer::Sum)?;
    let counts = merge_range_query_results_with_reducer(count_results, QueryShardReducer::Sum)?;
    let sum_squares =
        merge_range_query_results_with_reducer(sum_squares_results, QueryShardReducer::Sum)?;
    reduce_moment_range_query_results(sums, counts, sum_squares, kind)
}
