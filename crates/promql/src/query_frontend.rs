//! Query-frontend range splitting, sharding, and merge helpers.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use crabka_blockstore::{LabelMatcher, Labels, MatchOp, SeriesFingerprint};
use crabka_metrics::{BucketSpan, NativeHistogram};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use promql_parser::label as prom_label;
use promql_parser::parser::token::{
    T_AVG, T_BOTTOMK, T_COUNT, T_GROUP, T_MAX, T_MIN, T_STDDEV, T_STDVAR, T_SUM, T_TOPK, TokenType,
};
use promql_parser::parser::{AggregateExpr, Expr, LabelModifier, VectorSelector};

use crate::{
    MetricStore, PromqlEngine, PromqlError, QueryResult, RangeSeries, SampleValue,
    engine::MAX_RESOLUTION_POINTS, parse_promql,
};

pub use crabka_blockstore::QUERY_SHARD_LABEL;

/// Query-frontend range splitting and sharding options.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryFrontendOptions {
    pub split_interval_ms: i64,
    pub shard_count: usize,
}

/// One user range query entering the query-frontend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontendRangeRequest {
    pub tenant: String,
    pub query: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    pub opts: QueryFrontendOptions,
}

/// One Mimir-compatible query shard. Shards are one-based on the wire:
/// `1_of_3`, `2_of_3`, ...
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryShard {
    pub index: usize,
    pub total: usize,
}

impl QueryShard {
    #[must_use]
    pub fn selector_value(self) -> String {
        format!("{}_of_{}", self.index, self.total)
    }

    #[must_use]
    pub fn matcher(self) -> LabelMatcher {
        LabelMatcher {
            name: QUERY_SHARD_LABEL.to_string(),
            op: MatchOp::Eq,
            value: self.selector_value(),
        }
    }
}

/// One subquery the query-frontend can fan out to a querier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontendRangeQuery {
    pub query: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    pub shard: Option<QueryShard>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryShardReducer {
    First,
    Sum,
    Min,
    Max,
}

enum QueryShardExecution {
    Merge(QueryShardReducer),
    Avg {
        sum_query: String,
        count_query: String,
    },
    Moments {
        sum_query: String,
        count_query: String,
        sum_squares_query: String,
        kind: MomentReduction,
    },
    Rank {
        k: usize,
        kind: RankReduction,
        modifier: Option<LabelModifier>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MomentReduction {
    Stddev,
    Stdvar,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RankReduction {
    Bottom,
    Top,
}

impl FrontendRangeQuery {
    #[must_use]
    pub fn shard_matcher(&self) -> Option<LabelMatcher> {
        self.shard.map(QueryShard::matcher)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RangeCacheKey {
    tenant: String,
    query: String,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    shard: Option<QueryShard>,
}

impl RangeCacheKey {
    fn new(tenant: &str, query: &FrontendRangeQuery) -> Self {
        Self {
            tenant: tenant.to_string(),
            query: query.query.clone(),
            start_ms: query.start_ms,
            end_ms: query.end_ms,
            step_ms: query.step_ms,
            shard: query.shard,
        }
    }
}

/// Wall-clock source for cache-entry age checks.
///
/// Abstracted so tests can advance time deterministically (via
/// [`ManualClock`]) instead of sleeping. Production uses [`SystemClock`].
pub trait Clock: Send + Sync {
    /// Current time as Unix-epoch milliseconds.
    fn now_epoch_millis(&self) -> i64;
}

/// Default wall clock backed by [`std::time::SystemTime`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_epoch_millis(&self) -> i64 {
        let now = std::time::SystemTime::now();
        match now.duration_since(std::time::UNIX_EPOCH) {
            Ok(elapsed) => i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX),
            // Clock is before the Unix epoch; treat as time zero.
            Err(_) => 0,
        }
    }
}

/// Returns `true` when `inserted_epoch_millis` is older than `ttl` relative to
/// `now_epoch_millis`. `None` TTL never expires.
fn entry_is_expired(
    ttl: Option<std::time::Duration>,
    inserted_epoch_millis: i64,
    now_epoch_millis: i64,
) -> bool {
    let Some(ttl) = ttl else {
        return false;
    };
    let ttl_millis = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
    now_epoch_millis.saturating_sub(inserted_epoch_millis) > ttl_millis
}

/// In-memory range-result cache for query-frontend fan-out responses.
///
/// The backing store is intentionally small and swappable: production wiring can
/// replace it with an object-store/topic-backed implementation while preserving
/// the key contract tested here.
///
/// Entries carry an insertion timestamp (from the configured [`Clock`]). When a
/// TTL is set via [`QueryFrontendCache::with_ttl`], a `get` for an entry older
/// than the TTL evicts it and reports a miss. With no TTL (the default) entries
/// never expire.
pub struct QueryFrontendCache {
    range_results: Mutex<BTreeMap<RangeCacheKey, (i64, QueryResult)>>,
    ttl: Option<std::time::Duration>,
    clock: Arc<dyn Clock>,
}

impl Default for QueryFrontendCache {
    fn default() -> Self {
        Self {
            range_results: Mutex::new(BTreeMap::new()),
            ttl: None,
            clock: Arc::new(SystemClock),
        }
    }
}

impl QueryFrontendCache {
    /// Build a cache that expires entries older than `ttl`.
    #[must_use]
    pub fn with_ttl(ttl: std::time::Duration) -> Self {
        Self {
            ttl: Some(ttl),
            ..Self::default()
        }
    }

    /// Override the wall clock (primarily for deterministic tests).
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    #[must_use]
    pub fn get(&self, tenant: &str, query: &FrontendRangeQuery) -> Option<QueryResult> {
        let key = RangeCacheKey::new(tenant, query);
        let mut entries = self
            .range_results
            .lock()
            .expect("query frontend cache poisoned");
        let (inserted, result) = entries.get(&key)?;
        if entry_is_expired(self.ttl, *inserted, self.clock.now_epoch_millis()) {
            entries.remove(&key);
            return None;
        }
        Some(result.clone())
    }

    pub fn insert(&self, tenant: &str, query: &FrontendRangeQuery, result: QueryResult) {
        let inserted = self.clock.now_epoch_millis();
        self.range_results
            .lock()
            .expect("query frontend cache poisoned")
            .insert(RangeCacheKey::new(tenant, query), (inserted, result));
    }
}

#[async_trait]
pub trait RangeQueryCache: Send + Sync {
    async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<QueryResult>, PromqlError>;

    async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: QueryResult,
    ) -> Result<(), PromqlError>;
}

#[async_trait]
impl RangeQueryCache for QueryFrontendCache {
    async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<QueryResult>, PromqlError> {
        Ok(QueryFrontendCache::get(self, tenant, query))
    }

    async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: QueryResult,
    ) -> Result<(), PromqlError> {
        QueryFrontendCache::insert(self, tenant, query, result);
        Ok(())
    }
}

/// Cached object-store payload: the range result plus the wall-clock instant it
/// was stored, so a reader can enforce a TTL without depending on object-store
/// `last_modified` metadata.
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredRangeResult {
    stored_at_ms: i64,
    result: QueryResult,
}

/// Object-store backed range-result cache for query-frontend fan-out responses.
///
/// Each cached object embeds the epoch-millis instant it was stored. When a TTL
/// is set via [`ObjectStoreQueryFrontendCache::with_ttl`], a `get` for an object
/// older than the TTL reports a miss (and best-effort deletes the stale object).
pub struct ObjectStoreQueryFrontendCache {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    ttl: Option<std::time::Duration>,
    clock: Arc<dyn Clock>,
}

impl ObjectStoreQueryFrontendCache {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        Self {
            store,
            prefix: normalize_cache_prefix(&prefix),
            ttl: None,
            clock: Arc::new(SystemClock),
        }
    }

    /// Expire cached objects older than `ttl`.
    #[must_use]
    pub fn with_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Override the wall clock (primarily for deterministic tests).
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<QueryResult>, PromqlError> {
        <Self as RangeQueryCache>::get(self, tenant, query).await
    }

    pub async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: QueryResult,
    ) -> Result<(), PromqlError> {
        <Self as RangeQueryCache>::insert(self, tenant, query, result).await
    }

    fn path(&self, tenant: &str, query: &FrontendRangeQuery) -> Path {
        Path::from(format!(
            "{}/{}.json",
            self.prefix,
            range_cache_key_object_name(tenant, query)
        ))
    }
}

#[async_trait]
impl RangeQueryCache for ObjectStoreQueryFrontendCache {
    async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<QueryResult>, PromqlError> {
        let path = self.path(tenant, query);
        let bytes = match self.store.get(&path).await {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|error| cache_store_error(&error))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(cache_store_error(&error)),
        };
        let stored: StoredRangeResult = serde_json::from_slice(&bytes).map_err(|error| {
            PromqlError::Store(format!("query frontend cache decode failed: {error}"))
        })?;
        if entry_is_expired(self.ttl, stored.stored_at_ms, self.clock.now_epoch_millis()) {
            // Best-effort eviction; a delete failure must not fail the read.
            let _ = self.store.delete(&path).await;
            return Ok(None);
        }
        Ok(Some(stored.result))
    }

    async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: QueryResult,
    ) -> Result<(), PromqlError> {
        let path = self.path(tenant, query);
        let stored = StoredRangeResult {
            stored_at_ms: self.clock.now_epoch_millis(),
            result,
        };
        let bytes = serde_json::to_vec(&stored).map_err(|error| {
            PromqlError::Store(format!("query frontend cache encode failed: {error}"))
        })?;
        self.store
            .put(&path, PutPayload::from(bytes))
            .await
            .map_err(|error| cache_store_error(&error))?;
        Ok(())
    }
}

fn normalize_cache_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        "query-cache".to_string()
    } else {
        trimmed.to_string()
    }
}

fn cache_store_error(error: &object_store::Error) -> PromqlError {
    PromqlError::Store(format!("query frontend cache object-store error: {error}"))
}

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
            query.step_ms,
        )
        .await
    }
}

/// Execute a range query through query-frontend planning, cache, and merge.
#[tracing::instrument(
    name = "promql.query_frontend_range",
    level = "info",
    skip_all,
    fields(
        tenant = %request.tenant,
        query = %request.query,
        start_ms = request.start_ms,
        end_ms = request.end_ms,
        step_ms = request.step_ms
    ),
    err
)]
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
        request.step_ms,
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

/// Execute the planned sub-queries (split sub-ranges x shards) concurrently.
///
/// The planned sub-queries are independent, so they are dispatched all at once
/// via [`futures::future::join_all`] rather than awaited one-by-one. Results are
/// collected indexed by planned position, preserving the deterministic ordering
/// the matrix-stitching merge relies on regardless of which sub-query completes
/// first. The [`RangeQueryExecutor`] / [`RangeQueryCache`] bounds are `Send +
/// Sync`, so the per-sub-query futures are `Send` and safe to drive together.
async fn execute_planned_range_queries<E, C>(
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
        request.step_ms,
        request.opts,
    )?;
    let count_plan = plan_range_query(
        count_query,
        request.start_ms,
        request.end_ms,
        request.step_ms,
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
        request.step_ms,
        request.opts,
    )?;
    let count_plan = plan_range_query(
        count_query,
        request.start_ms,
        request.end_ms,
        request.step_ms,
        request.opts,
    )?;
    let sum_squares_plan = plan_range_query(
        sum_squares_query,
        request.start_ms,
        request.end_ms,
        request.step_ms,
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

fn range_cache_key_object_name(tenant: &str, query: &FrontendRangeQuery) -> String {
    let mut key = String::new();
    append_hex_component(&mut key, tenant.as_bytes());
    key.push('/');
    append_hex_component(&mut key, query.query.as_bytes());
    let shard = query
        .shard
        .map_or_else(|| "none".to_string(), QueryShard::selector_value);
    let _ = write!(
        key,
        "/{}-{}-{}-{}-{}",
        query.start_ms,
        query.end_ms,
        query.step_ms,
        query.shard.map_or(0, |shard| shard.index),
        query.shard.map_or(0, |shard| shard.total)
    );
    key.push('-');
    append_hex_component(&mut key, shard.as_bytes());
    key
}

fn append_hex_component(out: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
}

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

fn query_shard_execution(query: &str) -> Result<QueryShardExecution, PromqlError> {
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

/// Merge range-matrix subquery results back into one Prometheus matrix.
///
/// This is the query-frontend counterpart to [`plan_range_query`]: time-split
/// subqueries for the same series are stitched together, while sharded subqueries
/// naturally contribute distinct series.
pub fn merge_range_query_results(results: Vec<QueryResult>) -> Result<QueryResult, PromqlError> {
    merge_range_query_results_with_reducer(results, QueryShardReducer::Sum)
}

fn merge_range_query_results_with_reducer(
    results: Vec<QueryResult>,
    reducer: QueryShardReducer,
) -> Result<QueryResult, PromqlError> {
    let mut by_fp = BTreeMap::<SeriesFingerprint, RangeSeries>::new();

    for result in results {
        let QueryResult::RangeMatrix(series) = result else {
            return Err(PromqlError::Plan(
                "query-frontend range merge requires range matrix subquery results".into(),
            ));
        };
        for mut series in series {
            by_fp
                .entry(series.labels.fingerprint())
                .and_modify(|existing| existing.samples.append(&mut series.samples))
                .or_insert(series);
        }
    }

    let mut series = by_fp.into_values().collect::<Vec<_>>();
    series.sort_by_key(|series| label_sort_key(&series.labels));
    for series in &mut series {
        series.samples.sort_by_key(|(ts_ms, _)| *ts_ms);
        reduce_duplicate_step_samples(&mut series.samples, reducer)?;
    }
    Ok(QueryResult::RangeMatrix(series))
}

fn reduce_duplicate_step_samples(
    samples: &mut Vec<(i64, SampleValue)>,
    reducer: QueryShardReducer,
) -> Result<(), PromqlError> {
    let mut merged_samples = Vec::<(i64, SampleValue)>::with_capacity(samples.len());
    for (ts_ms, value) in samples.drain(..) {
        match merged_samples.last_mut() {
            Some((last_ts, SampleValue::Float(last_value))) if *last_ts == ts_ms => {
                if let SampleValue::Float(value) = value {
                    *last_value = match reducer {
                        QueryShardReducer::First => *last_value,
                        QueryShardReducer::Sum => *last_value + value,
                        QueryShardReducer::Min => last_value.min(value),
                        QueryShardReducer::Max => last_value.max(value),
                    };
                }
            }
            Some((last_ts, SampleValue::Histogram(last_value)))
                if *last_ts == ts_ms && reducer == QueryShardReducer::Sum =>
            {
                if let SampleValue::Histogram(value) = value {
                    add_compatible_native_histogram(last_value, &value)?;
                }
            }
            Some((last_ts, _)) if *last_ts == ts_ms => {}
            _ => merged_samples.push((ts_ms, value)),
        }
    }
    *samples = merged_samples;
    Ok(())
}

fn divide_range_query_results(
    sums: QueryResult,
    counts: QueryResult,
) -> Result<QueryResult, PromqlError> {
    let QueryResult::RangeMatrix(sum_series) = sums else {
        return Err(PromqlError::Plan(
            "avg query-frontend sum merge requires range matrix results".into(),
        ));
    };
    let QueryResult::RangeMatrix(count_series) = counts else {
        return Err(PromqlError::Plan(
            "avg query-frontend count merge requires range matrix results".into(),
        ));
    };
    let counts_by_fp = count_series
        .into_iter()
        .map(|series| (series.labels.fingerprint(), series))
        .collect::<BTreeMap<_, _>>();
    let mut avg_series = Vec::new();

    for series in sum_series {
        let Some(count_series) = counts_by_fp.get(&series.labels.fingerprint()) else {
            continue;
        };
        let counts_by_ts = count_series
            .samples
            .iter()
            .filter_map(|(ts_ms, value)| match value {
                SampleValue::Float(value) => Some((*ts_ms, *value)),
                SampleValue::Histogram(_) => None,
            })
            .collect::<BTreeMap<_, _>>();
        let samples = series
            .samples
            .into_iter()
            .filter_map(|(ts_ms, value)| {
                let count = *counts_by_ts.get(&ts_ms)?;
                if count == 0.0 {
                    return None;
                }
                Some((
                    ts_ms,
                    match value {
                        SampleValue::Float(value) => SampleValue::Float(value / count),
                        SampleValue::Histogram(histogram) => {
                            SampleValue::Histogram(scaled_native_histogram(&histogram, 1.0 / count))
                        }
                    },
                ))
            })
            .collect::<Vec<_>>();
        if !samples.is_empty() {
            avg_series.push(RangeSeries {
                labels: series.labels,
                samples,
            });
        }
    }

    Ok(QueryResult::RangeMatrix(avg_series))
}

fn reduce_moment_range_query_results(
    sums: QueryResult,
    counts: QueryResult,
    sum_squares: QueryResult,
    kind: MomentReduction,
) -> Result<QueryResult, PromqlError> {
    let QueryResult::RangeMatrix(sum_series) = sums else {
        return Err(PromqlError::Plan(
            "moment query-frontend sum merge requires range matrix results".into(),
        ));
    };
    let QueryResult::RangeMatrix(count_series) = counts else {
        return Err(PromqlError::Plan(
            "moment query-frontend count merge requires range matrix results".into(),
        ));
    };
    let QueryResult::RangeMatrix(sum_squares_series) = sum_squares else {
        return Err(PromqlError::Plan(
            "moment query-frontend sum-squares merge requires range matrix results".into(),
        ));
    };
    let counts_by_fp = float_samples_by_fingerprint(count_series);
    let sum_squares_by_fp = float_samples_by_fingerprint(sum_squares_series);
    let mut out_series = Vec::new();

    for series in sum_series {
        let fingerprint = series.labels.fingerprint();
        let Some(counts_by_ts) = counts_by_fp.get(&fingerprint) else {
            continue;
        };
        let Some(sum_squares_by_ts) = sum_squares_by_fp.get(&fingerprint) else {
            continue;
        };
        let samples = series
            .samples
            .into_iter()
            .filter_map(|(ts_ms, value)| {
                let SampleValue::Float(sum) = value else {
                    return None;
                };
                let count = *counts_by_ts.get(&ts_ms)?;
                let sum_squares = *sum_squares_by_ts.get(&ts_ms)?;
                if count == 0.0 {
                    return None;
                }
                let mean = sum / count;
                let variance = ((sum_squares / count) - (mean * mean)).max(0.0);
                let value = match kind {
                    MomentReduction::Stddev => variance.sqrt(),
                    MomentReduction::Stdvar => variance,
                };
                Some((ts_ms, SampleValue::Float(value)))
            })
            .collect::<Vec<_>>();
        if !samples.is_empty() {
            out_series.push(RangeSeries {
                labels: series.labels,
                samples,
            });
        }
    }

    Ok(QueryResult::RangeMatrix(out_series))
}

fn float_samples_by_fingerprint(
    series: Vec<RangeSeries>,
) -> BTreeMap<SeriesFingerprint, BTreeMap<i64, f64>> {
    series
        .into_iter()
        .map(|series| {
            (
                series.labels.fingerprint(),
                series
                    .samples
                    .into_iter()
                    .filter_map(|(ts_ms, value)| match value {
                        SampleValue::Float(value) => Some((ts_ms, value)),
                        SampleValue::Histogram(_) => None,
                    })
                    .collect::<BTreeMap<_, _>>(),
            )
        })
        .collect()
}

fn reduce_rank_range_query_results(
    result: QueryResult,
    k: usize,
    kind: RankReduction,
    modifier: Option<&LabelModifier>,
) -> Result<QueryResult, PromqlError> {
    let QueryResult::RangeMatrix(mut series) = result else {
        return Err(PromqlError::Plan(
            "rank query-frontend merge requires range matrix results".into(),
        ));
    };
    if k == 0 {
        return Ok(QueryResult::RangeMatrix(Vec::new()));
    }

    let mut keep = BTreeSet::<(SeriesFingerprint, usize)>::new();
    let mut candidates_by_step_and_group = BTreeMap::<(i64, String), Vec<RankCandidate>>::new();
    for (series_index, series) in series.iter().enumerate() {
        let group = label_sort_key(&aggregate_labels(&series.labels, modifier));
        let labels_key = label_sort_key(&series.labels);
        let fingerprint = series.labels.fingerprint();
        for (sample_index, (ts_ms, value)) in series.samples.iter().enumerate() {
            if let SampleValue::Float(value) = value {
                candidates_by_step_and_group
                    .entry((*ts_ms, group.clone()))
                    .or_default()
                    .push(RankCandidate {
                        fingerprint,
                        labels_key: labels_key.clone(),
                        sample_index,
                        series_index,
                        value: *value,
                    });
            }
        }
    }

    for mut candidates in candidates_by_step_and_group.into_values() {
        candidates.sort_by(|left, right| compare_rank_candidates(kind, left, right));
        candidates.truncate(k.min(candidates.len()));
        keep.extend(
            candidates
                .into_iter()
                .map(|candidate| (candidate.fingerprint, candidate.sample_index)),
        );
    }

    for series in &mut series {
        let fingerprint = series.labels.fingerprint();
        let mut sample_index = 0_usize;
        series.samples.retain(|_| {
            let keep_sample = keep.contains(&(fingerprint, sample_index));
            sample_index += 1;
            keep_sample
        });
    }
    series.retain(|series| !series.samples.is_empty());
    series.sort_by_key(|series| label_sort_key(&series.labels));
    Ok(QueryResult::RangeMatrix(series))
}

#[derive(Clone)]
struct RankCandidate {
    fingerprint: SeriesFingerprint,
    labels_key: String,
    sample_index: usize,
    series_index: usize,
    value: f64,
}

fn compare_rank_candidates(
    kind: RankReduction,
    left: &RankCandidate,
    right: &RankCandidate,
) -> std::cmp::Ordering {
    let by_value = match kind {
        RankReduction::Top => right.value.total_cmp(&left.value),
        RankReduction::Bottom => left.value.total_cmp(&right.value),
    };
    by_value
        .then_with(|| left.labels_key.cmp(&right.labels_key))
        .then_with(|| left.series_index.cmp(&right.series_index))
        .then_with(|| left.sample_index.cmp(&right.sample_index))
}

fn aggregate_labels(input: &Labels, modifier: Option<&LabelModifier>) -> Labels {
    let mut labels = Labels::new();
    match modifier {
        Some(LabelModifier::Include(include)) => {
            for name in &include.labels {
                if name == "__name__" {
                    continue;
                }
                if let Some(value) = input.get(name) {
                    labels.insert(name, value);
                }
            }
        }
        Some(LabelModifier::Exclude(exclude)) => {
            let excluded = exclude.labels.iter().collect::<BTreeSet<_>>();
            for (name, value) in input.iter() {
                if name == "__name__" || excluded.contains(name) {
                    continue;
                }
                labels.insert(name, value);
            }
        }
        None => {}
    }
    labels
}

fn add_compatible_native_histogram(
    left: &mut NativeHistogram,
    right: &NativeHistogram,
) -> Result<(), PromqlError> {
    if !native_histograms_are_compatible(left, right) {
        return Err(PromqlError::Unsupported(
            "incompatible native histogram query-frontend merge is not implemented yet".to_string(),
        ));
    }

    left.zero_count += right.zero_count;
    left.count += right.count;
    left.sum += right.sum;
    (left.positive_spans, left.positive_counts) = add_spanned_histogram_counts(
        &left.positive_spans,
        &left.positive_counts,
        &right.positive_spans,
        &right.positive_counts,
    );
    (left.negative_spans, left.negative_counts) = add_spanned_histogram_counts(
        &left.negative_spans,
        &left.negative_counts,
        &right.negative_spans,
        &right.negative_counts,
    );
    left.start_timestamp_ms = match (left.start_timestamp_ms, right.start_timestamp_ms) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    };
    Ok(())
}

fn native_histograms_are_compatible(left: &NativeHistogram, right: &NativeHistogram) -> bool {
    left.schema == right.schema
        && left.is_float == right.is_float
        && left.reset_hint == right.reset_hint
        && left.zero_threshold.to_bits() == right.zero_threshold.to_bits()
        && left.custom_values == right.custom_values
}

fn add_spanned_histogram_counts(
    left_spans: &[BucketSpan],
    left_counts: &[f64],
    right_spans: &[BucketSpan],
    right_counts: &[f64],
) -> (Vec<BucketSpan>, Vec<f64>) {
    let mut buckets = spanned_histogram_counts(left_spans, left_counts);
    for (index, count) in spanned_histogram_counts(right_spans, right_counts) {
        *buckets.entry(index).or_insert(0.0) += count;
    }
    compact_spanned_histogram_counts(buckets)
}

fn spanned_histogram_counts(spans: &[BucketSpan], counts: &[f64]) -> BTreeMap<i32, f64> {
    let mut buckets = BTreeMap::new();
    let mut index = 0_i32;
    let mut count_index = 0_usize;
    for (span_index, span) in spans.iter().enumerate() {
        if span_index == 0 {
            index = span.offset;
        } else {
            index += span.offset;
        }
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                return buckets;
            };
            buckets.insert(index, count);
            index += 1;
            count_index += 1;
        }
    }
    buckets
}

fn compact_spanned_histogram_counts(buckets: BTreeMap<i32, f64>) -> (Vec<BucketSpan>, Vec<f64>) {
    let buckets = buckets
        .into_iter()
        .filter(|(_, count)| *count != 0.0)
        .collect::<Vec<_>>();
    let mut spans = Vec::new();
    let mut counts = Vec::with_capacity(buckets.len());
    let mut span_start = None;
    let mut previous_index = 0_i32;
    let mut previous_span_end = 0_i32;
    for (index, count) in buckets {
        if span_start.is_none() {
            span_start = Some(index);
        } else if index != previous_index + 1 {
            let start = span_start.expect("checked is_some");
            spans.push(BucketSpan {
                offset: start - previous_span_end,
                length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
            });
            previous_span_end = previous_index + 1;
            span_start = Some(index);
        }
        counts.push(count);
        previous_index = index;
    }
    if let Some(start) = span_start {
        spans.push(BucketSpan {
            offset: start - previous_span_end,
            length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
        });
    }
    (spans, counts)
}

fn scaled_native_histogram(histogram: &NativeHistogram, factor: f64) -> NativeHistogram {
    let mut out = histogram.clone();
    out.zero_count *= factor;
    out.count *= factor;
    out.sum *= factor;
    for count in &mut out.positive_counts {
        *count *= factor;
    }
    for count in &mut out.negative_counts {
        *count *= factor;
    }
    out
}

fn query_with_shard_selector(query: &str, shard: QueryShard) -> Result<String, PromqlError> {
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

fn label_sort_key(labels: &Labels) -> String {
    labels.iter().fold(String::new(), |mut out, (name, value)| {
        let _ = writeln!(out, "{name}={value}");
        out
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use crabka_blockstore::{Labels, MatchOp};
    use crabka_metrics::{BucketSpan, ResetHint};

    use crate::{
        EngineOpts, InMemoryMetricStore, PromqlEngine, QueryResult, RangeSeries, SampleValue,
    };

    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert(*name, *value);
        }
        labels
    }

    /// Deterministic clock whose epoch-millis reading the test can advance, so
    /// TTL expiry can be exercised without sleeping.
    #[derive(Default)]
    struct ManualClock {
        now_ms: std::sync::atomic::AtomicI64,
    }

    impl ManualClock {
        fn new(now_ms: i64) -> Self {
            Self {
                now_ms: std::sync::atomic::AtomicI64::new(now_ms),
            }
        }

        fn advance(&self, delta_ms: i64) {
            self.now_ms
                .fetch_add(delta_ms, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl Clock for ManualClock {
        fn now_epoch_millis(&self) -> i64 {
            self.now_ms.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    fn native_histogram_with_positive_buckets(
        count: f64,
        sum: f64,
        positive_spans: Vec<BucketSpan>,
        positive_counts: Vec<f64>,
    ) -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum,
            positive_spans,
            positive_counts,
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        }
    }

    #[test]
    fn range_query_plan_splits_on_step_grid_without_duplicate_steps() {
        let plan = plan_range_query(
            "rate(http_requests_total[5m])",
            0,
            250_000,
            60_000,
            QueryFrontendOptions {
                split_interval_ms: 120_000,
                shard_count: 1,
            },
        )
        .unwrap();

        let ranges = plan
            .iter()
            .map(|subquery| (subquery.start_ms, subquery.end_ms, subquery.shard))
            .collect::<Vec<_>>();

        // Eval points 0, 60k, 120k, 180k, 240k bucket into absolute
        // split-interval windows [0,120k), [120k,240k), [240k,360k); each
        // sub-range spans the eval points landing in its absolute window with
        // no duplicate step across sub-ranges.
        assert_eq!(
            ranges,
            vec![
                (0, 60_000, None),
                (120_000, 180_000, None),
                (240_000, 240_000, None),
            ]
        );
    }

    #[test]
    fn range_query_plan_rejects_resolution_over_point_cap() {
        // (end-start)/step = 20_000 / 1 = 20_000 > 11_000: the frontend planner
        // must reject before expanding into ~20k per-step sub-queries, matching
        // Prometheus's unconditional resolution front-gate.
        let error = plan_range_query(
            "up",
            0,
            20_000,
            1,
            QueryFrontendOptions {
                split_interval_ms: 120_000,
                shard_count: 1,
            },
        )
        .unwrap_err();

        match error {
            PromqlError::Plan(message) => assert_eq!(
                message,
                "exceeded maximum resolution of 11,000 points per timeseries. \
                 Try decreasing the query resolution (?step=XX)"
            ),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn range_query_plan_allows_resolution_at_point_cap_boundary() {
        // (end-start)/step = 11_000 / 1 = 11_000 is allowed (cap is exclusive,
        // matching Prometheus's `> 11000`).
        let plan = plan_range_query(
            "up",
            0,
            11_000,
            1,
            QueryFrontendOptions {
                split_interval_ms: 120_000,
                shard_count: 1,
            },
        )
        .unwrap();
        assert!(!plan.is_empty());
    }

    #[test]
    fn range_query_plan_aligns_subranges_to_absolute_split_grid() {
        // A window that does not start on a split-interval multiple still
        // produces sub-ranges whose interior boundaries sit on the absolute
        // grid (multiples of split_interval), not relative to start_ms.
        let plan = plan_range_query(
            "up",
            60_000,
            300_000,
            60_000,
            QueryFrontendOptions {
                split_interval_ms: 120_000,
                shard_count: 1,
            },
        )
        .unwrap();

        let ranges = plan
            .iter()
            .map(|subquery| (subquery.start_ms, subquery.end_ms))
            .collect::<Vec<_>>();

        // Eval points 60k | 120k,180k | 240k,300k bucket into [0,120k),
        // [120k,240k), [240k,360k).
        assert_eq!(
            ranges,
            vec![(60_000, 60_000), (120_000, 180_000), (240_000, 300_000)]
        );
    }

    #[tokio::test]
    async fn moving_window_reuses_cached_subranges() {
        let opts = QueryFrontendOptions {
            split_interval_ms: 120_000,
            shard_count: 1,
        };
        let cache = QueryFrontendCache::default();
        let executor = RecordingExecutor::default();

        // First query window [0, 360_000].
        execute_range_query_frontend(
            &executor,
            &cache,
            &FrontendRangeRequest {
                tenant: "tenant-a".into(),
                query: "up".into(),
                start_ms: 0,
                end_ms: 360_000,
                step_ms: 60_000,
                opts,
            },
        )
        .await
        .unwrap();

        let first_fresh = executor
            .calls
            .lock()
            .expect("recording executor calls poisoned")
            .len();
        // Absolute buckets: [0,120k)->[0,60k], [120k,240k)->[120k,180k],
        // [240k,360k)->[240k,300k], [360k,480k)->[360k,360k] => 4 sub-queries.
        assert_eq!(first_fresh, 4);

        // Second window shifted by one step (60_000 < split 120_000) and the
        // same step phase, so the absolute-aligned interior buckets
        // [120k,240k) and [240k,360k) reproduce identical sub-ranges that are
        // already cached.
        execute_range_query_frontend(
            &executor,
            &cache,
            &FrontendRangeRequest {
                tenant: "tenant-a".into(),
                query: "up".into(),
                start_ms: 60_000,
                end_ms: 420_000,
                step_ms: 60_000,
                opts,
            },
        )
        .await
        .unwrap();

        let all_calls = executor
            .calls
            .lock()
            .expect("recording executor calls poisoned")
            .clone();
        let second_fresh = all_calls.len() - first_fresh;

        // Second window sub-ranges: [60k,60k] | [120k,180k]* | [240k,300k]* |
        // [360k,420k]. The two starred interior sub-ranges hit the cache, so
        // only the two non-cached sub-ranges execute fresh.
        assert_eq!(second_fresh, 2);
        let second_starts = all_calls[first_fresh..]
            .iter()
            .map(|query| (query.start_ms, query.end_ms))
            .collect::<Vec<_>>();
        assert_eq!(second_starts, vec![(60_000, 60_000), (360_000, 420_000)]);
    }

    #[test]
    fn range_query_plan_expands_each_split_across_mimir_query_shards() {
        let plan = plan_range_query(
            "sum(rate(http_requests_total[5m]))",
            0,
            60_000,
            60_000,
            QueryFrontendOptions {
                split_interval_ms: 120_000,
                shard_count: 3,
            },
        )
        .unwrap();

        let shard_values = plan
            .iter()
            .map(|subquery| subquery.shard_matcher().expect("sharded subquery").value)
            .collect::<Vec<_>>();

        assert_eq!(shard_values, vec!["1_of_3", "2_of_3", "3_of_3"]);
        assert!(
            plan.iter()
                .all(|subquery| subquery.start_ms == 0 && subquery.end_ms == 60_000)
        );

        let matcher = plan[0].shard_matcher().expect("first shard matcher");
        assert_eq!(matcher.name, "__query_shard__");
        assert_eq!(matcher.op, MatchOp::Eq);
    }

    #[test]
    fn range_query_plan_shards_avg_for_partial_sum_count_reduction() {
        let plan = plan_range_query(
            "avg(up)",
            0,
            60_000,
            60_000,
            QueryFrontendOptions {
                split_interval_ms: 120_000,
                shard_count: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.len(), 3);
        assert!(plan.iter().all(|subquery| subquery.shard.is_some()));
    }

    #[test]
    fn range_query_plan_shards_stddev_and_stdvar_for_moment_reduction() {
        for query in ["stddev(up)", "stdvar(up)"] {
            let plan = plan_range_query(
                query,
                0,
                60_000,
                60_000,
                QueryFrontendOptions {
                    split_interval_ms: 120_000,
                    shard_count: 3,
                },
            )
            .unwrap();

            assert_eq!(plan.len(), 3, "{query}");
            assert!(
                plan.iter().all(|subquery| subquery.shard.is_some()),
                "{query}"
            );
        }
    }

    #[test]
    fn range_query_plan_skips_shards_for_unsupported_aggregate_reducers() {
        let plan = plan_range_query(
            "quantile(0.9, up)",
            0,
            60_000,
            60_000,
            QueryFrontendOptions {
                split_interval_ms: 120_000,
                shard_count: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].shard, None);
    }

    #[test]
    fn range_query_plan_skips_nested_avg_until_rewrite_is_aggregate_aware() {
        let plan = plan_range_query(
            "avg(sum by (job)(up))",
            0,
            60_000,
            60_000,
            QueryFrontendOptions {
                split_interval_ms: 120_000,
                shard_count: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].shard, None);
    }

    #[test]
    fn range_query_plan_shards_min_and_max_aggregate_reducers() {
        for query in ["min(up)", "max(up)"] {
            let plan = plan_range_query(
                query,
                0,
                60_000,
                60_000,
                QueryFrontendOptions {
                    split_interval_ms: 120_000,
                    shard_count: 3,
                },
            )
            .unwrap();

            assert_eq!(plan.len(), 3, "{query}");
            assert!(
                plan.iter().all(|subquery| subquery.shard.is_some()),
                "{query}"
            );
        }
    }

    #[test]
    fn range_query_plan_shards_group_aggregate_reducer() {
        let plan = plan_range_query(
            "group(up)",
            0,
            60_000,
            60_000,
            QueryFrontendOptions {
                split_interval_ms: 120_000,
                shard_count: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.len(), 3);
        assert!(plan.iter().all(|subquery| subquery.shard.is_some()));
    }

    #[test]
    fn range_query_plan_shards_topk_and_bottomk_for_final_rank_reduction() {
        for query in ["topk(2, up)", "bottomk(2, up)"] {
            let plan = plan_range_query(
                query,
                0,
                60_000,
                60_000,
                QueryFrontendOptions {
                    split_interval_ms: 120_000,
                    shard_count: 3,
                },
            )
            .unwrap();

            assert_eq!(plan.len(), 3, "{query}");
            assert!(
                plan.iter().all(|subquery| subquery.shard.is_some()),
                "{query}"
            );
        }
    }

    #[test]
    fn shard_query_injection_adds_mimir_selector_to_vector_and_matrix_selectors() {
        let rewritten = query_with_shard_selector(
            r#"sum(rate(http_requests_total{job="api"}[5m])) + up"#,
            QueryShard { index: 1, total: 2 },
        )
        .unwrap();

        for needle in [
            r#"__query_shard__="1_of_2""#,
            r#"job="api""#,
            "http_requests_total",
            r#"up{__query_shard__="1_of_2"}"#,
        ] {
            assert!(
                rewritten.contains(needle),
                "missing {needle} in {rewritten}"
            );
        }
    }

    #[test]
    fn range_query_merge_combines_time_split_samples_for_same_series() {
        let api_labels = labels(&[("__name__", "up"), ("job", "api")]);
        let worker_labels = labels(&[("__name__", "up"), ("job", "worker")]);
        let result = merge_range_query_results(vec![
            QueryResult::RangeMatrix(vec![
                RangeSeries {
                    labels: api_labels.clone(),
                    samples: vec![(60_000, SampleValue::Float(2.0))],
                },
                RangeSeries {
                    labels: worker_labels.clone(),
                    samples: vec![(0, SampleValue::Float(3.0))],
                },
            ]),
            QueryResult::RangeMatrix(vec![RangeSeries {
                labels: api_labels.clone(),
                samples: vec![
                    (0, SampleValue::Float(1.0)),
                    (120_000, SampleValue::Float(4.0)),
                ],
            }]),
        ])
        .unwrap();

        assert_eq!(
            result,
            QueryResult::RangeMatrix(vec![
                RangeSeries {
                    labels: api_labels,
                    samples: vec![
                        (0, SampleValue::Float(1.0)),
                        (60_000, SampleValue::Float(2.0)),
                        (120_000, SampleValue::Float(4.0)),
                    ],
                },
                RangeSeries {
                    labels: worker_labels,
                    samples: vec![(0, SampleValue::Float(3.0))],
                },
            ])
        );
    }

    #[test]
    fn range_query_merge_sums_sharded_partial_float_samples_for_same_series() {
        let labels = labels(&[]);
        let result = merge_range_query_results(vec![
            QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels.clone(),
                samples: vec![
                    (0, SampleValue::Float(1.0)),
                    (60_000, SampleValue::Float(2.0)),
                ],
            }]),
            QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels.clone(),
                samples: vec![
                    (0, SampleValue::Float(10.0)),
                    (60_000, SampleValue::Float(20.0)),
                ],
            }]),
        ])
        .unwrap();

        assert_eq!(
            result,
            QueryResult::RangeMatrix(vec![RangeSeries {
                labels,
                samples: vec![
                    (0, SampleValue::Float(11.0)),
                    (60_000, SampleValue::Float(22.0)),
                ],
            }])
        );
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn range_query_merge_sums_native_histograms_with_different_span_layouts() {
        let labels = labels(&[]);
        let result = merge_range_query_results(vec![
            QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels.clone(),
                samples: vec![(
                    0,
                    SampleValue::Histogram(native_histogram_with_positive_buckets(
                        3.0,
                        9.0,
                        vec![BucketSpan {
                            offset: 0,
                            length: 2,
                        }],
                        vec![1.0, 2.0],
                    )),
                )],
            }]),
            QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels.clone(),
                samples: vec![(
                    0,
                    SampleValue::Histogram(native_histogram_with_positive_buckets(
                        7.0,
                        21.0,
                        vec![BucketSpan {
                            offset: 1,
                            length: 2,
                        }],
                        vec![3.0, 4.0],
                    )),
                )],
            }]),
        ])
        .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected range matrix");
        };
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].labels, labels);
        let SampleValue::Histogram(histogram) = &series[0].samples[0].1 else {
            panic!("expected histogram sample");
        };
        assert_eq!(histogram.count, 10.0);
        assert_eq!(histogram.sum, 30.0);
        assert_eq!(
            histogram.positive_spans,
            vec![BucketSpan {
                offset: 0,
                length: 3,
            }]
        );
        assert_eq!(histogram.positive_counts, vec![1.0, 5.0, 4.0]);
    }

    #[test]
    fn range_query_merge_rejects_non_matrix_subquery_results() {
        let err = merge_range_query_results(vec![QueryResult::Scalar {
            ts_ms: 0,
            value: 1.0,
        }])
        .unwrap_err();

        assert!(format!("{err}").contains("range matrix"));
    }

    #[test]
    fn range_result_cache_is_scoped_by_tenant_query_range_step_and_shard() {
        let cache = QueryFrontendCache::default();
        let query = FrontendRangeQuery {
            query: "up".into(),
            start_ms: 0,
            end_ms: 60_000,
            step_ms: 60_000,
            shard: Some(QueryShard { index: 1, total: 2 }),
        };
        let result = QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up"), ("job", "api")]),
            samples: vec![(0, SampleValue::Float(1.0))],
        }]);

        cache.insert("tenant-a", &query, result.clone());

        assert_eq!(cache.get("tenant-a", &query), Some(result));
        assert_eq!(cache.get("tenant-b", &query), None);

        let other_shard = FrontendRangeQuery {
            shard: Some(QueryShard { index: 2, total: 2 }),
            ..query
        };
        assert_eq!(cache.get("tenant-a", &other_shard), None);
    }

    #[test]
    fn range_result_cache_returns_owned_results() {
        let cache = QueryFrontendCache::default();
        let query = FrontendRangeQuery {
            query: "up".into(),
            start_ms: 0,
            end_ms: 0,
            step_ms: 60_000,
            shard: None,
        };
        let result = QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up")]),
            samples: vec![(0, SampleValue::Float(1.0))],
        }]);

        cache.insert("tenant-a", &query, result);
        let Some(QueryResult::RangeMatrix(mut first_hit)) = cache.get("tenant-a", &query) else {
            panic!("cached range matrix");
        };
        first_hit[0].samples.clear();

        let Some(QueryResult::RangeMatrix(second_hit)) = cache.get("tenant-a", &query) else {
            panic!("cached range matrix");
        };
        assert_eq!(second_hit[0].samples, vec![(0, SampleValue::Float(1.0))]);
    }

    #[test]
    fn in_memory_round_trips_then_expires() {
        let clock = Arc::new(ManualClock::new(1_000_000));
        let cache = QueryFrontendCache::with_ttl(std::time::Duration::from_secs(90))
            .with_clock(clock.clone());
        let query = FrontendRangeQuery {
            query: "up".into(),
            start_ms: 0,
            end_ms: 0,
            step_ms: 60_000,
            shard: None,
        };
        let result = QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up")]),
            samples: vec![(0, SampleValue::Float(1.0))],
        }]);

        cache.insert("tenant-a", &query, result.clone());

        // Within the TTL window: hit.
        clock.advance(89_000);
        assert_eq!(cache.get("tenant-a", &query), Some(result));

        // One step past the TTL: miss, and the entry is evicted.
        clock.advance(2_000);
        assert_eq!(cache.get("tenant-a", &query), None);
        assert_eq!(
            cache
                .range_results
                .lock()
                .expect("query frontend cache poisoned")
                .len(),
            0,
            "expired entry must be evicted on miss"
        );
    }

    #[test]
    fn in_memory_without_ttl_never_expires() {
        let clock = Arc::new(ManualClock::new(0));
        let cache = QueryFrontendCache::default().with_clock(clock.clone());
        let query = FrontendRangeQuery {
            query: "up".into(),
            start_ms: 0,
            end_ms: 0,
            step_ms: 60_000,
            shard: None,
        };
        let result = QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up")]),
            samples: vec![(0, SampleValue::Float(1.0))],
        }]);

        cache.insert("tenant-a", &query, result.clone());
        clock.advance(i64::from(u32::MAX));
        assert_eq!(cache.get("tenant-a", &query), Some(result));
    }

    #[tokio::test]
    async fn object_store_range_result_cache_expires_stale_objects() {
        let object_store = std::sync::Arc::new(object_store::memory::InMemory::new());
        let clock = Arc::new(ManualClock::new(5_000_000));
        let cache = ObjectStoreQueryFrontendCache::new(object_store, "query-cache".to_string())
            .with_ttl(std::time::Duration::from_secs(30))
            .with_clock(clock.clone());
        let query = FrontendRangeQuery {
            query: "up".into(),
            start_ms: 0,
            end_ms: 0,
            step_ms: 60_000,
            shard: None,
        };
        let result = QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up")]),
            samples: vec![(0, SampleValue::Float(1.0))],
        }]);

        cache
            .insert("tenant-a", &query, result.clone())
            .await
            .unwrap();

        // Within TTL: hit.
        clock.advance(29_000);
        assert_eq!(cache.get("tenant-a", &query).await.unwrap(), Some(result));

        // Past TTL: miss.
        clock.advance(2_000);
        assert_eq!(cache.get("tenant-a", &query).await.unwrap(), None);
    }

    #[tokio::test]
    async fn object_store_range_result_cache_persists_across_instances() {
        let object_store = std::sync::Arc::new(object_store::memory::InMemory::new());
        let first =
            ObjectStoreQueryFrontendCache::new(object_store.clone(), "query-cache".to_string());
        let second = ObjectStoreQueryFrontendCache::new(object_store, "query-cache".to_string());
        let query = FrontendRangeQuery {
            query: "up".into(),
            start_ms: 0,
            end_ms: 0,
            step_ms: 60_000,
            shard: Some(QueryShard { index: 1, total: 2 }),
        };
        let result = QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up"), ("job", "api")]),
            samples: vec![(0, SampleValue::Float(1.0))],
        }]);

        first
            .insert("tenant-a", &query, result.clone())
            .await
            .unwrap();

        assert_eq!(second.get("tenant-a", &query).await.unwrap(), Some(result));
    }

    #[derive(Default)]
    struct RecordingExecutor {
        calls: Mutex<Vec<FrontendRangeQuery>>,
    }

    #[async_trait]
    impl RangeQueryExecutor for RecordingExecutor {
        async fn execute_range_query(
            &self,
            _tenant: &str,
            query: &FrontendRangeQuery,
        ) -> Result<QueryResult, PromqlError> {
            self.calls
                .lock()
                .expect("recording executor calls poisoned")
                .push(query.clone());
            Ok(QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels(&[("__name__", "up"), ("job", "api")]),
                samples: vec![(query.start_ms, SampleValue::Float(120_000.0))],
            }]))
        }
    }

    #[derive(Default)]
    struct AvgPartialRecordingExecutor {
        calls: Mutex<Vec<FrontendRangeQuery>>,
    }

    #[async_trait]
    impl RangeQueryExecutor for AvgPartialRecordingExecutor {
        #[allow(clippy::match_same_arms)]
        async fn execute_range_query(
            &self,
            _tenant: &str,
            query: &FrontendRangeQuery,
        ) -> Result<QueryResult, PromqlError> {
            self.calls
                .lock()
                .expect("avg partial executor calls poisoned")
                .push(query.clone());

            let shard = query.shard.expect("avg partial query shard");
            let value = match (query.query.as_str(), shard.index) {
                ("sum(up)", 1) => 2.0,
                ("sum(up)", 2) => 10.0,
                ("count(up)", 1) => 1.0,
                ("count(up)", 2) => 2.0,
                _ => {
                    return Err(PromqlError::Plan(format!(
                        "unexpected avg partial query: {query:?}"
                    )));
                }
            };
            Ok(QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels(&[]),
                samples: vec![(query.start_ms, SampleValue::Float(value))],
            }]))
        }
    }

    #[derive(Default)]
    struct MomentPartialRecordingExecutor {
        calls: Mutex<Vec<FrontendRangeQuery>>,
    }

    #[async_trait]
    impl RangeQueryExecutor for MomentPartialRecordingExecutor {
        async fn execute_range_query(
            &self,
            _tenant: &str,
            query: &FrontendRangeQuery,
        ) -> Result<QueryResult, PromqlError> {
            self.calls
                .lock()
                .expect("moment partial executor calls poisoned")
                .push(query.clone());

            let shard = query.shard.expect("moment partial query shard");
            let value = match (query.query.as_str(), shard.index) {
                ("sum(up)", 1) => 12.0,
                ("sum(up)", 2) => 3.0,
                ("count(up)", 1) => 2.0,
                ("count(up)", 2) => 1.0,
                ("sum((up) * (up))", 1) => 104.0,
                ("sum((up) * (up))", 2) => 9.0,
                _ => {
                    return Err(PromqlError::Plan(format!(
                        "unexpected moment partial query: {query:?}"
                    )));
                }
            };
            Ok(QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels(&[]),
                samples: vec![(query.start_ms, SampleValue::Float(value))],
            }]))
        }
    }

    #[derive(Default)]
    struct RankRecordingExecutor {
        calls: Mutex<Vec<FrontendRangeQuery>>,
    }

    #[async_trait]
    impl RangeQueryExecutor for RankRecordingExecutor {
        async fn execute_range_query(
            &self,
            _tenant: &str,
            query: &FrontendRangeQuery,
        ) -> Result<QueryResult, PromqlError> {
            self.calls
                .lock()
                .expect("rank executor calls poisoned")
                .push(query.clone());

            let shard = query.shard.expect("rank query shard");
            let series = match shard.index {
                1 => vec![
                    RangeSeries {
                        labels: labels(&[("__name__", "up"), ("series", "a")]),
                        samples: vec![(0, SampleValue::Float(10.0))],
                    },
                    RangeSeries {
                        labels: labels(&[("__name__", "up"), ("series", "b")]),
                        samples: vec![(0, SampleValue::Float(2.0))],
                    },
                ],
                2 => vec![
                    RangeSeries {
                        labels: labels(&[("__name__", "up"), ("series", "c")]),
                        samples: vec![(0, SampleValue::Float(9.0))],
                    },
                    RangeSeries {
                        labels: labels(&[("__name__", "up"), ("series", "d")]),
                        samples: vec![(0, SampleValue::Float(8.0))],
                    },
                ],
                _ => Vec::new(),
            };
            Ok(QueryResult::RangeMatrix(series))
        }
    }

    #[tokio::test]
    async fn frontend_range_execution_reduces_sharded_topk_from_rank_candidates() {
        let cache = QueryFrontendCache::default();
        let executor = RankRecordingExecutor::default();

        let result = execute_range_query_frontend(
            &executor,
            &cache,
            &FrontendRangeRequest {
                tenant: "tenant-a".into(),
                query: "topk(2, up)".into(),
                start_ms: 0,
                end_ms: 0,
                step_ms: 60_000,
                opts: QueryFrontendOptions {
                    split_interval_ms: 60_000,
                    shard_count: 2,
                },
            },
        )
        .await
        .unwrap();

        let calls = executor
            .calls
            .lock()
            .expect("rank executor calls poisoned")
            .clone();
        assert_eq!(
            calls
                .iter()
                .map(|query| (query.query.as_str(), query.shard))
                .collect::<Vec<_>>(),
            vec![
                ("topk(2, up)", Some(QueryShard { index: 1, total: 2 })),
                ("topk(2, up)", Some(QueryShard { index: 2, total: 2 })),
            ]
        );
        let QueryResult::RangeMatrix(series) = result else {
            panic!("topk range matrix");
        };
        let selected = series
            .iter()
            .map(|series| {
                let SampleValue::Float(value) = series.samples[0].1 else {
                    panic!("topk float sample");
                };
                (series.labels.get("series").unwrap().to_string(), value)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            selected,
            vec![("a".to_string(), 10.0), ("c".to_string(), 9.0)]
        );
    }

    #[tokio::test]
    async fn frontend_range_execution_reduces_sharded_stdvar_from_moment_partials() {
        let cache = QueryFrontendCache::default();
        let executor = MomentPartialRecordingExecutor::default();

        let result = execute_range_query_frontend(
            &executor,
            &cache,
            &FrontendRangeRequest {
                tenant: "tenant-a".into(),
                query: "stdvar(up)".into(),
                start_ms: 0,
                end_ms: 0,
                step_ms: 60_000,
                opts: QueryFrontendOptions {
                    split_interval_ms: 60_000,
                    shard_count: 2,
                },
            },
        )
        .await
        .unwrap();

        let calls = executor
            .calls
            .lock()
            .expect("moment partial executor calls poisoned")
            .clone();
        assert_eq!(
            calls
                .iter()
                .map(|query| (query.query.as_str(), query.shard))
                .collect::<Vec<_>>(),
            vec![
                ("sum(up)", Some(QueryShard { index: 1, total: 2 })),
                ("sum(up)", Some(QueryShard { index: 2, total: 2 })),
                ("count(up)", Some(QueryShard { index: 1, total: 2 })),
                ("count(up)", Some(QueryShard { index: 2, total: 2 })),
                ("sum((up) * (up))", Some(QueryShard { index: 1, total: 2 }),),
                ("sum((up) * (up))", Some(QueryShard { index: 2, total: 2 }),),
            ]
        );
        let QueryResult::RangeMatrix(series) = result else {
            panic!("stdvar range matrix");
        };
        let SampleValue::Float(value) = series[0].samples[0].1 else {
            panic!("stdvar float sample");
        };
        assert!((value - (38.0 / 3.0)).abs() < 1e-9);
    }

    #[tokio::test]
    async fn frontend_range_execution_reduces_sharded_stddev_from_moment_partials() {
        let cache = QueryFrontendCache::default();
        let executor = MomentPartialRecordingExecutor::default();

        let result = execute_range_query_frontend(
            &executor,
            &cache,
            &FrontendRangeRequest {
                tenant: "tenant-a".into(),
                query: "stddev(up)".into(),
                start_ms: 0,
                end_ms: 0,
                step_ms: 60_000,
                opts: QueryFrontendOptions {
                    split_interval_ms: 60_000,
                    shard_count: 2,
                },
            },
        )
        .await
        .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("stddev range matrix");
        };
        let SampleValue::Float(value) = series[0].samples[0].1 else {
            panic!("stddev float sample");
        };
        assert!((value - (38.0_f64 / 3.0).sqrt()).abs() < 1e-9);
    }

    #[tokio::test]
    async fn frontend_range_execution_reduces_sharded_avg_from_sum_and_count_partials() {
        let cache = QueryFrontendCache::default();
        let executor = AvgPartialRecordingExecutor::default();

        let result = execute_range_query_frontend(
            &executor,
            &cache,
            &FrontendRangeRequest {
                tenant: "tenant-a".into(),
                query: "avg(up)".into(),
                start_ms: 0,
                end_ms: 0,
                step_ms: 60_000,
                opts: QueryFrontendOptions {
                    split_interval_ms: 60_000,
                    shard_count: 2,
                },
            },
        )
        .await
        .unwrap();

        let calls = executor
            .calls
            .lock()
            .expect("avg partial executor calls poisoned")
            .clone();
        assert_eq!(
            calls
                .iter()
                .map(|query| (query.query.as_str(), query.shard))
                .collect::<Vec<_>>(),
            vec![
                ("sum(up)", Some(QueryShard { index: 1, total: 2 })),
                ("sum(up)", Some(QueryShard { index: 2, total: 2 })),
                ("count(up)", Some(QueryShard { index: 1, total: 2 })),
                ("count(up)", Some(QueryShard { index: 2, total: 2 })),
            ]
        );
        assert_eq!(
            result,
            QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels(&[]),
                samples: vec![(0, SampleValue::Float(4.0))],
            }])
        );
    }

    #[tokio::test]
    async fn frontend_range_execution_uses_cache_and_merges_subquery_results() {
        let cache = QueryFrontendCache::default();
        let executor = RecordingExecutor::default();
        let cached_query = FrontendRangeQuery {
            query: "up".into(),
            start_ms: 0,
            end_ms: 60_000,
            step_ms: 60_000,
            shard: None,
        };
        cache.insert(
            "tenant-a",
            &cached_query,
            QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels(&[("__name__", "up"), ("job", "api")]),
                samples: vec![(0, SampleValue::Float(1.0))],
            }]),
        );

        let result = execute_range_query_frontend(
            &executor,
            &cache,
            &FrontendRangeRequest {
                tenant: "tenant-a".into(),
                query: "up".into(),
                start_ms: 0,
                end_ms: 180_000,
                step_ms: 60_000,
                opts: QueryFrontendOptions {
                    split_interval_ms: 120_000,
                    shard_count: 1,
                },
            },
        )
        .await
        .unwrap();

        // Absolute windows [0,120k)->[0,60k] (pre-cached) and
        // [120k,240k)->[120k,180k] (executed fresh).
        let calls = executor
            .calls
            .lock()
            .expect("recording executor calls poisoned")
            .clone();
        assert_eq!(
            calls
                .iter()
                .map(|query| (query.start_ms, query.end_ms))
                .collect::<Vec<_>>(),
            vec![(120_000, 180_000)]
        );
        assert_eq!(
            cache
                .get("tenant-a", &calls[0])
                .expect("fresh subquery cached"),
            QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels(&[("__name__", "up"), ("job", "api")]),
                samples: vec![(120_000, SampleValue::Float(120_000.0))],
            }])
        );
        assert_eq!(
            result,
            QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels(&[("__name__", "up"), ("job", "api")]),
                samples: vec![
                    (0, SampleValue::Float(1.0)),
                    (120_000, SampleValue::Float(120_000.0)),
                ],
            }])
        );
    }

    /// Executor that blocks every sub-query on a shared barrier sized to the
    /// expected fan-out width. A sequential dispatcher can never satisfy the
    /// barrier (only one sub-query is ever in flight), so the surrounding
    /// `tokio::time::timeout` trips; a concurrent dispatcher releases all N at
    /// once. The executor also records the wall-clock order in which sub-queries
    /// were admitted to prove every planned sub-query was dispatched.
    struct ConcurrencyProbeExecutor {
        barrier: tokio::sync::Barrier,
        calls: Mutex<Vec<FrontendRangeQuery>>,
    }

    impl ConcurrencyProbeExecutor {
        fn new(width: usize) -> Self {
            Self {
                barrier: tokio::sync::Barrier::new(width),
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl RangeQueryExecutor for ConcurrencyProbeExecutor {
        async fn execute_range_query(
            &self,
            _tenant: &str,
            query: &FrontendRangeQuery,
        ) -> Result<QueryResult, PromqlError> {
            self.calls
                .lock()
                .expect("probe executor calls poisoned")
                .push(query.clone());
            // All concurrently-dispatched sub-queries must reach here before any
            // can proceed. Under sequential dispatch this never completes.
            self.barrier.wait().await;
            // Each sub-query contributes a sample at a distinct timestamp
            // (its split start), so the stitched matrix is order-independent.
            Ok(QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels(&[("__name__", "up"), ("job", "api")]),
                samples: vec![(query.start_ms, SampleValue::Float(1.0))],
            }]))
        }
    }

    #[tokio::test]
    async fn frontend_range_execution_dispatches_subqueries_concurrently() {
        // 4 splits over [0, 720_000] with a 180_000 split interval and 60_000
        // step, times 1 shard => 4 independent sub-queries.
        let planned = plan_range_query(
            "up",
            0,
            720_000,
            60_000,
            QueryFrontendOptions {
                split_interval_ms: 180_000,
                shard_count: 1,
            },
        )
        .unwrap();
        let width = planned.len();
        assert!(width >= 2, "test needs multiple sub-queries, got {width}");

        let executor = ConcurrencyProbeExecutor::new(width);
        let cache = QueryFrontendCache::default();

        let results = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            execute_planned_range_queries(&executor, &cache, "tenant-a", planned.clone()),
        )
        .await
        .expect("parallel fan-out must not block on the per-subquery barrier")
        .unwrap();

        // Every planned sub-query was dispatched exactly once.
        let mut dispatched = executor
            .calls
            .lock()
            .expect("probe executor calls poisoned")
            .clone();
        dispatched.sort_by_key(|query| query.start_ms);
        let mut expected = planned.clone();
        expected.sort_by_key(|query| query.start_ms);
        assert_eq!(dispatched, expected);

        // Stitched result is identical to a deterministic sequential merge,
        // independent of completion order.
        let stitched =
            merge_range_query_results_with_reducer(results.clone(), QueryShardReducer::First)
                .unwrap();
        let mut sequential = Vec::new();
        for subquery in &planned {
            sequential.push(QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels(&[("__name__", "up"), ("job", "api")]),
                samples: vec![(subquery.start_ms, SampleValue::Float(1.0))],
            }]));
        }
        let sequential_merge =
            merge_range_query_results_with_reducer(sequential, QueryShardReducer::First).unwrap();
        assert_eq!(stitched, sequential_merge);
    }

    #[tokio::test]
    async fn frontend_range_execution_runs_against_promql_engine() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            0,
            1.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            60_000,
            2.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            120_000,
            3.0,
        );
        let engine = PromqlEngine::new(std::sync::Arc::new(store), EngineOpts::default());
        let cache = QueryFrontendCache::default();

        let result = execute_range_query_frontend(
            &engine,
            &cache,
            &FrontendRangeRequest {
                tenant: "tenant-a".into(),
                query: "up".into(),
                start_ms: 0,
                end_ms: 120_000,
                step_ms: 60_000,
                opts: QueryFrontendOptions {
                    split_interval_ms: 60_000,
                    shard_count: 1,
                },
            },
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels(&[("__name__", "up"), ("job", "api")]),
                samples: vec![
                    (0, SampleValue::Float(1.0)),
                    (60_000, SampleValue::Float(2.0)),
                    (120_000, SampleValue::Float(3.0)),
                ],
            }])
        );
    }
}
