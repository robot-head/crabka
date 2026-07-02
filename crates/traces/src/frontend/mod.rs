//! The `query-frontend` role: search sharding (block/row-group jobs + a live
//! shard), job queueing, querier fan-out, and spanSet/trace merge in front of N
//! queriers.
//!
//! The pipeline composes as
//! `plan jobs -> queue (bounded fan-out) -> per-job search -> merge (limit/spss)
//! -> render Tempo JSON`, all over typed serde structs ([`wire`]) rather than
//! raw `serde_json::Value`.

pub mod backend;
pub mod config;
pub mod http_backend;
pub mod job;
pub mod merge;
pub mod metrics_merge;
pub mod queue;
pub mod server;
pub mod wire;

pub use backend::{
    BackendError, MetricsJobRequest, MetricsPartial, MockQuerier, QuerierBackend, SearchJobRequest,
    SearchPartial, TagNamesJobRequest, TagNamesPartial, TagValuesJobRequest, TagValuesPartial,
    TraceByIdJobRequest, TracePartial,
};
pub use config::FrontendConfig;
pub use http_backend::{HttpQuerier, run_query_frontend};
pub use job::{
    BlockCatalog, BlockMetaInfo, CatalogError, JobPlan, JobShard, MockCatalog, RowGroupInfo,
    TraceIndexCatalog, blocks_for_tenant, plan_search_jobs,
};
pub use merge::{
    TraceStatus, assemble_trace, assembled_span_count, merge_search, merge_tag_names,
    merge_tag_values,
};
pub use metrics_merge::{
    Exemplar, KeyValue, MetricSample, MetricSeries, MetricsResponseJson, limit_exemplars,
    merge_metric_series, merge_metrics,
};
pub use queue::run_jobs;
pub use server::router_with_backend;
pub use wire::{
    AnyValueJson, ArrayValueJson, KeyValueJson, Metrics, OtlpSpanJson, ResourceSpansJson,
    ScopeSpansJson, SearchResponseJson, SpanJson, SpanSetJson, TraceByIdResponseJson,
    TraceEnvelopeJson, TraceJson, hex8, hex16, parse_hex8, parse_hex16,
};

use std::sync::Arc;

/// Map a block-catalog enumeration failure to a backend transport error so the
/// endpoint surfaces a 5xx instead of silently returning only live-tier results.
///
/// A search/tag query **partitions** the data across the live tier + disjoint
/// cold blocks; an empty block set from a catalog error is indistinguishable
/// from "no cold blocks", so swallowing it (`unwrap_or_default`) would drop the
/// cold partitions and return a misleading `200`. This matches the
/// partitioning-shard-errors-must-surface contract already applied to per-job
/// search errors.
fn catalog_error(err: &CatalogError) -> BackendError {
    BackendError::Transport(err.to_string())
}

/// The query-frontend pipeline: plan jobs -> queue (bounded fan-out) -> per-job
/// search -> merge (limit/spss) -> render Tempo JSON, in front of a
/// [`QuerierBackend`] pool with a [`BlockCatalog`] for block enumeration.
///
/// By-id does **not** fan per-block (the querier reassembles a trace across
/// blocks and exposes no block-scoped by-id); instead it queries every querier
/// in the pool and unions their v2 responses — meaningful because different
/// queriers' live-stores may hold different recent spans.
pub struct QueryFrontend<B: QuerierBackend, C: BlockCatalog> {
    backend: Arc<B>,
    catalog: Arc<C>,
    cfg: FrontendConfig,
}

impl<B: QuerierBackend + 'static, C: BlockCatalog + 'static> QueryFrontend<B, C> {
    #[must_use]
    pub fn new(backend: Arc<B>, catalog: Arc<C>, cfg: FrontendConfig) -> Self {
        Self {
            backend,
            catalog,
            cfg,
        }
    }

    /// Test/inspection accessor for the backend (e.g. `MockQuerier::search_calls`).
    #[must_use]
    pub fn backend_ref(&self) -> &B {
        &self.backend
    }

    /// The configured default trace limit.
    #[must_use]
    pub fn default_limit(&self) -> usize {
        self.cfg.default_limit
    }

    /// The configured default spans-per-spanSet.
    #[must_use]
    pub fn default_spss(&self) -> usize {
        self.cfg.default_spss
    }

    /// Run a `TraceQL` `/api/search` through the full pipeline.
    ///
    /// Search shards **partition** the data (live tier + disjoint cold blocks),
    /// so a failed shard means missing results — any job error propagates
    /// (an invalid query fails on every shard and must surface, not silently
    /// return an empty 200).
    pub async fn search(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        spss: usize,
    ) -> Result<SearchResponseJson, BackendError> {
        let blocks = self
            .catalog
            .blocks(tenant, start_ns, end_ns)
            .await
            .map_err(|e| catalog_error(&e))?;
        let plan = job::plan_search_jobs(
            &blocks,
            end_ns,
            self.cfg.hot_frontier_ns,
            self.cfg.target_bytes_per_job,
        );
        let total_jobs = plan.jobs.len() as u64;
        let total_blocks = plan.total_blocks;

        let backend = Arc::clone(&self.backend);
        let tenant_s = tenant.to_string();
        let query_s = query.to_string();
        let results = queue::run_jobs(plan.jobs, self.cfg.max_concurrency, move |shard| {
            let backend = Arc::clone(&backend);
            let req = SearchJobRequest {
                tenant: tenant_s.clone(),
                query: query_s.clone(),
                start_ns,
                end_ns,
                limit,
                spss,
                shard,
            };
            async move { backend.search_job(&req).await }
        })
        .await;
        let partials: Vec<SearchPartial> = results.into_iter().collect::<Result<_, _>>()?;

        let mut resp = merge::merge_search(partials, limit, spss);
        // Seed plan-derived totals (per-job metrics carry completed/bytes).
        resp.metrics.total_jobs = total_jobs;
        resp.metrics.total_blocks = total_blocks;
        Ok(resp)
    }

    /// Run a `/api/v2/traces/{id}` by-id lookup, fanning one job per querier.
    ///
    /// By-id queriers are **redundant** for a trace (each reassembles it from
    /// object storage; their live-stores differ only in recent spans), so
    /// per-querier failures are tolerated: the trace is assembled from any
    /// successes and an error propagates only when *every* querier failed.
    pub async fn trace_by_id(
        &self,
        tenant: &str,
        trace_id: [u8; 16],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(Option<TraceByIdResponseJson>, Metrics, TraceStatus), BackendError> {
        let queriers = self.backend.querier_count().max(1);
        let jobs: Vec<usize> = (0..queriers).collect();
        let total_jobs = jobs.len() as u64;

        let backend = Arc::clone(&self.backend);
        let tenant_s = tenant.to_string();
        let results = queue::run_jobs(jobs, self.cfg.max_concurrency, move |idx| {
            let backend = Arc::clone(&backend);
            let req = TraceByIdJobRequest {
                tenant: tenant_s.clone(),
                trace_id,
                start_ns,
                end_ns,
                querier: Some(idx),
            };
            async move { backend.trace_by_id_job(&req).await }
        })
        .await;

        let mut partials = Vec::new();
        let mut first_err = None;
        for r in results {
            match r {
                Ok(p) => partials.push(p),
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        if partials.is_empty()
            && let Some(e) = first_err
        {
            return Err(e);
        }

        let (trace, mut metrics, status) =
            merge::assemble_trace(partials, self.cfg.max_trace_bytes);
        metrics.total_jobs = total_jobs;
        Ok((trace, metrics, status))
    }

    /// Run `/api/v2/search/tags`: fan over the planned shards, union+dedupe.
    pub async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<crabka_traceql::TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(Vec<crabka_traceql::ScopedTag>, Metrics), BackendError> {
        let blocks = self
            .catalog
            .blocks(tenant, start_ns, end_ns)
            .await
            .map_err(|e| catalog_error(&e))?;
        let plan = job::plan_search_jobs(
            &blocks,
            end_ns,
            self.cfg.hot_frontier_ns,
            self.cfg.target_bytes_per_job,
        );
        let total_jobs = plan.jobs.len() as u64;
        let total_blocks = plan.total_blocks;

        let backend = Arc::clone(&self.backend);
        let tenant_s = tenant.to_string();
        let results = queue::run_jobs(plan.jobs, self.cfg.max_concurrency, move |shard| {
            let backend = Arc::clone(&backend);
            let req = TagNamesJobRequest {
                tenant: tenant_s.clone(),
                scope,
                start_ns,
                end_ns,
                shard,
            };
            async move { backend.tag_names_job(&req).await }
        })
        .await;
        let partials: Vec<TagNamesPartial> = results.into_iter().collect::<Result<_, _>>()?;

        let (tags, mut metrics) = merge::merge_tag_names(partials);
        metrics.total_jobs = total_jobs;
        metrics.total_blocks = total_blocks;
        Ok((tags, metrics))
    }

    /// Run `/api/v2/search/tag/{tag}/values`: fan over shards, union+dedupe.
    pub async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(Vec<crabka_traceql::TypedValue>, Metrics), BackendError> {
        let blocks = self
            .catalog
            .blocks(tenant, start_ns, end_ns)
            .await
            .map_err(|e| catalog_error(&e))?;
        let plan = job::plan_search_jobs(
            &blocks,
            end_ns,
            self.cfg.hot_frontier_ns,
            self.cfg.target_bytes_per_job,
        );
        let total_jobs = plan.jobs.len() as u64;
        let total_blocks = plan.total_blocks;

        let backend = Arc::clone(&self.backend);
        let tenant_s = tenant.to_string();
        let tag_s = tag.to_string();
        let results = queue::run_jobs(plan.jobs, self.cfg.max_concurrency, move |shard| {
            let backend = Arc::clone(&backend);
            let req = TagValuesJobRequest {
                tenant: tenant_s.clone(),
                tag: tag_s.clone(),
                start_ns,
                end_ns,
                shard,
            };
            async move { backend.tag_values_job(&req).await }
        })
        .await;
        let partials: Vec<TagValuesPartial> = results.into_iter().collect::<Result<_, _>>()?;

        let (values, mut metrics) = merge::merge_tag_values(partials);
        metrics.total_jobs = total_jobs;
        metrics.total_blocks = total_blocks;
        Ok((values, metrics))
    }

    /// Run a `TraceQL`-metrics query (`/api/metrics/query_range` or `query`) as a
    /// **single unsharded job** against one querier.
    ///
    /// Metrics are deliberately NOT sharded across blocks. The per-shard *reduced*
    /// results are not safely mergeable: summing them double-counts every cold
    /// block (the no-restriction "live" job already scans cold-before-frontier +
    /// live, which overlaps the per-block jobs), and summing is plain wrong for
    /// the non-additive aggregates (`min`/`max`/`avg`/`quantile_over_time`). A
    /// single unrestricted job lets one querier compute the full hot+cold union
    /// correctly for every aggregate; only exemplar limiting is applied here.
    #[allow(
        clippy::too_many_arguments,
        reason = "the metrics query surface (tenant/query/window/step/instant/exemplar-limit) is one cohesive request; grouping into a params struct would only relocate the fields"
    )]
    pub async fn metrics_query(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
        instant: bool,
        exemplar_limit: Option<usize>,
    ) -> Result<MetricsResponseJson, BackendError> {
        let req = MetricsJobRequest {
            tenant: tenant.to_string(),
            query: query.to_string(),
            start_ns,
            end_ns,
            step_ns,
            instant,
            // `JobShard::Live` sends no scan restriction, so the querier scans its
            // full hot+cold union — the whole result in one job.
            shard: JobShard::Live,
        };
        let mut series = self.backend.metrics_job(&req).await?.response.series;
        metrics_merge::limit_exemplars(&mut series, exemplar_limit);
        Ok(MetricsResponseJson { series })
    }
}

#[cfg(test)]
mod orch_tests {
    use std::sync::Arc;

    use assert2::{assert, check};

    use super::*;
    use crate::frontend::backend::{MockQuerier, SearchPartial};
    use crate::frontend::job::{BlockMetaInfo, MockCatalog, RowGroupInfo};
    use crate::frontend::wire::{Metrics, SpanJson, SpanSetJson, TraceJson};

    fn block(id: &str, start: i64, end: i64, rgs: &[u64]) -> BlockMetaInfo {
        let row_groups = rgs
            .iter()
            .enumerate()
            .map(|(i, &b)| RowGroupInfo {
                index: u32::try_from(i).unwrap(),
                compressed_bytes: b,
            })
            .collect();
        BlockMetaInfo {
            block_id: id.to_string(),
            start_ns: start,
            end_ns: end,
            size_bytes: rgs.iter().sum(),
            row_groups,
        }
    }

    fn one_trace(tid: &str, start: u64) -> SearchPartial {
        SearchPartial {
            traces: vec![TraceJson {
                trace_id: tid.to_string(),
                root_service_name: "svc".to_string(),
                root_trace_name: "GET /".to_string(),
                start_time_unix_nano: start.to_string(),
                duration_ms: 1,
                span_sets: vec![SpanSetJson {
                    spans: vec![SpanJson {
                        span_id: tid.to_string(),
                        start_time_unix_nano: start.to_string(),
                        duration_nanos: "1".to_string(),
                        attributes: vec![],
                    }],
                    matched: 1,
                }],
            }],
            metrics: Metrics {
                completed_jobs: 1,
                inspected_bytes: 100,
                inspected_traces: 1,
                inspected_spans: 1,
                ..Metrics::default()
            },
        }
    }

    #[tokio::test]
    async fn search_plans_jobs_fans_and_merges() {
        // Two small cold blocks + a hot window => 1 Live + 2 block jobs = 3.
        let catalog = MockCatalog::new(vec![
            block("b1", 0, 100, &[500]),
            block("b2", 100, 200, &[500]),
        ]);
        let backend = MockQuerier::new();
        backend.stub_search(one_trace("01", 50));
        backend.stub_search(one_trace("02", 150));
        backend.stub_search(one_trace("03", 250));
        let cfg = FrontendConfig {
            target_bytes_per_job: 10_000,
            max_concurrency: 1,
            hot_frontier_ns: 150,
            ..FrontendConfig::default()
        };
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);

        let resp = qf.search("t1", "{ }", 0, 300, 20, 3).await.unwrap();
        assert!(qf.backend_ref().search_calls().len() == 3);
        for c in qf.backend_ref().search_calls() {
            assert!(c.tenant == "t1");
        }
        check!(resp.traces.len() == 3);
        // A successful multi-job search folds real per-job accounting:
        // completedJobs == totalJobs, and non-zero inspected traces/spans (not
        // the all-zero block that the querier used to emit).
        check!(
            resp.metrics
                == Metrics {
                    total_jobs: 3,
                    completed_jobs: 3,
                    total_blocks: 2,
                    inspected_traces: 3,
                    inspected_bytes: 300,
                    inspected_spans: 3,
                }
        );
    }

    #[tokio::test]
    async fn search_honors_limit() {
        let catalog = MockCatalog::new(vec![block("b1", 0, 100, &[500])]);
        let backend = MockQuerier::new();
        backend.stub_search(SearchPartial {
            traces: vec![
                one_trace("01", 100).traces.pop().unwrap(),
                one_trace("02", 300).traces.pop().unwrap(),
                one_trace("03", 200).traces.pop().unwrap(),
            ],
            metrics: Metrics {
                completed_jobs: 1,
                ..Metrics::default()
            },
        });
        let cfg = FrontendConfig {
            hot_frontier_ns: i64::MAX,
            ..FrontendConfig::default()
        };
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);
        let resp = qf.search("t1", "{ }", 0, 300, 1, 3).await.unwrap();
        assert!(resp.traces.len() == 1);
        assert!(resp.traces[0].start_time_unix_nano == "300");
    }

    #[tokio::test]
    async fn trace_by_id_fans_one_job_per_querier() {
        let catalog = MockCatalog::new(vec![block("b1", 0, 100, &[500])]);
        let backend = MockQuerier::with_querier_count(3);
        let cfg = FrontendConfig {
            max_concurrency: 1,
            ..FrontendConfig::default()
        };
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);
        let (_t, metrics, status) = qf.trace_by_id("t1", [9; 16], 0, 300).await.unwrap();
        // One job per querier (3), none returned the trace => Complete + None.
        check!(qf.backend_ref().trace_calls().len() == 3);
        check!(metrics.total_jobs == 3);
        assert!(matches!(status, TraceStatus::Complete));
    }

    /// A catalog whose enumeration always fails (a partition is unreachable).
    struct FailingCatalog;

    #[async_trait::async_trait]
    impl crate::frontend::job::BlockCatalog for FailingCatalog {
        async fn blocks(
            &self,
            _tenant: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<BlockMetaInfo>, crate::frontend::job::CatalogError> {
            Err(crate::frontend::job::CatalogError::Backend(
                "partition unreachable".to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn search_surfaces_catalog_error_instead_of_empty_200() {
        // A catalog failure drops the cold partitions; swallowing it would return
        // a misleading live-only 200. It must surface as a backend error (5xx).
        let backend = MockQuerier::new();
        let qf = QueryFrontend::new(
            Arc::new(backend),
            Arc::new(FailingCatalog),
            FrontendConfig::default(),
        );
        let err = qf.search("t1", "{ }", 0, 300, 20, 3).await.unwrap_err();
        assert!(matches!(err, BackendError::Transport(_)));
        // The backend was never fanned out — the catalog error short-circuits.
        assert!(qf.backend_ref().search_calls().is_empty());
    }

    #[tokio::test]
    async fn tag_names_surfaces_catalog_error_instead_of_empty_200() {
        let backend = MockQuerier::new();
        let qf = QueryFrontend::new(
            Arc::new(backend),
            Arc::new(FailingCatalog),
            FrontendConfig::default(),
        );
        let err = qf.tag_names("t1", None, 0, 300).await.unwrap_err();
        assert!(matches!(err, BackendError::Transport(_)));
    }

    #[tokio::test]
    async fn tag_values_surfaces_catalog_error_instead_of_empty_200() {
        let backend = MockQuerier::new();
        let qf = QueryFrontend::new(
            Arc::new(backend),
            Arc::new(FailingCatalog),
            FrontendConfig::default(),
        );
        let err = qf.tag_values("t1", "span.name", 0, 300).await.unwrap_err();
        assert!(matches!(err, BackendError::Transport(_)));
    }
}
