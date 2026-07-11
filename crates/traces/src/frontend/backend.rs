//! The querier-backend abstraction the frontend fans out to, one call per
//! planned job. Tests use [`MockQuerier`]; real deployments use
//! [`crate::frontend::http_backend::HttpQuerier`].
//!
//! Partials are carried in the **typed serde edge model** ([`crate::frontend::wire`]),
//! not raw `serde_json::Value`: a search job returns `Vec<TraceJson>` + `Metrics`,
//! a by-id job returns a typed OTLP-JSON `TraceByIdResponseJson`, tag jobs return
//! the typed tag bodies. The merge layer (`merge.rs`) operates on these.

use std::sync::Mutex;

use async_trait::async_trait;
use crabka_traceql::{ScopedTag, TagScope, TypedValue};

use crate::frontend::{
    job::JobShard,
    metrics_merge::MetricsResponseJson,
    wire::{Metrics, TraceByIdResponseJson, TraceJson},
};

/// A single search job: a `TraceQL` search restricted to one shard (the live hot
/// tier, or one cold block narrowed to a row-group range) over a window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchJobRequest {
    pub tenant: String,
    pub query: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub limit: usize,
    pub spss: usize,
    pub shard: JobShard,
}

/// A by-id job: fetch one trace's spans from one querier. By-id does **not**
/// fan per-block (the querier reassembles a trace across blocks); the frontend
/// fans one job per querier and unions their v2 responses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceByIdJobRequest {
    pub tenant: String,
    pub trace_id: [u8; 16],
    pub start_ns: i64,
    pub end_ns: i64,
    /// Index into the backend's querier pool to target (so a fan-out queries
    /// each querier exactly once). `None` lets the backend pick (round-robin).
    pub querier: Option<usize>,
}

/// A tag-names job for one (optional) scope over a window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagNamesJobRequest {
    pub tenant: String,
    pub scope: Option<TagScope>,
    pub start_ns: i64,
    pub end_ns: i64,
    pub shard: JobShard,
}

/// A tag-values job for one tag over a window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagValuesJobRequest {
    pub tenant: String,
    pub tag: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub shard: JobShard,
}

/// A `TraceQL`-metrics job (`/api/metrics/query_range` or `/api/metrics/query`)
/// over a window with a step. `instant` selects the instant-query path.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricsJobRequest {
    pub tenant: String,
    pub query: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub step_ns: i64,
    pub instant: bool,
    pub shard: JobShard,
}

/// The partial result of one search job: matched traces (typed Tempo JSON) +
/// the job's accounting.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchPartial {
    pub traces: Vec<TraceJson>,
    pub metrics: Metrics,
}

/// The partial result of one by-id job: the (possibly empty) typed v2 trace
/// body + accounting.
#[derive(Clone, Debug, Default)]
pub struct TracePartial {
    pub trace: TraceByIdResponseJson,
    pub metrics: Metrics,
}

/// The partial result of one tag-names job.
#[derive(Clone, Debug, Default)]
pub struct TagNamesPartial {
    pub tags: Vec<ScopedTag>,
    pub metrics: Metrics,
}

/// The partial result of one tag-values job.
#[derive(Clone, Debug, Default)]
pub struct TagValuesPartial {
    pub values: Vec<TypedValue>,
    pub metrics: Metrics,
}

/// The partial result of one metrics job: the series body + accounting.
#[derive(Clone, Debug, Default)]
pub struct MetricsPartial {
    pub response: MetricsResponseJson,
    pub metrics: Metrics,
}

/// Failure modes of a single backend job.
#[derive(Clone, Debug, thiserror::Error)]
pub enum BackendError {
    #[error("backend job timed out")]
    Timeout,
    #[error("backend transport error: {0}")]
    Transport(String),
    #[error("backend returned error ({status}): {message}")]
    Backend { status: String, message: String },
}

impl BackendError {
    /// Map this backend failure to the `(status, body)` the frontend returns to
    /// its client, preserving the upstream querier's status code and error text
    /// where known (a timeout becomes `504`, a transport failure `502`).
    #[must_use]
    pub fn to_http(&self) -> (u16, String) {
        match self {
            BackendError::Timeout => (504, "backend job timed out".to_string()),
            BackendError::Transport(detail) => (502, format!("backend transport error: {detail}")),
            BackendError::Backend { status, message } => {
                (status.parse::<u16>().unwrap_or(502), message.clone())
            }
        }
    }
}

/// A queryable querier backend (a pool fronting N queriers). Every method is
/// one fanned-out job's worth of work.
#[async_trait]
pub trait QuerierBackend: Send + Sync {
    /// Number of queriers in the pool (the by-id fan-out width).
    fn querier_count(&self) -> usize;

    async fn search_job(&self, req: &SearchJobRequest) -> Result<SearchPartial, BackendError>;
    async fn trace_by_id_job(
        &self,
        req: &TraceByIdJobRequest,
    ) -> Result<TracePartial, BackendError>;
    async fn tag_names_job(
        &self,
        req: &TagNamesJobRequest,
    ) -> Result<TagNamesPartial, BackendError>;
    async fn tag_values_job(
        &self,
        req: &TagValuesJobRequest,
    ) -> Result<TagValuesPartial, BackendError>;
    async fn metrics_job(&self, req: &MetricsJobRequest) -> Result<MetricsPartial, BackendError>;
}

/// A programmable in-process backend for tests. Returns the next stubbed
/// response (FIFO; the last stub repeats if more calls arrive) and records
/// every request for assertions. Exposed un-gated so integration tests in
/// `tests/` can construct it — a fixture, not production wiring.
pub struct MockQuerier {
    querier_count: usize,
    search_stubs: Mutex<Vec<SearchPartial>>,
    trace_stubs: Mutex<Vec<TracePartial>>,
    tag_names_stubs: Mutex<Vec<TagNamesPartial>>,
    tag_values_stubs: Mutex<Vec<TagValuesPartial>>,
    metrics_stubs: Mutex<Vec<MetricsPartial>>,
    search_calls: Mutex<Vec<SearchJobRequest>>,
    trace_calls: Mutex<Vec<TraceByIdJobRequest>>,
    tag_names_calls: Mutex<Vec<TagNamesJobRequest>>,
    tag_values_calls: Mutex<Vec<TagValuesJobRequest>>,
    metrics_calls: Mutex<Vec<MetricsJobRequest>>,
}

impl MockQuerier {
    #[must_use]
    pub fn new() -> Self {
        Self::with_querier_count(1)
    }

    #[must_use]
    pub fn with_querier_count(querier_count: usize) -> Self {
        Self {
            querier_count: querier_count.max(1),
            search_stubs: Mutex::new(Vec::new()),
            trace_stubs: Mutex::new(Vec::new()),
            tag_names_stubs: Mutex::new(Vec::new()),
            tag_values_stubs: Mutex::new(Vec::new()),
            metrics_stubs: Mutex::new(Vec::new()),
            search_calls: Mutex::new(Vec::new()),
            trace_calls: Mutex::new(Vec::new()),
            tag_names_calls: Mutex::new(Vec::new()),
            tag_values_calls: Mutex::new(Vec::new()),
            metrics_calls: Mutex::new(Vec::new()),
        }
    }

    /// Enqueue a canned search-job response (FIFO).
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_search(&self, p: SearchPartial) {
        self.search_stubs.lock().unwrap().push(p);
    }

    /// Enqueue a canned by-id-job response (FIFO).
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_trace(&self, p: TracePartial) {
        self.trace_stubs.lock().unwrap().push(p);
    }

    /// Enqueue a canned tag-names-job response (FIFO).
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_tag_names(&self, p: TagNamesPartial) {
        self.tag_names_stubs.lock().unwrap().push(p);
    }

    /// Enqueue a canned tag-values-job response (FIFO).
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_tag_values(&self, p: TagValuesPartial) {
        self.tag_values_stubs.lock().unwrap().push(p);
    }

    /// Enqueue a canned metrics-job response (FIFO).
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_metrics(&self, p: MetricsPartial) {
        self.metrics_stubs.lock().unwrap().push(p);
    }

    /// All recorded search-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn search_calls(&self) -> Vec<SearchJobRequest> {
        self.search_calls.lock().unwrap().clone()
    }

    /// All recorded by-id-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn trace_calls(&self) -> Vec<TraceByIdJobRequest> {
        self.trace_calls.lock().unwrap().clone()
    }

    /// All recorded tag-names-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn tag_names_calls(&self) -> Vec<TagNamesJobRequest> {
        self.tag_names_calls.lock().unwrap().clone()
    }

    /// All recorded tag-values-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn tag_values_calls(&self) -> Vec<TagValuesJobRequest> {
        self.tag_values_calls.lock().unwrap().clone()
    }

    /// All recorded metrics-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn metrics_calls(&self) -> Vec<MetricsJobRequest> {
        self.metrics_calls.lock().unwrap().clone()
    }

    fn pop<T: Clone + Default>(stubs: &Mutex<Vec<T>>) -> T {
        let mut s = stubs.lock().unwrap();
        if s.len() > 1 {
            s.remove(0)
        } else {
            s.first().cloned().unwrap_or_default()
        }
    }
}

impl Default for MockQuerier {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QuerierBackend for MockQuerier {
    fn querier_count(&self) -> usize {
        self.querier_count
    }

    async fn search_job(&self, req: &SearchJobRequest) -> Result<SearchPartial, BackendError> {
        self.search_calls.lock().unwrap().push(req.clone());
        Ok(Self::pop(&self.search_stubs))
    }

    async fn trace_by_id_job(
        &self,
        req: &TraceByIdJobRequest,
    ) -> Result<TracePartial, BackendError> {
        self.trace_calls.lock().unwrap().push(req.clone());
        Ok(Self::pop(&self.trace_stubs))
    }

    async fn tag_names_job(
        &self,
        req: &TagNamesJobRequest,
    ) -> Result<TagNamesPartial, BackendError> {
        self.tag_names_calls.lock().unwrap().push(req.clone());
        Ok(Self::pop(&self.tag_names_stubs))
    }

    async fn tag_values_job(
        &self,
        req: &TagValuesJobRequest,
    ) -> Result<TagValuesPartial, BackendError> {
        self.tag_values_calls.lock().unwrap().push(req.clone());
        Ok(Self::pop(&self.tag_values_stubs))
    }

    async fn metrics_job(&self, req: &MetricsJobRequest) -> Result<MetricsPartial, BackendError> {
        self.metrics_calls.lock().unwrap().push(req.clone());
        Ok(Self::pop(&self.metrics_stubs))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::frontend::{job::JobShard, wire::TraceJson};

    fn trace(svc: &str) -> TraceJson {
        TraceJson {
            trace_id: "01".repeat(16),
            root_service_name: svc.to_string(),
            root_trace_name: "GET /".to_string(),
            start_time_unix_nano: "1".to_string(),
            duration_ms: 1,
            span_sets: vec![],
        }
    }

    #[tokio::test]
    async fn mock_returns_canned_and_records_calls() {
        let mock = MockQuerier::new();
        mock.stub_search(SearchPartial {
            traces: vec![trace("checkout")],
            metrics: Metrics {
                total_jobs: 1,
                completed_jobs: 1,
                inspected_bytes: 10,
                ..Metrics::default()
            },
        });
        let req = SearchJobRequest {
            tenant: "t1".to_string(),
            query: "{ .service.name = \"checkout\" }".to_string(),
            start_ns: 0,
            end_ns: 100,
            limit: 20,
            spss: 3,
            shard: JobShard::Live,
        };
        let out = mock.search_job(&req).await.unwrap();
        assert2::assert!(
            out == SearchPartial {
                traces: vec![trace("checkout")],
                metrics: Metrics {
                    total_jobs: 1,
                    completed_jobs: 1,
                    total_blocks: 0,
                    inspected_traces: 0,
                    inspected_bytes: 10,
                    inspected_spans: 0,
                },
            }
        );
        assert2::assert!(mock.search_calls().len() == 1);
        assert2::assert!(mock.search_calls()[0].tenant.as_str() == "t1");
        assert2::assert!(matches!(mock.search_calls()[0].shard, JobShard::Live));
    }

    #[tokio::test]
    async fn empty_stub_yields_default_partial() {
        let mock = MockQuerier::new();
        let req = SearchJobRequest {
            tenant: "t1".to_string(),
            query: "{ }".to_string(),
            start_ns: 0,
            end_ns: 100,
            limit: 20,
            spss: 3,
            shard: JobShard::Live,
        };
        let out = mock.search_job(&req).await.unwrap();
        assert2::assert!(out.traces == vec![]);
        assert2::assert!(out.metrics == Metrics::default());
    }

    #[test]
    fn querier_count_clamps_to_one() {
        assert2::assert!(MockQuerier::with_querier_count(0).querier_count() == 1);
        assert2::assert!(MockQuerier::with_querier_count(3).querier_count() == 3);
    }
}
