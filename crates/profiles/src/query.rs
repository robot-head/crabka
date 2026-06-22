//! Querier role: Pyroscope `querier.v1` Connect API and legacy flamebearer endpoints.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{Int64Type, UInt64Type};
use axum::extract::{Query, RawQuery};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json, Router, routing::get};
use connectrpc_axum::message::{Code, ConnectError, ConnectRequest, ConnectResponse};
use crabka_blockstore::{LABEL_PROFILE_TYPE, LabelMatcher, MatchOp};
use crabka_pprof::{
    COL_FINGERPRINT, COL_TIMESTAMP, EngineOpts, FlameEngine, FlameGraph, InMemoryProfileStore,
    LabeledHeatmap, PCOL_SPAN_ID, PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION, PCOL_TOTAL_VALUE,
    PCOL_VALUE, ProfileError, ProfileStore, ProfileType, Series, SeriesAgg, bin_heatmap,
    parse_label_selector, step_bucket_ms, step_ms_from_secs,
};
use prost::Message;
use serde::{Deserialize, Deserializer};
use serde_json::json;
use tokio::net::TcpListener;

use crate::limits::{Limits, OverridesProvider};
use crate::query_frontend::{FrontendConfig, split_inclusive_range};
use crate::wire::pb;

const DEFAULT_HEATMAP_VALUE_BUCKETS: usize = 32;
const MAX_HEATMAP_TIME_BUCKETS: usize = 4096;
const PROFILE_ID_LABEL: &str = "__profile_id__";

/// Labels stored internally for span/exemplar lookups that Pyroscope does not
/// expose through the label-enumeration APIs (`LabelNames`, `LabelValues`,
/// series counting). `__profile_id__` is attached per profile, so surfacing it
/// would leak per-profile cardinality that real Pyroscope never reports.
fn is_internal_label(name: &str) -> bool {
    name == PROFILE_ID_LABEL
}

pub type DefaultStore = InMemoryProfileStore;
type SeriesKey = Vec<(String, String)>;
type SpanExemplarsBySeries = BTreeMap<SeriesKey, BTreeMap<i64, Vec<pb::types::v1::Exemplar>>>;
type HeatmapSpanExemplarsBySeries =
    BTreeMap<SeriesKey, BTreeMap<i64, Vec<pb::querier::v1::Exemplar>>>;

pub struct QuerierState<S: ProfileStore = DefaultStore> {
    store: Arc<S>,
    engine: FlameEngine<S>,
    execution: QueryExecution,
    overrides: OverridesProvider,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum QueryExecution {
    Direct,
    Sharded(FrontendConfig),
}

impl QuerierState<DefaultStore> {
    #[must_use]
    pub fn empty() -> Self {
        Self::new(Arc::new(InMemoryProfileStore::new()))
    }
}

impl<S: ProfileStore> QuerierState<S> {
    #[must_use]
    pub fn new(store: Arc<S>) -> Self {
        Self::new_with_limits(store, Limits::default())
    }

    #[must_use]
    pub fn new_with_limits(store: Arc<S>, limits: Limits) -> Self {
        Self::new_with_overrides(store, OverridesProvider::new(limits))
    }

    #[must_use]
    pub fn new_with_overrides(store: Arc<S>, overrides: OverridesProvider) -> Self {
        Self::from_parts(store, QueryExecution::Direct, overrides)
    }

    #[must_use]
    pub fn new_frontend(store: Arc<S>, config: FrontendConfig) -> Self {
        Self::new_frontend_with_limits(store, config, Limits::default())
    }

    #[must_use]
    pub fn new_frontend_with_limits(store: Arc<S>, config: FrontendConfig, limits: Limits) -> Self {
        Self::new_frontend_with_overrides(store, config, OverridesProvider::new(limits))
    }

    #[must_use]
    pub fn new_frontend_with_overrides(
        store: Arc<S>,
        config: FrontendConfig,
        overrides: OverridesProvider,
    ) -> Self {
        Self::from_parts(store, QueryExecution::Sharded(config), overrides)
    }

    fn from_parts(store: Arc<S>, execution: QueryExecution, overrides: OverridesProvider) -> Self {
        let engine = FlameEngine::new(Arc::clone(&store), EngineOpts::default());
        Self {
            store,
            engine,
            execution,
            overrides,
        }
    }

    fn validate_query_range(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<(), ProfileError> {
        self.overrides
            .for_tenant(tenant)
            .validate_query_range_ms(start_ms, end_ms)
            .map_err(|err| ProfileError::Plan(err.message()))
    }

    fn effective_max_nodes(&self, tenant: &str, requested: i64) -> i64 {
        self.overrides
            .for_tenant(tenant)
            .effective_max_nodes(requested)
    }

    async fn select_merge_stacktraces(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
    ) -> Result<FlameGraph, ProfileError> {
        self.select_merge_stacktraces_with_stack_trace_selector(
            tenant,
            profile_type,
            label_selector,
            start_ms,
            end_ms,
            max_nodes,
            &[],
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_merge_stacktraces_grouped(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
        group_by: &[String],
    ) -> Result<FlameGraph, ProfileError> {
        if group_by.is_empty() {
            return self
                .select_merge_stacktraces(
                    tenant,
                    profile_type,
                    label_selector,
                    start_ms,
                    end_ms,
                    max_nodes,
                )
                .await;
        }
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let max_nodes = self.effective_max_nodes(tenant, max_nodes);
        self.engine
            .select_merge_stacktraces_grouped(
                tenant,
                profile_type,
                label_selector,
                start_ms,
                end_ms,
                max_nodes,
                group_by,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_merge_stacktraces_with_stack_trace_selector(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
        stack_trace_call_sites: &[String],
    ) -> Result<FlameGraph, ProfileError> {
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let max_nodes = self.effective_max_nodes(tenant, max_nodes);
        match &self.execution {
            QueryExecution::Direct => {
                self.engine
                    .select_merge_stacktraces_with_stack_trace_selector(
                        tenant,
                        profile_type,
                        label_selector,
                        start_ms,
                        end_ms,
                        max_nodes,
                        stack_trace_call_sites,
                    )
                    .await
            }
            QueryExecution::Sharded(config) => {
                let shards = split_inclusive_range(start_ms, end_ms, config.shard_width_ms)?;
                self.engine
                    .select_merge_stacktraces_with_stack_trace_selector_sharded(
                        tenant,
                        profile_type,
                        label_selector,
                        &shards,
                        max_nodes,
                        stack_trace_call_sites,
                    )
                    .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_merge_stacktraces_tree_with_stack_trace_selector(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
        stack_trace_call_sites: &[String],
    ) -> Result<Vec<u8>, ProfileError> {
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let max_nodes = self.effective_max_nodes(tenant, max_nodes);
        match &self.execution {
            QueryExecution::Direct => {
                self.engine
                    .select_merge_stacktraces_tree_with_stack_trace_selector(
                        tenant,
                        profile_type,
                        label_selector,
                        start_ms,
                        end_ms,
                        max_nodes,
                        stack_trace_call_sites,
                    )
                    .await
            }
            QueryExecution::Sharded(config) => {
                let shards = split_inclusive_range(start_ms, end_ms, config.shard_width_ms)?;
                self.engine
                    .select_merge_stacktraces_tree_with_stack_trace_selector_sharded(
                        tenant,
                        profile_type,
                        label_selector,
                        &shards,
                        max_nodes,
                        stack_trace_call_sites,
                    )
                    .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_series(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        group_by: &[String],
        step_secs: f64,
        agg: SeriesAgg,
        start_ms: i64,
        end_ms: i64,
        stack_trace_call_sites: &[String],
    ) -> Result<Vec<Series>, ProfileError> {
        self.validate_query_range(tenant, start_ms, end_ms)?;
        match &self.execution {
            QueryExecution::Direct => {
                self.engine
                    .select_series_with_stack_trace_selector(
                        tenant,
                        profile_type,
                        label_selector,
                        group_by,
                        step_secs,
                        agg,
                        start_ms,
                        end_ms,
                        stack_trace_call_sites,
                    )
                    .await
            }
            QueryExecution::Sharded(config) => {
                let shards = split_inclusive_range(start_ms, end_ms, config.shard_width_ms)?;
                self.engine
                    .select_series_with_stack_trace_selector_sharded(
                        tenant,
                        profile_type,
                        label_selector,
                        group_by,
                        step_secs,
                        agg,
                        &shards,
                        stack_trace_call_sites,
                    )
                    .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_series_span_exemplars(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        group_by: &[String],
        step_secs: f64,
        start_ms: i64,
        end_ms: i64,
        call_sites: &[String],
    ) -> Result<SpanExemplarsBySeries, ProfileError> {
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let step_ms = step_ms_from_secs(step_secs)?;
        let base_matchers = parse_label_selector(label_selector)?;
        let groups = if group_by.is_empty() {
            vec![Vec::new()]
        } else {
            self.store
                .series(tenant, &base_matchers, group_by, start_ms, end_ms)
                .await?
        };
        let mut out = BTreeMap::new();
        for labels in groups {
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let exemplars = span_exemplars_from_scan(&scan, step_ms, &labels, call_sites).await?;
            if !exemplars.is_empty() {
                out.insert(labels, exemplars);
            }
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_series_individual_exemplars(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        group_by: &[String],
        step_secs: f64,
        start_ms: i64,
        end_ms: i64,
        call_sites: &[String],
    ) -> Result<SpanExemplarsBySeries, ProfileError> {
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let step_ms = step_ms_from_secs(step_secs)?;
        let base_matchers = parse_label_selector(label_selector)?;
        let mut profile_group_by = group_by.to_vec();
        if !profile_group_by.iter().any(|name| name == PROFILE_ID_LABEL) {
            profile_group_by.push(PROFILE_ID_LABEL.to_string());
        }
        let groups = self
            .store
            .series(tenant, &base_matchers, &profile_group_by, start_ms, end_ms)
            .await?;
        let mut out: SpanExemplarsBySeries = BTreeMap::new();
        for labels in groups {
            let Some(profile_id) = labels
                .iter()
                .find(|(name, _)| name == PROFILE_ID_LABEL)
                .map(|(_, value)| value.clone())
            else {
                continue;
            };
            let series_labels: Vec<_> = labels
                .iter()
                .filter(|(name, _)| name != PROFILE_ID_LABEL)
                .cloned()
                .collect();
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let exemplars = individual_exemplars_from_scan(
                &scan,
                step_ms,
                &series_labels,
                &profile_id,
                call_sites,
            )
            .await?;
            let points = out.entry(series_labels).or_default();
            for (timestamp, mut exemplars) in exemplars {
                points.entry(timestamp).or_default().append(&mut exemplars);
            }
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_heatmap_span_exemplars(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        group_by: &[String],
        start_ms: i64,
        end_ms: i64,
        time_buckets: usize,
    ) -> Result<HeatmapSpanExemplarsBySeries, ProfileError> {
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let base_matchers = parse_label_selector(label_selector)?;
        let groups = if group_by.is_empty() {
            vec![Vec::new()]
        } else {
            self.store
                .series(tenant, &base_matchers, group_by, start_ms, end_ms)
                .await?
        };
        let mut out = BTreeMap::new();
        for labels in groups {
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let exemplars =
                heatmap_span_exemplars_from_scan(&scan, start_ms, end_ms, time_buckets, &labels)
                    .await?;
            if !exemplars.is_empty() {
                out.insert(labels, exemplars);
            }
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_heatmap_individual_exemplars(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        group_by: &[String],
        start_ms: i64,
        end_ms: i64,
        time_buckets: usize,
    ) -> Result<HeatmapSpanExemplarsBySeries, ProfileError> {
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let base_matchers = parse_label_selector(label_selector)?;
        let mut profile_group_by = group_by.to_vec();
        if !profile_group_by.iter().any(|name| name == PROFILE_ID_LABEL) {
            profile_group_by.push(PROFILE_ID_LABEL.to_string());
        }
        let groups = self
            .store
            .series(tenant, &base_matchers, &profile_group_by, start_ms, end_ms)
            .await?;
        let mut out: HeatmapSpanExemplarsBySeries = BTreeMap::new();
        for labels in groups {
            let Some(profile_id) = labels
                .iter()
                .find(|(name, _)| name == PROFILE_ID_LABEL)
                .map(|(_, value)| value.clone())
            else {
                continue;
            };
            let series_labels: Vec<_> = labels
                .iter()
                .filter(|(name, _)| name != PROFILE_ID_LABEL)
                .cloned()
                .collect();
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let exemplars = heatmap_individual_exemplars_from_scan(
                &scan,
                start_ms,
                end_ms,
                time_buckets,
                &series_labels,
                &profile_id,
            )
            .await?;
            let slots = out.entry(series_labels).or_default();
            for (timestamp, mut exemplars) in exemplars {
                slots.entry(timestamp).or_default().append(&mut exemplars);
            }
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_span_heatmaps(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        group_by: &[String],
        start_ms: i64,
        end_ms: i64,
        time_buckets: usize,
        value_buckets: usize,
    ) -> Result<Vec<LabeledHeatmap>, ProfileError> {
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let base_matchers = parse_label_selector(label_selector)?;
        let groups = if group_by.is_empty() {
            vec![Vec::new()]
        } else {
            self.store
                .series(tenant, &base_matchers, group_by, start_ms, end_ms)
                .await?
        };
        let mut out = Vec::new();
        for labels in groups {
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let points = span_heatmap_points_from_scan(&scan).await?;
            if points.is_empty() && !group_by.is_empty() {
                continue;
            }
            out.push(LabeledHeatmap {
                labels,
                heatmap: bin_heatmap(&points, start_ms, end_ms, time_buckets, value_buckets),
            });
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_merge_span_profile(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        span_ids: &[u64],
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
    ) -> Result<FlameGraph, ProfileError> {
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let max_nodes = self.effective_max_nodes(tenant, max_nodes);
        match &self.execution {
            QueryExecution::Direct => {
                self.engine
                    .select_merge_span_profile(
                        tenant,
                        profile_type,
                        label_selector,
                        span_ids,
                        start_ms,
                        end_ms,
                        max_nodes,
                    )
                    .await
            }
            QueryExecution::Sharded(config) => {
                let shards = split_inclusive_range(start_ms, end_ms, config.shard_width_ms)?;
                self.engine
                    .select_merge_span_profile_sharded(
                        tenant,
                        profile_type,
                        label_selector,
                        span_ids,
                        &shards,
                        max_nodes,
                    )
                    .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_merge_span_profile_tree(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        span_ids: &[u64],
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
    ) -> Result<Vec<u8>, ProfileError> {
        self.validate_query_range(tenant, start_ms, end_ms)?;
        let max_nodes = self.effective_max_nodes(tenant, max_nodes);
        match &self.execution {
            QueryExecution::Direct => {
                self.engine
                    .select_merge_span_profile_tree(
                        tenant,
                        profile_type,
                        label_selector,
                        span_ids,
                        start_ms,
                        end_ms,
                        max_nodes,
                    )
                    .await
            }
            QueryExecution::Sharded(config) => {
                let shards = split_inclusive_range(start_ms, end_ms, config.shard_width_ms)?;
                self.engine
                    .select_merge_span_profile_tree_sharded(
                        tenant,
                        profile_type,
                        label_selector,
                        span_ids,
                        &shards,
                        max_nodes,
                    )
                    .await
            }
        }
    }
}

async fn span_exemplars_from_scan(
    scan: &crabka_pprof::ProfileScan,
    step_ms: i64,
    labels: &[(String, String)],
    call_sites: &[String],
) -> Result<BTreeMap<i64, Vec<pb::types::v1::Exemplar>>, ProfileError> {
    if call_sites.is_empty() {
        return span_exemplars_from_totals(scan, step_ms, labels).await;
    }
    let sql = format!(
        "SELECT {timestamp}, {fingerprint}, {span}, {partition}, {stacktrace}, SUM({value}) AS v \
         FROM {table} WHERE {span} IS NOT NULL \
         GROUP BY {timestamp}, {fingerprint}, {span}, {partition}, {stacktrace} \
         ORDER BY {timestamp}, {fingerprint}, {span}, {partition}, {stacktrace}",
        timestamp = COL_TIMESTAMP,
        fingerprint = COL_FINGERPRINT,
        span = PCOL_SPAN_ID,
        partition = PCOL_STACKTRACE_PARTITION,
        stacktrace = PCOL_STACKTRACE_ID,
        value = PCOL_VALUE,
        table = scan.samples_table,
    );
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    let mut per_span: BTreeMap<(i64, u64, u64), i64> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let fingerprints = batch.column(1).as_primitive::<UInt64Type>();
        let span_ids = batch.column(2).as_primitive::<UInt64Type>();
        let partitions = batch.column(3).as_primitive::<UInt64Type>();
        let stacktrace_ids = batch.column(4).as_primitive::<UInt64Type>();
        let values = batch.column(5).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            if span_ids.is_null(row) {
                continue;
            }
            let partition = partitions.value(row);
            let stacktrace_id = u32::try_from(stacktrace_ids.value(row)).map_err(|err| {
                ProfileError::Symbolize(format!("stacktrace id does not fit u32: {err}"))
            })?;
            let frames = scan.symbols.resolve(partition, stacktrace_id);
            if frames_match_call_sites(&frames, call_sites) {
                *per_span
                    .entry((
                        timestamps.value(row),
                        fingerprints.value(row),
                        span_ids.value(row),
                    ))
                    .or_default() += values.value(row);
            }
        }
    }
    let label_pairs = types_label_pairs(labels.to_vec());
    let mut out: BTreeMap<i64, Vec<pb::types::v1::Exemplar>> = BTreeMap::new();
    for ((timestamp, _fingerprint, span_id), value) in per_span {
        out.entry(step_bucket_ms(timestamp, step_ms))
            .or_default()
            .push(pb::types::v1::Exemplar {
                timestamp,
                profile_id: String::new(),
                span_id: format!("{span_id:x}"),
                value,
                labels: label_pairs.clone(),
            });
    }
    Ok(out)
}

async fn span_exemplars_from_totals(
    scan: &crabka_pprof::ProfileScan,
    step_ms: i64,
    labels: &[(String, String)],
) -> Result<BTreeMap<i64, Vec<pb::types::v1::Exemplar>>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, {fingerprint}, {span}, MAX({total}) AS total \
         FROM {table} WHERE {span} IS NOT NULL \
         GROUP BY {timestamp}, {fingerprint}, {span} \
         ORDER BY {timestamp}, {fingerprint}, {span}",
        timestamp = COL_TIMESTAMP,
        fingerprint = COL_FINGERPRINT,
        span = PCOL_SPAN_ID,
        total = PCOL_TOTAL_VALUE,
        table = scan.samples_table,
    );
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    let label_pairs = types_label_pairs(labels.to_vec());
    let mut out: BTreeMap<i64, Vec<pb::types::v1::Exemplar>> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let span_ids = batch.column(2).as_primitive::<UInt64Type>();
        let totals = batch.column(3).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            if span_ids.is_null(row) {
                continue;
            }
            let timestamp = timestamps.value(row);
            out.entry(step_bucket_ms(timestamp, step_ms))
                .or_default()
                .push(pb::types::v1::Exemplar {
                    timestamp,
                    profile_id: String::new(),
                    span_id: format!("{:x}", span_ids.value(row)),
                    value: totals.value(row),
                    labels: label_pairs.clone(),
                });
        }
    }
    Ok(out)
}

async fn individual_exemplars_from_scan(
    scan: &crabka_pprof::ProfileScan,
    step_ms: i64,
    labels: &[(String, String)],
    profile_id: &str,
    call_sites: &[String],
) -> Result<BTreeMap<i64, Vec<pb::types::v1::Exemplar>>, ProfileError> {
    if call_sites.is_empty() {
        return individual_exemplars_from_totals(scan, step_ms, labels, profile_id).await;
    }
    let sql = format!(
        "SELECT {timestamp}, {fingerprint}, {partition}, {stacktrace}, SUM({value}) AS v \
         FROM {table} GROUP BY {timestamp}, {fingerprint}, {partition}, {stacktrace} \
         ORDER BY {timestamp}, {fingerprint}, {partition}, {stacktrace}",
        timestamp = COL_TIMESTAMP,
        fingerprint = COL_FINGERPRINT,
        partition = PCOL_STACKTRACE_PARTITION,
        stacktrace = PCOL_STACKTRACE_ID,
        value = PCOL_VALUE,
        table = scan.samples_table,
    );
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    let mut per_profile: BTreeMap<(i64, u64), i64> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let fingerprints = batch.column(1).as_primitive::<UInt64Type>();
        let partitions = batch.column(2).as_primitive::<UInt64Type>();
        let stacktrace_ids = batch.column(3).as_primitive::<UInt64Type>();
        let values = batch.column(4).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let partition = partitions.value(row);
            let stacktrace_id = u32::try_from(stacktrace_ids.value(row)).map_err(|err| {
                ProfileError::Symbolize(format!("stacktrace id does not fit u32: {err}"))
            })?;
            let frames = scan.symbols.resolve(partition, stacktrace_id);
            if frames_match_call_sites(&frames, call_sites) {
                *per_profile
                    .entry((timestamps.value(row), fingerprints.value(row)))
                    .or_default() += values.value(row);
            }
        }
    }
    let label_pairs = types_label_pairs(labels.to_vec());
    let mut out: BTreeMap<i64, Vec<pb::types::v1::Exemplar>> = BTreeMap::new();
    for ((timestamp, _fingerprint), value) in per_profile {
        out.entry(step_bucket_ms(timestamp, step_ms))
            .or_default()
            .push(pb::types::v1::Exemplar {
                timestamp,
                profile_id: profile_id.to_string(),
                span_id: String::new(),
                value,
                labels: label_pairs.clone(),
            });
    }
    Ok(out)
}

async fn individual_exemplars_from_totals(
    scan: &crabka_pprof::ProfileScan,
    step_ms: i64,
    labels: &[(String, String)],
    profile_id: &str,
) -> Result<BTreeMap<i64, Vec<pb::types::v1::Exemplar>>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, MAX({total}) AS total \
         FROM {table} GROUP BY {timestamp}, {fingerprint} \
         ORDER BY {timestamp}, {fingerprint}",
        timestamp = COL_TIMESTAMP,
        total = PCOL_TOTAL_VALUE,
        table = scan.samples_table,
        fingerprint = COL_FINGERPRINT,
    );
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    let label_pairs = types_label_pairs(labels.to_vec());
    let mut out: BTreeMap<i64, Vec<pb::types::v1::Exemplar>> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let totals = batch.column(1).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let timestamp = timestamps.value(row);
            out.entry(step_bucket_ms(timestamp, step_ms))
                .or_default()
                .push(pb::types::v1::Exemplar {
                    timestamp,
                    profile_id: profile_id.to_string(),
                    span_id: String::new(),
                    value: totals.value(row),
                    labels: label_pairs.clone(),
                });
        }
    }
    Ok(out)
}

fn frames_match_call_sites(frames: &[crabka_pprof::Frame], call_sites: &[String]) -> bool {
    call_sites.iter().all(|site| {
        frames
            .iter()
            .any(|frame| frame.function == *site || frame.file == *site)
    })
}

async fn heatmap_span_exemplars_from_scan(
    scan: &crabka_pprof::ProfileScan,
    start_ms: i64,
    end_ms: i64,
    time_buckets: usize,
    labels: &[(String, String)],
) -> Result<BTreeMap<i64, Vec<pb::querier::v1::Exemplar>>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, {fingerprint}, {span}, MAX({total}) AS total \
         FROM {table} WHERE {span} IS NOT NULL \
         GROUP BY {timestamp}, {fingerprint}, {span} \
         ORDER BY {timestamp}, {fingerprint}, {span}",
        timestamp = COL_TIMESTAMP,
        fingerprint = COL_FINGERPRINT,
        span = PCOL_SPAN_ID,
        total = PCOL_TOTAL_VALUE,
        table = scan.samples_table,
    );
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    let labels = label_pairs(labels.to_vec());
    let mut out: BTreeMap<i64, Vec<pb::querier::v1::Exemplar>> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let span_ids = batch.column(2).as_primitive::<UInt64Type>();
        let totals = batch.column(3).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            if span_ids.is_null(row) {
                continue;
            }
            let timestamp = timestamps.value(row);
            let Some(slot_timestamp) =
                heatmap_slot_timestamp(start_ms, end_ms, time_buckets, timestamp)
            else {
                continue;
            };
            out.entry(slot_timestamp)
                .or_default()
                .push(pb::querier::v1::Exemplar {
                    timestamp,
                    profile_id: String::new(),
                    span_id: format!("{:x}", span_ids.value(row)),
                    value: totals.value(row),
                    labels: labels.clone(),
                });
        }
    }
    Ok(out)
}

async fn heatmap_individual_exemplars_from_scan(
    scan: &crabka_pprof::ProfileScan,
    start_ms: i64,
    end_ms: i64,
    time_buckets: usize,
    labels: &[(String, String)],
    profile_id: &str,
) -> Result<BTreeMap<i64, Vec<pb::querier::v1::Exemplar>>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, MAX({total}) AS total \
         FROM {table} GROUP BY {timestamp}, {fingerprint} \
         ORDER BY {timestamp}, {fingerprint}",
        timestamp = COL_TIMESTAMP,
        total = PCOL_TOTAL_VALUE,
        table = scan.samples_table,
        fingerprint = COL_FINGERPRINT,
    );
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    let labels = label_pairs(labels.to_vec());
    let mut out: BTreeMap<i64, Vec<pb::querier::v1::Exemplar>> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let totals = batch.column(1).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let timestamp = timestamps.value(row);
            let Some(slot_timestamp) =
                heatmap_slot_timestamp(start_ms, end_ms, time_buckets, timestamp)
            else {
                continue;
            };
            out.entry(slot_timestamp)
                .or_default()
                .push(pb::querier::v1::Exemplar {
                    timestamp,
                    profile_id: profile_id.to_string(),
                    span_id: String::new(),
                    value: totals.value(row),
                    labels: labels.clone(),
                });
        }
    }
    Ok(out)
}

async fn span_heatmap_points_from_scan(
    scan: &crabka_pprof::ProfileScan,
) -> Result<Vec<(i64, i64)>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, MAX({total}) AS total \
         FROM {table} WHERE {span} IS NOT NULL \
         GROUP BY {timestamp}, {fingerprint}",
        timestamp = COL_TIMESTAMP,
        total = PCOL_TOTAL_VALUE,
        table = scan.samples_table,
        span = PCOL_SPAN_ID,
        fingerprint = COL_FINGERPRINT,
    );
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    let mut points = Vec::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let totals = batch.column(1).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            points.push((timestamps.value(row), totals.value(row)));
        }
    }
    Ok(points)
}

fn heatmap_slot_timestamp(
    start_ms: i64,
    end_ms: i64,
    time_buckets: usize,
    timestamp: i64,
) -> Option<i64> {
    if timestamp < start_ms || timestamp >= end_ms || start_ms >= end_ms || time_buckets == 0 {
        return None;
    }
    let time_span = i128::from(end_ms - start_ms);
    let raw = i128::from(timestamp - start_ms) * i128::try_from(time_buckets).ok()? / time_span;
    let bucket = raw.clamp(0, i128::try_from(time_buckets - 1).ok()?);
    let bucket = i64::try_from(bucket).ok()?;
    let step_ms = (end_ms - start_ms) / i64::try_from(time_buckets).ok()?;
    Some(start_ms + (bucket + 1) * step_ms)
}

pub fn router<S>(state: Arc<QuerierState<S>>) -> Router
where
    S: ProfileStore + 'static,
{
    let querier = pb::querier::v1::querier_service_connect::QuerierServiceBuilder::<()>::new()
        .profile_types(profile_types_handler::<S>)
        .label_names(label_names_handler::<S>)
        .label_values(label_values_handler::<S>)
        .series(series_handler::<S>)
        .select_merge_stacktraces(select_merge_stacktraces_handler::<S>)
        .select_merge_span_profile(select_merge_span_profile_handler::<S>)
        .select_merge_profile(select_merge_profile_handler::<S>)
        .select_series(select_series_handler::<S>)
        .select_heatmap(select_heatmap_handler::<S>)
        .diff(diff_handler::<S>)
        .get_profile_stats(get_profile_stats_handler::<S>)
        .analyze_query(analyze_query_handler::<S>)
        // `build_connect()` applies the `ConnectLayer` (protocol detection + per-request
        // `ConnectContext`); plain `.build()` omits it, which makes every Connect response
        // fall back to `application/json` regardless of the request's content-type and breaks
        // proto clients like Grafana's built-in Pyroscope datasource (a connect-go client).
        .build_connect();

    Router::new()
        .route("/pyroscope/render", get(render_handler::<S>))
        .route("/pyroscope/render-diff", get(render_diff_handler::<S>))
        .merge(querier)
        .layer(Extension(state))
}

pub async fn serve<S>(
    addr: SocketAddr,
    state: Arc<QuerierState<S>>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr>
where
    S: ProfileStore + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, router(state))
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::warn!(%err, "profiles querier server stopped with error");
        }
    });
    Ok(bound)
}

async fn profile_types_handler<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::ProfileTypesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::ProfileTypesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let req = req.0;
    let range_omitted = req.start == 0 && req.end == 0;
    let (start, end) = if range_omitted {
        (0, i64::MAX)
    } else {
        (req.start, req.end)
    };
    if !range_omitted {
        state
            .validate_query_range(&tenant, start, end)
            .map_err(connect_error)?;
    }
    let types = state
        .store
        .profile_types(&tenant, start, end)
        .await
        .map_err(connect_error)?;
    Ok(ConnectResponse::new(
        pb::querier::v1::ProfileTypesResponse {
            profile_types: types
                .into_iter()
                .map(|id| {
                    ProfileType::parse(&id).map(|parsed| pb::querier::v1::ProfileType {
                        id,
                        name: parsed.name,
                        sample_type: parsed.sample_type,
                        sample_unit: parsed.sample_unit,
                        period_type: parsed.period_type,
                        period_unit: parsed.period_unit,
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(connect_error)?,
        },
    ))
}

async fn label_names_handler<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::LabelNamesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::LabelNamesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let matchers = parse_matchers(&req.0.matchers).map_err(connect_error)?;
    state
        .validate_query_range(&tenant, req.0.start, req.0.end)
        .map_err(connect_error)?;
    let mut names = state
        .store
        .label_names(&tenant, &matchers, req.0.start, req.0.end)
        .await
        .map_err(connect_error)?;
    names.retain(|name| !is_internal_label(name));
    Ok(ConnectResponse::new(pb::querier::v1::LabelNamesResponse {
        names,
    }))
}

async fn label_values_handler<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::LabelValuesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::LabelValuesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let matchers = parse_matchers(&req.0.matchers).map_err(connect_error)?;
    state
        .validate_query_range(&tenant, req.0.start, req.0.end)
        .map_err(connect_error)?;
    if is_internal_label(&req.0.name) {
        return Ok(ConnectResponse::new(pb::querier::v1::LabelValuesResponse {
            names: Vec::new(),
        }));
    }
    let names = state
        .store
        .label_values(&tenant, &req.0.name, &matchers, req.0.start, req.0.end)
        .await
        .map_err(connect_error)?;
    Ok(ConnectResponse::new(pb::querier::v1::LabelValuesResponse {
        names,
    }))
}

async fn series_handler<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SeriesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SeriesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let matchers = parse_matchers(&req.0.matchers).map_err(connect_error)?;
    state
        .validate_query_range(&tenant, req.0.start, req.0.end)
        .map_err(connect_error)?;
    let labels_set = state
        .store
        .series(
            &tenant,
            &matchers,
            &req.0.label_names,
            req.0.start,
            req.0.end,
        )
        .await
        .map_err(connect_error)?
        .into_iter()
        .map(|labels| pb::querier::v1::Labels {
            labels: label_pairs(labels),
        })
        .collect();
    Ok(ConnectResponse::new(pb::querier::v1::SeriesResponse {
        labels_set,
    }))
}

async fn select_merge_stacktraces_handler<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectMergeStacktracesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectMergeStacktracesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let req = req.0;
    let label_selector = merge_profile_id_selector(&req.label_selector, &req.profile_id_selector)
        .map_err(connect_error)?;
    let stack_trace_call_sites =
        stack_trace_call_sites_from_json(&req.stack_trace_selector).map_err(connect_error)?;
    let response = match req.format {
        format if format == pb::querier::v1::ProfileFormat::Tree as i32 => {
            let tree = state
                .select_merge_stacktraces_tree_with_stack_trace_selector(
                    &tenant,
                    &req.profile_type_id,
                    &label_selector,
                    req.start,
                    req.end,
                    req.max_nodes,
                    &stack_trace_call_sites,
                )
                .await
                .map_err(connect_error)?;
            pb::querier::v1::SelectMergeStacktracesResponse {
                flamegraph: None,
                tree,
                dot: String::new(),
            }
        }
        format if format == pb::querier::v1::ProfileFormat::Dot as i32 => {
            let flamegraph = state
                .select_merge_stacktraces_with_stack_trace_selector(
                    &tenant,
                    &req.profile_type_id,
                    &label_selector,
                    req.start,
                    req.end,
                    req.max_nodes,
                    &stack_trace_call_sites,
                )
                .await
                .map_err(connect_error)?;
            pb::querier::v1::SelectMergeStacktracesResponse {
                flamegraph: None,
                tree: Vec::new(),
                dot: flamegraph_dot(&flamegraph),
            }
        }
        _ => {
            let flamegraph = state
                .select_merge_stacktraces_with_stack_trace_selector(
                    &tenant,
                    &req.profile_type_id,
                    &label_selector,
                    req.start,
                    req.end,
                    req.max_nodes,
                    &stack_trace_call_sites,
                )
                .await
                .map_err(connect_error)?;
            pb::querier::v1::SelectMergeStacktracesResponse {
                flamegraph: Some(flamegraph.into()),
                tree: Vec::new(),
                dot: String::new(),
            }
        }
    };
    Ok(ConnectResponse::new(response))
}

async fn select_series_handler<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectSeriesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectSeriesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let req = req.0;
    let agg = if req.aggregation
        == pb::querier::v1::SeriesAggregationType::TimeSeriesAggregationTypeAverage as i32
    {
        SeriesAgg::Average
    } else {
        SeriesAgg::Sum
    };
    let stack_trace_call_sites = stack_trace_call_sites(req.stack_trace_selector.as_ref());
    let span_exemplars = match req.exemplar_type {
        exemplar_type if exemplar_type == pb::querier::v1::ExemplarType::Span as i32 => state
            .select_series_span_exemplars(
                &tenant,
                &req.profile_type_id,
                &req.label_selector,
                &req.group_by,
                req.step,
                req.start,
                req.end,
                &stack_trace_call_sites,
            )
            .await
            .map_err(connect_error)?,
        exemplar_type if exemplar_type == pb::querier::v1::ExemplarType::Individual as i32 => state
            .select_series_individual_exemplars(
                &tenant,
                &req.profile_type_id,
                &req.label_selector,
                &req.group_by,
                req.step,
                req.start,
                req.end,
                &stack_trace_call_sites,
            )
            .await
            .map_err(connect_error)?,
        _ => BTreeMap::new(),
    };
    let series = state
        .select_series(
            &tenant,
            &req.profile_type_id,
            &req.label_selector,
            &req.group_by,
            req.step,
            agg,
            req.start,
            req.end,
            &stack_trace_call_sites,
        )
        .await
        .map_err(connect_error)?
        .into_iter()
        .take(limit(req.limit))
        .map(|series| {
            let exemplar_points = span_exemplars.get(&series.labels);
            let labels = label_pairs(series.labels);
            pb::querier::v1::ProfileSeries {
                labels,
                points: series
                    .points
                    .into_iter()
                    .map(|(timestamp, value)| pb::querier::v1::Point {
                        timestamp,
                        value,
                        annotations: Vec::new(),
                        exemplars: exemplar_points
                            .and_then(|points| points.get(&timestamp))
                            .cloned()
                            .unwrap_or_default(),
                    })
                    .collect(),
            }
        })
        .collect();
    Ok(ConnectResponse::new(
        pb::querier::v1::SelectSeriesResponse { series },
    ))
}

async fn select_merge_span_profile_handler<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectMergeSpanProfileRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectMergeSpanProfileResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let req = req.0;
    let span_ids = parse_span_selectors(&req.span_selector).map_err(connect_error)?;
    let response = if req.format == pb::querier::v1::ProfileFormat::Tree as i32 {
        let tree = state
            .select_merge_span_profile_tree(
                &tenant,
                &req.profile_type_id,
                &req.label_selector,
                &span_ids,
                req.start,
                req.end,
                req.max_nodes,
            )
            .await
            .map_err(connect_error)?;
        pb::querier::v1::SelectMergeSpanProfileResponse {
            flamegraph: None,
            tree,
        }
    } else {
        let flamegraph = state
            .select_merge_span_profile(
                &tenant,
                &req.profile_type_id,
                &req.label_selector,
                &span_ids,
                req.start,
                req.end,
                req.max_nodes,
            )
            .await
            .map_err(connect_error)?;
        pb::querier::v1::SelectMergeSpanProfileResponse {
            flamegraph: Some(flamegraph.into()),
            tree: Vec::new(),
        }
    };
    Ok(ConnectResponse::new(response))
}

async fn select_merge_profile_handler<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectMergeProfileRequest>,
) -> Result<ConnectResponse<pb::google::v1::Profile>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let req = req.0;
    let label_selector = merge_profile_id_selector(&req.label_selector, &req.profile_id_selector)
        .map_err(connect_error)?;
    let stack_trace_call_sites = stack_trace_call_sites(req.stack_trace_selector.as_ref());
    state
        .validate_query_range(&tenant, req.start, req.end)
        .map_err(connect_error)?;
    let max_nodes = state.effective_max_nodes(&tenant, req.max_nodes);
    let profile = state
        .engine
        .select_merge_profile_with_max_nodes_and_stack_trace_selector(
            &tenant,
            &req.profile_type_id,
            &label_selector,
            req.start,
            req.end,
            max_nodes,
            &stack_trace_call_sites,
        )
        .await
        .map_err(connect_error)?;
    let profile = pb::google::v1::Profile::decode(profile.as_slice())
        .map_err(|err| connect_error(ProfileError::Decode(err.to_string())))?;
    Ok(ConnectResponse::new(profile))
}

async fn select_heatmap_handler<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::SelectHeatmapRequest>,
) -> Result<ConnectResponse<pb::querier::v1::SelectHeatmapResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let req = req.0;
    state
        .validate_query_range(&tenant, req.start, req.end)
        .map_err(connect_error)?;
    let time_buckets = heatmap_time_buckets(req.start, req.end, req.step).map_err(connect_error)?;
    let span_exemplars = match req.exemplar_type {
        exemplar_type if exemplar_type == pb::querier::v1::ExemplarType::Span as i32 => state
            .select_heatmap_span_exemplars(
                &tenant,
                &req.profile_type_id,
                &req.label_selector,
                &req.group_by,
                req.start,
                req.end,
                time_buckets,
            )
            .await
            .map_err(connect_error)?,
        exemplar_type if exemplar_type == pb::querier::v1::ExemplarType::Individual as i32 => state
            .select_heatmap_individual_exemplars(
                &tenant,
                &req.profile_type_id,
                &req.label_selector,
                &req.group_by,
                req.start,
                req.end,
                time_buckets,
            )
            .await
            .map_err(connect_error)?,
        _ => BTreeMap::new(),
    };
    let heatmaps = if req.query_type == pb::querier::v1::HeatmapQueryType::Span as i32 {
        state
            .select_span_heatmaps(
                &tenant,
                &req.profile_type_id,
                &req.label_selector,
                &req.group_by,
                req.start,
                req.end,
                time_buckets,
                DEFAULT_HEATMAP_VALUE_BUCKETS,
            )
            .await
    } else {
        state
            .engine
            .select_heatmaps(
                &tenant,
                &req.profile_type_id,
                &req.label_selector,
                &req.group_by,
                req.start,
                req.end,
                time_buckets,
                DEFAULT_HEATMAP_VALUE_BUCKETS,
            )
            .await
    }
    .map_err(connect_error)?;
    let series = heatmaps
        .into_iter()
        .take(limit(req.limit))
        .map(|heatmap| {
            let exemplar_slots = span_exemplars.get(&heatmap.labels);
            let mut series = pb::querier::v1::HeatmapSeries::from(heatmap);
            if let Some(exemplar_slots) = exemplar_slots {
                for slot in &mut series.slots {
                    slot.exemplars = exemplar_slots
                        .get(&slot.timestamp)
                        .cloned()
                        .unwrap_or_default();
                }
            }
            series
        })
        .collect();
    Ok(ConnectResponse::new(
        pb::querier::v1::SelectHeatmapResponse { series },
    ))
}

async fn diff_handler<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::DiffRequest>,
) -> Result<ConnectResponse<pb::querier::v1::DiffResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let left = req
        .0
        .left
        .ok_or_else(|| connect_error(ProfileError::Plan("missing left query".to_string())))?;
    let right = req
        .0
        .right
        .ok_or_else(|| connect_error(ProfileError::Plan("missing right query".to_string())))?;
    state
        .validate_query_range(&tenant, left.start, left.end)
        .map_err(connect_error)?;
    state
        .validate_query_range(&tenant, right.start, right.end)
        .map_err(connect_error)?;
    let left_label_selector =
        merge_profile_id_selector(&left.label_selector, &left.profile_id_selector)
            .map_err(connect_error)?;
    let right_label_selector =
        merge_profile_id_selector(&right.label_selector, &right.profile_id_selector)
            .map_err(connect_error)?;
    let left_call_sites =
        stack_trace_call_sites_from_json(&left.stack_trace_selector).map_err(connect_error)?;
    let right_call_sites =
        stack_trace_call_sites_from_json(&right.stack_trace_selector).map_err(connect_error)?;
    let max_nodes = state.effective_max_nodes(&tenant, left.max_nodes.max(right.max_nodes));
    let flamegraph = state
        .engine
        .diff_with_stack_trace_selector(
            &tenant,
            (
                &left.profile_type_id,
                &left_label_selector,
                left.start,
                left.end,
            ),
            (
                &right.profile_type_id,
                &right_label_selector,
                right.start,
                right.end,
            ),
            max_nodes,
            &left_call_sites,
            &right_call_sites,
        )
        .await
        .map_err(connect_error)?;
    Ok(ConnectResponse::new(pb::querier::v1::DiffResponse {
        flamegraph: Some(flamegraph.into()),
    }))
}

async fn get_profile_stats_handler<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::GetProfileStatsRequest>,
) -> Result<ConnectResponse<pb::querier::v1::GetProfileStatsResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    state
        .validate_query_range(&tenant, req.0.start, req.0.end)
        .map_err(connect_error)?;
    let stats = state
        .store
        .stats(&tenant, req.0.start, req.0.end)
        .await
        .map_err(connect_error)?;
    Ok(ConnectResponse::new(
        pb::querier::v1::GetProfileStatsResponse {
            data_ingested: stats.data_ingested,
            oldest_profile_time: stats.oldest_profile_time.unwrap_or_default(),
            newest_profile_time: stats.newest_profile_time.unwrap_or_default(),
        },
    ))
}

async fn analyze_query_handler<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::AnalyzeQueryRequest>,
) -> Result<ConnectResponse<pb::querier::v1::AnalyzeQueryResponse>, ConnectError>
where
    S: ProfileStore,
{
    let tenant = tenant_from_headers(&headers).map_err(connect_error)?;
    let req = req.0;
    state
        .validate_query_range(&tenant, req.start, req.end)
        .map_err(connect_error)?;
    let (profile_type, selector) = parse_render_query(&req.query).map_err(connect_error)?;
    let selector = merge_profile_type_selector(&selector, &profile_type).map_err(connect_error)?;
    let matchers = parse_label_selector(&selector).map_err(connect_error)?;
    let mut label_names = state
        .store
        .label_names(&tenant, &matchers, req.start, req.end)
        .await
        .map_err(connect_error)?;
    label_names.retain(|name| !is_internal_label(name));
    let series_count = state
        .store
        .series(&tenant, &matchers, &label_names, req.start, req.end)
        .await
        .map_err(connect_error)?
        .len() as u64;
    let response = pb::querier::v1::AnalyzeQueryResponse {
        query_scopes: vec![pb::querier::v1::QueryScope {
            component_type: "Long term storage".to_string(),
            component_count: u64::from(series_count > 0),
            block_count: 0,
            series_count,
            profile_count: 0,
            sample_count: 0,
            index_bytes: 0,
            profile_bytes: 0,
            symbol_bytes: 0,
        }],
        query_impact: Some(pb::querier::v1::QueryImpact {
            total_bytes_in_time_range: 0,
            total_queried_series: series_count,
            deduplication_needed: false,
        }),
    };
    Ok(ConnectResponse::new(response))
}

#[derive(Debug, Deserialize)]
struct RenderQuery {
    query: String,
    from: Option<String>,
    until: Option<String>,
    #[serde(rename = "maxNodes")]
    max_nodes: Option<i64>,
    #[serde(default, rename = "groupBy", deserialize_with = "deserialize_group_by")]
    group_by: Vec<String>,
    format: Option<String>,
}

fn deserialize_group_by<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }

    let value = Option::<OneOrMany>::deserialize(deserializer)?;
    let values = match value {
        Some(OneOrMany::One(value)) => vec![value],
        Some(OneOrMany::Many(values)) => values,
        None => Vec::new(),
    };
    Ok(values
        .into_iter()
        .flat_map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .collect())
}

async fn render_handler<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    Query(query): Query<RenderQuery>,
) -> Response
where
    S: ProfileStore,
{
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(err) => return profile_error_response(err),
    };
    let (profile_type, selector) = match parse_render_query(&query.query) {
        Ok(parsed) => parsed,
        Err(err) => return profile_error_response(err),
    };
    let now_ms = unix_now_ms();
    let start = match parse_render_time_param(query.from.as_deref(), now_ms, 0) {
        Ok(value) => value,
        Err(err) => return profile_error_response(err),
    };
    let end = match parse_render_time_param(query.until.as_deref(), now_ms, i64::MAX) {
        Ok(value) => value,
        Err(err) => return profile_error_response(err),
    };
    match state
        .select_merge_stacktraces_grouped(
            &tenant,
            &profile_type,
            &selector,
            start,
            end,
            query.max_nodes.unwrap_or(0),
            &query.group_by,
        )
        .await
    {
        Ok(flamegraph)
            if query
                .format
                .as_deref()
                .is_some_and(|format| format.eq_ignore_ascii_case("dot")) =>
        {
            (
                [(axum::http::header::CONTENT_TYPE, "text/vnd.graphviz")],
                flamegraph_dot(&flamegraph),
            )
                .into_response()
        }
        Ok(flamegraph) => Json(flamebearer_json(flamegraph, &profile_type)).into_response(),
        Err(err) => profile_error_response(err),
    }
}

async fn render_diff_handler<S>(
    Extension(state): Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response
where
    S: ProfileStore,
{
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(err) => return profile_error_response(err),
    };
    let params = url::form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .into_owned()
        .collect::<Vec<_>>();
    let left_query = params
        .iter()
        .find(|(name, _)| name == "leftQuery" || name == "query")
        .map(|(_, value)| value.as_str())
        .unwrap_or("");
    let right_query = params
        .iter()
        .find(|(name, _)| name == "rightQuery")
        .map(|(_, value)| value.as_str())
        .unwrap_or(left_query);
    let (left_type, left_selector) = match parse_render_query(left_query) {
        Ok(parsed) => parsed,
        Err(err) => return profile_error_response(err),
    };
    let (right_type, right_selector) = match parse_render_query(right_query) {
        Ok(parsed) => parsed,
        Err(err) => return profile_error_response(err),
    };
    let now_ms = unix_now_ms();
    let global_start = match query_param_render_time(&params, "from", now_ms, 0) {
        Ok(value) => value,
        Err(err) => return profile_error_response(err),
    };
    let global_end = match query_param_render_time(&params, "until", now_ms, i64::MAX) {
        Ok(value) => value,
        Err(err) => return profile_error_response(err),
    };
    let left_start = match query_param_render_time(&params, "leftFrom", now_ms, global_start) {
        Ok(value) => value,
        Err(err) => return profile_error_response(err),
    };
    let left_end = match query_param_render_time(&params, "leftUntil", now_ms, global_end) {
        Ok(value) => value,
        Err(err) => return profile_error_response(err),
    };
    let right_start = match query_param_render_time(&params, "rightFrom", now_ms, global_start) {
        Ok(value) => value,
        Err(err) => return profile_error_response(err),
    };
    let right_end = match query_param_render_time(&params, "rightUntil", now_ms, global_end) {
        Ok(value) => value,
        Err(err) => return profile_error_response(err),
    };
    if let Err(err) = state.validate_query_range(&tenant, left_start, left_end) {
        return profile_error_response(err);
    }
    if let Err(err) = state.validate_query_range(&tenant, right_start, right_end) {
        return profile_error_response(err);
    }
    match state
        .engine
        .diff(
            &tenant,
            (&left_type, &left_selector, left_start, left_end),
            (&right_type, &right_selector, right_start, right_end),
            state.effective_max_nodes(&tenant, query_param_i64(&params, "maxNodes").unwrap_or(0)),
        )
        .await
    {
        Ok(diff) => Json(flamebearer_diff_json(diff, &left_type)).into_response(),
        Err(err) => profile_error_response(err),
    }
}

/// Resolve and validate the tenant from the `X-Scope-OrgID` header.
///
/// Absent, empty, or non-UTF-8 headers resolve to the anonymous tenant; a
/// present, non-empty header is validated against the tenant charset (see
/// [`crate::tenant::validate_tenant`]). An invalid tenant id is surfaced as a
/// [`ProfileError::Plan`] (Connect `invalid_argument` / legacy 400) with a
/// generic, non-leaky message rather than being used as a storage key.
fn tenant_from_headers(headers: &HeaderMap) -> Result<String, ProfileError> {
    let header = headers
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok());
    crate::tenant::tenant_from_header(header).map_err(|_| {
        // `validate_tenant` already returns a generic, non-leaky message; keep
        // it generic here too so we never echo an attacker-supplied tenant id.
        ProfileError::Plan("invalid tenant id".to_string())
    })
}

fn parse_matchers(
    matchers: &[String],
) -> Result<Vec<crabka_blockstore::LabelMatcher>, ProfileError> {
    let mut out = Vec::new();
    for matcher in matchers {
        out.extend(parse_label_selector(matcher)?);
    }
    Ok(out)
}

fn parse_render_query(query: &str) -> Result<(String, String), ProfileError> {
    let trimmed = query.trim();
    let Some(open) = trimmed.find('{') else {
        if trimmed.is_empty() {
            return Err(ProfileError::Plan("missing query".to_string()));
        }
        return Ok((trimmed.to_string(), "{}".to_string()));
    };
    let profile_type = trimmed[..open].trim();
    let selector = &trimmed[open..];
    if profile_type.is_empty() {
        return Err(ProfileError::Plan("missing profile type".to_string()));
    }
    parse_label_selector(selector)?;
    Ok((profile_type.to_string(), selector.to_string()))
}

fn parse_span_selectors(selectors: &[String]) -> Result<Vec<u64>, ProfileError> {
    selectors
        .iter()
        .map(|selector| {
            let trimmed = selector.trim();
            trimmed
                .parse::<u64>()
                .or_else(|_| u64::from_str_radix(trimmed.strip_prefix("0x").unwrap_or(trimmed), 16))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ProfileError::Plan(format!("invalid span_selector: {err}")))
}

fn stack_trace_call_sites(selector: Option<&pb::types::v1::StackTraceSelector>) -> Vec<String> {
    selector
        .map(|selector| {
            selector
                .call_site
                .iter()
                .map(|location| location.name.trim())
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

#[derive(Debug, Deserialize)]
struct StackTraceSelectorJson {
    #[serde(default, rename = "callSite")]
    call_site: Vec<StackTraceLocationJson>,
}

#[derive(Debug, Deserialize)]
struct StackTraceLocationJson {
    name: String,
}

fn stack_trace_call_sites_from_json(selector: &str) -> Result<Vec<String>, ProfileError> {
    let selector = selector.trim();
    if selector.is_empty() {
        return Ok(Vec::new());
    }
    let selector: StackTraceSelectorJson = serde_json::from_str(selector)
        .map_err(|err| ProfileError::Plan(format!("invalid stack_trace_selector: {err}")))?;
    Ok(selector
        .call_site
        .into_iter()
        .map(|location| location.name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect())
}

fn heatmap_time_buckets(start_ms: i64, end_ms: i64, step_secs: f64) -> Result<usize, ProfileError> {
    if start_ms >= end_ms {
        return Err(ProfileError::Plan(
            "heatmap start must be before end".to_string(),
        ));
    }
    let step_ms = step_ms_from_secs(step_secs)? as f64;
    let span_ms = (end_ms - start_ms) as f64;
    Ok(((span_ms / step_ms).ceil().max(1.0) as usize).min(MAX_HEATMAP_TIME_BUCKETS))
}

fn query_param_i64(params: &[(String, String)], name: &str) -> Option<i64> {
    params
        .iter()
        .find(|(key, _)| key == name)
        .and_then(|(_, value)| value.parse().ok())
}

fn query_param_render_time(
    params: &[(String, String)],
    name: &str,
    now_ms: i64,
    default: i64,
) -> Result<i64, ProfileError> {
    let value = params
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str());
    parse_render_time_param(value, now_ms, default)
}

fn parse_render_time_param(
    value: Option<&str>,
    now_ms: i64,
    default: i64,
) -> Result<i64, ProfileError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(default);
    };
    if value == "now" {
        return Ok(now_ms);
    }
    if let Some(offset) = value.strip_prefix("now-") {
        let resolved = now_ms - parse_render_duration_ms(offset)?;
        return reject_negative_render_time(resolved, value);
    }
    let numeric = value
        .parse::<i64>()
        .map_err(|err| ProfileError::Plan(format!("invalid render time {value:?}: {err}")))?;
    reject_negative_render_time(normalize_render_unix_time(numeric), value)
}

/// Reject a render time bound that resolved to a negative millisecond value.
///
/// A `now-<offset>` larger than `now` (clock skew / huge lookback) or a literal
/// negative timestamp both yield a negative bound, which is never a valid Unix
/// time and would otherwise be passed downstream as a query window edge.
fn reject_negative_render_time(resolved_ms: i64, raw: &str) -> Result<i64, ProfileError> {
    if resolved_ms < 0 {
        return Err(ProfileError::Plan(format!(
            "render time {raw:?} resolves to a negative timestamp"
        )));
    }
    Ok(resolved_ms)
}

fn normalize_render_unix_time(value: i64) -> i64 {
    if value.abs() < 10_000_000_000 {
        value.saturating_mul(1000)
    } else {
        value
    }
}

fn parse_render_duration_ms(value: &str) -> Result<i64, ProfileError> {
    let (number, unit) = value.split_at(value.len().saturating_sub(1));
    let amount = number.parse::<i64>().map_err(|err| {
        ProfileError::Plan(format!("invalid render relative duration {value:?}: {err}"))
    })?;
    let multiplier = match unit {
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => {
            return Err(ProfileError::Plan(format!(
                "invalid render relative duration unit {unit:?}"
            )));
        }
    };
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| ProfileError::Plan(format!("render relative duration overflows: {value}")))
}

fn unix_now_ms() -> i64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis());
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn flamebearer_json(flamegraph: crabka_pprof::FlameGraph, profile_type: &str) -> serde_json::Value {
    json!({
        "flamebearer": {
            "names": flamegraph.names,
            "levels": flamegraph.levels.into_iter().map(|level| level.values).collect::<Vec<_>>(),
            "numTicks": flamegraph.total,
            "maxSelf": flamegraph.max_self,
        },
        "metadata": flamebearer_metadata("single", profile_type)
    })
}

fn flamebearer_diff_json(
    diff: crabka_pprof::FlameGraphDiff,
    profile_type: &str,
) -> serde_json::Value {
    let max_self = diff
        .levels
        .iter()
        .flat_map(|level| level.values.chunks_exact(7))
        .fold(0_i64, |max_self, bar| max_self.max(bar[2]).max(bar[5]));
    json!({
        "flamebearer": {
            "names": diff.names,
            "levels": diff.levels.into_iter().map(|level| level.values).collect::<Vec<_>>(),
            "numTicks": diff.left_ticks + diff.right_ticks,
            "maxSelf": max_self,
            "leftTicks": diff.left_ticks,
            "rightTicks": diff.right_ticks,
        },
        "metadata": flamebearer_metadata("double", profile_type)
    })
}

fn flamebearer_metadata(format: &str, profile_type: &str) -> serde_json::Value {
    match ProfileType::parse(profile_type) {
        Ok(parsed) => json!({
            "format": format,
            "spyName": parsed.name,
            "sampleRate": 100,
            "units": parsed.sample_unit,
            "name": profile_type,
        }),
        Err(_) => json!({
            "format": format,
            "spyName": "",
            "sampleRate": 100,
            "units": "",
            "name": profile_type,
        }),
    }
}

fn flamegraph_dot(flamegraph: &crabka_pprof::FlameGraph) -> String {
    #[derive(Clone)]
    struct DotBar {
        id: usize,
        x_start: i64,
        total: i64,
    }

    let mut dot = String::from("digraph flamegraph {\n  node [shape=box];\n");
    let mut previous = Vec::<DotBar>::new();
    let mut next_id = 0_usize;
    for level in &flamegraph.levels {
        let mut current = Vec::new();
        let mut previous_end = 0_i64;
        for bar in level.values.chunks_exact(4) {
            let x_start = previous_end + bar[0];
            let total = bar[1];
            let self_ = bar[2];
            let name_idx = usize::try_from(bar[3]).unwrap_or(usize::MAX);
            let name = flamegraph
                .names
                .get(name_idx)
                .cloned()
                .unwrap_or_else(|| format!("unknown:{name_idx}"));
            let id = next_id;
            next_id += 1;
            dot.push_str(&format!(
                "  n{id} [label=\"{}\\ntotal={} self={}\"];\n",
                dot_escape(&name),
                total,
                self_,
            ));
            if let Some(parent) = previous
                .iter()
                .find(|parent| x_start >= parent.x_start && x_start < parent.x_start + parent.total)
            {
                dot.push_str(&format!("  n{} -> n{id};\n", parent.id));
            }
            current.push(DotBar { id, x_start, total });
            previous_end = x_start + total;
        }
        previous = current;
    }
    dot.push_str("}\n");
    dot
}

fn dot_escape(value: &str) -> String {
    value
        .chars()
        .flat_map(|ch| match ch {
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            _ => vec![ch],
        })
        .collect()
}

fn merge_profile_id_selector(
    label_selector: &str,
    profile_ids: &[String],
) -> Result<String, ProfileError> {
    if profile_ids.is_empty() {
        return Ok(label_selector.to_string());
    }

    let matcher = if profile_ids.len() == 1 {
        format!(
            r#"{PROFILE_ID_LABEL}="{}""#,
            label_matcher_value_escape(&profile_ids[0])
        )
    } else {
        let regex = profile_ids
            .iter()
            .map(|value| regex::escape(value))
            .collect::<Vec<_>>()
            .join("|");
        format!(
            r#"{PROFILE_ID_LABEL}=~"^(?:{})$""#,
            label_matcher_value_escape(&regex)
        )
    };

    let trimmed = label_selector.trim();
    let merged = if trimmed.is_empty() || trimmed == "{}" {
        format!("{{{matcher}}}")
    } else if let Some(inner) = trimmed.strip_prefix('{') {
        let inner = inner
            .strip_suffix('}')
            .ok_or_else(|| ProfileError::Plan("unclosed label selector".to_string()))?
            .trim();
        if inner.is_empty() {
            format!("{{{matcher}}}")
        } else {
            format!("{{{inner},{matcher}}}")
        }
    } else {
        format!("{{{trimmed},{matcher}}}")
    };

    parse_label_selector(&merged)?;
    Ok(merged)
}

fn merge_profile_type_selector(
    label_selector: &str,
    profile_type: &str,
) -> Result<String, ProfileError> {
    merge_label_matcher(
        label_selector,
        &format!(
            r#"{LABEL_PROFILE_TYPE}="{}""#,
            label_matcher_value_escape(profile_type)
        ),
    )
}

fn merge_label_matcher(label_selector: &str, matcher: &str) -> Result<String, ProfileError> {
    let trimmed = label_selector.trim();
    let merged = if trimmed.is_empty() || trimmed == "{}" {
        format!("{{{matcher}}}")
    } else if let Some(inner) = trimmed.strip_prefix('{') {
        let inner = inner
            .strip_suffix('}')
            .ok_or_else(|| ProfileError::Plan("unclosed label selector".to_string()))?
            .trim();
        if inner.is_empty() {
            format!("{{{matcher}}}")
        } else {
            format!("{{{inner},{matcher}}}")
        }
    } else {
        format!("{{{trimmed},{matcher}}}")
    };

    parse_label_selector(&merged)?;
    Ok(merged)
}

fn label_matcher_value_escape(value: &str) -> String {
    value
        .chars()
        .flat_map(|ch| match ch {
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            '\t' => "\\t".chars().collect::<Vec<_>>(),
            _ => vec![ch],
        })
        .collect()
}

/// Map a [`ProfileError`] to a legacy flamebearer HTTP response.
///
/// Mirrors [`connect_error`]'s code mapping: client-shaped errors
/// (`Decode`/`Plan`/`Unsupported` — including limit/range violations surfaced as
/// `Plan`) keep their user-facing message at 400, while internal failures
/// (`Exec`/`Store`/`Symbolize`) return a generic 500 and log the detail via
/// tracing so raw DataFusion/internal text never reaches the client.
fn profile_error_response(err: ProfileError) -> Response {
    match err {
        ProfileError::Decode(_) | ProfileError::Plan(_) | ProfileError::Unsupported(_) => {
            (StatusCode::BAD_REQUEST, err.to_string()).into_response()
        }
        ProfileError::Exec(_) | ProfileError::Store(_) | ProfileError::Symbolize(_) => {
            tracing::error!(%err, "profiles querier internal error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            )
                .into_response()
        }
    }
}

fn connect_error(err: ProfileError) -> ConnectError {
    let code = match err {
        ProfileError::Decode(_) | ProfileError::Plan(_) | ProfileError::Unsupported(_) => {
            Code::InvalidArgument
        }
        ProfileError::Exec(_) | ProfileError::Store(_) | ProfileError::Symbolize(_) => {
            Code::Internal
        }
    };
    ConnectError::new(code, err.to_string())
}

fn label_pairs(labels: Vec<(String, String)>) -> Vec<pb::querier::v1::LabelPair> {
    labels
        .into_iter()
        .map(|(name, value)| pb::querier::v1::LabelPair { name, value })
        .collect()
}

fn types_label_pairs(labels: Vec<(String, String)>) -> Vec<pb::types::v1::LabelPair> {
    labels
        .into_iter()
        .map(|(name, value)| pb::types::v1::LabelPair { name, value })
        .collect()
}

fn limit(limit: i64) -> usize {
    usize::try_from(limit)
        .ok()
        .filter(|limit| *limit > 0)
        .unwrap_or(usize::MAX)
}

impl From<crabka_pprof::FlameGraph> for pb::querier::v1::FlameGraph {
    fn from(value: crabka_pprof::FlameGraph) -> Self {
        Self {
            names: value.names,
            levels: value
                .levels
                .into_iter()
                .map(|level| pb::querier::v1::Level {
                    values: level.values,
                })
                .collect(),
            total: value.total,
            max_self: value.max_self,
        }
    }
}

impl From<crabka_pprof::FlameGraphDiff> for pb::querier::v1::FlameGraphDiff {
    fn from(value: crabka_pprof::FlameGraphDiff) -> Self {
        let max_self = value
            .levels
            .iter()
            .flat_map(|level| level.values.chunks_exact(7))
            .fold(0, |max_self, bar| max_self.max(bar[2]).max(bar[5]));
        let total = value.left_ticks + value.right_ticks;
        Self {
            names: value.names,
            levels: value
                .levels
                .into_iter()
                .map(|level| pb::querier::v1::Level {
                    values: level.values,
                })
                .collect(),
            total,
            max_self,
            left_ticks: value.left_ticks,
            right_ticks: value.right_ticks,
        }
    }
}

impl From<crabka_pprof::Heatmap> for pb::querier::v1::HeatmapSeries {
    fn from(value: crabka_pprof::Heatmap) -> Self {
        let step_ms = if value.time_buckets == 0 {
            0
        } else {
            (value.end_ms - value.start_ms)
                / i64::try_from(value.time_buckets).expect("bucket count fits i64")
        };
        let y_min = heatmap_y_mins(value.min_value, value.max_value, value.value_buckets);
        Self {
            labels: Vec::new(),
            slots: value
                .counts
                .into_iter()
                .enumerate()
                .map(|(bucket, counts)| pb::querier::v1::HeatmapSlot {
                    timestamp: value.start_ms
                        + (i64::try_from(bucket).expect("bucket index fits i64") + 1) * step_ms,
                    y_min: y_min.clone(),
                    counts: counts
                        .into_iter()
                        .map(|count| i32::try_from(count).unwrap_or(i32::MAX))
                        .collect(),
                    exemplars: Vec::new(),
                })
                .collect(),
        }
    }
}

impl From<crabka_pprof::LabeledHeatmap> for pb::querier::v1::HeatmapSeries {
    fn from(value: crabka_pprof::LabeledHeatmap) -> Self {
        let mut series = Self::from(value.heatmap);
        series.labels = label_pairs(value.labels);
        series
    }
}

fn heatmap_y_mins(min_value: i64, max_value: i64, value_buckets: usize) -> Vec<f64> {
    if value_buckets == 0 {
        return Vec::new();
    }
    let span = (max_value - min_value).max(0) as f64;
    (0..value_buckets)
        .map(|bucket| min_value as f64 + span * bucket as f64 / value_buckets as f64)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use base64::Engine;
    use crabka_pprof::{FunctionRec, LineRec, LocationRec};

    use super::*;
    use crate::{Limits, OverridesProvider};

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    fn store_with_frame(name: &str) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name_ref = store.symbols_mut().intern_string(name);
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        store.push_sample(
            "tenant-a",
            PT,
            vec![("service_name".to_string(), "api".to_string())],
            0,
            stacktrace,
            7,
            10,
        );
        store
    }

    fn store_with_two_profile_types() -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name_ref = store.symbols_mut().intern_string("main.work");
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        for profile_type in [PT, "memory:alloc_space:bytes:space:bytes"] {
            store.push_sample(
                "tenant-a",
                profile_type,
                vec![
                    ("service_name".to_string(), "api".to_string()),
                    ("__profile_type__".to_string(), profile_type.to_string()),
                ],
                0,
                stacktrace,
                7,
                10,
            );
        }
        store
    }

    fn store_with_span_frame(name: &str, span_id: u64) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name_ref = store.symbols_mut().intern_string(name);
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        store.push_sample_with_total_and_span(
            "tenant-a",
            PT,
            vec![("service_name".to_string(), "api".to_string())],
            0,
            stacktrace,
            7,
            7,
            10,
            span_id,
        );
        store
    }

    fn store_with_span_leaf_frames(frames: &[(&str, u64, i64)]) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        for (name, span_id, value) in frames {
            let name_ref = store.symbols_mut().intern_string(name);
            let function_id = store.symbols_mut().intern_function(FunctionRec {
                name: name_ref,
                system_name: name_ref,
                filename: 0,
                start_line: 0,
            });
            let location_id = store.symbols_mut().intern_location(LocationRec {
                address: 0,
                mapping_id: 0,
                lines: vec![LineRec {
                    function_id,
                    line: 1,
                }],
            });
            let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
            store.push_sample_with_total_and_span(
                "tenant-a",
                PT,
                vec![("service_name".to_string(), "api".to_string())],
                0,
                stacktrace,
                *value,
                *value,
                10,
                *span_id,
            );
        }
        store
    }

    fn store_with_frame_samples(name: &str, samples: &[(i64, i64)]) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name_ref = store.symbols_mut().intern_string(name);
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        for (timestamp, value) in samples {
            store.push_sample(
                "tenant-a",
                PT,
                vec![("service_name".to_string(), "api".to_string())],
                0,
                stacktrace,
                *value,
                *timestamp,
            );
        }
        store
    }

    fn store_with_services(samples: &[(&str, &str, i64)]) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name_ref = store.symbols_mut().intern_string("main.work");
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        for (service, env, value) in samples {
            store.push_sample(
                "tenant-a",
                PT,
                vec![
                    ("service_name".to_string(), (*service).to_string()),
                    ("env".to_string(), (*env).to_string()),
                ],
                0,
                stacktrace,
                *value,
                10,
            );
        }
        store
    }

    fn store_with_leaf_frames(frames: &[(&str, i64)]) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        for (name, value) in frames {
            let name_ref = store.symbols_mut().intern_string(name);
            let function_id = store.symbols_mut().intern_function(FunctionRec {
                name: name_ref,
                system_name: name_ref,
                filename: 0,
                start_line: 0,
            });
            let location_id = store.symbols_mut().intern_location(LocationRec {
                address: 0,
                mapping_id: 0,
                lines: vec![LineRec {
                    function_id,
                    line: 1,
                }],
            });
            let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
            store.push_sample(
                "tenant-a",
                PT,
                vec![("service_name".to_string(), "api".to_string())],
                0,
                stacktrace,
                *value,
                10,
            );
        }
        store
    }

    fn store_with_profile_ids() -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name_ref = store.symbols_mut().intern_string("main.work");
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        for (profile_id, value) in [("profile-a", 5), ("profile-b", 7)] {
            store.push_sample(
                "tenant-a",
                PT,
                vec![
                    ("service_name".to_string(), "api".to_string()),
                    ("__profile_id__".to_string(), profile_id.to_string()),
                ],
                0,
                stacktrace,
                value,
                10,
            );
        }
        store
    }

    fn store_with_profile_ids_and_leaf_frames(
        frames: &[(&str, &str, i64)],
    ) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        for (profile_id, name, value) in frames {
            let name_ref = store.symbols_mut().intern_string(name);
            let function_id = store.symbols_mut().intern_function(FunctionRec {
                name: name_ref,
                system_name: name_ref,
                filename: 0,
                start_line: 0,
            });
            let location_id = store.symbols_mut().intern_location(LocationRec {
                address: 0,
                mapping_id: 0,
                lines: vec![LineRec {
                    function_id,
                    line: 1,
                }],
            });
            let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
            store.push_sample(
                "tenant-a",
                PT,
                vec![
                    ("service_name".to_string(), "api".to_string()),
                    ("__profile_id__".to_string(), (*profile_id).to_string()),
                ],
                0,
                stacktrace,
                *value,
                10,
            );
        }
        store
    }

    fn json_i64(value: &serde_json::Value) -> Option<i64> {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    }

    #[tokio::test]
    async fn select_series_rejects_ranges_above_configured_limit() {
        let state = QuerierState::new_with_limits(
            Arc::new(store_with_frame("main.work")),
            Limits {
                max_query_length_secs: 1,
                ..Limits::default()
            },
        );

        let err = state
            .select_series(
                "tenant-a",
                PT,
                r#"{service_name="api"}"#,
                &[],
                1.0,
                SeriesAgg::Sum,
                0,
                2_000,
                &[],
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("query length exceeded"), "{err}");
    }

    #[tokio::test]
    async fn select_series_uses_tenant_specific_query_overrides() {
        let state = QuerierState::new_with_overrides(
            Arc::new(store_with_frame("main.work")),
            OverridesProvider::from_yaml(
                r#"
overrides:
  tenant-a:
    max_query_length_secs: 1
"#,
            )
            .unwrap(),
        );

        let tenant_a_err = state
            .select_series(
                "tenant-a",
                PT,
                r#"{service_name="api"}"#,
                &[],
                1.0,
                SeriesAgg::Sum,
                0,
                2_000,
                &[],
            )
            .await
            .unwrap_err();
        let tenant_b_series = state
            .select_series(
                "tenant-b",
                PT,
                r#"{service_name="api"}"#,
                &[],
                1.0,
                SeriesAgg::Sum,
                0,
                2_000,
                &[],
            )
            .await
            .unwrap();

        assert!(
            tenant_a_err.to_string().contains("query length exceeded"),
            "{tenant_a_err}"
        );
        assert!(tenant_b_series.is_empty());
    }

    #[tokio::test]
    async fn select_merge_stacktraces_clamps_requested_nodes_to_configured_max() {
        let state = QuerierState::new_with_limits(
            Arc::new(store_with_leaf_frames(&[
                ("hot.path", 10),
                ("warm.path", 8),
                ("cold.path", 6),
            ])),
            Limits {
                max_flamegraph_nodes_default: 2048,
                max_flamegraph_nodes_max: 2,
                ..Limits::default()
            },
        );

        let flamegraph = state
            .select_merge_stacktraces("tenant-a", PT, r#"{service_name="api"}"#, 0, 100, 10_000)
            .await
            .unwrap();

        assert!(flamegraph.names.iter().any(|name| name == "other"));
        assert!(!flamegraph.names.iter().any(|name| name == "warm.path"));
        assert!(!flamegraph.names.iter().any(|name| name == "cold.path"));
    }

    #[tokio::test]
    async fn render_format_dot_returns_dot_graph() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("query", &format!(r#"{PT}{{service_name="api"}}"#))
            .append_pair("from", "0")
            .append_pair("until", "100")
            .append_pair("format", "dot")
            .finish();
        let body = reqwest::Client::new()
            .get(format!("http://{bound}/pyroscope/render?{query}"))
            .header("x-scope-orgid", "tenant-a")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .text()
            .await
            .unwrap();

        assert!(body.starts_with("digraph flamegraph"));
        assert!(body.contains("main.work"), "{body}");
    }

    #[tokio::test]
    async fn render_group_by_adds_group_frames_to_flamebearer() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_services(&[
            ("api", "prod", 5),
            ("worker", "prod", 7),
        ]))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("query", &format!(r#"{PT}{{env="prod"}}"#))
            .append_pair("from", "0")
            .append_pair("until", "100")
            .append_pair("groupBy", "service_name")
            .finish();
        let body: serde_json::Value = reqwest::Client::new()
            .get(format!("http://{bound}/pyroscope/render?{query}"))
            .header("x-scope-orgid", "tenant-a")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let names = body
            .pointer("/flamebearer/names")
            .and_then(serde_json::Value::as_array)
            .unwrap();

        assert!(
            names.iter().any(|name| name.as_str() == Some("api")),
            "{body}"
        );
        assert!(
            names.iter().any(|name| name.as_str() == Some("worker")),
            "{body}"
        );
        assert!(
            body.pointer("/flamebearer/numTicks")
                .and_then(serde_json::Value::as_i64)
                == Some(12),
            "{body}"
        );
    }

    #[tokio::test]
    async fn render_diff_flamebearer_includes_legacy_ticks_and_max_self() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("leftQuery", &format!(r#"{PT}{{service_name="api"}}"#))
            .append_pair("rightQuery", &format!(r#"{PT}{{service_name="api"}}"#))
            .append_pair("from", "0")
            .append_pair("until", "100")
            .finish();
        let body: serde_json::Value = reqwest::Client::new()
            .get(format!("http://{bound}/pyroscope/render-diff?{query}"))
            .header("x-scope-orgid", "tenant-a")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(
            body.pointer("/metadata/format")
                .and_then(serde_json::Value::as_str)
                == Some("double"),
            "{body}"
        );
        assert!(
            body.pointer("/flamebearer/numTicks")
                .and_then(serde_json::Value::as_i64)
                == Some(14),
            "{body}"
        );
        assert!(
            body.pointer("/flamebearer/maxSelf")
                .and_then(serde_json::Value::as_i64)
                == Some(7),
            "{body}"
        );
    }

    #[tokio::test]
    async fn render_diff_uses_side_specific_windows() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame_samples(
            "main.work",
            &[(1_700_000_010_000, 5), (1_700_000_090_000, 7)],
        ))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("leftQuery", &format!(r#"{PT}{{service_name="api"}}"#))
            .append_pair("leftFrom", "1700000000000")
            .append_pair("leftUntil", "1700000060000")
            .append_pair("rightQuery", &format!(r#"{PT}{{service_name="api"}}"#))
            .append_pair("rightFrom", "1700000060000")
            .append_pair("rightUntil", "1700000120000")
            .finish();
        let body: serde_json::Value = reqwest::Client::new()
            .get(format!("http://{bound}/pyroscope/render-diff?{query}"))
            .header("x-scope-orgid", "tenant-a")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(
            body.pointer("/flamebearer/leftTicks")
                .and_then(serde_json::Value::as_i64)
                == Some(5),
            "{body}"
        );
        assert!(
            body.pointer("/flamebearer/rightTicks")
                .and_then(serde_json::Value::as_i64)
                == Some(7),
            "{body}"
        );
    }

    #[tokio::test]
    async fn select_merge_stacktraces_dot_format_returns_dot_only() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeStacktraces"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "format": "PROFILE_FORMAT_DOT",
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(response.get("flamegraph").is_none(), "{response}");
        assert!(
            response
                .get("dot")
                .and_then(serde_json::Value::as_str)
                .is_some_and(
                    |dot| dot.starts_with("digraph flamegraph") && dot.contains("main.work")
                ),
            "{response}"
        );
        assert!(
            response
                .get("tree")
                .and_then(serde_json::Value::as_str)
                .is_none_or(str::is_empty),
            "{response}"
        );
    }

    #[tokio::test]
    async fn select_merge_stacktraces_tree_format_returns_pyroscope_tree_bytes() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeStacktraces"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "format": "PROFILE_FORMAT_TREE",
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(response.get("flamegraph").is_none(), "{response}");
        assert!(
            response
                .get("dot")
                .and_then(serde_json::Value::as_str)
                .is_none_or(str::is_empty),
            "{response}"
        );
        let tree = response
            .get("tree")
            .and_then(serde_json::Value::as_str)
            .and_then(|tree| base64::engine::general_purpose::STANDARD.decode(tree).ok())
            .unwrap();

        assert!(tree == b"\x00\x00\x01\x09main.work\x07\x00", "{response}");
    }

    #[tokio::test]
    async fn select_merge_span_profile_tree_format_returns_pyroscope_tree_bytes() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_span_frame(
            "main.work",
            111,
        ))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeSpanProfile"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "spanSelector": ["111"],
                "start": 0,
                "end": 100,
                "format": "PROFILE_FORMAT_TREE",
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(response.get("flamegraph").is_none(), "{response}");
        let tree = response
            .get("tree")
            .and_then(serde_json::Value::as_str)
            .and_then(|tree| base64::engine::general_purpose::STANDARD.decode(tree).ok())
            .unwrap();

        assert!(tree == b"\x00\x00\x01\x09main.work\x07\x00", "{response}");
    }

    #[tokio::test]
    async fn select_merge_stacktraces_profile_id_selector_filters_profiles() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_profile_ids())));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeStacktraces"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "profileIdSelector": ["profile-a"],
                "start": 0,
                "end": 100,
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let total = response
            .get("flamegraph")
            .and_then(|flamegraph| flamegraph.get("total"))
            .and_then(json_i64);
        assert!(total == Some(5), "{response}");
    }

    #[tokio::test]
    async fn select_merge_profile_profile_id_selector_filters_profiles() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_profile_ids())));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeProfile"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "profileIdSelector": ["profile-a"],
                "start": 0,
                "end": 100,
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(response.get("profile").is_none(), "{response}");
        let total: i64 = response
            .get("sample")
            .and_then(serde_json::Value::as_array)
            .unwrap()
            .iter()
            .flat_map(|sample| {
                sample
                    .get("value")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .filter_map(json_i64)
            .sum();

        assert!(total == 5, "{response}");
    }

    #[tokio::test]
    async fn select_merge_profile_stack_trace_selector_filters_call_sites() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_leaf_frames(&[
            ("hot.path", 7),
            ("cold.path", 10),
        ]))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeProfile"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "stackTraceSelector": {
                    "callSite": [{ "name": "hot.path" }]
                },
                "start": 0,
                "end": 100,
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(response.get("profile").is_none(), "{response}");
        let total: i64 = response
            .get("sample")
            .and_then(serde_json::Value::as_array)
            .unwrap()
            .iter()
            .flat_map(|sample| {
                sample
                    .get("value")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .filter_map(json_i64)
            .sum();

        assert!(total == 7, "{response}");
    }

    #[tokio::test]
    async fn select_merge_profile_max_nodes_truncates_to_other() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_leaf_frames(&[
            ("leaf0", 1),
            ("leaf1", 1),
            ("leaf2", 1),
            ("leaf3", 1),
            ("leaf4", 1),
            ("leaf5", 1),
            ("leaf6", 1),
            ("leaf7", 1),
            ("leaf8", 1),
            ("leaf9", 1),
        ]))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeProfile"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "maxNodes": 4,
                "start": 0,
                "end": 100,
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let samples = response
            .get("sample")
            .and_then(serde_json::Value::as_array)
            .unwrap();
        let total: i64 = samples
            .iter()
            .flat_map(|sample| {
                sample
                    .get("value")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .filter_map(json_i64)
            .sum();
        let strings = response
            .get("stringTable")
            .and_then(serde_json::Value::as_array)
            .unwrap();

        assert!(samples.len() <= 4, "{response}");
        assert!(total == 10, "{response}");
        assert!(
            strings.iter().any(|value| value.as_str() == Some("other")),
            "{response}"
        );
    }

    #[tokio::test]
    async fn select_merge_stacktraces_stack_trace_selector_filters_call_sites() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_leaf_frames(&[
            ("hot.path", 7),
            ("cold.path", 10),
        ]))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeStacktraces"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "stackTraceSelector": r#"{"callSite":[{"name":"hot.path"}]}"#,
                "start": 0,
                "end": 100,
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let total = response
            .get("flamegraph")
            .and_then(|flamegraph| flamegraph.get("total"))
            .and_then(json_i64);
        assert!(total == Some(7), "{response}");
    }

    #[tokio::test]
    async fn diff_honors_embedded_stack_trace_selectors() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_leaf_frames(&[
            ("hot.path", 7),
            ("cold.path", 10),
        ]))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!("http://{bound}/querier.v1.QuerierService/Diff"))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "left": {
                    "profileTypeID": PT,
                    "labelSelector": r#"{service_name="api"}"#,
                    "stackTraceSelector": r#"{"callSite":[{"name":"hot.path"}]}"#,
                    "start": 0,
                    "end": 100
                },
                "right": {
                    "profileTypeID": PT,
                    "labelSelector": r#"{service_name="api"}"#,
                    "stackTraceSelector": r#"{"callSite":[{"name":"cold.path"}]}"#,
                    "start": 0,
                    "end": 100
                }
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(
            response.pointer("/flamegraph/leftTicks").and_then(json_i64) == Some(7),
            "{response}"
        );
        assert!(
            response
                .pointer("/flamegraph/rightTicks")
                .and_then(json_i64)
                == Some(10),
            "{response}"
        );
    }

    #[tokio::test]
    async fn profile_types_without_time_range_returns_ingested_types() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/ProfileTypes"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let profile_types = response
            .get("profileTypes")
            .and_then(serde_json::Value::as_array)
            .unwrap();
        assert!(
            profile_types.iter().any(|profile_type| {
                profile_type
                    .get("ID")
                    .or_else(|| profile_type.get("id"))
                    .and_then(serde_json::Value::as_str)
                    == Some(PT)
            }),
            "{response}"
        );
    }

    #[tokio::test]
    async fn profile_types_health_probe_ignores_query_range_limit_when_range_omitted() {
        let state = Arc::new(QuerierState::new_with_limits(
            Arc::new(store_with_frame("main.work")),
            Limits {
                max_query_length_secs: 1,
                ..Limits::default()
            },
        ));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/ProfileTypes"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(
            response
                .get("profileTypes")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|profile_types| !profile_types.is_empty()),
            "{response}"
        );
    }

    #[tokio::test]
    async fn select_series_stack_trace_selector_filters_call_sites() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_leaf_frames(&[
            ("hot.path", 7),
            ("cold.path", 10),
        ]))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectSeries"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "groupBy": ["service_name"],
                "step": 60.0,
                "stackTraceSelector": {
                    "callSite": [{ "name": "hot.path" }]
                }
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let points = response
            .pointer("/series/0/points")
            .and_then(serde_json::Value::as_array)
            .unwrap();
        assert!(points.len() == 1, "{response}");
        assert!(
            points[0].get("value").and_then(serde_json::Value::as_f64) == Some(7.0),
            "{response}"
        );
    }

    #[tokio::test]
    async fn select_series_span_exemplar_returns_span_metadata() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_span_frame(
            "span.path",
            0x2a,
        ))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectSeries"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "groupBy": ["service_name"],
                "step": 60.0,
                "exemplarType": "EXEMPLAR_TYPE_SPAN"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let exemplar = response
            .pointer("/series/0/points/0/exemplars/0")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| panic!("missing span exemplar: {response}"));

        assert!(exemplar.get("spanId").and_then(serde_json::Value::as_str) == Some("2a"));
        assert!(exemplar.get("timestamp").and_then(json_i64) == Some(10));
        assert!(exemplar.get("value").and_then(json_i64) == Some(7));
    }

    #[tokio::test]
    async fn select_series_span_exemplar_honors_stack_trace_selector() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_span_leaf_frames(&[
            ("hot.path", 0x2a, 5),
            ("cold.path", 0x2b, 7),
        ]))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectSeries"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "groupBy": ["service_name"],
                "step": 60.0,
                "stackTraceSelector": {
                    "callSite": [{ "name": "hot.path" }]
                },
                "exemplarType": "EXEMPLAR_TYPE_SPAN"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let exemplars = response
            .pointer("/series/0/points/0/exemplars")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| panic!("missing span exemplars: {response}"));
        let span_ids: Vec<_> = exemplars
            .iter()
            .filter_map(|exemplar| exemplar.get("spanId").and_then(serde_json::Value::as_str))
            .collect();

        assert!(span_ids == vec!["2a"], "{response}");
    }

    #[tokio::test]
    async fn select_series_individual_exemplar_returns_profile_ids() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_profile_ids())));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectSeries"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "groupBy": ["service_name"],
                "step": 60.0,
                "exemplarType": "EXEMPLAR_TYPE_INDIVIDUAL"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let exemplars = response
            .pointer("/series/0/points/0/exemplars")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| panic!("missing individual exemplars: {response}"));
        let profile_ids: Vec<_> = exemplars
            .iter()
            .filter_map(|exemplar| {
                exemplar
                    .get("profileId")
                    .and_then(serde_json::Value::as_str)
            })
            .collect();

        assert!(profile_ids.contains(&"profile-a"), "{response}");
        assert!(profile_ids.contains(&"profile-b"), "{response}");
    }

    #[tokio::test]
    async fn select_series_individual_exemplar_honors_stack_trace_selector() {
        let state = Arc::new(QuerierState::new(Arc::new(
            store_with_profile_ids_and_leaf_frames(&[
                ("profile-a", "hot.path", 5),
                ("profile-b", "cold.path", 7),
            ]),
        )));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectSeries"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "groupBy": ["service_name"],
                "step": 60.0,
                "stackTraceSelector": {
                    "callSite": [{ "name": "hot.path" }]
                },
                "exemplarType": "EXEMPLAR_TYPE_INDIVIDUAL"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let exemplars = response
            .pointer("/series/0/points/0/exemplars")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| panic!("missing individual exemplars: {response}"));
        let profile_ids: Vec<_> = exemplars
            .iter()
            .filter_map(|exemplar| {
                exemplar
                    .get("profileId")
                    .and_then(serde_json::Value::as_str)
            })
            .collect();

        assert!(profile_ids == vec!["profile-a"], "{response}");
    }

    #[tokio::test]
    async fn select_heatmap_group_by_returns_labeled_series() {
        let mut store = InMemoryProfileStore::new();
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("service_name".to_string(), "api".to_string())],
            0,
            1,
            4,
            4,
            0,
        );
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("service_name".to_string(), "worker".to_string())],
            0,
            2,
            9,
            9,
            0,
        );
        let state = Arc::new(QuerierState::new(Arc::new(store)));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectHeatmap"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": "{}",
                "start": 0,
                "end": 100,
                "step": 100.0,
                "groupBy": ["service_name"],
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let series = response
            .get("series")
            .and_then(serde_json::Value::as_array)
            .unwrap();
        assert!(series.len() == 2, "{response}");
        assert!(
            series.iter().any(|item| {
                item.pointer("/labels/0/name")
                    .and_then(serde_json::Value::as_str)
                    == Some("service_name")
                    && item
                        .pointer("/labels/0/value")
                        .and_then(serde_json::Value::as_str)
                        == Some("api")
            }),
            "{response}"
        );
        assert!(
            series.iter().any(|item| {
                item.pointer("/labels/0/name")
                    .and_then(serde_json::Value::as_str)
                    == Some("service_name")
                    && item
                        .pointer("/labels/0/value")
                        .and_then(serde_json::Value::as_str)
                        == Some("worker")
            }),
            "{response}"
        );
    }

    #[tokio::test]
    async fn select_heatmap_span_exemplar_returns_span_metadata() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_span_frame(
            "span.path",
            0x2a,
        ))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectHeatmap"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "step": 60.0,
                "groupBy": ["service_name"],
                "queryType": "HEATMAP_QUERY_TYPE_SPAN",
                "exemplarType": "EXEMPLAR_TYPE_SPAN"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let exemplar = response
            .pointer("/series/0/slots/0/exemplars/0")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| panic!("missing heatmap span exemplar: {response}"));

        assert!(exemplar.get("spanId").and_then(serde_json::Value::as_str) == Some("2a"));
        assert!(exemplar.get("timestamp").and_then(json_i64) == Some(10));
        assert!(exemplar.get("value").and_then(json_i64) == Some(7));
    }

    #[tokio::test]
    async fn select_heatmap_individual_exemplar_returns_profile_ids() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_profile_ids())));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectHeatmap"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "step": 60.0,
                "groupBy": ["service_name"],
                "exemplarType": "EXEMPLAR_TYPE_INDIVIDUAL"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let exemplars = response
            .pointer("/series/0/slots/0/exemplars")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| panic!("missing heatmap individual exemplars: {response}"));
        let profile_ids: Vec<_> = exemplars
            .iter()
            .filter_map(|exemplar| {
                exemplar
                    .get("profileId")
                    .and_then(serde_json::Value::as_str)
            })
            .collect();

        assert!(profile_ids.contains(&"profile-a"), "{response}");
        assert!(profile_ids.contains(&"profile-b"), "{response}");
    }

    #[tokio::test]
    async fn select_heatmap_span_query_type_counts_only_span_profiles() {
        let mut store = InMemoryProfileStore::new();
        store.push_sample_with_total_and_span(
            "tenant-a",
            PT,
            vec![("service_name".to_string(), "api".to_string())],
            0,
            1,
            7,
            7,
            10,
            0x2a,
        );
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("service_name".to_string(), "api".to_string())],
            0,
            2,
            11,
            11,
            20,
        );
        let state = Arc::new(QuerierState::new(Arc::new(store)));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectHeatmap"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "step": 100.0,
                "groupBy": ["service_name"],
                "queryType": "HEATMAP_QUERY_TYPE_SPAN"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let count: i64 = response
            .pointer("/series/0/slots/0/counts")
            .and_then(serde_json::Value::as_array)
            .unwrap()
            .iter()
            .filter_map(json_i64)
            .sum();

        assert!(count == 1, "{response}");
    }

    #[tokio::test]
    async fn analyze_query_returns_scope_and_impact_for_matching_series() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_two_profile_types())));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/AnalyzeQuery"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "start": 0,
                "end": 100,
                "query": format!(r#"{PT}{{service_name="api"}}"#),
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(response.get("valid").is_none(), "{response}");
        assert!(
            response
                .pointer("/queryImpact/totalQueriedSeries")
                .and_then(json_i64)
                == Some(1),
            "{response}"
        );
        assert!(
            response
                .pointer("/queryScopes/0/componentType")
                .and_then(serde_json::Value::as_str)
                == Some("Long term storage"),
            "{response}"
        );
        assert!(
            response
                .pointer("/queryScopes/0/seriesCount")
                .and_then(json_i64)
                == Some(1),
            "{response}"
        );
    }

    #[tokio::test]
    async fn analyze_query_counts_only_the_queried_profile_type() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_two_profile_types())));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/AnalyzeQuery"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "start": 0,
                "end": 100,
                "query": format!(r#"{PT}{{service_name="api"}}"#),
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(
            response
                .pointer("/queryImpact/totalQueriedSeries")
                .and_then(json_i64)
                == Some(1),
            "{response}"
        );
    }

    #[test]
    fn render_query_splits_profile_type_and_selector() {
        let (profile_type, selector) = parse_render_query(
            r#"process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="api"}"#,
        )
        .unwrap();

        assert!(profile_type == "process_cpu:cpu:nanoseconds:cpu:nanoseconds");
        assert!(selector == r#"{service_name="api"}"#);
    }

    #[test]
    fn render_query_allows_profile_type_only() {
        let (profile_type, selector) =
            parse_render_query("process_cpu:cpu:nanoseconds:cpu:nanoseconds").unwrap();

        assert!(profile_type == "process_cpu:cpu:nanoseconds:cpu:nanoseconds");
        assert!(selector == "{}");
    }

    #[test]
    fn flamebearer_json_includes_profile_metadata() {
        let response = flamebearer_json(
            crabka_pprof::FlameGraph {
                names: vec!["total".to_string()],
                levels: Vec::new(),
                total: 7,
                max_self: 7,
            },
            PT,
        );

        let metadata = response.get("metadata").unwrap();
        assert!(metadata.get("format").and_then(serde_json::Value::as_str) == Some("single"));
        assert!(metadata.get("spyName").and_then(serde_json::Value::as_str) == Some("process_cpu"));
        assert!(
            metadata
                .get("sampleRate")
                .and_then(serde_json::Value::as_u64)
                == Some(100)
        );
        assert!(metadata.get("units").and_then(serde_json::Value::as_str) == Some("nanoseconds"));
        assert!(
            metadata.get("name").and_then(serde_json::Value::as_str)
                == Some("process_cpu:cpu:nanoseconds:cpu:nanoseconds")
        );
    }

    #[test]
    fn flamegraph_dot_projects_levels_to_graphviz() {
        let dot = flamegraph_dot(&crabka_pprof::FlameGraph {
            names: vec![
                "total".to_string(),
                "main".to_string(),
                "main.work".to_string(),
            ],
            levels: vec![
                crabka_pprof::Level {
                    values: vec![0, 7, 0, 0],
                },
                crabka_pprof::Level {
                    values: vec![0, 7, 0, 1],
                },
                crabka_pprof::Level {
                    values: vec![0, 7, 7, 2],
                },
            ],
            total: 7,
            max_self: 7,
        });

        assert!(dot.starts_with("digraph flamegraph"));
        assert!(dot.contains("main.work"));
        assert!(dot.contains("n0 -> n1"));
        assert!(dot.contains("n1 -> n2"));
    }

    #[test]
    fn limit_zero_means_unlimited() {
        assert!(limit(0) == usize::MAX);
        assert!(limit(2) == 2);
    }

    #[test]
    fn render_time_params_accept_now_offsets() {
        let now_ms = 1_700_000_000_000;

        assert!(parse_render_time_param(None, now_ms, 0).unwrap() == 0);
        assert!(parse_render_time_param(Some("now"), now_ms, 0).unwrap() == now_ms);
        assert!(parse_render_time_param(Some("now-1h"), now_ms, 0).unwrap() == now_ms - 3_600_000);
        assert!(
            parse_render_time_param(Some("now-15m"), now_ms, 0).unwrap() == now_ms - 15 * 60_000
        );
    }

    #[test]
    fn render_time_params_accept_unix_seconds_and_millis() {
        let now_ms = 1_700_000_000_000;

        assert!(parse_render_time_param(Some("123"), now_ms, 0).unwrap() == 123_000);
        assert!(
            parse_render_time_param(Some("1700000000"), now_ms, 0).unwrap() == 1_700_000_000_000
        );
        assert!(
            parse_render_time_param(Some("1700000000000"), now_ms, 0).unwrap() == 1_700_000_000_000
        );
    }

    #[test]
    fn render_time_params_reject_negative_resolved_bounds() {
        let now_ms = 1_000;

        // A `now-<offset>` larger than `now` underflows past the epoch.
        assert!(parse_render_time_param(Some("now-1h"), now_ms, 0).is_err());
        // A literal negative timestamp (seconds or millis heuristic) is rejected.
        assert!(parse_render_time_param(Some("-5"), now_ms, 0).is_err());
        assert!(parse_render_time_param(Some("-1700000000000"), now_ms, 0).is_err());
        // A valid millisecond timestamp at/above the seconds-vs-millis cutoff is
        // left untouched (not mangled by the heuristic) and accepted.
        assert!(
            parse_render_time_param(Some("1700000000000"), now_ms, 0).unwrap() == 1_700_000_000_000
        );
    }

    #[test]
    fn tenant_from_headers_validates_and_defaults() {
        // Absent header -> anonymous.
        let empty = HeaderMap::new();
        assert!(tenant_from_headers(&empty).unwrap() == "anonymous");

        // Valid tenant passes through.
        let mut valid = HeaderMap::new();
        valid.insert("x-scope-orgid", "tenant-a".parse().unwrap());
        assert!(tenant_from_headers(&valid).unwrap() == "tenant-a");

        // Empty header value falls back to anonymous (preserved behaviour).
        let mut blank = HeaderMap::new();
        blank.insert("x-scope-orgid", "".parse().unwrap());
        assert!(tenant_from_headers(&blank).unwrap() == "anonymous");
    }

    #[test]
    fn tenant_from_headers_rejects_path_unsafe_tenant() {
        let mut headers = HeaderMap::new();
        headers.insert("x-scope-orgid", "../escape".parse().unwrap());
        let err = tenant_from_headers(&headers).unwrap_err();

        // Mapped to an invalid-argument-class error with a generic message that
        // does not echo the attacker-supplied id.
        assert!(matches!(err, ProfileError::Plan(_)));
        assert!(connect_error(err).code() == Code::InvalidArgument);
    }

    #[tokio::test]
    async fn invalid_tenant_header_is_rejected_by_connect_handler() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let status = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/ProfileTypes"
            ))
            .header("x-scope-orgid", "bad/tenant")
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status();

        assert!(status.is_client_error(), "{status}");
    }

    #[test]
    fn default_limits_reject_unbounded_explicit_range() {
        let state = QuerierState::new(Arc::new(InMemoryProfileStore::new()));

        // An explicit `start=0, end=i64::MAX` range (NOT the range-omitted health
        // probe) now exceeds the default `max_query_length_secs` cap.
        let err = state
            .validate_query_range("anonymous", 0, i64::MAX)
            .unwrap_err();
        assert!(err.to_string().contains("query length exceeded"), "{err}");

        // A bounded recent window stays well within the 721h default.
        assert!(state.validate_query_range("anonymous", 0, 60_000).is_ok());
    }

    #[tokio::test]
    async fn profile_types_health_probe_ok_under_default_limits() {
        // The range-omitted (`start==0 && end==0`) health probe must still work
        // even though the default cap now rejects explicit unbounded ranges.
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/ProfileTypes"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(
            response
                .get("profileTypes")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|profile_types| !profile_types.is_empty()),
            "{response}"
        );
    }

    #[tokio::test]
    async fn profile_error_response_maps_internal_to_generic_500() {
        // Internal failures become a generic 500 with no leaked detail.
        let response = profile_error_response(ProfileError::Exec(
            "datafusion: secret plan detail".to_string(),
        ));
        assert!(response.status() == StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body == "internal error", "{body}");
        assert!(!body.contains("datafusion"), "{body}");
        assert!(!body.contains("secret"), "{body}");
    }

    #[tokio::test]
    async fn profile_error_response_preserves_client_error_400() {
        // Client-shaped errors (including limit/range violations surfaced as
        // `Plan`) keep their user-facing message at 400.
        let response =
            profile_error_response(ProfileError::Plan("query length exceeded".to_string()));
        assert!(response.status() == StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("query length exceeded"), "{body}");
    }

    #[test]
    fn parse_span_selectors_accepts_decimal_and_hex() {
        let spans =
            parse_span_selectors(&["42".to_string(), "9a517183f26a089d".to_string()]).unwrap();

        assert!(spans == vec![42, 0x9a51_7183_f26a_089d]);
    }

    #[test]
    fn parse_span_selectors_rejects_bad_span() {
        assert!(parse_span_selectors(&["not-a-span".to_string()]).is_err());
    }

    #[test]
    fn heatmap_time_buckets_ceil_from_step_seconds() {
        assert!(heatmap_time_buckets(0, 21_000, 10.0).unwrap() == 3);
        assert!(heatmap_time_buckets(0, 1, 0.0).is_err());
        assert!(heatmap_time_buckets(1, 1, 1.0).is_err());
    }

    #[test]
    fn heatmap_time_buckets_rejects_sub_millisecond_steps() {
        assert!(heatmap_time_buckets(0, 1, 0.0001).is_err());
        assert!(heatmap_time_buckets(0, 1, 0.0005).is_err());
        assert!(heatmap_time_buckets(0, 1, 0.0009999).is_err());
        assert!(heatmap_time_buckets(0, 1, 0.001).unwrap() == 1);
    }

    #[test]
    fn heatmap_time_buckets_caps_large_ranges() {
        assert!(heatmap_time_buckets(0, i64::MAX, 10.0).unwrap() == MAX_HEATMAP_TIME_BUCKETS);
    }

    #[test]
    fn heatmap_series_projects_slots() {
        let series = pb::querier::v1::HeatmapSeries::from(crabka_pprof::Heatmap {
            start_ms: 0,
            end_ms: 20,
            time_buckets: 2,
            value_buckets: 2,
            min_value: 10,
            max_value: 30,
            counts: vec![vec![1, 0], vec![0, 2]],
        });

        assert!(series.labels.is_empty());
        assert!(series.slots[0].timestamp == 10);
        assert!(series.slots[1].timestamp == 20);
        assert!(series.slots[0].y_min == vec![10.0, 20.0]);
        assert!(series.slots[1].counts == vec![0, 2]);
    }
}
