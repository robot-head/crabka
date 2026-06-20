//! Public `TraceQL` engine.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::arrow::array::AsArray;

use crate::ast::{Aggregate, Field, Intrinsic, Pipeline, Query, Scope};
use crate::error::{Result, TraceqlError};
use crate::parser::parse;
use crate::planner::{PlannerContext, plan_query};
use crate::result::{
    AttrValue, ScopedTag, SearchResponse, SpanRef, SpanSet, TagScope, TraceMetricExemplar,
    TraceMetricSeries, TraceMetricsResponse, TraceResult, TraceSpans, TypedValue,
};
use crate::span_columns::{
    ATTR_PREFIX, COL_DURATION, COL_KIND, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID,
    COL_PARENT_SPAN_ID, COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_START,
    COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TRACE_DURATION, COL_TRACE_ID, COL_TRACE_START,
};
use crate::store::SpanStore;

const DEFAULT_HISTOGRAM_BUCKETS_NS: &[f64] = &[
    2_000_000.0,
    4_000_000.0,
    8_000_000.0,
    16_000_000.0,
    32_000_000.0,
    64_000_000.0,
    128_000_000.0,
    256_000_000.0,
    512_000_000.0,
    1_024_000_000.0,
    2_048_000_000.0,
    4_096_000_000.0,
    8_192_000_000.0,
    16_384_000_000.0,
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineOpts {
    pub default_limit: usize,
    pub default_spss: usize,
    pub max_traces: usize,
    pub max_exemplars: usize,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            default_limit: 20,
            default_spss: 3,
            max_traces: 1000,
            max_exemplars: 1,
        }
    }
}

pub struct TraceqlEngine<S: SpanStore> {
    store: Arc<S>,
    opts: EngineOpts,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SearchOptions {
    pub limit: usize,
    pub spss: usize,
    pub search_limit: Option<usize>,
}

impl<S: SpanStore> TraceqlEngine<S> {
    #[must_use]
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self {
        Self { store, opts }
    }

    #[must_use]
    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    #[must_use]
    pub fn effective_search_limit(&self, limit: usize) -> usize {
        if limit == 0 {
            self.opts.default_limit
        } else {
            limit
        }
        .min(self.opts.max_traces)
    }

    pub async fn search(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
    ) -> Result<SearchResponse> {
        self.search_with_options(
            tenant,
            query,
            start_ns,
            end_ns,
            SearchOptions {
                limit,
                ..SearchOptions::default()
            },
        )
        .await
    }

    pub async fn search_with_spss(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        spss: usize,
    ) -> Result<SearchResponse> {
        self.search_with_options(
            tenant,
            query,
            start_ns,
            end_ns,
            SearchOptions {
                limit,
                spss,
                search_limit: None,
            },
        )
        .await
    }

    pub async fn search_with_options(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        options: SearchOptions,
    ) -> Result<SearchResponse> {
        let q = parse(query)?;
        let planned = plan_query(
            self.store.as_ref(),
            &PlannerContext {
                tenant: tenant.to_string(),
                start_ns,
                end_ns,
            },
            &q,
        )
        .await?;
        let batches = planned
            .ctx
            .execute_logical_plan(planned.plan)
            .await?
            .collect()
            .await?;
        let effective_limit = self.effective_search_limit(options.limit);
        let search_limit = options
            .search_limit
            .unwrap_or(effective_limit)
            .min(self.opts.max_traces);
        let effective_spss = if options.spss == 0 {
            self.opts.default_spss
        } else {
            options.spss
        };
        assemble_search_response(&batches, search_limit, effective_spss)
    }

    pub async fn query_range(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
    ) -> Result<TraceMetricsResponse> {
        let q = parse(query)?;
        let metric = metric_plan(&q)?;
        if metric.compare {
            return self
                .query_range_compare(tenant, q, start_ns, end_ns, step_ns, metric)
                .await;
        }

        let planned = plan_query(
            self.store.as_ref(),
            &PlannerContext {
                tenant: tenant.to_string(),
                start_ns,
                end_ns,
            },
            &Query {
                root: q.root,
                pipeline: Vec::new(),
            },
        )
        .await?;
        let batches = collect_planned_batches(planned).await?;
        assemble_metrics_response(
            &batches,
            start_ns,
            end_ns,
            step_ns,
            &metric,
            self.opts.max_exemplars,
            start_ns,
        )
    }

    async fn query_range_compare(
        &self,
        tenant: &str,
        q: Query,
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
        metric: MetricPlan,
    ) -> Result<TraceMetricsResponse> {
        let width_ns = end_ns
            .checked_sub(start_ns)
            .ok_or_else(|| TraceqlError::Plan("metrics end must be >= start".into()))?;
        let previous_start_ns = start_ns
            .checked_sub(width_ns)
            .and_then(|v| v.checked_sub(step_ns))
            .ok_or_else(|| TraceqlError::Plan("compare range underflow".into()))?;
        let previous_end_ns = start_ns
            .checked_sub(step_ns)
            .ok_or_else(|| TraceqlError::Plan("compare range underflow".into()))?;
        let root = q.root;
        let current = self
            .metrics_for_range(
                tenant,
                root.clone(),
                MetricsRange {
                    scan_start: start_ns,
                    scan_end: end_ns,
                    output_start: start_ns,
                    step: step_ns,
                },
                &metric,
            )
            .await?;
        let previous = self
            .metrics_for_range(
                tenant,
                root,
                MetricsRange {
                    scan_start: previous_start_ns,
                    scan_end: previous_end_ns,
                    output_start: start_ns,
                    step: step_ns,
                },
                &metric,
            )
            .await?;

        Ok(TraceMetricsResponse {
            series: label_compared_series(current.series, previous.series),
        })
    }

    async fn metrics_for_range(
        &self,
        tenant: &str,
        root: crate::ast::SpansetExpr,
        range: MetricsRange,
        metric: &MetricPlan,
    ) -> Result<TraceMetricsResponse> {
        let planned = plan_query(
            self.store.as_ref(),
            &PlannerContext {
                tenant: tenant.to_string(),
                start_ns: range.scan_start,
                end_ns: range.scan_end,
            },
            &Query {
                root,
                pipeline: Vec::new(),
            },
        )
        .await?;
        let batches = collect_planned_batches(planned).await?;
        assemble_metrics_response(
            &batches,
            range.scan_start,
            range.scan_end,
            range.step,
            metric,
            self.opts.max_exemplars,
            range.output_start,
        )
    }

    pub async fn trace_by_id(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceSpans>> {
        self.store.trace_by_id(tenant, trace_id).await
    }

    pub async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>> {
        self.store.tag_names(tenant, scope, start_ns, end_ns).await
    }

    pub async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>> {
        self.store.tag_values(tenant, tag, start_ns, end_ns).await
    }
}

#[derive(Clone, Copy)]
enum MetricFunction {
    Rate,
    CountOverTime,
    SumOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    HistogramOverTime,
    QuantileOverTime,
}

struct MetricPlan {
    function: MetricFunction,
    value: Option<Field>,
    quantiles: Vec<f64>,
    by: Vec<Field>,
    rank: Option<RankLimit>,
    compare: bool,
}

#[derive(Clone, Copy)]
struct RankLimit {
    direction: RankDirection,
    k: usize,
}

#[derive(Clone, Copy)]
enum RankDirection {
    Top,
    Bottom,
}

#[derive(Clone, Copy)]
struct MetricsRange {
    scan_start: i64,
    scan_end: i64,
    output_start: i64,
    step: i64,
}

fn metric_plan(q: &Query) -> Result<MetricPlan> {
    match q.pipeline.as_slice() {
        [Pipeline::Aggregate(aggregate)] => metric_plan_for(aggregate, Vec::new(), None),
        [Pipeline::Aggregate(aggregate), Pipeline::By(by)] => {
            metric_plan_for(aggregate, by.clone(), None)
        }
        [
            Pipeline::Aggregate(aggregate),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
        ] => metric_plan_for(aggregate, Vec::new(), Some(rank_limit(rank)?)),
        [
            Pipeline::Aggregate(aggregate),
            Pipeline::By(by),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
        ] => metric_plan_for(aggregate, by.clone(), Some(rank_limit(rank)?)),
        [Pipeline::Aggregate(aggregate), Pipeline::Compare] => {
            metric_plan_with_compare(aggregate, Vec::new(), None)
        }
        [
            Pipeline::Aggregate(aggregate),
            Pipeline::By(by),
            Pipeline::Compare,
        ] => metric_plan_with_compare(aggregate, by.clone(), None),
        [
            Pipeline::Aggregate(aggregate),
            Pipeline::By(by),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::Compare,
        ] => metric_plan_with_compare(aggregate, by.clone(), Some(rank_limit(rank)?)),
        _ => Err(TraceqlError::Unsupported(
            "traceql metrics: expected supported *_over_time() metric".into(),
        )),
    }
}

fn rank_limit(pipeline: &Pipeline) -> Result<RankLimit> {
    match pipeline {
        Pipeline::TopK(k) => Ok(RankLimit {
            direction: RankDirection::Top,
            k: *k,
        }),
        Pipeline::BottomK(k) => Ok(RankLimit {
            direction: RankDirection::Bottom,
            k: *k,
        }),
        other => Err(TraceqlError::Unsupported(format!(
            "traceql metrics: expected topk/bottomk, got {other:?}"
        ))),
    }
}

fn metric_plan_for(
    aggregate: &Aggregate,
    by: Vec<Field>,
    rank: Option<RankLimit>,
) -> Result<MetricPlan> {
    let (function, value, quantiles) = match aggregate {
        Aggregate::Rate => (MetricFunction::Rate, None, Vec::new()),
        Aggregate::CountOverTime => (MetricFunction::CountOverTime, None, Vec::new()),
        Aggregate::SumOverTime(field) => {
            (MetricFunction::SumOverTime, Some(field.clone()), Vec::new())
        }
        Aggregate::AvgOverTime(field) => {
            (MetricFunction::AvgOverTime, Some(field.clone()), Vec::new())
        }
        Aggregate::MinOverTime(field) => {
            (MetricFunction::MinOverTime, Some(field.clone()), Vec::new())
        }
        Aggregate::MaxOverTime(field) => {
            (MetricFunction::MaxOverTime, Some(field.clone()), Vec::new())
        }
        Aggregate::HistogramOverTime(field) => (
            MetricFunction::HistogramOverTime,
            Some(field.clone()),
            Vec::new(),
        ),
        Aggregate::QuantileOverTime { field, quantiles } => (
            MetricFunction::QuantileOverTime,
            Some(field.clone()),
            quantiles.clone(),
        ),
        Aggregate::Count
        | Aggregate::Avg(_)
        | Aggregate::Sum(_)
        | Aggregate::Min(_)
        | Aggregate::Max(_) => {
            return Err(TraceqlError::Unsupported(
                "traceql metrics: expected supported *_over_time() metric".into(),
            ));
        }
    };
    Ok(MetricPlan {
        function,
        value,
        quantiles,
        by,
        rank,
        compare: false,
    })
}

fn metric_plan_with_compare(
    aggregate: &Aggregate,
    by: Vec<Field>,
    rank: Option<RankLimit>,
) -> Result<MetricPlan> {
    let mut plan = metric_plan_for(aggregate, by, rank)?;
    plan.compare = true;
    Ok(plan)
}

async fn collect_planned_batches(
    planned: crate::planner::PlannedSpanset,
) -> Result<Vec<RecordBatch>> {
    Ok(planned
        .ctx
        .execute_logical_plan(planned.plan)
        .await?
        .collect()
        .await?)
}

fn label_compared_series(
    current: Vec<TraceMetricSeries>,
    previous: Vec<TraceMetricSeries>,
) -> Vec<TraceMetricSeries> {
    current
        .into_iter()
        .map(|series| label_comparison(series, "current"))
        .chain(
            previous
                .into_iter()
                .map(|series| label_comparison(series, "previous")),
        )
        .collect()
}

fn label_comparison(mut series: TraceMetricSeries, value: &str) -> TraceMetricSeries {
    series
        .labels
        .insert(0, ("comparison".into(), value.to_string()));
    series
}

fn assemble_metrics_response(
    batches: &[RecordBatch],
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    metric: &MetricPlan,
    max_exemplars: usize,
    output_start_ns: i64,
) -> Result<TraceMetricsResponse> {
    if step_ns <= 0 {
        return Err(TraceqlError::Plan("metrics step must be positive".into()));
    }
    if end_ns < start_ns {
        return Err(TraceqlError::Plan("metrics end must be >= start".into()));
    }

    let bucket_count = usize::try_from((end_ns - start_ns) / step_ns + 1)
        .map_err(|e| TraceqlError::Plan(e.to_string()))?;
    let mut buckets: BTreeMap<Vec<(String, String)>, Vec<MetricBucket>> = BTreeMap::new();
    for batch in batches {
        let starts = batch
            .column_by_name(COL_START)
            .ok_or_else(|| TraceqlError::Exec(format!("missing column {COL_START}")))?
            .as_primitive::<arrow::datatypes::Int64Type>();
        for row in 0..batch.num_rows() {
            let ts = starts.value(row);
            if ts < start_ns || ts > end_ns {
                continue;
            }
            let idx = usize::try_from((ts - start_ns) / step_ns)
                .map_err(|e| TraceqlError::Exec(e.to_string()))?;
            let labels = metric_labels(batch, row, &metric.by)?;
            let value = metric
                .value
                .as_ref()
                .map(|field| metric_numeric_value(batch, row, field))
                .transpose()?;
            let exemplar = metric_exemplar(batch, row, ts, value.unwrap_or(1.0))?;
            let series_buckets = buckets
                .entry(labels)
                .or_insert_with(|| vec![MetricBucket::default(); bucket_count]);
            if let Some(bucket) = series_buckets.get_mut(idx) {
                bucket.record(value, Some(exemplar));
            }
        }
    }
    if buckets.is_empty() {
        buckets.insert(Vec::new(), vec![MetricBucket::default(); bucket_count]);
    }

    let step_seconds = f64_from_i64(step_ns)? / 1_000_000_000.0;
    let series = buckets
        .into_iter()
        .map(|(labels, buckets)| {
            metric_series_for_group(
                labels,
                buckets,
                metric,
                output_start_ns,
                step_ns,
                step_seconds,
                max_exemplars,
            )
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(TraceMetricsResponse {
        series: apply_rank(series, metric.rank),
    })
}

fn apply_rank(
    mut series: Vec<TraceMetricSeries>,
    rank: Option<RankLimit>,
) -> Vec<TraceMetricSeries> {
    let Some(rank) = rank else {
        return series;
    };
    series.sort_by(|a, b| {
        let a_score = series_rank_score(a);
        let b_score = series_rank_score(b);
        match rank.direction {
            RankDirection::Top => b_score
                .total_cmp(&a_score)
                .then_with(|| a.labels.cmp(&b.labels)),
            RankDirection::Bottom => a_score
                .total_cmp(&b_score)
                .then_with(|| a.labels.cmp(&b.labels)),
        }
    });
    series.truncate(rank.k);
    series
}

fn series_rank_score(series: &TraceMetricSeries) -> f64 {
    series.points.iter().map(|(_, value)| *value).sum()
}

fn metric_series_for_group(
    labels: Vec<(String, String)>,
    buckets: Vec<MetricBucket>,
    metric: &MetricPlan,
    start_ns: i64,
    step_ns: i64,
    step_seconds: f64,
    max_exemplars: usize,
) -> Result<Vec<TraceMetricSeries>> {
    let exemplars = metric_exemplars(&buckets, max_exemplars);
    if matches!(metric.function, MetricFunction::QuantileOverTime) {
        return metric
            .quantiles
            .iter()
            .map(|quantile| {
                let mut labels = labels.clone();
                labels.insert(0, ("p".into(), quantile_label(*quantile)));
                let points = buckets
                    .iter()
                    .enumerate()
                    .map(|(idx, bucket)| {
                        let ts = start_ns + i64::try_from(idx).unwrap_or(i64::MAX) * step_ns;
                        Ok((ts, bucket.quantile(*quantile)?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(TraceMetricSeries {
                    labels,
                    points,
                    exemplars: exemplars.clone(),
                })
            })
            .collect();
    }
    if matches!(metric.function, MetricFunction::HistogramOverTime) {
        return histogram_series_for_group(labels, &buckets, start_ns, step_ns, &exemplars);
    }

    let points = buckets
        .into_iter()
        .enumerate()
        .map(|(idx, bucket)| {
            let ts = start_ns + i64::try_from(idx).unwrap_or(i64::MAX) * step_ns;
            let value = match metric.function {
                MetricFunction::Rate => f64_from_u64(bucket.count)? / step_seconds,
                MetricFunction::CountOverTime => f64_from_u64(bucket.count)?,
                MetricFunction::SumOverTime => bucket.sum,
                MetricFunction::AvgOverTime => bucket.average()?,
                MetricFunction::MinOverTime => bucket.min.unwrap_or(0.0),
                MetricFunction::MaxOverTime => bucket.max.unwrap_or(0.0),
                MetricFunction::HistogramOverTime | MetricFunction::QuantileOverTime => {
                    unreachable!("handled above")
                }
            };
            Ok((ts, value))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(vec![TraceMetricSeries {
        labels,
        points,
        exemplars,
    }])
}

fn histogram_series_for_group(
    labels: Vec<(String, String)>,
    buckets: &[MetricBucket],
    start_ns: i64,
    step_ns: i64,
    exemplars: &[TraceMetricExemplar],
) -> Result<Vec<TraceMetricSeries>> {
    let mut out = Vec::with_capacity(DEFAULT_HISTOGRAM_BUCKETS_NS.len() + 3);
    for le in DEFAULT_HISTOGRAM_BUCKETS_NS {
        let mut labels = labels.clone();
        labels.insert(0, ("le".into(), quantile_label(*le)));
        out.push(TraceMetricSeries {
            labels,
            points: histogram_points(buckets, start_ns, step_ns, |bucket| {
                f64_from_usize(bucket.values.iter().filter(|value| **value <= *le).count())
            })?,
            exemplars: exemplars.to_owned(),
        });
    }

    let mut inf_labels = labels.clone();
    inf_labels.insert(0, ("le".into(), "+Inf".into()));
    out.push(TraceMetricSeries {
        labels: inf_labels,
        points: histogram_points(buckets, start_ns, step_ns, |bucket| {
            f64_from_u64(bucket.count)
        })?,
        exemplars: exemplars.to_owned(),
    });

    let mut sum_labels = labels.clone();
    sum_labels.insert(0, ("__metric__".into(), "sum".into()));
    out.push(TraceMetricSeries {
        labels: sum_labels,
        points: histogram_points(buckets, start_ns, step_ns, |bucket| Ok(bucket.sum))?,
        exemplars: Vec::new(),
    });

    let mut count_labels = labels;
    count_labels.insert(0, ("__metric__".into(), "count".into()));
    out.push(TraceMetricSeries {
        labels: count_labels,
        points: histogram_points(buckets, start_ns, step_ns, |bucket| {
            f64_from_u64(bucket.count)
        })?,
        exemplars: Vec::new(),
    });

    Ok(out)
}

fn histogram_points(
    buckets: &[MetricBucket],
    start_ns: i64,
    step_ns: i64,
    mut value: impl FnMut(&MetricBucket) -> Result<f64>,
) -> Result<Vec<(i64, f64)>> {
    buckets
        .iter()
        .enumerate()
        .map(|(idx, bucket)| {
            let ts = start_ns + i64::try_from(idx).unwrap_or(i64::MAX) * step_ns;
            Ok((ts, value(bucket)?))
        })
        .collect()
}

#[derive(Clone, Default)]
struct MetricBucket {
    count: u64,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
    values: Vec<f64>,
    exemplars: Vec<TraceMetricExemplar>,
}

impl MetricBucket {
    fn record(&mut self, value: Option<f64>, exemplar: Option<TraceMetricExemplar>) {
        self.count += 1;
        if let Some(exemplar) = exemplar
            && self.exemplars.is_empty()
        {
            self.exemplars.push(exemplar);
        }
        let Some(value) = value else {
            return;
        };
        self.sum += value;
        self.min = Some(self.min.map_or(value, |min| min.min(value)));
        self.max = Some(self.max.map_or(value, |max| max.max(value)));
        self.values.push(value);
    }

    fn average(&self) -> Result<f64> {
        if self.count == 0 {
            Ok(0.0)
        } else {
            Ok(self.sum / f64_from_u64(self.count)?)
        }
    }

    fn quantile(&self, quantile: f64) -> Result<f64> {
        if self.values.is_empty() {
            return Ok(0.0);
        }
        let mut values = self.values.clone();
        values.sort_by(f64::total_cmp);
        if values.len() == 1 {
            return Ok(values[0]);
        }
        let rank = quantile * f64_from_usize(values.len() - 1)?;
        let lower = usize_from_integer_f64(rank.floor())?;
        let upper = usize_from_integer_f64(rank.ceil())?;
        if lower == upper {
            Ok(values[lower])
        } else {
            Ok(values[lower] + (values[upper] - values[lower]) * (rank - f64_from_usize(lower)?))
        }
    }
}

fn metric_exemplar(
    batch: &RecordBatch,
    row: usize,
    timestamp_ns: i64,
    value: f64,
) -> Result<TraceMetricExemplar> {
    Ok(TraceMetricExemplar {
        labels: vec![
            (
                "trace_id".into(),
                bytes_to_hex(&fixed_16(batch, COL_TRACE_ID, row)?),
            ),
            (
                "span_id".into(),
                bytes_to_hex(&fixed_8(batch, COL_SPAN_ID, row)?),
            ),
        ],
        value,
        timestamp_ns,
    })
}

fn metric_exemplars(buckets: &[MetricBucket], max_exemplars: usize) -> Vec<TraceMetricExemplar> {
    buckets
        .iter()
        .flat_map(|bucket| bucket.exemplars.iter().cloned())
        .take(max_exemplars)
        .collect()
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn quantile_label(quantile: f64) -> String {
    let mut label = quantile.to_string();
    if label.contains('.') {
        while label.ends_with('0') {
            label.pop();
        }
        if label.ends_with('.') {
            label.push('0');
        }
    }
    label
}

fn metric_labels(
    batch: &RecordBatch,
    row: usize,
    fields: &[Field],
) -> Result<Vec<(String, String)>> {
    fields
        .iter()
        .map(|field| {
            let column = metric_field_column(field)?;
            let value = metric_label_value(batch, &column, row)?;
            Ok((field.key.clone(), value))
        })
        .collect()
}

fn metric_field_column(field: &Field) -> Result<String> {
    match field.scope {
        Scope::Both | Scope::Span | Scope::Resource => Ok(format!("{ATTR_PREFIX}{}", field.key)),
        Scope::Intrinsic(Intrinsic::Name) => Ok(COL_NAME.to_string()),
        Scope::Intrinsic(Intrinsic::Duration) => Ok(COL_DURATION.to_string()),
        Scope::Intrinsic(Intrinsic::TraceRootService) => Ok(COL_ROOT_SERVICE_NAME.to_string()),
        Scope::Intrinsic(Intrinsic::TraceRootName) => Ok(COL_ROOT_SPAN_NAME.to_string()),
        _ => Err(TraceqlError::Unsupported(format!(
            "metrics by() field {field:?} is not supported yet"
        ))),
    }
}

fn metric_label_value(batch: &RecordBatch, column: &str, row: usize) -> Result<String> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {column}")))?;
    if array.is_null(row) {
        return Ok(String::new());
    }
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_string::<i32>().value(row).to_string()),
        DataType::Int64 => Ok(array
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(row)
            .to_string()),
        DataType::Float64 => Ok(array
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(row)
            .to_string()),
        DataType::Boolean => Ok(array.as_boolean().value(row).to_string()),
        DataType::Int32 => Ok(array
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(row)
            .to_string()),
        other => Err(TraceqlError::Exec(format!(
            "unsupported metrics label column type {other:?}"
        ))),
    }
}

fn metric_numeric_value(batch: &RecordBatch, row: usize, field: &Field) -> Result<f64> {
    let column = metric_field_column(field)?;
    let array = batch
        .column_by_name(&column)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {column}")))?;
    if array.is_null(row) {
        return Ok(0.0);
    }
    match array.data_type() {
        DataType::Int64 => f64_from_i64(
            array
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row),
        ),
        DataType::Int32 => f64_from_i64(i64::from(
            array
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(row),
        )),
        DataType::Float64 => Ok(array
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(row)),
        other => Err(TraceqlError::Unsupported(format!(
            "metrics fold field {field:?} has non-numeric type {other:?}"
        ))),
    }
}

fn f64_from_i64(value: i64) -> Result<f64> {
    value
        .to_string()
        .parse()
        .map_err(|e: std::num::ParseFloatError| TraceqlError::Exec(e.to_string()))
}

fn f64_from_u64(value: u64) -> Result<f64> {
    value
        .to_string()
        .parse()
        .map_err(|e: std::num::ParseFloatError| TraceqlError::Exec(e.to_string()))
}

fn f64_from_usize(value: usize) -> Result<f64> {
    value
        .to_string()
        .parse()
        .map_err(|e: std::num::ParseFloatError| TraceqlError::Exec(e.to_string()))
}

fn usize_from_integer_f64(value: f64) -> Result<usize> {
    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 {
        return Err(TraceqlError::Exec(format!(
            "expected non-negative integer float, got {value}"
        )));
    }
    value
        .to_string()
        .parse()
        .map_err(|e: std::num::ParseIntError| TraceqlError::Exec(e.to_string()))
}

struct TraceAcc {
    root_service_name: String,
    root_trace_name: String,
    start_time_unix_nano: u64,
    duration_ms: u64,
    spans: Vec<SpanRef>,
}

pub(crate) fn assemble_search_response(
    batches: &[RecordBatch],
    limit: usize,
    spss: usize,
) -> Result<SearchResponse> {
    let mut traces: BTreeMap<[u8; 16], TraceAcc> = BTreeMap::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let trace_id = fixed_16(batch, COL_TRACE_ID, row)?;
            let span = SpanRef {
                span_id: fixed_8(batch, COL_SPAN_ID, row)?,
                parent_span_id: optional_fixed_8(batch, COL_PARENT_SPAN_ID, row)?,
                name: string_value(batch, COL_NAME, row).unwrap_or_default(),
                kind: i32_value(batch, COL_KIND, row)?,
                nested_set_left: i32_value(batch, COL_NS_LEFT, row)?,
                nested_set_right: i32_value(batch, COL_NS_RIGHT, row)?,
                nested_set_parent: i32_value(batch, COL_PARENT_ID, row)?,
                start_time_unix_nano: u64_from_i64(i64_value(batch, COL_START, row)?)?,
                duration_nanos: u64_from_i64(i64_value(batch, COL_DURATION, row)?)?,
                status_code: i32_value(batch, COL_STATUS_CODE, row)?,
                status_message: string_value(batch, COL_STATUS_MESSAGE, row).unwrap_or_default(),
                attributes: row_attrs(batch, row)?,
            };
            traces
                .entry(trace_id)
                .or_insert_with(|| TraceAcc {
                    root_service_name: string_value(batch, COL_ROOT_SERVICE_NAME, row)
                        .unwrap_or_default(),
                    root_trace_name: string_value(batch, COL_ROOT_SPAN_NAME, row)
                        .unwrap_or_default(),
                    start_time_unix_nano: u64_from_i64(
                        i64_value(batch, COL_TRACE_START, row).unwrap_or_default(),
                    )
                    .unwrap_or_default(),
                    duration_ms: u64_from_i64(
                        i64_value(batch, COL_TRACE_DURATION, row).unwrap_or_default(),
                    )
                    .unwrap_or_default()
                        / 1_000_000,
                    spans: Vec::new(),
                })
                .spans
                .push(span);
        }
    }

    let mut out: Vec<TraceResult> = traces
        .into_iter()
        .map(|(trace_id, mut acc)| {
            acc.spans
                .sort_by_key(|s| (s.start_time_unix_nano, s.span_id));
            let matched = u32::try_from(acc.spans.len()).unwrap_or(u32::MAX);
            let spans = acc.spans.into_iter().take(spss).collect();
            TraceResult {
                trace_id,
                root_service_name: acc.root_service_name,
                root_trace_name: acc.root_trace_name,
                start_time_unix_nano: acc.start_time_unix_nano,
                duration_ms: acc.duration_ms,
                span_sets: vec![SpanSet { spans, matched }],
            }
        })
        .collect();
    out.sort_by_key(|t| (t.start_time_unix_nano, t.trace_id));
    out.truncate(limit);
    Ok(SearchResponse { traces: out })
}

fn fixed_16(batch: &RecordBatch, col: &str, row: usize) -> Result<[u8; 16]> {
    batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_fixed_size_binary()
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 16 bytes")))
}

fn fixed_8(batch: &RecordBatch, col: &str, row: usize) -> Result<[u8; 8]> {
    batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_fixed_size_binary()
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 8 bytes")))
}

fn optional_fixed_8(batch: &RecordBatch, col: &str, row: usize) -> Result<Option<[u8; 8]>> {
    let arr = batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?;
    if arr.is_null(row) {
        return Ok(None);
    }
    arr.as_fixed_size_binary()
        .value(row)
        .try_into()
        .map(Some)
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 8 bytes")))
}

fn i64_value(batch: &RecordBatch, col: &str, row: usize) -> Result<i64> {
    Ok(batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_primitive::<arrow::datatypes::Int64Type>()
        .value(row))
}

fn i32_value(batch: &RecordBatch, col: &str, row: usize) -> Result<i32> {
    Ok(batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_primitive::<arrow::datatypes::Int32Type>()
        .value(row))
}

fn string_value(batch: &RecordBatch, col: &str, row: usize) -> Option<String> {
    let arr = batch.column_by_name(col)?.as_string::<i32>();
    (!arr.is_null(row)).then(|| arr.value(row).to_string())
}

fn row_attrs(batch: &RecordBatch, row: usize) -> Result<Vec<(String, AttrValue)>> {
    let schema = batch.schema();
    let mut attrs = Vec::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        let Some(name) = field.name().strip_prefix(ATTR_PREFIX) else {
            continue;
        };
        let array = batch.column(idx);
        if array.is_null(row) {
            continue;
        }
        let value = match field.data_type() {
            DataType::Utf8 => AttrValue::Str(array.as_string::<i32>().value(row).to_string()),
            DataType::Int64 => AttrValue::Int(
                array
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .value(row),
            ),
            DataType::Float64 => AttrValue::Float(
                array
                    .as_primitive::<arrow::datatypes::Float64Type>()
                    .value(row),
            ),
            DataType::Boolean => AttrValue::Bool(array.as_boolean().value(row)),
            other => {
                return Err(TraceqlError::Exec(format!(
                    "unsupported attribute column type {other:?}"
                )));
            }
        };
        attrs.push((name.to_string(), value));
    }
    attrs.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(attrs)
}

fn u64_from_i64(v: i64) -> Result<u64> {
    u64::try_from(v).map_err(|e| TraceqlError::Exec(e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use super::*;
    use crate::in_memory::InMemorySpanStore;
    use crate::result::AttrValue;
    use crate::span_columns::InputSpan;

    fn sp(tid: u8, id: u8, parent: Option<u8>, svc: &str) -> InputSpan {
        sp_at(tid, id, parent, svc, 1000 + i64::from(id))
    }

    fn sp_at(tid: u8, id: u8, parent: Option<u8>, svc: &str, start_unix_nano: i64) -> InputSpan {
        InputSpan {
            trace_id: [tid; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: format!("op-{id}"),
            kind: 0,
            start_unix_nano,
            duration_nanos: 200,
            status_code: 0,
            status_message: String::new(),
            attrs: vec![("svc".into(), AttrValue::Str(svc.into()))],
        }
    }

    fn engine() -> TraceqlEngine<InMemorySpanStore> {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![sp(9, 1, None, "a"), sp(9, 2, Some(1), "b")],
        );
        s.push_trace("t", "x", "root", vec![sp(8, 1, None, "x")]);
        TraceqlEngine::new(Arc::new(s), EngineOpts::default())
    }

    #[tokio::test]
    async fn search_selector_returns_matching_trace() {
        let e = engine();
        let r = e
            .search("t", "{ .svc = \"b\" }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].root_service_name == "a");
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_inter_brace_and_matches_different_spans() {
        let e = engine();
        let r = e
            .search("t", "{ .svc = \"a\" } && { .svc = \"b\" }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 2);
    }

    #[tokio::test]
    async fn search_descendant_structural() {
        let e = engine();
        let r = e
            .search("t", "{ .svc = \"b\" } >> { .svc = \"a\" }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_limit_uses_default_for_zero_and_caps_result_count() {
        let e = engine();
        let r = e
            .search("t", "{ .svc != nil }", 0, 100_000, 1)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
    }

    #[tokio::test]
    async fn trace_by_id_path() {
        let e = engine();
        let got = e.trace_by_id("t", &[9; 16]).await.unwrap().unwrap();
        assert!(got.spans.len() == 2);
        assert!(e.trace_by_id("t", &[1; 16]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn count_over_time_counts_matched_spans_per_bucket() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "a", 0),
                sp_at(1, 2, None, "a", 10_000),
                sp_at(1, 3, None, "a", 60_000),
                sp_at(1, 4, None, "b", 70_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let got = e
            .query_range(
                "t",
                "{ .svc = \"a\" } | count_over_time()",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(got.series.len() == 1);
        assert!(got.series[0].points == vec![(0, 2.0), (60_000, 1.0), (120_000, 0.0)]);
    }

    #[tokio::test]
    async fn rate_divides_bucket_count_by_step_seconds() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "a", 0),
                sp_at(1, 2, None, "a", 10_000),
                sp_at(1, 3, None, "a", 20_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let got = e
            .query_range(
                "t",
                "{ .svc = \"a\" } | rate()",
                0,
                10_000_000_000,
                10_000_000_000,
            )
            .await
            .unwrap();
        assert!(got.series.len() == 1);
        assert!(got.series[0].points == vec![(0, 0.3), (10_000_000_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_attribute_emits_one_series_per_group() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "api", 0),
                sp_at(1, 2, None, "api", 10_000),
                sp_at(1, 3, None, "db", 20_000),
                sp_at(1, 4, None, "db", 70_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut got = e
            .query_range(
                "t",
                "{ .svc != nil } | count_over_time() | by(span.svc)",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap()
            .series;
        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(got.len() == 2);
        assert!(got[0].labels == vec![("svc".into(), "api".into())]);
        assert!(got[0].points == vec![(0, 2.0), (60_000, 0.0), (120_000, 0.0)]);
        assert!(got[1].labels == vec![("svc".into(), "db".into())]);
        assert!(got[1].points == vec![(0, 1.0), (60_000, 1.0), (120_000, 0.0)]);
    }

    #[tokio::test]
    async fn avg_and_sum_over_time_fold_duration_per_bucket() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                InputSpan {
                    duration_nanos: 100,
                    ..sp_at(1, 1, None, "api", 0)
                },
                InputSpan {
                    duration_nanos: 300,
                    ..sp_at(1, 2, None, "api", 10_000)
                },
                InputSpan {
                    duration_nanos: 50,
                    ..sp_at(1, 3, None, "api", 70_000)
                },
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let avg = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | avg_over_time(span:duration)",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(avg.series[0].points == vec![(0, 200.0), (60_000, 50.0), (120_000, 0.0)]);

        let sum = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | sum_over_time(span:duration)",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(sum.series[0].points == vec![(0, 400.0), (60_000, 50.0), (120_000, 0.0)]);

        let min = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | min_over_time(span:duration)",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(min.series[0].points == vec![(0, 100.0), (60_000, 50.0), (120_000, 0.0)]);

        let max = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | max_over_time(span:duration)",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(max.series[0].points == vec![(0, 300.0), (60_000, 50.0), (120_000, 0.0)]);
    }

    #[tokio::test]
    async fn quantile_over_time_emits_per_quantile_series() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                InputSpan {
                    duration_nanos: 100,
                    ..sp_at(1, 1, None, "api", 0)
                },
                InputSpan {
                    duration_nanos: 200,
                    ..sp_at(1, 2, None, "api", 10_000)
                },
                InputSpan {
                    duration_nanos: 300,
                    ..sp_at(1, 3, None, "api", 20_000)
                },
                InputSpan {
                    duration_nanos: 400,
                    ..sp_at(1, 4, None, "api", 30_000)
                },
                InputSpan {
                    duration_nanos: 500,
                    ..sp_at(1, 5, None, "api", 40_000)
                },
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut series = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | quantile_over_time(span:duration, .5, .9) | by(span.svc)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;
        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(series.len() == 2);
        assert!(series[0].labels == vec![("p".into(), "0.5".into()), ("svc".into(), "api".into())]);
        assert!(series[0].points == vec![(0, 300.0), (60_000, 0.0)]);
        assert!(series[1].labels == vec![("p".into(), "0.9".into()), ("svc".into(), "api".into())]);
        assert!(series[1].points == vec![(0, 460.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn histogram_over_time_emits_cumulative_buckets_sum_and_count() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                InputSpan {
                    duration_nanos: 1_000_000,
                    ..sp_at(1, 1, None, "api", 0)
                },
                InputSpan {
                    duration_nanos: 2_000_000_000,
                    ..sp_at(1, 2, None, "api", 10_000)
                },
                InputSpan {
                    duration_nanos: 12_000_000_000,
                    ..sp_at(1, 3, None, "api", 20_000)
                },
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut series = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | histogram_over_time(span:duration) | by(span.svc)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(series.iter().any(|s| {
            s.labels
                == vec![
                    ("le".into(), "2000000".into()),
                    ("svc".into(), "api".into()),
                ]
                && s.points == vec![(0, 1.0), (60_000, 0.0)]
        }));
        assert!(series.iter().any(|s| {
            s.labels
                == vec![
                    ("le".into(), "2048000000".into()),
                    ("svc".into(), "api".into()),
                ]
                && s.points == vec![(0, 2.0), (60_000, 0.0)]
        }));
        assert!(series.iter().any(|s| {
            s.labels == vec![("le".into(), "+Inf".into()), ("svc".into(), "api".into())]
                && s.points == vec![(0, 3.0), (60_000, 0.0)]
        }));
        assert!(series.iter().any(|s| {
            s.labels
                == vec![
                    ("__metric__".into(), "sum".into()),
                    ("svc".into(), "api".into()),
                ]
                && s.points == vec![(0, 14_001_000_000.0), (60_000, 0.0)]
        }));
        assert!(series.iter().any(|s| {
            s.labels
                == vec![
                    ("__metric__".into(), "count".into()),
                    ("svc".into(), "api".into()),
                ]
                && s.points == vec![(0, 3.0), (60_000, 0.0)]
        }));
    }

    #[tokio::test]
    async fn topk_and_bottomk_rank_grouped_metric_series() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "api", 0),
                sp_at(1, 2, None, "api", 10_000),
                sp_at(1, 3, None, "db", 20_000),
                sp_at(1, 4, None, "worker", 30_000),
                sp_at(1, 5, None, "worker", 40_000),
                sp_at(1, 6, None, "worker", 50_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let mut top = e
            .query_range(
                "t",
                "{ .svc != nil } | count_over_time() | by(span.svc) | topk(2)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;
        top.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(top.len() == 2);
        assert!(top[0].labels == vec![("svc".into(), "api".into())]);
        assert!(top[0].points == vec![(0, 2.0), (60_000, 0.0)]);
        assert!(top[1].labels == vec![("svc".into(), "worker".into())]);
        assert!(top[1].points == vec![(0, 3.0), (60_000, 0.0)]);

        let bottom = e
            .query_range(
                "t",
                "{ .svc != nil } | count_over_time() | by(span.svc) | bottomk(1)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(bottom.series.len() == 1);
        assert!(bottom.series[0].labels == vec![("svc".into(), "db".into())]);
        assert!(bottom.series[0].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn compare_emits_current_and_previous_range_series() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![sp_at(1, 1, None, "api", -120_000)]);
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(2, 1, None, "api", 0),
                sp_at(2, 2, None, "api", 10_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut series = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time() | by(span.svc) | compare()",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(series.len() == 2);
        assert!(
            series[0].labels
                == vec![
                    ("comparison".into(), "current".into()),
                    ("svc".into(), "api".into())
                ]
        );
        assert!(series[0].points == vec![(0, 2.0), (60_000, 0.0)]);
        assert!(
            series[1].labels
                == vec![
                    ("comparison".into(), "previous".into()),
                    ("svc".into(), "api".into())
                ]
        );
        assert!(series[1].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_carries_trace_id_exemplars() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(0x11, 0x22, None, "api", 0),
                sp_at(0x11, 0x33, None, "api", 10_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time()",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(got.series.len() == 1);
        assert!(got.series[0].exemplars.len() == 1);
        assert!(
            got.series[0].exemplars[0].labels
                == vec![
                    ("trace_id".into(), "11111111111111111111111111111111".into()),
                    ("span_id".into(), "2222222222222222".into())
                ]
        );
        assert!(got.series[0].exemplars[0].timestamp_ns == 0);
        assert!((got.series[0].exemplars[0].value - 1.0).abs() < f64::EPSILON);
    }
}
