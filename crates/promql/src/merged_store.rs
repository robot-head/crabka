//! Merge two metric stores, typically compacted cold blocks plus a hot WAL head.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crabka_blockstore::{LabelMatcher, Labels, SeriesFingerprint};
use crabka_metrics::{
    COL_FINGERPRINT, COL_TIMESTAMP, float_sample_schema, native_histogram_schema,
};
use datafusion::{catalog::MemTable, prelude::SessionContext};

use crate::{
    ExemplarRecord, LabelNameCardinality, LabelValueCardinality, MetadataRecord, MetricStore,
    NamedTsdbStat, PromqlError, ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
};

const FLOAT_TABLE: &str = "merged_float_samples";
const HISTOGRAM_TABLE: &str = "merged_native_histograms";

/// A `MetricStore` that queries two stores as one.
pub struct MergedMetricStore<C, H> {
    cold: C,
    hot: H,
}

impl<C, H> MergedMetricStore<C, H> {
    #[must_use]
    pub fn new(cold: C, hot: H) -> Self {
        Self { cold, hot }
    }
}

#[async_trait::async_trait]
impl<C, H> MetricStore for MergedMetricStore<C, H>
where
    C: MetricStore,
    H: MetricStore,
{
    #[tracing::instrument(
        name = "promql.merged_scan",
        level = "debug",
        skip_all,
        fields(tenant = %tenant, matchers = matchers.len(), start_ms = start_ms, end_ms = end_ms),
        err
    )]
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult, PromqlError> {
        let cold = self.cold.scan(tenant, matchers, start_ms, end_ms).await?;
        let hot = self.hot.scan(tenant, matchers, start_ms, end_ms).await?;
        let ctx = SessionContext::new();
        let float_table = merge_scan_table(
            &ctx,
            FLOAT_TABLE,
            float_sample_schema(),
            [
                (cold.ctx.clone(), cold.float_table.clone()),
                (hot.ctx.clone(), hot.float_table.clone()),
            ],
        )
        .await?;
        let histogram_table = merge_scan_table(
            &ctx,
            HISTOGRAM_TABLE,
            native_histogram_schema(),
            [
                (cold.ctx, cold.histogram_table),
                (hot.ctx, hot.histogram_table),
            ],
        )
        .await?;

        Ok(ScanResult {
            ctx,
            float_table,
            histogram_table,
        })
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, PromqlError> {
        let mut values = BTreeSet::new();
        values.extend(
            self.cold
                .label_names(tenant, matchers, start_ms, end_ms)
                .await?,
        );
        values.extend(
            self.hot
                .label_names(tenant, matchers, start_ms, end_ms)
                .await?,
        );
        Ok(values.into_iter().collect())
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, PromqlError> {
        let mut values = BTreeSet::new();
        values.extend(
            self.cold
                .label_values(tenant, name, matchers, start_ms, end_ms)
                .await?,
        );
        values.extend(
            self.hot
                .label_values(tenant, name, matchers, start_ms, end_ms)
                .await?,
        );
        Ok(values.into_iter().collect())
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>, PromqlError> {
        let mut by_fp = BTreeMap::<SeriesFingerprint, Labels>::new();
        for labels in self
            .cold
            .series(tenant, matchers, start_ms, end_ms)
            .await?
            .into_iter()
            .chain(self.hot.series(tenant, matchers, start_ms, end_ms).await?)
        {
            by_fp.entry(labels.fingerprint()).or_insert(labels);
        }
        Ok(by_fp.into_values().collect())
    }

    async fn exemplars(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ExemplarRecord>, PromqlError> {
        let mut exemplars = self
            .cold
            .exemplars(tenant, matchers, start_ms, end_ms)
            .await?;
        exemplars.extend(
            self.hot
                .exemplars(tenant, matchers, start_ms, end_ms)
                .await?,
        );
        exemplars.sort_by_key(|row| (row.series_labels.fingerprint(), row.ts_ms));
        Ok(exemplars)
    }

    async fn metadata(
        &self,
        tenant: &str,
        metric: Option<&str>,
    ) -> Result<Vec<MetadataRecord>, PromqlError> {
        let mut by_key = BTreeMap::<(String, String, String, String), MetadataRecord>::new();
        for record in self
            .cold
            .metadata(tenant, metric)
            .await?
            .into_iter()
            .chain(self.hot.metadata(tenant, metric).await?)
        {
            by_key.insert(
                (
                    record.metric_family_name.clone(),
                    record.metric_type.clone(),
                    record.help.clone(),
                    record.unit.clone(),
                ),
                record,
            );
        }
        Ok(by_key.into_values().collect())
    }

    async fn cardinality_label_names(
        &self,
        tenant: &str,
    ) -> Result<Vec<LabelNameCardinality>, PromqlError> {
        let series = self.cardinality_active_series(tenant).await?;
        let mut by_name = BTreeMap::<String, BTreeSet<SeriesFingerprint>>::new();
        for labels in series {
            let fp = labels.fingerprint();
            for (name, _) in labels.iter() {
                by_name.entry(name.clone()).or_default().insert(fp);
            }
        }
        Ok(label_name_cardinality(by_name))
    }

    async fn cardinality_label_values(
        &self,
        tenant: &str,
    ) -> Result<Vec<LabelValueCardinality>, PromqlError> {
        let series = self.cardinality_active_series(tenant).await?;
        let mut by_value = BTreeMap::<(String, String), BTreeSet<SeriesFingerprint>>::new();
        for labels in series {
            let fp = labels.fingerprint();
            for (name, value) in labels.iter() {
                by_value
                    .entry((name.clone(), value.clone()))
                    .or_default()
                    .insert(fp);
            }
        }
        Ok(label_value_cardinality(by_value))
    }

    async fn cardinality_active_series(&self, tenant: &str) -> Result<Vec<Labels>, PromqlError> {
        self.series(tenant, &[], i64::MIN, i64::MAX).await
    }

    async fn tsdb_stats(&self, tenant: &str) -> Result<TsdbStats, PromqlError> {
        let cold = self.cold.tsdb_stats(tenant).await?;
        let hot = self.hot.tsdb_stats(tenant).await?;
        let series = self.cardinality_active_series(tenant).await?;
        Ok(TsdbStats {
            head_stats: TsdbHeadStats {
                num_series: series.len(),
                num_samples: cold.head_stats.num_samples + hot.head_stats.num_samples,
                num_chunks: cold.head_stats.num_chunks + hot.head_stats.num_chunks,
                min_time: min_present_time(
                    (cold.head_stats.num_samples > 0).then_some(cold.head_stats.min_time),
                    (hot.head_stats.num_samples > 0).then_some(hot.head_stats.min_time),
                ),
                max_time: cold.head_stats.max_time.max(hot.head_stats.max_time),
            },
            series_count_by_metric_name: merge_named_stats(
                cold.series_count_by_metric_name,
                hot.series_count_by_metric_name,
            ),
            label_value_count_by_label_name: merge_named_stats(
                cold.label_value_count_by_label_name,
                hot.label_value_count_by_label_name,
            ),
            memory_in_bytes_by_label_name: merge_named_stats(
                cold.memory_in_bytes_by_label_name,
                hot.memory_in_bytes_by_label_name,
            ),
            series_count_by_label_value_pair: merge_named_stats(
                cold.series_count_by_label_value_pair,
                hot.series_count_by_label_value_pair,
            ),
        })
    }

    async fn tsdb_blocks(&self, tenant: &str) -> Result<Vec<TsdbBlock>, PromqlError> {
        let mut blocks = self.cold.tsdb_blocks(tenant).await?;
        blocks.extend(self.hot.tsdb_blocks(tenant).await?);
        blocks.sort_by(|left, right| {
            left.min_time
                .cmp(&right.min_time)
                .then_with(|| left.max_time.cmp(&right.max_time))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(blocks)
    }
}

async fn merge_scan_table<const N: usize>(
    ctx: &SessionContext,
    table_name: &str,
    schema: arrow::datatypes::SchemaRef,
    scans: [(SessionContext, Option<String>); N],
) -> Result<Option<String>, PromqlError> {
    // Register each non-empty source's batches under a private alias in the
    // output context, tagged with a `__src` priority literal. The `scans` array
    // is ordered `[cold, hot]`, so the array index doubles as the priority:
    // hot (higher index) is authoritative when both stores hold the same
    // `(fingerprint, timestamp)` sample. Without this dedup, any sample present
    // in both stores — the steady state, since hot retention is time-based and
    // independent of compaction — is double-counted by range/rate/aggregate
    // queries.
    let mut sources = Vec::new();
    for (priority, (scan_ctx, table)) in scans.into_iter().enumerate() {
        let Some(table) = table else {
            continue;
        };
        let dataframe = scan_ctx.sql(&format!("SELECT * FROM {table}")).await?;
        let batches = dataframe.collect().await?;
        if batches.is_empty() {
            continue;
        }
        let source_table = MemTable::try_new(schema.clone(), vec![batches])?;
        let source_name = format!("{table_name}__src{priority}");
        ctx.register_table(source_name.as_str(), Arc::new(source_table))?;
        sources.push((priority, source_name));
    }
    if sources.is_empty() {
        return Ok(None);
    }

    // Project the real schema columns explicitly so the `__src` helper column
    // never escapes into the output (which must equal the passed-in schema).
    let projection = schema
        .fields()
        .iter()
        .map(|field| quote_ident(field.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let fp_col = quote_ident(COL_FINGERPRINT);
    let ts_col = quote_ident(COL_TIMESTAMP);

    // UNION ALL the tagged sources, then keep exactly one row per
    // `(fingerprint, timestamp)`, preferring the highest-priority source (hot).
    let union = sources
        .iter()
        .map(|(priority, name)| {
            format!(
                "SELECT *, {priority} AS __src FROM {name}",
                name = quote_ident(name)
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let deduped = format!(
        "SELECT {projection} FROM (\
            SELECT *, ROW_NUMBER() OVER (\
                PARTITION BY {fp_col}, {ts_col} ORDER BY __src DESC\
            ) AS __rn FROM ({union}) AS __tagged\
        ) AS __ranked WHERE __rn = 1"
    );
    let dataframe = ctx.sql(&deduped).await?;
    let batches = dataframe.collect().await?;

    // Drop the private source aliases and register the deduped result under the
    // public table name so the output schema exactly equals the input schema.
    for (_, source_name) in &sources {
        ctx.deregister_table(source_name.as_str())?;
    }
    let table = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table(table_name, Arc::new(table))?;
    Ok(Some(table_name.to_string()))
}

/// Quote a SQL identifier for safe interpolation into a `DataFusion` query.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn label_name_cardinality(
    by_name: BTreeMap<String, BTreeSet<SeriesFingerprint>>,
) -> Vec<LabelNameCardinality> {
    let mut out = by_name
        .into_iter()
        .map(|(name, fingerprints)| LabelNameCardinality {
            name,
            series_count: fingerprints.len(),
        })
        .collect::<Vec<_>>();
    out.sort_by(|left, right| {
        right
            .series_count
            .cmp(&left.series_count)
            .then_with(|| left.name.cmp(&right.name))
    });
    out
}

fn label_value_cardinality(
    by_value: BTreeMap<(String, String), BTreeSet<SeriesFingerprint>>,
) -> Vec<LabelValueCardinality> {
    let mut out = by_value
        .into_iter()
        .map(
            |((label_name, label_value), fingerprints)| LabelValueCardinality {
                label_name,
                label_value,
                series_count: fingerprints.len(),
            },
        )
        .collect::<Vec<_>>();
    out.sort_by(|left, right| {
        right
            .series_count
            .cmp(&left.series_count)
            .then_with(|| left.label_name.cmp(&right.label_name))
            .then_with(|| left.label_value.cmp(&right.label_value))
    });
    out
}

fn merge_named_stats(left: Vec<NamedTsdbStat>, right: Vec<NamedTsdbStat>) -> Vec<NamedTsdbStat> {
    let mut values = BTreeMap::<String, usize>::new();
    for stat in left.into_iter().chain(right) {
        *values.entry(stat.name).or_default() += stat.value;
    }
    let mut out = values
        .into_iter()
        .map(|(name, value)| NamedTsdbStat { name, value })
        .collect::<Vec<_>>();
    out.sort_by(|left, right| {
        right
            .value
            .cmp(&left.value)
            .then_with(|| left.name.cmp(&right.name))
    });
    out
}

/// Combine the head min-time of two stores using explicit presence flags.
///
/// Emptiness is reported via `None` (the caller threads a has-data flag) rather
/// than overloading `0`, so a legitimate `min_time == 0` from a store that does
/// hold samples is preserved instead of being mistaken for "empty".
fn min_present_time(left: Option<i64>, right: Option<i64>) -> i64 {
    match (left, right) {
        (Some(left), Some(right)) => left.min(right),
        (Some(value), None) | (None, Some(value)) => value,
        (None, None) => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_blockstore::Labels;

    use crate::{
        EngineOpts, ExemplarRecord, InMemoryMetricStore, InstantSample, MergedMetricStore,
        MetricStore, NamedTsdbStat, PromqlEngine, QueryResult, SampleValue, TsdbHeadStats,
        TsdbStats,
    };

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert(*name, *value);
        }
        labels
    }

    #[tokio::test]
    async fn instant_query_uses_hot_sample_newer_than_compacted_sample() {
        let mut cold = InMemoryMetricStore::new();
        let mut hot = InMemoryMetricStore::new();
        let labels = labels(&[("__name__", "up"), ("job", "api")]);
        cold.push_float("tenant-a", labels.clone(), 10_000, 1.0);
        hot.push_float("tenant-a", labels.clone(), 20_000, 2.0);

        let store = MergedMetricStore::new(cold, hot);
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "up", 20_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected instant vector");
        };
        assert!(
            samples
                == vec![InstantSample {
                    labels,
                    ts_ms: 20_000,
                    value: SampleValue::Float(2.0),
                }]
        );
    }

    #[tokio::test]
    async fn label_names_merges_cold_and_hot_series_metadata() {
        let mut cold = InMemoryMetricStore::new();
        let mut hot = InMemoryMetricStore::new();
        cold.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("instance", "a"), ("job", "api")]),
            10_000,
            1.0,
        );
        hot.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("cluster", "prod"), ("job", "api")]),
            20_000,
            2.0,
        );

        let store = MergedMetricStore::new(cold, hot);
        let names = store.label_names("tenant-a", &[], 0, 30_000).await.unwrap();

        assert!(names == vec!["__name__", "cluster", "instance", "job"]);
    }

    #[tokio::test]
    async fn label_values_merges_cold_and_hot_series_metadata() {
        let mut cold = InMemoryMetricStore::new();
        let mut hot = InMemoryMetricStore::new();
        cold.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            10_000,
            1.0,
        );
        hot.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "worker")]),
            20_000,
            2.0,
        );
        hot.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            30_000,
            3.0,
        );

        let store = MergedMetricStore::new(cold, hot);
        let values = store
            .label_values("tenant-a", "job", &[], 0, 30_000)
            .await
            .unwrap();

        assert!(values == vec!["api", "worker"]);
    }

    #[tokio::test]
    async fn exemplars_merges_cold_and_hot_records() {
        let mut cold = InMemoryMetricStore::new();
        let mut hot = InMemoryMetricStore::new();
        let series = labels(&[("__name__", "request_latency"), ("job", "api")]);
        cold.push_exemplar(
            "tenant-a",
            series.clone(),
            labels(&[("trace_id", "cold")]),
            10_000,
            1.0,
        );
        hot.push_exemplar(
            "tenant-a",
            series.clone(),
            labels(&[("trace_id", "hot")]),
            20_000,
            2.0,
        );

        let store = MergedMetricStore::new(cold, hot);
        let exemplars = store.exemplars("tenant-a", &[], 0, 30_000).await.unwrap();

        assert!(
            exemplars
                == vec![
                    ExemplarRecord {
                        series_labels: series.clone(),
                        labels: labels(&[("trace_id", "cold")]),
                        ts_ms: 10_000,
                        value: 1.0,
                    },
                    ExemplarRecord {
                        series_labels: series,
                        labels: labels(&[("trace_id", "hot")]),
                        ts_ms: 20_000,
                        value: 2.0,
                    },
                ]
        );
    }

    #[tokio::test]
    async fn metadata_merges_cold_and_hot_records() {
        let mut cold = InMemoryMetricStore::new();
        let mut hot = InMemoryMetricStore::new();
        cold.push_metadata(
            "tenant-a",
            "requests_total",
            "counter",
            "requests",
            "requests",
        );
        cold.push_metadata("tenant-a", "up", "gauge", "availability", "");
        hot.push_metadata(
            "tenant-a",
            "requests_total",
            "counter",
            "requests",
            "requests",
        );
        hot.push_metadata(
            "tenant-a",
            "latency_seconds",
            "histogram",
            "latency",
            "seconds",
        );

        let store = MergedMetricStore::new(cold, hot);
        let metadata = store.metadata("tenant-a", None).await.unwrap();
        let fields = metadata
            .iter()
            .map(|record| {
                (
                    record.metric_family_name.as_str(),
                    record.metric_type.as_str(),
                    record.help.as_str(),
                    record.unit.as_str(),
                )
            })
            .collect::<Vec<_>>();

        assert!(
            fields
                == vec![
                    ("latency_seconds", "histogram", "latency", "seconds"),
                    ("requests_total", "counter", "requests", "requests"),
                    ("up", "gauge", "availability", ""),
                ]
        );
    }

    #[tokio::test]
    async fn cardinality_methods_merge_cold_and_hot_series() {
        let mut cold = InMemoryMetricStore::new();
        let mut hot = InMemoryMetricStore::new();
        let api = labels(&[("__name__", "up"), ("instance", "a"), ("job", "api")]);
        let worker = labels(&[("__name__", "up"), ("instance", "b"), ("job", "worker")]);
        cold.push_float("tenant-a", api.clone(), 10_000, 1.0);
        hot.push_float("tenant-a", worker.clone(), 20_000, 2.0);

        let store = MergedMetricStore::new(cold, hot);
        let mut active_series = store.cardinality_active_series("tenant-a").await.unwrap();
        active_series.sort_by_key(|labels| labels.get("instance").unwrap_or("").to_string());
        assert!(active_series == vec![api, worker]);

        let label_names = store.cardinality_label_names("tenant-a").await.unwrap();
        let name_counts = label_names
            .iter()
            .map(|stat| (stat.name.as_str(), stat.series_count))
            .collect::<Vec<_>>();
        assert!(name_counts == vec![("__name__", 2), ("instance", 2), ("job", 2)]);

        let label_values = store.cardinality_label_values("tenant-a").await.unwrap();
        let value_counts = label_values
            .iter()
            .map(|stat| {
                (
                    stat.label_name.as_str(),
                    stat.label_value.as_str(),
                    stat.series_count,
                )
            })
            .collect::<Vec<_>>();
        assert!(
            value_counts
                == vec![
                    ("__name__", "up", 2),
                    ("instance", "a", 1),
                    ("instance", "b", 1),
                    ("job", "api", 1),
                    ("job", "worker", 1),
                ]
        );
    }

    #[tokio::test]
    async fn tsdb_stats_merge_cold_and_hot_counts() {
        let mut cold = InMemoryMetricStore::new();
        let mut hot = InMemoryMetricStore::new();
        cold.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            10_000,
            1.0,
        );
        cold.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            20_000,
            2.0,
        );
        hot.push_float(
            "tenant-a",
            labels(&[("__name__", "errors_total"), ("job", "worker")]),
            30_000,
            3.0,
        );

        let store = MergedMetricStore::new(cold, hot);
        let stats = store.tsdb_stats("tenant-a").await.unwrap();

        let stat = |name: &str, value: usize| NamedTsdbStat {
            name: name.to_string(),
            value,
        };
        assert!(
            stats
                == TsdbStats {
                    head_stats: TsdbHeadStats {
                        num_series: 2,
                        num_samples: 3,
                        num_chunks: 2,
                        min_time: 10_000,
                        max_time: 30_000,
                    },
                    series_count_by_metric_name: vec![stat("errors_total", 1), stat("up", 1)],
                    label_value_count_by_label_name: vec![stat("__name__", 2), stat("job", 2)],
                    // Byte counts sum name.len() + value.len() per series:
                    // "__name__"+"up" (10) + "__name__"+"errors_total" (20) = 30;
                    // "job"+"api" (6) + "job"+"worker" (9) = 15.
                    memory_in_bytes_by_label_name: vec![stat("__name__", 30), stat("job", 15)],
                    series_count_by_label_value_pair: vec![
                        stat("__name__=errors_total", 1),
                        stat("__name__=up", 1),
                        stat("job=api", 1),
                        stat("job=worker", 1),
                    ],
                }
        );
    }

    #[tokio::test]
    async fn tsdb_stats_ignore_empty_side_min_time() {
        let mut hot_only = InMemoryMetricStore::new();
        hot_only.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            40_000,
            1.0,
        );
        let store = MergedMetricStore::new(InMemoryMetricStore::new(), hot_only);
        let stats = store.tsdb_stats("tenant-a").await.unwrap();
        assert!(stats.head_stats.min_time == 40_000);

        let mut cold_only = InMemoryMetricStore::new();
        cold_only.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            50_000,
            1.0,
        );
        let store = MergedMetricStore::new(cold_only, InMemoryMetricStore::new());
        let stats = store.tsdb_stats("tenant-a").await.unwrap();
        assert!(stats.head_stats.min_time == 50_000);
    }

    #[tokio::test]
    async fn tsdb_blocks_merges_cold_and_hot_blocks() {
        let mut cold = InMemoryMetricStore::new();
        let mut hot = InMemoryMetricStore::new();
        cold.push_tsdb_block("tenant-a", "cold-b", 30_000, 40_000, 3, 1);
        hot.push_tsdb_block("tenant-a", "hot-a", 10_000, 20_000, 2, 1);
        cold.push_tsdb_block("tenant-a", "cold-a", 10_000, 15_000, 1, 1);

        let store = MergedMetricStore::new(cold, hot);
        let blocks = store.tsdb_blocks("tenant-a").await.unwrap();
        let ids = blocks
            .iter()
            .map(|block| block.id.as_str())
            .collect::<Vec<_>>();

        assert!(ids == vec!["cold-a", "hot-a", "cold-b"]);
    }

    #[test]
    fn min_present_time_preserves_legitimate_zero_min_time() {
        // A store that holds samples whose earliest is epoch 0 must report 0,
        // not be treated as empty and discarded in favor of the other store.
        // Absent stores fall back to the present one; both-empty is 0.
        for (left, right, want) in [
            (Some(0), Some(50), 0),
            (Some(0), None, 0),
            (None, Some(0), 0),
            (None, Some(50), 50),
            (Some(50), None, 50),
            (None, None, 0),
            (Some(20), Some(50), 20),
        ] {
            assert!(
                super::min_present_time(left, right) == want,
                "case ({left:?}, {right:?})"
            );
        }
    }

    #[tokio::test]
    async fn range_query_counts_sample_present_in_both_stores_once() {
        // The same (fingerprint, timestamp) sample lives in both cold and hot —
        // the steady state, since hot retention is time-based and independent of
        // compaction. Without (fp, ts) dedup the merged scan double-counts it.
        let mut cold = InMemoryMetricStore::new();
        let mut hot = InMemoryMetricStore::new();
        let labels = labels(&[("__name__", "up"), ("job", "api")]);
        cold.push_float("tenant-a", labels.clone(), 10_000, 1.0);
        cold.push_float("tenant-a", labels.clone(), 20_000, 1.0);
        // Hot re-reports the 20s sample (still within hot retention) and adds 30s.
        hot.push_float("tenant-a", labels.clone(), 20_000, 1.0);
        hot.push_float("tenant-a", labels.clone(), 30_000, 1.0);

        let store = MergedMetricStore::new(cold, hot);
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let result = engine
            .query_instant("tenant-a", "count_over_time(up[1m])", 30_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected instant vector");
        };
        assert!(samples.len() == 1);
        // Three distinct timestamps (10s, 20s, 30s); the duplicated 20s sample
        // must be counted once, not twice.
        assert!(samples[0].value == SampleValue::Float(3.0));

        // A windowed sum must likewise see each timestamp once.
        let result = engine
            .query_instant("tenant-a", "sum_over_time(up[1m])", 30_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected instant vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].value == SampleValue::Float(3.0));
    }
}
