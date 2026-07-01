//! `MetricStore` adapter backed by `crabka-blockstore`.

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{Array, Float64Array, Int64Array, MapArray, StringArray, UInt64Array};
use crabka_blockstore::{
    BlockMeta, BlockStore, LabelMatcher, Labels, ScanTableRequest, SeriesFingerprint,
};
use crabka_metrics::{
    CompactionIndexManifest, MetricBlockKind, exemplar_schema, float_sample_schema,
    metadata_schema, native_histogram_schema,
};
use datafusion::prelude::SessionContext;

use crate::PromqlError;
use crate::error::Result;
use crate::store::{
    ExemplarRecord, LabelNameCardinality, LabelValueCardinality, MetadataRecord, MetricStore,
    NamedTsdbStat, ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
};

const FLOAT_TABLE: &str = "metric_float_samples";
const HISTOGRAM_TABLE: &str = "metric_native_histograms";
const EXEMPLAR_TABLE: &str = "metric_exemplars";
const METADATA_TABLE: &str = "metric_metadata";

/// `PromQL` metric store over compacted metric blocks.
#[derive(Clone)]
pub struct MetricBlockStore {
    floats: BlockStore,
    histograms: Option<BlockStore>,
    exemplars: Option<BlockStore>,
    metadata: Option<BlockStore>,
}

impl MetricBlockStore {
    #[must_use]
    pub fn new(float_store: BlockStore) -> Self {
        Self {
            floats: float_store,
            histograms: None,
            exemplars: None,
            metadata: None,
        }
    }

    #[must_use]
    pub fn with_histograms(float_store: BlockStore, histogram_store: BlockStore) -> Self {
        Self {
            floats: float_store,
            histograms: Some(histogram_store),
            exemplars: None,
            metadata: None,
        }
    }

    #[must_use]
    pub fn from_compaction_manifests(
        mut float_store: BlockStore,
        histogram_store: Option<BlockStore>,
        manifests: &[CompactionIndexManifest],
    ) -> Self {
        let mut histograms = histogram_store;
        let mut exemplars = None::<BlockStore>;
        let mut metadata = None::<BlockStore>;
        for manifest in manifests {
            match manifest.kind {
                MetricBlockKind::Float => apply_manifest_to_blockstore(&mut float_store, manifest),
                MetricBlockKind::NativeHistograms => {
                    if let Some(store) = &mut histograms {
                        apply_manifest_to_blockstore(store, manifest);
                    }
                }
                MetricBlockKind::Exemplars => {
                    let store = exemplars.get_or_insert_with(|| float_store.empty_like());
                    apply_manifest_to_blockstore(store, manifest);
                }
                MetricBlockKind::Metadata => {
                    let store = metadata.get_or_insert_with(|| float_store.empty_like());
                    apply_manifest_to_blockstore(store, manifest);
                }
            }
        }
        Self {
            floats: float_store,
            histograms,
            exemplars,
            metadata,
        }
    }

    fn matching_series(&self, tenant: &str, matchers: &[LabelMatcher]) -> Result<Vec<Labels>> {
        let mut by_fp = BTreeMap::<SeriesFingerprint, Labels>::new();
        for labels in self
            .floats
            .index()
            .series(tenant, matchers)
            .map_err(blockstore_error)?
        {
            by_fp.insert(labels.fingerprint(), labels);
        }
        if let Some(histograms) = &self.histograms {
            for labels in histograms
                .index()
                .series(tenant, matchers)
                .map_err(blockstore_error)?
            {
                by_fp.insert(labels.fingerprint(), labels);
            }
        }
        Ok(by_fp.into_values().collect())
    }
}

fn apply_manifest_to_blockstore(store: &mut BlockStore, manifest: &CompactionIndexManifest) {
    for series in &manifest.series {
        store
            .index_mut()
            .add_series(&manifest.tenant, series.fingerprint, &series.labels);
    }
    store.index_mut().add_block(&BlockMeta {
        tenant: manifest.tenant.clone(),
        object_key: manifest.block_key.clone(),
        min_ts: manifest.min_ts,
        max_ts: manifest.max_ts,
        row_count: manifest.row_count,
        fingerprints: manifest.fingerprints.clone(),
    });
}

#[async_trait::async_trait]
impl MetricStore for MetricBlockStore {
    #[tracing::instrument(
        name = "promql.blockstore_scan",
        level = "debug",
        skip_all,
        fields(
            tenant = %tenant,
            matchers = matchers.len(),
            start_ms = start_ms,
            end_ms = end_ms,
            has_float = tracing::field::Empty,
            has_histograms = tracing::field::Empty
        ),
        err
    )]
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult> {
        let ctx = SessionContext::new();
        let has_float = self
            .floats
            .register_scan_table(
                &ctx,
                ScanTableRequest {
                    table_name: FLOAT_TABLE,
                    tenant,
                    matchers,
                    min_ts: start_ms,
                    max_ts: end_ms,
                    schema: float_sample_schema(),
                },
            )
            .await
            .map_err(blockstore_error)?;
        let has_histograms = if let Some(histograms) = &self.histograms {
            histograms
                .register_scan_table(
                    &ctx,
                    ScanTableRequest {
                        table_name: HISTOGRAM_TABLE,
                        tenant,
                        matchers,
                        min_ts: start_ms,
                        max_ts: end_ms,
                        schema: native_histogram_schema(),
                    },
                )
                .await
                .map_err(blockstore_error)?
        } else {
            false
        };

        let span = tracing::Span::current();
        span.record("has_float", has_float);
        span.record("has_histograms", has_histograms);

        Ok(ScanResult {
            ctx,
            float_table: has_float.then(|| FLOAT_TABLE.to_string()),
            histogram_table: has_histograms.then(|| HISTOGRAM_TABLE.to_string()),
        })
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>> {
        let mut names = BTreeSet::new();
        for labels in self.matching_series(tenant, matchers)? {
            names.extend(labels.iter().map(|(name, _)| name.clone()));
        }
        Ok(names.into_iter().collect())
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>> {
        let mut values = BTreeSet::new();
        for labels in self.matching_series(tenant, matchers)? {
            if let Some(value) = labels.get(name) {
                values.insert(value.to_string());
            }
        }
        Ok(values.into_iter().collect())
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<Labels>> {
        self.matching_series(tenant, matchers)
    }

    async fn exemplars(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ExemplarRecord>> {
        let Some(exemplars) = &self.exemplars else {
            return Ok(Vec::new());
        };
        let series_by_fp = exemplars
            .index()
            .series(tenant, matchers)
            .map_err(blockstore_error)?
            .into_iter()
            .map(|labels| (labels.fingerprint(), labels))
            .collect::<BTreeMap<_, _>>();
        if series_by_fp.is_empty() {
            return Ok(Vec::new());
        }

        let ctx = SessionContext::new();
        exemplars
            .register_scan_table(
                &ctx,
                ScanTableRequest {
                    table_name: EXEMPLAR_TABLE,
                    tenant,
                    matchers,
                    min_ts: start_ms,
                    max_ts: end_ms,
                    schema: exemplar_schema(),
                },
            )
            .await
            .map_err(blockstore_error)?;
        let batches = ctx
            .table(EXEMPLAR_TABLE)
            .await
            .map_err(datafusion_error)?
            .collect()
            .await
            .map_err(datafusion_error)?;
        let mut exemplars = Vec::new();
        for batch in batches {
            exemplars.extend(exemplars_from_batch(
                &batch,
                &series_by_fp,
                start_ms,
                end_ms,
            )?);
        }
        exemplars.sort_by_key(|row| (row.series_labels.fingerprint(), row.ts_ms));
        Ok(exemplars)
    }

    async fn metadata(&self, tenant: &str, metric: Option<&str>) -> Result<Vec<MetadataRecord>> {
        let Some(metadata) = &self.metadata else {
            return Ok(Vec::new());
        };
        let matchers = metric.map_or_else(Vec::new, |metric| {
            vec![LabelMatcher {
                name: "__name__".to_string(),
                op: crabka_blockstore::MatchOp::Eq,
                value: metric.to_string(),
            }]
        });
        if metadata
            .index()
            .series(tenant, &matchers)
            .map_err(blockstore_error)?
            .is_empty()
        {
            return Ok(Vec::new());
        }

        let ctx = SessionContext::new();
        metadata
            .register_scan_table(
                &ctx,
                ScanTableRequest {
                    table_name: METADATA_TABLE,
                    tenant,
                    matchers: &matchers,
                    min_ts: 0,
                    max_ts: i64::MAX,
                    schema: metadata_schema(),
                },
            )
            .await
            .map_err(blockstore_error)?;
        let batches = ctx
            .table(METADATA_TABLE)
            .await
            .map_err(datafusion_error)?
            .collect()
            .await
            .map_err(datafusion_error)?;
        let mut records = BTreeSet::<(String, String, String, String)>::new();
        for batch in batches {
            for record in metadata_from_batch(&batch)? {
                records.insert((
                    record.metric_family_name,
                    record.metric_type,
                    record.help,
                    record.unit,
                ));
            }
        }
        Ok(records
            .into_iter()
            .map(
                |(metric_family_name, metric_type, help, unit)| MetadataRecord {
                    metric_family_name,
                    metric_type,
                    help,
                    unit,
                },
            )
            .collect())
    }

    async fn cardinality_label_names(&self, tenant: &str) -> Result<Vec<LabelNameCardinality>> {
        let mut by_name = BTreeMap::<String, BTreeSet<SeriesFingerprint>>::new();
        for labels in self.matching_series(tenant, &[])? {
            let fp = labels.fingerprint();
            for (name, _) in labels.iter() {
                by_name.entry(name.clone()).or_default().insert(fp);
            }
        }
        let mut cardinality = by_name
            .into_iter()
            .map(|(name, fingerprints)| LabelNameCardinality {
                name,
                series_count: fingerprints.len(),
            })
            .collect::<Vec<_>>();
        cardinality.sort_by(|left, right| {
            right
                .series_count
                .cmp(&left.series_count)
                .then_with(|| left.name.cmp(&right.name))
        });
        Ok(cardinality)
    }

    async fn cardinality_label_values(&self, tenant: &str) -> Result<Vec<LabelValueCardinality>> {
        let mut by_value = BTreeMap::<(String, String), BTreeSet<SeriesFingerprint>>::new();
        for labels in self.matching_series(tenant, &[])? {
            let fp = labels.fingerprint();
            for (name, value) in labels.iter() {
                by_value
                    .entry((name.clone(), value.clone()))
                    .or_default()
                    .insert(fp);
            }
        }
        let mut cardinality = by_value
            .into_iter()
            .map(
                |((label_name, label_value), fingerprints)| LabelValueCardinality {
                    label_name,
                    label_value,
                    series_count: fingerprints.len(),
                },
            )
            .collect::<Vec<_>>();
        cardinality.sort_by(|left, right| {
            right
                .series_count
                .cmp(&left.series_count)
                .then_with(|| left.label_name.cmp(&right.label_name))
                .then_with(|| left.label_value.cmp(&right.label_value))
        });
        Ok(cardinality)
    }

    async fn cardinality_active_series(&self, tenant: &str) -> Result<Vec<Labels>> {
        self.matching_series(tenant, &[])
    }

    async fn tsdb_stats(&self, tenant: &str) -> Result<TsdbStats> {
        let series = self.matching_series(tenant, &[])?;
        let mut by_metric = BTreeMap::<String, usize>::new();
        let mut label_values_by_name = BTreeMap::<String, BTreeSet<String>>::new();
        let mut memory_by_name = BTreeMap::<String, usize>::new();
        let mut by_label_pair = BTreeMap::<String, usize>::new();
        for labels in &series {
            if let Some(metric) = labels.get("__name__") {
                *by_metric.entry(metric.to_string()).or_default() += 1;
            }
            for (name, value) in labels.iter() {
                label_values_by_name
                    .entry(name.clone())
                    .or_default()
                    .insert(value.clone());
                *memory_by_name.entry(name.clone()).or_default() += name.len() + value.len();
                *by_label_pair.entry(format!("{name}={value}")).or_default() += 1;
            }
        }

        Ok(TsdbStats {
            head_stats: TsdbHeadStats {
                num_series: series.len(),
                num_samples: 0,
                num_chunks: series.len(),
                min_time: 0,
                max_time: 0,
            },
            series_count_by_metric_name: named_stats(by_metric),
            label_value_count_by_label_name: named_stats(
                label_values_by_name
                    .into_iter()
                    .map(|(name, values)| (name, values.len()))
                    .collect(),
            ),
            memory_in_bytes_by_label_name: named_stats(memory_by_name),
            series_count_by_label_value_pair: named_stats(by_label_pair),
        })
    }

    async fn tsdb_blocks(&self, tenant: &str) -> Result<Vec<TsdbBlock>> {
        let mut blocks = self
            .floats
            .index()
            .all_blocks(tenant)
            .into_iter()
            .chain(
                self.histograms
                    .as_ref()
                    .into_iter()
                    .flat_map(|store| store.index().all_blocks(tenant)),
            )
            .map(|block| TsdbBlock {
                id: block.object_key,
                min_time: block.min_ts,
                max_time: block.max_ts,
                num_samples: block.row_count,
                num_series: block.fingerprints.len(),
            })
            .collect::<Vec<_>>();
        blocks.sort_by(|left, right| {
            left.min_time
                .cmp(&right.min_time)
                .then_with(|| left.max_time.cmp(&right.max_time))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(blocks)
    }
}

fn blockstore_error(error: crabka_blockstore::BlockStoreError) -> PromqlError {
    let message = error.to_string();
    drop(error);
    PromqlError::Store(message)
}

fn datafusion_error(error: datafusion::error::DataFusionError) -> PromqlError {
    let message = error.to_string();
    drop(error);
    PromqlError::Store(message)
}

fn exemplars_from_batch(
    batch: &arrow::record_batch::RecordBatch,
    series_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<ExemplarRecord>> {
    let fingerprints = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| PromqlError::Store("exemplar fingerprint column has wrong type".into()))?;
    let timestamps = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| PromqlError::Store("exemplar timestamp column has wrong type".into()))?;
    let values = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| PromqlError::Store("exemplar value column has wrong type".into()))?;
    let trace_ids = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromqlError::Store("exemplar trace_id column has wrong type".into()))?;
    let span_ids = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromqlError::Store("exemplar span_id column has wrong type".into()))?;
    let label_maps = batch
        .column(5)
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| PromqlError::Store("exemplar labels column has wrong type".into()))?;

    let mut out = Vec::new();
    for row in 0..batch.num_rows() {
        let fp = fingerprints.value(row);
        let Some(series_labels) = series_by_fp.get(&fp) else {
            continue;
        };
        let ts_ms = timestamps.value(row);
        if ts_ms < start_ms || ts_ms > end_ms {
            continue;
        }
        let mut labels = Labels::new();
        if !trace_ids.is_null(row) {
            labels.insert("trace_id", trace_ids.value(row));
        }
        if !span_ids.is_null(row) {
            labels.insert("span_id", span_ids.value(row));
        }
        append_exemplar_label_map(&mut labels, label_maps, row)?;
        out.push(ExemplarRecord {
            series_labels: series_labels.clone(),
            labels,
            ts_ms,
            value: values.value(row),
        });
    }
    Ok(out)
}

fn append_exemplar_label_map(labels: &mut Labels, label_maps: &MapArray, row: usize) -> Result<()> {
    let entries = label_maps.value(row);
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromqlError::Store("exemplar label map key column has wrong type".into()))?;
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            PromqlError::Store("exemplar label map value column has wrong type".into())
        })?;
    for index in 0..entries.len() {
        labels.insert(keys.value(index), values.value(index));
    }
    Ok(())
}

fn metadata_from_batch(batch: &arrow::record_batch::RecordBatch) -> Result<Vec<MetadataRecord>> {
    let names = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            PromqlError::Store("metadata metric_family_name column has wrong type".into())
        })?;
    let types = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromqlError::Store("metadata metric_type column has wrong type".into()))?;
    let helps = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromqlError::Store("metadata help column has wrong type".into()))?;
    let units = batch
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromqlError::Store("metadata unit column has wrong type".into()))?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        out.push(MetadataRecord {
            metric_family_name: names.value(row).to_string(),
            metric_type: types.value(row).to_string(),
            help: helps.value(row).to_string(),
            unit: units.value(row).to_string(),
        });
    }
    Ok(out)
}

fn named_stats(values: BTreeMap<String, usize>) -> Vec<NamedTsdbStat> {
    let mut stats = values
        .into_iter()
        .map(|(name, value)| NamedTsdbStat { name, value })
        .collect::<Vec<_>>();
    stats.sort_by(|left, right| {
        right
            .value
            .cmp(&left.value)
            .then_with(|| left.name.cmp(&right.name))
    });
    stats
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, Float64Builder, Int64Builder, MapBuilder, StringBuilder, UInt64Builder,
    };
    use arrow::datatypes::{DataType, Field};
    use arrow::record_batch::RecordBatch;
    use assert2::assert;
    use crabka_blockstore::{BlockStore, Labels};
    use crabka_metrics::{
        CompactionIndexManifest, CompactionObjectPlan, CompactionSeriesLabels, MetricBlockKind,
        encode_float_samples, exemplar_schema, float_sample_schema, metadata_schema,
    };
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use crate::{EngineOpts, MetricStore, PromqlEngine, QueryResult, SampleValue};

    use super::MetricBlockStore;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert(*name, *value);
        }
        labels
    }

    #[tokio::test]
    async fn prometheus_query_reads_float_samples_from_blockstore() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let mut block_store = BlockStore::new(object_store, base);

        let series_labels = labels(&[("__name__", "up"), ("job", "api")]);
        let fp = series_labels.fingerprint();
        let batch = encode_float_samples(&[(fp, 1_000, 1.0)]).unwrap();
        let block_meta = block_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/float/0001.parquet",
                float_sample_schema(),
                &[batch],
            )
            .await
            .unwrap();
        block_store
            .index_mut()
            .add_series("tenant-a", fp, &series_labels);
        block_store.index_mut().add_block(&block_meta);

        let store = MetricBlockStore::new(block_store);
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine.query_instant("tenant-a", "up", 1_000).await.unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected instant vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels == series_labels);
        assert!(samples[0].ts_ms == 1_000);
        assert!(samples[0].value == SampleValue::Float(1.0));
    }

    #[tokio::test]
    async fn prometheus_query_rebuilds_float_index_from_compaction_manifest() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = BlockStore::new(object_store.clone(), base.clone());

        let series_labels = labels(&[("__name__", "up"), ("job", "api")]);
        let fp = series_labels.fingerprint();
        let batch = encode_float_samples(&[(fp, 1_000, 1.0)]).unwrap();
        let block_meta = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/float/0001.parquet",
                float_sample_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let manifest = CompactionIndexManifest::from_block_meta(
            MetricBlockKind::Float,
            &CompactionObjectPlan {
                block_key: block_meta.object_key.clone(),
                index_key: "metrics/float/0001.index".to_string(),
                first_offset: 0,
                last_offset: 0,
                row_count: block_meta.row_count,
            },
            &block_meta,
            vec![CompactionSeriesLabels {
                fingerprint: fp,
                labels: series_labels.clone(),
            }],
        );

        let fresh_store = BlockStore::new(object_store, base);
        let store = MetricBlockStore::from_compaction_manifests(fresh_store, None, &[manifest]);
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine.query_instant("tenant-a", "up", 1_000).await.unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected instant vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels == series_labels);
        assert!(samples[0].value == SampleValue::Float(1.0));
    }

    #[tokio::test]
    async fn tsdb_blocks_reports_compaction_manifest_blocks() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = BlockStore::new(object_store.clone(), base.clone());

        let series_labels = labels(&[("__name__", "up"), ("job", "api")]);
        let fp = series_labels.fingerprint();
        let batch = encode_float_samples(&[(fp, 1_000, 1.0), (fp, 2_000, 0.0)]).unwrap();
        let block_meta = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/float/0002.parquet",
                float_sample_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let manifest = CompactionIndexManifest::from_block_meta(
            MetricBlockKind::Float,
            &CompactionObjectPlan {
                block_key: block_meta.object_key.clone(),
                index_key: "metrics/float/0002.index".to_string(),
                first_offset: 0,
                last_offset: 1,
                row_count: block_meta.row_count,
            },
            &block_meta,
            vec![CompactionSeriesLabels {
                fingerprint: fp,
                labels: series_labels,
            }],
        );

        let fresh_store = BlockStore::new(object_store, base);
        let store = MetricBlockStore::from_compaction_manifests(fresh_store, None, &[manifest]);
        let blocks = store.tsdb_blocks("tenant-a").await.unwrap();

        assert!(blocks.len() == 1);
        assert!(blocks[0].id == "metrics/float/0002.parquet");
        assert!(blocks[0].min_time == 1_000);
        assert!(blocks[0].max_time == 2_000);
        assert!(blocks[0].num_samples == 2);
        assert!(blocks[0].num_series == 1);
    }

    #[tokio::test]
    async fn index_metadata_methods_report_float_and_histogram_series() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let mut floats = BlockStore::new(object_store.clone(), base.clone());
        let mut histograms = BlockStore::new(object_store, base);

        let up = labels(&[("__name__", "up"), ("instance", "a"), ("job", "api")]);
        let latency = labels(&[
            ("__name__", "http_request_duration_seconds"),
            ("instance", "b"),
            ("job", "api"),
            ("le", "0.5"),
        ]);
        floats
            .index_mut()
            .add_series("tenant-a", up.fingerprint(), &up);
        histograms
            .index_mut()
            .add_series("tenant-a", latency.fingerprint(), &latency);
        let store = MetricBlockStore::with_histograms(floats, histograms);

        let names = store.label_names("tenant-a", &[], 0, 10_000).await.unwrap();
        assert!(names == vec!["__name__", "instance", "job", "le"]);

        let job_values = store
            .label_values("tenant-a", "job", &[], 0, 10_000)
            .await
            .unwrap();
        assert!(job_values == vec!["api"]);
        let instance_values = store
            .label_values("tenant-a", "instance", &[], 0, 10_000)
            .await
            .unwrap();
        assert!(instance_values == vec!["a", "b"]);

        let mut active_series = store.cardinality_active_series("tenant-a").await.unwrap();
        active_series.sort_by_key(|labels| labels.get("instance").unwrap_or("").to_string());
        assert!(active_series == vec![up, latency]);

        let label_names = store.cardinality_label_names("tenant-a").await.unwrap();
        let name_counts = label_names
            .iter()
            .map(|stat| (stat.name.as_str(), stat.series_count))
            .collect::<Vec<_>>();
        assert!(name_counts == vec![("__name__", 2), ("instance", 2), ("job", 2), ("le", 1)]);

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
                    ("job", "api", 2),
                    ("__name__", "http_request_duration_seconds", 1),
                    ("__name__", "up", 1),
                    ("instance", "a", 1),
                    ("instance", "b", 1),
                    ("le", "0.5", 1),
                ]
        );

        let stats = store.tsdb_stats("tenant-a").await.unwrap();
        assert!(stats.head_stats.num_series == 2);
        assert!(stats.head_stats.num_samples == 0);
        assert!(stats.head_stats.num_chunks == 2);
        assert!(stats.head_stats.min_time == 0);
        assert!(stats.head_stats.max_time == 0);
        assert!(
            named_pairs(&stats.series_count_by_metric_name)
                == vec![("http_request_duration_seconds", 1), ("up", 1)]
        );
        assert!(
            named_pairs(&stats.label_value_count_by_label_name)
                == vec![("__name__", 2), ("instance", 2), ("job", 1), ("le", 1)]
        );
        assert!(
            named_pairs(&stats.memory_in_bytes_by_label_name)
                == vec![("__name__", 47), ("instance", 18), ("job", 12), ("le", 5)]
        );
        assert!(
            named_pairs(&stats.series_count_by_label_value_pair)
                == vec![
                    ("job=api", 2),
                    ("__name__=http_request_duration_seconds", 1),
                    ("__name__=up", 1),
                    ("instance=a", 1),
                    ("instance=b", 1),
                    ("le=0.5", 1),
                ]
        );
    }

    #[tokio::test]
    async fn exemplars_reads_compacted_exemplar_sidecar_blocks() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = BlockStore::new(object_store.clone(), base.clone());

        let series_labels = labels(&[("__name__", "http_requests_total"), ("job", "api")]);
        let fp = series_labels.fingerprint();
        let batch = exemplar_batch(fp, 10_500, 7.0, "abc", "def", "kind", "slow");
        let block_meta = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/exemplars/0003.parquet",
                exemplar_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let manifest = CompactionIndexManifest::from_block_meta(
            MetricBlockKind::Exemplars,
            &CompactionObjectPlan {
                block_key: block_meta.object_key.clone(),
                index_key: "metrics/exemplars/0003.index".to_string(),
                first_offset: 0,
                last_offset: 0,
                row_count: block_meta.row_count,
            },
            &block_meta,
            vec![CompactionSeriesLabels {
                fingerprint: fp,
                labels: series_labels.clone(),
            }],
        );

        let fresh_store = BlockStore::new(object_store, base);
        let store = MetricBlockStore::from_compaction_manifests(fresh_store, None, &[manifest]);
        let exemplars = store
            .exemplars(
                "tenant-a",
                &[crabka_blockstore::LabelMatcher {
                    name: "job".to_string(),
                    op: crabka_blockstore::MatchOp::Eq,
                    value: "api".to_string(),
                }],
                10_000,
                11_000,
            )
            .await
            .unwrap();

        assert!(exemplars.len() == 1);
        assert!(exemplars[0].series_labels == series_labels);
        assert!(exemplars[0].labels.get("trace_id") == Some("abc"));
        assert!(exemplars[0].labels.get("span_id") == Some("def"));
        assert!(exemplars[0].labels.get("kind") == Some("slow"));
        assert!(exemplars[0].ts_ms == 10_500);
        assert!((exemplars[0].value - 7.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn exemplars_include_closed_range_boundaries_and_filter_outside_rows() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = BlockStore::new(object_store.clone(), base.clone());

        let series_labels = labels(&[("__name__", "http_requests_total"), ("job", "api")]);
        let fp = series_labels.fingerprint();
        let batch = exemplar_batch_from_rows(&[
            (fp, 9_999, 1.0, "too-low", "s1", "kind", "outside"),
            (fp, 10_000, 2.0, "start", "s2", "kind", "inside"),
            (fp, 11_000, 3.0, "end", "s3", "kind", "inside"),
            (fp, 11_001, 4.0, "too-high", "s4", "kind", "outside"),
        ]);
        let block_meta = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/exemplars/0005.parquet",
                exemplar_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let manifest = CompactionIndexManifest::from_block_meta(
            MetricBlockKind::Exemplars,
            &CompactionObjectPlan {
                block_key: block_meta.object_key.clone(),
                index_key: "metrics/exemplars/0005.index".to_string(),
                first_offset: 0,
                last_offset: 3,
                row_count: block_meta.row_count,
            },
            &block_meta,
            vec![CompactionSeriesLabels {
                fingerprint: fp,
                labels: series_labels.clone(),
            }],
        );

        let fresh_store = BlockStore::new(object_store, base);
        let store = MetricBlockStore::from_compaction_manifests(fresh_store, None, &[manifest]);
        let exemplars = store
            .exemplars(
                "tenant-a",
                &[crabka_blockstore::LabelMatcher {
                    name: "job".to_string(),
                    op: crabka_blockstore::MatchOp::Eq,
                    value: "api".to_string(),
                }],
                10_000,
                11_000,
            )
            .await
            .unwrap();

        assert!(exemplars.len() == 2);
        assert!(exemplars[0].series_labels == series_labels);
        assert!(exemplars[0].labels.get("trace_id") == Some("start"));
        assert!(exemplars[0].labels.get("span_id") == Some("s2"));
        assert!(exemplars[0].labels.get("kind") == Some("inside"));
        assert!(exemplars[0].ts_ms == 10_000);
        assert!(exemplars[0].value.to_bits() == 2.0_f64.to_bits());
        assert!(exemplars[1].series_labels == series_labels);
        assert!(exemplars[1].labels.get("trace_id") == Some("end"));
        assert!(exemplars[1].labels.get("span_id") == Some("s3"));
        assert!(exemplars[1].labels.get("kind") == Some("inside"));
        assert!(exemplars[1].ts_ms == 11_000);
        assert!(exemplars[1].value.to_bits() == 3.0_f64.to_bits());
    }

    #[tokio::test]
    async fn metadata_reads_compacted_metadata_sidecar_blocks() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = BlockStore::new(object_store.clone(), base.clone());

        let series_labels = labels(&[("__name__", "http_requests_total")]);
        let fp = series_labels.fingerprint();
        let batch = metadata_batch(
            fp,
            "http_requests_total",
            "counter",
            "Total HTTP requests.",
            "requests",
        );
        let block_meta = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/metadata/0004.parquet",
                metadata_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let manifest = CompactionIndexManifest::from_block_meta(
            MetricBlockKind::Metadata,
            &CompactionObjectPlan {
                block_key: block_meta.object_key.clone(),
                index_key: "metrics/metadata/0004.index".to_string(),
                first_offset: 0,
                last_offset: 0,
                row_count: block_meta.row_count,
            },
            &block_meta,
            vec![CompactionSeriesLabels {
                fingerprint: fp,
                labels: series_labels,
            }],
        );

        let fresh_store = BlockStore::new(object_store, base);
        let store = MetricBlockStore::from_compaction_manifests(fresh_store, None, &[manifest]);
        let metadata = store
            .metadata("tenant-a", Some("http_requests_total"))
            .await
            .unwrap();

        assert!(metadata.len() == 1);
        assert!(metadata[0].metric_family_name == "http_requests_total");
        assert!(metadata[0].metric_type == "counter");
        assert!(metadata[0].help == "Total HTTP requests.");
        assert!(metadata[0].unit == "requests");
    }

    fn exemplar_batch(
        fingerprint: u64,
        timestamp_ms: i64,
        value: f64,
        trace_id: &str,
        span_id: &str,
        label_name: &str,
        label_value: &str,
    ) -> RecordBatch {
        exemplar_batch_from_rows(&[(
            fingerprint,
            timestamp_ms,
            value,
            trace_id,
            span_id,
            label_name,
            label_value,
        )])
    }

    fn exemplar_batch_from_rows(rows: &[(u64, i64, f64, &str, &str, &str, &str)]) -> RecordBatch {
        let mut fingerprints = UInt64Builder::new();
        let mut timestamps = Int64Builder::new();
        let mut values = Float64Builder::new();
        let mut trace_ids = StringBuilder::new();
        let mut span_ids = StringBuilder::new();
        let mut labels = MapBuilder::new(
            Some(arrow::array::builder::MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            StringBuilder::new(),
            StringBuilder::new(),
        )
        .with_values_field(Field::new("value", DataType::Utf8, false));

        for (fingerprint, timestamp_ms, value, trace_id, span_id, label_name, label_value) in rows {
            fingerprints.append_value(*fingerprint);
            timestamps.append_value(*timestamp_ms);
            values.append_value(*value);
            trace_ids.append_value(*trace_id);
            span_ids.append_value(*span_id);
            labels.keys().append_value(*label_name);
            labels.values().append_value(*label_value);
            labels.append(true).unwrap();
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(fingerprints.finish()),
            Arc::new(timestamps.finish()),
            Arc::new(values.finish()),
            Arc::new(trace_ids.finish()),
            Arc::new(span_ids.finish()),
            Arc::new(labels.finish()),
        ];
        RecordBatch::try_new(exemplar_schema(), columns).unwrap()
    }

    fn metadata_batch(
        fingerprint: u64,
        metric_family_name: &str,
        metric_type: &str,
        help: &str,
        unit: &str,
    ) -> RecordBatch {
        let mut fingerprints = UInt64Builder::new();
        let mut timestamps = Int64Builder::new();
        let mut names = StringBuilder::new();
        let mut types = StringBuilder::new();
        let mut helps = StringBuilder::new();
        let mut units = StringBuilder::new();

        fingerprints.append_value(fingerprint);
        timestamps.append_value(0);
        names.append_value(metric_family_name);
        types.append_value(metric_type);
        helps.append_value(help);
        units.append_value(unit);

        let columns: Vec<ArrayRef> = vec![
            Arc::new(fingerprints.finish()),
            Arc::new(timestamps.finish()),
            Arc::new(names.finish()),
            Arc::new(types.finish()),
            Arc::new(helps.finish()),
            Arc::new(units.finish()),
        ];
        RecordBatch::try_new(metadata_schema(), columns).unwrap()
    }

    fn named_pairs(stats: &[crate::NamedTsdbStat]) -> Vec<(&str, usize)> {
        stats
            .iter()
            .map(|stat| (stat.name.as_str(), stat.value))
            .collect()
    }
}
