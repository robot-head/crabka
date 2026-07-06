use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crabka_blockstore::{LabelMatcher, Labels, SeriesFingerprint};
use crabka_metrics::{
    encode_float_samples, encode_native_histograms, float_sample_schema, native_histogram_schema,
};
use datafusion::{catalog::MemTable, prelude::SessionContext};

use super::{
    InMemoryMetricStore,
    matcher::{all_match, prepare_matchers, row_matches},
};
use crate::{
    PromqlError,
    error::Result,
    store::{
        ExemplarRecord, LabelNameCardinality, LabelValueCardinality, MetadataRecord, MetricStore,
        NamedTsdbStat, ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
    },
};

#[async_trait::async_trait]
impl MetricStore for InMemoryMetricStore {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult> {
        let ctx = SessionContext::new();
        let matchers = prepare_matchers(matchers)?;

        let mut float_rows = Vec::new();
        if let Some(rows) = self.floats.get(tenant) {
            for row in rows {
                if row_matches(row.fp, &row.labels, row.ts_ms, &matchers, start_ms, end_ms) {
                    float_rows.push((row.fp, row.ts_ms, row.value));
                }
            }
        }
        float_rows.sort_by_key(|(fp, ts, _)| (*fp, *ts));
        let float_table = if float_rows.is_empty() {
            None
        } else {
            let batch = encode_float_samples(&float_rows)
                .map_err(|error| PromqlError::Store(error.to_string()))?;
            let table = MemTable::try_new(float_sample_schema(), vec![vec![batch]])?;
            ctx.register_table("floats", Arc::new(table))?;
            Some("floats".to_string())
        };

        let mut hist_rows = Vec::new();
        if let Some(rows) = self.hists.get(tenant) {
            for row in rows {
                if row_matches(row.fp, &row.labels, row.ts_ms, &matchers, start_ms, end_ms) {
                    hist_rows.push((row.fp, row.ts_ms, row.hist.clone()));
                }
            }
        }
        hist_rows.sort_by_key(|(fp, ts, _)| (*fp, *ts));
        let histogram_table = if hist_rows.is_empty() {
            None
        } else {
            let batch = encode_native_histograms(&hist_rows)
                .map_err(|error| PromqlError::Store(error.to_string()))?;
            let table = MemTable::try_new(native_histogram_schema(), vec![vec![batch]])?;
            ctx.register_table("histograms", Arc::new(table))?;
            Some("histograms".to_string())
        };

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
    ) -> Result<Vec<String>> {
        let mut names = BTreeSet::new();
        for labels in self.matched_series(tenant, matchers, start_ms, end_ms)? {
            for (name, _) in labels.iter() {
                names.insert(name.clone());
            }
        }
        Ok(names.into_iter().collect())
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>> {
        let mut values = BTreeSet::new();
        for labels in self.matched_series(tenant, matchers, start_ms, end_ms)? {
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
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>> {
        self.matched_series(tenant, matchers, start_ms, end_ms)
    }

    async fn exemplars(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ExemplarRecord>> {
        let matchers = prepare_matchers(matchers)?;
        let mut exemplars = Vec::new();
        if let Some(rows) = self.exemplars.get(tenant) {
            for row in rows {
                if row.ts_ms >= start_ms
                    && row.ts_ms <= end_ms
                    && all_match(
                        row.series_labels.fingerprint(),
                        &row.series_labels,
                        &matchers,
                    )
                {
                    exemplars.push(ExemplarRecord {
                        series_labels: row.series_labels.clone(),
                        labels: row.labels.clone(),
                        ts_ms: row.ts_ms,
                        value: row.value,
                    });
                }
            }
        }
        exemplars.sort_by_key(|row| (row.series_labels.fingerprint(), row.ts_ms));
        Ok(exemplars)
    }

    async fn metadata(&self, tenant: &str, metric: Option<&str>) -> Result<Vec<MetadataRecord>> {
        let mut metadata = self
            .metadata
            .get(tenant)
            .into_iter()
            .flatten()
            .filter(|record| {
                metric.is_none_or(|metric| metric == record.metric_family_name.as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        metadata.sort_by(|left, right| {
            left.metric_family_name
                .cmp(&right.metric_family_name)
                .then_with(|| left.metric_type.cmp(&right.metric_type))
                .then_with(|| left.help.cmp(&right.help))
                .then_with(|| left.unit.cmp(&right.unit))
        });
        Ok(metadata)
    }

    async fn cardinality_label_names(&self, tenant: &str) -> Result<Vec<LabelNameCardinality>> {
        let mut by_name = BTreeMap::<String, BTreeSet<SeriesFingerprint>>::new();
        if let Some(rows) = self.floats.get(tenant) {
            for row in rows {
                for (name, _) in row.labels.iter() {
                    by_name.entry(name.clone()).or_default().insert(row.fp);
                }
            }
        }
        if let Some(rows) = self.hists.get(tenant) {
            for row in rows {
                for (name, _) in row.labels.iter() {
                    by_name.entry(name.clone()).or_default().insert(row.fp);
                }
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
        if let Some(rows) = self.floats.get(tenant) {
            for row in rows {
                for (name, value) in row.labels.iter() {
                    by_value
                        .entry((name.clone(), value.clone()))
                        .or_default()
                        .insert(row.fp);
                }
            }
        }
        if let Some(rows) = self.hists.get(tenant) {
            for row in rows {
                for (name, value) in row.labels.iter() {
                    by_value
                        .entry((name.clone(), value.clone()))
                        .or_default()
                        .insert(row.fp);
                }
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
        let mut by_fp = BTreeMap::<SeriesFingerprint, Labels>::new();
        if let Some(rows) = self.floats.get(tenant) {
            for row in rows {
                by_fp.entry(row.fp).or_insert_with(|| row.labels.clone());
            }
        }
        if let Some(rows) = self.hists.get(tenant) {
            for row in rows {
                by_fp.entry(row.fp).or_insert_with(|| row.labels.clone());
            }
        }

        let mut series = by_fp.into_values().collect::<Vec<_>>();
        series.sort_by_key(|labels| {
            labels.iter().fold(String::new(), |mut out, (name, value)| {
                out.push_str(name);
                out.push('=');
                out.push_str(value);
                out.push('\n');
                out
            })
        });
        Ok(series)
    }

    async fn tsdb_stats(&self, tenant: &str) -> Result<TsdbStats> {
        let mut series = BTreeMap::<SeriesFingerprint, Labels>::new();
        let mut sample_count = 0usize;
        let mut min_time = i64::MAX;
        let mut max_time = i64::MIN;

        if let Some(rows) = self.floats.get(tenant) {
            for row in rows {
                sample_count += 1;
                min_time = min_time.min(row.ts_ms);
                max_time = max_time.max(row.ts_ms);
                series.entry(row.fp).or_insert_with(|| row.labels.clone());
            }
        }
        if let Some(rows) = self.hists.get(tenant) {
            for row in rows {
                sample_count += 1;
                min_time = min_time.min(row.ts_ms);
                max_time = max_time.max(row.ts_ms);
                series.entry(row.fp).or_insert_with(|| row.labels.clone());
            }
        }
        if series.is_empty() {
            min_time = 0;
            max_time = 0;
        }

        let mut by_metric = BTreeMap::<String, usize>::new();
        let mut label_values_by_name = BTreeMap::<String, BTreeSet<String>>::new();
        let mut memory_by_name = BTreeMap::<String, usize>::new();
        let mut by_label_pair = BTreeMap::<String, usize>::new();
        for labels in series.values() {
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
                num_samples: sample_count,
                num_chunks: series.len(),
                min_time,
                max_time,
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
        let mut blocks = self.blocks.get(tenant).cloned().unwrap_or_default();
        blocks.sort_by(|left, right| {
            left.min_time
                .cmp(&right.min_time)
                .then_with(|| left.max_time.cmp(&right.max_time))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(blocks)
    }
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
