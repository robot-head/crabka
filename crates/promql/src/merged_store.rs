//! Merges two metric stores, usually compacted cold blocks and a hot WAL head.

use std::collections::{BTreeMap, BTreeSet};

use crabka_blockstore::{LabelMatcher, Labels, SeriesFingerprint};
use crabka_metrics::{float_sample_schema, native_histogram_schema};
use datafusion::prelude::SessionContext;

use self::{
    scan::{FLOAT_TABLE, HISTOGRAM_TABLE, merge_scan_table},
    stats::{label_name_cardinality, label_value_cardinality, merge_named_stats, min_present_time},
};
use crate::{
    ExemplarRecord, LabelNameCardinality, LabelValueCardinality, MetadataRecord, MetricStore,
    PromqlError, ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
};

mod scan;
mod stats;

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

#[cfg(test)]
mod tests;
