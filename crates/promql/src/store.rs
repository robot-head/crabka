//! Metric data access abstraction.

use crabka_blockstore::{LabelMatcher, Labels};
use datafusion::prelude::SessionContext;

use crate::PromqlError;

/// A leaf scan result with up to two `DataFusion` tables registered.
pub struct ScanResult {
    pub ctx: SessionContext,
    pub float_table: Option<String>,
    pub histogram_table: Option<String>,
}

/// One exemplar attached to a metric series.
#[derive(Clone, Debug, PartialEq)]
pub struct ExemplarRecord {
    pub series_labels: Labels,
    pub labels: Labels,
    pub ts_ms: i64,
    pub value: f64,
}

/// Metric metadata served by `/api/v1/metadata`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataRecord {
    pub metric_family_name: String,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
}

/// Cardinality for one label name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelNameCardinality {
    pub name: String,
    pub series_count: usize,
}

/// Cardinality for one label value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelValueCardinality {
    pub label_name: String,
    pub label_value: String,
    pub series_count: usize,
}

/// Prometheus-style head block stats for `/api/v1/status/tsdb`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TsdbHeadStats {
    pub num_series: usize,
    pub num_samples: usize,
    pub num_chunks: usize,
    pub min_time: i64,
    pub max_time: i64,
}

/// One named TSDB status statistic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedTsdbStat {
    pub name: String,
    pub value: usize,
}

/// Tenant-scoped TSDB status statistics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TsdbStats {
    pub head_stats: TsdbHeadStats,
    pub series_count_by_metric_name: Vec<NamedTsdbStat>,
    pub label_value_count_by_label_name: Vec<NamedTsdbStat>,
    pub memory_in_bytes_by_label_name: Vec<NamedTsdbStat>,
    pub series_count_by_label_value_pair: Vec<NamedTsdbStat>,
}

/// One compacted TSDB block exposed by `/api/v1/status/tsdb/blocks`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TsdbBlock {
    pub id: String,
    pub min_time: i64,
    pub max_time: i64,
    pub num_samples: usize,
    pub num_series: usize,
}

/// Resolves `PromQL` matchers to `DataFusion` tables over the metric data of a tenant.
#[async_trait::async_trait]
pub trait MetricStore: Send + Sync {
    /// Registers the float and histogram tables for matched series in `[start_ms, end_ms]`.
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult, PromqlError>;

    /// Returns the distinct label names across matched series.
    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, PromqlError>;

    /// Returns the distinct values of `name` across matched series.
    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, PromqlError>;

    /// Returns the label sets of matched series.
    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>, PromqlError>;

    /// Returns the exemplars attached to matched series in `[start_ms, end_ms]`.
    async fn exemplars(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ExemplarRecord>, PromqlError>;

    /// Returns the metric metadata for a tenant.
    ///
    /// The caller can restrict the result to one metric family.
    async fn metadata(
        &self,
        tenant: &str,
        metric: Option<&str>,
    ) -> Result<Vec<MetadataRecord>, PromqlError>;

    /// Returns the distinct active-series count for each label name in a tenant.
    async fn cardinality_label_names(
        &self,
        tenant: &str,
    ) -> Result<Vec<LabelNameCardinality>, PromqlError>;

    /// Returns the distinct active-series count for each label value in a tenant.
    async fn cardinality_label_values(
        &self,
        tenant: &str,
    ) -> Result<Vec<LabelValueCardinality>, PromqlError>;

    /// Returns the distinct label sets of the active series in a tenant.
    async fn cardinality_active_series(&self, tenant: &str) -> Result<Vec<Labels>, PromqlError>;

    /// Returns the tenant-scoped TSDB status statistics.
    async fn tsdb_stats(&self, tenant: &str) -> Result<TsdbStats, PromqlError>;

    /// Returns the tenant-scoped metadata of the compacted blocks.
    async fn tsdb_blocks(&self, tenant: &str) -> Result<Vec<TsdbBlock>, PromqlError>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crabka_blockstore::Labels;
    use datafusion::prelude::SessionContext;

    use super::*;

    struct Empty;

    #[async_trait::async_trait]
    impl MetricStore for Empty {
        async fn scan(
            &self,
            _tenant: &str,
            _matchers: &[crabka_blockstore::LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<ScanResult, PromqlError> {
            Ok(ScanResult {
                ctx: SessionContext::new(),
                float_table: None,
                histogram_table: None,
            })
        }

        async fn label_names(
            &self,
            _tenant: &str,
            _matchers: &[crabka_blockstore::LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<String>, PromqlError> {
            Ok(vec![])
        }

        async fn label_values(
            &self,
            _tenant: &str,
            _name: &str,
            _matchers: &[crabka_blockstore::LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<String>, PromqlError> {
            Ok(vec![])
        }

        async fn series(
            &self,
            _tenant: &str,
            _matchers: &[crabka_blockstore::LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<Labels>, PromqlError> {
            Ok(vec![])
        }

        async fn exemplars(
            &self,
            _tenant: &str,
            _matchers: &[crabka_blockstore::LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<ExemplarRecord>, PromqlError> {
            Ok(vec![])
        }

        async fn metadata(
            &self,
            _tenant: &str,
            _metric: Option<&str>,
        ) -> Result<Vec<MetadataRecord>, PromqlError> {
            Ok(vec![])
        }

        async fn cardinality_label_names(
            &self,
            _tenant: &str,
        ) -> Result<Vec<LabelNameCardinality>, PromqlError> {
            Ok(vec![])
        }

        async fn cardinality_label_values(
            &self,
            _tenant: &str,
        ) -> Result<Vec<LabelValueCardinality>, PromqlError> {
            Ok(vec![])
        }

        async fn cardinality_active_series(
            &self,
            _tenant: &str,
        ) -> Result<Vec<Labels>, PromqlError> {
            Ok(vec![])
        }

        async fn tsdb_stats(&self, _tenant: &str) -> Result<TsdbStats, PromqlError> {
            Ok(TsdbStats {
                head_stats: TsdbHeadStats {
                    num_series: 0,
                    num_samples: 0,
                    num_chunks: 0,
                    min_time: 0,
                    max_time: 0,
                },
                series_count_by_metric_name: Vec::new(),
                label_value_count_by_label_name: Vec::new(),
                memory_in_bytes_by_label_name: Vec::new(),
                series_count_by_label_value_pair: Vec::new(),
            })
        }

        async fn tsdb_blocks(&self, _tenant: &str) -> Result<Vec<TsdbBlock>, PromqlError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn trait_is_object_safe_and_default_returns_none_tables() {
        let store: Arc<dyn MetricStore> = Arc::new(Empty);
        let result = store.scan("t", &[], 0, 1).await.unwrap();
        assert2::assert!(result.float_table.is_none());
        assert2::assert!(result.histogram_table.is_none());
    }
}
