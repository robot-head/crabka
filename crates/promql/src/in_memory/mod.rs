//! In-memory `MetricStore` used by conformance and engine tests.

use std::collections::{BTreeMap, HashMap};

use crabka_blockstore::{LabelMatcher, Labels, SeriesFingerprint};
use crabka_metrics::NativeHistogram;
use crabka_units::prelude::*;

use crate::{
    error::Result,
    ids::{Offset, PartitionIndex},
    store::{MetadataRecord, TsdbBlock},
};

mod head;
mod ingest;
mod matcher;
mod store_impl;

pub use head::WalHead;
use matcher::{prepare_matchers, row_matches};

#[derive(Clone)]
struct FloatRow {
    fp: SeriesFingerprint,
    labels: Labels,
    ts_ms: i64,
    value: f64,
}

#[derive(Clone)]
struct HistRow {
    fp: SeriesFingerprint,
    labels: Labels,
    ts_ms: i64,
    hist: NativeHistogram,
}

#[derive(Clone)]
struct ExemplarRow {
    series_labels: Labels,
    labels: Labels,
    ts_ms: i64,
    value: f64,
}

/// Default head retention window: six hours of samples are kept hot.
pub const DEFAULT_RETENTION: Time = hours(6);

/// Counts of what a [`InMemoryMetricStore::prune`] pass evicted.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PruneStats {
    /// Float, histogram, and exemplar rows dropped because they fell out of the
    /// retention window.
    pub samples_dropped: usize,
    /// Series (distinct fingerprints) that lost their last sample and were
    /// therefore removed from the queryable index.
    pub series_dropped: usize,
}

/// WAL offset watermarks materialized in the head for one partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionWatermark {
    /// First WAL offset that was ingested into the head for this partition.
    pub low_water_offset: Offset,
    /// Most recent WAL offset that was ingested into the head for this partition.
    pub high_water_offset: Offset,
}

/// In-memory metric store keyed by tenant.
#[derive(Clone)]
pub struct InMemoryMetricStore {
    floats: HashMap<String, Vec<FloatRow>>,
    hists: HashMap<String, Vec<HistRow>>,
    exemplars: HashMap<String, Vec<ExemplarRow>>,
    metadata: HashMap<String, Vec<MetadataRecord>>,
    blocks: HashMap<String, Vec<TsdbBlock>>,
    /// Samples whose timestamp is older than `now_ms - retention` are eligible
    /// for [`InMemoryMetricStore::prune`].
    retention: Time,
    /// WAL offset range currently materialized in the head, keyed by partition.
    /// Offsets track ingestion progress for observability and rebuild bounds;
    /// they are independent of timestamp-based retention.
    watermarks: BTreeMap<PartitionIndex, PartitionWatermark>,
}

impl Default for InMemoryMetricStore {
    fn default() -> Self {
        Self {
            floats: HashMap::new(),
            hists: HashMap::new(),
            exemplars: HashMap::new(),
            metadata: HashMap::new(),
            blocks: HashMap::new(),
            retention: DEFAULT_RETENTION,
            watermarks: BTreeMap::new(),
        }
    }
}

impl InMemoryMetricStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a store with an explicit retention window.
    #[must_use]
    pub fn with_retention(retention: Time) -> Self {
        Self {
            retention,
            ..Self::default()
        }
    }

    /// The retention window.
    #[must_use]
    pub fn retention(&self) -> Time {
        self.retention
    }

    /// Set the retention window.
    pub fn set_retention(&mut self, retention: Time) {
        self.retention = retention;
    }

    /// Distinct label sets matching the matchers within the time window.
    fn matched_series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>> {
        let matchers = prepare_matchers(matchers)?;
        let mut by_fp: BTreeMap<SeriesFingerprint, Labels> = BTreeMap::new();
        if let Some(rows) = self.floats.get(tenant) {
            for row in rows {
                if row_matches(row.fp, &row.labels, row.ts_ms, &matchers, start_ms, end_ms) {
                    by_fp.entry(row.fp).or_insert_with(|| row.labels.clone());
                }
            }
        }
        if let Some(rows) = self.hists.get(tenant) {
            for row in rows {
                if row_matches(row.fp, &row.labels, row.ts_ms, &matchers, start_ms, end_ms) {
                    by_fp.entry(row.fp).or_insert_with(|| row.labels.clone());
                }
            }
        }
        Ok(by_fp.into_values().collect())
    }
}

#[cfg(test)]
mod tests;
