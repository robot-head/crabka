//! In-memory `MetricStore` used by conformance and engine tests.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::RwLock;

use crabka_blockstore::{
    LabelMatcher, Labels, MatchOp, QUERY_SHARD_LABEL, SeriesFingerprint, parse_query_shard_selector,
};
use crabka_metrics::{
    NativeHistogram, SamplePayload, WalRecord, encode_float_samples, encode_native_histograms,
    float_sample_schema, native_histogram_schema,
};
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;

use crate::PromqlError;
use crate::error::Result;
use crate::store::{
    ExemplarRecord, LabelNameCardinality, LabelValueCardinality, MetadataRecord, MetricStore,
    NamedTsdbStat, ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
};

/// Shared hot-head metric store rebuilt from the metrics WAL tail.
#[derive(Clone, Default)]
pub struct WalHead {
    inner: Arc<RwLock<InMemoryMetricStore>>,
}

impl WalHead {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a head with an explicit retention window in milliseconds.
    #[must_use]
    pub fn with_retention_ms(retention_ms: i64) -> Self {
        Self::from_store(InMemoryMetricStore::with_retention_ms(retention_ms))
    }

    #[must_use]
    pub fn from_store(store: InMemoryMetricStore) -> Self {
        Self {
            inner: Arc::new(RwLock::new(store)),
        }
    }

    /// Apply one decoded metrics WAL record into the shared hot head.
    pub fn apply_wal_record(&self, record: &WalRecord) {
        self.inner
            .write()
            .expect("wal head lock poisoned")
            .apply_wal_record(record);
    }

    /// Apply one decoded metrics WAL record and advance the offset watermarks
    /// for `partition` to include `offset`.
    pub fn apply_wal_record_at(&self, record: &WalRecord, partition: i32, offset: i64) {
        let mut inner = self.inner.write().expect("wal head lock poisoned");
        inner.apply_wal_record(record);
        inner.record_offset(partition, offset);
    }

    /// Apply decoded metrics WAL records in log order.
    pub fn apply_wal_records<'a>(&self, records: impl IntoIterator<Item = &'a WalRecord>) {
        let mut inner = self.inner.write().expect("wal head lock poisoned");
        inner.apply_wal_records(records);
    }

    /// Drop samples older than the retention window from the shared hot head.
    ///
    /// Returns how many samples and series were evicted. Offset watermarks are
    /// left untouched. The returned stats are advisory (metrics/tests); pruning
    /// for the side effect of bounding memory and discarding them is valid.
    #[allow(clippy::must_use_candidate)]
    pub fn prune(&self, now_ms: i64) -> PruneStats {
        self.inner
            .write()
            .expect("wal head lock poisoned")
            .prune(now_ms)
    }

    /// The lowest WAL offset materialized in the head for `partition`.
    #[must_use]
    pub fn low_water_offset(&self, partition: i32) -> Option<i64> {
        self.inner
            .read()
            .expect("wal head lock poisoned")
            .low_water_offset(partition)
    }

    /// The highest WAL offset materialized in the head for `partition`.
    #[must_use]
    pub fn high_water_offset(&self, partition: i32) -> Option<i64> {
        self.inner
            .read()
            .expect("wal head lock poisoned")
            .high_water_offset(partition)
    }

    /// Snapshot of all per-partition WAL offset watermarks.
    #[must_use]
    pub fn watermarks(&self) -> BTreeMap<i32, PartitionWatermark> {
        self.inner
            .read()
            .expect("wal head lock poisoned")
            .watermarks()
            .clone()
    }

    /// The retention window in milliseconds.
    #[must_use]
    pub fn retention_ms(&self) -> i64 {
        self.inner
            .read()
            .expect("wal head lock poisoned")
            .retention_ms()
    }
}

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
pub const DEFAULT_RETENTION_MS: i64 = 6 * 60 * 60 * 1_000;

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
    pub low_water_offset: i64,
    /// Most recent WAL offset that was ingested into the head for this partition.
    pub high_water_offset: i64,
}

/// In-memory metric store keyed by tenant.
#[derive(Clone)]
pub struct InMemoryMetricStore {
    floats: HashMap<String, Vec<FloatRow>>,
    hists: HashMap<String, Vec<HistRow>>,
    exemplars: HashMap<String, Vec<ExemplarRow>>,
    metadata: HashMap<String, Vec<MetadataRecord>>,
    blocks: HashMap<String, Vec<TsdbBlock>>,
    /// Samples whose timestamp is older than `now_ms - retention_ms` are
    /// eligible for [`InMemoryMetricStore::prune`].
    retention_ms: i64,
    /// WAL offset range currently materialized in the head, keyed by partition.
    /// Offsets track ingestion progress for observability and rebuild bounds;
    /// they are independent of timestamp-based retention.
    watermarks: BTreeMap<i32, PartitionWatermark>,
}

impl Default for InMemoryMetricStore {
    fn default() -> Self {
        Self {
            floats: HashMap::new(),
            hists: HashMap::new(),
            exemplars: HashMap::new(),
            metadata: HashMap::new(),
            blocks: HashMap::new(),
            retention_ms: DEFAULT_RETENTION_MS,
            watermarks: BTreeMap::new(),
        }
    }
}

impl InMemoryMetricStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a store with an explicit retention window in milliseconds.
    #[must_use]
    pub fn with_retention_ms(retention_ms: i64) -> Self {
        Self {
            retention_ms,
            ..Self::default()
        }
    }

    /// The retention window in milliseconds.
    #[must_use]
    pub fn retention_ms(&self) -> i64 {
        self.retention_ms
    }

    /// Set the retention window in milliseconds.
    pub fn set_retention_ms(&mut self, retention_ms: i64) {
        self.retention_ms = retention_ms;
    }

    pub fn push_float(&mut self, tenant: &str, labels: Labels, ts_ms: i64, value: f64) {
        let fp = labels.fingerprint();
        self.floats
            .entry(tenant.to_string())
            .or_default()
            .push(FloatRow {
                fp,
                labels,
                ts_ms,
                value,
            });
    }

    pub fn push_histogram(
        &mut self,
        tenant: &str,
        labels: Labels,
        ts_ms: i64,
        hist: NativeHistogram,
    ) {
        let fp = labels.fingerprint();
        self.hists
            .entry(tenant.to_string())
            .or_default()
            .push(HistRow {
                fp,
                labels,
                ts_ms,
                hist,
            });
    }

    pub fn push_exemplar(
        &mut self,
        tenant: &str,
        series_labels: Labels,
        labels: Labels,
        ts_ms: i64,
        value: f64,
    ) {
        self.exemplars
            .entry(tenant.to_string())
            .or_default()
            .push(ExemplarRow {
                series_labels,
                labels,
                ts_ms,
                value,
            });
    }

    pub fn push_metadata(
        &mut self,
        tenant: &str,
        metric_family_name: &str,
        metric_type: &str,
        help: &str,
        unit: &str,
    ) {
        self.metadata
            .entry(tenant.to_string())
            .or_default()
            .push(MetadataRecord {
                metric_family_name: metric_family_name.to_string(),
                metric_type: metric_type.to_string(),
                help: help.to_string(),
                unit: unit.to_string(),
            });
    }

    pub fn push_tsdb_block(
        &mut self,
        tenant: &str,
        id: &str,
        min_time: i64,
        max_time: i64,
        num_samples: usize,
        num_series: usize,
    ) {
        self.blocks
            .entry(tenant.to_string())
            .or_default()
            .push(TsdbBlock {
                id: id.to_string(),
                min_time,
                max_time,
                num_samples,
                num_series,
            });
    }

    /// Apply one decoded metrics WAL record into this in-memory head.
    pub fn apply_wal_record(&mut self, record: &WalRecord) {
        let series_labels = record.labels();
        match &record.payload {
            SamplePayload::Float {
                timestamp_ms,
                value,
                ..
            } => self.push_float(&record.tenant, series_labels.clone(), *timestamp_ms, *value),
            SamplePayload::Hist { timestamp_ms, hist } => {
                self.push_histogram(
                    &record.tenant,
                    series_labels.clone(),
                    *timestamp_ms,
                    hist.clone(),
                );
            }
            SamplePayload::Metadata {
                metric_family_name,
                metric_type,
                help,
                unit,
            } => self.push_metadata(&record.tenant, metric_family_name, metric_type, help, unit),
            SamplePayload::Exemplars => {}
        }
        for exemplar in &record.exemplars {
            self.push_exemplar(
                &record.tenant,
                series_labels.clone(),
                exemplar.labels.iter().cloned().collect(),
                exemplar.timestamp_ms,
                exemplar.value,
            );
        }
    }

    /// Apply decoded metrics WAL records in log order.
    pub fn apply_wal_records<'a>(&mut self, records: impl IntoIterator<Item = &'a WalRecord>) {
        for record in records {
            self.apply_wal_record(record);
        }
    }

    /// Record that `offset` for `partition` has been materialized in the head,
    /// advancing the high-water (and seeding the low-water on first sight).
    ///
    /// Offsets track ingestion progress for observability and rebuild bounds;
    /// they are never moved by [`InMemoryMetricStore::prune`].
    pub fn record_offset(&mut self, partition: i32, offset: i64) {
        self.watermarks
            .entry(partition)
            .and_modify(|watermark| {
                watermark.low_water_offset = watermark.low_water_offset.min(offset);
                watermark.high_water_offset = watermark.high_water_offset.max(offset);
            })
            .or_insert(PartitionWatermark {
                low_water_offset: offset,
                high_water_offset: offset,
            });
    }

    /// The lowest WAL offset materialized in the head for `partition`.
    #[must_use]
    pub fn low_water_offset(&self, partition: i32) -> Option<i64> {
        self.watermarks
            .get(&partition)
            .map(|watermark| watermark.low_water_offset)
    }

    /// The highest WAL offset materialized in the head for `partition`.
    #[must_use]
    pub fn high_water_offset(&self, partition: i32) -> Option<i64> {
        self.watermarks
            .get(&partition)
            .map(|watermark| watermark.high_water_offset)
    }

    /// All per-partition WAL offset watermarks materialized in the head.
    #[must_use]
    pub fn watermarks(&self) -> &BTreeMap<i32, PartitionWatermark> {
        &self.watermarks
    }

    /// Drop every sample older than `now_ms - retention_ms` from each series,
    /// removing series that become empty from the queryable index.
    ///
    /// Returns how many samples and series were evicted. Offset watermarks are
    /// left untouched: they track ingestion progress, not retention.
    pub fn prune(&mut self, now_ms: i64) -> PruneStats {
        let cutoff = now_ms.saturating_sub(self.retention_ms);
        let mut stats = PruneStats::default();

        // Fingerprints with at least one surviving sample after pruning.
        let mut live: BTreeSet<SeriesFingerprint> = BTreeSet::new();
        // Fingerprints that had a sample before pruning.
        let mut seen: BTreeSet<SeriesFingerprint> = BTreeSet::new();

        for rows in self.floats.values_mut() {
            for row in rows.iter() {
                seen.insert(row.fp);
            }
            let before = rows.len();
            rows.retain(|row| row.ts_ms >= cutoff);
            stats.samples_dropped += before - rows.len();
            for row in rows.iter() {
                live.insert(row.fp);
            }
        }
        for rows in self.hists.values_mut() {
            for row in rows.iter() {
                seen.insert(row.fp);
            }
            let before = rows.len();
            rows.retain(|row| row.ts_ms >= cutoff);
            stats.samples_dropped += before - rows.len();
            for row in rows.iter() {
                live.insert(row.fp);
            }
        }
        // Exemplars are not part of the series index, but they are samples that
        // must obey retention so the head stays bounded.
        for rows in self.exemplars.values_mut() {
            let before = rows.len();
            rows.retain(|row| row.ts_ms >= cutoff);
            stats.samples_dropped += before - rows.len();
        }

        // Drop the now-empty per-tenant vectors so iteration stays cheap and the
        // tenant disappears from the index once it has no live series.
        self.floats.retain(|_, rows| !rows.is_empty());
        self.hists.retain(|_, rows| !rows.is_empty());
        self.exemplars.retain(|_, rows| !rows.is_empty());

        stats.series_dropped = seen.difference(&live).count();
        stats
    }

    /// Distinct label sets matching the matchers within the time window.
    fn matched_series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>> {
        let mut by_fp: BTreeMap<SeriesFingerprint, Labels> = BTreeMap::new();
        if let Some(rows) = self.floats.get(tenant) {
            for row in rows {
                if row.ts_ms >= start_ms
                    && row.ts_ms <= end_ms
                    && all_match(row.fp, &row.labels, matchers)?
                {
                    by_fp.entry(row.fp).or_insert_with(|| row.labels.clone());
                }
            }
        }
        if let Some(rows) = self.hists.get(tenant) {
            for row in rows {
                if row.ts_ms >= start_ms
                    && row.ts_ms <= end_ms
                    && all_match(row.fp, &row.labels, matchers)?
                {
                    by_fp.entry(row.fp).or_insert_with(|| row.labels.clone());
                }
            }
        }
        Ok(by_fp.into_values().collect())
    }
}

#[async_trait::async_trait]
impl MetricStore for WalHead {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult> {
        let store = self.inner.read().expect("wal head lock poisoned").clone();
        store.scan(tenant, matchers, start_ms, end_ms).await
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>> {
        let store = self.inner.read().expect("wal head lock poisoned").clone();
        store.label_names(tenant, matchers, start_ms, end_ms).await
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>> {
        let store = self.inner.read().expect("wal head lock poisoned").clone();
        store
            .label_values(tenant, name, matchers, start_ms, end_ms)
            .await
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>> {
        let store = self.inner.read().expect("wal head lock poisoned").clone();
        store.series(tenant, matchers, start_ms, end_ms).await
    }

    async fn exemplars(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ExemplarRecord>> {
        let store = self.inner.read().expect("wal head lock poisoned").clone();
        store.exemplars(tenant, matchers, start_ms, end_ms).await
    }

    async fn metadata(&self, tenant: &str, metric: Option<&str>) -> Result<Vec<MetadataRecord>> {
        let store = self.inner.read().expect("wal head lock poisoned").clone();
        store.metadata(tenant, metric).await
    }

    async fn cardinality_label_names(&self, tenant: &str) -> Result<Vec<LabelNameCardinality>> {
        let store = self.inner.read().expect("wal head lock poisoned").clone();
        store.cardinality_label_names(tenant).await
    }

    async fn cardinality_label_values(&self, tenant: &str) -> Result<Vec<LabelValueCardinality>> {
        let store = self.inner.read().expect("wal head lock poisoned").clone();
        store.cardinality_label_values(tenant).await
    }

    async fn cardinality_active_series(&self, tenant: &str) -> Result<Vec<Labels>> {
        let store = self.inner.read().expect("wal head lock poisoned").clone();
        store.cardinality_active_series(tenant).await
    }

    async fn tsdb_stats(&self, tenant: &str) -> Result<TsdbStats> {
        let store = self.inner.read().expect("wal head lock poisoned").clone();
        store.tsdb_stats(tenant).await
    }

    async fn tsdb_blocks(&self, tenant: &str) -> Result<Vec<TsdbBlock>> {
        let store = self.inner.read().expect("wal head lock poisoned").clone();
        store.tsdb_blocks(tenant).await
    }
}

fn matcher_matches(fp: SeriesFingerprint, labels: &Labels, matcher: &LabelMatcher) -> Result<bool> {
    if matcher.name == QUERY_SHARD_LABEL {
        let selector = parse_query_shard_selector(&matcher.value)
            .map_err(|error| PromqlError::Plan(format!("invalid query shard matcher: {error}")))?;
        return match matcher.op {
            MatchOp::Eq => Ok(selector.matches(fp)),
            MatchOp::Neq => Ok(!selector.matches(fp)),
            MatchOp::Re | MatchOp::Nre => Err(PromqlError::Plan(
                "query shard matcher must use equality or inequality".into(),
            )),
        };
    }

    let actual = labels.get(&matcher.name).unwrap_or("");
    Ok(match matcher.op {
        MatchOp::Eq => actual == matcher.value,
        MatchOp::Neq => actual != matcher.value,
        MatchOp::Re | MatchOp::Nre => {
            let regex = regex_anchored(&matcher.value)?;
            let regex_hit = regex.is_match(actual);
            if matcher.op == MatchOp::Re {
                regex_hit
            } else {
                !regex_hit
            }
        }
    })
}

fn regex_anchored(pattern: &str) -> Result<regex::Regex> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
        .map_err(|error| PromqlError::Plan(format!("bad regex `{pattern}`: {error}")))
}

fn all_match(fp: SeriesFingerprint, labels: &Labels, matchers: &[LabelMatcher]) -> Result<bool> {
    for matcher in matchers {
        if !matcher_matches(fp, labels, matcher)? {
            return Ok(false);
        }
    }
    Ok(true)
}

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

        let mut float_rows = Vec::new();
        if let Some(rows) = self.floats.get(tenant) {
            for row in rows {
                if row.ts_ms >= start_ms
                    && row.ts_ms <= end_ms
                    && all_match(row.fp, &row.labels, matchers)?
                {
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
                if row.ts_ms >= start_ms
                    && row.ts_ms <= end_ms
                    && all_match(row.fp, &row.labels, matchers)?
                {
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
        let mut exemplars = Vec::new();
        if let Some(rows) = self.exemplars.get(tenant) {
            for row in rows {
                if row.ts_ms >= start_ms
                    && row.ts_ms <= end_ms
                    && all_match(
                        row.series_labels.fingerprint(),
                        &row.series_labels,
                        matchers,
                    )?
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

#[cfg(test)]
mod tests {
    use arrow::array::AsArray;
    use arrow::datatypes::Int64Type;
    use assert2::assert;
    use crabka_blockstore::{LabelMatcher, Labels, MatchOp};
    use crabka_metrics::{
        BucketSpan, NativeHistogram, ResetHint, SamplePayload, WalExemplar, WalRecord,
    };

    use super::*;
    use crate::{EngineOpts, PromqlEngine, QueryResult, SampleValue, WalHead};

    fn lbls(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (key, value) in pairs {
            labels.insert(*key, *value);
        }
        labels
    }

    fn native_histogram() -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            is_float: false,
            reset_hint: ResetHint::No,
            zero_threshold: 1e-128,
            zero_count: 0.0,
            count: 2.0,
            sum: 3.0,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 1,
            }],
            positive_counts: vec![2.0],
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        }
    }

    #[tokio::test]
    async fn replay_wal_records_populates_queryable_head() {
        let mut store = InMemoryMetricStore::new();
        let series_labels = vec![
            ("__name__".to_string(), "up".to_string()),
            ("job".to_string(), "api".to_string()),
        ];
        store.apply_wal_record(&WalRecord {
            tenant: "tenant-a".to_string(),
            labels: series_labels.clone(),
            payload: SamplePayload::Float {
                timestamp_ms: 10_000,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        });
        store.apply_wal_record(&WalRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![
                (
                    "__name__".to_string(),
                    "request_duration_seconds".to_string(),
                ),
                ("job".to_string(), "api".to_string()),
            ],
            payload: SamplePayload::Hist {
                timestamp_ms: 10_000,
                hist: native_histogram(),
            },
            exemplars: Vec::new(),
        });
        store.apply_wal_record(&WalRecord {
            tenant: "tenant-a".to_string(),
            labels: series_labels.clone(),
            payload: SamplePayload::Metadata {
                metric_family_name: "up".to_string(),
                metric_type: "gauge".to_string(),
                help: "Target health.".to_string(),
                unit: String::new(),
            },
            exemplars: Vec::new(),
        });
        store.apply_wal_record(&WalRecord {
            tenant: "tenant-a".to_string(),
            labels: series_labels.clone(),
            payload: SamplePayload::Exemplars,
            exemplars: vec![WalExemplar {
                labels: vec![("trace_id".to_string(), "abc".to_string())],
                value: 1.0,
                timestamp_ms: 10_000,
            }],
        });

        let engine = PromqlEngine::new(std::sync::Arc::new(store.clone()), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "up", 10_000)
            .await
            .expect("query");
        let QueryResult::InstantVector(vector) = result else {
            panic!("expected vector");
        };
        assert!(vector[0].value == SampleValue::Float(1.0));
        assert!(store.metadata("tenant-a", Some("up")).await.unwrap()[0].help == "Target health.");
        assert!(
            store.exemplars("tenant-a", &[], 0, 10_000).await.unwrap()[0].labels
                == lbls(&[("trace_id", "abc")])
        );
        assert!(
            store
                .scan("tenant-a", &[], 0, 10_000)
                .await
                .unwrap()
                .histogram_table
                .is_some()
        );
    }

    #[tokio::test]
    async fn cloned_wal_head_sees_records_replayed_through_original_handle() {
        let head = WalHead::new();
        let query_handle = head.clone();
        head.apply_wal_record(&WalRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![
                ("__name__".to_string(), "up".to_string()),
                ("job".to_string(), "api".to_string()),
            ],
            payload: SamplePayload::Float {
                timestamp_ms: 10_000,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        });

        let engine = PromqlEngine::new(std::sync::Arc::new(query_handle), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "up", 10_000)
            .await
            .expect("query");
        let QueryResult::InstantVector(vector) = result else {
            panic!("expected vector");
        };

        assert!(vector[0].value == SampleValue::Float(1.0));
    }

    #[tokio::test]
    async fn scan_filters_by_matcher_and_time_and_registers_float_table() {
        let mut store = InMemoryMetricStore::new();
        store.push_float("t", lbls(&[("__name__", "up"), ("job", "a")]), 1000, 1.0);
        store.push_float("t", lbls(&[("__name__", "up"), ("job", "a")]), 2000, 1.0);
        store.push_float("t", lbls(&[("__name__", "up"), ("job", "b")]), 1000, 0.0);
        store.push_float("t", lbls(&[("__name__", "down")]), 1000, 9.0);

        let matchers = [
            LabelMatcher::new("__name__", MatchOp::Eq, "up"),
            LabelMatcher::new("job", MatchOp::Eq, "a"),
        ];
        let result = store.scan("t", &matchers, 0, 1500).await.unwrap();
        let table = result.float_table.clone().unwrap();
        assert!(result.histogram_table.is_none());

        let df = result
            .ctx
            .sql(&format!("SELECT count(*) AS c FROM {table}"))
            .await
            .unwrap();
        let output = df.collect().await.unwrap();
        let count = output[0].column(0).as_primitive::<Int64Type>().value(0);
        assert!(count == 1);
    }

    #[tokio::test]
    async fn scan_with_no_match_returns_none_tables() {
        let mut store = InMemoryMetricStore::new();
        store.push_float("t", lbls(&[("__name__", "up")]), 1000, 1.0);
        let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "absent")];
        let result = store.scan("t", &matchers, 0, 5000).await.unwrap();
        assert!(result.float_table.is_none());
        assert!(result.histogram_table.is_none());
    }

    #[tokio::test]
    async fn label_values_returns_distinct_for_name() {
        let mut store = InMemoryMetricStore::new();
        store.push_float("t", lbls(&[("__name__", "up"), ("job", "a")]), 1, 1.0);
        store.push_float("t", lbls(&[("__name__", "up"), ("job", "b")]), 1, 1.0);
        let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
        let values = store
            .label_values("t", "job", &matchers, 0, 10)
            .await
            .unwrap();
        assert!(values == vec!["a".to_string(), "b".to_string()]);
    }

    #[tokio::test]
    async fn regex_matchers_are_anchored_and_absent_labels_match_empty() {
        let mut store = InMemoryMetricStore::new();
        store.push_float("t", lbls(&[("__name__", "up"), ("job", "api")]), 1, 1.0);
        store.push_float("t", lbls(&[("__name__", "up")]), 1, 2.0);

        let anchored = [LabelMatcher::new("job", MatchOp::Re, "a")];
        assert!(
            store
                .scan("t", &anchored, 0, 10)
                .await
                .unwrap()
                .float_table
                .is_none()
        );

        let empty = [LabelMatcher::new("missing", MatchOp::Eq, "")];
        assert!(
            store
                .scan("t", &empty, 0, 10)
                .await
                .unwrap()
                .float_table
                .is_some()
        );
    }

    #[tokio::test]
    async fn query_shard_matcher_filters_by_series_fingerprint_modulo() {
        let mut store = InMemoryMetricStore::new();
        let series = (0..12)
            .map(|id| lbls(&[("__name__", "up"), ("series", &id.to_string())]))
            .collect::<Vec<_>>();
        for labels in &series {
            store.push_float("t", labels.clone(), 1, 1.0);
        }

        let expected = series
            .iter()
            .filter(|labels| labels.fingerprint() % 2 == 0)
            .map(|labels| (labels.fingerprint(), labels.clone()))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect::<Vec<_>>();
        assert!(!expected.is_empty());
        assert!(expected.len() < series.len());

        let matchers = [
            LabelMatcher::new("__name__", MatchOp::Eq, "up"),
            LabelMatcher::new("__query_shard__", MatchOp::Eq, "1_of_2"),
        ];
        let got = store.series("t", &matchers, 0, 10).await.unwrap();

        assert!(got == expected);
    }

    #[tokio::test]
    async fn prune_drops_old_samples() {
        let mut store = InMemoryMetricStore::with_retention_ms(1_000);
        let series = lbls(&[("__name__", "up"), ("job", "api")]);
        // ts 100 and 500 are old; ts 9_500 and 9_900 are within the window.
        store.push_float("t", series.clone(), 100, 1.0);
        store.push_float("t", series.clone(), 500, 2.0);
        store.push_float("t", series.clone(), 9_500, 3.0);
        store.push_float("t", series.clone(), 9_900, 4.0);
        // A histogram sample that is also old.
        store.push_histogram("t", series.clone(), 200, native_histogram());

        // now = 10_000, retention = 1_000 -> cutoff = 9_000; ts < 9_000 dropped.
        let stats = store.prune(10_000);
        assert!(stats.samples_dropped == 3);
        assert!(stats.series_dropped == 0);

        let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
        let remaining = store
            .scan("t", &matchers, i64::MIN, i64::MAX)
            .await
            .unwrap();
        let table = remaining.float_table.clone().unwrap();
        let df = remaining
            .ctx
            .sql(&format!("SELECT count(*) AS c FROM {table}"))
            .await
            .unwrap();
        let output = df.collect().await.unwrap();
        let count = output[0].column(0).as_primitive::<Int64Type>().value(0);
        assert!(count == 2);
        // The old histogram sample is gone.
        assert!(remaining.histogram_table.is_none());
    }

    #[tokio::test]
    async fn prune_removes_emptied_series_from_index() {
        let mut store = InMemoryMetricStore::with_retention_ms(1_000);
        let stale = lbls(&[("__name__", "up"), ("job", "old")]);
        let fresh = lbls(&[("__name__", "up"), ("job", "new")]);
        store.push_float("t", stale.clone(), 100, 1.0);
        store.push_float("t", fresh.clone(), 9_900, 2.0);

        let stats = store.prune(10_000);
        assert!(stats.samples_dropped == 1);
        assert!(stats.series_dropped == 1);

        let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
        let series = store
            .series("t", &matchers, i64::MIN, i64::MAX)
            .await
            .unwrap();
        assert!(series == vec![fresh.clone()]);

        // The emptied series' label value no longer appears on the label surface.
        let jobs = store
            .label_values("t", "job", &matchers, i64::MIN, i64::MAX)
            .await
            .unwrap();
        assert!(jobs == vec!["new".to_string()]);
    }

    #[test]
    fn offsets_track_low_and_high_water() {
        let head = WalHead::new();
        let record = |ts: i64| WalRecord {
            tenant: "t".to_string(),
            labels: vec![("__name__".to_string(), "up".to_string())],
            payload: SamplePayload::Float {
                timestamp_ms: ts,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        };

        // No offsets ingested yet.
        assert!(head.high_water_offset(0).is_none());
        assert!(head.low_water_offset(0).is_none());

        head.apply_wal_record_at(&record(10), 0, 5);
        head.apply_wal_record_at(&record(20), 0, 6);
        head.apply_wal_record_at(&record(30), 1, 100);

        // High water is the latest applied offset per partition.
        assert!(head.high_water_offset(0) == Some(6));
        assert!(head.high_water_offset(1) == Some(100));
        // Low water is the first applied offset per partition.
        assert!(head.low_water_offset(0) == Some(5));
        assert!(head.low_water_offset(1) == Some(100));
        // Untracked partition stays empty.
        assert!(head.high_water_offset(2).is_none());

        // Pruning does not move offsets (they track ingestion, not retention).
        head.prune(i64::MAX);
        assert!(head.high_water_offset(0) == Some(6));
        assert!(head.low_water_offset(0) == Some(5));
    }
}
