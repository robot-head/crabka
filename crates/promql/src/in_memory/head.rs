use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use crabka_blockstore::{LabelMatcher, Labels};
use crabka_metrics::WalRecord;

use super::{InMemoryMetricStore, PartitionWatermark, PruneStats};
use crate::{
    error::Result,
    ids::{Offset, PartitionIndex},
    store::{
        ExemplarRecord, LabelNameCardinality, LabelValueCardinality, MetadataRecord, MetricStore,
        ScanResult, TsdbBlock, TsdbStats,
    },
};

/// Shared hot-head metric store rebuilt from the metrics WAL tail.
///
/// Reads clone the inner `Arc` pointer (O(1)); writers use `Arc::make_mut`
/// which clones the store only if a reader is concurrently holding a snapshot.
/// This avoids the O(N) full-store clone that the previous design incurred on
/// every single query.
#[derive(Clone, Default)]
pub struct WalHead {
    inner: Arc<RwLock<Arc<InMemoryMetricStore>>>,
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
            inner: Arc::new(RwLock::new(Arc::new(store))),
        }
    }

    /// Apply one decoded metrics WAL record into the shared hot head.
    pub fn apply_wal_record(&self, record: &WalRecord) {
        let mut guard = self.inner.write().expect("wal head lock poisoned");
        Arc::make_mut(&mut *guard).apply_wal_record(record);
    }

    /// Apply one decoded metrics WAL record and advance the offset watermarks
    /// for `partition` to include `offset`.
    pub fn apply_wal_record_at(
        &self,
        record: &WalRecord,
        partition: PartitionIndex,
        offset: Offset,
    ) {
        let mut guard = self.inner.write().expect("wal head lock poisoned");
        let store = Arc::make_mut(&mut *guard);
        store.apply_wal_record(record);
        store.record_offset(partition, offset);
    }

    /// Apply decoded metrics WAL records in log order.
    pub fn apply_wal_records<'a>(&self, records: impl IntoIterator<Item = &'a WalRecord>) {
        let mut guard = self.inner.write().expect("wal head lock poisoned");
        Arc::make_mut(&mut *guard).apply_wal_records(records);
    }

    /// Drop samples older than the retention window from the shared hot head.
    ///
    /// Returns how many samples and series were evicted. Offset watermarks are
    /// left untouched. The returned stats are advisory (metrics/tests); pruning
    /// for the side effect of bounding memory and discarding them is valid.
    #[allow(clippy::must_use_candidate)]
    pub fn prune(&self, now_ms: i64) -> PruneStats {
        let mut guard = self.inner.write().expect("wal head lock poisoned");
        Arc::make_mut(&mut *guard).prune(now_ms)
    }

    /// The lowest WAL offset materialized in the head for `partition`.
    #[must_use]
    pub fn low_water_offset(&self, partition: PartitionIndex) -> Option<Offset> {
        self.inner
            .read()
            .expect("wal head lock poisoned")
            .low_water_offset(partition)
    }

    /// The highest WAL offset materialized in the head for `partition`.
    #[must_use]
    pub fn high_water_offset(&self, partition: PartitionIndex) -> Option<Offset> {
        self.inner
            .read()
            .expect("wal head lock poisoned")
            .high_water_offset(partition)
    }

    /// Snapshot of all per-partition WAL offset watermarks.
    #[must_use]
    pub fn watermarks(&self) -> BTreeMap<PartitionIndex, PartitionWatermark> {
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

#[async_trait::async_trait]
impl MetricStore for WalHead {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult> {
        let store = Arc::clone(&*self.inner.read().expect("wal head lock poisoned"));
        store.scan(tenant, matchers, start_ms, end_ms).await
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>> {
        let store = Arc::clone(&*self.inner.read().expect("wal head lock poisoned"));
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
        let store = Arc::clone(&*self.inner.read().expect("wal head lock poisoned"));
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
        let store = Arc::clone(&*self.inner.read().expect("wal head lock poisoned"));
        store.series(tenant, matchers, start_ms, end_ms).await
    }

    async fn exemplars(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ExemplarRecord>> {
        let store = Arc::clone(&*self.inner.read().expect("wal head lock poisoned"));
        store.exemplars(tenant, matchers, start_ms, end_ms).await
    }

    async fn metadata(&self, tenant: &str, metric: Option<&str>) -> Result<Vec<MetadataRecord>> {
        let store = Arc::clone(&*self.inner.read().expect("wal head lock poisoned"));
        store.metadata(tenant, metric).await
    }

    async fn cardinality_label_names(&self, tenant: &str) -> Result<Vec<LabelNameCardinality>> {
        let store = Arc::clone(&*self.inner.read().expect("wal head lock poisoned"));
        store.cardinality_label_names(tenant).await
    }

    async fn cardinality_label_values(&self, tenant: &str) -> Result<Vec<LabelValueCardinality>> {
        let store = Arc::clone(&*self.inner.read().expect("wal head lock poisoned"));
        store.cardinality_label_values(tenant).await
    }

    async fn cardinality_active_series(&self, tenant: &str) -> Result<Vec<Labels>> {
        let store = Arc::clone(&*self.inner.read().expect("wal head lock poisoned"));
        store.cardinality_active_series(tenant).await
    }

    async fn tsdb_stats(&self, tenant: &str) -> Result<TsdbStats> {
        let store = Arc::clone(&*self.inner.read().expect("wal head lock poisoned"));
        store.tsdb_stats(tenant).await
    }

    async fn tsdb_blocks(&self, tenant: &str) -> Result<Vec<TsdbBlock>> {
        let store = Arc::clone(&*self.inner.read().expect("wal head lock poisoned"));
        store.tsdb_blocks(tenant).await
    }
}
