//! Live WAL-tail backed profile store.

use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
};

use crabka_blockstore::LabelMatcher;
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_pprof::{InMemoryProfileStore, ProfileError, ProfileScan, ProfileStats, ProfileStore};
use crabka_units::{Time, convert::TimeExt as _, hours};

use crate::{
    blockbuilder::{intern_record, profile_timestamp_ms},
    error::ProfilesError,
    wal::{PROFILES_WAL_TOPIC, ProfileRecord},
};

/// Default retention horizon for the in-memory WAL tail: samples older than this
/// (relative to the newest sample seen) are dropped so the hot store cannot grow
/// without bound.
const DEFAULT_MAX_AGE: Time = hours(6);

/// Hard cap on the number of retained source records, so a burst of same-instant
/// samples still cannot grow the hot store unboundedly.
const DEFAULT_MAX_RECORDS: usize = 1_000_000;

/// Rebuild the queryable store only once evictions reach `1 / FACTOR` of the
/// retained window. Rebuilding is O(window), so deferring it amortizes the cost
/// to O(1) per append in steady state, at the price of at most `window / FACTOR`
/// already-evicted records temporarily lingering in the queryable store (a
/// bounded memory slack, not a correctness issue — those rows are real, just
/// older than the strict horizon, and queries still filter by timestamp).
const REBUILD_AMORTIZE_FACTOR: usize = 8;

/// Retention policy for the in-memory WAL tail.
#[derive(Clone, Copy, Debug)]
pub struct RetentionConfig {
    /// Drop samples whose timestamp is older than `newest_ts - max_age`.
    pub max_age: Time,
    /// Drop the oldest records once more than this many are retained.
    pub max_records: usize,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_age: DEFAULT_MAX_AGE,
            max_records: DEFAULT_MAX_RECORDS,
        }
    }
}

/// A source record retained for retention bookkeeping / rebuilds.
struct Retained {
    /// Newest sample timestamp (ms) carried by this record.
    max_ts_ms: i64,
    record: ProfileRecord,
}

/// Retained source records plus the count of records evicted since the last
/// rebuild, used to amortize rebuilds (see [`REBUILD_AMORTIZE_FACTOR`]).
#[derive(Default)]
struct RetainedState {
    records: VecDeque<Retained>,
    evicted_since_rebuild: usize,
}

#[derive(Clone)]
pub struct WalTailProfileStore {
    /// Copy-on-write snapshot of the queryable store. Queries clone the inner
    /// `Arc` (a cheap refcount bump) instead of deep-cloning every sample; writes
    /// mutate via `Arc::make_mut`, which only deep-copies while a snapshot is
    /// still outstanding.
    inner: Arc<RwLock<Arc<InMemoryProfileStore>>>,
    /// Source records retained within the retention window, used to rebuild the
    /// queryable store after eviction (the inner store exposes no row-level
    /// prune API).
    retained: Arc<RwLock<RetainedState>>,
    retention: RetentionConfig,
}

impl Default for WalTailProfileStore {
    fn default() -> Self {
        Self::new()
    }
}

impl WalTailProfileStore {
    #[must_use]
    pub fn new() -> Self {
        Self::with_retention(RetentionConfig::default())
    }

    #[must_use]
    pub fn with_retention(retention: RetentionConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Arc::new(InMemoryProfileStore::new()))),
            retained: Arc::new(RwLock::new(RetainedState::default())),
            retention,
        }
    }

    ///
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn append_record(&self, record: ProfileRecord) -> Result<(), ProfilesError> {
        let max_ts_ms = record
            .samples
            .iter()
            .map(|sample| profile_timestamp_ms(sample.timestamp_ns))
            .max();

        {
            let mut guard = self
                .inner
                .write()
                .map_err(|_| ProfilesError::Wal("hot profile store lock poisoned".to_string()))?;
            apply_record(Arc::make_mut(&mut guard), &record)?;
        }

        // Records with no samples carry no timestamp and need no retention
        // bookkeeping (they contributed nothing to the store either).
        let Some(max_ts_ms) = max_ts_ms else {
            return Ok(());
        };

        let mut retained = self
            .retained
            .write()
            .map_err(|_| ProfilesError::Wal("hot profile store lock poisoned".to_string()))?;
        retained.records.push_back(Retained { max_ts_ms, record });
        Self::prune(&self.retention, &mut retained);
        if Self::should_rebuild(&retained) {
            self.rebuild(&retained.records)?;
            retained.evicted_since_rebuild = 0;
        }
        Ok(())
    }

    /// Drop retained records that fall outside the retention window, counting how
    /// many were evicted since the last rebuild.
    fn prune(retention: &RetentionConfig, state: &mut RetainedState) {
        // `max_ts_ms` is an epoch-millisecond instant; only the retention window
        // is an extent, so it converts here and the subtraction stays exact
        // integer arithmetic (and saturates rather than overflowing).
        let newest = state.records.back().map_or(i64::MIN, |item| item.max_ts_ms);
        let horizon = newest.saturating_sub(retention.max_age.millis_i64());
        while state
            .records
            .front()
            .is_some_and(|item| item.max_ts_ms < horizon)
        {
            state.records.pop_front();
            state.evicted_since_rebuild += 1;
        }
        while state.records.len() > retention.max_records {
            state.records.pop_front();
            state.evicted_since_rebuild += 1;
        }
    }

    /// Rebuild only once evictions reach `1 / REBUILD_AMORTIZE_FACTOR` of the live
    /// window (or the window has fully drained), so a steady-state append that
    /// evicts one record does not trigger an O(window) rebuild every time.
    fn should_rebuild(state: &RetainedState) -> bool {
        state.evicted_since_rebuild > 0
            && state
                .evicted_since_rebuild
                .saturating_mul(REBUILD_AMORTIZE_FACTOR)
                >= state.records.len()
    }

    /// Rebuild the queryable store from the surviving retained records. Re-interning
    /// is deterministic, so the rebuilt store is equivalent to the un-evicted tail.
    fn rebuild(&self, retained: &VecDeque<Retained>) -> Result<(), ProfilesError> {
        let mut fresh = InMemoryProfileStore::new();
        for item in retained {
            apply_record(&mut fresh, &item.record)?;
        }
        let mut guard = self
            .inner
            .write()
            .map_err(|_| ProfilesError::Wal("hot profile store lock poisoned".to_string()))?;
        *guard = Arc::new(fresh);
        Ok(())
    }

    /// Cheap copy-on-write snapshot: clones the inner `Arc`, not the samples.
    fn snapshot(&self) -> Result<Arc<InMemoryProfileStore>, ProfileError> {
        self.inner
            .read()
            .map_err(|_| ProfileError::Store("hot profile store lock poisoned".to_string()))
            .map(|guard| Arc::clone(&guard))
    }
}

/// Intern + push every sample of `record` into `store`.
fn apply_record(
    store: &mut InMemoryProfileStore,
    record: &ProfileRecord,
) -> Result<(), ProfilesError> {
    let stack_ids = intern_record(store.symbols_mut(), record)?;
    let total_value = record.samples.iter().map(|sample| sample.value).sum();
    for (sample, stack_id) in record.samples.iter().zip(stack_ids) {
        let timestamp_ms = profile_timestamp_ms(sample.timestamp_ns);
        store.push_sample_with_total_and_associations(
            (&record.tenant, &record.profile_type),
            record.labels.clone(),
            (crate::blockbuilder::STACKTRACE_PARTITION, stack_id),
            (sample.value, total_value),
            timestamp_ms,
            (sample.span_id, sample.trace_id.clone()),
        );
    }
    Ok(())
}

#[async_trait::async_trait]
impl ProfileStore for WalTailProfileStore {
    async fn select(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileScan, ProfileError> {
        self.snapshot()?
            .select(tenant, profile_type, matchers, start_ms, end_ms)
            .await
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        self.snapshot()?
            .label_names(tenant, matchers, start_ms, end_ms)
            .await
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        self.snapshot()?
            .label_values(tenant, name, matchers, start_ms, end_ms)
            .await
    }

    async fn profile_types(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        self.snapshot()?
            .profile_types(tenant, start_ms, end_ms)
            .await
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Vec<(String, String)>>, ProfileError> {
        self.snapshot()?
            .series(tenant, matchers, label_names, start_ms, end_ms)
            .await
    }

    async fn stats(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileStats, ProfileError> {
        self.snapshot()?.stats(tenant, start_ms, end_ms).await
    }
}

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn run_wal_tail(
    store: WalTailProfileStore,
    bootstrap: String,
    group_id: String,
    poll_timeout: Time,
    client_dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity,
    client_frame_max: crabka_client_core::ClientFrameMax,
) -> Result<(), ProfilesError> {
    run_wal_tail_with_topic(
        store,
        bootstrap,
        group_id,
        PROFILES_WAL_TOPIC.to_owned(),
        poll_timeout,
        client_dispatch_queue_capacity,
        client_frame_max,
    )
    .await
}

/// Consume the configured profiles WAL topic into the hot query store.
///
/// # Errors
/// Returns an error when the consumer cannot be built, polled, decoded, or committed.
pub async fn run_wal_tail_with_topic(
    store: WalTailProfileStore,
    bootstrap: String,
    group_id: String,
    wal_topic: String,
    poll_timeout: Time,
    client_dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity,
    client_frame_max: crabka_client_core::ClientFrameMax,
) -> Result<(), ProfilesError> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .dispatch_queue_capacity(client_dispatch_queue_capacity.get())
        .frame_max(client_frame_max.size())
        .group_id(group_id)
        .subscribe(vec![wal_topic])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .map_err(|err| ProfilesError::Wal(format!("hot WAL-tail consumer build failed: {err}")))?;

    loop {
        let records = consumer.poll(poll_timeout).await.map_err(|err| {
            ProfilesError::Wal(format!("hot WAL-tail consumer poll failed: {err}"))
        })?;
        for record in records {
            let Some(value) = record.value.as_deref() else {
                continue;
            };
            store.append_record(ProfileRecord::decode(value)?)?;
        }
        consumer
            .commit_sync()
            .await
            .map_err(|err| ProfilesError::Wal(format!("hot WAL-tail commit failed: {err}")))?;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, BinaryArray};
    use assert2::assert;
    use crabka_blockstore::{LabelMatcher, MatchOp};
    use crabka_pprof::{EngineOpts, FlameEngine, PCOL_TRACE_ID, ProfileStore, SeriesAgg};
    use crabka_units::{Time, convert::TimeExt as _, secs};

    use crate::wal::{ProfileRecord, WalFunction, WalLocation, WalSample, WalSymbolSet};

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    /// A retention window so long nothing can age out of it, for the tests that
    /// exercise the record-count budget in isolation. The pruning horizon
    /// saturates to `i64::MIN`, so no timestamp is ever below it.
    fn unlimited_max_age() -> Time {
        Time::from_millis(i64::MAX)
    }

    fn record() -> ProfileRecord {
        ProfileRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![
                ("__name__".to_string(), "process_cpu".to_string()),
                ("service_name".to_string(), "api".to_string()),
                ("__profile_type__".to_string(), PT.to_string()),
            ],
            profile_type: PT.to_string(),
            samples: vec![WalSample {
                stacktrace_location_refs: vec![0],
                value: 9,
                timestamp_ns: 1_700_000_000_000,
                span_id: Some(42),
                trace_id: Some(vec![0xaa; 16]),
            }],
            symbols: WalSymbolSet {
                strings: vec![String::new(), "hot_fn".to_string(), "hot.rs".to_string()],
                functions: vec![WalFunction {
                    name: 1,
                    system_name: 1,
                    filename: 2,
                    start_line: 1,
                }],
                locations: vec![WalLocation {
                    address: 0x40,
                    mapping_id: 0,
                    lines: vec![(0, 11)],
                }],
                mappings: vec![],
            },
        }
    }

    #[tokio::test]
    async fn appended_wal_records_are_queryable_as_hot_profiles() {
        let store = super::WalTailProfileStore::new();
        store.append_record(record()).unwrap();
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());

        let flamegraph = engine
            .select_merge_stacktraces("tenant-a", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();

        assert!(flamegraph.total == 9);
        assert!(flamegraph.names.iter().any(|name| name == "hot_fn"));
    }

    #[tokio::test]
    async fn appended_wal_records_are_queryable_with_millisecond_timestamps() {
        let store = super::WalTailProfileStore::new();
        store.append_record(record()).unwrap();
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());

        let series = engine
            .select_series(
                ("tenant-a", PT, r#"{service_name="api"}"#),
                &[],
                secs(1),
                SeriesAgg::Sum,
                (0, i64::MAX),
            )
            .await
            .unwrap();

        assert!(series.len() == 1);
        assert!(series[0].points == vec![(1_700_000, 9.0)]);
    }

    #[tokio::test]
    async fn appended_wal_records_preserve_trace_ids_in_hot_samples() {
        let store = super::WalTailProfileStore::new();
        store.append_record(record()).unwrap();

        let scan = store
            .select(
                "tenant-a",
                PT,
                &[LabelMatcher::new(
                    "service_name".to_string(),
                    MatchOp::Eq,
                    "api".to_string(),
                )],
                0,
                i64::MAX,
            )
            .await
            .unwrap();
        let batches = scan
            .ctx
            .sql(&format!(
                "SELECT {PCOL_TRACE_ID} FROM {}",
                scan.samples_table
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let trace_ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(!trace_ids.is_null(0));
        assert!(trace_ids.value(0) == &[0xaa; 16]);
    }

    fn record_at(value: i64, timestamp_ns: i64) -> ProfileRecord {
        let mut rec = record();
        rec.samples[0].value = value;
        rec.samples[0].timestamp_ns = timestamp_ns;
        rec
    }

    #[tokio::test]
    async fn retention_evicts_samples_older_than_the_horizon() {
        // Tight 1s window: an old sample must be dropped once a much newer one
        // arrives, so the hot store does not grow without bound.
        let store = super::WalTailProfileStore::with_retention(super::RetentionConfig {
            max_age: secs(1),
            max_records: usize::MAX,
        });
        // Old sample at t=0ms, then a fresh sample 10s later.
        store.append_record(record_at(5, 0)).unwrap();
        store
            .append_record(record_at(7, 10_000 * 1_000_000))
            .unwrap();

        // Querying the full range must see only the surviving fresh sample.
        let stats = store.stats("tenant-a", 0, i64::MAX).await.unwrap();
        assert!(stats.oldest_profile_time == Some(10_000), "{stats:?}");
        assert!(stats.newest_profile_time == Some(10_000), "{stats:?}");

        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());
        let fg = engine
            .select_merge_stacktraces("tenant-a", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();
        assert!(fg.total == 7, "old sample not evicted: {}", fg.total);
    }

    #[tokio::test]
    async fn retention_evicts_by_record_budget() {
        // max_records=2 with an unbounded age window: the third append drops the
        // oldest record regardless of age.
        let store = super::WalTailProfileStore::with_retention(super::RetentionConfig {
            max_age: unlimited_max_age(),
            max_records: 2,
        });
        store.append_record(record_at(1, 1_000_000)).unwrap();
        store.append_record(record_at(2, 2_000_000)).unwrap();
        store.append_record(record_at(4, 3_000_000)).unwrap();

        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());
        let fg = engine
            .select_merge_stacktraces("tenant-a", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();
        // Only the two most recent records (values 2 and 4) survive.
        assert!(fg.total == 6, "budget eviction wrong: {}", fg.total);
    }

    #[tokio::test]
    async fn amortized_eviction_preserves_recent_query_results() {
        // Small budget + many appends: rebuilds are amortized (deferred), so the
        // queryable store may briefly over-retain already-evicted rows. A
        // timestamp-scoped query must still return exactly the records inside the
        // requested window, regardless of any lingering older rows.
        let store = super::WalTailProfileStore::with_retention(super::RetentionConfig {
            max_age: unlimited_max_age(),
            max_records: 10,
        });
        for i in 1..=50_i64 {
            store
                .append_record(record_at(i, i * 1_000_000_000))
                .unwrap();
        }
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());
        // Records 46..=50 sit at 46_000..=50_000 ms; query that window only.
        let fg = engine
            .select_merge_stacktraces(
                "tenant-a",
                PT,
                r#"{service_name="api"}"#,
                46_000,
                i64::MAX,
                0,
            )
            .await
            .unwrap();
        assert!(
            fg.total == 46 + 47 + 48 + 49 + 50,
            "recent-window query wrong: {}",
            fg.total
        );
    }

    #[tokio::test]
    async fn copy_on_write_snapshot_is_isolated_from_later_appends() {
        // A snapshot taken before an append must not observe the appended sample:
        // proves queries read a consistent COW snapshot rather than a live store.
        let store = super::WalTailProfileStore::new();
        store.append_record(record_at(5, 1_000_000)).unwrap();
        let snapshot = store.snapshot().unwrap();

        // Mutate the store after taking the snapshot.
        store.append_record(record_at(11, 2_000_000)).unwrap();

        // The pre-append snapshot still sees only the original sample.
        let before = snapshot.stats("tenant-a", 0, i64::MAX).await.unwrap();
        assert!(before.oldest_profile_time == Some(1), "{before:?}");
        assert!(before.newest_profile_time == Some(1), "{before:?}");

        // A fresh snapshot sees both samples.
        let after = store
            .snapshot()
            .unwrap()
            .stats("tenant-a", 0, i64::MAX)
            .await
            .unwrap();
        assert!(after.newest_profile_time == Some(2), "{after:?}");
    }
}
