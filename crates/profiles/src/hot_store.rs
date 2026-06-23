//! Live WAL-tail backed profile store.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use crabka_blockstore::LabelMatcher;
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_pprof::{InMemoryProfileStore, ProfileError, ProfileScan, ProfileStats, ProfileStore};

use crate::blockbuilder::{intern_record, profile_timestamp_ms};
use crate::error::ProfilesError;
use crate::wal::{PROFILES_WAL_TOPIC, ProfileRecord};

#[derive(Clone, Default)]
pub struct WalTailProfileStore {
    inner: Arc<RwLock<InMemoryProfileStore>>,
}

impl WalTailProfileStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(InMemoryProfileStore::new())),
        }
    }

    pub fn append_record(&self, record: ProfileRecord) -> Result<(), ProfilesError> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| ProfilesError::Wal("hot profile store lock poisoned".to_string()))?;
        let stack_ids = intern_record(guard.symbols_mut(), &record)?;
        let total_value = record.samples.iter().map(|sample| sample.value).sum();
        for (sample, stack_id) in record.samples.iter().zip(stack_ids) {
            let timestamp_ms = profile_timestamp_ms(sample.timestamp_ns);
            guard.push_sample_with_total_and_associations(
                &record.tenant,
                &record.profile_type,
                record.labels.clone(),
                crate::blockbuilder::STACKTRACE_PARTITION,
                stack_id,
                sample.value,
                total_value,
                timestamp_ms,
                sample.span_id,
                sample.trace_id.clone(),
            );
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<InMemoryProfileStore, ProfileError> {
        self.inner
            .read()
            .map_err(|_| ProfileError::Store("hot profile store lock poisoned".to_string()))
            .map(|guard| guard.clone())
    }
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

pub async fn run_wal_tail(
    store: WalTailProfileStore,
    bootstrap: String,
    group_id: String,
    poll_timeout: Duration,
) -> Result<(), ProfilesError> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id(group_id)
        .subscribe(vec![PROFILES_WAL_TOPIC.to_string()])
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

    use crate::wal::{ProfileRecord, WalFunction, WalLocation, WalSample, WalSymbolSet};

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

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
                "tenant-a",
                PT,
                r#"{service_name="api"}"#,
                &[],
                1.0,
                SeriesAgg::Sum,
                0,
                i64::MAX,
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
}
