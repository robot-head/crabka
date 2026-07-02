//! Hot/cold `ProfileStore` union.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, UInt64Array};
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatch;
use crabka_blockstore::LabelMatcher;
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;

use crate::{
    Frame, PCOL_STACKTRACE_PARTITION, ProfileError, ProfileScan, ProfileStats, ProfileStore,
    SymbolSource, profile_samples_schema,
};

#[derive(Clone)]
pub struct UnionProfileStore<H, C> {
    hot: Arc<H>,
    cold: Arc<C>,
}

impl<H, C> UnionProfileStore<H, C> {
    #[must_use]
    pub fn new(hot: Arc<H>, cold: Arc<C>) -> Self {
        Self { hot, cold }
    }
}

#[async_trait::async_trait]
impl<H, C> ProfileStore for UnionProfileStore<H, C>
where
    H: ProfileStore,
    C: ProfileStore,
{
    async fn select(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileScan, ProfileError> {
        let hot = self
            .hot
            .select(tenant, profile_type, matchers, start_ms, end_ms)
            .await?;
        let cold = self
            .cold
            .select(tenant, profile_type, matchers, start_ms, end_ms)
            .await?;

        let mut batches = Vec::new();
        let mut symbols = UnionSymbols::default();
        batches.extend(collect_and_remap(hot, 1, &mut symbols).await?);
        batches.extend(collect_and_remap(cold, 2, &mut symbols).await?);
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(profile_samples_schema()));
        }

        let table = MemTable::try_new(profile_samples_schema(), vec![batches])
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        let ctx = SessionContext::new();
        let samples_table = "samples".to_string();
        ctx.register_table(&samples_table, Arc::new(table))
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        Ok(ProfileScan {
            ctx,
            samples_table,
            symbols: Arc::new(symbols),
        })
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        let hot = self
            .hot
            .label_names(tenant, matchers, start_ms, end_ms)
            .await?;
        let cold = self
            .cold
            .label_names(tenant, matchers, start_ms, end_ms)
            .await?;
        Ok(sorted_union([hot, cold]))
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        let hot = self
            .hot
            .label_values(tenant, name, matchers, start_ms, end_ms)
            .await?;
        let cold = self
            .cold
            .label_values(tenant, name, matchers, start_ms, end_ms)
            .await?;
        Ok(sorted_union([hot, cold]))
    }

    async fn profile_types(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        let hot = self.hot.profile_types(tenant, start_ms, end_ms).await?;
        let cold = self.cold.profile_types(tenant, start_ms, end_ms).await?;
        Ok(sorted_union([hot, cold]))
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Vec<(String, String)>>, ProfileError> {
        let hot = self
            .hot
            .series(tenant, matchers, label_names, start_ms, end_ms)
            .await?;
        let cold = self
            .cold
            .series(tenant, matchers, label_names, start_ms, end_ms)
            .await?;
        let mut set = BTreeSet::new();
        set.extend(hot);
        set.extend(cold);
        Ok(set.into_iter().collect())
    }

    async fn stats(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileStats, ProfileError> {
        let hot = self.hot.stats(tenant, start_ms, end_ms).await?;
        let cold = self.cold.stats(tenant, start_ms, end_ms).await?;
        Ok(ProfileStats {
            data_ingested: hot.data_ingested || cold.data_ingested,
            oldest_profile_time: min_option(hot.oldest_profile_time, cold.oldest_profile_time),
            newest_profile_time: max_option(hot.newest_profile_time, cold.newest_profile_time),
        })
    }
}

async fn collect_and_remap(
    scan: ProfileScan,
    source_id: u64,
    symbols: &mut UnionSymbols,
) -> Result<Vec<RecordBatch>, ProfileError> {
    let partition_base = source_id << 56;
    symbols.insert(partition_base, scan.symbols);
    let sql = format!("SELECT * FROM {}", scan.samples_table);
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    batches
        .into_iter()
        .map(|batch| remap_partitions(&batch, partition_base))
        .collect()
}

fn remap_partitions(batch: &RecordBatch, partition_base: u64) -> Result<RecordBatch, ProfileError> {
    let partition_idx = batch
        .schema()
        .column_with_name(PCOL_STACKTRACE_PARTITION)
        .ok_or_else(|| {
            ProfileError::Store(format!(
                "samples table missing {PCOL_STACKTRACE_PARTITION} column"
            ))
        })?
        .0;
    let partitions = batch.column(partition_idx).as_primitive::<UInt64Type>();
    let remapped = UInt64Array::from_iter_values(
        (0..batch.num_rows()).map(|row| partition_base | partitions.value(row)),
    );
    let mut columns = batch.columns().to_vec();
    columns[partition_idx] = Arc::new(remapped) as ArrayRef;
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|err| ProfileError::Store(err.to_string()))
}

#[derive(Default)]
struct UnionSymbols {
    sources: BTreeMap<u64, Arc<dyn SymbolSource>>,
}

impl UnionSymbols {
    fn insert(&mut self, partition_base: u64, source: Arc<dyn SymbolSource>) {
        self.sources.insert(partition_base, source);
    }
}

impl SymbolSource for UnionSymbols {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame> {
        let partition_base = partition & 0xff00_0000_0000_0000;
        self.sources
            .get(&partition_base)
            .map_or_else(Vec::new, |source| {
                source.resolve(partition ^ partition_base, id)
            })
    }
}

fn sorted_union<const N: usize>(values: [Vec<String>; N]) -> Vec<String> {
    values
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn min_option(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn max_option(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::UInt64Type;

    use crate::{
        EngineOpts, FlameEngine, FunctionRec, InMemoryProfileStore, LocationRec, ProfileStats,
        ProfileStore,
    };

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    fn store_with_frame(frame: &str, value: i64, timestamp_ms: i64) -> InMemoryProfileStore {
        store_with_frame_partition(frame, value, timestamp_ms, 0)
    }

    fn store_with_frame_partition(
        frame: &str,
        value: i64,
        timestamp_ms: i64,
        partition: u64,
    ) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name = store.symbols_mut().intern_string(frame);
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name,
            system_name: name,
            filename: 0,
            start_line: 0,
        });
        let location = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![crate::LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location]);
        store.push_sample(
            "tenant-a",
            PT,
            vec![("service_name".to_string(), "api".to_string())],
            partition,
            stacktrace,
            value,
            timestamp_ms,
        );
        store
    }

    #[tokio::test]
    async fn hot_cold_union_merges_samples_without_raw_id_collision() {
        let hot = store_with_frame("hot", 7, 20);
        let cold = store_with_frame("cold", 5, 10);
        let union = super::UnionProfileStore::new(Arc::new(hot), Arc::new(cold));
        let engine = FlameEngine::new(Arc::new(union), EngineOpts::default());

        let flamegraph = engine
            .select_merge_stacktraces("tenant-a", PT, r#"{service_name="api"}"#, 0, 60_000, 0)
            .await
            .unwrap();

        check!(flamegraph.total == 12);
        check!(flamegraph.names.iter().any(|name| name == "hot"));
        check!(flamegraph.names.iter().any(|name| name == "cold"));
    }

    #[tokio::test]
    async fn hot_cold_union_merges_metadata_and_stats() {
        let mut hot = store_with_frame("hot", 7, 20);
        hot.push_sample(
            "tenant-a",
            "memory:alloc_space:bytes:space:bytes",
            vec![("service_name".to_string(), "worker".to_string())],
            0,
            1,
            3,
            40,
        );
        let cold = store_with_frame("cold", 5, 10);
        let union = super::UnionProfileStore::new(Arc::new(hot), Arc::new(cold));

        let types = union.profile_types("tenant-a", 0, 100).await.unwrap();
        let values = union
            .label_values("tenant-a", "service_name", &[], 0, 100)
            .await
            .unwrap();
        let names = union.label_names("tenant-a", &[], 0, 100).await.unwrap();
        let series = union
            .series("tenant-a", &[], &["service_name".to_string()], 0, 100)
            .await
            .unwrap();
        let stats = union.stats("tenant-a", 0, 100).await.unwrap();

        check!(
            types
                == vec![
                    "memory:alloc_space:bytes:space:bytes".to_string(),
                    PT.to_string(),
                ]
        );
        check!(names == vec!["service_name".to_string()]);
        check!(values == vec!["api".to_string(), "worker".to_string()]);
        check!(series.len() == 2);
        check!(
            stats
                == ProfileStats {
                    data_ingested: true,
                    oldest_profile_time: Some(10),
                    newest_profile_time: Some(40),
                }
        );
    }

    #[tokio::test]
    async fn hot_cold_union_stats_reports_data_when_only_one_side_has_samples() {
        let hot = store_with_frame("hot", 7, 20);
        let cold = InMemoryProfileStore::new();
        let union = super::UnionProfileStore::new(Arc::new(hot), Arc::new(cold));

        let stats = union.stats("tenant-a", 0, 100).await.unwrap();

        assert!(
            stats
                == ProfileStats {
                    data_ingested: true,
                    oldest_profile_time: Some(20),
                    newest_profile_time: Some(20),
                }
        );
    }

    #[tokio::test]
    async fn hot_partition_remap_preserves_existing_low_and_high_bits() {
        let partition = 0x0100_0000_0000_0001;
        let hot = store_with_frame_partition("hot", 7, 20, partition);
        let cold = InMemoryProfileStore::new();
        let union = super::UnionProfileStore::new(Arc::new(hot), Arc::new(cold));
        let scan = union.select("tenant-a", PT, &[], 0, 100).await.unwrap();
        let df = scan
            .ctx
            .sql(&format!(
                "SELECT {} FROM {}",
                crate::PCOL_STACKTRACE_PARTITION,
                scan.samples_table
            ))
            .await
            .unwrap();
        let out = df.collect().await.unwrap();
        let partitions = out[0].column(0).as_primitive::<UInt64Type>();

        assert!(partitions.value(0) == partition);
    }
}
