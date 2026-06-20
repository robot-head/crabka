//! Object-store backed cold-block `ProfileStore`.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, UInt64Array};
use arrow::datatypes::{Int64Type, UInt64Type};
use arrow::record_batch::RecordBatch;
use crabka_blockstore::{LabelMatcher, ProfileIndex, SeriesFingerprint};
use crabka_pprof::{
    ChainedResolver, DebuginfodResolver, FileSystemResolver, Frame, NativeResolver, ProfileError,
    ProfileScan, ProfileStats, ProfileStore, SymbolDb, SymbolSource, profile_samples_schema,
};
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::blockbuilder::STACKTRACE_PARTITION;
use crate::symbolizer::AddressFallbackResolver;
use crabka_pprof::LazySymbolizer;

#[derive(Clone)]
pub struct ColdProfileStore {
    store: Arc<dyn ObjectStore>,
    index: Arc<ProfileIndex>,
    resolver: Arc<ChainedResolver>,
}

impl ColdProfileStore {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, index: Arc<ProfileIndex>) -> Self {
        Self {
            store,
            index,
            resolver: local_native_resolver(),
        }
    }

    pub fn new_with_debuginfod_urls(
        store: Arc<dyn ObjectStore>,
        index: Arc<ProfileIndex>,
        urls: Vec<String>,
    ) -> Result<Self, ProfileError> {
        let mut resolvers: Vec<Arc<dyn NativeResolver>> =
            vec![Arc::new(FileSystemResolver::default())];
        if !urls.is_empty() {
            let debuginfod = DebuginfodResolver::new(urls).map_err(ProfileError::Store)?;
            resolvers.push(Arc::new(debuginfod));
        }
        resolvers.push(Arc::new(AddressFallbackResolver));
        Ok(Self {
            store,
            index,
            resolver: Arc::new(ChainedResolver::new(resolvers)),
        })
    }

    async fn block_keys(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<(Vec<String>, std::collections::BTreeSet<SeriesFingerprint>), ProfileError> {
        let fps = self
            .index
            .select_fingerprints(tenant, profile_type, matchers)
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        if fps.is_empty() {
            return Ok((Vec::new(), fps));
        }
        let blocks = self
            .index
            .candidate_blocks_for_series(tenant, &fps, start_ms, end_ms);
        Ok((blocks, fps))
    }
}

fn local_native_resolver() -> Arc<ChainedResolver> {
    Arc::new(ChainedResolver::new(vec![
        Arc::new(FileSystemResolver::default()),
        Arc::new(AddressFallbackResolver),
    ]))
}

#[async_trait::async_trait]
impl ProfileStore for ColdProfileStore {
    async fn select(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileScan, ProfileError> {
        let (blocks, fps) = self
            .block_keys(tenant, profile_type, matchers, start_ms, end_ms)
            .await?;
        let mut batches = Vec::new();
        let mut symbols = CompositeSymbols::default();
        for (block_idx, block_key) in blocks.iter().enumerate() {
            let partition_base = u64::try_from(block_idx)
                .map_err(|err| ProfileError::Store(err.to_string()))?
                .saturating_add(1)
                << 32;
            let symdb = self.load_symdb(block_key).await?;
            let source = Arc::new(LazySymbolizer::new(symdb, Arc::clone(&self.resolver)));
            let mut partitions = self.index.stacktrace_partitions(block_key);
            if partitions.is_empty() {
                partitions.push(STACKTRACE_PARTITION);
            }
            for partition in partitions {
                symbols.insert(partition_base | partition, source.clone(), partition);
            }
            batches.extend(
                self.load_block_batches(
                    block_key,
                    partition_base,
                    &fps,
                    profile_type,
                    start_ms,
                    end_ms,
                )
                .await?,
            );
        }

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
        let active = self
            .active_fingerprints_for_rows(tenant, matchers, start_ms, end_ms)
            .await?;
        Ok(self.index.label_names_for_fingerprints(tenant, &active))
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        let active = self
            .active_fingerprints_for_rows(tenant, matchers, start_ms, end_ms)
            .await?;
        Ok(self
            .index
            .label_values_for_fingerprints(tenant, name, &active))
    }

    async fn profile_types(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        let active = self
            .active_fingerprints_for_rows(tenant, &[], start_ms, end_ms)
            .await?;
        Ok(self.index.profile_types_for_fingerprints(tenant, &active))
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Vec<(String, String)>>, ProfileError> {
        let active = self
            .active_fingerprints_for_rows(tenant, matchers, start_ms, end_ms)
            .await?;
        Ok(self
            .index
            .series_for_fingerprints(tenant, &active, label_names))
    }

    async fn stats(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileStats, ProfileError> {
        let bounds = self
            .sample_time_bounds_for_rows(tenant, start_ms, end_ms)
            .await?;
        Ok(ProfileStats {
            data_ingested: bounds.is_some(),
            oldest_profile_time: bounds.map(|(oldest, _)| oldest),
            newest_profile_time: bounds.map(|(_, newest)| newest),
        })
    }
}

impl ColdProfileStore {
    async fn sample_time_bounds_for_rows(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Option<(i64, i64)>, ProfileError> {
        let fps = self
            .index
            .matching_fingerprints(tenant, &[])
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        if fps.is_empty() {
            return Ok(None);
        }
        let blocks = self
            .index
            .candidate_blocks_for_series(tenant, &fps, start_ms, end_ms);
        let mut bounds: Option<(i64, i64)> = None;
        for block_key in blocks {
            for batch in self
                .load_block_batches_for_fingerprints(&block_key, &fps)
                .await?
            {
                let fingerprints = batch.column(0).as_primitive::<UInt64Type>();
                let timestamps = batch.column(1).as_primitive::<Int64Type>();
                for row in 0..batch.num_rows() {
                    let fp = fingerprints.value(row);
                    let ts = timestamps.value(row);
                    if fps.contains(&fp) && ts >= start_ms && ts <= end_ms {
                        bounds = Some(match bounds {
                            Some((oldest, newest)) => (oldest.min(ts), newest.max(ts)),
                            None => (ts, ts),
                        });
                    }
                }
            }
        }
        Ok(bounds)
    }

    async fn active_fingerprints_for_rows(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<BTreeSet<SeriesFingerprint>, ProfileError> {
        let fps = self
            .index
            .matching_fingerprints(tenant, matchers)
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        if fps.is_empty() {
            return Ok(BTreeSet::new());
        }
        let blocks = self
            .index
            .candidate_blocks_for_series(tenant, &fps, start_ms, end_ms);
        let mut active = BTreeSet::new();
        for block_key in blocks {
            for batch in self
                .load_block_batches_for_fingerprints(&block_key, &fps)
                .await?
            {
                let fingerprints = batch.column(0).as_primitive::<UInt64Type>();
                let timestamps = batch.column(1).as_primitive::<Int64Type>();
                for row in 0..batch.num_rows() {
                    let fp = fingerprints.value(row);
                    if timestamps.value(row) >= start_ms && timestamps.value(row) <= end_ms {
                        active.insert(fp);
                    }
                }
            }
        }
        Ok(active)
    }

    async fn load_block_batches_for_fingerprints(
        &self,
        block_key: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Result<Vec<RecordBatch>, ProfileError> {
        let bytes = self
            .store
            .get(&Path::from(block_key))
            .await
            .map_err(|err| ProfileError::Store(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|err| ProfileError::Store(err.to_string()))?
            .build()
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch.map_err(|err| ProfileError::Store(err.to_string()))?;
            if batch_fingerprints_overlap(&batch, fps) {
                out.push(batch);
            }
        }
        Ok(out)
    }

    async fn load_symdb(&self, block_key: &str) -> Result<SymbolDb, ProfileError> {
        let key = format!("{block_key}.symdb");
        let bytes = self
            .store
            .get(&Path::from(key))
            .await
            .map_err(|err| ProfileError::Store(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        SymbolDb::decode(&bytes)
    }

    async fn load_block_batches(
        &self,
        block_key: &str,
        partition_base: u64,
        fps: &std::collections::BTreeSet<SeriesFingerprint>,
        profile_type: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<RecordBatch>, ProfileError> {
        let bytes = self
            .store
            .get(&Path::from(block_key))
            .await
            .map_err(|err| ProfileError::Store(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|err| ProfileError::Store(err.to_string()))?
            .build()
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch.map_err(|err| ProfileError::Store(err.to_string()))?;
            let filtered = filter_and_remap_batch(
                &batch,
                partition_base,
                fps,
                profile_type,
                start_ms,
                end_ms,
            )?;
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        }
        Ok(out)
    }
}

fn batch_fingerprints_overlap(batch: &RecordBatch, fps: &BTreeSet<SeriesFingerprint>) -> bool {
    let fingerprints = batch.column(0).as_primitive::<UInt64Type>();
    (0..batch.num_rows()).any(|row| fps.contains(&fingerprints.value(row)))
}

#[derive(Default)]
struct CompositeSymbols {
    by_partition: HashMap<u64, (Arc<dyn SymbolSource>, u64)>,
}

impl CompositeSymbols {
    fn insert(
        &mut self,
        external_partition: u64,
        symbols: Arc<dyn SymbolSource>,
        local_partition: u64,
    ) {
        self.by_partition
            .insert(external_partition, (symbols, local_partition));
    }
}

impl SymbolSource for CompositeSymbols {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame> {
        self.by_partition
            .get(&partition)
            .map_or_else(Vec::new, |(symbols, local_partition)| {
                symbols.resolve(*local_partition, id)
            })
    }
}

fn filter_and_remap_batch(
    batch: &RecordBatch,
    partition_base: u64,
    fps: &std::collections::BTreeSet<SeriesFingerprint>,
    profile_type: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<RecordBatch, ProfileError> {
    let fingerprints = batch.column(0).as_primitive::<UInt64Type>();
    let timestamps = batch.column(1).as_primitive::<Int64Type>();
    let profile_types = batch
        .column(2)
        .as_dictionary::<arrow::datatypes::Int32Type>();
    let profile_values = profile_types.values().as_string::<i32>();
    let partitions = batch.column(5).as_primitive::<UInt64Type>();
    let mut indices = Vec::new();
    for row in 0..batch.num_rows() {
        let profile_key = profile_types.keys().value(row);
        let profile_idx = usize::try_from(profile_key)
            .map_err(|err| ProfileError::Store(format!("profile type key invalid: {err}")))?;
        let row_profile_type = profile_values.value(profile_idx);
        let ts = timestamps.value(row);
        if fps.contains(&fingerprints.value(row))
            && row_profile_type == profile_type
            && ts >= start_ms
            && ts <= end_ms
        {
            indices.push(row);
        }
    }

    let mut rows = Vec::with_capacity(indices.len());
    for row in indices {
        let mut cols = batch.columns().to_vec();
        let remapped = UInt64Array::from(
            (0..batch.num_rows())
                .map(|idx| partition_base | partitions.value(idx))
                .collect::<Vec<_>>(),
        );
        cols[5] = Arc::new(remapped) as ArrayRef;
        rows.push(arrow::compute::take_record_batch(
            &RecordBatch::try_new(batch.schema(), cols)
                .map_err(|err| ProfileError::Store(err.to_string()))?,
            &UInt64Array::from(vec![u64::try_from(row).expect("row fits u64")]),
        ));
    }

    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(profile_samples_schema()));
    }
    let rows = rows
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ProfileError::Store(err.to_string()))?;
    arrow::compute::concat_batches(&profile_samples_schema(), rows.iter())
        .map_err(|err| ProfileError::Store(err.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_blockstore::{BlockIndex, Labels, MatchOp};
    use crabka_pprof::{EngineOpts, FlameEngine, SymbolizeRequest};
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use super::*;
    use crate::blockbuilder::build_block;
    use crate::wal::{ProfileRecord, WalSample, WalSymbolSet};

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    #[tokio::test]
    async fn cold_store_merges_blocks_with_local_symbol_partitions() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec_a = record("t", "api", vec![0], 5);
        let rec_b = record("t", "api", vec![0], 7);
        let meta_a = build_block(&store, "t", 0, &[rec_a.clone()], (0, 0))
            .await
            .unwrap()
            .remove(0);
        let meta_b = build_block(&store, "t", 0, &[rec_b.clone()], (1, 1))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(rec_a.labels.iter().cloned());
        index.add_series("t", labels.fingerprint(), &labels);
        index.add_block(&meta_a);
        index.add_block(&meta_b);
        let cold = Arc::new(ColdProfileStore::new(store, Arc::new(index)));
        let engine = FlameEngine::new(cold, EngineOpts::default());

        let fg = engine
            .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();

        assert!(fg.total == 12);
        assert!(fg.names.iter().any(|name| name == "main"));
    }

    #[tokio::test]
    async fn cold_store_projects_labels_with_matchers() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec = record("t", "api", vec![0], 5);
        let meta = build_block(&store, "t", 0, std::slice::from_ref(&rec), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(rec.labels.iter().cloned());
        index.add_series("t", labels.fingerprint(), &labels);
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let values = cold
            .label_values(
                "t",
                "service_name",
                &[LabelMatcher::new("service_name", MatchOp::Eq, "api")],
                0,
                i64::MAX,
            )
            .await
            .unwrap();

        assert!(values == vec!["api".to_string()]);
    }

    #[tokio::test]
    async fn cold_store_stats_report_block_time_bounds() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec = record("t", "api", vec![0], 5);
        let meta = build_block(&store, "t", 0, std::slice::from_ref(&rec), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(rec.labels.iter().cloned());
        index.add_series("t", labels.fingerprint(), &labels);
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let stats = cold.stats("t", 0, i64::MAX).await.unwrap();

        assert!(stats.data_ingested);
        assert!(stats.oldest_profile_time == Some(1000));
        assert!(stats.newest_profile_time == Some(1000));
    }

    #[tokio::test]
    async fn cold_store_stats_honor_sample_time_inside_overlapping_block() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let later = record_at("t", "worker", vec![0], 7, 3_000_000_000);
        let records = vec![first.clone(), later.clone()];
        let meta = build_block(&store, "t", 0, &records, (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in [&first, &later] {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let stats = cold.stats("t", 1_000, 1_000).await.unwrap();

        assert!(stats.data_ingested);
        assert!(stats.oldest_profile_time == Some(1000), "{stats:?}");
        assert!(stats.newest_profile_time == Some(1000), "{stats:?}");
    }

    #[tokio::test]
    async fn cold_store_profile_types_honor_query_time_range() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec = record("t", "api", vec![0], 5);
        let meta = build_block(&store, "t", 0, std::slice::from_ref(&rec), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(rec.labels.iter().cloned());
        index.add_series("t", labels.fingerprint(), &labels);
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let types = cold.profile_types("t", 2_000, 3_000).await.unwrap();

        assert!(types.is_empty(), "{types:?}");
    }

    #[tokio::test]
    async fn cold_store_profile_types_do_not_leak_types_outside_time_range_in_same_block() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cpu = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let mut memory = record_at("t", "api", vec![0], 7, 3_000_000_000);
        memory.profile_type = "memory:alloc_space:bytes:space:bytes".to_string();
        memory.labels = vec![
            ("__name__".to_string(), "memory".to_string()),
            ("__profile_type__".to_string(), memory.profile_type.clone()),
            ("service_name".to_string(), "api".to_string()),
        ];
        let records = vec![cpu.clone(), memory.clone()];
        let meta = build_block(&store, "t", 0, &records, (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in &records {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let types = cold.profile_types("t", 1_000, 1_000).await.unwrap();

        assert!(types == vec![PT.to_string()], "{types:?}");
    }

    #[tokio::test]
    async fn cold_store_label_values_honor_query_time_range() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec = record("t", "api", vec![0], 5);
        let meta = build_block(&store, "t", 0, std::slice::from_ref(&rec), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(rec.labels.iter().cloned());
        index.add_series("t", labels.fingerprint(), &labels);
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let values = cold
            .label_values("t", "service_name", &[], 2_000, 3_000)
            .await
            .unwrap();

        assert!(values.is_empty(), "{values:?}");
    }

    #[tokio::test]
    async fn cold_store_label_values_do_not_leak_series_outside_time_range_in_same_block() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let api = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let worker = record_at("t", "worker", vec![0], 7, 3_000_000_000);
        let records = vec![api.clone(), worker.clone()];
        let meta = build_block(&store, "t", 0, &records, (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in &records {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let values = cold
            .label_values("t", "service_name", &[], 1_000, 1_000)
            .await
            .unwrap();

        assert!(values == vec!["api".to_string()], "{values:?}");
    }

    #[tokio::test]
    async fn cold_store_label_names_honor_query_time_range() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec = record("t", "api", vec![0], 5);
        let meta = build_block(&store, "t", 0, std::slice::from_ref(&rec), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(rec.labels.iter().cloned());
        index.add_series("t", labels.fingerprint(), &labels);
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let names = cold.label_names("t", &[], 2_000, 3_000).await.unwrap();

        assert!(names.is_empty(), "{names:?}");
    }

    #[tokio::test]
    async fn cold_store_label_names_do_not_leak_series_outside_time_range_in_same_block() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let api = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let mut worker = record_at("t", "worker", vec![0], 7, 3_000_000_000);
        worker
            .labels
            .push(("pod".to_string(), "worker-0".to_string()));
        let records = vec![api.clone(), worker.clone()];
        let meta = build_block(&store, "t", 0, &records, (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in &records {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let names = cold.label_names("t", &[], 1_000, 1_000).await.unwrap();

        assert!(!names.contains(&"pod".to_string()), "{names:?}");
    }

    #[tokio::test]
    async fn cold_store_series_honor_query_time_range() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec = record("t", "api", vec![0], 5);
        let meta = build_block(&store, "t", 0, std::slice::from_ref(&rec), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        let labels = Labels::from_pairs(rec.labels.iter().cloned());
        index.add_series("t", labels.fingerprint(), &labels);
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let series = cold
            .series("t", &[], &["service_name".to_string()], 2_000, 3_000)
            .await
            .unwrap();

        assert!(series.is_empty(), "{series:?}");
    }

    #[tokio::test]
    async fn cold_store_series_do_not_leak_series_outside_time_range_in_same_block() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let api = record_at("t", "api", vec![0], 5, 1_000_000_000);
        let worker = record_at("t", "worker", vec![0], 7, 3_000_000_000);
        let records = vec![api.clone(), worker.clone()];
        let meta = build_block(&store, "t", 0, &records, (0, 0))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in &records {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta);
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let series = cold
            .series("t", &[], &["service_name".to_string()], 1_000, 1_000)
            .await
            .unwrap();

        assert!(
            series == vec![vec![("service_name".to_string(), "api".to_string())]],
            "{series:?}"
        );
    }

    #[test]
    fn cold_store_native_resolver_falls_back_to_address_frame() {
        let resolver = local_native_resolver();
        let out = resolver
            .symbolize(&SymbolizeRequest {
                build_id: String::new(),
                filename: "/missing/native".to_string(),
                address: 0x99,
            })
            .unwrap();

        assert!(out[0].function == "/missing/native+0x99");
        assert!(out[0].file == "/missing/native");
    }

    fn record(tenant: &str, service: &str, stack: Vec<u32>, value: i64) -> ProfileRecord {
        record_at(tenant, service, stack, value, 1_000_000_000)
    }

    fn record_at(
        tenant: &str,
        service: &str,
        stack: Vec<u32>,
        value: i64,
        timestamp_ns: i64,
    ) -> ProfileRecord {
        ProfileRecord {
            tenant: tenant.to_string(),
            labels: vec![
                ("__name__".to_string(), "process_cpu".to_string()),
                ("__profile_type__".to_string(), PT.to_string()),
                ("service_name".to_string(), service.to_string()),
            ],
            profile_type: PT.to_string(),
            samples: vec![WalSample {
                stacktrace_location_refs: stack,
                value,
                timestamp_ns,
                span_id: None,
                trace_id: None,
            }],
            symbols: symbols(),
        }
    }

    fn symbols() -> WalSymbolSet {
        WalSymbolSet {
            strings: vec![String::new(), "main".to_string()],
            functions: vec![crate::wal::WalFunction {
                name: 1,
                system_name: 1,
                filename: 0,
                start_line: 0,
            }],
            locations: vec![crate::wal::WalLocation {
                address: 0,
                mapping_id: 0,
                lines: vec![(0, 1)],
            }],
            mappings: Vec::new(),
        }
    }
}
