//! Object-store backed cold-block `ProfileStore`.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, RwLock},
};

use arrow::{
    array::{ArrayRef, AsArray, UInt64Array},
    datatypes::{Int64Type, UInt64Type},
    record_batch::RecordBatch,
};
use crabka_blockstore::{LabelMatcher, ProfileIndex, SeriesFingerprint};
use crabka_pprof::{
    ChainedResolver, DebuginfodConfig, DebuginfodResolver, FileSystemResolver, Frame,
    LazySymbolizer, NativeResolver, ProfileError, ProfileScan, ProfileStats, ProfileStore,
    SymbolDb, SymbolSource, profile_samples_schema,
};
use datafusion::{catalog::MemTable, prelude::SessionContext};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{
    blockbuilder::STACKTRACE_PARTITION,
    ids::{ExternalPartition, LocalPartition},
    symbolizer::AddressFallbackResolver,
};

#[derive(Clone)]
pub struct ColdProfileStore {
    store: Arc<dyn ObjectStore>,
    // The block index is loaded from object storage and must be REFRESHED as the
    // block-builder writes new blocks — otherwise blocks created after the querier
    // started are invisible (a query only sees the startup snapshot). Held behind a
    // lock so a background task can swap in a freshly-loaded index; readers clone the
    // inner `Arc` out and never hold the guard across an await.
    index: Arc<RwLock<Arc<ProfileIndex>>>,
    resolver: Arc<ChainedResolver>,
}

impl ColdProfileStore {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, index: Arc<ProfileIndex>) -> Self {
        Self {
            store,
            index: Arc::new(RwLock::new(index)),
            resolver: local_native_resolver(),
        }
    }

    ///
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn new_with_debuginfod_urls(
        store: Arc<dyn ObjectStore>,
        index: Arc<ProfileIndex>,
        urls: Vec<String>,
    ) -> Result<Self, ProfileError> {
        Self::new_with_debuginfod_config(store, index, urls, DebuginfodConfig::default())
    }

    /// Create a cold profile store with explicit debuginfod resource policy.
    ///
    /// # Errors
    ///
    /// Returns an error when a configured debuginfod URL is invalid or its HTTP
    /// client cannot be built.
    pub fn new_with_debuginfod_config(
        store: Arc<dyn ObjectStore>,
        index: Arc<ProfileIndex>,
        urls: Vec<String>,
        config: DebuginfodConfig,
    ) -> Result<Self, ProfileError> {
        let mut resolvers: Vec<Arc<dyn NativeResolver>> =
            vec![Arc::new(FileSystemResolver::default())];
        if !urls.is_empty() {
            let debuginfod =
                DebuginfodResolver::with_config(urls, config).map_err(ProfileError::Store)?;
            resolvers.push(Arc::new(debuginfod));
        }
        resolvers.push(Arc::new(AddressFallbackResolver));
        Ok(Self {
            store,
            index: Arc::new(RwLock::new(index)),
            resolver: Arc::new(ChainedResolver::new(resolvers)),
        })
    }

    /// Current block index snapshot. The method clones the inner `Arc`, which is
    /// cheap, so it releases the lock immediately and never holds it across an
    /// `.await`.
    ///
    /// # Panics
    /// Panics if another thread poisoned the profile index lock.
    #[must_use]
    fn current_index(&self) -> Arc<ProfileIndex> {
        Arc::clone(&self.index.read().expect("profile index lock poisoned"))
    }

    /// Swap in a freshly-loaded block index so blocks written since the querier
    /// started become queryable. The periodic refresh task of the querier calls
    /// this method.
    ///
    /// # Panics
    /// Panics if another thread poisoned the profile index lock.
    pub fn replace_index(&self, index: Arc<ProfileIndex>) {
        *self.index.write().expect("profile index lock poisoned") = index;
    }

    fn block_keys(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<(Vec<String>, std::collections::BTreeSet<SeriesFingerprint>), ProfileError> {
        let fps = self
            .current_index()
            .select_fingerprints(tenant, profile_type, matchers)
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        if fps.is_empty() {
            return Ok((Vec::new(), fps));
        }
        let blocks = self
            .current_index()
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
        let (blocks, fps) = self.block_keys(tenant, profile_type, matchers, start_ms, end_ms)?;
        let mut batches = Vec::new();
        let mut symbols = CompositeSymbols::default();
        for (block_idx, block_key) in blocks.iter().enumerate() {
            // Re-base this block's stored partitions to a dense local `0..n` range
            // before OR-ing the per-block high-bit base. A block that has already
            // been compacted stores partitions that occupy the high bits; OR-ing a
            // fresh base straight onto them folds bits together and can collide
            // across blocks (e.g. `1<<32 | (2<<32)` and `2<<32 | (1<<32)` both ==
            // `3<<32`). Dense re-basing keeps each block's external keys unique.
            let stored_partitions = self.current_index().stacktrace_partitions(block_key);
            let partition_map = block_partition_map(block_idx, &stored_partitions)?;
            let symdb = self.load_symdb(block_key).await?;
            let source = Arc::new(LazySymbolizer::new(symdb, Arc::clone(&self.resolver)));
            for (source_partition, external) in &partition_map {
                // `source_partition` is the partition key within this block's own
                // symbol DB, so resolution stays scoped to the correct block.
                symbols.insert(
                    ExternalPartition(*external),
                    source.clone(),
                    LocalPartition(*source_partition),
                );
            }
            batches.extend(
                self.load_block_batches(
                    block_key,
                    &partition_map,
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
        if is_unbounded_metadata_range(start_ms, end_ms) {
            return self
                .current_index()
                .label_names_for(tenant, matchers)
                .map_err(|err| ProfileError::Store(err.to_string()));
        }
        let active = self
            .active_fingerprints_for_rows(tenant, matchers, start_ms, end_ms)
            .await?;
        Ok(self
            .current_index()
            .label_names_for_fingerprints(tenant, &active))
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        if is_unbounded_metadata_range(start_ms, end_ms) {
            return self
                .current_index()
                .label_values_for(tenant, name, matchers)
                .map_err(|err| ProfileError::Store(err.to_string()));
        }
        let active = self
            .active_fingerprints_for_rows(tenant, matchers, start_ms, end_ms)
            .await?;
        Ok(self
            .current_index()
            .label_values_for_fingerprints(tenant, name, &active))
    }

    async fn profile_types(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        if is_unbounded_metadata_range(start_ms, end_ms) {
            return Ok(self.current_index().profile_types(tenant));
        }
        let active = self
            .active_fingerprints_for_rows(tenant, &[], start_ms, end_ms)
            .await?;
        Ok(self
            .current_index()
            .profile_types_for_fingerprints(tenant, &active))
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Vec<(String, String)>>, ProfileError> {
        if is_unbounded_metadata_range(start_ms, end_ms) {
            return self
                .current_index()
                .series(tenant, matchers, label_names)
                .map_err(|err| ProfileError::Store(err.to_string()));
        }
        let active = self
            .active_fingerprints_for_rows(tenant, matchers, start_ms, end_ms)
            .await?;
        Ok(self
            .current_index()
            .series_for_fingerprints(tenant, &active, label_names))
    }

    async fn stats(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileStats, ProfileError> {
        // Derive the tenant's profile-time bounds from the per-block `min_ts`/
        // `max_ts` the index already tracks instead of loading and scanning every
        // candidate block's sample rows. `GetProfileStats` is unbounded
        // (`[0, i64::MAX]`), so a row scan reads the entire dataset on every
        // Grafana Profiles-Drilldown load; the index aggregate is in-memory and
        // O(blocks). Block bounds intersected with `[start_ms, end_ms]` are clamped
        // to the requested window so a narrower query never reports times outside
        // it, and `data_ingested` is true iff the tenant has any overlapping block.
        let bounds = self
            .current_index()
            .block_time_bounds(tenant, start_ms, end_ms)
            .map(|(block_min, block_max)| (block_min.max(start_ms), block_max.min(end_ms)));
        Ok(ProfileStats {
            data_ingested: bounds.is_some(),
            oldest_profile_time: bounds.map(|(oldest, _)| oldest),
            newest_profile_time: bounds.map(|(_, newest)| newest),
        })
    }
}

fn is_unbounded_metadata_range(start_ms: i64, end_ms: i64) -> bool {
    start_ms == 0 && end_ms == i64::MAX
}

impl ColdProfileStore {
    async fn active_fingerprints_for_rows(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<BTreeSet<SeriesFingerprint>, ProfileError> {
        let fps = self
            .current_index()
            .matching_fingerprints(tenant, matchers)
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        if fps.is_empty() {
            return Ok(BTreeSet::new());
        }
        let blocks = self
            .current_index()
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
        partition_map: &BTreeMap<u64, u64>,
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
            let filtered =
                filter_and_remap_batch(&batch, partition_map, fps, profile_type, start_ms, end_ms)?;
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        }
        Ok(out)
    }
}

/// Build the dense per-block partition map that the cold read path uses.
///
/// `stored_partitions` are the partitions of this block as the index records
/// them. They are already in the high bits if the block was compacted. This
/// function re-bases them to a dense local `0..n` range and ORs them with the
/// per-block high-bit base. The external keys are then unique even when a read
/// covers several already-compacted blocks. The map is
/// `stored_partition -> base | local_id`.
fn block_partition_map(
    block_idx: usize,
    stored_partitions: &[u64],
) -> Result<BTreeMap<u64, u64>, ProfileError> {
    let base = u64::try_from(block_idx + 1)
        .map_err(|err| ProfileError::Store(format!("block index does not fit u64: {err}")))?
        .checked_shl(32)
        .ok_or_else(|| {
            ProfileError::Store(format!("block base for index {block_idx} overflows u64"))
        })?;
    let mut sorted = stored_partitions.to_vec();
    if sorted.is_empty() {
        sorted.push(STACKTRACE_PARTITION);
    }
    sorted.sort_unstable();
    sorted.dedup();
    let mut map = BTreeMap::new();
    for (local, stored) in sorted.into_iter().enumerate() {
        let local = u64::try_from(local).map_err(|err| {
            ProfileError::Store(format!("local partition does not fit u64: {err}"))
        })?;
        if local >= 1 << 32 {
            return Err(ProfileError::Store(format!(
                "local partition {local} does not fit the low 32 bits"
            )));
        }
        map.insert(stored, base | local);
    }
    Ok(map)
}

fn batch_fingerprints_overlap(batch: &RecordBatch, fps: &BTreeSet<SeriesFingerprint>) -> bool {
    let fingerprints = batch.column(0).as_primitive::<UInt64Type>();
    (0..batch.num_rows()).any(|row| fps.contains(&fingerprints.value(row)))
}

#[derive(Default)]
struct CompositeSymbols {
    by_partition: HashMap<ExternalPartition, (Arc<dyn SymbolSource>, LocalPartition)>,
}

impl CompositeSymbols {
    fn insert(
        &mut self,
        external_partition: ExternalPartition,
        symbols: Arc<dyn SymbolSource>,
        local_partition: LocalPartition,
    ) {
        self.by_partition
            .insert(external_partition, (symbols, local_partition));
    }
}

impl SymbolSource for CompositeSymbols {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame> {
        self.by_partition
            .get(&ExternalPartition(partition))
            .map_or_else(Vec::new, |(symbols, local_partition)| {
                symbols.resolve(local_partition.0, id)
            })
    }
}

fn filter_and_remap_batch(
    batch: &RecordBatch,
    partition_map: &BTreeMap<u64, u64>,
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
    // Single pass: collect the indices of all surviving rows once.
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
            let row = u64::try_from(row)
                .map_err(|err| ProfileError::Store(format!("row index does not fit u64: {err}")))?;
            indices.push(row);
        }
    }

    if indices.is_empty() {
        return Ok(RecordBatch::new_empty(profile_samples_schema()));
    }

    // Remap the partition column once over the whole batch (cheap, O(N)) and
    // build the input batch a single time, then `take` all surviving rows in one
    // call instead of rebuilding the full batch per surviving row (O(R*N)).
    // Look up each stored partition in the dense per-block map so already-compacted
    // high-bit partitions are re-based without OR-folding/aliasing.
    let remapped = UInt64Array::from_iter_values((0..batch.num_rows()).map(|idx| {
        let stored = partitions.value(idx);
        partition_map.get(&stored).copied().unwrap_or(stored)
    }));
    let mut cols = batch.columns().to_vec();
    cols[5] = Arc::new(remapped) as ArrayRef;
    let remapped_batch = RecordBatch::try_new(profile_samples_schema(), cols)
        .map_err(|err| ProfileError::Store(err.to_string()))?;

    arrow::compute::take_record_batch(&remapped_batch, &UInt64Array::from(indices))
        .map_err(|err| ProfileError::Store(err.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use crabka_blockstore::{BlockIndex, Labels, MatchOp};
    use crabka_pprof::{DebuginfodConfig, EngineOpts, FlameEngine, SymbolizeRequest};
    use crabka_units::{mebibytes, millis, secs};
    use object_store::{ObjectStore, memory::InMemory};

    use super::*;
    use crate::{
        blockbuilder::build_block,
        wal::{ProfileRecord, WalSample, WalSymbolSet},
    };

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    #[test]
    fn cold_store_accepts_explicit_debuginfod_config() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = DebuginfodConfig::new(mebibytes(64), millis(250), secs(3)).unwrap();

        ColdProfileStore::new_with_debuginfod_config(
            store,
            Arc::new(ProfileIndex::new()),
            vec!["http://127.0.0.1:1".to_string()],
            config,
        )
        .unwrap();
    }

    #[tokio::test]
    async fn cold_store_merges_blocks_with_local_symbol_partitions() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec_a = record("t", "api", vec![0], 5);
        let rec_b = record("t", "api", vec![0], 7);
        let meta_a = build_block(&store, "t", 0, std::slice::from_ref(&rec_a), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let meta_b = build_block(&store, "t", 0, std::slice::from_ref(&rec_b), (1, 1))
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

        assert!(
            stats
                == ProfileStats {
                    data_ingested: true,
                    oldest_profile_time: Some(1000),
                    newest_profile_time: Some(1000),
                }
        );
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

        assert!(
            stats
                == ProfileStats {
                    data_ingested: true,
                    oldest_profile_time: Some(1000),
                    newest_profile_time: Some(1000),
                }
        );
    }

    #[tokio::test]
    async fn cold_store_stats_aggregate_block_bounds_without_scanning_batches() {
        // Two blocks with disjoint, known time spans. The global stats must be the
        // min of the mins and the max of the maxes derived from the index's
        // per-block metadata. To prove the bounds come from block metadata and not
        // from scanning sample rows, the parquet block objects are DELETED from the
        // store before calling `stats`: a row scan would fail to load them, but the
        // index aggregate succeeds because it never touches the blocks.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let early = record_at("t", "api", vec![0], 5, 1_000_000_000); // 1000 ms
        let late = record_at("t", "worker", vec![0], 7, 5_000_000_000); // 5000 ms
        let meta_early = build_block(&store, "t", 0, std::slice::from_ref(&early), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let meta_late = build_block(&store, "t", 0, std::slice::from_ref(&late), (1, 1))
            .await
            .unwrap()
            .remove(0);
        assert!(meta_early.min_ts == 1000 && meta_early.max_ts == 1000);
        assert!(meta_late.min_ts == 5000 && meta_late.max_ts == 5000);
        let mut index = ProfileIndex::new();
        for rec in [&early, &late] {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta_early);
        index.add_block(&meta_late);
        // Drop the block payloads so any attempt to load+scan a batch would error.
        store
            .delete(&Path::from(meta_early.object_key.clone()))
            .await
            .unwrap();
        store
            .delete(&Path::from(meta_late.object_key.clone()))
            .await
            .unwrap();
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let stats = cold.stats("t", 0, i64::MAX).await.unwrap();

        assert!(
            stats
                == ProfileStats {
                    data_ingested: true,
                    oldest_profile_time: Some(1000),
                    newest_profile_time: Some(5000),
                }
        );

        // A tenant with no blocks reports no data without touching the store.
        let empty = stats_for_unknown_tenant(&cold).await;
        assert!(
            empty
                == ProfileStats {
                    data_ingested: false,
                    oldest_profile_time: None,
                    newest_profile_time: None,
                }
        );
    }

    async fn stats_for_unknown_tenant(cold: &ColdProfileStore) -> ProfileStats {
        cold.stats("absent-tenant", 0, i64::MAX).await.unwrap()
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

    #[tokio::test]
    async fn cold_store_unbounded_label_metadata_uses_index_without_loading_blocks() {
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
        store
            .delete(&Path::from(meta.object_key.clone()))
            .await
            .unwrap();
        let cold = ColdProfileStore::new(store, Arc::new(index));

        let types = cold.profile_types("t", 0, i64::MAX).await.unwrap();
        let names = cold.label_names("t", &[], 0, i64::MAX).await.unwrap();
        let values = cold
            .label_values("t", "service_name", &[], 0, i64::MAX)
            .await
            .unwrap();
        let series = cold
            .series("t", &[], &["service_name".to_string()], 0, i64::MAX)
            .await
            .unwrap();

        check!(types == vec![PT.to_string()], "{types:?}");
        check!(names.contains(&"service_name".to_string()), "{names:?}");
        check!(
            values == vec!["api".to_string(), "worker".to_string()],
            "{values:?}"
        );
        check!(
            series
                == vec![
                    vec![("service_name".to_string(), "api".to_string())],
                    vec![("service_name".to_string(), "worker".to_string())]
                ],
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

    #[test]
    fn filter_and_remap_batch_selects_and_remaps_in_one_pass() {
        use crabka_blockstore::{ProfileSampleRow, encode_profile_samples};

        let fp_keep = 7_u64;
        let fp_drop = 99_u64;
        let rows = vec![
            // keep: matching fp, type, in range; partition 0
            ProfileSampleRow {
                series_fingerprint: fp_keep,
                timestamp: 1_000,
                profile_type: PT.to_string(),
                stacktrace_id: 1,
                value: 10,
                stacktrace_partition: 0,
                total_value: 10,
                span_id: None,
                trace_id: None,
            },
            // drop: wrong fingerprint
            ProfileSampleRow {
                series_fingerprint: fp_drop,
                timestamp: 1_000,
                profile_type: PT.to_string(),
                stacktrace_id: 2,
                value: 5,
                stacktrace_partition: 0,
                total_value: 5,
                span_id: None,
                trace_id: None,
            },
            // drop: out of time range
            ProfileSampleRow {
                series_fingerprint: fp_keep,
                timestamp: 9_999,
                profile_type: PT.to_string(),
                stacktrace_id: 3,
                value: 5,
                stacktrace_partition: 0,
                total_value: 5,
                span_id: None,
                trace_id: None,
            },
            // keep: matching, distinct partition 1 to verify per-row remap
            ProfileSampleRow {
                series_fingerprint: fp_keep,
                timestamp: 2_000,
                profile_type: PT.to_string(),
                stacktrace_id: 4,
                value: 20,
                stacktrace_partition: 1,
                total_value: 20,
                span_id: None,
                trace_id: None,
            },
        ];
        let batch = encode_profile_samples(&rows).unwrap();

        let partition_base = 1_u64 << 32;
        // Dense per-block map: stored partitions {0, 1} -> {base|0, base|1}.
        let partition_map = BTreeMap::from([(0_u64, partition_base), (1_u64, partition_base | 1)]);
        let fps = BTreeSet::from([fp_keep]);
        let out = filter_and_remap_batch(&batch, &partition_map, &fps, PT, 0, 5_000).unwrap();

        // Two surviving rows (the partition-0 and partition-1 keeps).
        assert!(out.num_rows() == 2);
        let out_fps = out.column(0).as_primitive::<UInt64Type>();
        let out_partitions = out.column(5).as_primitive::<UInt64Type>();
        check!(out_fps.value(0) == fp_keep);
        check!(out_fps.value(1) == fp_keep);
        // Partitions remapped once over the whole batch: base|local preserved
        // per surviving row.
        check!(out_partitions.value(0) == partition_base);
        check!(out_partitions.value(1) == (partition_base | 1));
        // Schema matches the canonical samples schema (consumed by the MemTable).
        check!(out.schema() == profile_samples_schema());
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
