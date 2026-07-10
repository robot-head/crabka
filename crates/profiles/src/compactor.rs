//! Profile block compaction.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, AsArray, BinaryArray, UInt64Array},
    datatypes::{Int32Type, Int64Type, UInt64Type},
    record_batch::RecordBatch,
};
use crabka_blockstore::{
    BlockMeta, COL_FINGERPRINT, COL_TIMESTAMP, PCOL_PROFILE_TYPE, PCOL_SPAN_ID, PCOL_STACKTRACE_ID,
    PCOL_STACKTRACE_PARTITION, PCOL_TOTAL_VALUE, PCOL_TRACE_ID, PCOL_VALUE, ProfileIndex,
    ProfileSampleRow, encode_profile_samples,
};
use crabka_pprof::SymbolDb;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

use crate::{blockbuilder::STACKTRACE_PARTITION, error::ProfilesError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionJob {
    pub tenant: String,
    pub input_keys: Vec<String>,
    pub output_key: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DownsamplePolicy {
    pub resolution_ns: i64,
}

#[must_use]
pub fn plan_compactions(index: &ProfileIndex, max_blocks_per_job: usize) -> Vec<CompactionJob> {
    let max_blocks_per_job = max_blocks_per_job.max(2);
    let mut by_tenant: BTreeMap<String, Vec<BlockMeta>> = BTreeMap::new();
    for block in index.all_blocks() {
        by_tenant
            .entry(block.tenant.clone())
            .or_default()
            .push(block);
    }
    let mut jobs = Vec::new();
    for (tenant, mut blocks) in by_tenant {
        blocks.sort_by(|left, right| {
            left.min_ts
                .cmp(&right.min_ts)
                .then_with(|| left.max_ts.cmp(&right.max_ts))
                .then_with(|| left.object_key.cmp(&right.object_key))
        });
        for chunk in blocks.chunks(max_blocks_per_job) {
            if chunk.len() < 2 {
                continue;
            }
            let input_keys = chunk
                .iter()
                .map(|block| block.object_key.clone())
                .collect::<Vec<_>>();
            jobs.push(CompactionJob {
                tenant: tenant.clone(),
                output_key: compacted_key(&tenant, chunk, &input_keys),
                input_keys,
            });
        }
    }
    jobs
}

pub async fn compact_once(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    max_blocks_per_job: usize,
) -> Result<Vec<BlockMeta>, ProfilesError> {
    compact_once_with_policy(store, index, max_blocks_per_job, None).await
}

pub async fn compact_once_with_policy(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    max_blocks_per_job: usize,
    downsample: Option<DownsamplePolicy>,
) -> Result<Vec<BlockMeta>, ProfilesError> {
    let jobs = plan_compactions(index, max_blocks_per_job);
    let mut metas = Vec::new();
    for job in jobs {
        metas.push(
            compact_blocks_with_policy(
                store,
                index,
                &job.tenant,
                &job.input_keys,
                &job.output_key,
                downsample,
            )
            .await?,
        );
    }
    Ok(metas)
}

pub async fn compact_blocks(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    tenant: &str,
    input_keys: &[String],
    output_key: &str,
) -> Result<BlockMeta, ProfilesError> {
    compact_blocks_with_policy(store, index, tenant, input_keys, output_key, None).await
}

pub async fn compact_blocks_with_policy(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    tenant: &str,
    input_keys: &[String],
    output_key: &str,
    downsample: Option<DownsamplePolicy>,
) -> Result<BlockMeta, ProfilesError> {
    if input_keys.len() < 2 {
        return Err(ProfilesError::Block(
            "compaction requires at least two input blocks".to_string(),
        ));
    }

    let mut out_batches = Vec::new();
    let mut out_symbols = SymbolDb::new();
    let mut out_partitions = BTreeSet::new();
    let mut fingerprints = BTreeSet::new();
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;

    for (block_idx, block_key) in input_keys.iter().enumerate() {
        let source_partitions = source_partitions(index, block_key);
        let partition_map = destination_partitions(block_idx, &source_partitions)?;
        let symdb = load_symdb(store, block_key).await?;
        for (source, dest) in &partition_map {
            out_symbols
                .copy_partition_from(&symdb, *source, *dest)
                .map_err(|err| ProfilesError::Block(err.to_string()))?;
            out_partitions.insert(*dest);
        }

        let batches = load_batches(store, block_key).await?;
        for batch in batches {
            let batch = remap_partitions(&batch, &partition_map)?;
            out_batches.push(batch);
        }
    }

    let out_batches = match downsample {
        Some(policy) => downsample_batches(&out_batches, policy)?,
        None => out_batches,
    };
    let mut row_count = 0_usize;
    for batch in &out_batches {
        collect_meta(batch, &mut fingerprints, &mut min_ts, &mut max_ts);
        row_count += batch.num_rows();
    }

    write_batches(store, output_key, &out_batches).await?;
    store
        .put(
            &Path::from(format!("{output_key}.symdb")),
            PutPayload::from(out_symbols.encode()),
        )
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;

    let meta = BlockMeta {
        tenant: tenant.to_string(),
        object_key: output_key.to_string(),
        min_ts,
        max_ts,
        row_count,
        fingerprints: fingerprints.into_iter().collect(),
    };
    index.replace_profile_blocks(
        tenant,
        input_keys,
        &[(meta.clone(), out_partitions.into_iter().collect())],
    );
    Ok(meta)
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DownsampleKey {
    series_fingerprint: u64,
    timestamp: i64,
    profile_type: String,
    stacktrace_id: u64,
    stacktrace_partition: u64,
    span_id: Option<u64>,
    trace_id: Option<Vec<u8>>,
}

fn downsample_batches(
    batches: &[RecordBatch],
    policy: DownsamplePolicy,
) -> Result<Vec<RecordBatch>, ProfilesError> {
    if policy.resolution_ns <= 0 {
        return Err(ProfilesError::Block(
            "downsample resolution must be positive".to_string(),
        ));
    }

    let mut values: BTreeMap<DownsampleKey, (i64, i64)> = BTreeMap::new();
    for batch in batches {
        let fp_idx = batch.schema().column_with_name(COL_FINGERPRINT).unwrap().0;
        let ts_idx = batch.schema().column_with_name(COL_TIMESTAMP).unwrap().0;
        let profile_idx = batch
            .schema()
            .column_with_name(PCOL_PROFILE_TYPE)
            .unwrap()
            .0;
        let stack_idx = batch
            .schema()
            .column_with_name(PCOL_STACKTRACE_ID)
            .unwrap()
            .0;
        let value_idx = batch.schema().column_with_name(PCOL_VALUE).unwrap().0;
        let partition_idx = batch
            .schema()
            .column_with_name(PCOL_STACKTRACE_PARTITION)
            .unwrap()
            .0;
        let total_idx = batch.schema().column_with_name(PCOL_TOTAL_VALUE).unwrap().0;
        let span_idx = batch.schema().column_with_name(PCOL_SPAN_ID).unwrap().0;
        let trace_idx = batch.schema().column_with_name(PCOL_TRACE_ID).unwrap().0;

        let fingerprints = batch.column(fp_idx).as_primitive::<UInt64Type>();
        let timestamps = batch.column(ts_idx).as_primitive::<Int64Type>();
        let profile_types = batch.column(profile_idx).as_dictionary::<Int32Type>();
        let profile_values = profile_types.values().as_string::<i32>();
        let stacktrace_ids = batch.column(stack_idx).as_primitive::<UInt64Type>();
        let sample_values = batch.column(value_idx).as_primitive::<Int64Type>();
        let partitions = batch.column(partition_idx).as_primitive::<UInt64Type>();
        let total_values = batch.column(total_idx).as_primitive::<Int64Type>();
        let span_ids = batch.column(span_idx).as_primitive::<UInt64Type>();
        let trace_ids = batch
            .column(trace_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| ProfilesError::Block(format!("`{PCOL_TRACE_ID}` must be Binary")))?;

        for row in 0..batch.num_rows() {
            let profile_key = profile_types.keys().value(row);
            let profile_pos = usize::try_from(profile_key).map_err(|err| {
                ProfilesError::Block(format!("profile type key invalid during downsample: {err}"))
            })?;
            let timestamp = timestamps
                .value(row)
                .div_euclid(policy.resolution_ns)
                .saturating_mul(policy.resolution_ns);
            let key = DownsampleKey {
                series_fingerprint: fingerprints.value(row),
                timestamp,
                profile_type: profile_values.value(profile_pos).to_string(),
                stacktrace_id: stacktrace_ids.value(row),
                stacktrace_partition: partitions.value(row),
                span_id: (!span_ids.is_null(row)).then(|| span_ids.value(row)),
                trace_id: (!trace_ids.is_null(row)).then(|| trace_ids.value(row).to_vec()),
            };
            let entry = values.entry(key).or_insert((0, 0));
            entry.0 += sample_values.value(row);
            entry.1 += total_values.value(row);
        }
    }

    let rows = values
        .into_iter()
        .map(|(key, (value, total_value))| ProfileSampleRow {
            series_fingerprint: key.series_fingerprint,
            timestamp: key.timestamp,
            profile_type: key.profile_type,
            stacktrace_id: key.stacktrace_id,
            value,
            stacktrace_partition: key.stacktrace_partition,
            total_value,
            span_id: key.span_id,
            trace_id: key.trace_id,
        })
        .collect::<Vec<_>>();
    encode_profile_samples(&rows)
        .map(|batch| vec![batch])
        .map_err(|err| ProfilesError::Block(err.to_string()))
}

fn compacted_key(tenant: &str, blocks: &[BlockMeta], input_keys: &[String]) -> String {
    let min_ts = blocks
        .iter()
        .map(|block| block.min_ts)
        .min()
        .unwrap_or_default();
    let max_ts = blocks
        .iter()
        .map(|block| block.max_ts)
        .max()
        .unwrap_or_default();
    format!(
        "blocks/{tenant}/compacted/{min_ts}-{max_ts}-{:016x}.parquet",
        fnv1a(input_keys)
    )
}

fn fnv1a(input_keys: &[String]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for key in input_keys {
        for byte in key.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn source_partitions(index: &ProfileIndex, block_key: &str) -> Vec<u64> {
    let mut partitions = index.stacktrace_partitions(block_key);
    if partitions.is_empty() {
        return vec![STACKTRACE_PARTITION];
    }
    // Sort + dedup so the dense local re-basing in `destination_partitions` is
    // deterministic regardless of the order the index recorded partitions in.
    partitions.sort_unstable();
    partitions.dedup();
    partitions
}

/// Build the source-partition -> destination-partition map for one input block.
///
/// The external partition scheme packs a per-block base in the high 32 bits and
/// a local partition id in the low 32 bits: `external = base | local`. That is
/// only collision-free while the local id fits the low 32 bits. After a block
/// has already been compacted once, its stored partitions already occupy the
/// high bits, so naively OR-ing a fresh base onto them folds bits together and
/// can alias partitions across blocks (and trips `copy_partition_from`'s
/// non-empty-destination reject).
///
/// To stay safe across repeated compactions we re-base each block's source
/// partitions to a dense local `0..n` range first, so the high-bit base is only
/// ever OR-ed with small local ids. `source_partitions` is sorted+deduped by the
/// caller, so the dense assignment is deterministic. We also use checked
/// arithmetic and error out rather than silently aliasing if a base or local id
/// would not fit.
fn destination_partitions(
    block_idx: usize,
    source_partitions: &[u64],
) -> Result<BTreeMap<u64, u64>, ProfilesError> {
    let block_base = u64::try_from(block_idx + 1)
        .map_err(|err| ProfilesError::Block(format!("block index does not fit u64: {err}")))?
        .checked_shl(32)
        .ok_or_else(|| {
            ProfilesError::Block(format!("block base for index {block_idx} overflows u64"))
        })?;
    let mut map = BTreeMap::new();
    for (local, source) in source_partitions.iter().enumerate() {
        let local = u64::try_from(local).map_err(|err| {
            ProfilesError::Block(format!("local partition does not fit u64: {err}"))
        })?;
        if local >= 1 << 32 {
            return Err(ProfilesError::Block(format!(
                "local partition {local} does not fit the low 32 bits"
            )));
        }
        // `block_base` is a multiple of `1 << 32` and `local < 1 << 32`, so the
        // low bits are guaranteed clear and OR is equivalent to addition.
        map.insert(*source, block_base | local);
    }
    Ok(map)
}

async fn load_symdb(
    store: &Arc<dyn ObjectStore>,
    block_key: &str,
) -> Result<SymbolDb, ProfilesError> {
    let bytes = store
        .get(&Path::from(format!("{block_key}.symdb")))
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?
        .bytes()
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
    SymbolDb::decode(&bytes).map_err(|err| ProfilesError::Block(err.to_string()))
}

async fn load_batches(
    store: &Arc<dyn ObjectStore>,
    block_key: &str,
) -> Result<Vec<RecordBatch>, ProfilesError> {
    let bytes = store
        .get(&Path::from(block_key))
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?
        .bytes()
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|err| ProfilesError::Block(err.to_string()))?
        .build()
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
    reader
        .map(|batch| batch.map_err(|err| ProfilesError::Block(err.to_string())))
        .collect()
}

fn remap_partitions(
    batch: &RecordBatch,
    partition_map: &BTreeMap<u64, u64>,
) -> Result<RecordBatch, ProfilesError> {
    let partition_idx = batch
        .schema()
        .column_with_name(PCOL_STACKTRACE_PARTITION)
        .ok_or_else(|| {
            ProfilesError::Block(format!("missing `{PCOL_STACKTRACE_PARTITION}` column"))
        })?
        .0;
    let partitions = batch.column(partition_idx).as_primitive::<UInt64Type>();
    let remapped = UInt64Array::from_iter_values((0..batch.num_rows()).map(|row| {
        let partition = partitions.value(row);
        partition_map.get(&partition).copied().unwrap_or(partition)
    }));
    let mut columns = batch.columns().to_vec();
    columns[partition_idx] = Arc::new(remapped) as ArrayRef;
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|err| ProfilesError::Block(err.to_string()))
}

fn collect_meta(
    batch: &RecordBatch,
    fingerprints: &mut BTreeSet<u64>,
    min_ts: &mut i64,
    max_ts: &mut i64,
) {
    let fp_idx = batch.schema().column_with_name(COL_FINGERPRINT).unwrap().0;
    let ts_idx = batch.schema().column_with_name(COL_TIMESTAMP).unwrap().0;
    let fps = batch.column(fp_idx).as_primitive::<UInt64Type>();
    let timestamps = batch.column(ts_idx).as_primitive::<Int64Type>();
    for row in 0..batch.num_rows() {
        fingerprints.insert(fps.value(row));
        *min_ts = (*min_ts).min(timestamps.value(row));
        *max_ts = (*max_ts).max(timestamps.value(row));
    }
}

async fn write_batches(
    store: &Arc<dyn ObjectStore>,
    output_key: &str,
    batches: &[RecordBatch],
) -> Result<(), ProfilesError> {
    let Some(first) = batches.first() else {
        return Err(ProfilesError::Block(
            "cannot compact empty block set".to_string(),
        ));
    };
    let mut bytes = Cursor::new(Vec::new());
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, first.schema(), None)
            .map_err(|err| ProfilesError::Block(err.to_string()))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|err| ProfilesError::Block(err.to_string()))?;
        }
        writer
            .close()
            .map_err(|err| ProfilesError::Block(err.to_string()))?;
    }
    store
        .put(
            &Path::from(output_key),
            PutPayload::from(bytes.into_inner()),
        )
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use crabka_blockstore::{BlockIndex, Labels};
    use crabka_pprof::{EngineOpts, FlameEngine};
    use object_store::{ObjectStore, memory::InMemory};

    use super::*;
    use crate::{
        blockbuilder::build_block,
        cold_store::ColdProfileStore,
        wal::{ProfileRecord, WalSample, WalSymbolSet},
    };

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    #[tokio::test]
    async fn compact_blocks_rewrites_blocks_and_preserves_query_results() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec_a = record("t", "api", 5, "main");
        let rec_b = record("t", "api", 7, "worker");
        let meta_a = build_block(&store, "t", 0, std::slice::from_ref(&rec_a), (0, 0))
            .await
            .unwrap()
            .remove(0);
        let meta_b = build_block(&store, "t", 0, std::slice::from_ref(&rec_b), (1, 1))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in [&rec_a, &rec_b] {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta_a);
        index.add_profile_block("t", &meta_a.object_key, vec![STACKTRACE_PARTITION]);
        index.add_block(&meta_b);
        index.add_profile_block("t", &meta_b.object_key, vec![STACKTRACE_PARTITION]);

        let meta = compact_blocks(
            &store,
            &mut index,
            "t",
            &[meta_a.object_key.clone(), meta_b.object_key.clone()],
            "blocks/t/compacted.parquet",
        )
        .await
        .unwrap();

        assert!(meta.row_count == 2);
        assert!(
            BlockIndex::candidate_blocks(&index, "t", 0, i64::MAX) == vec![meta.object_key.clone()]
        );
        let cold = Arc::new(ColdProfileStore::new(store, Arc::new(index)));
        let engine = FlameEngine::new(cold, EngineOpts::default());
        let fg = engine
            .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();

        check!(fg.total == 12);
        for name in ["main", "worker"] {
            check!(fg.names.iter().any(|frame| frame == name));
        }
    }

    #[tokio::test]
    async fn compact_blocks_can_downsample_rows_into_time_buckets() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec_a = record_at("t", "api", 4, "main", 1_000);
        let rec_b = record_at("t", "api", 6, "main", 1_500);
        let rec_c = record_at("t", "api", 3, "worker", 3_000);
        let meta_a = build_block(&store, "t", 0, &[rec_a.clone(), rec_b.clone()], (0, 1))
            .await
            .unwrap()
            .remove(0);
        let meta_b = build_block(&store, "t", 0, std::slice::from_ref(&rec_c), (2, 2))
            .await
            .unwrap()
            .remove(0);
        let mut index = ProfileIndex::new();
        for rec in [&rec_a, &rec_b, &rec_c] {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
        }
        index.add_block(&meta_a);
        index.add_profile_block("t", &meta_a.object_key, vec![STACKTRACE_PARTITION]);
        index.add_block(&meta_b);
        index.add_profile_block("t", &meta_b.object_key, vec![STACKTRACE_PARTITION]);

        let meta = compact_blocks_with_policy(
            &store,
            &mut index,
            "t",
            &[meta_a.object_key.clone(), meta_b.object_key.clone()],
            "blocks/t/downsampled.parquet",
            Some(DownsamplePolicy {
                resolution_ns: 1_000,
            }),
        )
        .await
        .unwrap();

        assert!(meta.row_count == 2);
        let cold = Arc::new(ColdProfileStore::new(store, Arc::new(index)));
        let engine = FlameEngine::new(cold, EngineOpts::default());
        let fg = engine
            .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();

        assert!(fg.total == 13);
    }

    #[tokio::test]
    async fn recompacting_an_already_compacted_block_does_not_alias_partitions() {
        // Round-trip two compactions: build four fresh blocks, compact them
        // pairwise (so each compacted block has high-bit-based partitions), then
        // compact the two compacted blocks together. Without dense re-basing the
        // second compaction OR-folds the already-high partitions and aliases
        // them across blocks (and `copy_partition_from` rejects the non-empty
        // destination). Query results must be identical before and after.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rec_a = record("t", "api", 5, "alpha");
        let rec_b = record("t", "api", 7, "bravo");
        let rec_c = record("t", "api", 11, "charlie");
        let rec_d = record("t", "api", 13, "delta");
        let mut index = ProfileIndex::new();

        let mut metas = Vec::new();
        for (idx, rec) in [&rec_a, &rec_b, &rec_c, &rec_d].into_iter().enumerate() {
            let labels = Labels::from_pairs(rec.labels.iter().cloned());
            index.add_series("t", labels.fingerprint(), &labels);
            let offset = i64::try_from(idx).unwrap();
            let bounds = (offset, offset);
            let meta = build_block(&store, "t", 0, std::slice::from_ref(rec), bounds)
                .await
                .unwrap()
                .remove(0);
            index.add_block(&meta);
            index.add_profile_block("t", &meta.object_key, vec![STACKTRACE_PARTITION]);
            metas.push(meta);
        }

        // First compaction: a+b -> c1, c+d -> c2.
        let c1 = compact_blocks(
            &store,
            &mut index,
            "t",
            &[metas[0].object_key.clone(), metas[1].object_key.clone()],
            "blocks/t/c1.parquet",
        )
        .await
        .unwrap();
        let c2 = compact_blocks(
            &store,
            &mut index,
            "t",
            &[metas[2].object_key.clone(), metas[3].object_key.clone()],
            "blocks/t/c2.parquet",
        )
        .await
        .unwrap();

        // Query the once-compacted state for the baseline. `ProfileIndex` is
        // not `Clone`, so hand it to an `Arc`, run the query, then reclaim it
        // (the cold store / engine clones are dropped once the query resolves).
        let mut index = {
            let shared = Arc::new(index);
            let cold = Arc::new(ColdProfileStore::new(store.clone(), shared.clone()));
            let engine = FlameEngine::new(cold, EngineOpts::default());
            let before = engine
                .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
                .await
                .unwrap();
            assert!(before.total == 36);
            for name in ["alpha", "bravo", "charlie", "delta"] {
                assert!(
                    before.names.iter().any(|leaf| leaf == name),
                    "{name} missing"
                );
            }
            drop(engine);
            Arc::try_unwrap(shared).unwrap_or_else(|_| panic!("sole owner after query"))
        };
        let before_total = 36_i64;

        // Second compaction: c1 + c2 -> c3 (both inputs already compacted).
        let c3 = compact_blocks(
            &store,
            &mut index,
            "t",
            &[c1.object_key.clone(), c2.object_key.clone()],
            "blocks/t/c3.parquet",
        )
        .await
        .unwrap();
        assert!(c3.row_count == 4);

        // After re-compaction every input partition must survive as a distinct
        // destination partition: four source partitions (two per input block)
        // must produce four distinct destinations with no aliasing.
        let final_partitions = index.stacktrace_partitions(&c3.object_key);
        assert!(final_partitions.len() == 4, "{final_partitions:?}");
        let distinct: BTreeSet<u64> = final_partitions.iter().copied().collect();
        assert!(
            distinct.len() == 4,
            "partitions aliased: {final_partitions:?}"
        );

        // Query results unchanged after the second compaction.
        let cold = Arc::new(ColdProfileStore::new(store, Arc::new(index)));
        let engine = FlameEngine::new(cold, EngineOpts::default());
        let after = engine
            .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .unwrap();
        assert!(after.total == before_total);
        for name in ["alpha", "bravo", "charlie", "delta"] {
            assert!(after.names.iter().any(|leaf| leaf == name), "{name} lost");
        }
    }

    #[test]
    fn destination_partitions_rebases_high_bit_partitions_to_dense_local_ids() {
        // Already-compacted source partitions live in the high bits. Re-basing
        // them onto a fresh block base must produce dense, collision-free
        // destinations rather than OR-folding the high bits together.
        let sources = [1_u64 << 32, 2_u64 << 32, 3_u64 << 32];
        let map = destination_partitions(1, &sources).unwrap();
        let base = 2_u64 << 32;
        assert!(
            map == BTreeMap::from([
                (1_u64 << 32, base),
                (2_u64 << 32, base | 1),
                (3_u64 << 32, base | 2),
            ])
        );
        let dests: BTreeSet<u64> = map.values().copied().collect();
        assert!(dests.len() == 3);
    }

    #[test]
    fn plan_compactions_groups_blocks_by_tenant_in_time_order() {
        let mut index = ProfileIndex::new();
        index.replace_profile_blocks(
            "t",
            &[],
            &[
                (
                    BlockMeta {
                        tenant: "t".to_string(),
                        object_key: "b.parquet".to_string(),
                        min_ts: 10,
                        max_ts: 20,
                        row_count: 1,
                        fingerprints: Vec::new(),
                    },
                    vec![0],
                ),
                (
                    BlockMeta {
                        tenant: "t".to_string(),
                        object_key: "a.parquet".to_string(),
                        min_ts: 0,
                        max_ts: 5,
                        row_count: 1,
                        fingerprints: Vec::new(),
                    },
                    vec![0],
                ),
            ],
        );

        let jobs = plan_compactions(&index, 2);

        assert_eq!(
            (jobs.len(), jobs[0].input_keys.as_slice()),
            (1, &["a.parquet".to_string(), "b.parquet".to_string()][..])
        );
    }

    fn record(tenant: &str, service: &str, value: i64, function: &str) -> ProfileRecord {
        record_at(tenant, service, value, function, 1000)
    }

    fn record_at(
        tenant: &str,
        service: &str,
        value: i64,
        function: &str,
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
                stacktrace_location_refs: vec![0],
                value,
                timestamp_ns,
                span_id: None,
                trace_id: None,
            }],
            symbols: symbols(function),
        }
    }

    fn symbols(function: &str) -> WalSymbolSet {
        WalSymbolSet {
            strings: vec![String::new(), function.to_string()],
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
