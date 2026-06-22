//! Block-builder helpers for WAL records -> profile sample blocks.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use crabka_blockstore::{
    BlockMeta, ProfileIndex, ProfileSampleRow, SeriesLabels as Labels, SignalBlockIndex,
    encode_profile_samples,
};
use crabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord};
use crabka_pprof::{FunctionRec, LineRec, LocationRec, MappingRec, SymbolDb};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;

use crate::error::ProfilesError;
use crate::wal::{PROFILES_WAL_TOPIC, ProfileRecord, WalMapping, WalSymbolSet};

pub const STACKTRACE_PARTITION: u64 = 0;
const NANOS_PER_MILLI: i64 = 1_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuiltSample {
    pub series_fingerprint: u64,
    pub timestamp_ns: i64,
    pub profile_type: String,
    pub stacktrace_id: u64,
    pub value: i64,
    pub stacktrace_partition: u64,
    pub total_value: i64,
    pub span_id: Option<u64>,
    pub trace_id: Option<Vec<u8>>,
}

#[derive(Clone)]
pub struct BlockBuilderConfig {
    pub bootstrap: String,
    pub group_id: String,
    pub store: Arc<dyn ObjectStore>,
    pub index_key: String,
    pub flush_records: usize,
    pub poll_timeout: Duration,
}

impl BlockBuilderConfig {
    #[must_use]
    pub fn new(bootstrap: String, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            bootstrap,
            group_id: "crabka-profiles-block-builder".to_string(),
            store,
            index_key: "index/profiles.json".to_string(),
            flush_records: 1024,
            poll_timeout: Duration::from_millis(500),
        }
    }
}

#[must_use]
pub fn object_key(
    tenant: &str,
    partition: i32,
    min_offset: i64,
    max_offset: i64,
    min_ts: i64,
    max_ts: i64,
) -> String {
    format!(
        "blocks/{tenant}/{partition:05}/{min_offset:020}-{max_offset:020}-{min_ts}-{max_ts}.parquet"
    )
}

pub fn intern_record(symdb: &mut SymbolDb, rec: &ProfileRecord) -> Result<Vec<u32>, ProfilesError> {
    let symbols = intern_symbols(symdb, &rec.symbols)?;
    rec.samples
        .iter()
        .map(|sample| {
            let stack = sample
                .stacktrace_location_refs
                .iter()
                .map(|location_ref| remap_ref(*location_ref, &symbols.locations))
                .collect::<Vec<_>>();
            Ok(symdb.intern_stacktrace(STACKTRACE_PARTITION, &stack))
        })
        .collect()
}

#[must_use]
pub fn profile_timestamp_ms(timestamp_ns: i64) -> i64 {
    timestamp_ns.div_euclid(NANOS_PER_MILLI)
}

pub fn samples_batch(rows: &[BuiltSample]) -> Result<RecordBatch, ProfilesError> {
    let rows = rows
        .iter()
        .map(|row| ProfileSampleRow {
            series_fingerprint: row.series_fingerprint,
            timestamp: row.timestamp_ns,
            profile_type: row.profile_type.clone(),
            stacktrace_id: row.stacktrace_id,
            value: row.value,
            stacktrace_partition: row.stacktrace_partition,
            total_value: row.total_value,
            span_id: row.span_id,
            trace_id: row.trace_id.clone(),
        })
        .collect::<Vec<_>>();
    encode_profile_samples(&rows).map_err(|err| ProfilesError::Block(err.to_string()))
}

pub async fn build_block(
    store: &Arc<dyn ObjectStore>,
    tenant: &str,
    partition: i32,
    records: &[ProfileRecord],
    offset_range: (i64, i64),
) -> Result<Vec<BlockMeta>, ProfilesError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    let mut symdb = SymbolDb::new();
    let mut rows = Vec::new();
    let mut fingerprints = BTreeSet::new();
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;

    for rec in records {
        let stack_ids = intern_record(&mut symdb, rec)?;
        let fp = rec.series_fingerprint();
        fingerprints.insert(fp);
        let total_value = rec.samples.iter().map(|sample| sample.value).sum();
        for (sample, stack_id) in rec.samples.iter().zip(stack_ids) {
            let timestamp_ms = profile_timestamp_ms(sample.timestamp_ns);
            min_ts = min_ts.min(timestamp_ms);
            max_ts = max_ts.max(timestamp_ms);
            rows.push(BuiltSample {
                series_fingerprint: fp,
                timestamp_ns: timestamp_ms,
                profile_type: rec.profile_type.clone(),
                stacktrace_id: u64::from(stack_id),
                value: sample.value,
                stacktrace_partition: STACKTRACE_PARTITION,
                total_value,
                span_id: sample.span_id,
                trace_id: sample.trace_id.clone(),
            });
        }
    }

    let key = object_key(
        tenant,
        partition,
        offset_range.0,
        offset_range.1,
        min_ts,
        max_ts,
    );
    let batch = samples_batch(&rows)?;
    let mut bytes = Cursor::new(Vec::new());
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None)
            .map_err(|err| ProfilesError::Block(err.to_string()))?;
        writer
            .write(&batch)
            .map_err(|err| ProfilesError::Block(err.to_string()))?;
        writer
            .close()
            .map_err(|err| ProfilesError::Block(err.to_string()))?;
    }

    store
        .put(
            &Path::from(key.clone()),
            PutPayload::from(bytes.into_inner()),
        )
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
    store
        .put(
            &Path::from(format!("{key}.symdb")),
            PutPayload::from(symdb.encode()),
        )
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;

    Ok(vec![BlockMeta {
        tenant: tenant.to_string(),
        object_key: key,
        min_ts,
        max_ts,
        row_count: rows.len(),
        fingerprints: fingerprints.into_iter().collect(),
    }])
}

pub async fn run_with_config(config: BlockBuilderConfig) -> Result<(), ProfilesError> {
    let mut index = match ProfileIndex::load(&config.store, &config.index_key).await {
        Ok(index) => index,
        Err(_) => ProfileIndex::new(),
    };
    let mut consumer = Consumer::builder()
        .bootstrap(config.bootstrap)
        .group_id(config.group_id)
        .subscribe(vec![PROFILES_WAL_TOPIC.to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .map_err(|err| ProfilesError::Block(format!("consumer build failed: {err}")))?;

    loop {
        let records = consumer
            .poll(config.poll_timeout)
            .await
            .map_err(|err| ProfilesError::Block(format!("consumer poll failed: {err}")))?;
        if records.is_empty() {
            continue;
        }
        flush_consumer_records_with_index(
            &config.store,
            &mut index,
            &records,
            config.flush_records,
        )
        .await?;
        index
            .save(&config.store, &config.index_key)
            .await
            .map_err(|err| ProfilesError::Block(err.to_string()))?;
        consumer
            .commit_sync()
            .await
            .map_err(|err| ProfilesError::Block(format!("consumer commit failed: {err}")))?;
    }
}

pub async fn run() -> Result<(), ProfilesError> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    run_with_config(BlockBuilderConfig::new("127.0.0.1:9092".to_string(), store)).await
}

pub async fn flush_consumer_records(
    store: &Arc<dyn ObjectStore>,
    records: &[ConsumerRecord],
    flush_records: usize,
) -> Result<Vec<BlockMeta>, ProfilesError> {
    let mut index = ProfileIndex::new();
    flush_consumer_records_with_index(store, &mut index, records, flush_records).await
}

pub async fn flush_consumer_records_with_index(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    records: &[ConsumerRecord],
    flush_records: usize,
) -> Result<Vec<BlockMeta>, ProfilesError> {
    let mut batches: BTreeMap<(String, i32), Vec<(i64, ProfileRecord)>> = BTreeMap::new();
    for record in records {
        let value = record
            .value
            .as_deref()
            .ok_or_else(|| ProfilesError::Wal("profiles WAL record has no value".to_string()))?;
        let decoded = ProfileRecord::decode(value)?;
        let labels = Labels::from_pairs(decoded.labels.iter().cloned());
        index.add_series(&decoded.tenant, labels.fingerprint(), &labels);
        batches
            .entry((decoded.tenant.clone(), record.partition))
            .or_default()
            .push((record.offset, decoded));
    }

    let mut metas = Vec::new();
    for ((tenant, partition), mut records) in batches {
        records.sort_by_key(|(offset, _)| *offset);
        for chunk in records.chunks(flush_records.max(1)) {
            let min_offset = chunk.first().map(|(offset, _)| *offset).unwrap_or_default();
            let max_offset = chunk.last().map(|(offset, _)| *offset).unwrap_or_default();
            let profile_records = chunk
                .iter()
                .map(|(_, record)| record.clone())
                .collect::<Vec<_>>();
            let built = build_block(
                store,
                &tenant,
                partition,
                &profile_records,
                (min_offset, max_offset),
            )
            .await?;
            for meta in &built {
                index.add_block(meta);
                index.add_profile_block(&meta.tenant, &meta.object_key, vec![STACKTRACE_PARTITION]);
            }
            metas.extend(built);
        }
    }
    Ok(metas)
}

struct SymbolRefs {
    locations: Vec<u32>,
}

fn intern_symbols(
    symdb: &mut SymbolDb,
    symbols: &WalSymbolSet,
) -> Result<SymbolRefs, ProfilesError> {
    let strings = symbols
        .strings
        .iter()
        .map(|value| symdb.intern_string(value))
        .collect::<Vec<_>>();

    let mappings = symbols
        .mappings
        .iter()
        .map(|mapping| symdb.intern_mapping(mapping_rec(mapping, &strings)))
        .collect::<Vec<_>>();

    let functions = symbols
        .functions
        .iter()
        .map(|function| {
            symdb.intern_function(FunctionRec {
                name: remap_ref(function.name, &strings),
                system_name: remap_ref(function.system_name, &strings),
                filename: remap_ref(function.filename, &strings),
                start_line: function.start_line,
            })
        })
        .collect::<Vec<_>>();

    let locations = symbols
        .locations
        .iter()
        .map(|location| {
            let location = LocationRec {
                address: location.address,
                mapping_id: remap_ref(location.mapping_id, &mappings),
                lines: location
                    .lines
                    .iter()
                    .map(|(function_id, line)| {
                        Ok(LineRec {
                            function_id: remap_ref(*function_id, &functions),
                            line: i32::try_from(*line).map_err(|err| {
                                ProfilesError::Block(format!("line number does not fit i32: {err}"))
                            })?,
                        })
                    })
                    .collect::<Result<Vec<_>, ProfilesError>>()?,
            };
            Ok(symdb.intern_location(location))
        })
        .collect::<Result<Vec<_>, ProfilesError>>()?;

    Ok(SymbolRefs { locations })
}

fn mapping_rec(mapping: &WalMapping, strings: &[u32]) -> MappingRec {
    MappingRec {
        memory_start: mapping.memory_start,
        memory_limit: mapping.memory_limit,
        file_offset: mapping.file_offset,
        filename: remap_ref(mapping.filename, strings),
        build_id: remap_ref(mapping.build_id, strings),
        has_functions: mapping.has_functions,
        has_filenames: mapping.has_functions,
        has_line_numbers: mapping.has_functions,
        has_inline_frames: mapping.has_functions,
    }
}

fn remap_ref(reference: u32, table: &[u32]) -> u32 {
    usize::try_from(reference)
        .ok()
        .and_then(|idx| table.get(idx))
        .copied()
        .or_else(|| {
            reference
                .checked_sub(1)
                .and_then(|idx| usize::try_from(idx).ok())
                .and_then(|idx| table.get(idx))
                .copied()
        })
        .unwrap_or(reference)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use bytes::Bytes;
    use crabka_client_consumer::ConsumerRecord;
    use crabka_pprof::SymbolDb;
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt};

    use super::*;
    use crate::wal::{ProfileRecord, WalSample, WalSymbolSet};

    fn rec(name: &str, value: i64) -> ProfileRecord {
        ProfileRecord {
            tenant: "t".into(),
            labels: vec![
                ("__name__".into(), name.into()),
                ("service_name".into(), "api".into()),
                (
                    "__profile_type__".into(),
                    "process_cpu:cpu:nanoseconds:cpu:nanoseconds".into(),
                ),
            ],
            profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".into(),
            samples: vec![WalSample {
                stacktrace_location_refs: vec![0, 1],
                value,
                timestamp_ns: 1_700_000_000_000_000_000,
                span_id: None,
                trace_id: None,
            }],
            symbols: WalSymbolSet {
                strings: vec![String::new(), "a".into(), "b".into()],
                functions: vec![],
                locations: vec![],
                mappings: vec![],
            },
        }
    }

    #[test]
    fn object_key_is_deterministic() {
        let a = object_key("t", 0, 10, 20, 100, 200);
        let b = object_key("t", 0, 10, 20, 100, 200);
        let c = object_key("t", 0, 10, 21, 100, 200);

        assert!(a == b);
        assert!(a != c);
    }

    #[test]
    fn intern_record_dedups_identical_stacks() {
        let mut symdb = SymbolDb::default();
        let r = rec("cpu", 5);

        let ids1 = intern_record(&mut symdb, &r).unwrap();
        let ids2 = intern_record(&mut symdb, &r).unwrap();

        assert!(ids1 == ids2);
    }

    #[test]
    fn samples_batch_matches_profile_schema() {
        let batch = samples_batch(&[BuiltSample {
            series_fingerprint: 1,
            timestamp_ns: 100,
            profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".into(),
            stacktrace_id: 7,
            value: 5,
            stacktrace_partition: 0,
            total_value: 5,
            span_id: None,
            trace_id: None,
        }])
        .unwrap();

        assert!(batch.schema() == crabka_blockstore::profile_samples_schema());
        assert!(batch.num_rows() == 1);
    }

    #[tokio::test]
    async fn build_block_writes_samples_and_symdb() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let records = vec![rec("cpu", 5), rec("cpu", 7)];

        let metas = build_block(&store, "t", 0, &records, (10, 20))
            .await
            .unwrap();

        assert!(metas.len() == 1);
        assert!(metas[0].tenant == "t");
        assert!(metas[0].row_count == 2);
        assert!(metas[0].min_ts == 1_700_000_000_000);
        assert!(metas[0].max_ts == 1_700_000_000_000);
        let symdb_key = format!("{}.symdb", metas[0].object_key);
        assert!(
            store
                .head(&object_store::path::Path::from(symdb_key))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn flush_consumer_records_groups_by_tenant_and_partition() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut index = ProfileIndex::new();
        let mut tenant_b = rec("cpu", 11);
        tenant_b.tenant = "u".to_string();
        let records = vec![
            consumer_record(0, 10, rec("cpu", 5)),
            consumer_record(0, 11, rec("cpu", 7)),
            consumer_record(1, 3, tenant_b),
        ];

        let metas = flush_consumer_records_with_index(&store, &mut index, &records, 100)
            .await
            .unwrap();

        assert!(metas.len() == 2);
        assert!(
            metas
                .iter()
                .any(|meta| meta.tenant == "t" && meta.row_count == 2)
        );
        assert!(
            metas
                .iter()
                .any(|meta| meta.tenant == "u" && meta.row_count == 1)
        );
        for meta in metas {
            assert!(
                store
                    .head(&object_store::path::Path::from(meta.object_key))
                    .await
                    .is_ok()
            );
        }
        assert!(index.profile_types("t") == vec!["process_cpu:cpu:nanoseconds:cpu:nanoseconds"]);
        assert!(index.block_count("t") == 1);
        assert!(index.block_count("u") == 1);
    }

    fn consumer_record(partition: i32, offset: i64, record: ProfileRecord) -> ConsumerRecord {
        ConsumerRecord {
            topic: PROFILES_WAL_TOPIC.to_string(),
            partition,
            offset,
            leader_epoch: -1,
            timestamp: 0,
            key: None,
            value: Some(Bytes::from(record.encode().unwrap())),
            headers: Vec::new(),
        }
    }
}
