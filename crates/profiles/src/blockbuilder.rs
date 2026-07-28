//! Block-builder helpers for WAL records -> profile sample blocks.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    sync::Arc,
    time::Instant,
};

use arrow::record_batch::RecordBatch;
use crabka_blockstore::{
    BlockIndex, BlockMeta, Labels, ProfileIndex, ProfileSampleRow, encode_profile_samples,
};
use crabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord};
use crabka_pprof::{FunctionRec, LineRec, LocationRec, MappingRec, MappingSymbolization, SymbolDb};
use crabka_units::{
    ByteSize, Time, convert::StdDurationExt as _, kibibytes, mebibytes, millis, secs,
};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use parquet::arrow::ArrowWriter;
use tracing::Instrument as _;

use crate::{
    error::ProfilesError,
    metrics::ServiceMetrics,
    wal::{PROFILES_WAL_TOPIC, ProfileRecord, WalMapping, WalSymbolSet},
};

pub const STACKTRACE_PARTITION: u64 = 0;
/// Scale between the epoch-nanosecond timestamps the WAL carries and the
/// epoch-millisecond timestamps blocks are indexed by.
///
/// This is instant arithmetic, not an extent, so it deliberately stays exact
/// integer division: an absolute nanosecond timestamp is ~1.8e18 and cannot
/// round-trip through the `f64` seconds a `Time` stores.
const NANOS_PER_MILLI: i64 = 1_000_000;
/// Fetch budget for one WAL poll across all assigned partitions.
const WAL_FETCH_MAX: ByteSize = mebibytes(2);
/// Fetch budget for one WAL poll per partition.
const WAL_FETCH_PARTITION_MAX: ByteSize = kibibytes(256);
pub const DEFAULT_FLUSH_RECORDS: usize = 1024;
pub const DEFAULT_FLUSH_MAX_AGE: Time = secs(10);

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
    /// Flush the accumulated buffer once the oldest buffered record reaches this age.
    pub flush_max_age: Time,
    /// How long each WAL poll waits for records.
    pub poll_timeout: Time,
    /// Optional self-instrumentation metrics. When set, the block-builder bumps
    /// `crabka_profiles_blocks_built_total` by the number of blocks each flush
    /// wrote. `None` (the default) disables metric emission, keeping the
    /// block-builder usable without a metrics registry (tests, `run()`).
    pub metrics: Option<ServiceMetrics>,
}

impl BlockBuilderConfig {
    #[must_use]
    pub fn new(bootstrap: String, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            bootstrap,
            group_id: "crabka-profiles-block-builder".to_string(),
            store,
            index_key: "index/profiles.json".to_string(),
            flush_records: DEFAULT_FLUSH_RECORDS,
            flush_max_age: DEFAULT_FLUSH_MAX_AGE,
            poll_timeout: millis(500),
            metrics: None,
        }
    }

    /// Attach a [`ServiceMetrics`] bundle so the block-builder emits
    /// `crabka_profiles_blocks_built_total`.
    #[must_use]
    pub fn with_metrics(mut self, metrics: ServiceMetrics) -> Self {
        self.metrics = Some(metrics);
        self
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

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
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

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
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

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
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

#[derive(Debug)]
struct ConsumerRecordAccumulator {
    records: Vec<ConsumerRecord>,
    oldest_record_at: Option<Instant>,
    flush_records: usize,
    flush_max_age: Time,
}

impl ConsumerRecordAccumulator {
    fn new(flush_records: usize, flush_max_age: Time) -> Self {
        Self {
            records: Vec::new(),
            oldest_record_at: None,
            flush_records: flush_records.max(1),
            flush_max_age,
        }
    }

    fn push(&mut self, mut records: Vec<ConsumerRecord>, now: Instant) {
        if records.is_empty() {
            return;
        }
        self.oldest_record_at.get_or_insert(now);
        self.records.append(&mut records);
    }

    fn should_flush(&self, now: Instant) -> bool {
        if self.records.is_empty() {
            return false;
        }
        if self.records.len() >= self.flush_records {
            return true;
        }
        self.oldest_record_at.is_some_and(|oldest| {
            now.saturating_duration_since(oldest).as_time() >= self.flush_max_age
        })
    }

    fn take(&mut self) -> Vec<ConsumerRecord> {
        self.oldest_record_at = None;
        std::mem::take(&mut self.records)
    }
}

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn run_with_config(config: BlockBuilderConfig) -> Result<(), ProfilesError> {
    let mut index = match ProfileIndex::load_latest_snapshot(&config.store, &config.index_key).await
    {
        Ok(index) => index,
        Err(_) => ProfileIndex::new(),
    };
    let mut consumer = Consumer::builder()
        .bootstrap(config.bootstrap)
        .group_id(config.group_id.clone())
        .group_instance_id(config.group_id)
        .fetch_max(WAL_FETCH_MAX)
        .fetch_partition_max(WAL_FETCH_PARTITION_MAX)
        .subscribe(vec![PROFILES_WAL_TOPIC.to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .map_err(|err| ProfilesError::Block(format!("consumer build failed: {err}")))?;

    let mut accumulator =
        ConsumerRecordAccumulator::new(config.flush_records, config.flush_max_age);
    loop {
        let records = consumer
            .poll(config.poll_timeout)
            .await
            .map_err(|err| ProfilesError::Block(format!("consumer poll failed: {err}")))?;
        let now = Instant::now();
        accumulator.push(records, now);
        if !accumulator.should_flush(now) {
            continue;
        }
        let records = accumulator.take();
        // ONE consumer span per poll batch (not per record). Re-parent it onto
        // the ingest span of a record carrying `traceparent`, stitching the
        // block-build stage onto the distributed trace that produced the WAL.
        let build_span = tracing::info_span!(
            "profiles_block_build",
            otel.kind = "consumer",
            crabka.wal.records = records.len(),
        );
        if let Some(rec) = records
            .iter()
            .find(|rec| rec.headers.iter().any(|h| h.key == "traceparent"))
        {
            crabka_telemetry::propagation::set_remote_parent(
                &build_span,
                rec.headers
                    .iter()
                    .map(|h| (h.key.as_str(), h.value.as_deref().unwrap_or(&[][..]))),
            );
        }
        async {
            let metas = flush_consumer_records_with_index(
                &config.store,
                &mut index,
                &records,
                config.flush_records,
            )
            .await?;
            if let Some(metrics) = &config.metrics {
                metrics.record_blocks_built(metas.len() as u64);
            }
            index
                .save_latest_snapshot(&config.store, &config.index_key)
                .await
                .map_err(|err| ProfilesError::Block(err.to_string()))?;
            consumer
                .commit_sync()
                .await
                .map_err(|err| ProfilesError::Block(format!("consumer commit failed: {err}")))?;
            Ok::<(), ProfilesError>(())
        }
        .instrument(build_span)
        .await?;
    }
}

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn run() -> Result<(), ProfilesError> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    run_with_config(BlockBuilderConfig::new("127.0.0.1:9092".to_string(), store)).await
}

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn flush_consumer_records(
    store: &Arc<dyn ObjectStore>,
    records: &[ConsumerRecord],
    flush_records: usize,
) -> Result<Vec<BlockMeta>, ProfilesError> {
    let mut index = ProfileIndex::new();
    flush_consumer_records_with_index(store, &mut index, records, flush_records).await
}

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
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
        symbolization: MappingSymbolization::from_parts((
            mapping.has_functions.get(),
            mapping.has_filenames.get(),
            mapping.has_line_numbers.get(),
            mapping.has_inline_frames.get(),
        )),
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

    use assert2::{assert, check};
    use bytes::Bytes;
    use crabka_client_consumer::ConsumerRecord;
    use crabka_pprof::SymbolDb;
    use crabka_units::{convert::TimeExt as _, minutes};
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};

    use super::*;
    use crate::wal::{ProfileRecord, WalMapping, WalSample, WalSymbolSet};

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
    fn mapping_rec_maps_each_flag_from_its_own_source_field() {
        // Each flag distinct so a wrong source assignment (e.g. all from
        // has_functions) is caught.
        let mapping = WalMapping {
            memory_start: 0x1000,
            memory_limit: 0x2000,
            file_offset: 0x10,
            filename: 1,
            build_id: 2,
            has_functions: true.into(),
            has_filenames: false.into(),
            has_line_numbers: true.into(),
            has_inline_frames: false.into(),
        };
        let strings = [0_u32, 10, 20];

        let rec = mapping_rec(&mapping, &strings);

        assert!(
            rec == MappingRec {
                memory_start: 0x1000,
                memory_limit: 0x2000,
                file_offset: 0x10,
                filename: 10,
                build_id: 20,
                symbolization: MappingSymbolization::from_parts((true, false, true, false)),
            }
        );

        // And the inverse pattern, to ensure no field is hard-wired.
        let inverted = WalMapping {
            has_functions: false.into(),
            has_filenames: true.into(),
            has_line_numbers: false.into(),
            has_inline_frames: true.into(),
            ..mapping
        };
        let rec = mapping_rec(&inverted, &strings);
        assert!(
            rec == MappingRec {
                memory_start: 0x1000,
                memory_limit: 0x2000,
                file_offset: 0x10,
                filename: 10,
                build_id: 20,
                symbolization: MappingSymbolization::from_parts((false, true, false, true)),
            }
        );
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
        check!(metas[0].tenant == "t");
        check!(metas[0].row_count == 2);
        check!(metas[0].min_ts == 1_700_000_000_000);
        check!(metas[0].max_ts == 1_700_000_000_000);
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

        check!(metas.len() == 2);
        for (tenant, row_count) in [("t", 2), ("u", 1)] {
            check!(
                metas
                    .iter()
                    .any(|meta| meta.tenant == tenant && meta.row_count == row_count)
            );
        }
        for meta in metas {
            assert!(
                store
                    .head(&object_store::path::Path::from(meta.object_key))
                    .await
                    .is_ok()
            );
        }
        check!(index.profile_types("t") == vec!["process_cpu:cpu:nanoseconds:cpu:nanoseconds"]);
        for tenant in ["t", "u"] {
            check!(BlockIndex::block_count(&index, tenant) == 1);
        }
    }

    #[test]
    fn accumulator_flushes_on_record_threshold() {
        let mut accumulator = ConsumerRecordAccumulator::new(2, minutes(1));
        let start = Instant::now();

        accumulator.push(vec![consumer_record(0, 10, rec("cpu", 5))], start);
        assert!(!accumulator.should_flush(start));

        accumulator.push(
            vec![consumer_record(0, 11, rec("cpu", 7))],
            start + millis(1).to_std(),
        );
        check!(accumulator.should_flush(start + millis(1).to_std()));
        check!(accumulator.take().len() == 2);
        check!(!accumulator.should_flush(start + minutes(2).to_std()));
    }

    #[test]
    fn accumulator_flushes_on_max_age() {
        let mut accumulator = ConsumerRecordAccumulator::new(100, secs(10));
        let start = Instant::now();

        accumulator.push(vec![consumer_record(0, 10, rec("cpu", 5))], start);
        assert!(!accumulator.should_flush(start + secs(9).to_std()));
        assert!(accumulator.should_flush(start + secs(10).to_std()));
    }

    fn consumer_record(partition: i32, offset: i64, record: ProfileRecord) -> ConsumerRecord {
        let value = Bytes::from(record.encode().unwrap());
        drop(record);
        ConsumerRecord {
            topic: PROFILES_WAL_TOPIC.to_string(),
            partition,
            offset,
            leader_epoch: -1,
            timestamp: 0,
            key: None,
            value: Some(value),
            headers: Vec::new(),
        }
    }
}
