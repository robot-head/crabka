//! Block-builder helpers for WAL records -> profile sample blocks.

use std::collections::BTreeSet;
use std::io::Cursor;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use crabka_blockstore::{BlockMeta, ProfileSampleRow, encode_profile_samples};
use crabka_pprof::{FunctionRec, LineRec, LocationRec, MappingRec, SymbolDb};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;

use crate::error::ProfilesError;
use crate::wal::{ProfileRecord, WalMapping, WalSymbolSet};

pub const STACKTRACE_PARTITION: u64 = 0;

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
            min_ts = min_ts.min(sample.timestamp_ns);
            max_ts = max_ts.max(sample.timestamp_ns);
            rows.push(BuiltSample {
                series_fingerprint: fp,
                timestamp_ns: sample.timestamp_ns,
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

pub async fn run() -> Result<(), ProfilesError> {
    Err(ProfilesError::Block(
        "block-builder consumer wiring waits for the role binary".to_string(),
    ))
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
        let symdb_key = format!("{}.symdb", metas[0].object_key);
        assert!(
            store
                .head(&object_store::path::Path::from(symdb_key))
                .await
                .is_ok()
        );
    }
}
