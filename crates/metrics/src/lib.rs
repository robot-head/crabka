//! Prometheus/Grafana-Mimir-equivalent metrics backend for Crabka.
//!
//! The crate contains the metrics ingest/data path: Arrow block schemas,
//! native-histogram codecs, `remote_write`/OTLP decode, distributor WAL append,
//! and compactor block/index writes. Query execution lives in `crabka-promql`.

pub mod compactor;
pub mod distributor;
pub mod histogram;
pub mod limits;
pub mod metadata;
pub mod otlp;
pub mod sample;
pub mod schema;
pub mod symbols;
pub mod tenant;
pub mod wal;
pub mod wire;

pub use compactor::{
    CompactedBlockWrite, CompactionBatchResult, CompactionCommitError, CompactionConsumerCommit,
    CompactionConsumerCommitError, CompactionConsumerCommitMut, CompactionConsumerCommitter,
    CompactionConsumerPoll, CompactionConsumerPollError, CompactionConsumerRecordError,
    CompactionIndexError, CompactionIndexManifest, CompactionIndexSink, CompactionLoopConfig,
    CompactionLoopResult, CompactionObjectPlan, CompactionOffsetCommitter,
    CompactionPartitionOffset, CompactionPollError, CompactionPollResult, CompactionSeriesLabels,
    CompactionWalRecord, CompactionWindowError, CompactionWindowResult, CompactionWriteError,
    ExemplarRow, FloatRow, MetricBlockKind, MetricsCompactorBuildError, MetricsCompactorConfig,
    MetricsCompactorConfigError, MetricsCompactorRuntime, NativeHistogramRow,
    ObjectStoreCompactionIndexSink, TenantBatches, TenantCompactionRows, compact_wal_records,
    compaction_object_key, compaction_object_plan, compaction_object_plan_for_rows,
    compaction_partition_object_key, compaction_partition_object_plan,
    compaction_wal_records_from_consumer_records, encode_tenant_batches,
    poll_compactor_consumer_once, poll_compactor_once, process_compaction_partition_window,
    process_compaction_record_batch, run_compactor_consumer_loop, run_compactor_loop,
    write_compacted_tenant_blocks, write_compacted_tenant_partition_blocks,
};
pub use histogram::{
    BucketSpan, HistogramCodecError, NativeHistogram, ResetHint, decode_native_histograms,
    encode_native_histograms,
};
pub use limits::{
    IngestEnforcer, LimitError, Limits, OverridesError, OverridesProvider, QueryEnforcer,
};
pub use metadata::{MetadataIndex, MetricMetadata};
pub use otlp::{
    DeltaAccumulator, OtlpError, TranslationStrategy, decode_otlp, decode_otlp_bytes,
    decode_otlp_stateful, decode_otlp_stateful_bytes, exponential_histogram_to_native,
    normalize_name,
};
pub use sample::{decode_float_samples, encode_float_samples};
pub use schema::{
    COL_FINGERPRINT, COL_NH_COUNT, COL_NH_CUSTOM_VALUES, COL_NH_IS_FLOAT, COL_NH_NEG_COUNTS,
    COL_NH_NEG_SPANS, COL_NH_POS_COUNTS, COL_NH_POS_SPANS, COL_NH_RESET_HINT, COL_NH_SCHEMA,
    COL_NH_START_TS, COL_NH_SUM, COL_NH_ZERO_COUNT, COL_NH_ZERO_THRESHOLD, COL_TIMESTAMP,
    exemplar_schema, float_sample_schema, metadata_schema, native_histogram_schema,
};
pub use symbols::{SymbolError, SymbolTable};
pub use tenant::validate_tenant;
pub use wal::{SamplePayload, WAL_TOPIC, WalError, WalExemplar, WalRecord, partition_key};
