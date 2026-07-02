//! The `metrics-generator` role: the third traces consumer group.
//!
//! It runs span-metrics (RED) and service-graph processors over the traces WAL
//! stream and flushes their series via Prometheus `remote_write`.

pub mod checkpoint;
pub mod clock;
pub mod config;
pub mod processor;
pub mod remotewrite;
pub mod series;
pub mod service;
pub mod servicegraph;
pub mod sink;
pub mod spanmetrics;

/// Single point of truth for types consumed from sibling traces/metrics slices.
pub mod contract {
    pub use crate::{
        span::{SpanKind, StatusCode},
        wal::TRACES_WAL_TOPIC,
    };

    /// The flattened WAL projection read by metrics-generator processors.
    #[derive(Clone, Debug, PartialEq)]
    pub struct SpanRecord {
        pub tenant: String,
        pub trace_id: [u8; 16],
        pub span_id: [u8; 8],
        pub parent_span_id: [u8; 8],
        pub name: String,
        pub kind: SpanKind,
        pub start_ns: i64,
        pub duration_ns: i64,
        pub status: StatusCode,
        pub status_message: String,
        pub service_name: String,
        pub attributes: Vec<(String, String)>,
        pub size_bytes: u64,
    }

    /// A run of populated native histogram buckets.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct BucketSpan {
        pub offset: i32,
        pub length: u32,
    }

    /// Native histogram with absolute bucket counts.
    #[derive(Clone, Debug, PartialEq)]
    pub struct NativeHistogram {
        pub schema: i8,
        pub zero_threshold: f64,
        pub zero_count: f64,
        pub count: f64,
        pub sum: f64,
        pub positive_spans: Vec<BucketSpan>,
        pub positive_counts: Vec<f64>,
    }
}

pub use checkpoint::{
    EdgeCheckpointStore, InMemoryCheckpointStore, encode_checkpoint_key, parse_checkpoint_key,
};
pub use clock::{Clock, MockClock, SystemClock};
pub use config::{DEFAULT_LATENCY_BUCKETS_NS, MetricsGenConfig};
pub use contract::{
    BucketSpan, NativeHistogram, SpanKind, SpanRecord, StatusCode, TRACES_WAL_TOPIC,
};
pub use processor::MetricsGenerator;
pub use remotewrite::{PrometheusRemoteWriteSink, WireTimeSeries, le_label, to_timeseries};
pub use series::{Exemplar, Series, SeriesPayload, SeriesSample, sorted_labels};
pub use service::MetricsGenService;
pub use servicegraph::{ConnectionType, EdgeStore, RecordOutcome};
pub use sink::{
    KafkaSpanSource, MockRemoteWriteSink, MockSpanSource, RemoteWriteSink, SinkError, SpanSource,
    decode_consumer_records, project_wal_record,
};
pub use spanmetrics::{SpanMetricsRegistry, dimension_labels};
