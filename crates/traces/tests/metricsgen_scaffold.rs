use assert2::check;
use crabka_traces::metricsgen::{
    BucketSpan, NativeHistogram, SpanKind, SpanRecord, StatusCode, TRACES_WAL_TOPIC,
};

#[test]
fn metricsgen_contract_exposes_wal_projection() {
    let record = SpanRecord {
        tenant: "acme".into(),
        trace_id: [1; 16],
        span_id: [2; 8],
        parent_span_id: [0; 8],
        name: "GET /checkout".into(),
        kind: SpanKind::Server,
        start_ns: 10,
        duration_ns: 250_000,
        status: StatusCode::Ok,
        status_message: String::new(),
        service_name: "checkout".into(),
        attributes: vec![("http.method".into(), "GET".into())],
        size_bytes: 128,
    };

    let hist = NativeHistogram {
        schema: 8,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count: 1.0,
        sum: 250_000.0,
        positive_spans: vec![BucketSpan {
            offset: 0,
            length: 1,
        }],
        positive_counts: vec![1.0],
    };

    check!(TRACES_WAL_TOPIC == "__crabka_traces_wal");
    check!(record.service_name == "checkout");
    check!(hist.positive_counts == vec![1.0]);
}
