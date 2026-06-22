//! Prometheus `remote_write` sink implementation.

use async_trait::async_trait;
use prost::Message as _;

use crate::metricsgen::series::{Exemplar, Series, SeriesPayload, SeriesSample};
use crate::metricsgen::sink::{RemoteWriteSink, SinkError};
use crate::metricsgen::{BucketSpan, NativeHistogram};

/// Encoder-neutral flat `remote_write` row.
#[derive(Clone, Debug, PartialEq)]
pub struct WireTimeSeries {
    pub labels: Vec<(String, String)>,
    pub value: f64,
    pub timestamp_ms: i64,
    pub exemplars: Vec<Exemplar>,
    pub native_histogram: Option<NativeHistogram>,
}

#[must_use]
pub fn le_label(le_seconds: f64) -> String {
    if le_seconds.is_infinite() {
        "+Inf".to_string()
    } else {
        le_seconds.to_string()
    }
}

#[must_use]
pub fn to_timeseries(series: &[Series]) -> Vec<WireTimeSeries> {
    let mut out = Vec::new();
    for s in series {
        match &s.sample {
            SeriesSample::Counter(value) | SeriesSample::Gauge(value) => {
                out.push(WireTimeSeries {
                    labels: with_name(&s.name, &s.labels),
                    value: *value,
                    timestamp_ms: s.timestamp_ms,
                    exemplars: s.exemplars.clone(),
                    native_histogram: None,
                });
            }
            SeriesSample::ClassicHistogram {
                buckets,
                sum,
                count,
            } => {
                push_classic_histogram(&mut out, s, buckets, *sum, *count);
            }
            SeriesSample::NativeHistogram(histogram) => {
                out.push(WireTimeSeries {
                    labels: with_name(&s.name, &s.labels),
                    value: 0.0,
                    timestamp_ms: s.timestamp_ms,
                    exemplars: s.exemplars.clone(),
                    native_histogram: Some(histogram.clone()),
                });
            }
        }
    }
    out
}

fn push_classic_histogram(
    out: &mut Vec<WireTimeSeries>,
    s: &Series,
    buckets: &[(f64, f64)],
    sum: f64,
    count: f64,
) {
    let bucket_name = format!("{}_bucket", s.name);
    let mut assigned_exemplars = vec![false; s.exemplars.len()];

    for (le, cumulative) in buckets {
        let mut labels = s.labels.clone();
        labels.push(("le".to_string(), le_label(*le)));
        let exemplars =
            bucket_exemplars(&s.exemplars, &mut assigned_exemplars, |ex| ex.value <= *le);
        out.push(WireTimeSeries {
            labels: with_name(&bucket_name, &labels),
            value: *cumulative,
            timestamp_ms: s.timestamp_ms,
            exemplars,
            native_histogram: None,
        });
    }

    let mut inf_labels = s.labels.clone();
    inf_labels.push(("le".to_string(), "+Inf".to_string()));
    let inf_exemplars = bucket_exemplars(&s.exemplars, &mut assigned_exemplars, |_| true);
    out.push(WireTimeSeries {
        labels: with_name(&bucket_name, &inf_labels),
        value: count,
        timestamp_ms: s.timestamp_ms,
        exemplars: inf_exemplars,
        native_histogram: None,
    });
    out.push(WireTimeSeries {
        labels: with_name(&format!("{}_sum", s.name), &s.labels),
        value: sum,
        timestamp_ms: s.timestamp_ms,
        exemplars: Vec::new(),
        native_histogram: None,
    });
    out.push(WireTimeSeries {
        labels: with_name(&format!("{}_count", s.name), &s.labels),
        value: count,
        timestamp_ms: s.timestamp_ms,
        exemplars: Vec::new(),
        native_histogram: None,
    });
}

fn bucket_exemplars(
    exemplars: &[Exemplar],
    assigned: &mut [bool],
    mut predicate: impl FnMut(&Exemplar) -> bool,
) -> Vec<Exemplar> {
    let mut out = Vec::new();
    for (idx, exemplar) in exemplars.iter().enumerate() {
        if !assigned[idx] && predicate(exemplar) {
            assigned[idx] = true;
            out.push(exemplar.clone());
        }
    }
    out
}

fn with_name(name: &str, labels: &[(String, String)]) -> Vec<(String, String)> {
    let mut out = Vec::with_capacity(labels.len() + 1);
    out.push(("__name__".to_string(), name.to_string()));
    out.extend(labels.iter().cloned());
    out.sort();
    out
}

/// HTTP client for Prometheus `remote_write`.
pub struct PrometheusRemoteWriteSink {
    url: String,
    http: reqwest::Client,
}

impl PrometheusRemoteWriteSink {
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            http: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl RemoteWriteSink for PrometheusRemoteWriteSink {
    async fn write(&self, payload: &SeriesPayload) -> Result<(), SinkError> {
        let rows = to_timeseries(&payload.series);
        let body = encode_write_request(&rows).map_err(SinkError::Decode)?;
        let resp = self
            .http
            .post(&self.url)
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .header("X-Scope-OrgID", &payload.tenant)
            .body(body)
            .send()
            .await
            .map_err(|err| SinkError::Transport(err.to_string()))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(SinkError::Transport(format!(
                "remote_write status {}",
                resp.status()
            )))
        }
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    timeseries: Vec<TimeSeries>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    samples: Vec<Sample>,
    #[prost(message, repeated, tag = "3")]
    exemplars: Vec<RemoteWriteExemplar>,
    #[prost(message, repeated, tag = "4")]
    histograms: Vec<Histogram>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Label {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Sample {
    #[prost(double, tag = "1")]
    value: f64,
    #[prost(int64, tag = "2")]
    timestamp: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RemoteWriteExemplar {
    #[prost(message, repeated, tag = "1")]
    labels: Vec<Label>,
    #[prost(double, tag = "2")]
    value: f64,
    #[prost(int64, tag = "3")]
    timestamp: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RemoteWriteBucketSpan {
    #[prost(sint32, tag = "1")]
    offset: i32,
    #[prost(uint32, tag = "2")]
    length: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Histogram {
    #[prost(double, tag = "2")]
    count_float: f64,
    #[prost(double, tag = "3")]
    sum: f64,
    #[prost(sint32, tag = "4")]
    schema: i32,
    #[prost(double, tag = "5")]
    zero_threshold: f64,
    #[prost(double, tag = "7")]
    zero_count_float: f64,
    #[prost(message, repeated, tag = "11")]
    positive_spans: Vec<RemoteWriteBucketSpan>,
    #[prost(double, repeated, tag = "13")]
    positive_counts: Vec<f64>,
    #[prost(enumeration = "ResetHint", tag = "14")]
    reset_hint: i32,
    #[prost(int64, tag = "15")]
    timestamp: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum ResetHint {
    Unknown = 0,
    Yes = 1,
    No = 2,
    Gauge = 3,
}

fn encode_write_request(rows: &[WireTimeSeries]) -> Result<Vec<u8>, String> {
    let request = WriteRequest {
        timeseries: rows
            .iter()
            .map(|row| TimeSeries {
                labels: labels_to_proto(&row.labels),
                samples: samples_to_proto(row),
                exemplars: row
                    .exemplars
                    .iter()
                    .map(|exemplar| RemoteWriteExemplar {
                        labels: labels_to_proto(&exemplar.labels),
                        value: exemplar.value,
                        timestamp: exemplar.timestamp_ms,
                    })
                    .collect(),
                histograms: histograms_to_proto(row),
            })
            .collect(),
    };

    let mut protobuf = Vec::with_capacity(request.encoded_len());
    request
        .encode(&mut protobuf)
        .map_err(|err| format!("remote_write pb encode: {err}"))?;
    snap::raw::Encoder::new()
        .compress_vec(&protobuf)
        .map_err(|err| format!("remote_write snappy encode: {err}"))
}

fn samples_to_proto(row: &WireTimeSeries) -> Vec<Sample> {
    if row.native_histogram.is_some() {
        Vec::new()
    } else {
        vec![Sample {
            value: row.value,
            timestamp: row.timestamp_ms,
        }]
    }
}

fn histograms_to_proto(row: &WireTimeSeries) -> Vec<Histogram> {
    row.native_histogram
        .iter()
        .map(|histogram| Histogram {
            count_float: histogram.count,
            sum: histogram.sum,
            schema: i32::from(histogram.schema),
            zero_threshold: histogram.zero_threshold,
            zero_count_float: histogram.zero_count,
            positive_spans: bucket_spans_to_proto(&histogram.positive_spans),
            positive_counts: histogram.positive_counts.clone(),
            reset_hint: ResetHint::No as i32,
            timestamp: row.timestamp_ms,
        })
        .collect()
}

fn bucket_spans_to_proto(spans: &[BucketSpan]) -> Vec<RemoteWriteBucketSpan> {
    spans
        .iter()
        .map(|span| RemoteWriteBucketSpan {
            offset: span.offset,
            length: span.length,
        })
        .collect()
}

fn labels_to_proto(labels: &[(String, String)]) -> Vec<Label> {
    labels
        .iter()
        .map(|(name, value)| Label {
            name: name.clone(),
            value: value.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use prost::Message as _;

    use super::*;
    use crate::metricsgen::series::{Exemplar, Series, SeriesSample};
    use crate::metricsgen::{BucketSpan, NativeHistogram};

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestWriteRequest {
        #[prost(message, repeated, tag = "1")]
        timeseries: Vec<TestTimeSeries>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestTimeSeries {
        #[prost(message, repeated, tag = "1")]
        labels: Vec<TestLabel>,
        #[prost(message, repeated, tag = "2")]
        samples: Vec<TestSample>,
        #[prost(message, repeated, tag = "3")]
        exemplars: Vec<TestExemplar>,
        #[prost(message, repeated, tag = "4")]
        histograms: Vec<TestHistogram>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestLabel {
        #[prost(string, tag = "1")]
        name: String,
        #[prost(string, tag = "2")]
        value: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestSample {
        #[prost(double, tag = "1")]
        value: f64,
        #[prost(int64, tag = "2")]
        timestamp: i64,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestExemplar {
        #[prost(message, repeated, tag = "1")]
        labels: Vec<TestLabel>,
        #[prost(double, tag = "2")]
        value: f64,
        #[prost(int64, tag = "3")]
        timestamp: i64,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestBucketSpan {
        #[prost(sint32, tag = "1")]
        offset: i32,
        #[prost(uint32, tag = "2")]
        length: u32,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestHistogram {
        #[prost(double, tag = "2")]
        count_float: f64,
        #[prost(double, tag = "3")]
        sum: f64,
        #[prost(sint32, tag = "4")]
        schema: i32,
        #[prost(double, tag = "5")]
        zero_threshold: f64,
        #[prost(double, tag = "7")]
        zero_count_float: f64,
        #[prost(message, repeated, tag = "11")]
        positive_spans: Vec<TestBucketSpan>,
        #[prost(double, repeated, tag = "13")]
        positive_counts: Vec<f64>,
        #[prost(enumeration = "TestResetHint", tag = "14")]
        reset_hint: i32,
        #[prost(int64, tag = "15")]
        timestamp: i64,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
    #[repr(i32)]
    enum TestResetHint {
        Unknown = 0,
        Yes = 1,
        No = 2,
        Gauge = 3,
    }

    fn has_label(ts: &WireTimeSeries, k: &str, v: &str) -> bool {
        ts.labels.iter().any(|(lk, lv)| lk == k && lv == v)
    }

    #[test]
    fn encode_write_request_emits_snappy_protobuf_remote_write() {
        let rows = vec![WireTimeSeries {
            labels: vec![
                ("__name__".into(), "traces_spanmetrics_calls_total".into()),
                ("service".into(), "api".into()),
            ],
            value: 7.0,
            timestamp_ms: 1_234,
            exemplars: vec![Exemplar {
                value: 0.042,
                labels: vec![("trace_id".into(), "0abc".into())],
                timestamp_ms: 1_235,
            }],
            native_histogram: None,
        }];

        let compressed = encode_write_request(&rows).unwrap();
        let decoded = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .unwrap();
        let request = TestWriteRequest::decode(decoded.as_slice()).unwrap();

        assert!(request.timeseries.len() == 1);
        let ts = &request.timeseries[0];
        assert!(ts.samples.len() == 1);
        assert!((ts.samples[0].value - 7.0).abs() < f64::EPSILON);
        assert!(ts.samples[0].timestamp == 1_234);
        assert!(ts.labels.iter().any(
            |label| label.name == "__name__" && label.value == "traces_spanmetrics_calls_total"
        ));
        assert!(
            ts.labels
                .iter()
                .any(|label| label.name == "service" && label.value == "api")
        );
        assert!(ts.exemplars.len() == 1);
        assert!((ts.exemplars[0].value - 0.042).abs() < f64::EPSILON);
        assert!(ts.exemplars[0].timestamp == 1_235);
        assert!(
            ts.exemplars[0]
                .labels
                .iter()
                .any(|label| label.name == "trace_id" && label.value == "0abc")
        );
    }

    #[test]
    fn counter_becomes_one_timeseries_with_name_label() {
        let s = Series {
            name: "traces_spanmetrics_calls_total".into(),
            labels: vec![("service".into(), "api".into())],
            sample: SeriesSample::Counter(3.0),
            exemplars: vec![],
            timestamp_ms: 1_000,
        };

        let out = to_timeseries(&[s]);

        assert!(out.len() == 1);
        assert!(has_label(
            &out[0],
            "__name__",
            "traces_spanmetrics_calls_total"
        ));
        assert!(has_label(&out[0], "service", "api"));
        assert!((out[0].value - 3.0).abs() < 1e-9);
        assert!(out[0].timestamp_ms == 1_000);
    }

    #[test]
    fn classic_histogram_fans_into_bucket_sum_count() {
        let s = Series {
            name: "traces_spanmetrics_latency".into(),
            labels: vec![("service".into(), "api".into())],
            sample: SeriesSample::ClassicHistogram {
                buckets: vec![(0.004, 0.0), (0.008, 2.0)],
                sum: 0.012,
                count: 2.0,
            },
            exemplars: vec![Exemplar {
                value: 0.005,
                labels: vec![("trace_id".into(), "ab".into())],
                timestamp_ms: 1_000,
            }],
            timestamp_ms: 1_000,
        };

        let out = to_timeseries(&[s]);

        assert!(out.len() == 5);
        let bucket_inf = out
            .iter()
            .find(|t| {
                has_label(t, "__name__", "traces_spanmetrics_latency_bucket")
                    && has_label(t, "le", "+Inf")
            })
            .unwrap();
        assert!((bucket_inf.value - 2.0).abs() < 1e-9);

        let sum = out
            .iter()
            .find(|t| has_label(t, "__name__", "traces_spanmetrics_latency_sum"))
            .unwrap();
        assert!((sum.value - 0.012).abs() < 1e-9);

        let count = out
            .iter()
            .find(|t| has_label(t, "__name__", "traces_spanmetrics_latency_count"))
            .unwrap();
        assert!((count.value - 2.0).abs() < 1e-9);

        let le_8 = out
            .iter()
            .find(|t| {
                has_label(t, "__name__", "traces_spanmetrics_latency_bucket")
                    && has_label(t, "le", "0.008")
            })
            .unwrap();
        assert!(le_8.exemplars.len() == 1);
        assert!(le_8.exemplars[0].labels[0].0 == "trace_id");
    }

    #[test]
    fn native_histogram_encodes_remote_write_histogram() {
        let rows = to_timeseries(&[Series {
            name: "traces_spanmetrics_latency".into(),
            labels: vec![("service".into(), "api".into())],
            sample: SeriesSample::NativeHistogram(NativeHistogram {
                schema: 8,
                zero_threshold: 0.001,
                zero_count: 1.5,
                count: 4.5,
                sum: 0.25,
                positive_spans: vec![BucketSpan {
                    offset: -2,
                    length: 2,
                }],
                positive_counts: vec![2.0, 1.0],
            }),
            exemplars: vec![Exemplar {
                value: 0.12,
                labels: vec![("trace_id".into(), "abc".into())],
                timestamp_ms: 1_235,
            }],
            timestamp_ms: 1_234,
        }]);

        let compressed = encode_write_request(&rows).unwrap();
        let decoded = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .unwrap();
        let request = TestWriteRequest::decode(decoded.as_slice()).unwrap();

        assert!(request.timeseries.len() == 1);
        let ts = &request.timeseries[0];
        assert!(ts.samples.is_empty());
        assert!(ts.histograms.len() == 1);
        assert!(ts.exemplars.len() == 1);
        assert!(
            ts.labels.iter().any(
                |label| label.name == "__name__" && label.value == "traces_spanmetrics_latency"
            )
        );
        let histogram = &ts.histograms[0];
        assert!((histogram.count_float - 4.5).abs() < f64::EPSILON);
        assert!((histogram.sum - 0.25).abs() < f64::EPSILON);
        assert!(histogram.schema == 8);
        assert!((histogram.zero_threshold - 0.001).abs() < f64::EPSILON);
        assert!((histogram.zero_count_float - 1.5).abs() < f64::EPSILON);
        assert!(histogram.positive_spans.len() == 1);
        assert!(histogram.positive_spans[0].offset == -2);
        assert!(histogram.positive_spans[0].length == 2);
        assert!(histogram.positive_counts == vec![2.0, 1.0]);
        assert!(histogram.reset_hint == TestResetHint::No as i32);
        assert!(histogram.timestamp == 1_234);
    }

    #[test]
    fn le_label_renders_inf_and_floats() {
        assert!(le_label(f64::INFINITY) == "+Inf");
        assert!(le_label(0.008) == "0.008");
    }
}
