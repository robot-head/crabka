//! Neutral series payload model consumed by `remote_write` sinks.

use crate::metricsgen::NativeHistogram;

/// Prometheus exemplar attached to a sample.
#[derive(Clone, Debug, PartialEq)]
pub struct Exemplar {
    pub value: f64,
    pub labels: Vec<(String, String)>,
    pub timestamp_ms: i64,
}

/// Neutral sample shape emitted by metrics-generator processors.
#[derive(Clone, Debug, PartialEq)]
pub enum SeriesSample {
    Counter(f64),
    Gauge(f64),
    ClassicHistogram {
        buckets: Vec<(f64, f64)>,
        sum: f64,
        count: f64,
    },
    NativeHistogram(NativeHistogram),
}

/// One named Prometheus series without the `__name__` label.
#[derive(Clone, Debug, PartialEq)]
pub struct Series {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub sample: SeriesSample,
    pub exemplars: Vec<Exemplar>,
    pub timestamp_ms: i64,
}

/// Batch of series for one tenant.
#[derive(Clone, Debug, PartialEq)]
pub struct SeriesPayload {
    pub tenant: String,
    pub series: Vec<Series>,
}

/// Sort labels into a deterministic encoder/test order.
#[must_use]
pub fn sorted_labels(mut pairs: Vec<(String, String)>) -> Vec<(String, String)> {
    pairs.sort();
    pairs
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn sorted_labels_orders_by_name_then_value() {
        let labels = sorted_labels(vec![
            ("service".into(), "checkout".into()),
            ("span_kind".into(), "server".into()),
            ("service".into(), "api".into()),
        ]);

        assert!(
            labels
                == vec![
                    ("service".into(), "api".into()),
                    ("service".into(), "checkout".into()),
                    ("span_kind".into(), "server".into()),
                ]
        );
    }

    #[test]
    fn series_payload_carries_histogram_and_exemplars() {
        let payload = SeriesPayload {
            tenant: "acme".into(),
            series: vec![Series {
                name: "traces_spanmetrics_latency".into(),
                labels: sorted_labels(vec![("service".into(), "checkout".into())]),
                sample: SeriesSample::NativeHistogram(NativeHistogram {
                    schema: 8,
                    zero_threshold: 0.0,
                    zero_count: 0.0,
                    count: 1.0,
                    sum: 0.25,
                    positive_spans: Vec::new(),
                    positive_counts: Vec::new(),
                }),
                exemplars: vec![Exemplar {
                    value: 0.25,
                    labels: sorted_labels(vec![("trace_id".into(), "01".into())]),
                    timestamp_ms: 123,
                }],
                timestamp_ms: 123,
            }],
        };

        assert!(payload.tenant == "acme");
        assert!((payload.series[0].exemplars[0].value - 0.25).abs() < 1e-9);
    }
}
