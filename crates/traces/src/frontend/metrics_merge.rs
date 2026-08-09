//! Typed merge of `TraceQL`-metrics series across shards.
//!
//! The merge unions series by their label set, sums samples at equal
//! timestamps, concatenates exemplars, and applies exemplar limiting.
//!
//! The serde structs are shaped to the querier's `trace_metrics_json` body,
//! which is Tempo's protojson `QueryRangeResponse`. `labels` is a `KeyValue`
//! array. Each entry in `samples` carries a `timestampMs`, an int64 count of
//! milliseconds rendered as a string, plus a `value`. The body also carries
//! `promLabels` and exemplars.
//!
//! This is the same shape Grafana's Tempo backend unmarshals. The frontend
//! therefore both decodes per-shard querier responses and re-serializes the
//! merged result correctly.

use serde::{Deserialize, Serialize};

/// One label as Tempo's `commonv1.KeyValue`: `{"key": k, "value": <AnyValue>}`.
///
/// The value, such as `{"stringValue": "api"}`, stays raw JSON. The merge only
/// compares label sets, and never interprets values.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: serde_json::Value,
}

/// One metric sample: `{"timestampMs": "<ms>", "value": <f64>}`.
///
/// Tempo's protojson renders the int64 millisecond timestamp as a string, so it
/// stays a string here. The merge compares and orders it numerically.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricSample {
    #[serde(rename = "timestampMs")]
    pub timestamp_ms: String,
    pub value: f64,
}

/// One exemplar in Tempo's metrics shape.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Exemplar {
    #[serde(default)]
    pub labels: Vec<KeyValue>,
    pub value: f64,
    #[serde(rename = "timestampMs", default)]
    pub timestamp_ms: String,
}

/// One metric series: a label set, its Prometheus label string, step-aligned
/// samples, and exemplars.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricSeries {
    #[serde(default)]
    pub labels: Vec<KeyValue>,
    #[serde(rename = "promLabels", default)]
    pub prom_labels: String,
    #[serde(default)]
    pub samples: Vec<MetricSample>,
    #[serde(default)]
    pub exemplars: Vec<Exemplar>,
}

/// The response body for `/api/metrics/query_range` and
/// `/api/metrics/query`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MetricsResponseJson {
    #[serde(default)]
    pub series: Vec<MetricSeries>,
}

/// Merge a series into the accumulator.
///
/// This unions by label set, sums samples at equal timestamps, and
/// concatenates exemplars. The querier emits a series' labels in a
/// deterministic order for a given group, so equal label sets across shards
/// compare equal as vectors.
pub fn merge_metric_series(acc: &mut Vec<MetricSeries>, incoming: MetricSeries) {
    let Some(existing) = acc.iter_mut().find(|s| s.labels == incoming.labels) else {
        acc.push(incoming);
        return;
    };
    merge_samples(&mut existing.samples, incoming.samples);
    existing.exemplars.extend(incoming.exemplars);
    // `prom_labels` is derived from the (matching) label set, so the existing
    // series already carries the correct value.
}

fn merge_samples(existing: &mut Vec<MetricSample>, incoming: Vec<MetricSample>) {
    for sample in incoming {
        if let Some(found) = existing
            .iter_mut()
            .find(|s| s.timestamp_ms == sample.timestamp_ms)
        {
            found.value += sample.value;
        } else {
            existing.push(sample);
        }
    }
    existing.sort_by(|a, b| {
        let ka = a.timestamp_ms.parse::<i128>().unwrap_or(i128::MAX);
        let kb = b.timestamp_ms.parse::<i128>().unwrap_or(i128::MAX);
        ka.cmp(&kb)
            .then_with(|| a.timestamp_ms.cmp(&b.timestamp_ms))
    });
}

/// Truncate each series' exemplars to `limit`. `None` disables limiting.
pub fn limit_exemplars(series: &mut [MetricSeries], limit: Option<usize>) {
    let Some(limit) = limit else { return };
    for s in series {
        s.exemplars.truncate(limit);
    }
}

/// Merge all metric partials' series into one response, then apply exemplar
/// limiting.
#[must_use]
pub fn merge_metrics(
    partials: Vec<MetricsResponseJson>,
    exemplar_limit: Option<usize>,
) -> MetricsResponseJson {
    let mut merged: Vec<MetricSeries> = Vec::new();
    for p in partials {
        for s in p.series {
            merge_metric_series(&mut merged, s);
        }
    }
    limit_exemplars(&mut merged, exemplar_limit);
    MetricsResponseJson { series: merged }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn labels(svc: &str) -> Vec<KeyValue> {
        vec![KeyValue {
            key: "svc".to_string(),
            value: serde_json::json!({ "stringValue": svc }),
        }]
    }

    fn sample(ts_ms: &str, value: f64) -> MetricSample {
        MetricSample {
            timestamp_ms: ts_ms.to_string(),
            value,
        }
    }

    #[test]
    fn merges_samples_with_same_timestamp() {
        let a = MetricSeries {
            labels: labels("api"),
            prom_labels: "{svc=\"api\"}".into(),
            samples: vec![sample("1000", 2.0), sample("2000", 4.0)],
            exemplars: vec![],
        };
        let b = MetricSeries {
            labels: labels("api"),
            prom_labels: "{svc=\"api\"}".into(),
            samples: vec![sample("1000", 3.0), sample("3000", 5.0)],
            exemplars: vec![],
        };
        let mut merged = Vec::new();
        merge_metric_series(&mut merged, a);
        merge_metric_series(&mut merged, b);
        assert2::assert!(merged.len() == 1);
        assert2::assert!(
            merged[0].samples.as_slice()
                == &[
                    sample("1000", 5.0),
                    sample("2000", 4.0),
                    sample("3000", 5.0)
                ][..]
        );
    }

    #[test]
    fn distinct_label_sets_stay_separate() {
        let a = MetricSeries {
            labels: labels("api"),
            prom_labels: String::new(),
            samples: vec![],
            exemplars: vec![],
        };
        let b = MetricSeries {
            labels: labels("db"),
            prom_labels: String::new(),
            samples: vec![],
            exemplars: vec![],
        };
        let mut merged = Vec::new();
        merge_metric_series(&mut merged, a);
        merge_metric_series(&mut merged, b);
        assert2::assert!(merged.len() == 2);
    }

    #[test]
    fn exemplar_limit_truncates() {
        let mut series = vec![MetricSeries {
            labels: labels("api"),
            prom_labels: String::new(),
            samples: vec![],
            exemplars: vec![
                Exemplar {
                    labels: vec![],
                    value: 1.0,
                    timestamp_ms: "1".into(),
                },
                Exemplar {
                    labels: vec![],
                    value: 2.0,
                    timestamp_ms: "2".into(),
                },
            ],
        }];
        limit_exemplars(&mut series, Some(1));
        assert2::assert!(series[0].exemplars.len() == 1);
    }

    #[test]
    fn merge_metrics_end_to_end() {
        let p0 = MetricsResponseJson {
            series: vec![MetricSeries {
                labels: labels("api"),
                prom_labels: "{svc=\"api\"}".into(),
                samples: vec![sample("1", 1.0)],
                exemplars: vec![Exemplar {
                    labels: vec![],
                    value: 1.0,
                    timestamp_ms: "1".into(),
                }],
            }],
        };
        let p1 = MetricsResponseJson {
            series: vec![MetricSeries {
                labels: labels("api"),
                prom_labels: "{svc=\"api\"}".into(),
                samples: vec![sample("1", 2.0)],
                exemplars: vec![Exemplar {
                    labels: vec![],
                    value: 2.0,
                    timestamp_ms: "2".into(),
                }],
            }],
        };
        let merged = merge_metrics(vec![p0, p1], Some(1));
        assert2::assert!(
            merged
                == MetricsResponseJson {
                    series: vec![MetricSeries {
                        labels: labels("api"),
                        prom_labels: "{svc=\"api\"}".to_string(),
                        samples: vec![sample("1", 3.0)],
                        exemplars: vec![Exemplar {
                            labels: vec![],
                            value: 1.0,
                            timestamp_ms: "1".to_string(),
                        }],
                    }],
                }
        );
    }

    #[test]
    fn round_trips_querier_metrics_body() {
        // Exactly the shape the querier's `trace_metrics_json` emits.
        let body = serde_json::json!({
            "series": [{
                "labels": [{"key": "svc", "value": {"stringValue": "api"}}],
                "promLabels": "{svc=\"api\"}",
                "samples": [{"timestampMs": "1000", "value": 2.0}],
                "exemplars": [{
                    "labels": [{"key": "trace_id", "value": {"stringValue": "0a"}}],
                    "value": 1.5,
                    "timestampMs": "1000"
                }]
            }]
        });
        let resp: MetricsResponseJson = serde_json::from_value(body.clone()).unwrap();
        assert2::assert!(
            resp == MetricsResponseJson {
                series: vec![MetricSeries {
                    labels: vec![KeyValue {
                        key: "svc".to_string(),
                        value: serde_json::json!({ "stringValue": "api" }),
                    }],
                    prom_labels: "{svc=\"api\"}".to_string(),
                    samples: vec![sample("1000", 2.0)],
                    exemplars: vec![Exemplar {
                        labels: vec![KeyValue {
                            key: "trace_id".to_string(),
                            value: serde_json::json!({ "stringValue": "0a" }),
                        }],
                        value: 1.5,
                        timestamp_ms: "1000".to_string(),
                    }],
                }],
            }
        );
        // Re-serializes to the same Tempo shape (round-trip stable).
        assert2::assert!(serde_json::to_value(&resp).unwrap() == body);
    }
}
