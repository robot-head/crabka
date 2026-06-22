//! Typed merge of `TraceQL`-metrics series across shards: union series by their
//! label set, sum points at equal timestamps, concatenate exemplars, and apply
//! exemplar limiting, over typed serde structs shaped to the querier's
//! `trace_metrics_json` body.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One metric point: `[timestamp_string, value]` (the querier emits the
/// timestamp as a string and the value as a float).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricPoint(pub String, pub f64);

/// One Prometheus-style exemplar.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Exemplar {
    #[serde(default)]
    pub labels: BTreeMap<String, serde_json::Value>,
    pub value: f64,
    #[serde(default)]
    pub timestamp: String,
}

/// One metric series: a label set + step-aligned points + exemplars.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricSeries {
    #[serde(default)]
    pub labels: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub points: Vec<MetricPoint>,
    #[serde(default)]
    pub exemplars: Vec<Exemplar>,
}

/// The `/api/metrics/query_range` + `/api/metrics/query` response body.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MetricsResponseJson {
    #[serde(default)]
    pub series: Vec<MetricSeries>,
}

/// Merge a series into the accumulator: union by label set, summing points at
/// equal timestamps and concatenating exemplars.
pub fn merge_metric_series(acc: &mut Vec<MetricSeries>, incoming: MetricSeries) {
    let Some(existing) = acc.iter_mut().find(|s| s.labels == incoming.labels) else {
        acc.push(incoming);
        return;
    };
    merge_points(&mut existing.points, incoming.points);
    existing.exemplars.extend(incoming.exemplars);
}

fn merge_points(existing: &mut Vec<MetricPoint>, incoming: Vec<MetricPoint>) {
    for point in incoming {
        if let Some(found) = existing.iter_mut().find(|p| p.0 == point.0) {
            found.1 += point.1;
        } else {
            existing.push(point);
        }
    }
    existing.sort_by(|a, b| {
        let ka = a.0.parse::<i128>().unwrap_or(i128::MAX);
        let kb = b.0.parse::<i128>().unwrap_or(i128::MAX);
        ka.cmp(&kb).then_with(|| a.0.cmp(&b.0))
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
#[allow(
    clippy::float_cmp,
    reason = "test assertions compare exact hand-constructed point/exemplar values, not computed floats"
)]
mod tests {
    use assert2::assert;

    use super::*;

    fn labels(svc: &str) -> BTreeMap<String, serde_json::Value> {
        let mut m = BTreeMap::new();
        m.insert("svc".to_string(), serde_json::json!(svc));
        m
    }

    #[test]
    fn merges_points_with_same_timestamp() {
        let a = MetricSeries {
            labels: labels("api"),
            points: vec![
                MetricPoint("1000".into(), 2.0),
                MetricPoint("2000".into(), 4.0),
            ],
            exemplars: vec![],
        };
        let b = MetricSeries {
            labels: labels("api"),
            points: vec![
                MetricPoint("1000".into(), 3.0),
                MetricPoint("3000".into(), 5.0),
            ],
            exemplars: vec![],
        };
        let mut merged = Vec::new();
        merge_metric_series(&mut merged, a);
        merge_metric_series(&mut merged, b);
        assert!(merged.len() == 1);
        assert!(
            merged[0].points
                == vec![
                    MetricPoint("1000".into(), 5.0),
                    MetricPoint("2000".into(), 4.0),
                    MetricPoint("3000".into(), 5.0),
                ]
        );
    }

    #[test]
    fn distinct_label_sets_stay_separate() {
        let a = MetricSeries {
            labels: labels("api"),
            points: vec![],
            exemplars: vec![],
        };
        let b = MetricSeries {
            labels: labels("db"),
            points: vec![],
            exemplars: vec![],
        };
        let mut merged = Vec::new();
        merge_metric_series(&mut merged, a);
        merge_metric_series(&mut merged, b);
        assert!(merged.len() == 2);
    }

    #[test]
    fn exemplar_limit_truncates() {
        let mut series = vec![MetricSeries {
            labels: labels("api"),
            points: vec![],
            exemplars: vec![
                Exemplar {
                    labels: BTreeMap::new(),
                    value: 1.0,
                    timestamp: "1".into(),
                },
                Exemplar {
                    labels: BTreeMap::new(),
                    value: 2.0,
                    timestamp: "2".into(),
                },
            ],
        }];
        limit_exemplars(&mut series, Some(1));
        assert!(series[0].exemplars.len() == 1);
    }

    #[test]
    fn merge_metrics_end_to_end() {
        let p0 = MetricsResponseJson {
            series: vec![MetricSeries {
                labels: labels("api"),
                points: vec![MetricPoint("1".into(), 1.0)],
                exemplars: vec![Exemplar {
                    labels: BTreeMap::new(),
                    value: 1.0,
                    timestamp: "1".into(),
                }],
            }],
        };
        let p1 = MetricsResponseJson {
            series: vec![MetricSeries {
                labels: labels("api"),
                points: vec![MetricPoint("1".into(), 2.0)],
                exemplars: vec![Exemplar {
                    labels: BTreeMap::new(),
                    value: 2.0,
                    timestamp: "2".into(),
                }],
            }],
        };
        let merged = merge_metrics(vec![p0, p1], Some(1));
        assert!(merged.series.len() == 1);
        assert!(merged.series[0].points[0].1 == 3.0);
        assert!(merged.series[0].exemplars.len() == 1);
    }

    #[test]
    fn round_trips_querier_metrics_body() {
        let body = serde_json::json!({
            "series": [{
                "labels": { "svc": "api" },
                "points": [["1000", 2.0]],
                "exemplars": [{ "labels": {}, "value": 1.5, "timestamp": "1000" }]
            }]
        });
        let resp: MetricsResponseJson = serde_json::from_value(body).unwrap();
        assert!(resp.series.len() == 1);
        assert!(resp.series[0].points[0] == MetricPoint("1000".into(), 2.0));
        assert!(resp.series[0].exemplars[0].value == 1.5);
    }
}
