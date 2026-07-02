#![allow(dead_code)]

use std::sync::Arc;

use crabka_blockstore::Labels;
use crabka_metrics::{BucketSpan, NativeHistogram, ResetHint};

use crate::{
    EngineOpts, InMemoryMetricStore, InstantSample, PromqlEngine, QueryResult, SampleValue,
    conformance::testkit::metric_to_labels, error::Result,
};

const TENANT: &str = "test";

pub(crate) fn empty_store() -> InMemoryMetricStore {
    InMemoryMetricStore::new()
}

pub(crate) fn store_with_series(name: &str, samples: &[(i64, f64)]) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    for (ts_ms, value) in samples {
        store.push_float(TENANT, labels.clone(), *ts_ms, *value);
    }
    store
}

pub(crate) fn store_with_series_multi(series: &[(&str, f64)]) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for (selector, value) in series {
        store.push_float(TENANT, metric_to_labels(selector), 0, *value);
    }
    store
}

pub(crate) fn store_with_labeled_series(
    name: &str,
    labels: &[(&str, &str)],
    value: f64,
) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut label_set = Labels::new();
    label_set.insert("__name__", name);
    for (key, value) in labels {
        label_set.insert(*key, *value);
    }
    store.push_float(TENANT, label_set, 0, value);
    store
}

pub(crate) fn store_with_classic_histogram() -> InMemoryMetricStore {
    store_with_series_multi(&[
        ("http_request_duration_seconds_bucket{le=\"0.1\"}", 0.0),
        ("http_request_duration_seconds_bucket{le=\"0.2\"}", 1.0),
        ("http_request_duration_seconds_bucket{le=\"0.4\"}", 3.0),
        ("http_request_duration_seconds_bucket{le=\"+Inf\"}", 3.0),
    ])
}

pub(crate) fn eval_instant_nh(name: &str, histogram: &NativeHistogram) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    store.push_histogram(TENANT, labels, 0, histogram.clone());
    store
}

pub(crate) async fn eval_instant(
    store: &InMemoryMetricStore,
    query: &str,
    ts_ms: i64,
) -> QueryResult {
    eval_instant_err(store, query, ts_ms).await.unwrap()
}

pub(crate) async fn eval_instant_err(
    store: &InMemoryMetricStore,
    query: &str,
    ts_ms: i64,
) -> Result<QueryResult> {
    let engine = PromqlEngine::new(Arc::new(store.clone()), EngineOpts::default());
    engine.query_instant(TENANT, query, ts_ms).await
}

pub(crate) fn nh(
    count: f64,
    sum: f64,
    schema: i8,
    positive_buckets: &[(i32, f64)],
) -> NativeHistogram {
    let mut buckets = positive_buckets.to_vec();
    buckets.sort_by_key(|(index, _)| *index);
    let (positive_spans, positive_counts) = spans_and_counts(&buckets);
    NativeHistogram {
        schema,
        is_float: true,
        reset_hint: ResetHint::No,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count,
        sum,
        positive_spans,
        positive_counts,
        negative_spans: vec![],
        negative_counts: vec![],
        custom_values: None,
        start_timestamp_ms: None,
    }
}

fn spans_and_counts(buckets: &[(i32, f64)]) -> (Vec<BucketSpan>, Vec<f64>) {
    let mut spans = Vec::new();
    let mut counts = Vec::new();
    let Some((first_index, first_count)) = buckets.first().copied() else {
        return (spans, counts);
    };

    let mut current_offset = first_index;
    let mut current_len = 1_u32;
    let mut previous_index = first_index;
    counts.push(first_count);

    for (index, count) in buckets.iter().copied().skip(1) {
        if index == previous_index + 1 {
            current_len += 1;
        } else {
            spans.push(BucketSpan {
                offset: current_offset,
                length: current_len,
            });
            current_offset = index - previous_index - 1;
            current_len = 1;
        }
        previous_index = index;
        counts.push(count);
    }
    spans.push(BucketSpan {
        offset: current_offset,
        length: current_len,
    });

    (spans, counts)
}

pub(crate) trait QueryResultExt {
    fn single(&self) -> &InstantSample;
    fn as_scalar(&self) -> f64;
    fn values_f64(&self) -> Vec<f64>;
    fn is_empty(&self) -> bool;
    fn iter(&self) -> std::slice::Iter<'_, InstantSample>;
    fn len(&self) -> usize;
}

impl QueryResultExt for QueryResult {
    fn single(&self) -> &InstantSample {
        let QueryResult::InstantVector(samples) = self else {
            panic!("expected instant vector");
        };
        assert_eq!(samples.len(), 1, "expected exactly one sample");
        &samples[0]
    }

    fn as_scalar(&self) -> f64 {
        let QueryResult::Scalar { value, .. } = self else {
            panic!("expected scalar");
        };
        *value
    }

    fn values_f64(&self) -> Vec<f64> {
        let QueryResult::InstantVector(samples) = self else {
            panic!("expected instant vector");
        };
        samples.iter().map(InstantSampleExt::value_f64).collect()
    }

    fn is_empty(&self) -> bool {
        matches!(self, QueryResult::InstantVector(samples) if samples.is_empty())
    }

    fn iter(&self) -> std::slice::Iter<'_, InstantSample> {
        let QueryResult::InstantVector(samples) = self else {
            panic!("expected instant vector");
        };
        samples.iter()
    }

    fn len(&self) -> usize {
        let QueryResult::InstantVector(samples) = self else {
            panic!("expected instant vector");
        };
        samples.len()
    }
}

pub(crate) trait InstantSampleExt {
    fn value_f64(&self) -> f64;
}

impl InstantSampleExt for InstantSample {
    fn value_f64(&self) -> f64 {
        match &self.value {
            SampleValue::Float(value) => *value,
            SampleValue::Histogram(_) => panic!("expected float sample"),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[tokio::test]
    async fn store_with_series_round_trips_through_eval_instant() {
        let store = store_with_series("up", &[(0, 1.0)]);
        let result = eval_instant(&store, "up", 0).await;

        assert!((result.single().value_f64() - 1.0).abs() < f64::EPSILON);
    }
}
