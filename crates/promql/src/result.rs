//! Prometheus-shaped query result model.

use crabka_blockstore::Labels;
use crabka_metrics::NativeHistogram;

/// A single sample value: a float or a native histogram.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum SampleValue {
    Float(f64),
    Histogram(NativeHistogram),
}

/// One labeled point in an instant vector.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct InstantSample {
    pub labels: Labels,
    pub ts_ms: i64,
    pub value: SampleValue,
}

/// One labeled series of points in a range matrix.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RangeSeries {
    pub labels: Labels,
    pub samples: Vec<(i64, SampleValue)>,
}

/// A `PromQL` evaluation result.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum QueryResult {
    Scalar { ts_ms: i64, value: f64 },
    InstantVector(Vec<InstantSample>),
    RangeMatrix(Vec<RangeSeries>),
    Str { ts_ms: i64, value: String },
}

impl QueryResult {
    /// Prometheus `data.resultType` string.
    #[must_use]
    pub fn result_type(&self) -> &'static str {
        match self {
            Self::Scalar { .. } => "scalar",
            Self::InstantVector(_) => "vector",
            Self::RangeMatrix(_) => "matrix",
            Self::Str { .. } => "string",
        }
    }
}

/// Warnings and info annotations raised while evaluating a query.
///
/// Mirrors Prometheus' `util/annotations` channel: `PromQLWarning`-class
/// messages land in [`Annotations::warnings`] and `PromQLInfo`-class messages
/// land in [`Annotations::infos`]. Messages are deduplicated and stored as the
/// exact Prometheus annotation text (without the trailing position suffix,
/// which Crabka does not track through evaluation).
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Annotations {
    /// `PromQL warning:`-class annotations, in first-seen order.
    pub warnings: Vec<String>,
    /// `PromQL info:`-class annotations, in first-seen order.
    pub infos: Vec<String>,
}

impl Annotations {
    /// An empty annotation set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a warning, ignoring exact duplicates.
    pub fn warn(&mut self, message: impl Into<String>) {
        let message = message.into();
        if !self.warnings.contains(&message) {
            self.warnings.push(message);
        }
    }

    /// Record an info annotation, ignoring exact duplicates.
    pub fn info(&mut self, message: impl Into<String>) {
        let message = message.into();
        if !self.infos.contains(&message) {
            self.infos.push(message);
        }
    }

    /// Merge another set's annotations into this one, preserving dedup.
    pub fn extend(&mut self, other: &Annotations) {
        for warning in &other.warnings {
            self.warn(warning.clone());
        }
        for info in &other.infos {
            self.info(info.clone());
        }
    }

    /// True when no annotations have been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.warnings.is_empty() && self.infos.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_blockstore::Labels;

    use super::*;

    #[test]
    fn result_type_strings_match_prometheus() {
        assert!(
            QueryResult::Scalar {
                ts_ms: 0,
                value: 1.0
            }
            .result_type()
                == "scalar"
        );
        assert!(QueryResult::InstantVector(vec![]).result_type() == "vector");
        assert!(QueryResult::RangeMatrix(vec![]).result_type() == "matrix");
        assert!(
            QueryResult::Str {
                ts_ms: 0,
                value: "x".into(),
            }
            .result_type()
                == "string"
        );
    }

    #[test]
    fn instant_sample_holds_float() {
        let mut labels = Labels::new();
        labels.insert("__name__", "up");
        let sample = InstantSample {
            labels,
            ts_ms: 1000,
            value: SampleValue::Float(1.0),
        };
        assert!(sample.value == SampleValue::Float(1.0));
    }
}
