use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

mod enforce;
mod overrides;
pub use enforce::{IngestEnforcer, QueryEnforcer};
pub use overrides::{OverridesError, OverridesProvider};

/// Mimir-style per-tenant limits used by metrics ingest and query paths.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    /// Samples per second; `0.0` disables ingestion rate limiting.
    pub ingestion_rate: f64,
    pub ingestion_burst_size: u64,
    /// Active series per tenant; `0` disables the cap.
    pub max_global_series_per_user: u64,
    pub max_label_name_length: u64,
    pub max_label_value_length: u64,
    pub max_samples_per_query: u64,
    pub max_fetched_series_per_query: u64,
    /// Query lookback cap in seconds; `0` disables the cap.
    pub max_query_lookback_secs: u64,
    /// Range-query span cap in seconds; `0` disables the cap.
    pub max_query_length_secs: u64,
    /// Accepted out-of-order ingest window in milliseconds; negative disables the cap.
    pub out_of_order_time_window_ms: i64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            ingestion_rate: 10_000.0,
            ingestion_burst_size: 200_000,
            max_global_series_per_user: 150_000,
            max_label_name_length: 1024,
            max_label_value_length: 2048,
            max_samples_per_query: 50_000_000,
            max_fetched_series_per_query: 100_000,
            max_query_lookback_secs: 0,
            max_query_length_secs: 0,
            out_of_order_time_window_ms: 0,
        }
    }
}

impl Limits {
    #[must_use]
    pub fn max_query_lookback(&self) -> Duration {
        Duration::from_secs(self.max_query_lookback_secs)
    }

    #[must_use]
    pub fn max_query_length(&self) -> Duration {
        Duration::from_secs(self.max_query_length_secs)
    }
}

/// Per-surface limit failures with Prometheus/Mimir status metadata.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum LimitError {
    #[error("ingestion rate exceeded: observed {observed} samples/sec above limit {rate}")]
    IngestionRateExceeded { rate: f64, observed: f64 },
    #[error("maximum active series per user exceeded: observed {observed} above limit {limit}")]
    MaxSeriesPerUser { limit: u64, observed: u64 },
    #[error("label name too long: observed {observed} bytes above limit {limit}")]
    LabelNameTooLong { limit: u64, observed: u64 },
    #[error("label value too long: observed {observed} bytes above limit {limit}")]
    LabelValueTooLong { limit: u64, observed: u64 },
    #[error("samples per query exceeded: observed {observed} above limit {limit}")]
    SamplesPerQueryExceeded { limit: u64, observed: u64 },
    #[error("series per query exceeded: observed {observed} above limit {limit}")]
    SeriesPerQueryExceeded { limit: u64, observed: u64 },
    #[error("query lookback exceeded: observed {observed_secs}s above limit {limit_secs}s")]
    QueryLookbackExceeded { limit_secs: u64, observed_secs: u64 },
    #[error("query range too long: observed {observed_secs}s above limit {limit_secs}s")]
    QueryRangeTooLong { limit_secs: u64, observed_secs: u64 },
}

impl LimitError {
    #[must_use]
    pub const fn http_status(&self) -> u16 {
        match self {
            Self::IngestionRateExceeded { .. } => 429,
            Self::MaxSeriesPerUser { .. }
            | Self::LabelNameTooLong { .. }
            | Self::LabelValueTooLong { .. } => 400,
            Self::SamplesPerQueryExceeded { .. }
            | Self::SeriesPerQueryExceeded { .. }
            | Self::QueryLookbackExceeded { .. }
            | Self::QueryRangeTooLong { .. } => 422,
        }
    }

    #[must_use]
    pub const fn error_type(&self) -> &'static str {
        match self {
            Self::SamplesPerQueryExceeded { .. }
            | Self::SeriesPerQueryExceeded { .. }
            | Self::QueryLookbackExceeded { .. }
            | Self::QueryRangeTooLong { .. } => "execution",
            Self::IngestionRateExceeded { .. }
            | Self::MaxSeriesPerUser { .. }
            | Self::LabelNameTooLong { .. }
            | Self::LabelValueTooLong { .. } => "bad_data",
        }
    }

    #[must_use]
    pub fn message(&self) -> String {
        self.to_string()
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn default_limits_are_generous_and_finite() {
        let l = Limits::default();
        check!(l.ingestion_rate > 0.0);
        check!(l.max_global_series_per_user >= 100_000);
        check!(l.max_label_name_length == 1024);
    }

    #[test]
    fn limit_errors_carry_prometheus_status_and_type() {
        let rate = LimitError::IngestionRateExceeded {
            rate: 10_000.0,
            observed: 12_000.0,
        };
        assert!(rate.http_status() == 429);

        let series = LimitError::SeriesPerQueryExceeded {
            limit: 100,
            observed: 101,
        };
        assert!(series.http_status() == 422);
        assert!(series.error_type() == "execution");

        let label = LimitError::LabelValueTooLong {
            limit: 2048,
            observed: 5000,
        };
        assert!(label.http_status() == 400);
        assert!(label.error_type() == "bad_data");
    }
}
