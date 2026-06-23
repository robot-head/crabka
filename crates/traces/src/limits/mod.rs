use serde::{Deserialize, Serialize};
use thiserror::Error;

mod enforce;
mod overrides;

pub use enforce::{IngestEnforcer, QueryEnforcer};
pub use overrides::{OverridesError, OverridesProvider};

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    /// Tempo `ingestion_rate_limit_bytes` analog, counted as spans/sec; `0.0` is unlimited.
    pub ingestion_rate_spans_per_sec: f64,
    /// Tempo `ingestion_burst_size_bytes` analog, counted as spans.
    pub ingestion_burst_spans: u64,
    /// Per-tenant ceiling for `/api/search`'s `limit` query parameter; `0` is unlimited.
    pub max_traces_per_search: u64,
    /// Tempo `max_bytes_per_trace` analog, counted as spans; `0` is unlimited.
    pub max_spans_per_trace: u64,
    /// Maximum UTF-8 bytes in any attribute key or string value; `0` is unlimited.
    pub max_attribute_bytes: u64,
    /// Tempo `max_search_duration`; `(end-start)` ceiling in seconds; `0` is unlimited.
    pub max_search_duration_secs: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            ingestion_rate_spans_per_sec: 100_000.0,
            ingestion_burst_spans: 100_000,
            max_traces_per_search: 1000,
            max_spans_per_trace: 200_000,
            max_attribute_bytes: 2048,
            max_search_duration_secs: 0,
        }
    }
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum LimitError {
    #[error("ingestion rate exceeded: observed {observed} spans/sec over limit {rate}")]
    IngestionRateExceeded { rate: f64, observed: f64 },
    #[error("trace exceeds max spans per trace ({limit}): observed {observed}")]
    MaxSpansPerTrace { limit: u64, observed: u64 },
    #[error("attribute exceeds max attribute bytes ({limit}): observed {observed}")]
    AttributeTooLong { limit: u64, observed: u64 },
    #[error("search limit exceeds max traces per search ({limit}): requested {requested}")]
    TracesPerSearchExceeded { limit: u64, requested: u64 },
    #[error(
        "range specified by start and end exceeds max search duration ({limit_secs}s): observed {observed_secs}s"
    )]
    SearchDurationExceeded { limit_secs: u64, observed_secs: u64 },
}

impl LimitError {
    #[must_use]
    pub fn http_status(&self) -> u16 {
        match self {
            Self::IngestionRateExceeded { .. } => 429,
            Self::MaxSpansPerTrace { .. }
            | Self::AttributeTooLong { .. }
            | Self::TracesPerSearchExceeded { .. }
            | Self::SearchDurationExceeded { .. } => 400,
        }
    }

    /// Human-readable Tempo-style cap message; exact wording is pinned by the real-Tempo suite.
    #[must_use]
    pub fn message(&self) -> String {
        self.to_string()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn default_limits_are_generous_and_finite() {
        let limits = Limits::default();

        assert!(limits.ingestion_rate_spans_per_sec > 0.0);
        assert!(limits.max_spans_per_trace >= 100_000);
        assert!(limits.max_attribute_bytes == 2048);
        assert!(limits.max_search_duration_secs == 0);
    }

    #[test]
    fn limit_errors_carry_tempo_status() {
        let rate = LimitError::IngestionRateExceeded {
            rate: 100_000.0,
            observed: 120_000.0,
        };
        assert!(rate.http_status() == 429);

        let big = LimitError::MaxSpansPerTrace {
            limit: 200_000,
            observed: 200_001,
        };
        assert!(big.http_status() == 400);

        let attr = LimitError::AttributeTooLong {
            limit: 2048,
            observed: 5000,
        };
        assert!(attr.http_status() == 400);

        let dur = LimitError::SearchDurationExceeded {
            limit_secs: 3600,
            observed_secs: 7200,
        };
        assert!(dur.http_status() == 400);
    }

    #[test]
    fn limit_error_message_names_the_cap() {
        let big = LimitError::MaxSpansPerTrace {
            limit: 200_000,
            observed: 200_001,
        };

        assert!(big.message().contains("200000"));
    }
}
