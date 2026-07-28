use crabka_units::{prelude::*, serde_units};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod enforce;
mod overrides;
pub use enforce::{IngestEnforcer, QueryEnforcer};
pub use overrides::{OverridesError, OverridesProvider};

/// Mimir-style per-tenant limits used by metrics ingest and query paths.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    /// Accepted sample rate; a zero rate disables ingestion rate limiting.
    #[serde(with = "serde_units::human::frequency")]
    pub ingestion_rate: Frequency,
    /// Samples the token bucket may hand out in one burst.
    pub ingestion_burst_size: u64,
    /// Active series per tenant; `0` disables the cap.
    pub max_global_series_per_user: u64,
    #[serde(with = "serde_units::human::byte_size")]
    pub max_label_name_length: ByteSize,
    #[serde(with = "serde_units::human::byte_size")]
    pub max_label_value_length: ByteSize,
    pub max_samples_per_query: u64,
    pub max_fetched_series_per_query: u64,
    /// How far back a query may reach; a zero extent disables the cap.
    #[serde(with = "non_negative_time")]
    pub max_query_lookback: Time,
    /// The widest span a range query may cover; a zero extent disables the cap.
    #[serde(with = "non_negative_time")]
    pub max_query_length: Time,
    /// Accepted out-of-order ingest window; a negative extent disables the cap.
    #[serde(with = "serde_units::human::time")]
    pub out_of_order_time_window: Time,
}

/// A configured extent that must not be negative.
///
/// `human::time` accepts a signed magnitude, and `QueryEnforcer::check_range`
/// only applies a cap that is greater than zero — so a runtime override of
/// `"-1s"` would load cleanly and silently mean *unlimited*, when zero is the
/// documented way to disable a cap. Rejecting it at parse time keeps the
/// sentinel single.
pub mod non_negative_time {
    use serde::{Deserializer, Serializer, de::Error as _};

    use crate::limits::{Time, serde_units};

    /// Writes the extent in its human form.
    ///
    /// # Errors
    ///
    /// Whatever the serializer reports for a string.
    pub fn serialize<S: Serializer>(value: &Time, serializer: S) -> Result<S::Ok, S::Error> {
        serde_units::human::time::serialize(value, serializer)
    }

    /// Reads the extent, rejecting a negative one.
    ///
    /// # Errors
    ///
    /// If the value is not a human time string, or names a negative extent.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Time, D::Error> {
        let value = serde_units::human::time::deserialize(deserializer)?;
        if value < Time::default() {
            return Err(D::Error::custom(
                "query span caps cannot be negative; use 0 to disable the cap",
            ));
        }
        Ok(value)
    }
}

/// The `Option` form of [`non_negative_time`], for the sparse override struct.
///
/// The override path deserializes through `PartialLimits`, not `Limits`, so the
/// guard has to exist on both or a per-tenant override slips past it.
/// Deserialize-only: `PartialLimits` is never serialized.
pub(crate) mod option_non_negative_time {
    use serde::{Deserializer, de::Error as _};

    use crate::limits::{Time, serde_units};

    /// Reads the optional extent, rejecting a negative one.
    ///
    /// # Errors
    ///
    /// If the value is not a human time string, or names a negative extent.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Time>, D::Error> {
        let value = serde_units::human::option_time::deserialize(deserializer)?;
        if value.is_some_and(|value| value < Time::default()) {
            return Err(D::Error::custom(
                "query span caps cannot be negative; use 0 to disable the cap",
            ));
        }
        Ok(value)
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            ingestion_rate: per_sec(10_000),
            ingestion_burst_size: 200_000,
            max_global_series_per_user: 150_000,
            max_label_name_length: kibibytes(1),
            max_label_value_length: kibibytes(2),
            max_samples_per_query: 50_000_000,
            max_fetched_series_per_query: 100_000,
            max_query_lookback: Time::ZERO,
            max_query_length: Time::ZERO,
            out_of_order_time_window: Time::ZERO,
        }
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
        check!(l.ingestion_rate > Frequency::ZERO);
        check!(l.max_global_series_per_user >= 100_000);
        check!(l.max_label_name_length == kibibytes(1));
    }

    #[test]
    fn query_span_caps_are_extents() {
        let l = Limits {
            max_query_lookback: days(1),
            max_query_length: hours(1),
            ..Limits::default()
        };
        check!(l.max_query_lookback == secs(86_400));
        check!(l.max_query_length == secs(3600));
        check!(Limits::default().max_query_lookback == Time::ZERO);
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
