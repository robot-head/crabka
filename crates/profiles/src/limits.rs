//! Pyroscope-shaped per-tenant limits for profiles ingest and query paths.

use crabka_units::{ByteSize, Frequency, Time, bytes, convert::TimeExt, hours, per_sec};
use num_traits::ToPrimitive as _;
use serde::{Deserialize, Serialize};

use crate::ids::{EndMs, StartMs};

#[path = "limits/overrides.rs"]
mod overrides;

pub use overrides::{OverridesError, OverridesProvider};

/// Per-tenant profile limits.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    /// Pyroscope `ingestion_rate_mb` analog, counted in profiles per second.
    /// Zero means unlimited.
    #[serde(with = "crabka_units::serde_units::human::frequency")]
    pub ingestion_rate: Frequency,
    /// Pyroscope `ingestion_burst_size_mb` analog, counted in profiles.
    pub ingestion_burst_profiles: u64,
    /// Pyroscope `max_series`; `0` means unlimited.
    pub max_series: u64,
    /// Pyroscope `max_label_name_length`, a cap on the UTF-8 bytes of a label
    /// name; zero means unlimited.
    #[serde(with = "crabka_units::serde_units::human::byte_size")]
    pub max_label_name: ByteSize,
    /// Pyroscope `max_label_value_length`, a cap on the UTF-8 bytes of a label
    /// value; zero means unlimited.
    #[serde(with = "crabka_units::serde_units::human::byte_size")]
    pub max_label_value: ByteSize,
    /// Pyroscope `max_label_names_per_series`; `0` means unlimited.
    pub max_label_names_per_series: u64,
    /// Pyroscope `max_flamegraph_nodes_default`.
    pub max_flamegraph_nodes_default: i64,
    /// Pyroscope `max_flamegraph_nodes_max`; `0` means unlimited.
    pub max_flamegraph_nodes_max: i64,
    /// Pyroscope `max_query_length`, the `(end-start)` ceiling; zero means
    /// unlimited.
    #[serde(with = "crabka_units::serde_units::human::time")]
    pub max_query_length: Time,
    /// `__session_id__` modulo-hash bucket cap; `0` means unlimited.
    pub max_session_id_cardinality: u64,
}

/// Pyroscope's default `max_query_length` (`721h`).
///
/// This matches the upstream Pyroscope default
/// `validation.Limits.MaxQueryLength`, so the querier rejects an unbounded
/// explicit range of `start=0, end=i64::MAX` instead of a scan of the whole
/// store.
pub const DEFAULT_MAX_QUERY_LENGTH: Time = hours(721);

impl Default for Limits {
    fn default() -> Self {
        Self {
            ingestion_rate: per_sec(10_000),
            ingestion_burst_profiles: 10_000,
            max_series: 0,
            max_label_name: bytes(1024),
            max_label_value: bytes(2048),
            max_label_names_per_series: 40,
            max_flamegraph_nodes_default: 2048,
            max_flamegraph_nodes_max: 0,
            max_query_length: DEFAULT_MAX_QUERY_LENGTH,
            max_session_id_cardinality: 0,
        }
    }
}

impl Limits {
    #[must_use]
    pub fn effective_max_nodes(&self, requested: i64) -> i64 {
        let requested = if requested > 0 {
            requested
        } else {
            self.max_flamegraph_nodes_default
        };
        if self.max_flamegraph_nodes_max > 0 {
            requested.min(self.max_flamegraph_nodes_max)
        } else {
            requested
        }
    }

    ///
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn validate_query_range_ms(
        &self,
        start_ms: StartMs,
        end_ms: EndMs,
    ) -> Result<(), LimitError> {
        // The bounds are instants, so they stay epoch milliseconds; only their
        // difference is an extent. Pyroscope reports both sides of this limit in
        // whole seconds, rounded up, so the extent is ceilinged on the way out.
        let limit_secs = self.max_query_length.secs_f64().to_u64().unwrap_or(0);
        if limit_secs == 0 || end_ms.0 <= start_ms.0 {
            return Ok(());
        }
        let observed = Time::from_millis(end_ms.0.saturating_sub(start_ms.0));
        let observed_secs = observed.secs_f64().ceil().to_u64().unwrap_or(u64::MAX);
        if observed_secs > limit_secs {
            return Err(LimitError::QueryLengthExceeded {
                limit_secs,
                observed_secs,
            });
        }
        Ok(())
    }
}

/// A profile limit violation with the Connect and HTTP projection Pyroscope clients expect.
///
/// The variant payloads are raw numbers and not quantities. Each one goes
/// straight into a Pyroscope-facing error string in a fixed unit: profiles per
/// second, bytes, or whole seconds. The extraction therefore happens once at
/// construction and not at every render site.
#[derive(Debug, thiserror::Error, Clone, PartialEq)]
pub enum LimitError {
    #[error("ingestion rate exceeded: observed {observed} profiles/sec above limit {rate}")]
    IngestionRateExceeded { rate: f64, observed: f64 },
    #[error("max series exceeded: observed {observed} above limit {limit}")]
    MaxSeries { limit: u64, observed: u64 },
    #[error("label name too long: observed {observed} bytes above limit {limit}")]
    LabelNameTooLong { limit: u64, observed: u64 },
    #[error("label value too long: observed {observed} bytes above limit {limit}")]
    LabelValueTooLong { limit: u64, observed: u64 },
    #[error("too many label names: observed {observed} above limit {limit}")]
    TooManyLabels { limit: u64, observed: u64 },
    #[error("query length exceeded: observed {observed_secs}s above limit {limit_secs}s")]
    QueryLengthExceeded { limit_secs: u64, observed_secs: u64 },
    #[error("session cardinality exceeded: limit {limit}")]
    SessionCardinalityExceeded { limit: u64 },
}

impl LimitError {
    #[must_use]
    pub fn connect_code(&self) -> &'static str {
        match self {
            Self::IngestionRateExceeded { .. }
            | Self::MaxSeries { .. }
            | Self::SessionCardinalityExceeded { .. } => "resource_exhausted",
            Self::LabelNameTooLong { .. }
            | Self::LabelValueTooLong { .. }
            | Self::TooManyLabels { .. }
            | Self::QueryLengthExceeded { .. } => "invalid_argument",
        }
    }

    #[must_use]
    pub fn http_status(&self) -> u16 {
        match self {
            Self::IngestionRateExceeded { .. }
            | Self::MaxSeries { .. }
            | Self::SessionCardinalityExceeded { .. } => 429,
            Self::LabelNameTooLong { .. }
            | Self::LabelValueTooLong { .. }
            | Self::TooManyLabels { .. }
            | Self::QueryLengthExceeded { .. } => 400,
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
    use crabka_units::secs;

    use super::*;

    #[test]
    fn default_limits_are_generous_and_finite() {
        let limits = Limits::default();

        assert!(
            limits
                == Limits {
                    ingestion_rate: per_sec(10_000),
                    ingestion_burst_profiles: 10_000,
                    max_series: 0,
                    max_label_name: bytes(1024),
                    max_label_value: bytes(2048),
                    max_label_names_per_series: 40,
                    max_flamegraph_nodes_default: 2048,
                    max_flamegraph_nodes_max: 0,
                    max_query_length: secs(2_595_600),
                    max_session_id_cardinality: 0,
                }
        );
    }

    #[test]
    fn limit_errors_carry_pyroscope_code_and_status() {
        let rate = LimitError::IngestionRateExceeded {
            rate: 10_000.0,
            observed: 12_000.0,
        };
        assert!(rate.http_status() == 429);
        assert!(rate.connect_code() == "resource_exhausted");

        let name = LimitError::LabelNameTooLong {
            limit: 1024,
            observed: 5000,
        };
        assert!(name.http_status() == 400);
        assert!(name.connect_code() == "invalid_argument");

        let many = LimitError::TooManyLabels {
            limit: 40,
            observed: 41,
        };
        assert!(many.http_status() == 400);

        let duration = LimitError::QueryLengthExceeded {
            limit_secs: 3600,
            observed_secs: 7200,
        };
        assert!(duration.http_status() == 400);

        let cardinality = LimitError::SessionCardinalityExceeded { limit: 1000 };
        assert!(cardinality.http_status() == 429);
    }

    #[test]
    fn limit_error_message_names_the_cap() {
        let value = LimitError::LabelValueTooLong {
            limit: 2048,
            observed: 5000,
        };

        assert!(value.message().contains("2048"));
    }

    #[test]
    fn effective_max_nodes_defaults_and_clamps_like_pyroscope() {
        let limits = Limits {
            max_flamegraph_nodes_default: 2048,
            max_flamegraph_nodes_max: 4096,
            ..Limits::default()
        };

        for (requested, want) in [(0, 2048), (-1, 2048), (1024, 1024), (10_000, 4096)] {
            check!(limits.effective_max_nodes(requested) == want);
        }
    }

    #[test]
    fn validate_query_range_rejects_ranges_above_limit() {
        let limits = Limits {
            max_query_length: secs(60),
            ..Limits::default()
        };

        assert!(
            limits
                .validate_query_range_ms(StartMs(0), EndMs(60_000))
                .is_ok()
        );
        let err = limits
            .validate_query_range_ms(StartMs(0), EndMs(120_000))
            .unwrap_err();
        assert!(
            err == LimitError::QueryLengthExceeded {
                limit_secs: 60,
                observed_secs: 120,
            }
        );
    }

    #[test]
    fn validate_query_range_unlimited_accepts_any_range() {
        // A zero `max_query_length` means unlimited: even a range far larger
        // than any finite cap must be accepted. This pins the `||` in the early
        // return — an `&&` there would fall through and reject against a `0`
        // limit, turning unlimited into "reject everything".
        let limits = Limits {
            max_query_length: <Time as TimeExt>::ZERO,
            ..Limits::default()
        };

        assert!(
            limits
                .validate_query_range_ms(StartMs(0), EndMs(120_000))
                .is_ok()
        );
    }

    #[test]
    fn validate_query_range_rejects_open_ended_ranges_without_overflow() {
        let limits = Limits {
            max_query_length: secs(60),
            ..Limits::default()
        };

        let err = limits
            .validate_query_range_ms(StartMs(0), EndMs(i64::MAX))
            .unwrap_err();

        assert!(matches!(
            err,
            LimitError::QueryLengthExceeded {
                limit_secs: 60,
                observed_secs
            } if observed_secs > 60
        ));
    }
}
