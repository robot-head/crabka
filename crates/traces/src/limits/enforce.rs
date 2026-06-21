use std::sync::Arc;

use crabka_broker::throttle::TokenBucket;
use dashmap::DashMap;

use super::{LimitError, Limits};

#[derive(Debug, Default)]
pub struct IngestEnforcer {
    buckets: DashMap<String, Arc<TokenBucket>>,
}

impl IngestEnforcer {
    #[must_use]
    pub fn new() -> Self {
        Self {
            buckets: DashMap::new(),
        }
    }

    pub fn check_span_rate(
        &self,
        limits: &Limits,
        tenant: &str,
        n_spans: u64,
    ) -> Result<(), LimitError> {
        if limits.ingestion_rate_spans_per_sec == 0.0 || n_spans == 0 {
            return Ok(());
        }
        let rate = rounded_positive_rate(limits.ingestion_rate_spans_per_sec);
        if rate == 0 {
            return Ok(());
        }

        // `TokenBucket` couples burst capacity and refill rate. Seed with the larger
        // of steady rate and configured burst so the first request can use the burst.
        let bucket_rate = rate.max(limits.ingestion_burst_spans);
        let bucket = self
            .buckets
            .entry(tenant.to_string())
            .or_insert_with(|| {
                let bucket = Arc::new(TokenBucket::new());
                bucket.set_rate(bucket_rate);
                bucket
            })
            .clone();
        let granted = bucket.try_consume(n_spans);
        if granted < n_spans {
            return Err(LimitError::IngestionRateExceeded {
                rate: limits.ingestion_rate_spans_per_sec,
                observed: f64_from_u64(n_spans),
            });
        }
        Ok(())
    }

    pub fn check_trace_size(limits: &Limits, spans_in_trace: u64) -> Result<(), LimitError> {
        let limit = limits.max_spans_per_trace;
        if limit != 0 && spans_in_trace > limit {
            return Err(LimitError::MaxSpansPerTrace {
                limit,
                observed: spans_in_trace,
            });
        }
        Ok(())
    }

    pub fn check_attributes(limits: &Limits, attrs: &[(String, String)]) -> Result<(), LimitError> {
        let limit = limits.max_attribute_bytes;
        if limit == 0 {
            return Ok(());
        }
        for (key, value) in attrs {
            let observed = key.len().max(value.len()) as u64;
            if observed > limit {
                return Err(LimitError::AttributeTooLong { limit, observed });
            }
        }
        Ok(())
    }
}

pub struct QueryEnforcer;

impl QueryEnforcer {
    pub fn check_search_limit(limits: &Limits, requested: u64) -> Result<(), LimitError> {
        let limit = limits.max_traces_per_search;
        if limit != 0 && requested > limit {
            return Err(LimitError::TracesPerSearchExceeded { limit, requested });
        }
        Ok(())
    }

    pub fn check_search_duration(
        limits: &Limits,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(), LimitError> {
        let limit_secs = limits.max_search_duration_secs;
        if limit_secs == 0 {
            return Ok(());
        }
        let observed_ns = end_ns.saturating_sub(start_ns);
        let observed_secs = u64::try_from(observed_ns)
            .unwrap_or(0)
            .div_ceil(1_000_000_000);
        if observed_secs > limit_secs {
            return Err(LimitError::SearchDurationExceeded {
                limit_secs,
                observed_secs,
            });
        }
        Ok(())
    }
}

fn rounded_positive_rate(rate: f64) -> u64 {
    if !rate.is_finite() || rate <= 0.0 {
        return 0;
    }
    rate.round().to_string().parse().unwrap_or(u64::MAX)
}

fn f64_from_u64(value: u64) -> f64 {
    value.to_string().parse().unwrap_or(f64::INFINITY)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::limits::{LimitError, Limits};

    fn limits_with(spans: u64, attr_bytes: u64) -> Limits {
        Limits {
            max_spans_per_trace: spans,
            max_attribute_bytes: attr_bytes,
            ..Limits::default()
        }
    }

    #[test]
    fn trace_size_cap_rejects_over_limit() {
        let limits = limits_with(100, 2048);

        assert!(IngestEnforcer::check_trace_size(&limits, 100).is_ok());
        assert!(matches!(
            IngestEnforcer::check_trace_size(&limits, 101),
            Err(LimitError::MaxSpansPerTrace { .. })
        ));
    }

    #[test]
    fn zero_trace_size_cap_is_unlimited() {
        let limits = limits_with(0, 2048);

        assert!(IngestEnforcer::check_trace_size(&limits, 5_000_000).is_ok());
    }

    #[test]
    fn attribute_size_cap_enforced() {
        let limits = limits_with(0, 4);
        let ok = vec![("ab".to_string(), "cd".to_string())];
        let bad_key = vec![("toolong".to_string(), "x".to_string())];
        let bad_value = vec![("a".to_string(), "toolong".to_string())];

        assert!(IngestEnforcer::check_attributes(&limits, &ok).is_ok());
        assert!(matches!(
            IngestEnforcer::check_attributes(&limits, &bad_key),
            Err(LimitError::AttributeTooLong { .. })
        ));
        assert!(matches!(
            IngestEnforcer::check_attributes(&limits, &bad_value),
            Err(LimitError::AttributeTooLong { .. })
        ));
    }

    #[test]
    fn ingest_rate_bucket_eventually_rejects() {
        let enforcer = IngestEnforcer::new();
        let limits = Limits {
            ingestion_rate_spans_per_sec: 100.0,
            ingestion_burst_spans: 100,
            ..Limits::default()
        };

        assert!(enforcer.check_span_rate(&limits, "t", 100).is_ok());
        assert!(matches!(
            enforcer.check_span_rate(&limits, "t", 100),
            Err(LimitError::IngestionRateExceeded { .. })
        ));
    }

    #[test]
    fn search_limit_and_duration_caps() {
        let limits = Limits {
            max_traces_per_search: 1000,
            max_search_duration_secs: 3600,
            ..Limits::default()
        };

        assert!(QueryEnforcer::check_search_limit(&limits, 1000).is_ok());
        assert!(matches!(
            QueryEnforcer::check_search_limit(&limits, 1001),
            Err(LimitError::TracesPerSearchExceeded { .. })
        ));
        let start_ns = 1_000_000_000_000_000_000_i64;
        assert!(matches!(
            QueryEnforcer::check_search_duration(&limits, start_ns, start_ns + 7_200_000_000_000),
            Err(LimitError::SearchDurationExceeded { .. })
        ));
        assert!(
            QueryEnforcer::check_search_duration(&limits, start_ns, start_ns + 1_800_000_000_000)
                .is_ok()
        );
    }
}
