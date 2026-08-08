use std::sync::Arc;

use crabka_units::{
    Time,
    convert::{ByteSizeExt as _, FrequencyExt as _, TimeExt as _},
};
use dashmap::DashMap;
use num_traits::ToPrimitive as _;
use rate_bucket::RateBucket;

use super::{LimitError, Limits};

#[derive(Debug, Default)]
pub struct IngestEnforcer {
    buckets: DashMap<String, Arc<RateBucket>>,
}

impl IngestEnforcer {
    #[must_use]
    pub fn new() -> Self {
        Self {
            buckets: DashMap::new(),
        }
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn check_span_rate(
        &self,
        limits: &Limits,
        tenant: &str,
        n_spans: u64,
    ) -> Result<(), LimitError> {
        if limits.ingestion_rate.per_sec_f64() == 0.0 || n_spans == 0 {
            return Ok(());
        }
        let rate = rounded_positive_rate(limits.ingestion_rate.per_sec_f64());
        if rate == 0 {
            return Ok(());
        }

        // Refill rate and burst capacity are seeded separately: the sustained
        // refill tracks the configured spans/sec, while the bucket capacity is
        // the larger of rate and configured burst so a burst can be absorbed
        // without raising the sustained rate.
        //
        // NOTE: `crabka_broker::throttle::TokenBucket` couples refill rate and
        // capacity in a single `set_rate` (capacity == rate, no separate burst
        // knob) and offers no peek/refund, so it cannot express either a
        // distinct burst capacity (M4) or all-or-nothing consumption. We use a
        // local `RateBucket` (same refill math) that decouples the two and
        // consumes atomically all-or-nothing instead of editing the broker crate.
        let capacity = rate.max(limits.ingestion_burst_spans);
        let bucket = self
            .buckets
            .entry(tenant.to_string())
            .or_insert_with(|| Arc::new(RateBucket::new(rate, capacity)))
            .clone();
        // All-or-nothing: a rejected over-limit request consumes no tokens, so
        // it does not starve a subsequent within-limit request.
        if bucket.try_consume_all(n_spans) {
            Ok(())
        } else {
            Err(LimitError::IngestionRateExceeded {
                rate: limits.ingestion_rate.per_sec_f64(),
                observed: f64_from_u64(n_spans),
            })
        }
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
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

    /// Enforce the per-attribute byte cap.
    ///
    /// Each entry is `(key, value_bytes)`, where `value_bytes` is the value's
    /// TRUE encoded byte length. Callers must therefore measure `Bytes`, `Int`
    /// and `Double` values, and must not convert them to a string first.
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn check_attributes(limits: &Limits, attrs: &[(String, u64)]) -> Result<(), LimitError> {
        let limit = limits.max_attribute.bytes_u64();
        if limit == 0 {
            return Ok(());
        }
        for (key, value_bytes) in attrs {
            let observed = (key.len() as u64).max(*value_bytes);
            if observed > limit {
                return Err(LimitError::AttributeTooLong { limit, observed });
            }
        }
        Ok(())
    }
}

pub struct QueryEnforcer;

impl QueryEnforcer {
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn check_search_limit(limits: &Limits, requested: u64) -> Result<(), LimitError> {
        let limit = limits.max_traces_per_search;
        if limit != 0 && requested > limit {
            return Err(LimitError::TracesPerSearchExceeded { limit, requested });
        }
        Ok(())
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn check_search_duration(
        limits: &Limits,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(), LimitError> {
        let limit_secs = u64::try_from(limits.max_search_duration.secs_i64()).unwrap_or(0);
        if limit_secs == 0 {
            return Ok(());
        }
        let observed = Time::from_nanos(end_ns.saturating_sub(start_ns));
        let observed_secs = observed.secs_f64().ceil().to_u64().unwrap_or(0);
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

mod rate_bucket {
    use std::{sync::Mutex, time::Instant};

    /// Token bucket with a separately-configured refill rate and burst
    /// capacity, and with all-or-nothing consumption.
    ///
    /// The broker's `TokenBucket` couples capacity to the refill rate, because
    /// a single `set_rate` sets both, and it consumes only partially. Neither
    /// behaviour fits the traces ingest limiter, which needs a distinct burst
    /// capacity and reject-without-spend semantics. The refill arithmetic
    /// mirrors the broker bucket: tokens accrue at `rate_per_sec` and saturate
    /// at `capacity`.
    #[derive(Debug)]
    pub struct RateBucket {
        rate_per_sec: u64,
        capacity: u64,
        state: Mutex<State>,
    }

    #[derive(Debug)]
    struct State {
        available: u64,
        last_refill: Instant,
    }

    impl RateBucket {
        pub fn new(rate_per_sec: u64, capacity: u64) -> Self {
            Self {
                rate_per_sec,
                capacity,
                state: Mutex::new(State {
                    available: capacity,
                    last_refill: Instant::now(),
                }),
            }
        }

        /// Consume `requested` tokens all-or-nothing.
        ///
        /// This returns `true` and spends the tokens if the full amount is
        /// available after refill. Otherwise it returns `false` and spends
        /// nothing.
        pub fn try_consume_all(&self, requested: u64) -> bool {
            let now = Instant::now();
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let elapsed = now.saturating_duration_since(state.last_refill);
            // Tokens accrued = elapsed_nanos * rate / 1e9, saturated into u64.
            let accrued = elapsed.as_nanos() * u128::from(self.rate_per_sec) / 1_000_000_000;
            let refill = u64::try_from(accrued).unwrap_or(u64::MAX);
            state.available = state.available.saturating_add(refill).min(self.capacity);
            state.last_refill = now;
            if state.available >= requested {
                state.available -= requested;
                true
            } else {
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crabka_units::{bytes, hours, per_sec};

    use super::*;
    use crate::limits::{LimitError, Limits};

    fn limits_with(spans: u64, attr_bytes: u32) -> Limits {
        Limits {
            max_spans_per_trace: spans,
            max_attribute: bytes(attr_bytes),
            ..Limits::default()
        }
    }

    #[test]
    fn trace_size_cap_rejects_over_limit() {
        let limits = limits_with(100, 2048);

        assert2::assert!(IngestEnforcer::check_trace_size(&limits, 100).is_ok());
        assert2::assert!(matches!(
            IngestEnforcer::check_trace_size(&limits, 101),
            Err(LimitError::MaxSpansPerTrace { .. })
        ));
    }

    #[test]
    fn zero_trace_size_cap_is_unlimited() {
        let limits = limits_with(0, 2048);

        assert2::assert!(IngestEnforcer::check_trace_size(&limits, 5_000_000).is_ok());
    }

    #[test]
    fn attribute_size_cap_enforced() {
        let limits = limits_with(0, 4);

        for (attrs, want) in [
            (vec![("ab".to_string(), 2_u64)], Ok(())),
            (
                // Over-long key.
                vec![("toolong".to_string(), 1_u64)],
                Err(LimitError::AttributeTooLong {
                    limit: 4,
                    observed: 7,
                }),
            ),
            (
                // Over-long value.
                vec![("a".to_string(), 7_u64)],
                Err(LimitError::AttributeTooLong {
                    limit: 4,
                    observed: 7,
                }),
            ),
        ] {
            assert2::assert!(IngestEnforcer::check_attributes(&limits, &attrs) == want);
        }
    }

    #[test]
    fn attribute_size_cap_measures_true_value_bytes() {
        // A value whose TRUE byte length exceeds the cap must be rejected even
        // when a stringification would under-report it (e.g. `Bytes`).
        let limits = limits_with(0, 4);
        let oversized_bytes = vec![("k".to_string(), 8_u64)];
        assert2::assert!(matches!(
            IngestEnforcer::check_attributes(&limits, &oversized_bytes),
            Err(LimitError::AttributeTooLong { .. })
        ));
    }

    #[test]
    fn ingest_rate_bucket_eventually_rejects() {
        let enforcer = IngestEnforcer::new();
        let limits = Limits {
            ingestion_rate: per_sec(100),
            ingestion_burst_spans: 100,
            ..Limits::default()
        };

        assert2::assert!(enforcer.check_span_rate(&limits, "t", 100).is_ok());
        assert2::assert!(matches!(
            enforcer.check_span_rate(&limits, "t", 100),
            Err(LimitError::IngestionRateExceeded { .. })
        ));
    }

    #[test]
    fn sustained_rate_does_not_exceed_configured_rate_when_burst_is_larger() {
        // rate=100, burst=1000. The configured burst may be absorbed once, but
        // the SUSTAINED rate must track the configured rate, not the larger
        // burst: a steady stream above `rate` must eventually reject rather than
        // sustaining `burst` forever (the old code raised the refill rate to the
        // burst, sustaining 1000/sec indefinitely).
        let enforcer = IngestEnforcer::new();
        let limits = Limits {
            ingestion_rate: per_sec(100),
            ingestion_burst_spans: 1000,
            ..Limits::default()
        };

        // Drain the one-time burst capacity (1000 spans). With no time to
        // refill, the next 100-span request must reject because the sustained
        // refill is only 100/sec, not the burst.
        for _ in 0..10 {
            assert2::assert!(enforcer.check_span_rate(&limits, "t", 100).is_ok());
        }
        assert2::assert!(matches!(
            enforcer.check_span_rate(&limits, "t", 100),
            Err(LimitError::IngestionRateExceeded { .. })
        ));
    }

    #[test]
    fn rejected_over_limit_request_does_not_starve_later_within_limit_request() {
        // An over-limit request that is rejected must not consume tokens that a
        // subsequent within-limit request needs (all-or-nothing).
        let enforcer = IngestEnforcer::new();
        let limits = Limits {
            ingestion_rate: per_sec(100),
            ingestion_burst_spans: 100,
            ..Limits::default()
        };

        // Reject an over-limit request (150 > 100 available).
        assert2::assert!(matches!(
            enforcer.check_span_rate(&limits, "t", 150),
            Err(LimitError::IngestionRateExceeded { .. })
        ));
        // A following within-limit request for the full capacity still succeeds
        // because the rejected request consumed nothing.
        assert2::assert!(enforcer.check_span_rate(&limits, "t", 100).is_ok());
    }

    #[test]
    fn search_limit_and_duration_caps() {
        let limits = Limits {
            max_traces_per_search: 1000,
            max_search_duration: hours(1),
            ..Limits::default()
        };

        assert2::assert!(QueryEnforcer::check_search_limit(&limits, 1000).is_ok());
        assert2::assert!(matches!(
            QueryEnforcer::check_search_limit(&limits, 1001),
            Err(LimitError::TracesPerSearchExceeded { .. })
        ));
        let start_ns = 1_000_000_000_000_000_000_i64;
        assert2::assert!(matches!(
            QueryEnforcer::check_search_duration(&limits, start_ns, start_ns + 7_200_000_000_000),
            Err(LimitError::SearchDurationExceeded { .. })
        ));
        assert2::assert!(
            QueryEnforcer::check_search_duration(&limits, start_ns, start_ns + 1_800_000_000_000)
                .is_ok()
        );
    }
}
