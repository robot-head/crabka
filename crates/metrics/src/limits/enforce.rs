use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crabka_blockstore::Labels;
use crabka_throttle::TokenBucket;
use dashmap::DashMap;

use super::{LimitError, Limits};

/// Default cap on the number of distinct tenants tracked for per-tenant
/// ingestion-rate token buckets. The map is otherwise insert-only, so an
/// unbounded set of tenant strings (e.g. from a misbehaving or hostile client)
/// would grow memory without limit; once this many tenants are tracked, the
/// least-recently-touched bucket is evicted to make room.
const DEFAULT_MAX_RATE_BUCKETS: usize = 100_000;

/// Per-tenant ingestion-rate token bucket with a monotonic last-touch stamp
/// used for least-recently-used eviction once `max_rate_buckets` is reached.
#[derive(Debug)]
struct RateBucket {
    bucket: Arc<TokenBucket>,
    last_touch: AtomicU64,
}

#[derive(Debug)]
pub struct IngestEnforcer {
    sample_rate_buckets: DashMap<String, RateBucket>,
    /// Maximum number of distinct tenants tracked in `sample_rate_buckets`.
    max_rate_buckets: usize,
    /// Monotonic logical clock used to stamp bucket touches for LRU eviction.
    touch_clock: AtomicU64,
}

impl Default for IngestEnforcer {
    fn default() -> Self {
        Self {
            sample_rate_buckets: DashMap::new(),
            max_rate_buckets: DEFAULT_MAX_RATE_BUCKETS,
            touch_clock: AtomicU64::new(0),
        }
    }
}

impl IngestEnforcer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct an enforcer that tracks at most `max_rate_buckets` distinct
    /// tenants for ingestion-rate limiting. A value of `0` is clamped to `1`.
    #[must_use]
    pub fn with_max_rate_buckets(max_rate_buckets: usize) -> Self {
        Self {
            max_rate_buckets: max_rate_buckets.max(1),
            ..Self::default()
        }
    }

    /// Next value of the monotonic logical clock used to stamp bucket touches.
    fn next_touch_stamp(&self) -> u64 {
        self.touch_clock.fetch_add(1, Ordering::Relaxed)
    }

    /// Evict least-recently-touched tenants until the map is within the cap.
    ///
    /// Called only on the cold path where a brand-new tenant is inserted while
    /// the map is already at capacity, so the linear scan is bounded by
    /// `max_rate_buckets` and does not run on the steady-state hot path.
    fn evict_if_over_cap(&self) {
        while self.sample_rate_buckets.len() > self.max_rate_buckets {
            let oldest = self
                .sample_rate_buckets
                .iter()
                .min_by_key(|entry| entry.value().last_touch.load(Ordering::Relaxed))
                .map(|entry| entry.key().clone());
            match oldest {
                Some(key) => {
                    self.sample_rate_buckets.remove(&key);
                }
                None => break,
            }
        }
    }

    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "Mimir sample rates are configured as finite samples/sec and enforced by an integer token bucket."
    )]
    pub fn check_sample_rate(
        &self,
        limits: &Limits,
        tenant: &str,
        n_samples: u64,
    ) -> Result<(), LimitError> {
        // Only a finite, strictly-positive rate of `0.0` disables the limit
        // (Mimir's `ingestion_rate: 0` sentinel). A non-finite rate (NaN/Inf)
        // never reaches the unlimited path: NaN would slip past `== 0.0`, and
        // Inf is treated as effectively unbounded throughput, both of which we
        // collapse to "rate limiting disabled" rather than the integer-bucket
        // path, which cannot represent them.
        if !limits.ingestion_rate.is_finite() || limits.ingestion_rate <= 0.0 {
            return Ok(());
        }

        // A configured positive rate must never round down to `0`, which the
        // token bucket interprets as the unlimited sentinel. Round to nearest
        // but clamp to at least one sample/sec so e.g. `0.4` still throttles.
        let rate = (limits.ingestion_rate.round() as u64).max(1);
        let stamp = self.next_touch_stamp();
        let entry = self
            .sample_rate_buckets
            .entry(tenant.to_string())
            .or_insert_with(|| {
                let bucket = Arc::new(TokenBucket::new());
                bucket.set_rate_with_burst(rate, limits.ingestion_burst_size);
                RateBucket {
                    bucket,
                    last_touch: AtomicU64::new(stamp),
                }
            });
        // Stamp this access for LRU eviction, then drop the dashmap entry guard
        // before scanning so the eviction sweep never contends with the shard
        // lock this tenant lives in.
        entry.last_touch.store(stamp, Ordering::Relaxed);
        let bucket = entry.bucket.clone();
        drop(entry);
        self.evict_if_over_cap();
        let granted = bucket.try_consume(n_samples);
        if granted == n_samples {
            Ok(())
        } else {
            Err(LimitError::IngestionRateExceeded {
                rate: limits.ingestion_rate,
                observed: n_samples as f64,
            })
        }
    }

    pub fn check_active_series(
        &self,
        limits: &Limits,
        _tenant: &str,
        would_add: u64,
        current: u64,
    ) -> Result<(), LimitError> {
        if limits.max_global_series_per_user == 0 {
            return Ok(());
        }
        let observed = current.saturating_add(would_add);
        if observed > limits.max_global_series_per_user {
            Err(LimitError::MaxSeriesPerUser {
                limit: limits.max_global_series_per_user,
                observed,
            })
        } else {
            Ok(())
        }
    }

    pub fn check_labels(limits: &Limits, labels: &Labels) -> Result<(), LimitError> {
        for (name, value) in labels.iter() {
            let name_len = u64::try_from(name.len()).unwrap_or(u64::MAX);
            if name_len > limits.max_label_name_length {
                return Err(LimitError::LabelNameTooLong {
                    limit: limits.max_label_name_length,
                    observed: name_len,
                });
            }
            let value_len = u64::try_from(value.len()).unwrap_or(u64::MAX);
            if value_len > limits.max_label_value_length {
                return Err(LimitError::LabelValueTooLong {
                    limit: limits.max_label_value_length,
                    observed: value_len,
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct QueryEnforcer;

impl QueryEnforcer {
    pub fn check_range(
        limits: &Limits,
        start_ms: i64,
        end_ms: i64,
        now_ms: i64,
    ) -> Result<(), LimitError> {
        if limits.max_query_length_secs != 0 {
            let span_ms = duration_ms(start_ms, end_ms);
            let limit_ms = limits.max_query_length_secs.saturating_mul(1000);
            if span_ms > limit_ms {
                return Err(LimitError::QueryRangeTooLong {
                    limit_secs: limits.max_query_length_secs,
                    observed_secs: millis_to_secs_ceil(span_ms),
                });
            }
        }

        if limits.max_query_lookback_secs != 0 {
            let lookback_ms = duration_ms(start_ms, now_ms);
            let limit_ms = limits.max_query_lookback_secs.saturating_mul(1000);
            if lookback_ms > limit_ms {
                return Err(LimitError::QueryLookbackExceeded {
                    limit_secs: limits.max_query_lookback_secs,
                    observed_secs: millis_to_secs_ceil(lookback_ms),
                });
            }
        }

        Ok(())
    }

    pub fn check_series_count(limits: &Limits, selected: u64) -> Result<(), LimitError> {
        if limits.max_fetched_series_per_query != 0
            && selected > limits.max_fetched_series_per_query
        {
            Err(LimitError::SeriesPerQueryExceeded {
                limit: limits.max_fetched_series_per_query,
                observed: selected,
            })
        } else {
            Ok(())
        }
    }

    pub fn check_sample_count(limits: &Limits, processed: u64) -> Result<(), LimitError> {
        if limits.max_samples_per_query != 0 && processed > limits.max_samples_per_query {
            Err(LimitError::SamplesPerQueryExceeded {
                limit: limits.max_samples_per_query,
                observed: processed,
            })
        } else {
            Ok(())
        }
    }
}

fn duration_ms(start_ms: i64, end_ms: i64) -> u64 {
    u64::try_from(end_ms.saturating_sub(start_ms).max(0)).unwrap_or(u64::MAX)
}

fn millis_to_secs_ceil(ms: u64) -> u64 {
    ms.saturating_add(999) / 1000
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_blockstore::Labels;

    use super::*;
    use crate::limits::Limits;

    fn limits_with(series: u64, name_len: u64, val_len: u64) -> Limits {
        Limits {
            max_global_series_per_user: series,
            max_label_name_length: name_len,
            max_label_value_length: val_len,
            ..Limits::default()
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert(*name, *value);
        }
        labels
    }

    #[test]
    fn active_series_cap_rejects_over_limit() {
        let e = IngestEnforcer::new();
        let l = limits_with(100, 1024, 2048);
        assert!(e.check_active_series(&l, "t", 1, 99).is_ok());
        assert!(e.check_active_series(&l, "t", 1, 100).is_err());
    }

    #[test]
    fn zero_series_cap_is_unlimited() {
        let e = IngestEnforcer::new();
        let l = limits_with(0, 1024, 2048);
        assert!(e.check_active_series(&l, "t", 1_000_000, 5_000_000).is_ok());
    }

    #[test]
    fn label_length_caps_enforced() {
        let l = limits_with(0, 4, 4);
        let ok = labels(&[("ab", "cd")]);
        let bad_name = labels(&[("toolong", "x")]);
        let bad_val = labels(&[("a", "toolong")]);
        check!(IngestEnforcer::check_labels(&l, &ok).is_ok());
        assert!(matches!(
            IngestEnforcer::check_labels(&l, &bad_name),
            Err(LimitError::LabelNameTooLong { .. })
        ));
        assert!(matches!(
            IngestEnforcer::check_labels(&l, &bad_val),
            Err(LimitError::LabelValueTooLong { .. })
        ));
    }

    #[test]
    fn ingestion_rate_bucket_eventually_rejects() {
        let e = IngestEnforcer::new();
        let l = Limits {
            ingestion_rate: 100.0,
            ingestion_burst_size: 100,
            ..Limits::default()
        };
        assert!(e.check_sample_rate(&l, "t", 100).is_ok());
        assert!(e.check_sample_rate(&l, "t", 100).is_err());
    }

    #[test]
    fn fractional_rate_still_throttles() {
        // A positive but sub-0.5 rate must not round down to the unlimited
        // (rate-0) sentinel; it clamps to >= 1 sample/sec and still throttles.
        let e = IngestEnforcer::new();
        let l = Limits {
            ingestion_rate: 0.4,
            ingestion_burst_size: 1,
            ..Limits::default()
        };
        assert!(e.check_sample_rate(&l, "t", 1).is_ok());
        assert!(e.check_sample_rate(&l, "t", 1).is_err());
    }

    #[test]
    fn zero_rate_is_unlimited() {
        let e = IngestEnforcer::new();
        let l = Limits {
            ingestion_rate: 0.0,
            ingestion_burst_size: 0,
            ..Limits::default()
        };
        assert!(e.check_sample_rate(&l, "t", 1_000_000).is_ok());
    }

    #[test]
    fn non_finite_rate_is_handled() {
        let e = IngestEnforcer::new();
        // NaN must not slip past `== 0.0` into the unlimited path, and is
        // treated as "limiting disabled" rather than reaching the int bucket.
        let nan = Limits {
            ingestion_rate: f64::NAN,
            ingestion_burst_size: 0,
            ..Limits::default()
        };
        assert!(e.check_sample_rate(&nan, "nan", 1_000_000).is_ok());
        // +Inf is unbounded throughput, also disabled rather than int-bucketed.
        let inf = Limits {
            ingestion_rate: f64::INFINITY,
            ingestion_burst_size: 0,
            ..Limits::default()
        };
        assert!(e.check_sample_rate(&inf, "inf", 1_000_000).is_ok());
    }

    #[test]
    fn rate_bucket_map_stays_bounded() {
        // Many distinct tenants must not grow the bucket map without bound;
        // LRU eviction keeps it at or below the configured cap.
        let cap = 8;
        let e = IngestEnforcer::with_max_rate_buckets(cap);
        let l = Limits {
            ingestion_rate: 100.0,
            ingestion_burst_size: 100,
            ..Limits::default()
        };
        for i in 0..1_000 {
            let tenant = format!("tenant-{i}");
            let _ = e.check_sample_rate(&l, &tenant, 1);
            assert!(e.sample_rate_buckets.len() <= cap);
        }
        assert!(e.sample_rate_buckets.len() <= cap);
    }

    #[test]
    fn ingestion_burst_is_independent_of_rate() {
        let e = IngestEnforcer::new();
        let l = Limits {
            ingestion_rate: 100.0,
            ingestion_burst_size: 1000,
            ..Limits::default()
        };
        check!(e.check_sample_rate(&l, "t", 500).is_ok());
        check!(e.check_sample_rate(&l, "t", 500).is_ok());
        check!(e.check_sample_rate(&l, "t", 1).is_err());
    }

    #[test]
    fn query_range_and_lookback_caps() {
        let l = Limits {
            max_query_length_secs: 3600,
            max_query_lookback_secs: 86_400,
            ..Limits::default()
        };
        let now = 1_000_000_000_000_i64;
        assert!(matches!(
            QueryEnforcer::check_range(&l, now - 7_200_000, now, now),
            Err(LimitError::QueryRangeTooLong { .. })
        ));
        assert!(matches!(
            QueryEnforcer::check_range(&l, now - 172_800_000, now - 172_799_000, now),
            Err(LimitError::QueryLookbackExceeded { .. })
        ));
    }

    #[test]
    fn query_count_caps() {
        let l = Limits {
            max_fetched_series_per_query: 10,
            max_samples_per_query: 1000,
            ..Limits::default()
        };
        check!(QueryEnforcer::check_series_count(&l, 11).is_err());
        check!(QueryEnforcer::check_sample_count(&l, 1001).is_err());
        check!(QueryEnforcer::check_series_count(&l, 10).is_ok());
    }
}
