//! KIP-599 `controller_mutation_rate` helper. The `CreateTopics`,
//! `CreatePartitions`, and `DeleteTopics` handlers call it after they assemble
//! the response.

use crabka_metadata::MetadataImage;
use crabka_units::{Time, convert::TimeExt, secs};

use super::buckets::QuotaBuckets;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ControllerMutationQuotaDecision {
    Allowed { delay: Time },
    Rejected { delay: Time },
}

impl ControllerMutationQuotaDecision {
    #[must_use]
    pub(crate) fn delay(self) -> Time {
        match self {
            Self::Allowed { delay } | Self::Rejected { delay } => delay,
        }
    }

    #[must_use]
    pub(crate) fn is_rejected(self) -> bool {
        matches!(self, Self::Rejected { .. })
    }
}

/// Consume `mutations` from the `controller_mutation_rate` bucket for
/// `(principal, client_id)`. This function returns the throttle delay to apply
/// before the handler sends the response. The delay is zero if no quota is
/// configured, if there is no overage, or if `mutations == 0`. The delay is
/// capped at `maximum_delay`.
#[must_use]
pub fn consume_controller_mutation_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: &str,
    mutations: u64,
    maximum_delay: Time,
) -> Time {
    apply_controller_mutation_quota_mode(
        image,
        buckets,
        principal,
        client_id,
        mutations,
        secs(1),
        maximum_delay,
        false,
    )
    .delay()
}

/// Atomically check accumulated controller-mutation debt and record this
/// operation. Strict APIs reject only when debt already exists; an operation
/// that crosses the limit is accepted and makes the next operation fail.
pub(crate) fn apply_controller_mutation_quota_mode(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: &str,
    mutations: u64,
    window: Time,
    maximum_delay: Time,
    strict: bool,
) -> ControllerMutationQuotaDecision {
    if mutations == 0 {
        return ControllerMutationQuotaDecision::Allowed {
            delay: <Time as TimeExt>::ZERO,
        };
    }
    let Some((entity_key, rate)) = super::lookup::lookup_quota_with_key(
        image,
        principal,
        client_id,
        "controller_mutation_rate",
    ) else {
        return ControllerMutationQuotaDecision::Allowed {
            delay: <Time as TimeExt>::ZERO,
        };
    };
    let window_secs = window.secs_f64();
    if !rate.is_finite() || rate <= 0.0 || !window_secs.is_finite() || window_secs <= 0.0 {
        return ControllerMutationQuotaDecision::Allowed {
            delay: <Time as TimeExt>::ZERO,
        };
    }

    let bucket = buckets.controller_mutation_bucket(&entity_key, rate, window_secs);
    let mut bucket = bucket
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let now = std::time::Instant::now();
    let capacity = rate * window_secs;
    if bucket.rate.to_bits() != rate.to_bits()
        || bucket.window_secs.to_bits() != window_secs.to_bits()
    {
        bucket.rate = rate;
        bucket.window_secs = window_secs;
        bucket.tokens = capacity;
    } else {
        bucket.tokens = (bucket.tokens
            + now.duration_since(bucket.updated_at).as_secs_f64() * rate)
            .min(capacity);
    }
    bucket.updated_at = now;

    if strict && bucket.tokens < 0.0 {
        return ControllerMutationQuotaDecision::Rejected {
            delay: Time::from_secs_f64((-bucket.tokens / rate).max(0.0)).min(maximum_delay),
        };
    }

    bucket.tokens -= mutations as f64;
    let delay = if !strict && bucket.tokens < 0.0 {
        Time::from_secs_f64((-bucket.tokens / rate).max(0.0)).min(maximum_delay)
    } else {
        <Time as TimeExt>::ZERO
    };
    ControllerMutationQuotaDecision::Allowed { delay }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_units::{millis, secs};

    use super::*;
    use crate::quota::test_support::image_with_quota as quota_image;

    fn img_with_quota(entity: Vec<(&str, Option<&str>)>, rate: f64) -> MetadataImage {
        quota_image(entity, "controller_mutation_rate", rate)
    }

    #[test]
    fn zero_mutations_returns_zero_delay() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", "", 0, secs(1));
        assert!(delay == <Time as TimeExt>::ZERO);
    }

    #[test]
    fn under_rate_returns_zero_delay() {
        // rate=10/sec, burst capacity=10 (one second of capacity).
        // 5 mutations consumed → bucket has 5 left → no overage.
        let img = img_with_quota(vec![("user", Some("alice"))], 10.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", "", 5, secs(1));
        assert!(delay == <Time as TimeExt>::ZERO);
    }

    #[test]
    fn overage_returns_capped_delay() {
        // rate=1/sec, burst=1; 100 mutations → overage 99 → delay 99s
        // → capped at 1s.
        let img = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", "", 100, secs(1));
        assert!(delay == secs(1));
    }

    #[test]
    fn overage_uses_configured_maximum_delay() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let buckets = QuotaBuckets::new();

        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", "", 100, millis(25));

        assert!(delay == millis(25));
    }

    #[test]
    fn fractional_strict_quota_rejects_the_operation_after_debt() {
        let img = img_with_quota(vec![("user", Some("alice"))], 0.015);
        let buckets = QuotaBuckets::new();
        let apply = |mutations| {
            apply_controller_mutation_quota_mode(
                &img,
                &buckets,
                "alice",
                "",
                mutations,
                secs(2_000),
                secs(1),
                true,
            )
        };

        assert!(!apply(10).is_rejected());
        assert!(!apply(10).is_rejected());
        assert!(!apply(20).is_rejected());
        assert!(apply(1).is_rejected());
    }
}
